pub mod discovery;
pub mod parser;
pub mod tailer;

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::adapters::common::identity::stable_hash16;
use crate::adapters::common::jsonl::{read_scan_safe_eof, JsonlLine, JsonlTailer};
use crate::core::types::ProcessOutcome;
use crate::core::types::{
    BaselineMode, EngineError, EventKind, MeasurementKind, RawSourceSample, RawUsage,
    RequestCorrelationKey, SourceCheckpoint, SourceNativeIdentity, SourceType, TemporalAccuracy,
    TimingInfo, TokenAccuracy, UsageAccountingStrategy, UsageSemantics,
};
use crate::core::EnginePipeline;
use crate::runtime::types::ObservationTime;

use discovery::{ClaudeDiscovery, DiscoveredTranscript};
use parser::{parse_claude_line, ClaudeUsageFinality, ClaudeUsageRecord};

pub const CLAUDE_TRANSCRIPT_PRIORITY: u8 = 50;
pub const CLAUDE_AGENT_ID: &str = "claude";
pub const CLAUDE_AGENT_NAME: &str = "Claude Code";
/// Source client / accounting surface — NOT a declaration of the upstream model provider.
pub const CLAUDE_PROVIDER: &str = "claude_code";
pub const CLAUDE_MODEL_UNKNOWN: &str = "unknown";

/// Frozen Ground Truth semantics for Claude Code transcripts.
pub fn claude_semantics() -> UsageSemantics {
    UsageSemantics {
        reasoning_is_output_subset: true,
        accounting_strategy: UsageAccountingStrategy::AnthropicStyle,
        provider_name: CLAUDE_PROVIDER.to_string(),
    }
}

/// Hash a raw Claude sessionId -> `claude_session_<16 hex>` (raw ID never enters DB/logs).
pub fn claude_session_id(raw_session: &str) -> String {
    format!("claude_session_{}", stable_hash16(raw_session))
}

/// Hash a raw Claude message.id -> `claude_message_<16 hex>` (stable dedup identity).
pub fn claude_message_id(raw_message: &str) -> String {
    format!("claude_message_{}", stable_hash16(raw_message))
}

/// Build the canonical RawSourceSample for an AuthoritativeFinal record (§12-§13).
/// `observation` comes from the runtime SHARED CollectorClock (Task 03A §6).
pub fn build_final_sample(
    file_hash: &str,
    collector_run_id: &str,
    record: &ClaudeUsageRecord,
    line_start_offset: u64,
    observation: &ObservationTime,
) -> RawSourceSample {
    let session_id = record
        .session_id
        .as_deref()
        .map(claude_session_id)
        .unwrap_or_default();
    let message_id = record
        .message_id
        .as_deref()
        .map(claude_message_id)
        .unwrap_or_default();

    RawSourceSample {
        sample_id: format!("claude_{}_{}", session_id, message_id),
        collector_run_id: collector_run_id.to_string(),
        source_adapter_id: format!("claude_transcript_{}", file_hash),
        source_type: SourceType::JSONL,
        observed_monotonic_ns: observation.monotonic_ns,
        wall_timestamp_ms: observation.wall_timestamp_ms,
        source_timestamp_ms: record.source_timestamp_ms,
        process_id: None,
        agent_id: CLAUDE_AGENT_ID.to_string(),
        agent_name: CLAUDE_AGENT_NAME.to_string(),
        session_id,
        request_id: Some(message_id.clone()),
        turn_id: None,
        response_id: None,
        native_identity: SourceNativeIdentity {
            native_event_id: Some(message_id.clone()),
            native_message_id: Some(message_id),
            file_path_hash: Some(file_hash.to_string()),
            byte_offset: Some(line_start_offset),
            ..Default::default()
        },
        model: record
            .model
            .clone()
            .unwrap_or_else(|| CLAUDE_MODEL_UNKNOWN.to_string()),
        provider: CLAUDE_PROVIDER.to_string(),
        event_kind: EventKind::Final,
        is_cumulative: false,
        is_final: true,
        counter_reset_hint: false,
        raw_usage: RawUsage {
            raw_input_tokens: record.input_tokens,
            raw_output_tokens: record.output_tokens,
            raw_cache_read_tokens: record.cache_read_input_tokens,
            raw_cache_write_tokens: record.cache_creation_input_tokens,
            raw_reasoning_tokens: None,
            raw_total_tokens: None,
        },
        timing: TimingInfo {
            ..Default::default()
        },
        source_priority: CLAUDE_TRANSCRIPT_PRIORITY,
        token_accuracy: TokenAccuracy::Exact,
        temporal_accuracy: TemporalAccuracy::TurnExact,
        measurement_kind: MeasurementKind::NativeCounter,
    }
}

#[derive(Debug, Clone)]
pub struct ClaudeAdapterConfig {
    pub tail_poll_interval: Duration,
    pub discovery_interval: Duration,
}

impl Default for ClaudeAdapterConfig {
    fn default() -> Self {
        Self {
            tail_poll_interval: Duration::from_millis(500),
            discovery_interval: Duration::from_secs(3),
        }
    }
}

/// Fatal adapter errors. All sanitized — never carry raw paths, JSON, prompts or IDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaudeAdapterError {
    CheckpointLoad,
    CheckpointPersist,
    EngineStorage,
    FatalNeedsEngineRestart,
}

#[derive(Debug, Clone, Default)]
pub struct ClaudePollStats {
    pub files_tracked: usize,
    pub token_records_consumed: usize,
    pub placeholders: u64,
    pub authoritative_finals: u64,
    /// Identical final re-emits resolved adapter-side as checkpoint-only (§16 option B).
    pub identical_reemit_dedup: u64,
    /// Same message.id re-finalized with DIFFERENT values -> Core authoritative reconciliation (§15).
    pub changed_final_rewrites: u64,
    /// Records whose identity (sessionId/message.id) could not be resolved (§31 health degraded).
    pub health_degraded: u64,
}

/// Per-file state. NEVER shared between files (offsets, checkpoints, partial buffers are per-file).
/// The logical Claude session comes from the event's own sessionId hash — never from file_hash.
#[derive(Debug, Clone)]
pub struct TranscriptFileState {
    pub path: PathBuf,
    pub file_hash: String,
    pub source_adapter_id: String,
    pub tailer: JsonlTailer,
    pub checkpoint: SourceCheckpoint,
    /// Existing-attach file whose Safe EOF checkpoint still needs a checkpoint-only persist.
    pub initial_attach_pending: bool,
}

/// Claude Code Transcript JSONL Adapter V1.
/// Passive read only. Multiple transcript files managed independently.
///
/// Failure policy (frozen, same as Codex): any durable storage error is FATAL.
/// The adapter stops ingesting; subsequent `poll()` returns `FatalNeedsEngineRestart`.
/// Recovery = drop adapter AND engine, recreate both from durable SQLite.
pub struct ClaudeAdapter {
    pub config: ClaudeAdapterConfig,
    pub discovery: ClaudeDiscovery,
    files: HashMap<String, TranscriptFileState>,
    last_discovery: Option<Instant>,
    /// True after the first `refresh_discovery()` completed: later new files are Runtime New Files.
    initial_discovery_complete: bool,
    /// Set on any durable failure; the adapter must be dropped and recreated.
    fatal: Option<ClaudeAdapterError>,
}

impl ClaudeAdapter {
    pub fn new(config: ClaudeAdapterConfig) -> Self {
        Self::with_discovery(config, ClaudeDiscovery::new())
    }

    pub fn with_discovery(config: ClaudeAdapterConfig, discovery: ClaudeDiscovery) -> Self {
        Self {
            config,
            discovery,
            files: HashMap::new(),
            last_discovery: None,
            initial_discovery_complete: false,
            fatal: None,
        }
    }

    pub fn tracked_count(&self) -> usize {
        self.files.len()
    }

    /// Discover new transcript files and track them.
    /// - First completed refresh: Existing Attach semantics (§18: Safe EOF, no history import).
    /// - Files discovered afterwards: Runtime New File semantics (§19: tail from 0, capture all).
    /// - Checkpoint load failure stops discovery and halts the adapter.
    pub fn refresh_discovery(
        &mut self,
        engine: &mut EnginePipeline,
    ) -> Result<usize, ClaudeAdapterError> {
        let now = Instant::now();
        let due = match self.last_discovery {
            Some(t) => now.duration_since(t) >= self.config.discovery_interval,
            None => true,
        };
        if !due {
            return Ok(0);
        }
        self.last_discovery = Some(now);

        let existing_checkpoints = match engine.storage.lock().load_checkpoints() {
            Ok(cps) => cps,
            Err(_) => {
                self.fatal = Some(ClaudeAdapterError::CheckpointLoad);
                return Err(ClaudeAdapterError::CheckpointLoad);
            }
        };

        let mut added = 0;
        for transcript in self.discovery.discover_transcripts() {
            if self.files.contains_key(&transcript.file_hash) {
                continue;
            }
            let cp = existing_checkpoints
                .iter()
                .find(|c| c.source_id == format!("claude_transcript_{}", transcript.file_hash))
                .cloned();
            self.add_tracked_file(&transcript, cp);
            added += 1;
        }
        self.initial_discovery_complete = true;
        Ok(added)
    }

    /// Track a transcript file.
    /// - `existing_checkpoint` (SQLite): restart attach — tail from checkpoint, NO ReplayRestore
    ///   (Claude usage is per-message Final NativeCounter, §20).
    /// - no checkpoint + discovery completed: Runtime New File — tail from 0, capture everything.
    /// - no checkpoint + first discovery: Existing Attach — Safe EOF checkpoint, tail from Safe EOF,
    ///   NO historical usage import (§18).
    pub fn add_tracked_file(
        &mut self,
        transcript: &DiscoveredTranscript,
        existing_checkpoint: Option<SourceCheckpoint>,
    ) {
        if self.files.contains_key(&transcript.file_hash) {
            return;
        }
        let source_adapter_id = format!("claude_transcript_{}", transcript.file_hash);

        let (checkpoint, tail_start, initial_attach_pending) = match existing_checkpoint {
            Some(mut cp) => {
                let tail_start = cp.last_file_offset;
                cp.source_id = source_adapter_id.clone();
                (cp, tail_start, false)
            }
            None if self.initial_discovery_complete => {
                let cp = SourceCheckpoint {
                    source_id: source_adapter_id.clone(),
                    last_file_offset: 0,
                    last_db_row_id: None,
                    last_sequence_id: None,
                    watermark_timestamp_ms: 0,
                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                };
                (cp, 0, false)
            }
            None => {
                // Existing Attach: Safe EOF only — never `transcript.size` (§23).
                let safe_eof = read_scan_safe_eof(&transcript.path, transcript.size);
                let cp = SourceCheckpoint {
                    source_id: source_adapter_id.clone(),
                    last_file_offset: safe_eof,
                    last_db_row_id: None,
                    last_sequence_id: None,
                    watermark_timestamp_ms: 0,
                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                };
                (cp, safe_eof, true)
            }
        };

        self.files.insert(
            transcript.file_hash.clone(),
            TranscriptFileState {
                path: transcript.path.clone(),
                file_hash: transcript.file_hash.clone(),
                source_adapter_id,
                tailer: JsonlTailer::new(tail_start),
                checkpoint,
                initial_attach_pending,
            },
        );
    }

    /// Poll all tracked files once and feed the engine.
    /// `observation` comes from the runtime SHARED CollectorClock — never an adapter-local clock.
    pub fn poll(
        &mut self,
        engine: &mut EnginePipeline,
        observation: &ObservationTime,
    ) -> Result<ClaudePollStats, ClaudeAdapterError> {
        if self.fatal.is_some() {
            return Err(ClaudeAdapterError::FatalNeedsEngineRestart);
        }
        let mut stats = ClaudePollStats {
            files_tracked: self.files.len(),
            ..Default::default()
        };
        for state in self.files.values_mut() {
            if let Err(e) = Self::poll_file(state, engine, observation, &mut stats) {
                self.fatal = Some(e);
                return Err(e);
            }
        }
        Ok(stats)
    }

    fn poll_file(
        state: &mut TranscriptFileState,
        engine: &mut EnginePipeline,
        observation: &ObservationTime,
        stats: &mut ClaudePollStats,
    ) -> Result<(), ClaudeAdapterError> {
        let file_size = std::fs::metadata(&state.path).map(|m| m.len()).unwrap_or(0);

        // §31: Truncate / replacement. Claude identity (sessionId+message.id) is exact, so a
        // full re-read from 0 is SAFE: placeholders stay ignored, already-finalized identical
        // messages dedup to checkpoint-only, new messages count normally. No double count.
        if file_size < state.tailer.offset {
            state.tailer.reset(0);
            state.checkpoint.last_file_offset = 0;
            state.checkpoint.updated_at_ms = chrono::Utc::now().timestamp_millis();
            eprintln!(
                "claude_adapter: transcript truncated, safe re-read from 0: {}",
                state.source_adapter_id
            );
        }

        // §18: existing attach -> persist Safe EOF checkpoint even with no new records.
        if state.initial_attach_pending {
            state.initial_attach_pending = false;
            Self::persist_checkpoint(&state.checkpoint, engine)?;
        }

        if file_size > state.tailer.offset {
            let mut file = match std::fs::File::open(&state.path) {
                Ok(f) => f,
                Err(_) => return Ok(()),
            };
            if file.seek(SeekFrom::Start(state.tailer.offset)).is_err() {
                return Ok(());
            }
            let mut chunk = Vec::new();
            if file.read_to_end(&mut chunk).is_err() {
                return Ok(());
            }
            let lines = state.tailer.feed(&chunk);
            for line in lines {
                Self::process_line(state, line, engine, observation, stats)?;
            }
        }
        Ok(())
    }

    /// Idempotent checkpoint-only durable write. Failure is fatal.
    fn persist_checkpoint(
        cp: &SourceCheckpoint,
        engine: &mut EnginePipeline,
    ) -> Result<(), ClaudeAdapterError> {
        engine
            .storage
            .lock()
            .save_canonical_transaction(&[], &[], &[], Some(cp))
            .map_err(|_| ClaudeAdapterError::CheckpointPersist)
    }

    fn process_line(
        state: &mut TranscriptFileState,
        line: JsonlLine,
        engine: &mut EnginePipeline,
        observation: &ObservationTime,
        stats: &mut ClaudePollStats,
    ) -> Result<(), ClaudeAdapterError> {
        match parse_claude_line(&line.bytes) {
            Ok(Some(record)) => {
                stats.token_records_consumed += 1;

                let cp = SourceCheckpoint {
                    source_id: state.source_adapter_id.clone(),
                    last_file_offset: line.line_end_offset,
                    last_db_row_id: None,
                    last_sequence_id: None,
                    watermark_timestamp_ms: record.source_timestamp_ms.unwrap_or(0),
                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                };

                match record.finality {
                    ClaudeUsageFinality::Placeholder => {
                        // §10: Placeholder/Prefill NEVER canonical — no ledger, no TPS, no totals.
                        // Only the durable checkpoint advances.
                        stats.placeholders += 1;
                        Self::persist_checkpoint(&cp, engine)?;
                        state.checkpoint = cp;
                    }
                    ClaudeUsageFinality::AuthoritativeFinal => {
                        // Identity resolution (§31): unparseable identity -> health degraded,
                        // record consumed (checkpoint advances), never canonical.
                        let (Some(raw_session), Some(raw_message)) =
                            (&record.session_id, &record.message_id)
                        else {
                            stats.health_degraded += 1;
                            eprintln!(
                                "claude_adapter: identity unresolved {} @ {}",
                                state.source_adapter_id, line.line_start_offset
                            );
                            Self::persist_checkpoint(&cp, engine)?;
                            state.checkpoint = cp;
                            return Ok(());
                        };

                        let session_id = claude_session_id(raw_session);
                        let request_id = claude_message_id(raw_message);
                        let key = RequestCorrelationKey {
                            agent_id: CLAUDE_AGENT_ID.to_string(),
                            session_id: session_id.clone(),
                            request_id: request_id.clone(),
                        };

                        // §16 option B: identical final re-emit detected via finalized ledger
                        // with matching totals -> checkpoint-only (no pointless SQLite rewrite).
                        let expected = expected_totals(&record);
                        let already_finalized_identical = engine
                            .request_ledger
                            .get_ledger(&key)
                            .map(|l| l.is_finalized && ledger_matches(l, &expected))
                            .unwrap_or(false);

                        if already_finalized_identical {
                            stats.identical_reemit_dedup += 1;
                            Self::persist_checkpoint(&cp, engine)?;
                            state.checkpoint = cp;
                            return Ok(());
                        }

                        // §15: changed final rewrite -> re-enter Core Final authoritative path.
                        let changed = engine
                            .request_ledger
                            .get_ledger(&key)
                            .map(|l| l.is_finalized && !ledger_matches(l, &expected))
                            .unwrap_or(false);
                        if changed {
                            stats.changed_final_rewrites += 1;
                        }

                        let sample = build_final_sample(
                            &state.file_hash,
                            &engine.collector_run_id,
                            &record,
                            line.line_start_offset,
                            observation,
                        );

                        // §14: Final path -> Core authoritative reconciliation; BaselineMode is
                        // irrelevant for Final (no cumulative baseline), use KnownZeroOrigin.
                        match engine.process_sample_with_checkpoint(
                            &sample,
                            &claude_semantics(),
                            BaselineMode::KnownZeroOrigin,
                            Some(&cp),
                        ) {
                            Ok(ProcessOutcome::Committed(_)) => {
                                stats.authoritative_finals += 1;
                                state.checkpoint = cp;
                            }
                            Ok(ProcessOutcome::Rejected { .. }) => {
                                // Defensive: checkpoint already persisted by Core in this path.
                                state.checkpoint = cp;
                            }
                            Ok(ProcessOutcome::Retryable { .. }) => {
                                // Leave everything unchanged; retry later.
                            }
                            Err(e) => {
                                eprintln!(
                                    "claude_adapter: engine error {} @ {}",
                                    state.source_adapter_id, line.line_start_offset
                                );
                                return Err(map_engine_error(e));
                            }
                        }
                    }
                }
            }
            Ok(None) => {
                // Non-assistant record: in-memory offset only.
            }
            Err(_) => {
                // Sanitized: complete record, malformed -> consumed.
                eprintln!(
                    "claude_adapter: parse error {} @ {}",
                    state.source_adapter_id, line.line_start_offset
                );
            }
        }
        Ok(())
    }
}

/// Expected canonical totals for one AuthoritativeFinal under AnthropicStyle accounting.
fn expected_totals(record: &ClaudeUsageRecord) -> [u64; 6] {
    let input = record.input_tokens.unwrap_or(0);
    let cache_read = record.cache_read_input_tokens.unwrap_or(0);
    let cache_write = record.cache_creation_input_tokens.unwrap_or(0);
    [
        input + cache_read + cache_write,  // context input
        input,                             // fresh input
        record.output_tokens.unwrap_or(0), // output
        cache_read,                        // cache read
        cache_write,                       // cache write
        0,                                 // reasoning (Unavailable)
    ]
}

fn ledger_matches(l: &crate::core::types::CanonicalRequestLedger, expected: &[u64; 6]) -> bool {
    l.canonical_context_input_total == expected[0]
        && l.canonical_fresh_input_total == expected[1]
        && l.canonical_output_total == expected[2]
        && l.canonical_cache_read == expected[3]
        && l.canonical_cache_write == expected[4]
        && l.canonical_reasoning == expected[5]
}

/// Any engine error is fatal for this adapter instance.
fn map_engine_error(e: EngineError) -> ClaudeAdapterError {
    match e {
        EngineError::StorageError(_) | EngineError::InvalidSample(_) => {
            ClaudeAdapterError::EngineStorage
        }
    }
}
