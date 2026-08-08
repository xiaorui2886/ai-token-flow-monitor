pub mod discovery;
pub mod parser;
pub mod tailer;

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::core::types::ProcessOutcome;
use crate::core::types::{
    BaselineMode, EngineError, EventKind, MeasurementKind, RawSourceSample, RawUsage,
    SourceCheckpoint, SourceNativeIdentity, SourceType, TemporalAccuracy, TimingInfo,
    TokenAccuracy, UsageAccountingStrategy, UsageSemantics,
};
use crate::core::EnginePipeline;

use discovery::{CodexDiscovery, DiscoveredRollout};
use parser::{parse_rollout_line, CodexTokenSnapshot};
use tailer::{JsonlLine, JsonlTailer};

pub const CODEX_ROLLOUT_PRIORITY: u8 = 50;
pub const CODEX_AGENT_ID: &str = "codex";
pub const CODEX_AGENT_NAME: &str = "Codex";
pub const CODEX_PROVIDER: &str = "openai";
pub const CODEX_MODEL_UNKNOWN: &str = "unknown";
/// Internal canonical aggregation bucket for a rollout file (NOT a native Codex request id).
pub const CODEX_LOGICAL_REQUEST_ID: &str = "session_cumulative";

static ADAPTER_START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

fn monotonic_now_ns() -> u64 {
    let start = *ADAPTER_START.get_or_init(Instant::now);
    start.elapsed().as_nanos() as u64
}

/// Frozen Ground Truth semantics for Codex rollout JSONL.
pub fn codex_semantics() -> UsageSemantics {
    UsageSemantics {
        reasoning_is_output_subset: true,
        accounting_strategy: UsageAccountingStrategy::OpenAiStyle,
        provider_name: "openai".to_string(),
    }
}

/// Build a RawSourceSample for one token_count record.
/// - cumulative snapshot, no identity guessing, no fake IN timing.
pub fn build_snapshot_sample(
    file_hash: &str,
    session_id: &str,
    collector_run_id: &str,
    snapshot: &CodexTokenSnapshot,
    line_start_offset: u64,
    interval_ms: Option<u64>,
) -> RawSourceSample {
    RawSourceSample {
        sample_id: format!("codex_{}_{}", file_hash, line_start_offset),
        collector_run_id: collector_run_id.to_string(),
        source_adapter_id: format!("codex_rollout_{}", file_hash),
        source_type: SourceType::JSONL,
        observed_monotonic_ns: monotonic_now_ns(),
        wall_timestamp_ms: chrono::Utc::now().timestamp_millis(),
        source_timestamp_ms: snapshot.source_timestamp_ms,
        process_id: None,
        agent_id: CODEX_AGENT_ID.to_string(),
        agent_name: CODEX_AGENT_NAME.to_string(),
        session_id: session_id.to_string(),
        request_id: Some(CODEX_LOGICAL_REQUEST_ID.to_string()),
        turn_id: None,
        response_id: None,
        native_identity: SourceNativeIdentity {
            file_path_hash: Some(file_hash.to_string()),
            byte_offset: Some(line_start_offset),
            ..Default::default()
        },
        model: CODEX_MODEL_UNKNOWN.to_string(),
        provider: CODEX_PROVIDER.to_string(),
        event_kind: EventKind::Snapshot,
        is_cumulative: true,
        is_final: false,
        counter_reset_hint: false,
        raw_usage: RawUsage {
            raw_input_tokens: snapshot.total_usage.input_tokens,
            raw_output_tokens: snapshot.total_usage.output_tokens,
            raw_cache_read_tokens: snapshot.total_usage.cached_input_tokens,
            raw_cache_write_tokens: snapshot.total_usage.cache_write_input_tokens,
            raw_reasoning_tokens: snapshot.total_usage.reasoning_output_tokens,
            raw_total_tokens: snapshot.total_usage.total_tokens,
        },
        timing: TimingInfo {
            measurement_interval_ms: interval_ms,
            ..Default::default()
        },
        source_priority: CODEX_ROLLOUT_PRIORITY,
        token_accuracy: TokenAccuracy::Exact,
        temporal_accuracy: TemporalAccuracy::IntervalExact,
        measurement_kind: MeasurementKind::SnapshotDelta,
    }
}

#[derive(Debug, Clone)]
pub struct CodexAdapterConfig {
    pub tail_poll_interval: Duration,
    pub discovery_interval: Duration,
}

impl Default for CodexAdapterConfig {
    fn default() -> Self {
        Self {
            tail_poll_interval: Duration::from_millis(500),
            discovery_interval: Duration::from_secs(3),
        }
    }
}

/// Fatal adapter errors. All sanitized — never carry raw paths, JSON, prompts or IDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexAdapterError {
    /// Loading persisted checkpoints from SQLite failed.
    CheckpointLoad,
    /// A checkpoint-only durable write failed.
    CheckpointPersist,
    /// The engine reported a durable storage failure.
    EngineStorage,
    /// The adapter has halted; drop adapter + engine and recreate from durable SQLite.
    FatalNeedsEngineRestart,
}

/// Result of scanning `[0, observed_size)` for newline-terminated complete records.
#[derive(Debug, Clone)]
pub struct FileScanResult {
    /// Offset just after the last complete newline-terminated record.
    /// NEVER points into an EOF partial line.
    pub safe_complete_end_offset: u64,
    /// Last complete token_count record as (line start offset, snapshot).
    pub last_token_snapshot: Option<(u64, CodexTokenSnapshot)>,
}

/// Scan `[0, end_offset)` recognizing ONLY newline-terminated complete records.
/// A partial line at EOF is never included in `safe_complete_end_offset` — the tailer
/// re-reads it from that offset once the line is completed by the next append.
pub fn scan_file(path: &Path, end_offset: u64) -> FileScanResult {
    let data = std::fs::read(path).unwrap_or_default();
    let limit = (end_offset as usize).min(data.len());
    let mut safe_end = 0u64;
    let mut last_snapshot: Option<(u64, CodexTokenSnapshot)> = None;
    let mut start = 0usize;
    for (i, &b) in data[..limit].iter().enumerate() {
        if b == b'\n' {
            let end = i + 1;
            if let Ok(Some(snap)) = parse_rollout_line(&data[start..end]) {
                last_snapshot = Some((start as u64, snap));
            }
            safe_end = end as u64;
            start = end;
        }
    }
    FileScanResult {
        safe_complete_end_offset: safe_end,
        last_token_snapshot: last_snapshot,
    }
}

/// Per-file state. NEVER shared between files (offsets, baselines, checkpoints, identity are per-file).
#[derive(Debug, Clone)]
pub struct RolloutFileState {
    pub path: PathBuf,
    pub file_hash: String,
    pub source_adapter_id: String,
    pub session_id: String,
    pub tailer: JsonlTailer,
    pub last_token_source_ts_ms: Option<i64>,
    pub last_total_output: Option<u64>,
    pub warmup_snapshot: Option<(u64, CodexTokenSnapshot)>,
    pub mode_for_next: BaselineMode,
    pub checkpoint: SourceCheckpoint,
    pub truncated_recovered: bool,
    /// Existing-attach file whose Safe EOF checkpoint still needs a checkpoint-only persist (§15).
    pub initial_attach_pending: bool,
}

#[derive(Debug, Clone, Default)]
pub struct PollStats {
    pub files_tracked: usize,
    pub token_records_consumed: usize,
    pub canonical_deltas: usize,
    pub validation_matches: u64,
    pub validation_mismatches: u64,
}

/// Codex Rollout JSONL Adapter V1.
/// Passive read only. Multiple rollout files managed independently.
///
/// Failure policy (§9): any durable storage error is FATAL. The adapter stops ingesting,
/// subsequent `poll()` returns `FatalNeedsEngineRestart`. Recovery = drop adapter AND engine,
/// recreate both from durable SQLite, reload checkpoints, ReplayRestore, continue.
pub struct CodexAdapter {
    pub config: CodexAdapterConfig,
    pub discovery: CodexDiscovery,
    files: HashMap<String, RolloutFileState>,
    last_discovery: Option<Instant>,
    /// True after the first `refresh_discovery()` completed: later new files are Runtime New Files.
    initial_discovery_complete: bool,
    /// Set on any durable failure; the adapter must be dropped and recreated.
    fatal: Option<CodexAdapterError>,
}

impl CodexAdapter {
    pub fn new(config: CodexAdapterConfig) -> Self {
        Self::with_discovery(config, CodexDiscovery::new())
    }

    pub fn with_discovery(config: CodexAdapterConfig, discovery: CodexDiscovery) -> Self {
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

    /// Discover new rollout files and track them.
    /// - The first completed refresh marks `initial_discovery_complete`; files found then use
    ///   Existing Attach semantics (checkpoint / ReplayRestore warm-up).
    /// - Files discovered AFTERWARDS are Runtime New Files: usage written while the monitor is
    ///   running must be counted (tail from 0, first record KnownZeroOrigin), never treated as
    ///   pre-monitor history.
    /// - Checkpoint load failure stops discovery and halts the adapter (§8, §9).
    pub fn refresh_discovery(
        &mut self,
        engine: &mut EnginePipeline,
    ) -> Result<usize, CodexAdapterError> {
        let now = Instant::now();
        let due = match self.last_discovery {
            Some(t) => now.duration_since(t) >= self.config.discovery_interval,
            None => true,
        };
        if !due {
            return Ok(0);
        }
        self.last_discovery = Some(now);

        // §8: never fake an empty checkpoint set.
        let existing_checkpoints = match engine.storage.lock().load_checkpoints() {
            Ok(cps) => cps,
            Err(_) => {
                self.fatal = Some(CodexAdapterError::CheckpointLoad);
                return Err(CodexAdapterError::CheckpointLoad);
            }
        };

        let mut added = 0;
        for rollout in self.discovery.discover_rollouts() {
            if self.files.contains_key(&rollout.file_hash) {
                continue;
            }
            let cp = existing_checkpoints
                .iter()
                .find(|c| c.source_id == format!("codex_rollout_{}", rollout.file_hash))
                .cloned();
            self.add_tracked_file(&rollout, cp);
            added += 1;
        }
        self.initial_discovery_complete = true;
        Ok(added)
    }

    /// Track a rollout file.
    /// - `existing_checkpoint` (from SQLite): restart attach — tail from checkpoint, warm-up the
    ///   last complete token_count before it (ReplayRestore).
    /// - no checkpoint + discovery already completed: Runtime New File — tail from 0, first record
    ///   KnownZeroOrigin (usage generated after monitor start MUST be counted).
    /// - no checkpoint + first discovery: Existing Attach — scan to Safe EOF (never `rollout.size`),
    ///   warm-up last complete token_count, Safe EOF checkpoint persisted at first poll.
    pub fn add_tracked_file(
        &mut self,
        rollout: &DiscoveredRollout,
        existing_checkpoint: Option<SourceCheckpoint>,
    ) {
        if self.files.contains_key(&rollout.file_hash) {
            return;
        }
        let source_adapter_id = format!("codex_rollout_{}", rollout.file_hash);
        let session_id = format!("codex_session_{}", rollout.file_hash);

        let (checkpoint, mode_for_next, warmup, tail_start, initial_attach_pending) =
            match existing_checkpoint {
                Some(mut cp) => {
                    // Restart attach: only the region BEFORE the checkpoint may be warm-up baseline.
                    let tail_start = cp.last_file_offset;
                    let warmup = scan_file(&rollout.path, cp.last_file_offset).last_token_snapshot;
                    let mode = if warmup.is_some() {
                        BaselineMode::ContinuousEpoch
                    } else {
                        BaselineMode::KnownZeroOrigin
                    };
                    cp.source_id = source_adapter_id.clone();
                    (cp, mode, warmup, tail_start, false)
                }
                None if self.initial_discovery_complete => {
                    // Runtime New File: nothing in the file predates the monitor.
                    let cp = SourceCheckpoint {
                        source_id: source_adapter_id.clone(),
                        last_file_offset: 0,
                        last_db_row_id: None,
                        last_sequence_id: None,
                        watermark_timestamp_ms: 0,
                        updated_at_ms: chrono::Utc::now().timestamp_millis(),
                    };
                    (cp, BaselineMode::KnownZeroOrigin, None, 0, false)
                }
                None => {
                    // Existing Attach: Safe EOF, never rollout.size (§1, §2).
                    let scan = scan_file(&rollout.path, rollout.size);
                    let warmup = scan.last_token_snapshot;
                    let mode = if warmup.is_some() {
                        BaselineMode::ContinuousEpoch
                    } else {
                        BaselineMode::KnownZeroOrigin
                    };
                    let watermark = warmup
                        .as_ref()
                        .and_then(|(_, s)| s.source_timestamp_ms)
                        .unwrap_or(0);
                    let cp = SourceCheckpoint {
                        source_id: source_adapter_id.clone(),
                        last_file_offset: scan.safe_complete_end_offset,
                        last_db_row_id: None,
                        last_sequence_id: None,
                        watermark_timestamp_ms: watermark,
                        updated_at_ms: chrono::Utc::now().timestamp_millis(),
                    };
                    (cp, mode, warmup, scan.safe_complete_end_offset, true)
                }
            };

        let last_token_ts = warmup.as_ref().and_then(|(_, s)| s.source_timestamp_ms);

        self.files.insert(
            rollout.file_hash.clone(),
            RolloutFileState {
                path: rollout.path.clone(),
                file_hash: rollout.file_hash.clone(),
                source_adapter_id,
                session_id,
                tailer: JsonlTailer::new(tail_start),
                last_token_source_ts_ms: last_token_ts,
                last_total_output: warmup
                    .as_ref()
                    .and_then(|(_, s)| s.total_usage.output_tokens),
                warmup_snapshot: warmup,
                mode_for_next,
                checkpoint,
                truncated_recovered: false,
                initial_attach_pending,
            },
        );
    }

    /// Poll all tracked files once and feed the engine.
    /// On any durable failure the adapter halts permanently (drop + recreate required).
    pub fn poll(&mut self, engine: &mut EnginePipeline) -> Result<PollStats, CodexAdapterError> {
        if self.fatal.is_some() {
            // §9: after a storage failure this adapter instance must not ingest anything more.
            return Err(CodexAdapterError::FatalNeedsEngineRestart);
        }
        let mut stats = PollStats {
            files_tracked: self.files.len(),
            ..Default::default()
        };
        for state in self.files.values_mut() {
            if let Err(e) = Self::poll_file(state, engine, &mut stats) {
                self.fatal = Some(e);
                return Err(e);
            }
        }
        Ok(stats)
    }

    fn poll_file(
        state: &mut RolloutFileState,
        engine: &mut EnginePipeline,
        stats: &mut PollStats,
    ) -> Result<(), CodexAdapterError> {
        let file_size = std::fs::metadata(&state.path).map(|m| m.len()).unwrap_or(0);

        // §3: Truncate / unexpected replacement -> Safe EOF scan. checkpoint=file_size is forbidden.
        if file_size < state.tailer.offset {
            let scan = scan_file(&state.path, file_size);
            state.tailer.reset(scan.safe_complete_end_offset);
            state.warmup_snapshot = scan.last_token_snapshot;
            state.last_token_source_ts_ms = state
                .warmup_snapshot
                .as_ref()
                .and_then(|(_, s)| s.source_timestamp_ms);
            state.last_total_output = state
                .warmup_snapshot
                .as_ref()
                .and_then(|(_, s)| s.total_usage.output_tokens);
            state.mode_for_next = BaselineMode::ContinuousEpoch;
            state.checkpoint.last_file_offset = scan.safe_complete_end_offset;
            state.checkpoint.watermark_timestamp_ms = state.last_token_source_ts_ms.unwrap_or(0);
            state.checkpoint.updated_at_ms = chrono::Utc::now().timestamp_millis();
            state.truncated_recovered = true;
            // Sanitized warning: source hash + offsets only, no raw path.
            eprintln!(
                "codex_adapter: source truncated, safe re-baseline: {} @ safe_eof={}",
                state.source_adapter_id, scan.safe_complete_end_offset
            );
        }

        // Warm baseline (ReplayRestore) BEFORE reading any new data. Produces zero canonical delta.
        if let Some((warm_offset, warm_snap)) = state.warmup_snapshot.take() {
            let sample = build_snapshot_sample(
                &state.file_hash,
                &state.session_id,
                &engine.collector_run_id,
                &warm_snap,
                warm_offset,
                None,
            );
            engine
                .process_sample(&sample, &codex_semantics(), BaselineMode::ReplayRestore)
                .map_err(map_engine_error)?;
        }

        // §3: persist the post-truncation Safe EOF checkpoint (failure is fatal).
        if state.truncated_recovered {
            state.truncated_recovered = false;
            Self::persist_checkpoint(&state.checkpoint, engine)?;
        }

        // §15/§20: existing attach -> persist Safe EOF checkpoint even with no new token_count.
        if state.initial_attach_pending {
            state.initial_attach_pending = false;
            Self::persist_checkpoint(&state.checkpoint, engine)?;
        }

        // Tail new bytes (if any) and process complete records.
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
            // §13: one fatal record aborts the whole chunk — N+1/N+2 must NOT be processed.
            for line in lines {
                Self::process_line(state, line, engine, stats)?;
            }
        }
        Ok(())
    }

    /// Idempotent checkpoint-only durable write. Failure is fatal (§9, §11).
    fn persist_checkpoint(
        cp: &SourceCheckpoint,
        engine: &mut EnginePipeline,
    ) -> Result<(), CodexAdapterError> {
        engine
            .storage
            .lock()
            .save_canonical_transaction(&[], &[], &[], Some(cp))
            .map_err(|_| CodexAdapterError::CheckpointPersist)
    }

    fn process_line(
        state: &mut RolloutFileState,
        line: JsonlLine,
        engine: &mut EnginePipeline,
        stats: &mut PollStats,
    ) -> Result<(), CodexAdapterError> {
        match parse_rollout_line(&line.bytes) {
            Ok(Some(snapshot)) => {
                stats.token_records_consumed += 1;

                // Candidate runtime state — committed only after durable success (§12).
                let prev_total_output = state.last_total_output;
                let candidate_output = snapshot.total_usage.output_tokens;
                let candidate_ts = snapshot.source_timestamp_ms;

                // §19: interval from source timestamps only (never file mtime).
                let interval_ms =
                    match (snapshot.source_timestamp_ms, state.last_token_source_ts_ms) {
                        (Some(cur), Some(prev)) if cur > prev => Some((cur - prev) as u64),
                        _ => None,
                    };

                // Checkpoint always points at the END of a complete newline-terminated record.
                let cp = SourceCheckpoint {
                    source_id: state.source_adapter_id.clone(),
                    last_file_offset: line.line_end_offset,
                    last_db_row_id: None,
                    last_sequence_id: None,
                    watermark_timestamp_ms: snapshot.source_timestamp_ms.unwrap_or(0),
                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                };

                let sample = build_snapshot_sample(
                    &state.file_hash,
                    &state.session_id,
                    &engine.collector_run_id,
                    &snapshot,
                    line.line_start_offset,
                    interval_ms,
                );
                let mode = state.mode_for_next;

                match engine.process_sample_with_checkpoint(
                    &sample,
                    &codex_semantics(),
                    mode,
                    Some(&cp),
                ) {
                    Ok(ProcessOutcome::Committed(details)) => {
                        if details.delta.is_none() {
                            // §23: zero-delta / dedup-suppressed -> the engine persisted nothing;
                            // idempotent checkpoint-only commit MUST succeed before state advances (§11, §12).
                            Self::persist_checkpoint(&cp, engine)?;
                        }
                        state.last_total_output = candidate_output;
                        state.last_token_source_ts_ms = candidate_ts;
                        state.mode_for_next = BaselineMode::ContinuousEpoch;
                        state.checkpoint = cp;
                        if details.delta.is_some() {
                            stats.canonical_deltas += 1;
                        }
                    }
                    Ok(ProcessOutcome::Rejected { .. }) => {
                        // Finalized late event: engine already persisted checkpoint-only; runtime state advances.
                        state.last_total_output = candidate_output;
                        state.last_token_source_ts_ms = candidate_ts;
                        state.mode_for_next = BaselineMode::ContinuousEpoch;
                        state.checkpoint = cp;
                    }
                    Ok(ProcessOutcome::Retryable { .. }) => {
                        // Leave everything unchanged; retry later.
                        return Ok(());
                    }
                    Err(e) => {
                        // Sanitized: source hash + offset only. Fatal halt (§9, §13).
                        eprintln!(
                            "codex_adapter: engine error {} @ {}",
                            state.source_adapter_id, line.line_start_offset
                        );
                        return Err(map_engine_error(e));
                    }
                }

                // §22: last_token_usage validation ONLY (never a second canonical source).
                if let Some(prev) = prev_total_output {
                    let delta = candidate_output.unwrap_or(0).saturating_sub(prev);
                    if Some(delta) == snapshot.last_usage.output_tokens {
                        stats.validation_matches += 1;
                    } else {
                        stats.validation_mismatches += 1;
                    }
                }
            }
            Ok(None) => {
                // Non-token record: in-memory offset only (no SQLite write required).
            }
            Err(_) => {
                // Sanitized: source hash + offset only. Record is complete -> consumed.
                eprintln!(
                    "codex_adapter: parse error {} @ {}",
                    state.source_adapter_id, line.line_start_offset
                );
            }
        }
        Ok(())
    }
}

/// Any engine error is fatal for this adapter instance (§9).
fn map_engine_error(e: EngineError) -> CodexAdapterError {
    match e {
        EngineError::StorageError(_) | EngineError::InvalidSample(_) => {
            CodexAdapterError::EngineStorage
        }
    }
}
