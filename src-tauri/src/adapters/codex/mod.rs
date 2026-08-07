pub mod discovery;
pub mod parser;
pub mod tailer;

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::core::types::ProcessOutcome;
use crate::core::types::{
    BaselineMode, EventKind, MeasurementKind, RawSourceSample, RawUsage, SourceCheckpoint,
    SourceNativeIdentity, SourceType, TemporalAccuracy, TimingInfo, TokenAccuracy,
    UsageAccountingStrategy, UsageSemantics,
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
pub struct CodexAdapter {
    pub config: CodexAdapterConfig,
    pub discovery: CodexDiscovery,
    files: HashMap<String, RolloutFileState>,
    last_discovery: Option<Instant>,
}

impl CodexAdapter {
    pub fn new(config: CodexAdapterConfig) -> Self {
        Self {
            config,
            discovery: CodexDiscovery::new(),
            files: HashMap::new(),
            last_discovery: None,
        }
    }

    pub fn tracked_count(&self) -> usize {
        self.files.len()
    }

    /// Discover new rollout files and track them (no checkpoint: existing-file attach semantics).
    pub fn refresh_discovery(&mut self, engine: &mut EnginePipeline) -> usize {
        let now = Instant::now();
        let due = match self.last_discovery {
            Some(t) => now.duration_since(t) >= self.config.discovery_interval,
            None => true,
        };
        if !due {
            return 0;
        }
        self.last_discovery = Some(now);

        let mut added = 0;
        let existing_checkpoints = engine.storage.lock().load_checkpoints().unwrap_or_default();
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
        added
    }

    /// Track a rollout file. `existing_checkpoint` = persisted checkpoint from SQLite (if any).
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

        let (checkpoint, mode_for_next, warmup, tail_start) = match existing_checkpoint {
            Some(mut cp) => {
                let tail_start = cp.last_file_offset;
                // Restart warm-up: find last complete token_count BEFORE checkpoint.
                let warmup = scan_last_token_snapshot(&rollout.path, cp.last_file_offset);
                let mode = if warmup.is_some() {
                    BaselineMode::ContinuousEpoch
                } else {
                    BaselineMode::KnownZeroOrigin
                };
                cp.source_id = source_adapter_id.clone();
                (cp, mode, warmup, tail_start)
            }
            None => {
                // No checkpoint: find last complete token_count in the whole file.
                match scan_last_token_snapshot(&rollout.path, rollout.size) {
                    Some((_, snap)) => {
                        let cp = SourceCheckpoint {
                            source_id: source_adapter_id.clone(),
                            last_file_offset: rollout.size,
                            last_db_row_id: None,
                            last_sequence_id: None,
                            watermark_timestamp_ms: snap.source_timestamp_ms.unwrap_or(0),
                            updated_at_ms: chrono::Utc::now().timestamp_millis(),
                        };
                        let warmup = Some((0u64, snap));
                        (cp, BaselineMode::ContinuousEpoch, warmup, rollout.size)
                    }
                    None => {
                        let cp = SourceCheckpoint {
                            source_id: source_adapter_id.clone(),
                            last_file_offset: rollout.size,
                            last_db_row_id: None,
                            last_sequence_id: None,
                            watermark_timestamp_ms: 0,
                            updated_at_ms: chrono::Utc::now().timestamp_millis(),
                        };
                        (cp, BaselineMode::KnownZeroOrigin, None, rollout.size)
                    }
                }
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
            },
        );
    }

    /// Poll all tracked files once and feed the engine.
    pub fn poll(&mut self, engine: &mut EnginePipeline) -> PollStats {
        let mut stats = PollStats {
            files_tracked: self.files.len(),
            ..Default::default()
        };
        for state in self.files.values_mut() {
            Self::poll_file(state, engine, &mut stats);
        }
        stats
    }

    fn poll_file(state: &mut RolloutFileState, engine: &mut EnginePipeline, stats: &mut PollStats) {
        let file_size = std::fs::metadata(&state.path).map(|m| m.len()).unwrap_or(0);

        // §18 Truncate / unexpected replacement: source continuity cannot be proven.
        if file_size < state.tailer.offset {
            state.tailer.reset(0);
            state.warmup_snapshot = scan_last_token_snapshot(&state.path, file_size);
            state.last_token_source_ts_ms = state
                .warmup_snapshot
                .as_ref()
                .and_then(|(_, s)| s.source_timestamp_ms);
            state.last_total_output = state
                .warmup_snapshot
                .as_ref()
                .and_then(|(_, s)| s.total_usage.output_tokens);
            state.mode_for_next = BaselineMode::ContinuousEpoch;
            state.checkpoint.last_file_offset = 0;
            state.checkpoint.watermark_timestamp_ms = state.last_token_source_ts_ms.unwrap_or(0);
            state.checkpoint.updated_at_ms = chrono::Utc::now().timestamp_millis();
            state.truncated_recovered = true;
            // Sanitized warning: source hash + offsets only, no raw path.
            eprintln!(
                "codex_adapter: source truncated, safe re-baseline: {} @ size={}",
                state.source_adapter_id, file_size
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
            let _ = engine.process_sample(&sample, &codex_semantics(), BaselineMode::ReplayRestore);
        }

        // Tail new bytes (if any) and process complete records.
        if file_size > state.tailer.offset {
            let mut file = match std::fs::File::open(&state.path) {
                Ok(f) => f,
                Err(_) => return,
            };
            if file.seek(SeekFrom::Start(state.tailer.offset)).is_err() {
                return;
            }
            let mut chunk = Vec::new();
            if file.read_to_end(&mut chunk).is_err() {
                return;
            }
            let lines = state.tailer.feed(&chunk);
            for line in lines {
                Self::process_line(state, line, engine, stats);
            }
        }

        // After truncation recovery: checkpoint to safe EOF.
        if state.truncated_recovered {
            state.truncated_recovered = false;
            state.checkpoint.last_file_offset = file_size;
            state.checkpoint.watermark_timestamp_ms = state.last_token_source_ts_ms.unwrap_or(0);
            state.checkpoint.updated_at_ms = chrono::Utc::now().timestamp_millis();
            let cp = state.checkpoint.clone();
            let _ = engine
                .storage
                .lock()
                .save_canonical_transaction(&[], &[], &[], Some(&cp));
        }
    }

    fn process_line(
        state: &mut RolloutFileState,
        line: JsonlLine,
        engine: &mut EnginePipeline,
        stats: &mut PollStats,
    ) {
        match parse_rollout_line(&line.bytes) {
            Ok(Some(snapshot)) => {
                stats.token_records_consumed += 1;

                // §22: last_token_usage validation ONLY (never a second canonical source).
                if let Some(prev_total_out) = state.last_total_output {
                    let delta = snapshot
                        .total_usage
                        .output_tokens
                        .unwrap_or(0)
                        .saturating_sub(prev_total_out);
                    if Some(delta) == snapshot.last_usage.output_tokens {
                        stats.validation_matches += 1;
                    } else {
                        stats.validation_mismatches += 1;
                    }
                }
                state.last_total_output = snapshot.total_usage.output_tokens;

                // §19: interval from source timestamps only (never file mtime).
                let interval_ms =
                    match (snapshot.source_timestamp_ms, state.last_token_source_ts_ms) {
                        (Some(cur), Some(prev)) if cur > prev => Some((cur - prev) as u64),
                        _ => None,
                    };
                state.last_token_source_ts_ms = snapshot.source_timestamp_ms;

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
                state.mode_for_next = BaselineMode::ContinuousEpoch;

                match engine.process_sample_with_checkpoint(
                    &sample,
                    &codex_semantics(),
                    mode,
                    Some(&cp),
                ) {
                    Ok(ProcessOutcome::Committed(details)) => {
                        if details.delta.is_some() {
                            stats.canonical_deltas += 1;
                        } else {
                            // §23: zero-delta / dedup-suppressed -> idempotent checkpoint-only commit.
                            let _ = engine.storage.lock().save_canonical_transaction(
                                &[],
                                &[],
                                &[],
                                Some(&cp),
                            );
                        }
                    }
                    Ok(ProcessOutcome::Rejected { .. }) => {
                        // Finalized late event: checkpoint must still advance.
                        let _ = engine.storage.lock().save_canonical_transaction(
                            &[],
                            &[],
                            &[],
                            Some(&cp),
                        );
                    }
                    Ok(ProcessOutcome::Retryable { .. }) => {
                        // Leave checkpoint unchanged; retry later.
                    }
                    Err(_) => {
                        // Sanitized: source hash + offset only.
                        eprintln!(
                            "codex_adapter: engine error {} @ {}",
                            state.source_adapter_id, line.line_start_offset
                        );
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
    }
}

/// Scan `[0, end_offset)` and return the LAST complete token_count snapshot with its line start offset.
fn scan_last_token_snapshot(
    path: &std::path::Path,
    end_offset: u64,
) -> Option<(u64, CodexTokenSnapshot)> {
    let data = std::fs::read(path).ok()?;
    let limit = (end_offset as usize).min(data.len());
    let mut last: Option<(u64, CodexTokenSnapshot)> = None;
    let mut start = 0usize;
    for (i, &b) in data[..limit].iter().enumerate() {
        if b == b'\n' {
            if let Ok(Some(snap)) = parse_rollout_line(&data[start..=i]) {
                last = Some((start as u64, snap));
            }
            start = i + 1;
        }
    }
    last
}
