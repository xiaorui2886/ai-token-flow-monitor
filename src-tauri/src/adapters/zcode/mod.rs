pub mod discovery;
pub mod reader;
pub mod types;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::adapters::common::identity::stable_hash16;
use crate::core::types::{
    BaselineMode, EngineError, EventKind, MeasurementKind, ProcessOutcome, RawSourceSample,
    RawUsage, RequestCorrelationKey, SourceCheckpoint, SourceNativeIdentity, SourceType,
    TemporalAccuracy, TimingInfo, TokenAccuracy, UsageAccountingStrategy, UsageSemantics,
};
use crate::core::EnginePipeline;

use discovery::{DiscoveredZCodeDb, ZCodeDiscovery};
use reader::{fetch_max_terminal_completed_at, fetch_rows, open_read_only};
use types::{is_terminal_status, ZCodeUsageRow};

pub const ZCODE_SQLITE_PRIORITY: u8 = 50;
pub const ZCODE_AGENT_ID: &str = "zcode";
pub const ZCODE_AGENT_NAME: &str = "ZCode";
/// Accounting surface name (source client). The real upstream provider comes per-row
/// from `provider_id` (registry identity, not a credential).
pub const ZCODE_ACCOUNTING_PROVIDER: &str = "zcode";
/// Task 02F §17: overlap replay window (ZCODE_DB_LOOKBACK_MS = 10 minutes).
pub const DEFAULT_LOOKBACK_MS: i64 = 600_000;

static ADAPTER_START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

fn monotonic_now_ns() -> u64 {
    let start = *ADAPTER_START.get_or_init(Instant::now);
    start.elapsed().as_nanos() as u64
}

/// Frozen Ground Truth semantics for ZCode model_usage.
pub fn zcode_semantics() -> UsageSemantics {
    UsageSemantics {
        reasoning_is_output_subset: true,
        accounting_strategy: UsageAccountingStrategy::OpenAiStyle,
        provider_name: ZCODE_ACCOUNTING_PROVIDER.to_string(),
    }
}

/// Hash a raw ZCode logical_request_id -> `zcode_request_<16 hex>` (raw ID never enters DB/logs).
pub fn zcode_request_id(raw: &str) -> String {
    format!("zcode_request_{}", stable_hash16(raw))
}

pub fn zcode_session_id(raw: &str) -> String {
    format!("zcode_session_{}", stable_hash16(raw))
}

pub fn zcode_turn_id(raw: &str) -> String {
    format!("zcode_turn_{}", stable_hash16(raw))
}

pub fn zcode_row_id(raw: &str) -> String {
    format!("zcode_row_{}", stable_hash16(raw))
}

/// Build the canonical RawSourceSample for one terminal model_usage row (§10, §23).
pub fn build_final_sample(
    db_hash: &str,
    collector_run_id: &str,
    row: &ZCodeUsageRow,
) -> RawSourceSample {
    let session_id = zcode_session_id(&row.session_id);
    let request_id = zcode_request_id(&row.logical_request_id);
    let turn_id = zcode_turn_id(&row.turn_id);
    // §4: provider_total_tokens OR computed_total_tokens fallback.
    let raw_total = row.provider_total_tokens.or(row.computed_total_tokens);

    RawSourceSample {
        sample_id: format!("zcode_{}_{}", session_id, request_id),
        collector_run_id: collector_run_id.to_string(),
        source_adapter_id: format!("zcode_model_usage_{}", db_hash),
        source_type: SourceType::SQLite,
        observed_monotonic_ns: monotonic_now_ns(),
        wall_timestamp_ms: chrono::Utc::now().timestamp_millis(),
        source_timestamp_ms: Some(row.completed_at),
        process_id: None,
        agent_id: ZCODE_AGENT_ID.to_string(),
        agent_name: ZCODE_AGENT_NAME.to_string(),
        session_id,
        request_id: Some(request_id.clone()),
        turn_id: Some(turn_id.clone()),
        response_id: None,
        native_identity: SourceNativeIdentity {
            native_event_id: Some(request_id.clone()),
            native_request_id: Some(request_id),
            native_turn_id: Some(turn_id),
            db_row_id: Some(zcode_row_id(&row.id)),
            ..Default::default()
        },
        model: row.model_id.clone(),
        provider: row.provider_id.clone(),
        event_kind: EventKind::Final,
        is_cumulative: false,
        is_final: true,
        counter_reset_hint: false,
        raw_usage: RawUsage {
            raw_input_tokens: row.input_tokens.map(|v| v as u64),
            raw_output_tokens: row.output_tokens.map(|v| v as u64),
            raw_cache_read_tokens: row.cache_read_input_tokens.map(|v| v as u64),
            raw_cache_write_tokens: row.cache_creation_input_tokens.map(|v| v as u64),
            raw_reasoning_tokens: row.reasoning_tokens.map(|v| v as u64),
            raw_total_tokens: raw_total.map(|v| v as u64),
        },
        timing: TimingInfo {
            // §23: request_start / first_token / last_token only. prefill + interval NEVER set.
            request_start_ms: Some(row.started_at),
            first_token_ms: row.first_token_at,
            last_token_ms: Some(row.completed_at),
            ..Default::default()
        },
        source_priority: ZCODE_SQLITE_PRIORITY,
        token_accuracy: TokenAccuracy::Exact,
        temporal_accuracy: TemporalAccuracy::TurnExact,
        measurement_kind: MeasurementKind::NativeCounter,
    }
}

#[derive(Debug, Clone)]
pub struct ZCodeAdapterConfig {
    pub poll_interval: Duration,
    pub discovery_interval: Duration,
    /// Overlap replay window (§17). Rows with `completed_at >= watermark - lookback_ms`
    /// are re-read for late-insert / in-place UPDATE safety.
    pub lookback_ms: i64,
}

impl Default for ZCodeAdapterConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(500),
            discovery_interval: Duration::from_secs(3),
            lookback_ms: DEFAULT_LOOKBACK_MS,
        }
    }
}

/// Fatal adapter errors — OUR durable storage only. External source errors are NEVER fatal (§14).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZCodeAdapterError {
    CheckpointLoad,
    CheckpointPersist,
    EngineStorage,
    FatalNeedsEngineRestart,
}

#[derive(Debug, Clone, Default)]
pub struct ZCodePollStats {
    pub sources_tracked: usize,
    pub rows_consumed: u64,
    pub authoritative_finals: u64,
    /// Overlap re-read of an already-final identical request -> checkpoint-only (§19).
    pub identical_final_dedup: u64,
    /// Overlap re-read with changed values -> Core authoritative reconciliation (§20).
    pub changed_final_rewrites: u64,
    /// Unknown future status rows skipped (§11).
    pub health_unknown_status: u64,
    /// This poll's external read failed (busy/unavailable) — checkpoint NOT advanced (§14).
    pub source_unavailable: bool,
}

/// Per-DB state. The ZCode DB is a single logical source; sessions are rows, not files.
#[derive(Debug, Clone)]
pub struct ZCodeDbState {
    pub path: PathBuf,
    pub db_hash: String,
    pub source_adapter_id: String,
    pub checkpoint: SourceCheckpoint,
    /// Initial attach watermark (§15): rows with `completed_at <= history_boundary_ms` are
    /// pre-attach history and NEVER canonical. Runtime-new sources use boundary 0 (§16).
    /// Persisted in `checkpoint.last_sequence_id` so restarts keep skipping history.
    pub history_boundary_ms: i64,
    /// Existing-attach: persist the initial watermark checkpoint at the first poll.
    pub initial_attach_pending: bool,
}

/// ZCode SQLite model_usage Adapter V1.
/// Passive read only (SQLITE_OPEN_READ_ONLY). Canonical source: `model_usage` table only.
/// Rollout JSONL = VALIDATION ONLY (never canonical); logs = ignored.
///
/// Failure policy: OUR durable storage errors are FATAL (drop + recreate adapter AND engine).
/// External source read errors are `SourceUnavailable` — checkpoint untouched, retry next poll.
pub struct ZCodeAdapter {
    pub config: ZCodeAdapterConfig,
    pub discovery: ZCodeDiscovery,
    db_state: Option<ZCodeDbState>,
    last_discovery: Option<Instant>,
    /// True after the first `refresh_discovery()` completed: a DB appearing later is
    /// a Runtime New Source (§16, watermark 0) instead of an Initial Attach (§15).
    initial_discovery_complete: bool,
    fatal: Option<ZCodeAdapterError>,
}

impl ZCodeAdapter {
    pub fn new(config: ZCodeAdapterConfig) -> Self {
        Self::with_discovery(config, ZCodeDiscovery::new())
    }

    pub fn with_discovery(config: ZCodeAdapterConfig, discovery: ZCodeDiscovery) -> Self {
        Self {
            config,
            discovery,
            db_state: None,
            last_discovery: None,
            initial_discovery_complete: false,
            fatal: None,
        }
    }

    pub fn tracked_count(&self) -> usize {
        usize::from(self.db_state.is_some())
    }

    /// Discover the primary DB and attach it.
    /// - First completed refresh: Initial Attach (§15) — boundary = MAX terminal completed_at.
    /// - DB found afterwards: Runtime New Source (§16) — boundary 0, everything counts.
    /// - Checkpoint load failure stops discovery (our storage -> fatal).
    pub fn refresh_discovery(
        &mut self,
        engine: &mut EnginePipeline,
    ) -> Result<usize, ZCodeAdapterError> {
        let now = Instant::now();
        let due = match self.last_discovery {
            Some(t) => now.duration_since(t) >= self.config.discovery_interval,
            None => true,
        };
        if !due {
            return Ok(0);
        }
        self.last_discovery = Some(now);

        let checkpoints = match engine.storage.lock().load_checkpoints() {
            Ok(c) => c,
            Err(_) => {
                self.fatal = Some(ZCodeAdapterError::CheckpointLoad);
                return Err(ZCodeAdapterError::CheckpointLoad);
            }
        };

        let mut added = 0;
        if self.db_state.is_none() {
            if let Some(db) = self.discovery.discover_db() {
                let cp = checkpoints
                    .iter()
                    .find(|c| c.source_id == format!("zcode_model_usage_{}", db.db_hash))
                    .cloned();
                self.attach_db(&db, cp);
                added = usize::from(self.db_state.is_some());
            }
        }
        self.initial_discovery_complete = true;
        Ok(added)
    }

    fn attach_db(&mut self, db: &DiscoveredZCodeDb, existing_checkpoint: Option<SourceCheckpoint>) {
        let source_adapter_id = format!("zcode_model_usage_{}", db.db_hash);
        let (checkpoint, boundary, pending) = match existing_checkpoint {
            Some(mut cp) => {
                // Restart: watermark + boundary come from the persisted checkpoint.
                let boundary = cp.last_sequence_id.map(|v| v as i64).unwrap_or(0);
                cp.source_id = source_adapter_id.clone();
                (cp, boundary, false)
            }
            None if self.initial_discovery_complete => {
                // Runtime New Source (§16): watermark 0, boundary 0 — all rows count.
                let cp = SourceCheckpoint {
                    source_id: source_adapter_id.clone(),
                    last_file_offset: 0,
                    last_db_row_id: None,
                    last_sequence_id: Some(0),
                    watermark_timestamp_ms: 0,
                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                };
                (cp, 0, false)
            }
            None => {
                // Initial Attach (§15): MAX(completed_at) over terminal rows -> initial watermark.
                // External read failure at attach: do not track; retry next discovery.
                let conn = match open_read_only(&db.path) {
                    Ok(c) => c,
                    Err(_) => return,
                };
                let max_ts = match fetch_max_terminal_completed_at(&conn) {
                    Ok(v) => v.unwrap_or(0),
                    Err(_) => return,
                };
                let cp = SourceCheckpoint {
                    source_id: source_adapter_id.clone(),
                    last_file_offset: 0,
                    last_db_row_id: None,
                    // Boundary persisted in last_sequence_id (documented adapter convention):
                    // rows with completed_at <= boundary are pre-attach history, never canonical.
                    last_sequence_id: Some(max_ts as u64),
                    watermark_timestamp_ms: max_ts,
                    updated_at_ms: chrono::Utc::now().timestamp_millis(),
                };
                (cp, max_ts, true)
            }
        };

        self.db_state = Some(ZCodeDbState {
            path: db.path.clone(),
            db_hash: db.db_hash.clone(),
            source_adapter_id,
            checkpoint,
            history_boundary_ms: boundary,
            initial_attach_pending: pending,
        });
    }

    /// Poll the external DB once and feed the engine.
    pub fn poll(
        &mut self,
        engine: &mut EnginePipeline,
    ) -> Result<ZCodePollStats, ZCodeAdapterError> {
        if self.fatal.is_some() {
            return Err(ZCodeAdapterError::FatalNeedsEngineRestart);
        }
        let mut stats = ZCodePollStats {
            sources_tracked: usize::from(self.db_state.is_some()),
            ..Default::default()
        };
        if let Some(state) = self.db_state.as_mut() {
            if let Err(e) = Self::poll_db(state, self.config.lookback_ms, engine, &mut stats) {
                // §14: OUR durable storage failure -> fatal halt; adapter must be recreated.
                self.fatal = Some(e);
                return Err(e);
            }
        }
        Ok(stats)
    }

    fn poll_db(
        state: &mut ZCodeDbState,
        lookback_ms: i64,
        engine: &mut EnginePipeline,
        stats: &mut ZCodePollStats,
    ) -> Result<(), ZCodeAdapterError> {
        // §15: persist the initial watermark checkpoint at the first poll.
        if state.initial_attach_pending {
            state.initial_attach_pending = false;
            Self::persist_checkpoint(&state.checkpoint, engine)?;
        }

        // §14: external read errors -> SourceUnavailable. Checkpoint NOT advanced, retry next poll.
        let conn = match open_read_only(&state.path) {
            Ok(c) => c,
            Err(_) => {
                stats.source_unavailable = true;
                return Ok(());
            }
        };
        let lookback = state
            .checkpoint
            .watermark_timestamp_ms
            .saturating_sub(lookback_ms);
        let rows = match fetch_rows(&conn, lookback) {
            Ok(r) => r,
            Err(_) => {
                stats.source_unavailable = true;
                return Ok(());
            }
        };

        for row in rows {
            // §15: pre-attach history is never canonical.
            if row.completed_at <= state.history_boundary_ms {
                continue;
            }
            // §11: only frozen terminal statuses are Authoritative Usage Finals.
            if !is_terminal_status(&row.status) {
                stats.health_unknown_status += 1;
                // §22: do NOT advance the watermark past a potential future-final row.
                continue;
            }
            stats.rows_consumed += 1;

            let cp = SourceCheckpoint {
                source_id: state.source_adapter_id.clone(),
                last_file_offset: 0,
                last_db_row_id: Some(zcode_row_id(&row.id)),
                last_sequence_id: Some(state.history_boundary_ms as u64),
                watermark_timestamp_ms: state
                    .checkpoint
                    .watermark_timestamp_ms
                    .max(row.completed_at),
                updated_at_ms: chrono::Utc::now().timestamp_millis(),
            };

            let key = RequestCorrelationKey {
                agent_id: ZCODE_AGENT_ID.to_string(),
                session_id: zcode_session_id(&row.session_id),
                request_id: zcode_request_id(&row.logical_request_id),
            };
            let expected = expected_totals(&row);

            let (identical, changed) = {
                let ledger = engine.request_ledger.get_ledger(&key);
                match ledger {
                    Some(l) if l.is_finalized => (
                        ledger_matches(l, &expected, &row),
                        !ledger_matches(l, &expected, &row),
                    ),
                    _ => (false, false),
                }
            };

            if identical {
                // §19: unchanged duplicate final -> checkpoint-only (advance watermark).
                stats.identical_final_dedup += 1;
                Self::persist_checkpoint(&cp, engine)?;
                state.checkpoint = cp;
                continue;
            }
            if changed {
                // §20: changed row -> re-enter Core Final authoritative reconciliation. Never old+new.
                stats.changed_final_rewrites += 1;
            }

            let sample = build_final_sample(&state.db_hash, &engine.collector_run_id, &row);
            match engine.process_sample_with_checkpoint(
                &sample,
                &zcode_semantics(),
                BaselineMode::KnownZeroOrigin,
                Some(&cp),
            ) {
                Ok(ProcessOutcome::Committed(_)) => {
                    stats.authoritative_finals += 1;
                    state.checkpoint = cp;
                }
                Ok(ProcessOutcome::Rejected { .. }) => {
                    // Defensive: Core persisted checkpoint-only in this path.
                    state.checkpoint = cp;
                }
                Ok(ProcessOutcome::Retryable { .. }) => {
                    // Leave everything unchanged; retry later.
                }
                Err(e) => {
                    eprintln!(
                        "zcode_adapter: engine error {} @ row {}",
                        state.source_adapter_id,
                        zcode_row_id(&row.id)
                    );
                    return Err(map_engine_error(e));
                }
            }
        }
        Ok(())
    }

    /// Idempotent checkpoint-only durable write. Failure is fatal (§14).
    fn persist_checkpoint(
        cp: &SourceCheckpoint,
        engine: &mut EnginePipeline,
    ) -> Result<(), ZCodeAdapterError> {
        engine
            .storage
            .lock()
            .save_canonical_transaction(&[], &[], &[], Some(cp))
            .map_err(|_| ZCodeAdapterError::CheckpointPersist)
    }
}

/// Expected canonical totals for one terminal row under OpenAIStyle accounting (§4):
/// Context = input_tokens; Fresh = input - cache_read; reasoning is an output subset.
fn expected_totals(row: &ZCodeUsageRow) -> [u64; 6] {
    let input = row.input_tokens.unwrap_or(0).max(0) as u64;
    let cache_read = row.cache_read_input_tokens.unwrap_or(0).max(0) as u64;
    [
        input,                                                      // context input
        input.saturating_sub(cache_read),                           // fresh input
        row.output_tokens.unwrap_or(0).max(0) as u64,               // output
        cache_read,                                                 // cache read
        row.cache_creation_input_tokens.unwrap_or(0).max(0) as u64, // cache write
        row.reasoning_tokens.unwrap_or(0).max(0) as u64,            // reasoning
    ]
}

/// §19: identical = six canonical fields + model + provider all match.
fn ledger_matches(
    l: &crate::core::types::CanonicalRequestLedger,
    expected: &[u64; 6],
    row: &ZCodeUsageRow,
) -> bool {
    l.canonical_context_input_total == expected[0]
        && l.canonical_fresh_input_total == expected[1]
        && l.canonical_output_total == expected[2]
        && l.canonical_cache_read == expected[3]
        && l.canonical_cache_write == expected[4]
        && l.canonical_reasoning == expected[5]
        && l.model == row.model_id
        && l.provider == row.provider_id
}

/// Any engine error is fatal for this adapter instance.
fn map_engine_error(e: EngineError) -> ZCodeAdapterError {
    match e {
        EngineError::StorageError(_) | EngineError::InvalidSample(_) => {
            ZCodeAdapterError::EngineStorage
        }
    }
}
