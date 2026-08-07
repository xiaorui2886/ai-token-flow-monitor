use serde::{Deserialize, Serialize};
use std::fmt;

/// Event kinds produced by data sources
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventKind {
    Snapshot,
    Delta,
    Final,
    Replay,
    Correction,
}

/// Category of raw source
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceType {
    AppServer,
    JSONL,
    SQLite,
    Proxy,
    Hook,
    Mock,
}

/// Baseline handling mode for initial snapshot observation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BaselineMode {
    KnownZeroOrigin,
    UnknownAttach,
    ReplayRestore,
    ContinuousEpoch,
}

/// Gap state for time discontinuities
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GapState {
    Normal,
    CatchUp,
    Stale,
    Resume,
}

/// Confidence rating for cross-source handoff alignment (Fix 5)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HandoffConfidence {
    Exact,
    Uncertain,
}

/// Accuracy level of token counts
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub enum TokenAccuracy {
    #[default]
    Unavailable,
    Estimated,
    Measured,
    Exact,
}

/// Accuracy level of timestamping & frequency
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub enum TemporalAccuracy {
    #[default]
    Unavailable,
    Estimated,
    TurnExact,
    IntervalExact,
    StreamExact,
}

/// How tokens were measured by source
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum MeasurementKind {
    #[default]
    Unknown,
    NativeCounter,
    StreamCounter,
    SnapshotDelta,
    TurnAverage,
    TokenizerEstimate,
}

/// Confidence rating for cross-source correlation
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CorrelationConfidence {
    Unknown,
    Weak,
    Strong,
    Exact,
}

/// Event identity for same-source deduplication
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq, Hash)]
pub struct SourceEventIdentity {
    pub native_event_id: Option<String>,
    pub db_row_id: Option<String>,
    pub file_path_hash: Option<String>,
    pub byte_offset: Option<u64>,
    pub native_sequence_id: Option<u64>,
    pub stable_ingestion_id: String,
}

/// Native identity fields from source
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SourceNativeIdentity {
    pub native_event_id: Option<String>,
    pub native_message_id: Option<String>,
    pub native_request_id: Option<String>,
    pub native_turn_id: Option<String>,
    pub file_path_hash: Option<String>,
    pub byte_offset: Option<u64>,
    pub db_row_id: Option<String>,
    pub native_sequence_id: Option<u64>,
}

/// Unique correlation key for a logical request/turn
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestCorrelationKey {
    pub agent_id: String,
    pub session_id: String,
    pub request_id: String,
}

/// Result of request correlation logic
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrelationResult {
    pub canonical_request_key: RequestCorrelationKey,
    pub correlation_method: String,
    pub correlation_confidence: CorrelationConfidence,
}

/// Raw usage reported by provider/adapter
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RawUsage {
    pub raw_input_tokens: Option<u64>,
    pub raw_output_tokens: Option<u64>,
    pub raw_cache_read_tokens: Option<u64>,
    pub raw_cache_write_tokens: Option<u64>,
    pub raw_reasoning_tokens: Option<u64>,
    pub raw_total_tokens: Option<u64>,
}

/// Detailed timing information (Fix 1: measurement_interval_ms)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TimingInfo {
    pub request_start_ms: Option<i64>,
    pub first_token_ms: Option<i64>,
    pub last_token_ms: Option<i64>,
    pub prefill_start_ms: Option<i64>,
    pub prefill_end_ms: Option<i64>,
    pub measurement_interval_ms: Option<u64>,
}

/// Raw sample from any adapter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawSourceSample {
    pub sample_id: String,
    pub collector_run_id: String,
    pub source_adapter_id: String,
    pub source_type: SourceType,
    pub observed_monotonic_ns: u64,
    pub wall_timestamp_ms: i64,
    pub source_timestamp_ms: Option<i64>,
    pub process_id: Option<u32>,
    pub agent_id: String,
    pub agent_name: String,
    pub session_id: String,
    pub request_id: Option<String>,
    pub turn_id: Option<String>,
    pub response_id: Option<String>,
    pub native_identity: SourceNativeIdentity,
    pub model: String,
    pub provider: String,
    pub event_kind: EventKind,
    pub is_cumulative: bool,
    pub is_final: bool,
    pub counter_reset_hint: bool,
    pub raw_usage: RawUsage,
    pub timing: TimingInfo,
    pub source_priority: u8,
    pub token_accuracy: TokenAccuracy,
    pub temporal_accuracy: TemporalAccuracy,
    pub measurement_kind: MeasurementKind,
}

/// Provider input accounting strategy
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UsageAccountingStrategy {
    OpenAiStyle,
    AnthropicStyle,
    GenericStyle,
}

/// Usage semantics for provider token definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageSemantics {
    pub reasoning_is_output_subset: bool,
    pub accounting_strategy: UsageAccountingStrategy,
    pub provider_name: String,
}

impl Default for UsageSemantics {
    fn default() -> Self {
        Self {
            reasoning_is_output_subset: true,
            accounting_strategy: UsageAccountingStrategy::GenericStyle,
            provider_name: "generic".to_string(),
        }
    }
}

/// Normalized token usage after applying provider rules
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NormalizedUsage {
    pub normalized_context_input_tokens: u64,
    pub normalized_fresh_input_tokens: u64,
    pub normalized_output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
    pub reasoning_tokens: u64,
    pub provider_reported_total: Option<u64>,
    pub normalized_total: u64,
    pub usage_semantics: UsageSemantics,
    pub normalization_version: u32,
}

/// Input throughput speed metric
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum InputThroughputMetric {
    PrefillExact(f64),
    EffectiveMeasured(f64),
    #[default]
    Unavailable,
}

/// Interval average OUT TPS metric (Fix 1: Option<f64>)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct IntervalAverageMetric {
    pub interval_tokens: u64,
    pub interval_duration_sec: Option<f64>,
    pub interval_tps: Option<f64>,
}

/// Canonical positive token delta (u64)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalTokenDelta {
    pub delta_id: String,
    pub collector_run_id: String,
    pub stable_ingestion_id: String,
    pub source_adapter_id: String,
    pub correlation_key: RequestCorrelationKey,
    pub correlation_confidence: CorrelationConfidence,
    pub observed_monotonic_ns: u64,
    pub wall_timestamp_ms: i64,
    pub agent_id: String,
    pub agent_name: String,
    pub model: String,
    pub provider: String,
    pub delta_context_input_tokens: u64,
    pub delta_fresh_input_tokens: u64,
    pub delta_output_tokens: u64,
    pub delta_cache_read: u64,
    pub delta_cache_write: u64,
    pub delta_reasoning: u64,
    pub delta_total: u64,
    pub timing: TimingInfo,
    pub token_accuracy: TokenAccuracy,
    pub temporal_accuracy: TemporalAccuracy,
    pub measurement_kind: MeasurementKind,
    pub gap_state: GapState,
    pub source_priority: u8,
    pub source_cumulative_context_input: Option<u64>,
    pub source_cumulative_output: Option<u64>,
}

/// Canonical correction event for ledger adjustments (i64)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalCorrection {
    pub correction_id: String,
    pub collector_run_id: String,
    pub correlation_key: RequestCorrelationKey,
    pub wall_timestamp_ms: i64,
    pub context_input_correction: i64,
    pub fresh_input_correction: i64,
    pub output_correction: i64,
    pub cache_read_correction: i64,
    pub cache_write_correction: i64,
    pub reasoning_correction: i64,
    pub reason: String,
    pub old_source: String,
    pub new_authoritative_source: String,
    pub old_total: u64,
    pub new_total: u64,
}

/// Canonical request ledger tracking authoritative usage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalRequestLedger {
    pub correlation_key: RequestCorrelationKey,
    pub agent_id: String,
    pub model: String,
    pub provider: String,
    pub canonical_context_input_total: u64,
    pub canonical_fresh_input_total: u64,
    pub canonical_output_total: u64,
    pub canonical_cache_read: u64,
    pub canonical_cache_write: u64,
    pub canonical_reasoning: u64,
    pub live_contributed_context_input: u64,
    pub live_contributed_fresh_input: u64,
    pub live_contributed_output: u64,
    pub live_contributed_cache_read: u64,
    pub live_contributed_cache_write: u64,
    pub live_contributed_reasoning: u64,
    pub authoritative_final_context_input: Option<u64>,
    pub authoritative_final_fresh_input: Option<u64>,
    pub authoritative_final_output: Option<u64>,
    pub authoritative_final_cache_read: Option<u64>,
    pub authoritative_final_cache_write: Option<u64>,
    pub authoritative_final_reasoning: Option<u64>,
    pub winning_source: String,
    pub active_live_source_priority: u8,
    pub active_live_token_accuracy: TokenAccuracy,
    pub active_live_temporal_accuracy: TemporalAccuracy,
    pub is_finalized: bool,
    pub normalization_version: u32,
    pub last_reconciled_at_ms: i64,
}

/// Source Checkpoint state for persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceCheckpoint {
    pub source_id: String,
    pub last_file_offset: u64,
    pub last_db_row_id: Option<String>,
    pub last_sequence_id: Option<u64>,
    pub watermark_timestamp_ms: i64,
    pub updated_at_ms: i64,
}

/// Today's aggregated token usage metrics
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TodayTokenAggregates {
    pub today_date_str: String,
    pub today_context_input: u64,
    pub today_fresh_input: u64,
    pub today_output: u64,
    pub today_cache_read: u64,
    pub today_cache_write: u64,
    pub today_reasoning: u64,
    pub today_canonical_total: u64,
}

/// Orthogonal runtime flags for an agent
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AgentRuntimeFlags {
    pub installed: bool,
    pub running: bool,
    pub request_active: bool,
    pub generating: bool,
    pub supported: bool,
    pub adapter_healthy: bool,
}

/// Combined agent status for UI & aggregation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentStatus {
    pub agent_id: String,
    pub agent_name: String,
    pub model: String,
    pub provider: String,
    pub flags: AgentRuntimeFlags,
    pub current_in_tps: Option<f64>,
    pub current_out_tps: f64,
    pub interval_avg_metric: Option<IntervalAverageMetric>,
    // Freeze Patch Fix 4: No committed aggregate provider yet -> always None (never fake 0!)
    pub today_tokens: Option<u64>,
    pub session_tokens: Option<u64>,
    pub token_accuracy: TokenAccuracy,
    pub temporal_accuracy: TemporalAccuracy,
    pub last_updated_at_ms: i64,
}

/// Adapter capabilities discovered at runtime
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AdapterCapability {
    pub supports_exact_usage: bool,
    pub supports_live_output: bool,
    pub supports_live_input: bool,
    pub supports_cache: bool,
    pub supports_reasoning: bool,
    pub supports_ttft: bool,
    pub supports_request_id: bool,
    pub supports_session_id: bool,
    pub default_token_accuracy: TokenAccuracy,
    pub default_temporal_accuracy: TemporalAccuracy,
    pub default_measurement_kind: MeasurementKind,
}

/// Engine processing errors
#[derive(Debug, Clone)]
pub enum EngineError {
    StorageError(String),
    InvalidSample(String),
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineError::StorageError(e) => write!(f, "Storage error: {}", e),
            EngineError::InvalidSample(e) => write!(f, "Invalid sample error: {}", e),
        }
    }
}

impl std::error::Error for EngineError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommittedDetails {
    pub delta: Option<CanonicalTokenDelta>,
    pub correction: Option<CanonicalCorrection>,
}

/// Outcome of processing a sample in EnginePipeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProcessOutcome {
    Committed(Box<CommittedDetails>),
    Rejected { reason: String },
    Retryable { reason: String },
}
