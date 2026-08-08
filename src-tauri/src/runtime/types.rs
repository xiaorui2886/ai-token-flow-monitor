use crate::core::aggregator::GlobalAggregatedMetrics;

/// One shared wall-clock observation for a Runtime tick.
/// All adapters in one tick MUST use the SAME `ObservationTime` — no adapter-local clocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservationTime {
    /// Monotonic ns since the CollectorClock origin (freshness basis for IN TPS metrics).
    pub monotonic_ns: u64,
    /// Wall UTC ms at observation time.
    pub wall_timestamp_ms: i64,
}

/// Sanitized adapter error kind for health reporting.
/// NEVER carries raw paths, prompts, responses or raw IDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RuntimeErrorKind {
    #[default]
    None,
    /// External source temporarily unavailable (retry later, not fatal).
    SourceUnavailable,
    /// Monitor checkpoint durable write failed.
    CheckpointPersist,
    /// Monitor checkpoint load failed.
    CheckpointLoad,
    /// Engine durable storage failure.
    EngineStorage,
    /// Adapter halted; runtime must be recreated.
    Fatal,
}

impl std::fmt::Display for RuntimeErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            RuntimeErrorKind::None => "none",
            RuntimeErrorKind::SourceUnavailable => "source_unavailable",
            RuntimeErrorKind::CheckpointPersist => "checkpoint_persist",
            RuntimeErrorKind::CheckpointLoad => "checkpoint_load",
            RuntimeErrorKind::EngineStorage => "engine_storage",
            RuntimeErrorKind::Fatal => "fatal",
        };
        write!(f, "{}", s)
    }
}

/// Per-adapter runtime health (sanitized).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeAdapterHealth {
    pub agent_id: String,
    /// External source readable on the last poll (false = degraded, not fatal).
    pub source_available: bool,
    /// Number of tracked source files/DBs.
    pub tracked_sources: usize,
    /// Adapter halted (monitor durable failure) — whole runtime must stop.
    pub fatal: bool,
    /// Source degraded (e.g. unknown-status barrier, source unavailable).
    pub source_degraded: bool,
    /// Wall ms of the last successful (non-fatal) poll.
    pub last_successful_poll_ms: i64,
    /// Last sanitized error kind.
    pub last_error_kind: RuntimeErrorKind,
}

/// Read-only snapshot of one runtime tick.
#[derive(Debug, Clone)]
pub struct RuntimeSnapshot {
    pub collector_run_id: String,
    pub observed_monotonic_ns: u64,
    pub wall_timestamp_ms: i64,
    pub global_metrics: GlobalAggregatedMetrics,
    pub adapter_health: Vec<RuntimeAdapterHealth>,
}
