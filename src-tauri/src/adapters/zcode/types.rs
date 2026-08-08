/// Whitelist row struct — ONLY the frozen Task 02F §9 columns are ever selected/read.
/// `error_message`, `raw_usage_json`, `provider_metadata_json` are structurally ABSENT
/// (never selected, never deserialized into any struct).
#[derive(Debug, Clone)]
pub struct ZCodeUsageRow {
    pub id: String,
    pub logical_request_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub provider_id: String,
    pub model_id: String,
    #[allow(dead_code)] // frozen whitelist field, kept for schema contract / future diagnostics
    pub variant: Option<String>,
    pub status: String,
    pub started_at: i64,
    pub first_token_at: Option<i64>,
    pub completed_at: i64,
    pub duration_ms: Option<i64>,
    pub time_to_first_token_ms: Option<i64>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub reasoning_tokens: Option<i64>,
    pub cache_creation_input_tokens: Option<i64>,
    pub cache_read_input_tokens: Option<i64>,
    pub provider_total_tokens: Option<i64>,
    pub computed_total_tokens: Option<i64>,
    pub retry_count: Option<i64>,
    pub retryable: bool,
    pub cancelled_by_user: bool,
    pub context_exceeded: bool,
}

/// Frozen Ground Truth terminal statuses (§11): all three carry real usage and are
/// Authoritative Usage Finals (tokens consumed by the provider are real cost even on error).
pub fn is_terminal_status(status: &str) -> bool {
    matches!(status, "completed" | "error" | "cancelled")
}
