use serde::Deserialize;

/// Usage counters with Option<u64> — missing field stays None, never coerced to 0.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CodexUsage {
    pub input_tokens: Option<u64>,
    pub cached_input_tokens: Option<u64>,
    pub cache_write_input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub reasoning_output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
}

/// A parsed token_count snapshot (schema verified on this machine, 2026-08).
#[derive(Debug, Clone)]
pub struct CodexTokenSnapshot {
    pub source_timestamp_ms: Option<i64>,
    pub total_usage: CodexUsage,
    pub last_usage: CodexUsage,
    pub model_context_window: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexParseError {
    InvalidJson,
    InvalidTimestamp,
}

// ---- Minimal serde structs: ONLY fields needed. Content fields (prompt/response/code) never deserialized. ----

#[derive(Deserialize)]
struct RawLine {
    #[serde(default)]
    timestamp: Option<String>,
    #[serde(rename = "type", default)]
    event_type: Option<String>,
    #[serde(default)]
    payload: Option<RawPayload>,
}

#[derive(Deserialize)]
struct RawPayload {
    #[serde(rename = "type", default)]
    payload_type: Option<String>,
    #[serde(default)]
    info: Option<RawInfo>,
}

#[derive(Deserialize)]
struct RawInfo {
    #[serde(default)]
    total_token_usage: Option<RawUsage>,
    #[serde(default)]
    last_token_usage: Option<RawUsage>,
    #[serde(default)]
    model_context_window: Option<u64>,
}

#[derive(Deserialize, Default)]
struct RawUsage {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    cached_input_tokens: Option<u64>,
    #[serde(default)]
    cache_write_input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    reasoning_output_tokens: Option<u64>,
    #[serde(default)]
    total_tokens: Option<u64>,
}

/// Parse one complete JSONL line.
/// - Non token_count / non event_msg lines -> Ok(None)
/// - token_count lines -> Ok(Some(CodexTokenSnapshot))
/// - Malformed JSON or missing/invalid timestamp -> Err
///
/// Privacy: error values never include raw line content.
pub fn parse_rollout_line(line: &[u8]) -> Result<Option<CodexTokenSnapshot>, CodexParseError> {
    let raw: RawLine = serde_json::from_slice(line).map_err(|_| CodexParseError::InvalidJson)?;

    if raw.event_type.as_deref() != Some("event_msg") {
        return Ok(None);
    }
    let payload = match raw.payload {
        Some(p) => p,
        None => return Ok(None),
    };
    if payload.payload_type.as_deref() != Some("token_count") {
        return Ok(None);
    }
    let info = match payload.info {
        Some(i) => i,
        None => return Ok(None),
    };
    let total = match info.total_token_usage {
        Some(u) => u,
        None => return Ok(None),
    };
    let ts_str = raw
        .timestamp
        .as_deref()
        .ok_or(CodexParseError::InvalidTimestamp)?;
    let ts_ms = parse_iso_timestamp_ms(ts_str).ok_or(CodexParseError::InvalidTimestamp)?;

    let last = info.last_token_usage.unwrap_or_default();

    Ok(Some(CodexTokenSnapshot {
        source_timestamp_ms: Some(ts_ms),
        total_usage: CodexUsage {
            input_tokens: total.input_tokens,
            cached_input_tokens: total.cached_input_tokens,
            cache_write_input_tokens: total.cache_write_input_tokens,
            output_tokens: total.output_tokens,
            reasoning_output_tokens: total.reasoning_output_tokens,
            total_tokens: total.total_tokens,
        },
        last_usage: CodexUsage {
            input_tokens: last.input_tokens,
            cached_input_tokens: last.cached_input_tokens,
            cache_write_input_tokens: last.cache_write_input_tokens,
            output_tokens: last.output_tokens,
            reasoning_output_tokens: last.reasoning_output_tokens,
            total_tokens: last.total_tokens,
        },
        model_context_window: info.model_context_window,
    }))
}

/// ISO8601 with Z / offset -> epoch milliseconds.
fn parse_iso_timestamp_ms(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_millis())
}
