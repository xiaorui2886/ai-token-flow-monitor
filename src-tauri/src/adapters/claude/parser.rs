use serde::Deserialize;

/// Ground Truth frozen finality rule (Task 02D §9):
/// both cache fields present -> AuthoritativeFinal (Some(0) = Known Zero, NOT missing);
/// otherwise -> Placeholder / Prefill (NEVER canonical).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaudeUsageFinality {
    Placeholder,
    AuthoritativeFinal,
}

/// One parsed usage-bearing transcript record (whitelist only — see §7).
#[derive(Debug, Clone)]
pub struct ClaudeUsageRecord {
    pub source_timestamp_ms: Option<i64>,
    pub session_id: Option<String>,
    pub message_id: Option<String>,
    pub model: Option<String>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cache_read_input_tokens: Option<u64>,
    pub cache_creation_input_tokens: Option<u64>,
    pub finality: ClaudeUsageFinality,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaudeParseError {
    InvalidJson,
    InvalidTimestamp,
}

// ---- Strict whitelist: ONLY these fields are ever deserialized. Content fields
// (content/thinking/text/tool_use/tool_result/prompt/response/cwd) are structurally absent. ----

/// Frozen Ground Truth whitelist (§7): ALL these fields are intentionally deserialized —
/// they document the observed schema even if V1 logic does not read every one of them.
#[derive(Deserialize)]
#[allow(dead_code)]
struct RawLine {
    #[serde(rename = "type", default)]
    event_type: Option<String>,
    #[serde(default)]
    timestamp: Option<String>,
    #[serde(rename = "sessionId", default)]
    session_id: Option<String>,
    #[serde(default)]
    uuid: Option<String>,
    #[serde(rename = "parentUuid", default)]
    parent_uuid: Option<String>,
    #[serde(rename = "agentId", default)]
    agent_id: Option<String>,
    #[serde(default)]
    version: Option<String>,
    #[serde(rename = "isSidechain", default)]
    is_sidechain: Option<bool>,
    #[serde(default)]
    message: Option<RawMessage>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct RawMessage {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    usage: Option<RawUsage>,
}

#[derive(Deserialize, Default)]
struct RawUsage {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    cache_read_input_tokens: Option<u64>,
    #[serde(default)]
    cache_creation_input_tokens: Option<u64>,
}

/// Parse one complete JSONL line from a Claude Code transcript.
/// - Non-assistant lines / lines without usage -> Ok(None)
/// - assistant + usage -> Ok(Some(ClaudeUsageRecord))
/// - Malformed JSON or missing/invalid timestamp -> Err
///
/// Privacy: error values never include raw line content; raw JSON is never logged.
pub fn parse_claude_line(line: &[u8]) -> Result<Option<ClaudeUsageRecord>, ClaudeParseError> {
    let raw: RawLine = serde_json::from_slice(line).map_err(|_| ClaudeParseError::InvalidJson)?;

    if raw.event_type.as_deref() != Some("assistant") {
        return Ok(None);
    }
    let msg = match raw.message {
        Some(m) => m,
        None => return Ok(None),
    };
    let usage = match msg.usage {
        Some(u) => u,
        None => return Ok(None),
    };
    let ts_str = raw
        .timestamp
        .as_deref()
        .ok_or(ClaudeParseError::InvalidTimestamp)?;
    let ts_ms = parse_iso_timestamp_ms(ts_str).ok_or(ClaudeParseError::InvalidTimestamp)?;

    let finality =
        if usage.cache_read_input_tokens.is_some() && usage.cache_creation_input_tokens.is_some() {
            ClaudeUsageFinality::AuthoritativeFinal
        } else {
            ClaudeUsageFinality::Placeholder
        };

    Ok(Some(ClaudeUsageRecord {
        source_timestamp_ms: Some(ts_ms),
        session_id: raw.session_id,
        message_id: msg.id,
        model: msg.model,
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cache_read_input_tokens: usage.cache_read_input_tokens,
        cache_creation_input_tokens: usage.cache_creation_input_tokens,
        finality,
    }))
}

/// ISO8601 with Z / offset -> epoch milliseconds.
fn parse_iso_timestamp_ms(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_millis())
}
