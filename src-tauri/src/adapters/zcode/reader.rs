/// Read-only access to the external ZCode `model_usage` table.
///
/// - Always `SQLITE_OPEN_READ_ONLY` (`mode=ro` equivalent). NEVER `immutable=1`:
///   the external DB is WAL-active and immutable would ignore committed WAL frames.
/// - Whitelist SELECT only (§9): `error_message` / `raw_usage_json` /
///   `provider_metadata_json` are NEVER selected, even though the columns exist.
use std::path::Path;

use rusqlite::{Connection, OpenFlags};

use types::ZCodeUsageRow;

use super::types;

/// External source read failure (busy / unavailable / missing). NOT an engine-fatal condition:
/// the poll simply does not advance its checkpoint and retries next time (§14).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalReadError {
    OpenFailed,
    QueryFailed,
}

pub fn open_read_only(path: &Path) -> Result<Connection, ExternalReadError> {
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|_| ExternalReadError::OpenFailed)
}

/// Frozen whitelist SELECT — columns only, ordered, with the completed_at watermark window.
const SELECT_WHITELIST: &str = "SELECT id, logical_request_id, session_id, turn_id, provider_id, \
model_id, variant, status, started_at, first_token_at, completed_at, duration_ms, \
time_to_first_token_ms, input_tokens, output_tokens, reasoning_tokens, \
cache_creation_input_tokens, cache_read_input_tokens, provider_total_tokens, \
computed_total_tokens, retry_count, retryable, cancelled_by_user, context_exceeded \
FROM model_usage WHERE completed_at >= ?1 ORDER BY completed_at ASC, id ASC";

/// Fetch rows with `completed_at >= watermark_lookback` (Overlap Replay, §17-§18).
/// Overlap is safe: dedup by exact logical_request_id + Authoritative Final reconciliation.
pub fn fetch_rows(
    conn: &Connection,
    watermark_lookback: i64,
) -> Result<Vec<ZCodeUsageRow>, ExternalReadError> {
    let mut stmt = conn
        .prepare(SELECT_WHITELIST)
        .map_err(|_| ExternalReadError::QueryFailed)?;
    let rows = stmt
        .query_map([watermark_lookback], |row| {
            Ok(ZCodeUsageRow {
                id: row.get(0)?,
                logical_request_id: row.get(1)?,
                session_id: row.get(2)?,
                turn_id: row.get(3)?,
                provider_id: row.get(4)?,
                model_id: row.get(5)?,
                variant: row.get(6)?,
                status: row.get(7)?,
                started_at: row.get(8)?,
                first_token_at: row.get(9)?,
                completed_at: row.get(10)?,
                duration_ms: row.get(11)?,
                time_to_first_token_ms: row.get(12)?,
                input_tokens: row.get(13)?,
                output_tokens: row.get(14)?,
                reasoning_tokens: row.get(15)?,
                cache_creation_input_tokens: row.get(16)?,
                cache_read_input_tokens: row.get(17)?,
                provider_total_tokens: row.get(18)?,
                computed_total_tokens: row.get(19)?,
                retry_count: row.get(20)?,
                retryable: row.get::<_, i64>(21)? != 0,
                cancelled_by_user: row.get::<_, i64>(22)? != 0,
                context_exceeded: row.get::<_, i64>(23)? != 0,
            })
        })
        .map_err(|_| ExternalReadError::QueryFailed)?;

    let mut out = Vec::new();
    for r in rows {
        out.push(r.map_err(|_| ExternalReadError::QueryFailed)?);
    }
    Ok(out)
}

/// MAX(completed_at) over terminal rows — the Initial Attach watermark (§15).
pub fn fetch_max_terminal_completed_at(
    conn: &Connection,
) -> Result<Option<i64>, ExternalReadError> {
    conn.query_row(
        "SELECT MAX(completed_at) FROM model_usage \
         WHERE status IN ('completed','error','cancelled')",
        [],
        |row| row.get::<_, Option<i64>>(0),
    )
    .map_err(|_| ExternalReadError::QueryFailed)
}
