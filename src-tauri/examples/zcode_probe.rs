//! ZCode SQLite model_usage Adapter V1 - Local Ground Truth Probe.
//!
//! Passive read only (SQLITE_OPEN_READ_ONLY). Never creates ZCode tasks or sends prompts.
//!
//! Usage:
//!   cargo run --example zcode_probe -- --validate-existing
//!   cargo run --example zcode_probe -- --live-passive

use std::collections::HashSet;

use ai_token_flow_monitor_lib::adapters::zcode::discovery::ZCodeDiscovery;
use ai_token_flow_monitor_lib::adapters::zcode::reader::{fetch_rows, open_read_only};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args
        .iter()
        .find(|a| a.as_str() == "--validate-existing" || a.as_str() == "--live-passive")
        .map(|s| s.as_str())
        .unwrap_or("--validate-existing");

    match mode {
        "--validate-existing" => validate_existing(),
        "--live-passive" => live_passive(),
        _ => unreachable!(),
    }
}

fn validate_existing() {
    let discovery = ZCodeDiscovery::new();
    let env = discovery.discover_environment();
    let Some(db) = discovery.discover_db() else {
        println!("db_found=false");
        return;
    };
    println!("db_found=true");

    let conn = match open_read_only(&db.path) {
        Ok(c) => c,
        Err(_) => {
            println!("db_open=FAILED (unavailable)");
            return;
        }
    };
    let journal_mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap_or_else(|_| "unknown".to_string());
    println!("journal_mode={}", journal_mode);

    // Entire table (whitelist columns only) — probe reads all rows for statistics.
    let rows = match fetch_rows(&conn, i64::MIN) {
        Ok(r) => r,
        Err(_) => {
            println!("db_query=FAILED");
            return;
        }
    };

    let n = rows.len();
    let distinct_requests = rows
        .iter()
        .map(|r| r.logical_request_id.clone())
        .collect::<HashSet<_>>()
        .len();
    let distinct_sessions = rows
        .iter()
        .map(|r| r.session_id.clone())
        .collect::<HashSet<_>>()
        .len();
    let distinct_models = rows
        .iter()
        .map(|r| r.model_id.clone())
        .collect::<HashSet<_>>()
        .len();

    let rows_with_input = rows.iter().filter(|r| r.input_tokens.is_some()).count();
    let rows_with_output = rows.iter().filter(|r| r.output_tokens.is_some()).count();
    let rows_with_cache = rows
        .iter()
        .filter(|r| r.cache_read_input_tokens.is_some() || r.cache_creation_input_tokens.is_some())
        .count();
    let rows_with_reasoning = rows.iter().filter(|r| r.reasoning_tokens.is_some()).count();
    let rows_with_ttft = rows.iter().filter(|r| r.first_token_at.is_some()).count();

    let completed = rows.iter().filter(|r| r.status == "completed").count();
    let error = rows.iter().filter(|r| r.status == "error").count();
    let cancelled = rows.iter().filter(|r| r.status == "cancelled").count();

    // Ground truth validation flags (counts only, no values/ids printed).
    let input_accounting_valid =
        rows.iter()
            .all(|r| match (r.input_tokens, r.cache_read_input_tokens) {
                (Some(i), Some(c)) => i >= c,
                _ => true,
            });
    let reasoning_subset_valid = rows
        .iter()
        .all(|r| match (r.reasoning_tokens, r.output_tokens) {
            (Some(re), Some(o)) => re <= o,
            _ => true,
        });

    let rollout_files = env.rollout_files;
    let rollout_events = count_rollout_events(&discovery);

    println!("usage_rows={}", n);
    println!("distinct_requests={}", distinct_requests);
    println!("distinct_sessions={}", distinct_sessions);
    println!("distinct_models={}", distinct_models);
    println!("rows_with_input={}", rows_with_input);
    println!("rows_with_output={}", rows_with_output);
    println!("rows_with_cache={}", rows_with_cache);
    println!("rows_with_reasoning={}", rows_with_reasoning);
    println!("rows_with_ttft={}", rows_with_ttft);
    println!("completed={}", completed);
    println!("error={}", error);
    println!("cancelled={}", cancelled);
    println!("rollout_files={}", rollout_files);
    println!("rollout_events={}", rollout_events);
    println!("input_accounting_valid={}", input_accounting_valid);
    println!("reasoning_subset_valid={}", reasoning_subset_valid);
}

/// Metadata-only count of `model_io` events in rollout files (never read as canonical).
fn count_rollout_events(discovery: &ZCodeDiscovery) -> usize {
    let dir = discovery.cli_dir().join("rollout");
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut count = 0usize;
    for e in entries.flatten() {
        let p = e.path();
        if !p.is_file() || !p.extension().map(|x| x == "jsonl").unwrap_or(false) {
            continue;
        }
        if let Ok(data) = std::fs::read(&p) {
            count += data
                .split(|&b| b == b'\n')
                .filter(|line| line.windows(8).any(|w| w == b"model_io"))
                .count();
        }
    }
    count
}

fn live_passive() {
    let discovery = ZCodeDiscovery::new();
    let Some(db) = discovery.discover_db() else {
        println!("db_found=false");
        println!("LIVE EVENT NOT OBSERVED");
        return;
    };
    let before = {
        let conn = match open_read_only(&db.path) {
            Ok(c) => c,
            Err(_) => {
                println!("db_open=FAILED");
                println!("LIVE EVENT NOT OBSERVED");
                return;
            }
        };
        terminal_snapshot(&conn)
    };
    std::thread::sleep(std::time::Duration::from_millis(1000));
    let after = match open_read_only(&db.path) {
        Ok(c) => terminal_snapshot(&c),
        Err(_) => before,
    };

    if after != before {
        println!("LIVE EVENT OBSERVED (terminal rows / max completed_at changed)");
    } else {
        println!("LIVE EVENT NOT OBSERVED");
    }
}

/// (terminal row count, max completed_at) — passive observation only.
fn terminal_snapshot(conn: &rusqlite::Connection) -> (i64, i64) {
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM model_usage WHERE status IN ('completed','error','cancelled')",
            [],
            |r| r.get(0),
        )
        .unwrap_or(-1);
    let max_ts: Option<i64> = conn
        .query_row(
            "SELECT MAX(completed_at) FROM model_usage WHERE status IN ('completed','error','cancelled')",
            [],
            |r| r.get(0),
        )
        .unwrap_or(None);
    (count, max_ts.unwrap_or(-1))
}
