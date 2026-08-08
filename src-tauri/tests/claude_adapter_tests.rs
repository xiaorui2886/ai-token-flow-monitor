//! Claude Code Transcript Adapter tests (CL1-CL22). All fixtures are 100% synthetic.
//! Content fields in fixtures are FAKE and must never surface (CL1 whitelist).

use ai_token_flow_monitor_lib::adapters::claude::discovery::{
    ClaudeDiscovery, DiscoveredTranscript,
};
use ai_token_flow_monitor_lib::adapters::claude::parser::{
    parse_claude_line, ClaudeParseError, ClaudeUsageFinality, ClaudeUsageRecord,
};
use ai_token_flow_monitor_lib::adapters::claude::{
    build_final_sample, claude_message_id, claude_session_id, ClaudeAdapter, ClaudeAdapterConfig,
    ClaudeAdapterError,
};
use ai_token_flow_monitor_lib::adapters::common::identity::stable_path_hash;
use ai_token_flow_monitor_lib::core::persistence::StorageManager;
use ai_token_flow_monitor_lib::core::types::*;
use ai_token_flow_monitor_lib::core::EnginePipeline;
use ai_token_flow_monitor_lib::runtime::types::ObservationTime;
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn temp_dir() -> PathBuf {
    std::env::temp_dir().join(format!("claude_test_{}", uuid::Uuid::new_v4()))
}

fn write_transcript(dir: &Path, name: &str, lines: &[String]) -> (PathBuf, DiscoveredTranscript) {
    std::fs::create_dir_all(dir).unwrap();
    let path = dir.join(name);
    let mut content = String::new();
    for l in lines {
        content.push_str(l);
        content.push('\n');
    }
    std::fs::write(&path, content).unwrap();
    let meta = std::fs::metadata(&path).unwrap();
    let transcript = DiscoveredTranscript {
        path: path.clone(),
        file_hash: stable_path_hash(&path),
        size: meta.len(),
        modified_ms: meta
            .modified()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
    };
    (path, transcript)
}

fn append_line(path: &Path, line: &str) {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    writeln!(f, "{}", line).unwrap();
}

fn append_partial(path: &Path, bytes: &[u8]) {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    f.write_all(bytes).unwrap();
}

/// Synthetic assistant event. `cache_read`/`cache_creation` = None -> fields ABSENT (placeholder).
/// Every line carries FAKE content/thinking/tool data to exercise the parser whitelist.
#[allow(clippy::too_many_arguments)]
fn assistant_line(
    ts: &str,
    session: &str,
    msg: &str,
    uuid: &str,
    model: &str,
    input: u64,
    output: u64,
    cache_read: Option<u64>,
    cache_creation: Option<u64>,
) -> String {
    let cache = match (cache_read, cache_creation) {
        (Some(cr), Some(cc)) => {
            format!(
                r#","cache_read_input_tokens":{},"cache_creation_input_tokens":{}"#,
                cr, cc
            )
        }
        _ => String::new(),
    };
    format!(
        r#"{{"type":"assistant","timestamp":"{}","sessionId":"{}","uuid":"{}","parentUuid":"parent-{}","version":"2.1.222","isSidechain":false,"userType":"external","message":{{"id":"{}","type":"message","role":"assistant","model":"{}","content":[{{"type":"text","text":"TOP_SECRET_PROMPT"}},{{"type":"thinking","thinking":"TOP_SECRET_THINKING"}},{{"type":"tool_use","id":"tu1","name":"bash","input":{{"command":"rm -rf /"}}}}],"usage":{{"input_tokens":{},"output_tokens":{}{}}}}}}}"#,
        ts, session, uuid, msg, msg, model, input, output, cache
    )
}

fn user_line(ts: &str, session: &str, text: &str) -> String {
    format!(
        r#"{{"type":"user","timestamp":"{}","sessionId":"{}","message":{{"role":"user","content":[{{"type":"text","text":"{}"}}]}}}}"#,
        ts, session, text
    )
}

fn make_pipeline() -> (EnginePipeline, Arc<Mutex<StorageManager>>) {
    let storage = Arc::new(Mutex::new(StorageManager::new_in_memory().unwrap()));
    let engine = EnginePipeline::new("claude_test_run", storage.clone()).unwrap();
    (engine, storage)
}

fn make_adapter() -> ClaudeAdapter {
    ClaudeAdapter::new(ClaudeAdapterConfig::default())
}

fn claude_ledger(
    engine: &EnginePipeline,
    session: &str,
    msg: &str,
) -> Option<CanonicalRequestLedger> {
    engine
        .request_ledger
        .get_ledger(&RequestCorrelationKey {
            agent_id: "claude".to_string(),
            session_id: claude_session_id(session),
            request_id: claude_message_id(msg),
        })
        .cloned()
}

fn monotonic_now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

// ---------------------------------------------------------------------------
// CL1 Parser Whitelist
// ---------------------------------------------------------------------------

#[test]
fn cl1_parser_whitelist() {
    // Line carries FAKE prompt/thinking/tool content — parser must ignore it structurally.
    let line = assistant_line(
        "2026-07-29T09:00:00.000Z",
        "s1",
        "m1",
        "uuid-1",
        "model-a",
        2000,
        300,
        Some(8000),
        Some(500),
    );
    let record = parse_claude_line(line.as_bytes())
        .unwrap()
        .expect("assistant with usage must parse");

    assert_eq!(record.source_timestamp_ms, Some(1785315600000i64));
    assert_eq!(record.session_id.as_deref(), Some("s1"));
    assert_eq!(record.message_id.as_deref(), Some("m1"));
    assert_eq!(record.model.as_deref(), Some("model-a"));
    assert_eq!(record.input_tokens, Some(2000));
    assert_eq!(record.output_tokens, Some(300));
    assert_eq!(record.cache_read_input_tokens, Some(8000));
    assert_eq!(record.cache_creation_input_tokens, Some(500));
    assert_eq!(record.finality, ClaudeUsageFinality::AuthoritativeFinal);
    println!("PARSER WHITELIST = PASS");
}

// ---------------------------------------------------------------------------
// CL2 Non-assistant Ignored
// ---------------------------------------------------------------------------

#[test]
fn cl2_non_assistant_ignored() {
    let line = user_line("2026-07-29T09:00:00.000Z", "s1", "TOP_SECRET_PROMPT");
    let result = parse_claude_line(line.as_bytes()).unwrap();
    assert!(result.is_none(), "user event must be ignored");
    let _ = ClaudeParseError::InvalidJson;
    println!("NON-ASSISTANT IGNORED = PASS");
}

// ---------------------------------------------------------------------------
// CL3 Placeholder Detection
// ---------------------------------------------------------------------------

#[test]
fn cl3_placeholder_detection() {
    // cache fields MISSING -> Placeholder (even with nonzero input/output).
    let line = assistant_line(
        "2026-07-29T09:00:00.000Z",
        "s1",
        "m1",
        "uuid-1",
        "model-a",
        137000,
        1,
        None,
        None,
    );
    let record = parse_claude_line(line.as_bytes())
        .unwrap()
        .expect("placeholder must still parse");
    assert_eq!(record.finality, ClaudeUsageFinality::Placeholder);
    assert_eq!(record.input_tokens, Some(137000));
    assert_eq!(record.cache_read_input_tokens, None);
    assert_eq!(record.cache_creation_input_tokens, None);
    println!("PLACEHOLDER DETECTION = PASS");
}

// ---------------------------------------------------------------------------
// CL4 Known Zero Final
// ---------------------------------------------------------------------------

#[test]
fn cl4_known_zero_final() {
    // cache fields PRESENT with Some(0) = Known Zero, NOT missing -> AuthoritativeFinal.
    let line = assistant_line(
        "2026-07-29T09:00:00.000Z",
        "s1",
        "m1",
        "uuid-1",
        "model-a",
        0,
        0,
        Some(0),
        Some(0),
    );
    let record = parse_claude_line(line.as_bytes())
        .unwrap()
        .expect("known-zero final must parse");
    assert_eq!(record.finality, ClaudeUsageFinality::AuthoritativeFinal);
    assert_eq!(record.cache_read_input_tokens, Some(0));
    assert_eq!(record.cache_creation_input_tokens, Some(0));
    println!("KNOWN ZERO FINAL = PASS");
}

// ---------------------------------------------------------------------------
// CL5 Anthropic Accounting
// ---------------------------------------------------------------------------

#[test]
fn cl5_anthropic_accounting() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let (path, t) = write_transcript(&dir, "t5.jsonl", &[]);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&t, None).unwrap();
    append_line(
        &path,
        &assistant_line(
            "2026-07-29T09:00:00.000Z",
            "s1",
            "m1",
            "uuid-1",
            "model-a",
            2000,
            300,
            Some(8000),
            Some(500),
        ),
    );
    let stats = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(stats.authoritative_finals, 1);
    let ledger = claude_ledger(&engine, "s1", "m1").expect("ledger must exist");
    assert_eq!(ledger.canonical_fresh_input_total, 2000, "Fresh = input");
    assert_eq!(
        ledger.canonical_context_input_total, 10500,
        "Context = input + cache_read + cache_creation"
    );
    assert_eq!(ledger.canonical_output_total, 300);
    assert_eq!(ledger.canonical_cache_read, 8000);
    assert_eq!(ledger.canonical_cache_write, 500);
    assert_eq!(ledger.canonical_reasoning, 0);
    println!("ANTHROPIC ACCOUNTING = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// CL6 Reasoning Unavailable
// ---------------------------------------------------------------------------

#[test]
fn cl6_reasoning_unavailable() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let (path, t) = write_transcript(&dir, "t6.jsonl", &[]);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&t, None).unwrap();
    // Model name contains "thinking" — must NOT create reasoning tokens.
    append_line(
        &path,
        &assistant_line(
            "2026-07-29T09:00:00.000Z",
            "s1",
            "m1",
            "uuid-1",
            "claude-thinking",
            1000,
            500,
            Some(0),
            Some(0),
        ),
    );
    adapter.poll(&mut engine, &test_obs()).unwrap();
    let ledger = claude_ledger(&engine, "s1", "m1").expect("ledger must exist");
    assert_eq!(
        ledger.canonical_reasoning, 0,
        "no reasoning tokens may be fabricated"
    );
    println!("REASONING UNAVAILABLE = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// CL7 Per-message Final
// ---------------------------------------------------------------------------

#[test]
fn cl7_per_message_final() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let (path, t) = write_transcript(&dir, "t7.jsonl", &[]);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&t, None).unwrap();
    append_line(
        &path,
        &assistant_line(
            "2026-07-29T09:00:00.000Z",
            "s1",
            "mA",
            "uuid-a",
            "model-a",
            1000,
            100,
            Some(0),
            Some(0),
        ),
    );
    append_line(
        &path,
        &assistant_line(
            "2026-07-29T09:10:00.000Z",
            "s1",
            "mB",
            "uuid-b",
            "model-a",
            1000,
            60,
            Some(0),
            Some(0),
        ),
    );
    adapter.poll(&mut engine, &test_obs()).unwrap();
    let a = claude_ledger(&engine, "s1", "mA").expect("ledger A");
    let b = claude_ledger(&engine, "s1", "mB").expect("ledger B");
    assert_eq!(a.canonical_output_total, 100, "message A ledger = 100");
    assert_eq!(b.canonical_output_total, 60, "message B ledger = 60");
    assert_eq!(a.canonical_output_total + b.canonical_output_total, 160);
    println!("PER-MESSAGE FINAL = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// CL8 Identical Re-emit
// ---------------------------------------------------------------------------

#[test]
fn cl8_identical_reemit() {
    let (mut engine, storage) = make_pipeline();
    let dir = temp_dir();
    let (path, t) = write_transcript(&dir, "t8.jsonl", &[]);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&t, None).unwrap();
    let line = assistant_line(
        "2026-07-29T09:00:00.000Z",
        "s1",
        "m1",
        "uuid-1",
        "model-a",
        1000,
        100,
        Some(0),
        Some(0),
    );
    for _ in 0..6 {
        append_line(&path, &line);
    }
    let stats = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(stats.authoritative_finals, 1, "only ONE canonical final");
    assert_eq!(stats.identical_reemit_dedup, 5);
    let ledger = claude_ledger(&engine, "s1", "m1").expect("ledger");
    assert_eq!(ledger.canonical_output_total, 100, "counted exactly once");

    // Checkpoint must have advanced past the LAST re-emit line.
    let cps = storage.lock().load_checkpoints().unwrap();
    let cp = cps
        .iter()
        .find(|c| c.source_id == format!("claude_transcript_{}", t.file_hash))
        .expect("checkpoint persisted");
    let file_size = std::fs::metadata(&path).unwrap().len();
    assert_eq!(
        cp.last_file_offset, file_size,
        "checkpoint at final record end"
    );
    println!("IDENTICAL REEMIT DEDUP = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// CL9 Placeholder -> Final
// ---------------------------------------------------------------------------

#[test]
fn cl9_placeholder_to_final() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let (path, t) = write_transcript(&dir, "t9.jsonl", &[]);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&t, None).unwrap();

    // Placeholder: large provisional input, cache fields MISSING.
    append_line(
        &path,
        &assistant_line(
            "2026-07-29T09:00:00.000Z",
            "s1",
            "m1",
            "uuid-1",
            "model-a",
            137000,
            1,
            None,
            None,
        ),
    );
    // Final: authoritative values, cache fields PRESENT.
    append_line(
        &path,
        &assistant_line(
            "2026-07-29T09:10:00.000Z",
            "s1",
            "m1",
            "uuid-1",
            "model-a",
            59000,
            750,
            Some(1024),
            Some(0),
        ),
    );
    let stats = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(stats.placeholders, 1);
    assert_eq!(stats.authoritative_finals, 1);
    let ledger = claude_ledger(&engine, "s1", "m1").expect("ledger");
    assert_eq!(
        ledger.canonical_fresh_input_total, 59000,
        "ledger must contain FINAL values only"
    );
    assert_eq!(ledger.canonical_output_total, 750);
    assert_eq!(
        ledger.canonical_context_input_total,
        59000 + 1024,
        "never the placeholder 137000"
    );
    println!("PLACEHOLDER TO FINAL = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// CL10 Changed Final Rewrite
// ---------------------------------------------------------------------------

#[test]
fn cl10_changed_final_rewrite() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let (path, t) = write_transcript(&dir, "t10.jsonl", &[]);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&t, None).unwrap();
    append_line(
        &path,
        &assistant_line(
            "2026-07-29T09:00:00.000Z",
            "s1",
            "m1",
            "uuid-1",
            "model-a",
            1000,
            100,
            Some(0),
            Some(0),
        ),
    );
    let stats1 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(stats1.authoritative_finals, 1);
    assert_eq!(stats1.changed_final_rewrites, 0);

    // Same message.id, DIFFERENT final values -> Core authoritative reconciliation.
    append_line(
        &path,
        &assistant_line(
            "2026-07-29T09:10:00.000Z",
            "s1",
            "m1",
            "uuid-1",
            "model-a",
            1200,
            150,
            Some(0),
            Some(0),
        ),
    );
    let stats2 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(
        stats2.changed_final_rewrites, 1,
        "health counter must increment"
    );
    let ledger = claude_ledger(&engine, "s1", "m1").expect("ledger");
    assert_eq!(
        ledger.canonical_output_total, 150,
        "ledger exactly equals B, not A+B"
    );
    assert_eq!(ledger.canonical_fresh_input_total, 1200);
    println!("CHANGED FINAL RECONCILIATION = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// CL11 Exact Correlation
// ---------------------------------------------------------------------------

#[test]
fn cl11_exact_correlation() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let (path, t) = write_transcript(&dir, "t11.jsonl", &[]);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&t, None).unwrap();
    append_line(
        &path,
        &assistant_line(
            "2026-07-29T09:00:00.000Z",
            "s1",
            "m1",
            "uuid-1",
            "model-a",
            1000,
            100,
            Some(0),
            Some(0),
        ),
    );
    append_line(
        &path,
        &assistant_line(
            "2026-07-29T09:10:00.000Z",
            "s1",
            "m2",
            "uuid-2",
            "model-a",
            1000,
            200,
            Some(0),
            Some(0),
        ),
    );
    adapter.poll(&mut engine, &test_obs()).unwrap();
    let l1 = claude_ledger(&engine, "s1", "m1").expect("ledger m1");
    let l2 = claude_ledger(&engine, "s1", "m2").expect("ledger m2");
    assert_eq!(l1.canonical_output_total, 100);
    assert_eq!(l2.canonical_output_total, 200);
    assert_ne!(
        l1.correlation_key, l2.correlation_key,
        "two messages -> two request ledgers"
    );
    println!("EXACT CORRELATION = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// CL12 UUID Unstable
// ---------------------------------------------------------------------------

#[test]
fn cl12_uuid_unstable() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let (path, t) = write_transcript(&dir, "t12.jsonl", &[]);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&t, None).unwrap();
    // Same message.id written 3 times with DIFFERENT line uuids + same values.
    for i in 0..3 {
        append_line(
            &path,
            &assistant_line(
                "2026-07-29T09:00:00.000Z",
                "s1",
                "m1",
                &format!("uuid-{}", i),
                "model-a",
                1000,
                100,
                Some(0),
                Some(0),
            ),
        );
    }
    let stats = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(
        stats.authoritative_finals, 1,
        "uuid must NOT split one message"
    );
    assert_eq!(stats.identical_reemit_dedup, 2);
    let ledger = claude_ledger(&engine, "s1", "m1").expect("single ledger");
    assert_eq!(ledger.canonical_output_total, 100);
    println!("UUID UNSTABLE = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// CL13 Cross-file Same Message
// ---------------------------------------------------------------------------

#[test]
fn cl13_cross_file_same_message() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let (path_a, t_a) = write_transcript(&dir, "t13a.jsonl", &[]);
    let (path_b, t_b) = write_transcript(&dir, "t13b.jsonl", &[]);
    let line = assistant_line(
        "2026-07-29T09:00:00.000Z",
        "s1",
        "m1",
        "uuid-1",
        "model-a",
        1000,
        100,
        Some(0),
        Some(0),
    );
    append_line(&path_a, &line);
    append_line(&path_b, &line);

    let mut adapter = make_adapter();
    adapter.add_tracked_file(&t_a, None).unwrap();
    adapter.add_tracked_file(&t_b, None).unwrap();
    let stats = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(
        stats.authoritative_finals, 1,
        "cross-file same message counted once"
    );
    assert_eq!(stats.identical_reemit_dedup, 1);
    let ledger = claude_ledger(&engine, "s1", "m1").expect("ledger");
    assert_eq!(ledger.canonical_output_total, 100);
    println!("CROSS FILE MESSAGE DEDUP = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// CL14 Same Session Multi-file
// ---------------------------------------------------------------------------

#[test]
fn cl14_same_session_multi_file() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let (path_a, t_a) = write_transcript(&dir, "t14a.jsonl", &[]);
    let (path_b, t_b) = write_transcript(&dir, "t14b.jsonl", &[]);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&t_a, None).unwrap();
    adapter.add_tracked_file(&t_b, None).unwrap();
    append_line(
        &path_a,
        &assistant_line(
            "2026-07-29T09:00:00.000Z",
            "s1",
            "m1",
            "uuid-1",
            "model-a",
            1000,
            100,
            Some(0),
            Some(0),
        ),
    );
    append_line(
        &path_b,
        &assistant_line(
            "2026-07-29T09:10:00.000Z",
            "s1",
            "m2",
            "uuid-2",
            "model-a",
            1000,
            200,
            Some(0),
            Some(0),
        ),
    );
    adapter.poll(&mut engine, &test_obs()).unwrap();
    let l1 = claude_ledger(&engine, "s1", "m1").expect("ledger m1");
    let l2 = claude_ledger(&engine, "s1", "m2").expect("ledger m2");
    assert_eq!(
        l1.correlation_key.session_id, l2.correlation_key.session_id,
        "same logical session across files"
    );
    assert_eq!(l1.canonical_output_total + l2.canonical_output_total, 300);
    println!("MULTI FILE SESSION = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// CL15 Initial Existing Attach
// ---------------------------------------------------------------------------

#[test]
fn cl15_initial_existing_attach() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let history = assistant_line(
        "2026-07-29T09:00:00.000Z",
        "s1",
        "mOld",
        "uuid-old",
        "model-a",
        1000,
        5000,
        Some(0),
        Some(0),
    );
    let (path, t) = write_transcript(&dir, "t15.jsonl", &[history]);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&t, None).unwrap(); // first attach, file already has history

    let stats1 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(
        stats1.authoritative_finals, 0,
        "history must NOT be imported at attach"
    );
    assert!(
        claude_ledger(&engine, "s1", "mOld").is_none(),
        "no canonical state for historical usage"
    );

    // New message after attach: only +50.
    append_line(
        &path,
        &assistant_line(
            "2026-07-29T09:10:00.000Z",
            "s1",
            "mNew",
            "uuid-new",
            "model-a",
            1000,
            50,
            Some(0),
            Some(0),
        ),
    );
    let stats2 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(stats2.authoritative_finals, 1);
    let ledger = claude_ledger(&engine, "s1", "mNew").expect("new ledger");
    assert_eq!(ledger.canonical_output_total, 50, "only +50 counted");
    println!("INITIAL ATTACH = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// CL16 Runtime New File
// ---------------------------------------------------------------------------

#[test]
fn cl16_runtime_new_file() {
    let (mut engine, _) = make_pipeline();
    let root = temp_dir(); // synthetic ~/.claude/projects root
    std::fs::create_dir_all(&root).unwrap();

    let config = ClaudeAdapterConfig {
        tail_poll_interval: Duration::from_millis(1),
        discovery_interval: Duration::ZERO,
    };
    let mut adapter =
        ClaudeAdapter::with_discovery(config, ClaudeDiscovery::with_projects_root(root.clone()));

    assert_eq!(adapter.refresh_discovery(&mut engine).unwrap(), 0);

    // File appears AFTER monitor start with usage already written before discovery poll.
    let (path_b, _) = write_transcript(
        &root,
        "t16.jsonl",
        &[
            assistant_line(
                "2026-07-29T09:00:00.000Z",
                "s1",
                "m1",
                "uuid-1",
                "model-a",
                1000,
                100,
                Some(0),
                Some(0),
            ),
            assistant_line(
                "2026-07-29T09:10:00.000Z",
                "s1",
                "m2",
                "uuid-2",
                "model-a",
                1000,
                60,
                Some(0),
                Some(0),
            ),
        ],
    );
    assert_eq!(
        adapter.refresh_discovery(&mut engine).unwrap(),
        1,
        "runtime new file discovered"
    );
    let stats = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(stats.authoritative_finals, 2);
    let l1 = claude_ledger(&engine, "s1", "m1").expect("m1");
    let l2 = claude_ledger(&engine, "s1", "m2").expect("m2");
    assert_eq!(
        l1.canonical_output_total + l2.canonical_output_total,
        160,
        "runtime-new usage must be fully captured"
    );
    let _ = std::fs::metadata(&path_b).unwrap();
    println!("RUNTIME NEW FILE = PASS");
    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// CL17 Durable Restart
// ---------------------------------------------------------------------------

#[test]
fn cl17_durable_restart() {
    let dir = temp_dir();
    let db_path = dir.join("cl17.sqlite");
    let (path, t) = write_transcript(&dir, "t17.jsonl", &[]);
    let source_id = format!("claude_transcript_{}", t.file_hash);

    // Run 1: M1 output 100 -> ledger persisted.
    {
        let storage = Arc::new(Mutex::new(StorageManager::new_file(&db_path).unwrap()));
        let mut engine = EnginePipeline::new("claude_run1", storage.clone()).unwrap();
        let mut adapter = make_adapter();
        adapter.add_tracked_file(&t, None).unwrap();
        append_line(
            &path,
            &assistant_line(
                "2026-07-29T09:00:00.000Z",
                "s1",
                "m1",
                "uuid-1",
                "model-a",
                1000,
                100,
                Some(0),
                Some(0),
            ),
        );
        adapter.poll(&mut engine, &test_obs()).unwrap();
        let ledger = claude_ledger(&engine, "s1", "m1").expect("run1 ledger");
        assert_eq!(ledger.canonical_output_total, 100);
        let cps = storage.lock().load_checkpoints().unwrap();
        assert!(
            cps.iter().any(|c| c.source_id == source_id),
            "run1 checkpoint persisted"
        );
    } // Drop Adapter 1, Engine 1, Storage 1.

    // Run 2: same DB; new M2 output 60 -> total 160.
    {
        let storage = Arc::new(Mutex::new(StorageManager::new_file(&db_path).unwrap()));
        let mut engine = EnginePipeline::new("claude_run2", storage.clone()).unwrap();
        let cp = storage
            .lock()
            .load_checkpoints()
            .unwrap()
            .into_iter()
            .find(|c| c.source_id == source_id)
            .expect("run2 loads persisted checkpoint");
        let mut adapter = make_adapter();
        adapter.add_tracked_file(&t, Some(cp)).unwrap();
        adapter.poll(&mut engine, &test_obs()).unwrap(); // nothing new yet

        append_line(
            &path,
            &assistant_line(
                "2026-07-29T09:10:00.000Z",
                "s1",
                "m2",
                "uuid-2",
                "model-a",
                1000,
                60,
                Some(0),
                Some(0),
            ),
        );
        let stats = adapter.poll(&mut engine, &test_obs()).unwrap();
        assert_eq!(stats.authoritative_finals, 1);
        let l1 = claude_ledger(&engine, "s1", "m1").expect("restored m1");
        let l2 = claude_ledger(&engine, "s1", "m2").expect("new m2");
        assert_eq!(l1.canonical_output_total, 100);
        assert_eq!(l1.canonical_output_total + l2.canonical_output_total, 160);
        let ledgers = storage.lock().load_ledgers().unwrap();
        assert_eq!(
            ledgers
                .iter()
                .map(|l| l.canonical_output_total)
                .sum::<u64>(),
            160,
            "SQLite ledger total 160"
        );
    }
    println!("DURABLE RESTART = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// CL18 Duplicate After Restart
// ---------------------------------------------------------------------------

#[test]
fn cl18_duplicate_after_restart() {
    let dir = temp_dir();
    let db_path = dir.join("cl18.sqlite");
    let (path, t) = write_transcript(&dir, "t18.jsonl", &[]);
    let source_id = format!("claude_transcript_{}", t.file_hash);
    let line = assistant_line(
        "2026-07-29T09:00:00.000Z",
        "s1",
        "m1",
        "uuid-1",
        "model-a",
        1000,
        100,
        Some(0),
        Some(0),
    );

    // Run 1: M1 final 100.
    {
        let storage = Arc::new(Mutex::new(StorageManager::new_file(&db_path).unwrap()));
        let mut engine = EnginePipeline::new("claude_run1", storage.clone()).unwrap();
        let mut adapter = make_adapter();
        adapter.add_tracked_file(&t, None).unwrap();
        append_line(&path, &line);
        adapter.poll(&mut engine, &test_obs()).unwrap();
    }

    // Run 2: file appends an identical re-emit of M1 AFTER the checkpoint.
    {
        let storage = Arc::new(Mutex::new(StorageManager::new_file(&db_path).unwrap()));
        let mut engine = EnginePipeline::new("claude_run2", storage.clone()).unwrap();
        let cp = storage
            .lock()
            .load_checkpoints()
            .unwrap()
            .into_iter()
            .find(|c| c.source_id == source_id)
            .expect("checkpoint restored");
        let mut adapter = make_adapter();
        adapter.add_tracked_file(&t, Some(cp)).unwrap();
        append_line(&path, &line); // re-emit appended after checkpoint

        let stats = adapter.poll(&mut engine, &test_obs()).unwrap();
        assert_eq!(
            stats.identical_reemit_dedup, 1,
            "post-restart re-emit deduped to checkpoint-only"
        );
        assert_eq!(stats.authoritative_finals, 0);
        let ledger = claude_ledger(&engine, "s1", "m1").expect("ledger restored");
        assert_eq!(ledger.canonical_output_total, 100, "still exactly 100");
    }
    println!("DUPLICATE AFTER RESTART = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// CL19 Partial EOF
// ---------------------------------------------------------------------------

#[test]
fn cl19_partial_eof() {
    let (mut engine, storage) = make_pipeline();
    let dir = temp_dir();
    let complete = assistant_line(
        "2026-07-29T09:00:00.000Z",
        "s1",
        "m1",
        "uuid-1",
        "model-a",
        1000,
        100,
        Some(0),
        Some(0),
    );
    let (path, t) = write_transcript(&dir, "t19.jsonl", std::slice::from_ref(&complete));
    // Partial assistant line at EOF (no newline).
    append_partial(
        &path,
        br#"{"type":"assistant","timestamp":"2026-07-29T09:10:00.000Z","sessionId":"s1","message":{"id":"m2","usage":{"input_tokens":1000,"output_tokens":50"#,
    );
    let line1_len = (complete.len() + 1) as u64;

    let mut adapter = make_adapter();
    adapter.add_tracked_file(&t, None).unwrap();
    let stats1 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(
        stats1.authoritative_finals, 0,
        "attach history (m1) not imported (§18); partial (m2) not complete"
    );

    // Checkpoint must NOT cross the partial line.
    let file_size = std::fs::metadata(&path).unwrap().len();
    let cps = storage.lock().load_checkpoints().unwrap();
    let cp = cps
        .iter()
        .find(|c| c.source_id == format!("claude_transcript_{}", t.file_hash))
        .expect("checkpoint persisted");
    assert_eq!(cp.last_file_offset, line1_len, "checkpoint == safe EOF");
    assert!(cp.last_file_offset < file_size);

    // Complete the partial line + newline -> processed exactly once.
    append_partial(
        &path,
        br#","cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}"#,
    );
    append_partial(&path, b"\n");
    let stats2 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(
        stats2.authoritative_finals, 1,
        "completed partial counted once"
    );
    let stats3 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(stats3.authoritative_finals, 0, "no double processing");
    let ledger = claude_ledger(&engine, "s1", "m2").expect("m2 ledger");
    assert_eq!(ledger.canonical_output_total, 50);
    println!("PARTIAL EOF = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// CL20 TurnExact No Fake TPS
// ---------------------------------------------------------------------------

#[test]
fn cl20_turnexact_no_fake_tps() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let (path, t) = write_transcript(&dir, "t20.jsonl", &[]);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&t, None).unwrap();
    append_line(
        &path,
        &assistant_line(
            "2026-07-29T09:00:00.000Z",
            "s1",
            "m1",
            "uuid-1",
            "model-a",
            1000,
            500,
            Some(0),
            Some(0),
        ),
    );
    adapter.poll(&mut engine, &test_obs()).unwrap();

    let metrics =
        engine
            .tps_engine
            .calculate_agent_tps("claude", monotonic_now_ns(), "claude_test_run");
    assert_eq!(
        metrics.current_out_tps, 0.0,
        "TurnExact must NOT enter Instant TPS"
    );
    assert_eq!(metrics.avg_5s_out_tps, 0.0, "no Live 5s stream TPS");
    assert!(
        metrics.interval_avg_metric.is_none(),
        "no IntervalAverageMetric for TurnExact"
    );
    assert!(
        metrics.current_in_tps.is_none(),
        "no IN TPS without request timing ground truth"
    );
    println!("TURN EXACT CLASSIFICATION = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// CL21 Storage Fatal Halt
// ---------------------------------------------------------------------------

#[test]
fn cl21_storage_fatal_halt() {
    let dir = temp_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("cl21.sqlite");
    let storage = Arc::new(Mutex::new(StorageManager::new_file(&db).unwrap()));
    // Inject a durable failure: every source_checkpoints INSERT aborts.
    {
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch(
            "CREATE TRIGGER fail_checkpoint BEFORE INSERT ON source_checkpoints
             BEGIN SELECT RAISE(ABORT, 'injected durable failure'); END;",
        )
        .unwrap();
    }
    let mut engine = EnginePipeline::new("claude_fatal", storage.clone()).unwrap();

    let (path, t) = write_transcript(&dir, "t21.jsonl", &[]);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&t, None).unwrap();
    append_line(
        &path,
        &assistant_line(
            "2026-07-29T09:00:00.000Z",
            "s1",
            "m1",
            "uuid-1",
            "model-a",
            1000,
            100,
            Some(0),
            Some(0),
        ),
    );

    // First durable write (initial attach checkpoint) fails -> CheckpointPersist + fatal halt.
    let err = adapter.poll(&mut engine, &test_obs()).unwrap_err();
    assert!(
        matches!(err, ClaudeAdapterError::CheckpointPersist),
        "expected CheckpointPersist, got {err:?}"
    );
    let err2 = adapter.poll(&mut engine, &test_obs()).unwrap_err();
    assert!(
        matches!(err2, ClaudeAdapterError::FatalNeedsEngineRestart),
        "second poll must return Fatal: {err2:?}"
    );
    let ledgers = storage.lock().load_ledgers().unwrap();
    assert!(
        ledgers.is_empty(),
        "no canonical state may survive a fatal storage failure"
    );
    println!("STORAGE FATAL POLICY = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// CL22 Model Pass-through
// ---------------------------------------------------------------------------

#[test]
fn cl22_model_pass_through() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let (path, t) = write_transcript(&dir, "t22.jsonl", &[]);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&t, None).unwrap();
    append_line(
        &path,
        &assistant_line(
            "2026-07-29T09:00:00.000Z",
            "s1",
            "m1",
            "uuid-1",
            "model-a",
            1000,
            100,
            Some(0),
            Some(0),
        ),
    );
    append_line(
        &path,
        &assistant_line(
            "2026-07-29T09:10:00.000Z",
            "s1",
            "m2",
            "uuid-2",
            "model-b",
            1000,
            60,
            Some(0),
            Some(0),
        ),
    );
    adapter.poll(&mut engine, &test_obs()).unwrap();
    let l1 = claude_ledger(&engine, "s1", "m1").expect("m1");
    let l2 = claude_ledger(&engine, "s1", "m2").expect("m2");
    assert_eq!(
        l1.model, "model-a",
        "ledger model must come from the real field"
    );
    assert_eq!(l2.model, "model-b");
    println!("MODEL PASSTHROUGH = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Extra helpers used by tests
// ---------------------------------------------------------------------------

// Keep types referenced for clarity (no dead-code warnings).
#[allow(dead_code)]
fn _type_refs(r: &ClaudeUsageRecord) -> Option<i64> {
    r.source_timestamp_ms
}
#[allow(dead_code)]
fn _build_ref() {
    let _ = build_final_sample(
        "h",
        "r",
        &ClaudeUsageRecord {
            source_timestamp_ms: None,
            session_id: None,
            message_id: None,
            model: None,
            input_tokens: None,
            output_tokens: None,
            cache_read_input_tokens: None,
            cache_creation_input_tokens: None,
            finality: ClaudeUsageFinality::Placeholder,
        },
        0,
        &test_obs(),
    );
    let _ = claude_semantics_ref();
}

#[allow(dead_code)]
fn claude_semantics_ref() -> UsageSemantics {
    ai_token_flow_monitor_lib::adapters::claude::claude_semantics()
}

/// Deterministic synthetic observation (Task 03A §7): tests never create adapter-local clocks.
fn test_obs() -> ObservationTime {
    ObservationTime {
        monotonic_ns: 1_000_000_000,
        wall_timestamp_ms: 1_700_000_000_000,
    }
}

// ---------------------------------------------------------------------------
// CL23 Initial Attach Read Failure Retry
// ---------------------------------------------------------------------------

/// Hold an exclusive Windows handle (deny all sharing) so `std::fs::read` fails while
/// `std::fs::metadata` still succeeds — a transient source read failure (03A-FIX §15).
#[cfg(windows)]
fn make_unreadable(path: &Path) -> std::fs::File {
    use std::os::windows::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(path)
        .unwrap()
}

#[test]
fn cl23_initial_attach_read_failure_retry() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let history = assistant_line(
        "2026-07-29T09:00:00.000Z",
        "s1",
        "mOld",
        "uuid-old",
        "model-a",
        1000,
        5000,
        Some(0),
        Some(0),
    );
    let (path, _t) = write_transcript(&dir, "t-cl23.jsonl", std::slice::from_ref(&history));

    // First Initial Attach: file unreadable -> Safe EOF scan fails -> NOT attached (pending).
    let guard = make_unreadable(&path);
    let mut adapter = ClaudeAdapter::with_discovery(
        ClaudeAdapterConfig {
            tail_poll_interval: Duration::from_millis(1),
            discovery_interval: Duration::ZERO,
        },
        ClaudeDiscovery::with_projects_root(dir.clone()),
    );
    adapter.refresh_discovery(&mut engine).unwrap();
    assert_eq!(
        adapter.tracked_count(),
        0,
        "must not attach / must not checkpoint=0 on read failure"
    );
    drop(guard);

    // Restore readability: retry MUST use Existing Attach (history 5000 -> Live 0).
    adapter.refresh_discovery(&mut engine).unwrap();
    assert_eq!(adapter.tracked_count(), 1);
    let stats1 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(
        stats1.authoritative_finals, 0,
        "history must NOT be imported"
    );
    assert!(claude_ledger(&engine, "s1", "mOld").is_none());

    // New message output=50 -> +50 only.
    append_line(
        &path,
        &assistant_line(
            "2026-07-29T09:10:00.000Z",
            "s1",
            "mNew",
            "uuid-new",
            "model-a",
            1000,
            50,
            Some(0),
            Some(0),
        ),
    );
    let stats2 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(stats2.authoritative_finals, 1);
    let ledger = claude_ledger(&engine, "s1", "mNew").expect("new ledger");
    assert_eq!(ledger.canonical_output_total, 50);
    println!("CLAUDE INITIAL READ RETRY SAFETY = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// CL24 Transient Source Failure (no reset, no history replay)
// ---------------------------------------------------------------------------

#[test]
fn cl24_transient_source_failure() {
    let (mut engine, storage) = make_pipeline();
    let dir = temp_dir();
    let (path, t) = write_transcript(&dir, "t-cl24.jsonl", &[]);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&t, None).unwrap();
    append_line(
        &path,
        &assistant_line(
            "2026-07-29T09:00:00.000Z",
            "s1",
            "m1",
            "uuid-1",
            "model-a",
            1000,
            100,
            Some(0),
            Some(0),
        ),
    );
    adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(
        claude_ledger(&engine, "s1", "m1")
            .unwrap()
            .canonical_output_total,
        100
    );

    let cp_before = {
        let cps = storage.lock().load_checkpoints().unwrap();
        cps.iter()
            .find(|c| c.source_id == format!("claude_transcript_{}", t.file_hash))
            .cloned()
            .expect("checkpoint exists")
    };

    // New bytes exist so the poll must OPEN the file, then the read fails.
    append_line(
        &path,
        &assistant_line(
            "2026-07-29T09:10:00.000Z",
            "s1",
            "m2",
            "uuid-2",
            "model-a",
            1000,
            50,
            Some(0),
            Some(0),
        ),
    );

    // Transient metadata-OK / read-fail: no reset to 0, no history replay.
    let guard = make_unreadable(&path);
    let stats = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert!(
        stats.source_read_failures >= 1,
        "read failure must be counted"
    );
    assert!(stats.sources_available >= 1);
    assert_eq!(stats.authoritative_finals, 0);
    assert_eq!(
        claude_ledger(&engine, "s1", "m1")
            .unwrap()
            .canonical_output_total,
        100
    );
    let cp_during = {
        let cps = storage.lock().load_checkpoints().unwrap();
        cps.iter()
            .find(|c| c.source_id == format!("claude_transcript_{}", t.file_hash))
            .cloned()
            .expect("checkpoint exists")
    };
    assert_eq!(
        cp_before.last_file_offset, cp_during.last_file_offset,
        "checkpoint unchanged (no reset to 0)"
    );
    drop(guard);

    // Restore: the pending m2 line is read -> total 150, no replay.
    let stats2 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(stats2.authoritative_finals, 1);
    assert_eq!(
        claude_ledger(&engine, "s1", "m1")
            .unwrap()
            .canonical_output_total,
        100
    );
    assert_eq!(
        claude_ledger(&engine, "s1", "m2")
            .unwrap()
            .canonical_output_total,
        50
    );
    println!("CLAUDE TRANSIENT SOURCE FAILURE = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}
