//! Codex Rollout Adapter tests (C1-C22). All fixtures are 100% synthetic.

use ai_token_flow_monitor_lib::adapters::codex::discovery::{
    stable_path_hash, CodexDiscovery, DiscoveredRollout,
};
use ai_token_flow_monitor_lib::adapters::codex::parser::{
    parse_rollout_line, CodexParseError, CodexTokenSnapshot,
};
use ai_token_flow_monitor_lib::adapters::codex::tailer::JsonlTailer;
use ai_token_flow_monitor_lib::adapters::codex::{
    build_snapshot_sample, codex_semantics, CodexAdapter, CodexAdapterConfig, CodexAdapterError,
};
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
    std::env::temp_dir().join(format!("codex_test_{}", uuid::Uuid::new_v4()))
}

fn write_rollout(dir: &Path, name: &str, lines: &[String]) -> (PathBuf, DiscoveredRollout) {
    std::fs::create_dir_all(dir).unwrap();
    let path = dir.join(name);
    let mut content = String::new();
    for l in lines {
        content.push_str(l);
        content.push('\n');
    }
    std::fs::write(&path, content).unwrap();
    let meta = std::fs::metadata(&path).unwrap();
    let rollout = DiscoveredRollout {
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
    (path, rollout)
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

fn token_count_line(
    ts: &str,
    input: u64,
    cached: u64,
    out: u64,
    reasoning: u64,
    last_out: u64,
) -> String {
    format!(
        r#"{{"timestamp":"{}","type":"event_msg","payload":{{"type":"token_count","info":{{"total_token_usage":{{"input_tokens":{},"cached_input_tokens":{},"cache_write_input_tokens":0,"output_tokens":{},"reasoning_output_tokens":{},"total_tokens":{}}},"last_token_usage":{{"input_tokens":{},"output_tokens":{}}},"model_context_window":258400}},"rate_limits":{{}}}}}}"#,
        ts,
        input,
        cached,
        out,
        reasoning,
        input + out,
        input,
        last_out
    )
}

fn make_pipeline() -> (EnginePipeline, Arc<Mutex<StorageManager>>) {
    let storage = Arc::new(Mutex::new(StorageManager::new_in_memory().unwrap()));
    let engine = EnginePipeline::new("codex_test_run", storage.clone()).unwrap();
    (engine, storage)
}

fn make_adapter() -> CodexAdapter {
    CodexAdapter::new(CodexAdapterConfig::default())
}

fn ledger_output(engine: &EnginePipeline, session_id: &str) -> u64 {
    engine
        .request_ledger
        .get_ledger(&RequestCorrelationKey {
            agent_id: "codex".to_string(),
            session_id: session_id.to_string(),
            request_id: "session_cumulative".to_string(),
        })
        .map(|l| l.canonical_output_total)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// C1 Parser Token Count
// ---------------------------------------------------------------------------

#[test]
fn c1_parser_token_count() {
    let line = token_count_line("2026-07-29T09:36:20.281Z", 22190, 9984, 454, 379, 454);
    let snap = parse_rollout_line(line.as_bytes())
        .unwrap()
        .expect("token_count must parse");

    assert_eq!(snap.source_timestamp_ms, Some(1785317780281i64)); // 2026-07-29T09:36:20.281Z epoch ms
    assert_eq!(snap.total_usage.input_tokens, Some(22190));
    assert_eq!(snap.total_usage.cached_input_tokens, Some(9984));
    assert_eq!(snap.total_usage.cache_write_input_tokens, Some(0));
    assert_eq!(snap.total_usage.output_tokens, Some(454));
    assert_eq!(snap.total_usage.reasoning_output_tokens, Some(379));
    assert_eq!(snap.total_usage.total_tokens, Some(22644));
    assert_eq!(snap.last_usage.output_tokens, Some(454));
    assert_eq!(snap.model_context_window, Some(258400));
}

// ---------------------------------------------------------------------------
// C2 Non-token Event Ignored
// ---------------------------------------------------------------------------

#[test]
fn c2_non_token_event_ignored() {
    // Contains fake prompt content; must NEVER surface.
    let line = r#"{"timestamp":"2026-07-29T09:36:20.281Z","type":"event_msg","payload":{"type":"user_message","client_id":"fake","message":{"role":"user","content":"TOP_SECRET_PROMPT"}}}"#;
    let result = parse_rollout_line(line.as_bytes()).unwrap();
    assert!(result.is_none());
}

// ---------------------------------------------------------------------------
// C3 Known Zero vs Missing
// ---------------------------------------------------------------------------

#[test]
fn c3_known_zero_vs_missing() {
    // cache_write present = 0 -> Some(0)
    let with_zero = token_count_line("2026-07-29T09:36:20.281Z", 1000, 0, 100, 0, 100);
    let snap = parse_rollout_line(with_zero.as_bytes()).unwrap().unwrap();
    assert_eq!(snap.total_usage.cache_write_input_tokens, Some(0));

    // cache_write absent -> None
    let without = r#"{"timestamp":"2026-07-29T09:36:20.281Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1000,"output_tokens":100},"last_token_usage":{}}}}"#;
    let snap2 = parse_rollout_line(without.as_bytes()).unwrap().unwrap();
    assert_eq!(snap2.total_usage.cache_write_input_tokens, None);
    assert_eq!(snap2.total_usage.input_tokens, Some(1000));
}

// ---------------------------------------------------------------------------
// C4 OpenAI Accounting
// ---------------------------------------------------------------------------

#[test]
fn c4_openai_accounting() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let (path, rollout) = write_rollout(&dir, "rollout-c4.jsonl", &[]);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&rollout, None).unwrap();

    append_line(
        &path,
        &token_count_line("2026-07-29T09:36:20.281Z", 1000, 600, 100, 0, 100),
    );
    adapter.poll(&mut engine, &test_obs()).unwrap();

    // OpenAI style: Context=1000, Fresh=400, Output=100
    let key = RequestCorrelationKey {
        agent_id: "codex".to_string(),
        session_id: format!("codex_session_{}", rollout.file_hash),
        request_id: "session_cumulative".to_string(),
    };
    let ledger = engine.request_ledger.get_ledger(&key).unwrap();
    assert_eq!(ledger.canonical_context_input_total, 1000);
    assert_eq!(ledger.canonical_fresh_input_total, 400);
    assert_eq!(ledger.canonical_output_total, 100);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// C5 Session Cumulative Delta
// ---------------------------------------------------------------------------

#[test]
fn c5_session_cumulative_delta() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let (path, rollout) = write_rollout(&dir, "rollout-c5.jsonl", &[]);
    let session_id = format!("codex_session_{}", rollout.file_hash);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&rollout, None).unwrap();

    append_line(
        &path,
        &token_count_line("2026-07-29T09:36:20.281Z", 1000, 0, 100, 0, 100),
    );
    append_line(
        &path,
        &token_count_line("2026-07-29T09:40:00.000Z", 1000, 0, 160, 0, 60),
    );
    append_line(
        &path,
        &token_count_line("2026-07-29T09:41:00.000Z", 1000, 0, 196, 0, 36),
    );
    adapter.poll(&mut engine, &test_obs()).unwrap();

    assert_eq!(ledger_output(&engine, &session_id), 196);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// C6 Duplicate token_count Re-emit
// ---------------------------------------------------------------------------

#[test]
fn c6_duplicate_reemit() {
    let (mut engine, storage) = make_pipeline();
    let dir = temp_dir();
    let (path, rollout) = write_rollout(&dir, "rollout-c6.jsonl", &[]);
    let session_id = format!("codex_session_{}", rollout.file_hash);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&rollout, None).unwrap();

    append_line(
        &path,
        &token_count_line("2026-07-29T09:36:20.281Z", 1000, 0, 196, 0, 196),
    );
    let stats1 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(stats1.canonical_deltas, 1);

    // Duplicate re-emit: same cumulative 196
    append_line(
        &path,
        &token_count_line("2026-07-29T09:37:00.000Z", 1000, 0, 196, 0, 196),
    );
    let stats2 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(
        stats2.canonical_deltas, 0,
        "Re-emit must produce NO canonical delta"
    );

    assert_eq!(ledger_output(&engine, &session_id), 196);

    // Checkpoint must have advanced past BOTH records.
    let cps = storage.lock().load_checkpoints().unwrap();
    let cp = cps
        .iter()
        .find(|c| c.source_id == format!("codex_rollout_{}", rollout.file_hash))
        .unwrap();
    let file_size = std::fs::metadata(&path).unwrap().len();
    assert_eq!(
        cp.last_file_offset, file_size,
        "Checkpoint must advance to file end"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// C7 Existing File Attach (ReplayRestore)
// ---------------------------------------------------------------------------

#[test]
fn c7_existing_file_attach() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    // File already contains history ending at output=5000
    let (path, rollout) = write_rollout(
        &dir,
        "rollout-c7.jsonl",
        &[
            token_count_line("2026-07-29T09:00:00.000Z", 1000, 0, 4900, 0, 4900),
            token_count_line("2026-07-29T09:10:00.000Z", 1000, 0, 5000, 0, 100),
        ],
    );
    let session_id = format!("codex_session_{}", rollout.file_hash);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&rollout, None).unwrap(); // first attach, has history

    let stats1 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(
        stats1.canonical_deltas, 0,
        "Attach history must NOT produce live delta"
    );
    assert_eq!(
        ledger_output(&engine, &session_id),
        0,
        "Historical cumulative must not enter Live"
    );

    // Next snapshot 5050 -> delta 50
    append_line(
        &path,
        &token_count_line("2026-07-29T09:20:00.000Z", 1000, 0, 5050, 0, 50),
    );
    let stats2 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(stats2.canonical_deltas, 1);
    assert_eq!(
        ledger_output(&engine, &session_id),
        50,
        "Only the real delta (50) must be counted"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// C8 Restart Checkpoint Warm-up
// ---------------------------------------------------------------------------

#[test]
fn c8_restart_checkpoint_warmup() {
    let dir = temp_dir();
    let (path, rollout) = write_rollout(&dir, "rollout-c8.jsonl", &[]);
    let session_id = format!("codex_session_{}", rollout.file_hash);

    // Run 1: new file, first snapshot 500 -> KnownZeroOrigin delta 500, checkpoint persisted.
    {
        let (mut engine, storage) = make_pipeline();
        let mut adapter = make_adapter();
        adapter.add_tracked_file(&rollout, None).unwrap();
        append_line(
            &path,
            &token_count_line("2026-07-29T09:00:00.000Z", 1000, 0, 500, 0, 500),
        );
        let stats = adapter.poll(&mut engine, &test_obs()).unwrap();
        assert_eq!(stats.canonical_deltas, 1);
        assert_eq!(ledger_output(&engine, &session_id), 500);
        let _ = storage;
    }

    // Run 2 (restart): load checkpoint from storage, warm-up ReplayRestore, then +50.
    {
        let storage = Arc::new(Mutex::new(
            StorageManager::new_file(dir.join("db.sqlite")).unwrap(),
        ));
        let mut engine = EnginePipeline::new("codex_test_restart", storage.clone()).unwrap();
        let cp = {
            let _st = storage.lock();
            // Fresh file DB won't have the checkpoint; simulate persisted checkpoint at first record end.
            // (Run 1 used in-memory storage; persist equivalent checkpoint here.)
            let first_line_len = {
                let data = std::fs::read(&path).unwrap();
                data.iter()
                    .position(|&b| b == b'\n')
                    .map(|p| p as u64 + 1)
                    .unwrap()
            };
            Some(SourceCheckpoint {
                source_id: format!("codex_rollout_{}", rollout.file_hash),
                last_file_offset: first_line_len,
                last_db_row_id: None,
                last_sequence_id: None,
                watermark_timestamp_ms: 0,
                updated_at_ms: 0,
            })
        };

        let mut adapter = make_adapter();
        adapter.add_tracked_file(&rollout, cp).unwrap();
        adapter.poll(&mut engine, &test_obs()).unwrap(); // warm-up ReplayRestore -> 0 delta

        append_line(
            &path,
            &token_count_line("2026-07-29T09:10:00.000Z", 1000, 0, 550, 0, 50),
        );
        let stats = adapter.poll(&mut engine, &test_obs()).unwrap();
        assert_eq!(stats.canonical_deltas, 1);
        assert_eq!(
            ledger_output(&engine, &session_id),
            50,
            "Restart must only add 50, not 550!"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// C9 Partial Line
// ---------------------------------------------------------------------------

#[test]
fn c9_partial_line() {
    let mut tailer = JsonlTailer::new(0);
    // Half a JSON line, no newline
    let partial = br#"{"timestamp":"2026-07-29T09:36:20.281Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":10"#;
    let lines = tailer.feed(partial);
    assert!(lines.is_empty(), "Partial line must NOT be consumed");
    assert!(tailer.has_pending_partial());

    // Complete the line with remainder + newline
    let rest = br#","output_tokens":100}},"last_token_usage":{}}}}}"#;
    let mut full = rest.to_vec();
    full.push(b'\n');
    let lines = tailer.feed(&full);
    assert_eq!(
        lines.len(),
        1,
        "Completed line must be consumed exactly once"
    );
    assert_eq!(lines[0].line_start_offset, 0);
    assert!(lines[0].line_end_offset > 0);
}

// ---------------------------------------------------------------------------
// C10 Multiple Rollout Isolation
// ---------------------------------------------------------------------------

#[test]
fn c10_multiple_rollout_isolation() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();

    let (path_a, rollout_a) = write_rollout(&dir, "rollout-a.jsonl", &[]);
    let (path_b, rollout_b) = write_rollout(&dir, "rollout-b.jsonl", &[]);
    let session_a = format!("codex_session_{}", rollout_a.file_hash);
    let session_b = format!("codex_session_{}", rollout_b.file_hash);

    let mut adapter = make_adapter();
    adapter.add_tracked_file(&rollout_a, None).unwrap();
    adapter.add_tracked_file(&rollout_b, None).unwrap();

    // File A: 100 -> 150
    append_line(
        &path_a,
        &token_count_line("2026-07-29T09:00:00.000Z", 1000, 0, 100, 0, 100),
    );
    append_line(
        &path_a,
        &token_count_line("2026-07-29T09:10:00.000Z", 1000, 0, 150, 0, 50),
    );
    // File B: 200 -> 260
    append_line(
        &path_b,
        &token_count_line("2026-07-29T09:00:00.000Z", 1000, 0, 200, 0, 200),
    );
    append_line(
        &path_b,
        &token_count_line("2026-07-29T09:10:00.000Z", 1000, 0, 260, 0, 60),
    );

    adapter.poll(&mut engine, &test_obs()).unwrap();

    // Per-session ledgers must be isolated.
    assert_eq!(ledger_output(&engine, &session_a), 150);
    assert_eq!(ledger_output(&engine, &session_b), 260);

    // Global codex new output = 100 + 50 + 200 + 60 = 410 (sum of both sessions).
    let global = ledger_output(&engine, &session_a) + ledger_output(&engine, &session_b);
    assert_eq!(
        global, 410,
        "Sessions must not pollute each other's baselines"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// C11 Interval Timing
// ---------------------------------------------------------------------------

#[test]
fn c11_interval_timing() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let (path, rollout) = write_rollout(&dir, "rollout-c11.jsonl", &[]);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&rollout, None).unwrap();

    // t=1000ms output=100 ; t=5000ms output=300 -> delta 200 over 4s
    append_line(
        &path,
        &token_count_line("2026-07-29T09:00:01.000Z", 1000, 0, 100, 0, 100),
    );
    append_line(
        &path,
        &token_count_line("2026-07-29T09:00:05.000Z", 1000, 0, 300, 0, 200),
    );
    adapter.poll(&mut engine, &test_obs()).unwrap();

    let metrics =
        engine
            .tps_engine
            .calculate_agent_tps("codex", monotonic_now_ns(), "codex_test_run");
    assert_eq!(
        metrics.current_out_tps, 0.0,
        "IntervalExact must NOT enter Instant OUT TPS"
    );
    let interval = metrics
        .interval_avg_metric
        .expect("IntervalAverageMetric must exist");
    assert_eq!(interval.interval_tokens, 200);
    assert_eq!(interval.interval_duration_sec, Some(4.0));
    assert_eq!(interval.interval_tps, Some(50.0));
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// C12 No Fake IN TPS
// ---------------------------------------------------------------------------

#[test]
fn c12_no_fake_in_tps() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let (path, rollout) = write_rollout(&dir, "rollout-c12.jsonl", &[]);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&rollout, None).unwrap();

    // Input delta > 0 but no TTFT / prefill timing.
    append_line(
        &path,
        &token_count_line("2026-07-29T09:00:01.000Z", 1000, 0, 100, 0, 100),
    );
    append_line(
        &path,
        &token_count_line("2026-07-29T09:00:05.000Z", 1200, 0, 120, 0, 20),
    );
    adapter.poll(&mut engine, &test_obs()).unwrap();

    let metrics =
        engine
            .tps_engine
            .calculate_agent_tps("codex", monotonic_now_ns(), "codex_test_run");
    assert_eq!(
        metrics.current_in_tps, None,
        "IN TPS must stay None without TTFT/prefill timing"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// C13 Stable Identity Restart (no double count)
// ---------------------------------------------------------------------------

#[test]
fn c13_stable_identity_restart() {
    let dir = temp_dir();
    let db_path = dir.join("db.sqlite");
    let (path, rollout) = write_rollout(&dir, "rollout-c13.jsonl", &[]);
    let session_id = format!("codex_session_{}", rollout.file_hash);

    // Run 1: process records 100 -> 196 with file-backed storage.
    {
        let storage = Arc::new(Mutex::new(StorageManager::new_file(&db_path).unwrap()));
        let mut engine = EnginePipeline::new("run_c13_1", storage).unwrap();
        let mut adapter = make_adapter();
        adapter.add_tracked_file(&rollout, None).unwrap();
        append_line(
            &path,
            &token_count_line("2026-07-29T09:00:00.000Z", 1000, 0, 100, 0, 100),
        );
        append_line(
            &path,
            &token_count_line("2026-07-29T09:10:00.000Z", 1000, 0, 196, 0, 96),
        );
        let stats = adapter.poll(&mut engine, &test_obs()).unwrap();
        assert_eq!(stats.canonical_deltas, 2);
        assert_eq!(ledger_output(&engine, &session_id), 196);
    }

    // Run 2: same file, same storage. Adapter re-attaches WITHOUT checkpoint -> history warm-up (delta 0),
    // then replays the same records if re-read. Stable identity (file_hash + byte_offset) must prevent double count.
    {
        let storage = Arc::new(Mutex::new(StorageManager::new_file(&db_path).unwrap()));
        let mut engine = EnginePipeline::new("run_c13_2", storage.clone()).unwrap();
        let mut adapter = make_adapter();
        // Force full history re-read by attaching with a zero checkpoint (worst case replay).
        let cp = SourceCheckpoint {
            source_id: format!("codex_rollout_{}", rollout.file_hash),
            last_file_offset: 0,
            last_db_row_id: None,
            last_sequence_id: None,
            watermark_timestamp_ms: 0,
            updated_at_ms: 0,
        };
        adapter.add_tracked_file(&rollout, Some(cp)).unwrap();
        let stats = adapter.poll(&mut engine, &test_obs()).unwrap();
        // Warm-up consumes the last snapshot (ReplayRestore, no delta); no NEW records exist.
        assert_eq!(stats.canonical_deltas, 0);
        let total = storage.lock().get_total_output_tokens("codex").unwrap();
        assert_eq!(total, 196, "Stable identity replay must NOT double count!");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// C14 Truncated File Safe Recovery
// ---------------------------------------------------------------------------

#[test]
fn c14_truncated_file_safe_recovery() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let (path, rollout) = write_rollout(&dir, "rollout-c14.jsonl", &[]);
    let session_id = format!("codex_session_{}", rollout.file_hash);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&rollout, None).unwrap();

    // Grow the file: 100 -> 500 (checkpoint advances well past 1000 bytes)
    append_line(
        &path,
        &token_count_line("2026-07-29T09:00:00.000Z", 1000, 0, 100, 0, 100),
    );
    append_line(
        &path,
        &token_count_line("2026-07-29T09:10:00.000Z", 1000, 0, 500, 0, 400),
    );
    let stats1 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(stats1.canonical_deltas, 2);
    assert_eq!(ledger_output(&engine, &session_id), 500);

    // Truncate: replace file with a single short record (size << checkpoint).
    std::fs::write(
        &path,
        format!(
            "{}\n",
            token_count_line("2026-07-29T09:00:00.000Z", 1000, 0, 100, 0, 100)
        ),
    )
    .unwrap();

    let stats2 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(
        stats2.canonical_deltas, 0,
        "Truncation must NOT produce a live delta / TPS spike"
    );
    assert_eq!(
        ledger_output(&engine, &session_id),
        500,
        "Ledger must remain unchanged"
    );

    let metrics =
        engine
            .tps_engine
            .calculate_agent_tps("codex", monotonic_now_ns(), "codex_test_run");
    assert_eq!(
        metrics.current_out_tps, 0.0,
        "No TPS spike after truncation"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// C15 last_usage Validation Only
// ---------------------------------------------------------------------------

#[test]
fn c15_last_usage_validation_only() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let (path, rollout) = write_rollout(&dir, "rollout-c15.jsonl", &[]);
    let session_id = format!("codex_session_{}", rollout.file_hash);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&rollout, None).unwrap();

    // Record 1: total 0 (baseline), last 0
    append_line(
        &path,
        &token_count_line("2026-07-29T09:00:00.000Z", 1000, 0, 0, 0, 0),
    );
    // Record 2: total 50, last 50 -> delta 50 == last 50 -> validation match
    append_line(
        &path,
        &token_count_line("2026-07-29T09:10:00.000Z", 1000, 0, 50, 0, 50),
    );
    let stats = adapter.poll(&mut engine, &test_obs()).unwrap();

    assert_eq!(stats.validation_matches, 1);
    assert_eq!(stats.validation_mismatches, 0);
    assert_eq!(
        ledger_output(&engine, &session_id),
        50,
        "Canonical must count 50 exactly once"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// C16 Existing Attach Partial EOF
// ---------------------------------------------------------------------------

#[test]
fn c16_existing_attach_partial_eof() {
    let (mut engine, storage) = make_pipeline();
    let dir = temp_dir();
    // Complete history line (ends with newline) then a PARTIAL token_count line (no newline).
    let line1 = token_count_line("2026-07-29T09:00:00.000Z", 1000, 0, 5000, 0, 5000);
    let (path, rollout) = write_rollout(&dir, "rollout-c16.jsonl", std::slice::from_ref(&line1));
    let line1_len = (line1.len() + 1) as u64; // including terminating newline
    append_partial(
        &path,
        br#"{"timestamp":"2026-07-29T09:10:00.000Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1000,"output_tokens":5050"#,
    );

    let session_id = format!("codex_session_{}", rollout.file_hash);
    let source_id = format!("codex_rollout_{}", rollout.file_hash);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&rollout, None).unwrap();

    let stats1 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(
        stats1.canonical_deltas, 0,
        "Attach must not produce a live delta"
    );
    assert_eq!(
        ledger_output(&engine, &session_id),
        0,
        "History must not enter Live"
    );

    // Checkpoint must be at the safe complete offset (end of line 1), NOT at file size.
    let file_size = std::fs::metadata(&path).unwrap().len();
    let cps = storage.lock().load_checkpoints().unwrap();
    let cp = cps
        .iter()
        .find(|c| c.source_id == source_id)
        .expect("initial attach checkpoint must be persisted on first poll");
    assert_eq!(
        cp.last_file_offset, line1_len,
        "checkpoint must equal safe EOF before the partial line"
    );
    assert!(
        cp.last_file_offset < file_size,
        "file has a partial tail beyond safe EOF"
    );

    // Complete the partial line + newline: cumulative 5050 -> exactly +50.
    append_partial(
        &path,
        br#","total_tokens":6050},"last_token_usage":{"input_tokens":1000,"output_tokens":50},"model_context_window":258400},"rate_limits":{}}}"#,
    );
    append_partial(&path, b"\n");
    let stats2 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(stats2.canonical_deltas, 1);
    assert_eq!(
        ledger_output(&engine, &session_id),
        50,
        "Only the real delta (50) must be counted"
    );
    println!("EXISTING ATTACH PARTIAL EOF = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// C17 Runtime New Rollout Capture
// ---------------------------------------------------------------------------

#[test]
fn c17_runtime_new_rollout_capture() {
    let (mut engine, _) = make_pipeline();
    let root = temp_dir(); // synthetic ~/.codex root
    std::fs::create_dir_all(root.join("sessions")).unwrap();

    let config = CodexAdapterConfig {
        tail_poll_interval: Duration::from_millis(1),
        discovery_interval: Duration::ZERO,
    };
    let mut adapter = CodexAdapter::with_discovery(config, CodexDiscovery::with_root(root.clone()));

    // Initial discovery: no files exist yet -> discovery completed, 0 tracked.
    assert_eq!(adapter.refresh_discovery(&mut engine).unwrap(), 0);

    // File B appears AFTER monitor start and already contains usage before the next discovery poll.
    let (path_b, _) = write_rollout(
        &root.join("sessions"),
        "rollout-b.jsonl",
        &[
            token_count_line("2026-07-29T09:00:00.000Z", 1000, 0, 100, 0, 100),
            token_count_line("2026-07-29T09:10:00.000Z", 1000, 0, 160, 0, 60),
        ],
    );
    assert_eq!(
        adapter.refresh_discovery(&mut engine).unwrap(),
        1,
        "runtime new file must be discovered"
    );

    let session_b = format!("codex_session_{}", stable_path_hash(&path_b));
    let stats = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(stats.canonical_deltas, 2);
    assert_eq!(
        ledger_output(&engine, &session_b),
        160,
        "Runtime-new usage (160) must be captured, not skipped as history"
    );
    println!("RUNTIME NEW ROLLOUT CAPTURE = PASS");
    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// C18 Durable Restart End-to-End
// ---------------------------------------------------------------------------

#[test]
fn c18_durable_restart_end_to_end() {
    let dir = temp_dir();
    let db_path = dir.join("durable.sqlite");
    let (path, rollout) = write_rollout(&dir, "rollout-c18.jsonl", &[]);
    let session_id = format!("codex_session_{}", rollout.file_hash);
    let source_id = format!("codex_rollout_{}", rollout.file_hash);

    // Run 1: real file-backed storage. 500 committed, checkpoint persisted in SQLite.
    let line1_len;
    {
        let storage = Arc::new(Mutex::new(StorageManager::new_file(&db_path).unwrap()));
        let mut engine = EnginePipeline::new("codex_test_run1", storage.clone()).unwrap();
        let mut adapter = make_adapter();
        adapter.add_tracked_file(&rollout, None).unwrap();
        append_line(
            &path,
            &token_count_line("2026-07-29T09:00:00.000Z", 1000, 0, 500, 0, 500),
        );
        let stats = adapter.poll(&mut engine, &test_obs()).unwrap();
        assert_eq!(stats.canonical_deltas, 1);
        assert_eq!(ledger_output(&engine, &session_id), 500);

        let cps = storage.lock().load_checkpoints().unwrap();
        let cp = cps
            .iter()
            .find(|c| c.source_id == source_id)
            .expect("checkpoint must be persisted in run 1");
        let data = std::fs::read(&path).unwrap();
        line1_len = data
            .iter()
            .position(|&b| b == b'\n')
            .map(|p| p as u64 + 1)
            .unwrap();
        assert_eq!(
            cp.last_file_offset, line1_len,
            "run 1 checkpoint at safe EOF"
        );
        let ledgers = storage.lock().load_ledgers().unwrap();
        assert_eq!(
            ledgers
                .iter()
                .map(|l| l.canonical_output_total)
                .sum::<u64>(),
            500
        );
    } // Drop Adapter 1, Engine 1, Storage 1.

    // Run 2: same DB file -> engine restores 500; adapter reloads checkpoint, ReplayRestore, +50 only.
    {
        let storage = Arc::new(Mutex::new(StorageManager::new_file(&db_path).unwrap()));
        let mut engine = EnginePipeline::new("codex_test_run2", storage.clone()).unwrap();
        let cp = storage
            .lock()
            .load_checkpoints()
            .unwrap()
            .into_iter()
            .find(|c| c.source_id == source_id)
            .expect("run 2 must load the persisted checkpoint");

        let mut adapter = make_adapter();
        adapter.add_tracked_file(&rollout, Some(cp)).unwrap();
        let stats0 = adapter.poll(&mut engine, &test_obs()).unwrap();
        assert_eq!(
            stats0.canonical_deltas, 0,
            "restart warm-up must not re-count history"
        );

        append_line(
            &path,
            &token_count_line("2026-07-29T09:10:00.000Z", 1000, 0, 550, 0, 50),
        );
        let stats1 = adapter.poll(&mut engine, &test_obs()).unwrap();
        assert_eq!(stats1.canonical_deltas, 1);
        assert_eq!(ledger_output(&engine, &session_id), 550);

        let ledgers = storage.lock().load_ledgers().unwrap();
        assert_eq!(
            ledgers
                .iter()
                .map(|l| l.canonical_output_total)
                .sum::<u64>(),
            550,
            "final SQLite ledger must be 550 — never 1050 (history re-count)"
        );
    }
    println!("DURABLE RESTART END TO END = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// C19 Core Restore Error Propagation
// ---------------------------------------------------------------------------

#[test]
fn c19_core_restore_error_propagates() {
    let dir = temp_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("broken-ledgers.sqlite");
    {
        let conn = rusqlite::Connection::open(&db).unwrap();
        // Claim CURRENT schema version but create a structurally broken ledgers table.
        conn.execute_batch("PRAGMA user_version = 2;").unwrap();
        conn.execute_batch(
            "CREATE TABLE canonical_request_ledgers (correlation_key TEXT PRIMARY KEY);",
        )
        .unwrap();
    }
    let storage = Arc::new(Mutex::new(StorageManager::new_file(&db).unwrap()));
    let res = EnginePipeline::new("codex_test_broken", storage);
    match res {
        Ok(_) => panic!("EnginePipeline::new must NOT start an empty engine on broken storage"),
        Err(e) => {
            assert!(
                matches!(e, EngineError::StorageError(_)),
                "expected StorageError, got {e:?}"
            );
        }
    }
    println!("RESTORE ERROR PROPAGATION = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// C20 Initial Attach Checkpoint Persist
// ---------------------------------------------------------------------------

#[test]
fn c20_initial_attach_checkpoint_persist() {
    let (mut engine, storage) = make_pipeline();
    let dir = temp_dir();
    let line1 = token_count_line("2026-07-29T09:00:00.000Z", 1000, 0, 5000, 0, 5000);
    let (_path, rollout) = write_rollout(&dir, "rollout-c20.jsonl", std::slice::from_ref(&line1));
    let line1_len = (line1.len() + 1) as u64;
    let session_id = format!("codex_session_{}", rollout.file_hash);
    let source_id = format!("codex_rollout_{}", rollout.file_hash);

    let mut adapter = make_adapter();
    adapter.add_tracked_file(&rollout, None).unwrap();
    let stats = adapter.poll(&mut engine, &test_obs()).unwrap(); // no new token events
    assert_eq!(stats.canonical_deltas, 0);

    let cps = storage.lock().load_checkpoints().unwrap();
    let cp = cps
        .iter()
        .find(|c| c.source_id == source_id)
        .expect("initial attach checkpoint must be persisted after first poll");
    assert_eq!(
        cp.last_file_offset, line1_len,
        "checkpoint == safe EOF (file ends with newline here)"
    );
    assert_eq!(
        cp.watermark_timestamp_ms,
        1785315600000i64, // 2026-07-29T09:00:00.000Z epoch ms
        "watermark = last complete token_count source timestamp"
    );
    assert_eq!(
        ledger_output(&engine, &session_id),
        0,
        "history must not be counted as Live"
    );
    println!("INITIAL ATTACH CHECKPOINT PERSIST = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// C21 Checkpoint Load Failure -> Fatal Halt
// ---------------------------------------------------------------------------

#[test]
fn c21_checkpoint_load_failure_fatal() {
    let dir = temp_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("broken-cp.sqlite");
    {
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch("PRAGMA user_version = 2;").unwrap();
        conn.execute_batch("CREATE TABLE source_checkpoints (source_id TEXT PRIMARY KEY);")
            .unwrap();
    }
    let storage = Arc::new(Mutex::new(StorageManager::new_file(&db).unwrap()));
    let mut engine = EnginePipeline::new("codex_test_cp", storage).unwrap(); // ledgers load fine

    let mut adapter = make_adapter();
    let err = adapter.refresh_discovery(&mut engine).unwrap_err();
    assert!(
        matches!(err, CodexAdapterError::CheckpointLoad),
        "expected CheckpointLoad, got {err:?}"
    );
    assert_eq!(
        adapter.tracked_count(),
        0,
        "nothing may be tracked with a broken checkpoint load"
    );
    let err2 = adapter.poll(&mut engine, &test_obs()).unwrap_err();
    assert!(
        matches!(err2, CodexAdapterError::FatalNeedsEngineRestart),
        "adapter must halt after checkpoint load failure: {err2:?}"
    );
    println!("CHECKPOINT LOAD FAILURE FATAL = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// C22 Storage Failure -> Fatal Halt
// ---------------------------------------------------------------------------

#[test]
fn c22_storage_failure_fatal_halt() {
    let dir = temp_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("fatal.sqlite");
    let storage = Arc::new(Mutex::new(StorageManager::new_file(&db).unwrap()));
    // Inject a durable failure: every source_checkpoints INSERT aborts (simulates disk/SQLite failure).
    {
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch(
            "CREATE TRIGGER fail_checkpoint BEFORE INSERT ON source_checkpoints
             BEGIN SELECT RAISE(ABORT, 'injected durable failure'); END;",
        )
        .unwrap();
    }
    let mut engine = EnginePipeline::new("codex_test_fatal", storage.clone()).unwrap();

    let (path, rollout) = write_rollout(&dir, "rollout-c22.jsonl", &[]);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&rollout, None).unwrap();
    append_line(
        &path,
        &token_count_line("2026-07-29T09:00:00.000Z", 1000, 0, 100, 0, 100),
    );

    // First durable write (initial attach checkpoint) fails -> CheckpointPersist + fatal halt.
    let err = adapter.poll(&mut engine, &test_obs()).unwrap_err();
    assert!(
        matches!(err, CodexAdapterError::CheckpointPersist),
        "expected CheckpointPersist, got {err:?}"
    );
    // Subsequent polls return Fatal directly; no further ingest.
    let err2 = adapter.poll(&mut engine, &test_obs()).unwrap_err();
    assert!(
        matches!(err2, CodexAdapterError::FatalNeedsEngineRestart),
        "second poll must return Fatal: {err2:?}"
    );
    // Nothing durable: no canonical ledger row was written.
    let ledgers = storage.lock().load_ledgers().unwrap();
    assert!(
        ledgers.is_empty(),
        "no canonical state may survive a fatal storage failure"
    );
    println!("STORAGE FAILURE FATAL HALT = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Extra helpers used by tests
// ---------------------------------------------------------------------------

fn monotonic_now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

// Keep types referenced for clarity (no dead-code warnings).
#[allow(dead_code)]
fn _type_refs(snap: &CodexTokenSnapshot) -> Option<i64> {
    snap.source_timestamp_ms
}
#[allow(dead_code)]
fn _parse_err_ref(_e: CodexParseError) {}
#[allow(dead_code)]
fn _sample_ref(s: &RawSourceSample) -> &str {
    &s.agent_id
}
#[allow(dead_code)]
fn _build_ref() {
    let _ = build_snapshot_sample(
        "h",
        "s",
        "r",
        &CodexTokenSnapshot {
            source_timestamp_ms: None,
            total_usage: Default::default(),
            last_usage: Default::default(),
            model_context_window: None,
        },
        0,
        None,
        &test_obs(),
    );
    let _ = codex_semantics();
}

/// Deterministic synthetic observation (Task 03A §7): tests never create adapter-local clocks.
fn test_obs() -> ObservationTime {
    ObservationTime {
        monotonic_ns: 1_000_000_000,
        wall_timestamp_ms: 1_700_000_000_000,
    }
}

// ---------------------------------------------------------------------------
// C23 Initial Attach Read Failure Retry
// ---------------------------------------------------------------------------

/// Hold an exclusive Windows handle (deny all sharing) so `std::fs::read` fails while
/// `std::fs::metadata` still succeeds — a transient source read failure (03A-FIX §13).
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
fn c23_initial_attach_read_failure_retry() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let sessions = dir.join("sessions");
    std::fs::create_dir_all(&sessions).unwrap();
    let line1 = token_count_line("2026-07-29T09:00:00.000Z", 1000, 0, 5000, 0, 5000);
    let (path, rollout) =
        write_rollout(&sessions, "rollout-c23.jsonl", std::slice::from_ref(&line1));

    // First Initial Attach: file unreadable -> scan fails -> NOT attached (pending).
    let guard = make_unreadable(&path);
    let mut adapter = CodexAdapter::with_discovery(
        CodexAdapterConfig {
            tail_poll_interval: Duration::from_millis(1),
            discovery_interval: Duration::ZERO,
        },
        CodexDiscovery::with_root(dir.clone()),
    );
    adapter.refresh_discovery(&mut engine).unwrap();
    assert_eq!(
        adapter.tracked_count(),
        0,
        "must not attach / must not build a checkpoint=0 on read failure"
    );
    drop(guard);

    // Restore readability: retry MUST use Existing Attach (history 5000 -> Live 0).
    adapter.refresh_discovery(&mut engine).unwrap();
    assert_eq!(adapter.tracked_count(), 1);
    let stats1 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(stats1.canonical_deltas, 0, "history must NOT be imported");

    // Append 5050 -> only +50.
    append_line(
        &path,
        &token_count_line("2026-07-29T09:10:00.000Z", 1000, 0, 5050, 0, 50),
    );
    let stats2 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(stats2.canonical_deltas, 1);
    let session_id = format!("codex_session_{}", rollout.file_hash);
    assert_eq!(ledger_output(&engine, &session_id), 50);
    println!("CODEX INITIAL READ RETRY SAFETY = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// C24 Transient Source Failure (no truncate, no state change)
// ---------------------------------------------------------------------------

#[test]
fn c24_transient_source_failure() {
    let (mut engine, storage) = make_pipeline();
    let dir = temp_dir();
    let (path, rollout) = write_rollout(&dir, "rollout-c24.jsonl", &[]);
    let session_id = format!("codex_session_{}", rollout.file_hash);
    let mut adapter = make_adapter();
    adapter.add_tracked_file(&rollout, None).unwrap();
    append_line(
        &path,
        &token_count_line("2026-07-29T09:00:00.000Z", 1000, 0, 100, 0, 100),
    );
    adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(ledger_output(&engine, &session_id), 100);

    // Capture the durable checkpoint BEFORE the failure.
    let cp_before = {
        let cps = storage.lock().load_checkpoints().unwrap();
        cps.iter()
            .find(|c| c.source_id == format!("codex_rollout_{}", rollout.file_hash))
            .cloned()
            .expect("checkpoint exists")
    };

    // New bytes exist so the poll must OPEN the file, then the read fails.
    append_line(
        &path,
        &token_count_line("2026-07-29T09:10:00.000Z", 1000, 0, 150, 0, 50),
    );

    // Transient metadata-OK / read-fail: no truncate, no state advancement.
    let guard = make_unreadable(&path);
    let stats = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert!(
        stats.source_read_failures >= 1,
        "read failure must be counted"
    );
    assert!(
        stats.sources_available >= 1,
        "metadata-visible file counts as available"
    );
    assert_eq!(stats.canonical_deltas, 0);
    assert_eq!(ledger_output(&engine, &session_id), 100, "ledger unchanged");
    let cp_during = {
        let cps = storage.lock().load_checkpoints().unwrap();
        cps.iter()
            .find(|c| c.source_id == format!("codex_rollout_{}", rollout.file_hash))
            .cloned()
            .expect("checkpoint exists")
    };
    assert_eq!(
        cp_before.last_file_offset, cp_during.last_file_offset,
        "checkpoint offset unchanged (no truncate recovery)"
    );
    assert_eq!(
        cp_before.watermark_timestamp_ms, cp_during.watermark_timestamp_ms,
        "checkpoint watermark unchanged"
    );
    drop(guard);

    // Restore: the pending 150 line is read -> +50 only (no re-read of history).
    let stats2 = adapter.poll(&mut engine, &test_obs()).unwrap();
    assert_eq!(stats2.canonical_deltas, 1);
    assert_eq!(ledger_output(&engine, &session_id), 150);
    println!("CODEX TRANSIENT SOURCE FAILURE = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}
