//! First Batch Runtime Integration tests (RT1-RT7). All sources are 100% synthetic.

use ai_token_flow_monitor_lib::adapters::claude::{claude_message_id, claude_session_id};
use ai_token_flow_monitor_lib::adapters::codex::CODEX_LOGICAL_REQUEST_ID;
use ai_token_flow_monitor_lib::adapters::common::identity::stable_path_hash;
use ai_token_flow_monitor_lib::adapters::zcode::{zcode_request_id, zcode_session_id};
use ai_token_flow_monitor_lib::core::persistence::StorageManager;
use ai_token_flow_monitor_lib::core::types::RequestCorrelationKey;
use ai_token_flow_monitor_lib::runtime::clock::CollectorClock;
use ai_token_flow_monitor_lib::runtime::types::ObservationTime;
use ai_token_flow_monitor_lib::runtime::{CollectorRuntime, RuntimeConfig, RuntimeError};
use std::path::{Path, PathBuf};
use std::time::Duration;

fn temp_dir() -> PathBuf {
    std::env::temp_dir().join(format!("rt_test_{}", uuid::Uuid::new_v4()))
}

fn test_obs() -> ObservationTime {
    ObservationTime {
        monotonic_ns: 1_000_000_000,
        wall_timestamp_ms: 1_700_000_000_000,
    }
}

fn test_config(monitor_db: Option<PathBuf>) -> RuntimeConfig {
    RuntimeConfig {
        monitor_db_path: monitor_db,
        codex: ai_token_flow_monitor_lib::adapters::codex::CodexAdapterConfig {
            tail_poll_interval: Duration::from_millis(1),
            discovery_interval: Duration::ZERO,
        },
        claude: ai_token_flow_monitor_lib::adapters::claude::ClaudeAdapterConfig {
            tail_poll_interval: Duration::from_millis(1),
            discovery_interval: Duration::ZERO,
        },
        zcode: ai_token_flow_monitor_lib::adapters::zcode::ZCodeAdapterConfig {
            poll_interval: Duration::from_millis(1),
            discovery_interval: Duration::ZERO,
            lookback_ms: 600_000,
        },
    }
}

/// Synthetic roots: codex = <root>/sessions, claude = <root>, zcode = <root>/db/db.sqlite.
struct SyntheticRoots {
    #[allow(dead_code)] // root dir kept alive by its children; field documents the layout
    dir: PathBuf,
    codex_sessions: PathBuf,
    claude: PathBuf,
    zcode_db: PathBuf,
}

fn make_roots(dir: &Path) -> SyntheticRoots {
    let codex_sessions = dir.join("codex").join("sessions");
    let claude = dir.join("claude");
    let zcode_db = dir.join("zcode").join("db").join("db.sqlite");
    std::fs::create_dir_all(&codex_sessions).unwrap();
    std::fs::create_dir_all(&claude).unwrap();
    std::fs::create_dir_all(zcode_db.parent().unwrap()).unwrap();
    SyntheticRoots {
        dir: dir.to_path_buf(),
        codex_sessions,
        claude,
        zcode_db,
    }
}

fn write_line(path: &Path, line: &str) {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(f, "{}", line).unwrap();
}

/// Synthetic codex token_count line (cumulative output = `out`).
fn codex_line(ts: &str, input: u64, out: u64) -> String {
    format!(
        r#"{{"timestamp":"{}","type":"event_msg","payload":{{"type":"token_count","info":{{"total_token_usage":{{"input_tokens":{},"cached_input_tokens":0,"cache_write_input_tokens":0,"output_tokens":{},"reasoning_output_tokens":0,"total_tokens":{}}},"last_token_usage":{{"input_tokens":{},"output_tokens":{}}},"model_context_window":258400}},"rate_limits":{{}}}}}}"#,
        ts,
        input,
        out,
        input + out,
        input,
        out
    )
}

/// Synthetic claude assistant final line.
fn claude_line(ts: &str, session: &str, msg: &str, model: &str, input: u64, output: u64) -> String {
    format!(
        r#"{{"type":"assistant","timestamp":"{}","sessionId":"{}","uuid":"u-{}","parentUuid":"p-{}","version":"2.1.222","isSidechain":false,"userType":"external","message":{{"id":"{}","type":"message","role":"assistant","model":"{}","usage":{{"input_tokens":{},"output_tokens":{},"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}}}"#,
        ts, session, msg, msg, msg, model, input, output
    )
}

const ZCODE_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS model_usage (
    id TEXT PRIMARY KEY,
    logical_request_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    variant TEXT,
    status TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    first_token_at INTEGER,
    completed_at INTEGER NOT NULL,
    duration_ms INTEGER,
    time_to_first_token_ms INTEGER,
    input_tokens INTEGER,
    output_tokens INTEGER,
    reasoning_tokens INTEGER,
    cache_creation_input_tokens INTEGER,
    cache_read_input_tokens INTEGER,
    provider_total_tokens INTEGER,
    computed_total_tokens INTEGER,
    retry_count INTEGER NOT NULL DEFAULT 0,
    retryable INTEGER NOT NULL DEFAULT 0,
    cancelled_by_user INTEGER NOT NULL DEFAULT 0,
    context_exceeded INTEGER NOT NULL DEFAULT 0
)";

#[allow(clippy::too_many_arguments)]
fn zcode_row(
    conn: &rusqlite::Connection,
    id: &str,
    req: &str,
    session: &str,
    status: &str,
    started_at: i64,
    first_token_at: Option<i64>,
    completed_at: i64,
    input: i64,
    output: i64,
) {
    conn.execute(
        "INSERT INTO model_usage (id, logical_request_id, session_id, turn_id, provider_id, model_id, status, started_at, first_token_at, completed_at, input_tokens, output_tokens, reasoning_tokens, cache_creation_input_tokens, cache_read_input_tokens, provider_total_tokens, computed_total_tokens)
         VALUES (?1,?2,?3,'T1','P1','model-a',?4,?5,?6,?7,?8,?9,0,0,0,?10,?10)",
        rusqlite::params![
            id, req, session, status, started_at, first_token_at, completed_at, input, output,
            input + output
        ],
    )
    .unwrap();
}

fn codex_ledger(runtime: &CollectorRuntime, file_hash: &str) -> u64 {
    runtime
        .engine
        .request_ledger
        .get_ledger(&RequestCorrelationKey {
            agent_id: "codex".to_string(),
            session_id: format!("codex_session_{}", file_hash),
            request_id: CODEX_LOGICAL_REQUEST_ID.to_string(),
        })
        .map(|l| l.canonical_output_total)
        .unwrap_or(0)
}

fn claude_ledger(runtime: &CollectorRuntime, session: &str, msg: &str) -> u64 {
    runtime
        .engine
        .request_ledger
        .get_ledger(&RequestCorrelationKey {
            agent_id: "claude".to_string(),
            session_id: claude_session_id(session),
            request_id: claude_message_id(msg),
        })
        .map(|l| l.canonical_output_total)
        .unwrap_or(0)
}

fn zcode_ledger(runtime: &CollectorRuntime, session: &str, req: &str) -> u64 {
    runtime
        .engine
        .request_ledger
        .get_ledger(&RequestCorrelationKey {
            agent_id: "zcode".to_string(),
            session_id: zcode_session_id(session),
            request_id: zcode_request_id(req),
        })
        .map(|l| l.canonical_output_total)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// RT1 Shared Collector Clock
// ---------------------------------------------------------------------------

#[test]
fn rt1_shared_collector_clock() {
    let dir = temp_dir();
    let roots = make_roots(&dir);
    let mut runtime = CollectorRuntime::with_roots(
        test_config(None),
        Some(roots.codex_sessions.parent().unwrap().to_path_buf()),
        Some(roots.claude.clone()),
        Some(
            roots
                .zcode_db
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .to_path_buf(),
        ),
    )
    .unwrap();

    // EnginePipeline shares the runtime's run id.
    assert_eq!(
        runtime.engine.collector_run_id,
        runtime.clock.run_id(),
        "ONE collector_run_id for engine + adapters"
    );
    assert!(runtime.clock.run_id().starts_with("run_"));
    assert!(runtime.clock.started_wall_ms() > 0);

    // Deterministic observation flows through the tick unchanged.
    let obs = ObservationTime {
        monotonic_ns: 123_456_789,
        wall_timestamp_ms: 999_000,
    };
    let snap = runtime.tick_with_observation(obs).unwrap();
    assert_eq!(snap.collector_run_id, runtime.clock.run_id());
    assert_eq!(snap.observed_monotonic_ns, 123_456_789);
    assert_eq!(snap.wall_timestamp_ms, 999_000);

    // Clock observe() is monotonic (real Instant origin, no adapter-local axis).
    let c = CollectorClock::with_run_id("run_test".to_string());
    let o1 = c.observe();
    let o2 = c.observe();
    assert!(o2.monotonic_ns >= o1.monotonic_ns);
    assert_eq!(c.run_id(), "run_test");
    println!("SHARED COLLECTOR CLOCK = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// RT2 First Batch Single Engine
// ---------------------------------------------------------------------------

#[test]
fn rt2_first_batch_single_engine() {
    let dir = temp_dir();
    let monitor_db = dir.join("monitor.sqlite");
    let roots = make_roots(&dir);
    let mut runtime = CollectorRuntime::with_roots(
        test_config(Some(monitor_db.clone())),
        Some(roots.codex_sessions.parent().unwrap().to_path_buf()),
        Some(roots.claude.clone()),
        Some(
            roots
                .zcode_db
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .to_path_buf(),
        ),
    )
    .unwrap();

    // Sources all empty -> initial discovery completes.
    runtime.tick_with_observation(test_obs()).unwrap();

    // Codex: new rollout, cumulative output = 100.
    let codex_path = roots.codex_sessions.join("rollout-a.jsonl");
    write_line(
        &codex_path,
        &codex_line("2026-07-29T09:00:00.000Z", 1000, 100),
    );
    // Claude: new final, output = 60.
    let claude_path = roots.claude.join("t1.jsonl");
    write_line(
        &claude_path,
        &claude_line("2026-07-29T09:00:00.000Z", "S1", "M1", "model-a", 1000, 60),
    );
    // ZCode: new final, output = 40.
    let conn = rusqlite::Connection::open(&roots.zcode_db).unwrap();
    conn.execute_batch(ZCODE_SCHEMA).unwrap();
    zcode_row(
        &conn,
        "r1",
        "L1",
        "S1",
        "completed",
        1000,
        None,
        2000,
        1000,
        40,
    );
    drop(conn);

    let snap = runtime.tick_with_observation(test_obs()).unwrap();

    let codex_hash = stable_path_hash(&codex_path);
    assert_eq!(
        codex_ledger(&runtime, &codex_hash),
        100,
        "Codex ledger = 100"
    );
    assert_eq!(
        claude_ledger(&runtime, "S1", "M1"),
        60,
        "Claude ledger = 60"
    );
    assert_eq!(zcode_ledger(&runtime, "S1", "L1"), 40, "ZCode ledger = 40");

    // All three live in the SAME Engine / Monitor SQLite, no cross pollution.
    let storage = runtime.storage.clone();
    let ledgers = storage.lock().load_ledgers().unwrap();
    let total: u64 = ledgers.iter().map(|l| l.canonical_output_total).sum();
    assert_eq!(total, 200, "one engine, one SQLite, total 200");
    assert_eq!(snap.adapter_health.len(), 3);
    println!("FIRST BATCH SINGLE ENGINE = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// RT3 Agent Identity Isolation
// ---------------------------------------------------------------------------

#[test]
fn rt3_agent_identity_isolation() {
    let dir = temp_dir();
    let roots = make_roots(&dir);
    let mut runtime = CollectorRuntime::with_roots(
        test_config(None),
        Some(roots.codex_sessions.parent().unwrap().to_path_buf()),
        Some(roots.claude.clone()),
        Some(
            roots
                .zcode_db
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .to_path_buf(),
        ),
    )
    .unwrap();

    runtime.tick_with_observation(test_obs()).unwrap();

    // All three sources deliberately use the SAME raw ids "same-id".
    let codex_path = roots.codex_sessions.join("rollout-same.jsonl");
    write_line(
        &codex_path,
        &codex_line("2026-07-29T09:00:00.000Z", 1000, 100),
    );
    let claude_path = roots.claude.join("t-same.jsonl");
    write_line(
        &claude_path,
        &claude_line(
            "2026-07-29T09:00:00.000Z",
            "same-id",
            "same-id",
            "model-a",
            1000,
            60,
        ),
    );
    let conn = rusqlite::Connection::open(&roots.zcode_db).unwrap();
    conn.execute_batch(ZCODE_SCHEMA).unwrap();
    zcode_row(
        &conn,
        "r1",
        "same-id",
        "same-id",
        "completed",
        1000,
        None,
        2000,
        1000,
        40,
    );
    drop(conn);

    runtime.tick_with_observation(test_obs()).unwrap();

    // Three independent canonical domains despite colliding raw ids.
    let codex_hash = stable_path_hash(&codex_path);
    assert_eq!(codex_ledger(&runtime, &codex_hash), 100);
    assert_eq!(claude_ledger(&runtime, "same-id", "same-id"), 60);
    assert_eq!(zcode_ledger(&runtime, "same-id", "same-id"), 40);

    let ledgers = runtime.storage.lock().load_ledgers().unwrap();
    let agents: Vec<String> = ledgers
        .iter()
        .map(|l| l.correlation_key.agent_id.clone())
        .collect();
    assert!(agents.contains(&"codex".to_string()));
    assert!(agents.contains(&"claude".to_string()));
    assert!(agents.contains(&"zcode".to_string()));
    assert_eq!(agents.len(), 3, "three distinct canonical domains");
    println!("AGENT IDENTITY ISOLATION = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// RT4 Runtime Durable Restart
// ---------------------------------------------------------------------------

#[test]
fn rt4_runtime_durable_restart() {
    let dir = temp_dir();
    let monitor_db = dir.join("monitor.sqlite");
    let roots = make_roots(&dir);
    let codex_path = roots.codex_sessions.join("rollout-a.jsonl");
    let claude_path = roots.claude.join("t1.jsonl");

    // Run 1: all three agents produce data.
    {
        let mut runtime = CollectorRuntime::with_roots(
            test_config(Some(monitor_db.clone())),
            Some(roots.codex_sessions.parent().unwrap().to_path_buf()),
            Some(roots.claude.clone()),
            Some(
                roots
                    .zcode_db
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .to_path_buf(),
            ),
        )
        .unwrap();
        runtime.tick_with_observation(test_obs()).unwrap();

        write_line(
            &codex_path,
            &codex_line("2026-07-29T09:00:00.000Z", 1000, 100),
        );
        write_line(
            &claude_path,
            &claude_line("2026-07-29T09:00:00.000Z", "S1", "M1", "model-a", 1000, 60),
        );
        let conn = rusqlite::Connection::open(&roots.zcode_db).unwrap();
        conn.execute_batch(ZCODE_SCHEMA).unwrap();
        zcode_row(
            &conn,
            "r1",
            "L1",
            "S1",
            "completed",
            1000,
            None,
            2000,
            1000,
            40,
        );
        drop(conn);
        runtime.tick_with_observation(test_obs()).unwrap();

        let codex_hash = stable_path_hash(&codex_path);
        assert_eq!(codex_ledger(&runtime, &codex_hash), 100);
        assert_eq!(claude_ledger(&runtime, "S1", "M1"), 60);
        assert_eq!(zcode_ledger(&runtime, "S1", "L1"), 40);
    } // Drop Runtime + Engine + Storage.

    // Run 2: same Monitor SQLite; second batch; no history repeat / no double count.
    {
        let mut runtime = CollectorRuntime::with_roots(
            test_config(Some(monitor_db.clone())),
            Some(roots.codex_sessions.parent().unwrap().to_path_buf()),
            Some(roots.claude.clone()),
            Some(
                roots
                    .zcode_db
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .to_path_buf(),
            ),
        )
        .unwrap();
        runtime.tick_with_observation(test_obs()).unwrap(); // restore ledgers + checkpoints

        // Second batch.
        write_line(
            &codex_path,
            &codex_line("2026-07-29T09:10:00.000Z", 1000, 150),
        ); // +50
        write_line(
            &claude_path,
            &claude_line("2026-07-29T09:10:00.000Z", "S1", "M2", "model-a", 1000, 30),
        );
        let conn = rusqlite::Connection::open(&roots.zcode_db).unwrap();
        zcode_row(
            &conn,
            "r2",
            "L2",
            "S1",
            "completed",
            3000,
            None,
            4000,
            1000,
            20,
        );
        drop(conn);
        runtime.tick_with_observation(test_obs()).unwrap();

        let codex_hash = stable_path_hash(&codex_path);
        assert_eq!(codex_ledger(&runtime, &codex_hash), 150, "codex 100 + 50");
        assert_eq!(
            claude_ledger(&runtime, "S1", "M1"),
            60,
            "history not repeated"
        );
        assert_eq!(claude_ledger(&runtime, "S1", "M2"), 30);
        assert_eq!(
            zcode_ledger(&runtime, "S1", "L1"),
            40,
            "history not repeated"
        );
        assert_eq!(zcode_ledger(&runtime, "S1", "L2"), 20);

        let ledgers = runtime.storage.lock().load_ledgers().unwrap();
        let total: u64 = ledgers.iter().map(|l| l.canonical_output_total).sum();
        assert_eq!(total, 300, "150 + 90 + 60 — no cross-agent double count");
    }
    println!("FIRST BATCH RUNTIME RESTART = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// RT5 ZCode Shared Clock IN Freshness
// ---------------------------------------------------------------------------

#[test]
fn rt5_zcode_shared_clock_in() {
    let dir = temp_dir();
    let roots = make_roots(&dir);
    let zcode_root = roots
        .zcode_db
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let mut runtime = CollectorRuntime::with_roots(
        test_config(None),
        Some(roots.codex_sessions.parent().unwrap().to_path_buf()),
        Some(roots.claude.clone()),
        Some(zcode_root),
    )
    .unwrap();

    runtime.tick_with_observation(test_obs()).unwrap(); // no sources yet

    // ZCode final: Context Input = 20000, TTFT = 500ms (started 1000 -> first token 1500).
    let conn = rusqlite::Connection::open(&roots.zcode_db).unwrap();
    conn.execute_batch(ZCODE_SCHEMA).unwrap();
    zcode_row(
        &conn,
        "r1",
        "L1",
        "S1",
        "completed",
        1000,
        Some(1500),
        2000,
        20000,
        500,
    );
    drop(conn);

    // Tick at monotonic 1e9: EffectiveMeasured = 20000 / 0.5s = 40000 t/s, fresh <= 1s.
    let snap = runtime
        .tick_with_observation(ObservationTime {
            monotonic_ns: 1_000_000_000,
            wall_timestamp_ms: 1_700_000_000_000,
        })
        .unwrap();
    let g_in = snap
        .global_metrics
        .global_in_tps
        .expect("global IN must exist");
    assert!((g_in - 40_000.0).abs() < 0.001);
    assert_eq!(snap.global_metrics.in_coverage_measured, 1);

    // Advance synthetic runtime time beyond 1s -> IN metric expires.
    let snap2 = runtime
        .tick_with_observation(ObservationTime {
            monotonic_ns: 1_000_000_000 + 2_000_000_000,
            wall_timestamp_ms: 1_700_000_002_000,
        })
        .unwrap();
    assert!(
        snap2.global_metrics.global_in_tps.is_none(),
        "IN TPS must expire after 1s on the shared clock"
    );
    assert_eq!(snap2.global_metrics.in_coverage_measured, 0);
    println!("SHARED CLOCK IN FRESHNESS = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// RT6 No Fake Global OUT
// ---------------------------------------------------------------------------

#[test]
fn rt6_no_fake_global_out() {
    let dir = temp_dir();
    let roots = make_roots(&dir);
    let mut runtime = CollectorRuntime::with_roots(
        test_config(None),
        Some(roots.codex_sessions.parent().unwrap().to_path_buf()),
        Some(roots.claude.clone()),
        Some(
            roots
                .zcode_db
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .to_path_buf(),
        ),
    )
    .unwrap();

    runtime.tick_with_observation(test_obs()).unwrap();

    // All three sources have token usage: codex IntervalExact, claude+zcode TurnExact.
    let codex_path = roots.codex_sessions.join("rollout-a.jsonl");
    write_line(
        &codex_path,
        &codex_line("2026-07-29T09:00:00.000Z", 1000, 100),
    );
    let claude_path = roots.claude.join("t1.jsonl");
    write_line(
        &claude_path,
        &claude_line("2026-07-29T09:00:00.000Z", "S1", "M1", "model-a", 1000, 60),
    );
    let conn = rusqlite::Connection::open(&roots.zcode_db).unwrap();
    conn.execute_batch(ZCODE_SCHEMA).unwrap();
    zcode_row(
        &conn,
        "r1",
        "L1",
        "S1",
        "completed",
        1000,
        None,
        2000,
        1000,
        40,
    );
    drop(conn);

    let snap = runtime.tick_with_observation(test_obs()).unwrap();
    assert_eq!(
        snap.global_metrics.global_out_tps, 0.0,
        "no StreamExact source -> global OUT must stay 0 (never fabricated)"
    );
    assert_eq!(snap.global_metrics.generating_agents_count, 0);
    println!("NO FAKE GLOBAL OUT = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// RT7 Source Failure Isolation
// ---------------------------------------------------------------------------

#[test]
fn rt7_source_failure_isolation() {
    let dir = temp_dir();
    let monitor_db = dir.join("monitor.sqlite");
    let roots = make_roots(&dir);
    let codex_path = roots.codex_sessions.join("rollout-a.jsonl");
    let claude_path = roots.claude.join("t1.jsonl");
    let mut runtime = CollectorRuntime::with_roots(
        test_config(Some(monitor_db.clone())),
        Some(roots.codex_sessions.parent().unwrap().to_path_buf()),
        Some(roots.claude.clone()),
        Some(
            roots
                .zcode_db
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .to_path_buf(),
        ),
    )
    .unwrap();
    runtime.tick_with_observation(test_obs()).unwrap();

    write_line(
        &codex_path,
        &codex_line("2026-07-29T09:00:00.000Z", 1000, 100),
    );
    write_line(
        &claude_path,
        &claude_line("2026-07-29T09:00:00.000Z", "S1", "M1", "model-a", 1000, 60),
    );
    let conn = rusqlite::Connection::open(&roots.zcode_db).unwrap();
    conn.execute_batch(ZCODE_SCHEMA).unwrap();
    zcode_row(
        &conn,
        "r1",
        "L1",
        "S1",
        "completed",
        1000,
        None,
        2000,
        1000,
        40,
    );
    drop(conn);
    runtime.tick_with_observation(test_obs()).unwrap();

    // ZCode external DB disappears -> degraded health ONLY; codex/claude keep polling.
    std::fs::remove_file(&roots.zcode_db).unwrap();
    write_line(
        &codex_path,
        &codex_line("2026-07-29T09:10:00.000Z", 1000, 150),
    );
    let snap = runtime.tick_with_observation(test_obs()).unwrap();

    let zhealth = snap
        .adapter_health
        .iter()
        .find(|h| h.agent_id == "zcode")
        .expect("zcode health");
    assert!(!zhealth.source_available, "zcode source degraded");
    assert!(zhealth.source_degraded);
    assert!(!zhealth.fatal, "external failure is NOT runtime fatal");

    let codex_hash = stable_path_hash(&codex_path);
    assert_eq!(
        codex_ledger(&runtime, &codex_hash),
        150,
        "codex still polls normally (+50)"
    );
    assert!(!runtime.is_fatal());

    // Monitor durable failure IS whole-runtime fatal.
    let dir2 = temp_dir();
    let monitor_db2 = dir2.join("monitor2.sqlite");
    let roots2 = make_roots(&dir2);
    let conn = rusqlite::Connection::open(&roots2.zcode_db).unwrap();
    conn.execute_batch(ZCODE_SCHEMA).unwrap();
    drop(conn);
    let storage = std::sync::Arc::new(parking_lot::Mutex::new(
        StorageManager::new_file(&monitor_db2).unwrap(),
    ));
    drop(storage); // create the file + schema first
    {
        let conn = rusqlite::Connection::open(&monitor_db2).unwrap();
        conn.execute_batch(
            "CREATE TRIGGER fail_checkpoint BEFORE INSERT ON source_checkpoints
             BEGIN SELECT RAISE(ABORT, 'injected durable failure'); END;",
        )
        .unwrap();
    }
    let mut runtime2 = CollectorRuntime::with_roots(
        test_config(Some(monitor_db2)),
        Some(roots2.codex_sessions.clone()),
        Some(roots2.claude.clone()),
        Some(
            roots2
                .zcode_db
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .to_path_buf(),
        ),
    )
    .unwrap();
    let res = runtime2.tick_with_observation(test_obs());
    assert!(
        matches!(res, Err(RuntimeError::AdapterFatal(_))),
        "monitor durable failure must be runtime-fatal"
    );
    assert!(runtime2.is_fatal());
    println!("SOURCE FAILURE ISOLATION = PASS");
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&dir2);
}
