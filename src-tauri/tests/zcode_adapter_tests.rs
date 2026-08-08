//! ZCode SQLite model_usage Adapter tests (ZC1-ZC20). All fixtures are 100% synthetic —
//! the external "ZCode DB" is a temp SQLite file created by the tests themselves.

use ai_token_flow_monitor_lib::adapters::zcode::discovery::{DiscoveredZCodeDb, ZCodeDiscovery};
use ai_token_flow_monitor_lib::adapters::zcode::reader::{fetch_rows, open_read_only};
use ai_token_flow_monitor_lib::adapters::zcode::{
    build_final_sample, zcode_request_id, zcode_session_id, ZCodeAdapter, ZCodeAdapterConfig,
    ZCodeAdapterError,
};
use ai_token_flow_monitor_lib::core::persistence::StorageManager;
use ai_token_flow_monitor_lib::core::types::*;
use ai_token_flow_monitor_lib::core::EnginePipeline;
use parking_lot::Mutex;
use rusqlite::params;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn temp_dir() -> PathBuf {
    std::env::temp_dir().join(format!("zcode_test_{}", uuid::Uuid::new_v4()))
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS model_usage (
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

/// Create (or reuse) a synthetic external ZCode DB at `path`.
fn create_external_db(path: &Path) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch(SCHEMA).unwrap();
    conn
}

/// Insert one synthetic model_usage row. provider_total/computed_total = input + output.
#[allow(clippy::too_many_arguments)]
fn insert_row(
    conn: &rusqlite::Connection,
    id: &str,
    logical_request_id: &str,
    session: &str,
    turn: &str,
    model: &str,
    provider: &str,
    status: &str,
    started_at: i64,
    first_token_at: Option<i64>,
    completed_at: i64,
    input: Option<i64>,
    output: Option<i64>,
    reasoning: Option<i64>,
    cache_read: Option<i64>,
    cache_creation: Option<i64>,
) {
    let total = input.zip(output).map(|(i, o)| i + o);
    conn.execute(
        "INSERT INTO model_usage (id, logical_request_id, session_id, turn_id, provider_id, model_id, status, started_at, first_token_at, completed_at, input_tokens, output_tokens, reasoning_tokens, cache_creation_input_tokens, cache_read_input_tokens, provider_total_tokens, computed_total_tokens)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
        params![
            id,
            logical_request_id,
            session,
            turn,
            provider,
            model,
            status,
            started_at,
            first_token_at,
            completed_at,
            input,
            output,
            reasoning,
            cache_creation,
            cache_read,
            total,
            total
        ],
    )
    .unwrap();
}

fn make_pipeline() -> (EnginePipeline, Arc<Mutex<StorageManager>>) {
    let storage = Arc::new(Mutex::new(StorageManager::new_in_memory().unwrap()));
    let engine = EnginePipeline::new("zcode_test_run", storage.clone()).unwrap();
    (engine, storage)
}

/// Adapter pointed at a synthetic `~/.zcode/cli` root; immediate discovery.
fn adapter_for(root: &Path) -> ZCodeAdapter {
    ZCodeAdapter::with_discovery(
        ZCodeAdapterConfig {
            poll_interval: Duration::from_millis(1),
            discovery_interval: Duration::ZERO,
            lookback_ms: 600_000,
        },
        ZCodeDiscovery::with_cli_root(root.to_path_buf()),
    )
}

fn cli_db_path(root: &Path) -> PathBuf {
    root.join("db").join("db.sqlite")
}

fn zcode_ledger(
    engine: &EnginePipeline,
    session: &str,
    req: &str,
) -> Option<CanonicalRequestLedger> {
    engine
        .request_ledger
        .get_ledger(&RequestCorrelationKey {
            agent_id: "zcode".to_string(),
            session_id: zcode_session_id(session),
            request_id: zcode_request_id(req),
        })
        .cloned()
}

fn monotonic_now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

/// Create an EMPTY external DB, attach the adapter (boundary 0, §15), then seed rows.
/// Rows inserted BEFORE attach would be pre-attach history and correctly skipped —
/// tests that must count rows therefore attach first, then insert.
fn attach_and_seed(
    engine: &mut EnginePipeline,
    adapter: &mut ZCodeAdapter,
    db_path: &Path,
    seed: impl FnOnce(&rusqlite::Connection),
) {
    std::fs::create_dir_all(db_path.parent().unwrap()).unwrap();
    let conn = create_external_db(db_path);
    drop(conn);
    adapter.refresh_discovery(engine).unwrap(); // attach on empty DB -> boundary 0
    let conn = rusqlite::Connection::open(db_path).unwrap();
    seed(&conn);
    drop(conn);
}

// ---------------------------------------------------------------------------
// ZC1 Read Only Discovery
// ---------------------------------------------------------------------------

#[test]
fn zc1_read_only_discovery() {
    let dir = temp_dir();
    let db_path = cli_db_path(&dir);
    std::fs::create_dir_all(db_path.parent().unwrap()).unwrap();
    create_external_db(&db_path);

    let discovery = ZCodeDiscovery::with_cli_root(dir.clone());
    let db = discovery.discover_db().expect("db must be discovered");
    assert_eq!(db.path, db_path);

    // Read-only open: any write must fail.
    let conn = open_read_only(&db.path).expect("read-only open must succeed");
    let write_res = conn.execute("INSERT INTO model_usage (id) VALUES ('x')", []);
    assert!(
        write_res.is_err(),
        "write on read-only connection must fail"
    );
    println!("READ ONLY SQLITE = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// ZC2 Row Parser Whitelist
// ---------------------------------------------------------------------------

#[test]
fn zc2_row_parser_whitelist() {
    let dir = temp_dir();
    let db_path = cli_db_path(&dir);
    std::fs::create_dir_all(db_path.parent().unwrap()).unwrap();
    let conn = create_external_db(&db_path);
    // Fake sensitive payload columns exist in the real DB — the whitelist reader must
    // NEVER select them (struct has no such fields; the SELECT column list omits them).
    conn.execute_batch(
        "ALTER TABLE model_usage ADD COLUMN error_message TEXT;
         ALTER TABLE model_usage ADD COLUMN raw_usage_json TEXT;
         ALTER TABLE model_usage ADD COLUMN provider_metadata_json TEXT;",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO model_usage (id, logical_request_id, session_id, turn_id, provider_id, model_id, status, started_at, completed_at, input_tokens, output_tokens, reasoning_tokens, cache_read_input_tokens, cache_creation_input_tokens, error_message, raw_usage_json, provider_metadata_json)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
        params![
            "r1", "L1", "S1", "T1", "P1", "model-a", "completed", 1000i64, 2000i64, 500i64,
            100i64, 0i64, 400i64, 0i64, "TOP_SECRET_ERROR", r#"{"top":"secret"}"#,
            r#"{"api_key":"sk-fake"}"#
        ],
    )
    .unwrap();
    drop(conn);

    let rows = fetch_rows(&open_read_only(&db_path).unwrap(), 0).unwrap();
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(r.id, "r1");
    assert_eq!(r.logical_request_id, "L1");
    assert_eq!(r.model_id, "model-a");
    assert_eq!(r.input_tokens, Some(500));
    assert_eq!(r.cache_read_input_tokens, Some(400));
    println!("ROW PARSER WHITELIST = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// ZC3 OpenAI Accounting
// ---------------------------------------------------------------------------

#[test]
fn zc3_openai_accounting() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let db_path = cli_db_path(&dir);
    let mut adapter = adapter_for(&dir);
    attach_and_seed(&mut engine, &mut adapter, &db_path, |conn| {
        insert_row(
            conn,
            "r1",
            "L1",
            "S1",
            "T1",
            "model-a",
            "P1",
            "completed",
            1000,
            Some(1500),
            2000,
            Some(1000),
            Some(100),
            Some(0),
            Some(600),
            Some(0),
        );
    });
    adapter.poll(&mut engine).unwrap();

    let ledger = zcode_ledger(&engine, "S1", "L1").expect("ledger");
    assert_eq!(
        ledger.canonical_context_input_total, 1000,
        "Context = input"
    );
    assert_eq!(
        ledger.canonical_fresh_input_total, 400,
        "Fresh = input - cache_read"
    );
    assert_eq!(ledger.canonical_output_total, 100);
    assert_eq!(ledger.canonical_cache_read, 600);
    assert_eq!(ledger.canonical_cache_write, 0);
    println!("OPENAI ACCOUNTING = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// ZC4 Reasoning Subset
// ---------------------------------------------------------------------------

#[test]
fn zc4_reasoning_subset() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let db_path = cli_db_path(&dir);
    let mut adapter = adapter_for(&dir);
    attach_and_seed(&mut engine, &mut adapter, &db_path, |conn| {
        insert_row(
            conn,
            "r1",
            "L1",
            "S1",
            "T1",
            "model-a",
            "P1",
            "completed",
            1000,
            None,
            2000,
            Some(2000),
            Some(1000),
            Some(600),
            Some(0),
            Some(0),
        );
    });
    adapter.poll(&mut engine).unwrap();

    let ledger = zcode_ledger(&engine, "S1", "L1").expect("ledger");
    assert_eq!(
        ledger.canonical_output_total, 1000,
        "Output must NOT include reasoning again"
    );
    assert_eq!(ledger.canonical_reasoning, 600);
    println!("REASONING SUBSET = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// ZC5 Exact Request Identity
// ---------------------------------------------------------------------------

#[test]
fn zc5_exact_request_identity() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let db_path = cli_db_path(&dir);
    let mut adapter = adapter_for(&dir);
    attach_and_seed(&mut engine, &mut adapter, &db_path, |conn| {
        // Same turn, TWO logical requests -> TWO ledgers.
        insert_row(
            conn,
            "r1",
            "L1",
            "S1",
            "T1",
            "model-a",
            "P1",
            "completed",
            1000,
            None,
            2000,
            Some(1000),
            Some(100),
            Some(0),
            Some(0),
            Some(0),
        );
        insert_row(
            conn,
            "r2",
            "L2",
            "S1",
            "T1",
            "model-a",
            "P1",
            "completed",
            3000,
            None,
            4000,
            Some(1000),
            Some(60),
            Some(0),
            Some(0),
            Some(0),
        );
    });
    adapter.poll(&mut engine).unwrap();

    let l1 = zcode_ledger(&engine, "S1", "L1").expect("L1 ledger");
    let l2 = zcode_ledger(&engine, "S1", "L2").expect("L2 ledger");
    assert_eq!(l1.canonical_output_total, 100);
    assert_eq!(l2.canonical_output_total, 60);
    assert_ne!(l1.correlation_key, l2.correlation_key);
    println!("EXACT REQUEST IDENTITY = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// ZC6 Terminal Error/Cancelled Usage
// ---------------------------------------------------------------------------

#[test]
fn zc6_terminal_error_cancelled_usage() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let db_path = cli_db_path(&dir);
    let mut adapter = adapter_for(&dir);
    attach_and_seed(&mut engine, &mut adapter, &db_path, |conn| {
        insert_row(
            conn,
            "r1",
            "L1",
            "S1",
            "T1",
            "model-a",
            "P1",
            "completed",
            1000,
            None,
            2000,
            Some(1000),
            Some(100),
            Some(0),
            Some(0),
            Some(0),
        );
        insert_row(
            conn,
            "r2",
            "L2",
            "S1",
            "T1",
            "model-a",
            "P1",
            "error",
            3000,
            None,
            4000,
            Some(1000),
            Some(50),
            Some(0),
            Some(0),
            Some(0),
        );
        insert_row(
            conn,
            "r3",
            "L3",
            "S1",
            "T1",
            "model-a",
            "P1",
            "cancelled",
            5000,
            None,
            6000,
            Some(1000),
            Some(25),
            Some(0),
            Some(0),
            Some(0),
        );
    });
    let stats = adapter.poll(&mut engine).unwrap();
    assert_eq!(
        stats.authoritative_finals, 3,
        "all terminal statuses counted"
    );
    let l1 = zcode_ledger(&engine, "S1", "L1").unwrap();
    let l2 = zcode_ledger(&engine, "S1", "L2").unwrap();
    let l3 = zcode_ledger(&engine, "S1", "L3").unwrap();
    assert_eq!(
        l1.canonical_output_total + l2.canonical_output_total + l3.canonical_output_total,
        175
    );
    println!("TERMINAL USAGE = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// ZC7 Initial Attach
// ---------------------------------------------------------------------------

#[test]
fn zc7_initial_attach() {
    let (mut engine, storage) = make_pipeline();
    let dir = temp_dir();
    let db_path = cli_db_path(&dir);
    std::fs::create_dir_all(db_path.parent().unwrap()).unwrap();
    let conn = create_external_db(&db_path);
    // History present at attach: output 5000 at completed_at T0.
    insert_row(
        &conn,
        "rOld",
        "LOld",
        "S1",
        "T1",
        "model-a",
        "P1",
        "completed",
        1000,
        None,
        2000,
        Some(10000),
        Some(5000),
        Some(0),
        Some(0),
        Some(0),
    );
    drop(conn);

    let mut adapter = adapter_for(&dir);
    adapter.refresh_discovery(&mut engine).unwrap(); // initial attach -> boundary = 2000
    let stats1 = adapter.poll(&mut engine).unwrap();
    assert_eq!(
        stats1.authoritative_finals, 0,
        "history must NOT be imported"
    );
    assert!(zcode_ledger(&engine, "S1", "LOld").is_none());

    // New final after attach: output 50 at T1 > T0 -> +50 only.
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    insert_row(
        &conn,
        "rNew",
        "LNew",
        "S1",
        "T1",
        "model-a",
        "P1",
        "completed",
        3000,
        None,
        4000,
        Some(1000),
        Some(50),
        Some(0),
        Some(0),
        Some(0),
    );
    drop(conn);
    let stats2 = adapter.poll(&mut engine).unwrap();
    assert_eq!(stats2.authoritative_finals, 1);
    let ledger = zcode_ledger(&engine, "S1", "LNew").expect("new ledger");
    assert_eq!(ledger.canonical_output_total, 50);

    // Checkpoint persisted with the initial watermark.
    let cps = storage.lock().load_checkpoints().unwrap();
    let cp = cps
        .iter()
        .find(|c| c.source_id.starts_with("zcode_model_usage_"))
        .expect("checkpoint persisted");
    assert_eq!(cp.watermark_timestamp_ms, 4000);
    println!("INITIAL ATTACH = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// ZC8 Runtime DB Appear
// ---------------------------------------------------------------------------

#[test]
fn zc8_runtime_db_appear() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    std::fs::create_dir_all(cli_db_path(&dir).parent().unwrap()).unwrap();

    let mut adapter = adapter_for(&dir);
    assert_eq!(
        adapter.refresh_discovery(&mut engine).unwrap(),
        0,
        "no DB yet"
    );

    // DB appears AFTER monitor start, already containing two finals.
    let conn = create_external_db(&cli_db_path(&dir));
    insert_row(
        &conn,
        "r1",
        "L1",
        "S1",
        "T1",
        "model-a",
        "P1",
        "completed",
        1000,
        None,
        2000,
        Some(1000),
        Some(100),
        Some(0),
        Some(0),
        Some(0),
    );
    insert_row(
        &conn,
        "r2",
        "L2",
        "S1",
        "T1",
        "model-a",
        "P1",
        "completed",
        3000,
        None,
        4000,
        Some(1000),
        Some(60),
        Some(0),
        Some(0),
        Some(0),
    );
    drop(conn);

    assert_eq!(
        adapter.refresh_discovery(&mut engine).unwrap(),
        1,
        "runtime DB attached"
    );
    let stats = adapter.poll(&mut engine).unwrap();
    assert_eq!(
        stats.authoritative_finals, 2,
        "runtime rows must be fully captured"
    );
    let l1 = zcode_ledger(&engine, "S1", "L1").unwrap();
    let l2 = zcode_ledger(&engine, "S1", "L2").unwrap();
    assert_eq!(l1.canonical_output_total + l2.canonical_output_total, 160);
    println!("RUNTIME DB APPEAR = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// ZC9 Watermark Overlap Dedup
// ---------------------------------------------------------------------------

#[test]
fn zc9_watermark_overlap_dedup() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let db_path = cli_db_path(&dir);
    let mut adapter = adapter_for(&dir);
    attach_and_seed(&mut engine, &mut adapter, &db_path, |conn| {
        insert_row(
            conn,
            "r1",
            "L1",
            "S1",
            "T1",
            "model-a",
            "P1",
            "completed",
            1000,
            None,
            2000,
            Some(1000),
            Some(100),
            Some(0),
            Some(0),
            Some(0),
        );
        insert_row(
            conn,
            "r2",
            "L2",
            "S1",
            "T1",
            "model-a",
            "P1",
            "completed",
            3000,
            None,
            4000,
            Some(1000),
            Some(60),
            Some(0),
            Some(0),
            Some(0),
        );
    });
    let s1 = adapter.poll(&mut engine).unwrap();
    assert_eq!(s1.authoritative_finals, 2);
    // Overlap re-read of the SAME rows (still within lookback) -> dedup, no new finals.
    let s2 = adapter.poll(&mut engine).unwrap();
    assert_eq!(s2.authoritative_finals, 0);
    assert_eq!(s2.identical_final_dedup, 2);
    let l1 = zcode_ledger(&engine, "S1", "L1").unwrap();
    let l2 = zcode_ledger(&engine, "S1", "L2").unwrap();
    assert_eq!(l1.canonical_output_total + l2.canonical_output_total, 160);
    println!("WATERMARK OVERLAP = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// ZC10 Changed Row Reconciliation
// ---------------------------------------------------------------------------

#[test]
fn zc10_changed_row_reconciliation() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let db_path = cli_db_path(&dir);
    let mut adapter = adapter_for(&dir);
    attach_and_seed(&mut engine, &mut adapter, &db_path, |conn| {
        insert_row(
            conn,
            "r1",
            "L1",
            "S1",
            "T1",
            "model-a",
            "P1",
            "completed",
            1000,
            None,
            2000,
            Some(1000),
            Some(100),
            Some(0),
            Some(0),
            Some(0),
        );
    });
    adapter.poll(&mut engine).unwrap();
    assert_eq!(
        zcode_ledger(&engine, "S1", "L1")
            .unwrap()
            .canonical_output_total,
        100
    );

    // External synthetic DB UPDATE: same logical request, different values (150).
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute(
        "UPDATE model_usage SET output_tokens = 150, provider_total_tokens = 1150, computed_total_tokens = 1150, completed_at = 3000 WHERE logical_request_id = 'L1'",
        [],
    )
    .unwrap();
    drop(conn);

    let stats = adapter.poll(&mut engine).unwrap();
    assert_eq!(
        stats.changed_final_rewrites, 1,
        "health counter must increment"
    );
    let ledger = zcode_ledger(&engine, "S1", "L1").unwrap();
    assert_eq!(
        ledger.canonical_output_total, 150,
        "ledger exactly B, never A+B (250)"
    );
    println!("CHANGED ROW RECONCILIATION = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// ZC11 Same Completed_at
// ---------------------------------------------------------------------------

#[test]
fn zc11_same_completed_at() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let db_path = cli_db_path(&dir);
    let mut adapter = adapter_for(&dir);
    attach_and_seed(&mut engine, &mut adapter, &db_path, |conn| {
        // Two DIFFERENT requests with the SAME completed_at -> both must count.
        insert_row(
            conn,
            "r1",
            "L1",
            "S1",
            "T1",
            "model-a",
            "P1",
            "completed",
            1000,
            None,
            2000,
            Some(1000),
            Some(100),
            Some(0),
            Some(0),
            Some(0),
        );
        insert_row(
            conn,
            "r2",
            "L2",
            "S1",
            "T1",
            "model-a",
            "P1",
            "completed",
            1000,
            None,
            2000,
            Some(1000),
            Some(60),
            Some(0),
            Some(0),
            Some(0),
        );
    });
    let stats = adapter.poll(&mut engine).unwrap();
    assert_eq!(
        stats.authoritative_finals, 2,
        "same completed_at must not drop a row"
    );
    let l1 = zcode_ledger(&engine, "S1", "L1").unwrap();
    let l2 = zcode_ledger(&engine, "S1", "L2").unwrap();
    assert_eq!(l1.canonical_output_total + l2.canonical_output_total, 160);
    println!("SAME TIMESTAMP SAFETY = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// ZC12 Durable Restart
// ---------------------------------------------------------------------------

#[test]
fn zc12_durable_restart() {
    let dir = temp_dir();
    let monitor_db = dir.join("monitor.sqlite");
    let ext_db = cli_db_path(&dir);
    std::fs::create_dir_all(ext_db.parent().unwrap()).unwrap();

    // Run 1: empty external DB at attach; M1 100 after attach -> ledger + checkpoint.
    {
        let conn = create_external_db(&ext_db);
        drop(conn);
        let storage = Arc::new(Mutex::new(StorageManager::new_file(&monitor_db).unwrap()));
        let mut engine = EnginePipeline::new("zcode_run1", storage.clone()).unwrap();
        let mut adapter = adapter_for(&dir);
        adapter.refresh_discovery(&mut engine).unwrap();
        let conn = rusqlite::Connection::open(&ext_db).unwrap();
        insert_row(
            &conn,
            "r1",
            "L1",
            "S1",
            "T1",
            "model-a",
            "P1",
            "completed",
            1000,
            None,
            2000,
            Some(1000),
            Some(100),
            Some(0),
            Some(0),
            Some(0),
        );
        drop(conn);
        adapter.poll(&mut engine).unwrap();
        assert_eq!(
            zcode_ledger(&engine, "S1", "L1")
                .unwrap()
                .canonical_output_total,
            100
        );
    }

    // Run 2: same monitor DB + same external DB; new M2 60 -> total 160.
    {
        let storage = Arc::new(Mutex::new(StorageManager::new_file(&monitor_db).unwrap()));
        let mut engine = EnginePipeline::new("zcode_run2", storage.clone()).unwrap();
        let mut adapter = adapter_for(&dir);
        adapter.refresh_discovery(&mut engine).unwrap();
        adapter.poll(&mut engine).unwrap(); // overlap re-reads M1 -> dedup

        let conn = rusqlite::Connection::open(&ext_db).unwrap();
        insert_row(
            &conn,
            "r2",
            "L2",
            "S1",
            "T1",
            "model-a",
            "P1",
            "completed",
            3000,
            None,
            4000,
            Some(1000),
            Some(60),
            Some(0),
            Some(0),
            Some(0),
        );
        drop(conn);
        let stats = adapter.poll(&mut engine).unwrap();
        assert_eq!(stats.authoritative_finals, 1);
        let l1 = zcode_ledger(&engine, "S1", "L1").unwrap();
        let l2 = zcode_ledger(&engine, "S1", "L2").unwrap();
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
// ZC13 Duplicate After Restart
// ---------------------------------------------------------------------------

#[test]
fn zc13_duplicate_after_restart() {
    let dir = temp_dir();
    let monitor_db = dir.join("monitor.sqlite");
    let ext_db = cli_db_path(&dir);
    std::fs::create_dir_all(ext_db.parent().unwrap()).unwrap();

    {
        let conn = create_external_db(&ext_db);
        drop(conn);
        let storage = Arc::new(Mutex::new(StorageManager::new_file(&monitor_db).unwrap()));
        let mut engine = EnginePipeline::new("zcode_run1", storage.clone()).unwrap();
        let mut adapter = adapter_for(&dir);
        adapter.refresh_discovery(&mut engine).unwrap();
        let conn = rusqlite::Connection::open(&ext_db).unwrap();
        insert_row(
            &conn,
            "r1",
            "L1",
            "S1",
            "T1",
            "model-a",
            "P1",
            "completed",
            1000,
            None,
            2000,
            Some(1000),
            Some(100),
            Some(0),
            Some(0),
            Some(0),
        );
        drop(conn);
        adapter.poll(&mut engine).unwrap();
    }

    // Run 2: overlap re-reads the old request -> identical dedup, still exactly 100.
    {
        let storage = Arc::new(Mutex::new(StorageManager::new_file(&monitor_db).unwrap()));
        let mut engine = EnginePipeline::new("zcode_run2", storage.clone()).unwrap();
        let mut adapter = adapter_for(&dir);
        adapter.refresh_discovery(&mut engine).unwrap();
        let stats = adapter.poll(&mut engine).unwrap();
        assert_eq!(stats.identical_final_dedup, 1);
        assert_eq!(stats.authoritative_finals, 0);
        let ledger = zcode_ledger(&engine, "S1", "L1").unwrap();
        assert_eq!(ledger.canonical_output_total, 100, "still exactly 100");
    }
    println!("DUPLICATE AFTER RESTART = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// ZC14 TTFT Effective IN
// ---------------------------------------------------------------------------

#[test]
fn zc14_ttft_effective_in() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let db_path = cli_db_path(&dir);
    let mut adapter = adapter_for(&dir);
    attach_and_seed(&mut engine, &mut adapter, &db_path, |conn| {
        // started=1000ms, first_token=1500ms -> TTFT = 500ms; Context Input = 20000.
        insert_row(
            conn,
            "r1",
            "L1",
            "S1",
            "T1",
            "model-a",
            "P1",
            "completed",
            1000,
            Some(1500),
            2000,
            Some(20000),
            Some(500),
            Some(0),
            Some(0),
            Some(0),
        );
    });
    adapter.poll(&mut engine).unwrap();

    // Freshness clock: the adapter records observed_monotonic_ns from its own Instant anchor
    // (created during poll). An anchor created NOW guarantees current <= stored, so the
    // 1-second freshness slot is satisfied deterministically.
    let anchor = std::time::Instant::now();
    let now_ns = anchor.elapsed().as_nanos() as u64;
    let metrics = engine
        .tps_engine
        .calculate_agent_tps("zcode", now_ns, "zcode_test_run");
    let in_tps = metrics.current_in_tps.expect("Effective IN TPS must exist");
    assert!(
        (in_tps - 40_000.0).abs() < 0.001,
        "20000 tokens / 0.5s = 40000 t/s, got {in_tps}"
    );
    assert!(matches!(
        metrics.input_metric,
        InputThroughputMetric::EffectiveMeasured(_)
    ));

    // Global IN: 40000, coverage 1/1.
    let g = engine.global_aggregator.compute_global_metrics(
        &mut engine.tps_engine,
        now_ns,
        "zcode_test_run",
    );
    assert_eq!(g.in_coverage_measured, 1);
    assert_eq!(g.in_coverage_total, 1);
    let g_in = g.global_in_tps.expect("global IN must exist");
    assert!((g_in - 40_000.0).abs() < 0.001);
    println!("EFFECTIVE IN TPS = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// ZC15 TTFT Missing
// ---------------------------------------------------------------------------

#[test]
fn zc15_ttft_missing() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let db_path = cli_db_path(&dir);
    let mut adapter = adapter_for(&dir);
    attach_and_seed(&mut engine, &mut adapter, &db_path, |conn| {
        insert_row(
            conn,
            "r1",
            "L1",
            "S1",
            "T1",
            "model-a",
            "P1",
            "completed",
            1000,
            None,
            2000,
            Some(20000),
            Some(500),
            Some(0),
            Some(0),
            Some(0),
        );
    });
    adapter.poll(&mut engine).unwrap();

    let metrics =
        engine
            .tps_engine
            .calculate_agent_tps("zcode", monotonic_now_ns(), "zcode_test_run");
    assert!(metrics.current_in_tps.is_none(), "no TTFT -> no IN TPS");
    println!("TTFT MISSING = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// ZC16 Final No OUT TPS
// ---------------------------------------------------------------------------

#[test]
fn zc16_final_no_out_tps() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let db_path = cli_db_path(&dir);
    let mut adapter = adapter_for(&dir);
    attach_and_seed(&mut engine, &mut adapter, &db_path, |conn| {
        insert_row(
            conn,
            "r1",
            "L1",
            "S1",
            "T1",
            "model-a",
            "P1",
            "completed",
            1000,
            None,
            2000,
            Some(1000),
            Some(500),
            Some(0),
            Some(0),
            Some(0),
        );
    });
    adapter.poll(&mut engine).unwrap();

    let metrics =
        engine
            .tps_engine
            .calculate_agent_tps("zcode", monotonic_now_ns(), "zcode_test_run");
    assert_eq!(
        metrics.current_out_tps, 0.0,
        "TurnExact Final must NOT enter Instant OUT"
    );
    assert_eq!(metrics.avg_5s_out_tps, 0.0);
    assert!(metrics.interval_avg_metric.is_none());
    println!("FINAL NO OUT TPS = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// ZC17 Storage Failure
// ---------------------------------------------------------------------------

#[test]
fn zc17_storage_failure() {
    let dir = temp_dir();
    let monitor_db = dir.join("monitor.sqlite");
    let ext_db = cli_db_path(&dir);
    std::fs::create_dir_all(ext_db.parent().unwrap()).unwrap();
    let conn = create_external_db(&ext_db);
    drop(conn);

    let storage = Arc::new(Mutex::new(StorageManager::new_file(&monitor_db).unwrap()));
    // Inject durable failure: every monitor source_checkpoints INSERT aborts.
    {
        let conn = rusqlite::Connection::open(&monitor_db).unwrap();
        conn.execute_batch(
            "CREATE TRIGGER fail_checkpoint BEFORE INSERT ON source_checkpoints
             BEGIN SELECT RAISE(ABORT, 'injected durable failure'); END;",
        )
        .unwrap();
    }
    let mut engine = EnginePipeline::new("zcode_fatal", storage.clone()).unwrap();

    let mut adapter = adapter_for(&dir);
    adapter.refresh_discovery(&mut engine).unwrap();
    let err = adapter.poll(&mut engine).unwrap_err(); // initial attach checkpoint persist fails
    assert!(
        matches!(err, ZCodeAdapterError::CheckpointPersist),
        "expected CheckpointPersist, got {err:?}"
    );
    let err2 = adapter.poll(&mut engine).unwrap_err();
    assert!(matches!(err2, ZCodeAdapterError::FatalNeedsEngineRestart));
    let ledgers = storage.lock().load_ledgers().unwrap();
    assert!(
        ledgers.is_empty(),
        "no canonical state may survive fatal storage failure"
    );
    println!("STORAGE FATAL POLICY = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// ZC18 External DB Read Failure
// ---------------------------------------------------------------------------

#[test]
fn zc18_external_db_read_failure() {
    let (mut engine, storage) = make_pipeline();
    let dir = temp_dir();
    let db_path = cli_db_path(&dir);
    let mut adapter = adapter_for(&dir);
    attach_and_seed(&mut engine, &mut adapter, &db_path, |conn| {
        insert_row(
            conn,
            "r1",
            "L1",
            "S1",
            "T1",
            "model-a",
            "P1",
            "completed",
            1000,
            None,
            2000,
            Some(1000),
            Some(100),
            Some(0),
            Some(0),
            Some(0),
        );
    });
    let s1 = adapter.poll(&mut engine).unwrap();
    assert_eq!(s1.authoritative_finals, 1);
    let watermark_before = {
        let cps = storage.lock().load_checkpoints().unwrap();
        cps.iter()
            .find(|c| c.source_id.starts_with("zcode_model_usage_"))
            .unwrap()
            .watermark_timestamp_ms
    };

    // External DB disappears -> SourceUnavailable: Ok poll, checkpoint untouched, NOT fatal.
    std::fs::remove_file(&db_path).unwrap();
    let s2 = adapter.poll(&mut engine).unwrap();
    assert!(
        s2.source_unavailable,
        "external read failure must be reported"
    );
    assert_eq!(s2.authoritative_finals, 0);
    let watermark_after = {
        let cps = storage.lock().load_checkpoints().unwrap();
        cps.iter()
            .find(|c| c.source_id.starts_with("zcode_model_usage_"))
            .unwrap()
            .watermark_timestamp_ms
    };
    assert_eq!(
        watermark_before, watermark_after,
        "checkpoint must NOT advance"
    );

    // DB reappears with a new row -> normal processing resumes.
    let conn = create_external_db(&db_path);
    insert_row(
        &conn,
        "r2",
        "L2",
        "S1",
        "T1",
        "model-a",
        "P1",
        "completed",
        3000,
        None,
        4000,
        Some(1000),
        Some(60),
        Some(0),
        Some(0),
        Some(0),
    );
    drop(conn);
    let s3 = adapter.poll(&mut engine).unwrap();
    assert_eq!(s3.authoritative_finals, 1);
    let l2 = zcode_ledger(&engine, "S1", "L2").expect("L2 ledger");
    assert_eq!(l2.canonical_output_total, 60);
    println!("EXTERNAL SOURCE RETRY = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// ZC19 Provider Model Passthrough
// ---------------------------------------------------------------------------

#[test]
fn zc19_provider_model_passthrough() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let db_path = cli_db_path(&dir);
    let mut adapter = adapter_for(&dir);
    attach_and_seed(&mut engine, &mut adapter, &db_path, |conn| {
        insert_row(
            conn,
            "r1",
            "L1",
            "S1",
            "T1",
            "model-a",
            "provider-x",
            "completed",
            1000,
            None,
            2000,
            Some(1000),
            Some(100),
            Some(0),
            Some(0),
            Some(0),
        );
        insert_row(
            conn,
            "r2",
            "L2",
            "S1",
            "T1",
            "model-b",
            "provider-y",
            "completed",
            3000,
            None,
            4000,
            Some(1000),
            Some(60),
            Some(0),
            Some(0),
            Some(0),
        );
    });
    adapter.poll(&mut engine).unwrap();

    let l1 = zcode_ledger(&engine, "S1", "L1").unwrap();
    let l2 = zcode_ledger(&engine, "S1", "L2").unwrap();
    assert_eq!(
        l1.model, "model-a",
        "model comes from the row, never hardcoded"
    );
    assert_eq!(l1.provider, "provider-x");
    assert_eq!(l2.model, "model-b");
    assert_eq!(l2.provider, "provider-y");
    println!("MODEL PROVIDER PASSTHROUGH = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// ZC20 No Cross-source Double Count
// ---------------------------------------------------------------------------

#[test]
fn zc20_no_cross_source_double_count() {
    let (mut engine, _) = make_pipeline();
    let dir = temp_dir();
    let db_path = cli_db_path(&dir);
    let mut adapter = adapter_for(&dir);
    attach_and_seed(&mut engine, &mut adapter, &db_path, |conn| {
        insert_row(
            conn,
            "r1",
            "L1",
            "S1",
            "T1",
            "model-a",
            "P1",
            "completed",
            1000,
            None,
            2000,
            Some(1000),
            Some(100),
            Some(0),
            Some(0),
            Some(0),
        );
    });

    // Rollout dir exists with a fake model_io event carrying 5000 output —
    // the canonical adapter must NEVER read it.
    let rollout_dir = dir.join("rollout");
    std::fs::create_dir_all(&rollout_dir).unwrap();
    std::fs::write(
        rollout_dir.join("model-io-sess_fake.jsonl"),
        r#"{"type":"model_io","requestId":"R1","sessionId":"S1","turnId":"T1","startedAt":"2026-07-29T09:00:00.000Z","completedAt":"2026-07-29T09:01:00.000Z","durationMs":60000,"model":{"modelId":"model-a","providerId":"P1"},"response":{"usage":{"inputTokens":9000,"outputTokens":5000}}}"#,
    )
    .unwrap();

    let stats = adapter.poll(&mut engine).unwrap();
    assert_eq!(
        stats.authoritative_finals, 1,
        "canonical comes from SQLite only"
    );
    let ledger = zcode_ledger(&engine, "S1", "L1").unwrap();
    assert_eq!(
        ledger.canonical_output_total, 100,
        "rollout usage (5000) must NOT enter canonical"
    );
    println!("NO CROSS SOURCE DOUBLE COUNT = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Extra helpers used by tests
// ---------------------------------------------------------------------------

// Keep types referenced for clarity (no dead-code warnings).
#[allow(dead_code)]
fn _refs() {
    let _ = build_final_sample(
        "h",
        "r",
        &ai_token_flow_monitor_lib::adapters::zcode::types::ZCodeUsageRow {
            id: String::new(),
            logical_request_id: String::new(),
            session_id: String::new(),
            turn_id: String::new(),
            provider_id: String::new(),
            model_id: String::new(),
            variant: None,
            status: "completed".to_string(),
            started_at: 0,
            first_token_at: None,
            completed_at: 0,
            duration_ms: None,
            time_to_first_token_ms: None,
            input_tokens: None,
            output_tokens: None,
            reasoning_tokens: None,
            cache_creation_input_tokens: None,
            cache_read_input_tokens: None,
            provider_total_tokens: None,
            computed_total_tokens: None,
            retry_count: None,
            retryable: false,
            cancelled_by_user: false,
            context_exceeded: false,
        },
    );
    let _ = DiscoveredZCodeDb {
        path: PathBuf::new(),
        db_hash: String::new(),
        size: 0,
        modified_ms: 0,
    };
}
