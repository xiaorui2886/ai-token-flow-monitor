//! RuntimeHost tests (HOST1-HOST6, Task 03B). All sources are 100% synthetic.

use ai_token_flow_monitor_lib::adapters::claude::ClaudeAdapterConfig;
use ai_token_flow_monitor_lib::adapters::codex::CodexAdapterConfig;
use ai_token_flow_monitor_lib::adapters::zcode::ZCodeAdapterConfig;
use ai_token_flow_monitor_lib::runtime::host::{
    HostStartError, RuntimeHost, RuntimeHostConfig, RuntimeHostState,
};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

fn temp_dir() -> PathBuf {
    std::env::temp_dir().join(format!("host_test_{}", uuid::Uuid::new_v4()))
}

fn host_config(monitor_db: Option<PathBuf>) -> RuntimeHostConfig {
    RuntimeHostConfig {
        monitor_db_path: monitor_db,
        tick_interval: Duration::from_millis(50),
        codex: CodexAdapterConfig {
            tail_poll_interval: Duration::from_millis(1),
            discovery_interval: Duration::ZERO,
        },
        claude: ClaudeAdapterConfig {
            tail_poll_interval: Duration::from_millis(1),
            discovery_interval: Duration::ZERO,
        },
        zcode: ZCodeAdapterConfig {
            poll_interval: Duration::from_millis(1),
            discovery_interval: Duration::ZERO,
            lookback_ms: 600_000,
        },
    }
}

/// Synthetic roots: codex = <root>/codex/sessions, claude = <root>/claude,
/// zcode = <root>/zcode/db/db.sqlite (mirrors `~/.zcode/cli` layout).
struct SyntheticRoots {
    #[allow(dead_code)]
    dir: PathBuf,
    codex_root: PathBuf,
    claude_root: PathBuf,
    zcode_root: PathBuf,
    codex_rollout: PathBuf,
    zcode_db: PathBuf,
}

fn make_roots(dir: &Path) -> SyntheticRoots {
    let codex_sessions = dir.join("codex").join("sessions");
    let claude_root = dir.join("claude");
    let zcode_root = dir.join("zcode");
    let zcode_db = zcode_root.join("db").join("db.sqlite");
    std::fs::create_dir_all(&codex_sessions).unwrap();
    std::fs::create_dir_all(&claude_root).unwrap();
    std::fs::create_dir_all(zcode_db.parent().unwrap()).unwrap();
    SyntheticRoots {
        dir: dir.to_path_buf(),
        codex_root: dir.join("codex"),
        claude_root,
        zcode_root,
        codex_rollout: codex_sessions.join("rollout-2026-08-08T00-00-00.jsonl"),
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

fn zcode_row(
    conn: &rusqlite::Connection,
    id: &str,
    status: &str,
    completed_at: i64,
    input: i64,
    output: i64,
) {
    conn.execute(
        "INSERT INTO model_usage (id, logical_request_id, session_id, turn_id, provider_id, model_id, status, started_at, first_token_at, completed_at, input_tokens, output_tokens, reasoning_tokens, cache_creation_input_tokens, cache_read_input_tokens, provider_total_tokens, computed_total_tokens)
         VALUES (?1,?1,'sess-a','T1','P1','model-a',?2,?3,?3,?4,?5,?6,0,0,0,?7,?7)",
        rusqlite::params![
            id, status, completed_at - 5000, completed_at, input, output, input + output
        ],
    )
    .unwrap();
}

fn seed_all(roots: &SyntheticRoots) {
    write_line(
        &roots.codex_rollout,
        &codex_line("2026-08-08T00:00:01.000Z", 100, 150),
    );
    write_line(
        &roots.claude_root.join("claude-2026-08-08T00-00-00.jsonl"),
        &claude_line(
            "2026-08-08T00:00:01.000Z",
            "session-raw-1",
            "msg-raw-1",
            "claude-sonnet-4",
            50,
            40,
        ),
    );
    let conn = rusqlite::Connection::open(&roots.zcode_db).unwrap();
    conn.execute_batch(ZCODE_SCHEMA).unwrap();
    zcode_row(&conn, "req-zc-1", "completed", 1_750_000_000_000, 30, 20);
    drop(conn);
}

fn wait_for(mut cond: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    cond()
}

/// Hold an exclusive Windows handle (deny all sharing) so reads fail while metadata
/// succeeds — transient external source failure.
#[cfg(windows)]
fn make_unreadable(path: &Path) -> std::fs::File {
    use std::os::windows::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(path)
        .unwrap()
}

/// Inject a durable failure: RAISE on every INSERT into source_checkpoints.
/// Retries while the worker may hold the DB (SQLITE_BUSY).
fn inject_checkpoint_trigger(monitor_db: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match rusqlite::Connection::open(monitor_db) {
            Ok(conn) => {
                conn.execute_batch(
                    "CREATE TRIGGER fail_checkpoint BEFORE INSERT ON source_checkpoints
                     BEGIN SELECT RAISE(ABORT, 'injected durable failure'); END;",
                )
                .unwrap();
                return;
            }
            Err(_) => {
                assert!(
                    Instant::now() < deadline,
                    "could not open monitor db to inject trigger"
                );
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// HOST1 START STOP
// ---------------------------------------------------------------------------

#[test]
fn host1_start_stop() {
    let dir = temp_dir();
    let roots = make_roots(&dir);
    let mut host = RuntimeHost::with_roots(
        host_config(None),
        Some(roots.codex_root.clone()),
        Some(roots.claude_root.clone()),
        Some(roots.zcode_root.clone()),
    )
    .unwrap();
    let handle = host.handle();
    assert_eq!(handle.state(), RuntimeHostState::Starting);
    host.start().unwrap();
    assert!(
        wait_for(
            || handle.state() == RuntimeHostState::Running,
            Duration::from_secs(5)
        ),
        "host must reach Running after start"
    );
    handle.stop();
    assert_eq!(handle.state(), RuntimeHostState::Stopped);
    // Worker joined: no further snapshot writes after stop.
    let before = handle.snapshot();
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(handle.snapshot(), before, "snapshot must freeze after stop");
    // Stop is idempotent and never panics.
    handle.stop();
    handle.stop();
    println!("HOST START STOP = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// HOST2 SINGLE WORKER
// ---------------------------------------------------------------------------

#[test]
fn host2_single_worker() {
    let dir = temp_dir();
    let roots = make_roots(&dir);
    let mut host = RuntimeHost::with_roots(
        host_config(None),
        Some(roots.codex_root.clone()),
        Some(roots.claude_root.clone()),
        Some(roots.zcode_root.clone()),
    )
    .unwrap();
    let handle = host.handle();
    host.start().unwrap();
    // A second start must NOT create a second CollectorRuntime.
    assert_eq!(host.start(), Err(HostStartError::AlreadyStarted));
    // The original worker is untouched and still publishing.
    assert!(
        wait_for(|| handle.snapshot().is_some(), Duration::from_secs(5)),
        "first worker must keep publishing"
    );
    let s1 = handle.snapshot().unwrap();
    assert!(
        wait_for(
            || handle
                .snapshot()
                .map(|s| s.wall_timestamp_ms > s1.wall_timestamp_ms)
                .unwrap_or(false),
            Duration::from_secs(5)
        ),
        "snapshot must keep advancing (worker alive)"
    );
    // Dropping the RuntimeHost (e.g. end of Tauri setup) must NOT stop the worker while a
    // managed handle is still alive (03B smoke-test regression: premature Drop::stop).
    drop(host);
    let s2 = handle.snapshot().unwrap();
    assert!(
        wait_for(
            || handle
                .snapshot()
                .map(|s| s.wall_timestamp_ms > s2.wall_timestamp_ms)
                .unwrap_or(false),
            Duration::from_secs(5)
        ),
        "worker must survive host drop while a handle is alive"
    );
    handle.stop();
    println!("SINGLE COLLECTOR WORKER = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// HOST3 SNAPSHOT PUBLICATION
// ---------------------------------------------------------------------------

#[test]
fn host3_snapshot_publication() {
    let dir = temp_dir();
    let roots = make_roots(&dir);
    seed_all(&roots);
    let mut host = RuntimeHost::with_roots(
        host_config(None),
        Some(roots.codex_root.clone()),
        Some(roots.claude_root.clone()),
        Some(roots.zcode_root.clone()),
    )
    .unwrap();
    let handle = host.handle();
    host.start().unwrap();
    assert!(
        wait_for(
            || handle
                .snapshot()
                .map(|s| s.codex.tracked_sources > 0
                    && s.claude.tracked_sources > 0
                    && s.zcode.tracked_sources > 0)
                .unwrap_or(false),
            Duration::from_secs(5)
        ),
        "all three agents must be discovered in the public snapshot"
    );
    let snap = handle.snapshot().unwrap();

    // Identity: short run id only (never the full uuid).
    assert!(snap.collector_run_id_short.starts_with("run_"));
    assert!(
        snap.collector_run_id_short.len() <= 12,
        "run id must be truncated"
    );
    assert!(!snap.collector_run_id_short.contains('-'));

    // No fake active agents / no fake OUT TPS (frozen rules §16/§17).
    assert_eq!(snap.working_agents_count, 0);
    assert_eq!(snap.generating_agents_count, 0);
    assert_eq!(snap.global_out_tps, 0.0);

    // Health surface is populated and sanitized.
    assert_eq!(snap.codex.agent_id, "codex");
    assert_eq!(snap.claude.agent_id, "claude");
    assert_eq!(snap.zcode.agent_id, "zcode");
    assert!(snap.codex.source_available);
    assert!(snap.claude.source_available);
    assert!(snap.zcode.source_available);

    // Privacy: the serialized DTO must not leak raw paths or raw IDs.
    let json = serde_json::to_string(&snap).unwrap();
    let dir_str = dir.to_string_lossy().to_string();
    assert!(
        !json.contains(&dir_str),
        "public snapshot must not contain the source root path"
    );
    assert!(
        !json.contains("session-raw-1")
            && !json.contains("msg-raw-1")
            && !json.contains("req-zc-1"),
        "public snapshot must not contain raw session/request/message ids"
    );
    assert!(
        !json.contains("rollout-") && !json.contains(".jsonl") && !json.contains("db.sqlite"),
        "public snapshot must not contain external file names"
    );
    handle.stop();
    println!("HOST SNAPSHOT PUBLICATION = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// HOST4 EXTERNAL FAILURE SURVIVES
// ---------------------------------------------------------------------------

#[test]
fn host4_external_failure_survives() {
    let dir = temp_dir();
    let roots = make_roots(&dir);
    seed_all(&roots);
    let mut host = RuntimeHost::with_roots(
        host_config(None),
        Some(roots.codex_root.clone()),
        Some(roots.claude_root.clone()),
        Some(roots.zcode_root.clone()),
    )
    .unwrap();
    let handle = host.handle();
    host.start().unwrap();
    assert!(
        wait_for(
            || handle.state() == RuntimeHostState::Running
                && handle
                    .snapshot()
                    .map(|s| s.zcode.source_available)
                    .unwrap_or(false),
            Duration::from_secs(5)
        ),
        "host must reach Running with zcode available"
    );

    // External ZCode source fails (Windows exclusive handle) -> Degraded, NOT Fatal.
    #[cfg(windows)]
    let _guard = make_unreadable(&roots.zcode_db);
    #[cfg(not(windows))]
    let _guard = (); // non-Windows: delete the db instead
    #[cfg(not(windows))]
    std::fs::remove_file(&roots.zcode_db).unwrap();

    assert!(
        wait_for(
            || {
                handle.state() == RuntimeHostState::Degraded
                    && handle
                        .snapshot()
                        .map(|s| s.zcode.source_degraded && !s.zcode.source_available)
                        .unwrap_or(false)
            },
            Duration::from_secs(5)
        ),
        "host must degrade (not die) on external source failure"
    );
    assert_ne!(handle.state(), RuntimeHostState::Fatal);

    // Codex/Claude keep going: snapshots keep advancing.
    let s1 = handle.snapshot().unwrap();
    assert!(
        wait_for(
            || {
                handle
                    .snapshot()
                    .map(|s| {
                        s.wall_timestamp_ms > s1.wall_timestamp_ms
                            && s.codex.last_successful_poll_ms > 0
                    })
                    .unwrap_or(false)
            },
            Duration::from_secs(5)
        ),
        "loop must keep ticking and codex must keep polling while zcode is degraded"
    );

    // Recovery: source becomes readable again -> back to Running.
    drop(_guard);
    assert!(
        wait_for(
            || {
                handle.state() == RuntimeHostState::Running
                    && handle
                        .snapshot()
                        .map(|s| s.zcode.source_available)
                        .unwrap_or(false)
            },
            Duration::from_secs(5)
        ),
        "host must recover to Running after external source recovers"
    );
    handle.stop();
    println!("HOST SOURCE DEGRADATION = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// HOST5 DURABLE FATAL STOPS
// ---------------------------------------------------------------------------

#[test]
fn host5_durable_fatal_stops() {
    let dir = temp_dir();
    let roots = make_roots(&dir);
    let monitor_db = dir.join("monitor.sqlite");
    // Seed one codex line so the checkpoint path is exercised after injection.
    write_line(
        &roots.codex_rollout,
        &codex_line("2026-08-08T00:00:01.000Z", 100, 150),
    );

    let mut host = RuntimeHost::with_roots(
        host_config(Some(monitor_db.clone())),
        Some(roots.codex_root.clone()),
        Some(roots.claude_root.clone()),
        Some(roots.zcode_root.clone()),
    )
    .unwrap();
    let handle = host.handle();
    host.start().unwrap();
    assert!(
        wait_for(
            || handle.state() == RuntimeHostState::Running
                && handle
                    .snapshot()
                    .map(|s| s.codex.tracked_sources > 0)
                    .unwrap_or(false),
            Duration::from_secs(5)
        ),
        "host must be Running with codex attached before injecting failure"
    );

    // Inject a monitor durable failure, then make the adapter write durably again.
    inject_checkpoint_trigger(&monitor_db);
    write_line(
        &roots.codex_rollout,
        &codex_line("2026-08-08T00:00:02.000Z", 100, 200),
    );

    assert!(
        wait_for(
            || handle.state() == RuntimeHostState::Fatal,
            Duration::from_secs(5)
        ),
        "monitor durable failure must stop the host as Fatal"
    );
    // Sanitized fatal kind: adapter:<agent>:<kind> — never a raw rusqlite/path message.
    let kind = handle.fatal_kind().expect("fatal kind must be recorded");
    assert!(
        kind.starts_with("adapter:codex:")
            && !kind.contains('\\')
            && !kind.contains("monitor.sqlite"),
        "fatal kind must be sanitized, got: {kind}"
    );

    // No auto-restart: state stays Fatal and the snapshot freezes.
    let frozen = handle.snapshot();
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(
        handle.state(),
        RuntimeHostState::Fatal,
        "no auto-restart allowed"
    );
    assert_eq!(
        handle.snapshot(),
        frozen,
        "worker must be stopped (snapshot frozen)"
    );

    // stop() after fatal: joins (already exited), never panics, keeps fatal state.
    handle.stop();
    handle.stop();
    assert_eq!(handle.state(), RuntimeHostState::Fatal);
    println!("HOST DURABLE FATAL = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// HOST6 CLEAN SHUTDOWN
// ---------------------------------------------------------------------------

#[test]
fn host6_clean_shutdown() {
    let dir = temp_dir();
    let roots = make_roots(&dir);
    seed_all(&roots);
    let monitor_db = dir.join("monitor.sqlite");
    let mut host = RuntimeHost::with_roots(
        host_config(Some(monitor_db.clone())),
        Some(roots.codex_root.clone()),
        Some(roots.claude_root.clone()),
        Some(roots.zcode_root.clone()),
    )
    .unwrap();
    let handle = host.handle();
    host.start().unwrap();
    // Host is mid-tick (Running + advancing snapshots).
    assert!(
        wait_for(
            || handle.state() == RuntimeHostState::Running && handle.snapshot().is_some(),
            Duration::from_secs(5)
        ),
        "host must be running"
    );
    let s1 = handle.snapshot().unwrap();
    assert!(
        wait_for(
            || handle
                .snapshot()
                .map(|s| s.wall_timestamp_ms > s1.wall_timestamp_ms)
                .unwrap_or(false),
            Duration::from_secs(5)
        ),
        "host must be actively ticking"
    );

    handle.stop();
    assert_eq!(handle.state(), RuntimeHostState::Stopped);
    // No more snapshot writes after shutdown.
    let frozen = handle.snapshot();
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        handle.snapshot(),
        frozen,
        "snapshot must freeze after clean stop"
    );

    // The monitor SQLite is reopenable (no corrupt/half-open state).
    let conn = rusqlite::Connection::open(&monitor_db).unwrap();
    let runs: i64 = conn
        .query_row("SELECT COUNT(*) FROM collector_runs", [], |r| r.get(0))
        .unwrap();
    assert!(runs >= 1, "collector run must be durably recorded");
    drop(conn);
    println!("HOST CLEAN SHUTDOWN = PASS");
    let _ = std::fs::remove_dir_all(&dir);
}
