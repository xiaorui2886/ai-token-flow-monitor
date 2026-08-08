//! RuntimeHost — hosts the frozen CollectorRuntime inside ONE dedicated collector worker.
//!
//! Guarantees (Task 03B):
//! - `CollectorRuntime` exists ONLY inside the collector worker thread; the Tauri
//!   main/frontend thread never locks the EnginePipeline, adapters or external files.
//! - The worker publishes sanitized `RuntimePublicSnapshot`s; commands/frontend read only.
//! - Stop is cooperative and idempotent (request -> loop exits -> worker joins).
//! - Monitor durable failure -> `Fatal` + worker stops. V1: NO auto-restart (no restart storm).
//! - External agent source degradation -> `Degraded`, loop continues, snapshot continues.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use parking_lot::{Condvar, Mutex, RwLock};
use serde::Serialize;

use super::types::{RuntimeAdapterHealth, RuntimeSnapshot};
use super::{CollectorRuntime, RuntimeConfig, RuntimeError};
use crate::adapters::claude::ClaudeAdapterConfig;
use crate::adapters::codex::CodexAdapterConfig;
use crate::adapters::zcode::ZCodeAdapterConfig;

/// Host lifecycle state (sanitized; serialized for the read-only commands).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeHostState {
    /// Host constructed, worker not started yet.
    Starting,
    /// Worker ticking; no external source degradation.
    Running,
    /// Worker ticking; at least one external source degraded (NOT fatal).
    Degraded,
    /// Monitor durable failure — worker stopped, no auto-restart.
    Fatal,
    /// Worker joined after a cooperative stop.
    Stopped,
}

impl std::fmt::Display for RuntimeHostState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            RuntimeHostState::Starting => "starting",
            RuntimeHostState::Running => "running",
            RuntimeHostState::Degraded => "degraded",
            RuntimeHostState::Fatal => "fatal",
            RuntimeHostState::Stopped => "stopped",
        };
        write!(f, "{}", s)
    }
}

/// Sanitized per-agent health for the public snapshot (serde boundary: DTO only).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentHealthPublic {
    pub agent_id: String,
    pub source_available: bool,
    pub tracked_sources: usize,
    pub fatal: bool,
    pub source_degraded: bool,
    pub last_successful_poll_ms: i64,
    pub last_error_kind: String,
}

impl AgentHealthPublic {
    fn from_health(h: &RuntimeAdapterHealth) -> Self {
        AgentHealthPublic {
            agent_id: h.agent_id.clone(),
            source_available: h.source_available,
            tracked_sources: h.tracked_sources,
            fatal: h.fatal,
            source_degraded: h.source_degraded,
            last_successful_poll_ms: h.last_successful_poll_ms,
            last_error_kind: h.last_error_kind.to_string(),
        }
    }
}

/// Public read-only snapshot (sanitized). NEVER carries raw paths, raw IDs, prompts,
/// responses or API credentials (Task 03B §12/§22).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RuntimePublicSnapshot {
    /// `run_` + first 8 hex chars of the uuid — never the full run id.
    pub collector_run_id_short: String,
    pub wall_timestamp_ms: i64,
    pub global_out_tps: f64,
    pub global_in_tps: Option<f64>,
    pub in_coverage_measured: usize,
    pub in_coverage_total: usize,
    pub working_agents_count: usize,
    pub generating_agents_count: usize,
    pub codex: AgentHealthPublic,
    pub claude: AgentHealthPublic,
    pub zcode: AgentHealthPublic,
    pub host_state: RuntimeHostState,
}

impl RuntimePublicSnapshot {
    fn from_tick(snapshot: &RuntimeSnapshot, host_state: RuntimeHostState) -> Self {
        let mut codex = AgentHealthPublic::from_health(&neutral_health("codex"));
        let mut claude = AgentHealthPublic::from_health(&neutral_health("claude"));
        let mut zcode = AgentHealthPublic::from_health(&neutral_health("zcode"));
        for h in &snapshot.adapter_health {
            match h.agent_id.as_str() {
                "codex" => codex = AgentHealthPublic::from_health(h),
                "claude" => claude = AgentHealthPublic::from_health(h),
                "zcode" => zcode = AgentHealthPublic::from_health(h),
                _ => {}
            }
        }
        RuntimePublicSnapshot {
            collector_run_id_short: short_run_id(&snapshot.collector_run_id),
            wall_timestamp_ms: snapshot.wall_timestamp_ms,
            global_out_tps: snapshot.global_metrics.global_out_tps,
            global_in_tps: snapshot.global_metrics.global_in_tps,
            in_coverage_measured: snapshot.global_metrics.in_coverage_measured,
            in_coverage_total: snapshot.global_metrics.in_coverage_total,
            working_agents_count: snapshot.global_metrics.working_agents_count,
            generating_agents_count: snapshot.global_metrics.generating_agents_count,
            codex,
            claude,
            zcode,
            host_state,
        }
    }

    fn with_host_state(mut self, state: RuntimeHostState) -> Self {
        self.host_state = state;
        self
    }
}

fn neutral_health(agent_id: &str) -> RuntimeAdapterHealth {
    RuntimeAdapterHealth {
        agent_id: agent_id.to_string(),
        source_available: false,
        tracked_sources: 0,
        fatal: false,
        source_degraded: false,
        last_successful_poll_ms: 0,
        last_error_kind: super::types::RuntimeErrorKind::None,
    }
}

/// `run_<uuid>` -> `run_` + first 8 hex chars (never the full run id).
fn short_run_id(run_id: &str) -> String {
    match run_id.split_once('_') {
        Some((prefix, rest)) => format!("{}_{}", prefix, rest.chars().take(8).collect::<String>()),
        None => run_id.chars().take(12).collect(),
    }
}

/// Cooperative stop signal: `wait()` sleeps until the tick interval elapses OR stop is
/// requested (instant wake, no busy loop, no mpsc allocation per tick).
struct StopSignal {
    flag: AtomicBool,
    cond: Condvar,
    mutex: Mutex<()>,
}

impl StopSignal {
    fn new() -> Self {
        Self {
            flag: AtomicBool::new(false),
            cond: Condvar::new(),
            mutex: Mutex::new(()),
        }
    }

    fn request(&self) {
        self.flag.store(true, Ordering::Release);
        self.cond.notify_all();
    }

    fn is_requested(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }

    /// Interruptible sleep: returns immediately when stop was requested.
    fn wait(&self, duration: Duration) {
        let mut guard = self.mutex.lock();
        if !self.is_requested() {
            self.cond.wait_for(&mut guard, duration);
        }
    }
}

/// Shared, cheap-clone host state. The worker is the ONLY writer of `snapshot`;
/// commands/frontend only read (`state`, `snapshot`, `fatal_kind`).
struct SharedState {
    /// (host state, sanitized fatal kind).
    state: Mutex<(RuntimeHostState, Option<String>)>,
    snapshot: RwLock<Option<RuntimePublicSnapshot>>,
    /// Single worker join handle; taken exactly once by `stop()` (idempotent join).
    join: Mutex<Option<JoinHandle<()>>>,
}

/// Cloneable handle managed by Tauri (`app.manage`). All methods are non-blocking except
/// `stop()` which joins the worker (bounded: one tick at most).
#[derive(Clone)]
pub struct RuntimeHostHandle {
    shared: Arc<SharedState>,
    stop: Arc<StopSignal>,
}

impl RuntimeHostHandle {
    pub fn state(&self) -> RuntimeHostState {
        self.shared.state.lock().0
    }

    /// Sanitized fatal kind (`monitor_storage` / `engine` / `adapter:<kind>`). Never a raw error.
    pub fn fatal_kind(&self) -> Option<String> {
        self.shared.state.lock().1.clone()
    }

    pub fn snapshot(&self) -> Option<RuntimePublicSnapshot> {
        self.shared.snapshot.read().clone()
    }

    /// Cooperative stop: request -> collector loop exits -> worker joins.
    /// Idempotent: repeated calls join nothing. Never panics, never double-joins.
    /// A `Fatal` host keeps its fatal state; otherwise the state becomes `Stopped`.
    pub fn stop(&self) {
        self.stop.request();
        if let Some(join) = self.shared.join.lock().take() {
            let _ = join.join();
        }
        let mut state = self.shared.state.lock();
        if state.0 != RuntimeHostState::Fatal {
            state.0 = RuntimeHostState::Stopped;
        }
    }
}

impl Drop for RuntimeHostHandle {
    fn drop(&mut self) {
        // Drop fallback: the normal shutdown path (RunEvent::Exit) already called stop();
        // this only prevents a leaked worker when that path was never reached.
        self.stop();
    }
}

/// Error from `RuntimeHost::start`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostStartError {
    /// This host already owns a running collector worker (single-worker guarantee).
    AlreadyStarted,
}

/// Host configuration.
#[derive(Debug, Clone)]
pub struct RuntimeHostConfig {
    /// Monitor SQLite path. `None` = in-memory (tests only). The Tauri app always passes
    /// `Some(<AppLocalData>/ai-token-flow-monitor/monitor.sqlite)`.
    pub monitor_db_path: Option<PathBuf>,
    /// Collector tick cadence (default 200 ms, configurable).
    pub tick_interval: Duration,
    pub codex: CodexAdapterConfig,
    pub claude: ClaudeAdapterConfig,
    pub zcode: ZCodeAdapterConfig,
}

impl Default for RuntimeHostConfig {
    fn default() -> Self {
        Self {
            monitor_db_path: None,
            tick_interval: Duration::from_millis(200),
            codex: CodexAdapterConfig::default(),
            claude: ClaudeAdapterConfig::default(),
            zcode: ZCodeAdapterConfig::default(),
        }
    }
}

/// RuntimeHost: owns the (not yet started) CollectorRuntime and the handle.
/// `start()` moves the collector into the worker thread — a second start is impossible
/// by construction (HOST2 single worker).
pub struct RuntimeHost {
    collector: Option<CollectorRuntime>,
    handle: RuntimeHostHandle,
    tick_interval: Duration,
}

impl RuntimeHost {
    /// Real discovery roots (Tauri app startup path).
    pub fn new(config: RuntimeHostConfig) -> Result<Self, RuntimeError> {
        Self::build(config, None, None, None)
    }

    /// Explicit synthetic discovery roots (tests / probes).
    pub fn with_roots(
        config: RuntimeHostConfig,
        codex_root: Option<PathBuf>,
        claude_root: Option<PathBuf>,
        zcode_root: Option<PathBuf>,
    ) -> Result<Self, RuntimeError> {
        Self::build(config, codex_root, claude_root, zcode_root)
    }

    fn build(
        config: RuntimeHostConfig,
        codex_root: Option<PathBuf>,
        claude_root: Option<PathBuf>,
        zcode_root: Option<PathBuf>,
    ) -> Result<Self, RuntimeError> {
        let runtime_config = RuntimeConfig {
            monitor_db_path: config.monitor_db_path,
            codex: config.codex,
            claude: config.claude,
            zcode: config.zcode,
        };
        let collector =
            CollectorRuntime::with_roots(runtime_config, codex_root, claude_root, zcode_root)?;
        Ok(Self {
            collector: Some(collector),
            handle: RuntimeHostHandle {
                shared: Arc::new(SharedState {
                    state: Mutex::new((RuntimeHostState::Starting, None)),
                    snapshot: RwLock::new(None),
                    join: Mutex::new(None),
                }),
                stop: Arc::new(StopSignal::new()),
            },
            tick_interval: config.tick_interval,
        })
    }

    pub fn handle(&self) -> RuntimeHostHandle {
        self.handle.clone()
    }

    /// Spawn the dedicated collector worker (single loop: observe -> tick -> publish -> wait).
    /// `AlreadyStarted` if a worker is already running — no second CollectorRuntime is ever
    /// created (the collector is consumed on the first start).
    pub fn start(&mut self) -> Result<(), HostStartError> {
        let collector = self
            .collector
            .take()
            .ok_or(HostStartError::AlreadyStarted)?;
        let shared = self.handle.shared.clone();
        let stop = self.handle.stop.clone();
        let tick_interval = self.tick_interval;
        let join = std::thread::Builder::new()
            .name("collector-worker".to_string())
            .spawn(move || worker_loop(collector, shared, stop, tick_interval))
            .expect("failed to spawn collector worker");
        *self.handle.shared.join.lock() = Some(join);
        Ok(())
    }
}

/// The single collector loop. Runs `CollectorRuntime` exclusively on this thread.
fn worker_loop(
    mut collector: CollectorRuntime,
    shared: Arc<SharedState>,
    stop: Arc<StopSignal>,
    tick_interval: Duration,
) {
    while !stop.is_requested() {
        match collector.tick_once() {
            Ok(snapshot) => {
                // External source degradation -> Degraded (NOT fatal); loop continues.
                let state = host_state_from_health(&snapshot);
                *shared.state.lock() = (state, None);
                *shared.snapshot.write() = Some(RuntimePublicSnapshot::from_tick(&snapshot, state));
            }
            Err(e) => {
                // Monitor durable failure (AdapterFatal) -> Fatal + stop the loop.
                // External agent failures never produce Err (degraded health only).
                let kind = sanitized_fatal_kind(&e);
                *shared.state.lock() = (RuntimeHostState::Fatal, Some(kind));
                // Clone FIRST so the read guard drops before the write (never read+write
                // on the same thread — parking_lot RwLock would deadlock).
                let last_snapshot = shared.snapshot.read().clone();
                if let Some(last) = last_snapshot {
                    *shared.snapshot.write() = Some(last.with_host_state(RuntimeHostState::Fatal));
                }
                // V1: no auto-restart — a broken DB/disk must not restart-storm.
                break;
            }
        }
        stop.wait(tick_interval);
    }
}

fn host_state_from_health(snapshot: &RuntimeSnapshot) -> RuntimeHostState {
    if snapshot.adapter_health.iter().any(|h| h.source_degraded) {
        RuntimeHostState::Degraded
    } else {
        RuntimeHostState::Running
    }
}

/// Sanitized fatal kind. `RuntimeError` may wrap raw rusqlite messages (paths) — never
/// forward them; map to fixed kinds only.
fn sanitized_fatal_kind(e: &RuntimeError) -> String {
    match e {
        RuntimeError::MonitorStorage(_) => "monitor_storage".to_string(),
        RuntimeError::Engine(_) => "engine".to_string(),
        RuntimeError::AdapterFatal(s) => format!("adapter:{}", s),
    }
}
