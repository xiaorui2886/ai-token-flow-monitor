pub mod clock;
pub mod host;
pub mod types;

use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::adapters::claude::{ClaudeAdapter, ClaudeAdapterConfig, ClaudeAdapterError};
use crate::adapters::codex::{CodexAdapter, CodexAdapterConfig, CodexAdapterError};
use crate::adapters::zcode::{ZCodeAdapter, ZCodeAdapterConfig, ZCodeAdapterError};
use crate::core::persistence::StorageManager;
use crate::core::EnginePipeline;

use clock::CollectorClock;
use types::{ObservationTime, RuntimeAdapterHealth, RuntimeErrorKind, RuntimeSnapshot};

/// Runtime configuration.
#[derive(Debug, Clone, Default)]
pub struct RuntimeConfig {
    /// Monitor SQLite path. `None` = in-memory (tests only).
    pub monitor_db_path: Option<PathBuf>,
    pub codex: CodexAdapterConfig,
    pub claude: ClaudeAdapterConfig,
    pub zcode: ZCodeAdapterConfig,
}

/// Whole-runtime fatal error (monitor durable storage). External source failures are NOT
/// runtime errors — they surface as degraded adapter health only.
#[derive(Debug)]
pub enum RuntimeError {
    MonitorStorage(String),
    Engine(crate::core::types::EngineError),
    AdapterFatal(String),
}

/// CollectorRuntime — ONE EnginePipeline + ONE CollectorClock + three adapters.
///
/// Single collector loop (V1): `tick_once()` observes the SHARED clock once, then polls
/// Codex, Claude and ZCode in order with the SAME `ObservationTime`. No adapter-local clocks.
pub struct CollectorRuntime {
    pub clock: CollectorClock,
    pub engine: EnginePipeline,
    pub storage: Arc<Mutex<StorageManager>>,
    pub codex: CodexAdapter,
    pub claude: ClaudeAdapter,
    pub zcode: ZCodeAdapter,
    /// Set on any monitor durable failure: the whole runtime must be recreated.
    fatal: Option<String>,
    /// Persistent per-agent health across ticks (Task 03A-FIX §11): last_successful_poll_ms
    /// only advances on a truly successful readable poll; source-unavailable/fatal keep old.
    codex_health: RuntimeAdapterHealth,
    claude_health: RuntimeAdapterHealth,
    zcode_health: RuntimeAdapterHealth,
}

impl CollectorRuntime {
    pub fn new(config: RuntimeConfig) -> Result<Self, RuntimeError> {
        Self::build(config, None, None, None)
    }

    /// Runtime with explicit synthetic discovery roots (tests / probes). None = real home.
    pub fn with_roots(
        config: RuntimeConfig,
        codex_root: Option<PathBuf>,
        claude_root: Option<PathBuf>,
        zcode_root: Option<PathBuf>,
    ) -> Result<Self, RuntimeError> {
        Self::build(config, codex_root, claude_root, zcode_root)
    }

    fn build(
        config: RuntimeConfig,
        codex_root: Option<PathBuf>,
        claude_root: Option<PathBuf>,
        zcode_root: Option<PathBuf>,
    ) -> Result<Self, RuntimeError> {
        let clock = CollectorClock::new();
        let storage = Arc::new(Mutex::new(
            match &config.monitor_db_path {
                Some(p) => StorageManager::new_file(p),
                None => StorageManager::new_in_memory(),
            }
            .map_err(|e| RuntimeError::MonitorStorage(e.to_string()))?,
        ));

        // §8: record this collector run (run_id + started wall time).
        storage
            .lock()
            .record_collector_run(clock.run_id(), clock.started_wall_ms())
            .map_err(|e| RuntimeError::MonitorStorage(e.to_string()))?;

        let engine =
            EnginePipeline::new(clock.run_id(), storage.clone()).map_err(RuntimeError::Engine)?;

        let codex = match codex_root {
            Some(root) => CodexAdapter::with_discovery(
                config.codex,
                crate::adapters::codex::discovery::CodexDiscovery::with_root(root),
            ),
            None => CodexAdapter::new(config.codex),
        };
        let claude = match claude_root {
            Some(root) => ClaudeAdapter::with_discovery(
                config.claude,
                crate::adapters::claude::discovery::ClaudeDiscovery::with_projects_root(root),
            ),
            None => ClaudeAdapter::new(config.claude),
        };
        let zcode = match zcode_root {
            Some(root) => ZCodeAdapter::with_discovery(
                config.zcode,
                crate::adapters::zcode::discovery::ZCodeDiscovery::with_cli_root(root),
            ),
            None => ZCodeAdapter::new(config.zcode),
        };

        Ok(Self {
            clock,
            engine,
            storage,
            codex,
            claude,
            zcode,
            fatal: None,
            codex_health: RuntimeAdapterHealth {
                agent_id: "codex".to_string(),
                source_available: false,
                tracked_sources: 0,
                fatal: false,
                source_degraded: false,
                last_successful_poll_ms: 0,
                last_error_kind: RuntimeErrorKind::None,
            },
            claude_health: RuntimeAdapterHealth {
                agent_id: "claude".to_string(),
                source_available: false,
                tracked_sources: 0,
                fatal: false,
                source_degraded: false,
                last_successful_poll_ms: 0,
                last_error_kind: RuntimeErrorKind::None,
            },
            zcode_health: RuntimeAdapterHealth {
                agent_id: "zcode".to_string(),
                source_available: false,
                tracked_sources: 0,
                fatal: false,
                source_degraded: false,
                last_successful_poll_ms: 0,
                last_error_kind: RuntimeErrorKind::None,
            },
        })
    }

    pub fn is_fatal(&self) -> bool {
        self.fatal.is_some()
    }

    /// One runtime tick on the SHARED clock.
    pub fn tick_once(&mut self) -> Result<RuntimeSnapshot, RuntimeError> {
        let observation = self.clock.observe();
        self.tick_with_observation(observation)
    }

    /// One runtime tick with an explicit observation (deterministic tests / RT5 freshness).
    pub fn tick_with_observation(
        &mut self,
        observation: ObservationTime,
    ) -> Result<RuntimeSnapshot, RuntimeError> {
        if let Some(f) = &self.fatal {
            return Err(RuntimeError::AdapterFatal(f.clone()));
        }

        // Monitor durable failure in ANY adapter -> whole runtime fatal; the tick aborts and
        // the remaining adapters stop ingesting (§24 RT7).
        self.run_codex(&observation)?;
        self.run_claude(&observation)?;
        self.run_zcode(&observation)?;

        // §14: the three usage sources cannot prove request_active/generating — health and
        // token metrics stay separate; nothing is fabricated here.
        let global_metrics = self.engine.global_aggregator.compute_global_metrics(
            &mut self.engine.tps_engine,
            observation.monotonic_ns,
            self.clock.run_id(),
        );

        Ok(RuntimeSnapshot {
            collector_run_id: self.clock.run_id().to_string(),
            observed_monotonic_ns: observation.monotonic_ns,
            wall_timestamp_ms: observation.wall_timestamp_ms,
            global_metrics,
            adapter_health: vec![
                self.codex_health.clone(),
                self.claude_health.clone(),
                self.zcode_health.clone(),
            ],
        })
    }

    fn run_codex(
        &mut self,
        observation: &ObservationTime,
    ) -> Result<RuntimeAdapterHealth, RuntimeError> {
        let mut health = self.codex_health.clone();
        match self
            .codex
            .refresh_discovery(&mut self.engine)
            .and_then(|_| self.codex.poll(&mut self.engine, observation))
        {
            Ok(stats) => {
                // Task 03A-FIX §10: tracked=0 -> not available; degraded = any read failure.
                health.tracked_sources = stats.files_tracked;
                health.source_available = stats.sources_available > 0;
                health.source_degraded = stats.source_read_failures > 0;
                health.fatal = false;
                health.last_error_kind = if stats.source_read_failures > 0 {
                    RuntimeErrorKind::SourceUnavailable
                } else {
                    RuntimeErrorKind::None
                };
                if health.source_available && !health.source_degraded {
                    health.last_successful_poll_ms = observation.wall_timestamp_ms;
                }
            }
            Err(e) => {
                // Monitor durable failure -> whole runtime fatal; stop ingesting.
                health.fatal = true;
                health.last_error_kind = codex_error_kind(e);
                self.fatal = Some(format!("codex: {}", health.last_error_kind));
                return Err(RuntimeError::AdapterFatal(format!(
                    "codex: {}",
                    health.last_error_kind
                )));
            }
        }
        self.codex_health = health.clone();
        Ok(health)
    }

    fn run_claude(
        &mut self,
        observation: &ObservationTime,
    ) -> Result<RuntimeAdapterHealth, RuntimeError> {
        let mut health = self.claude_health.clone();
        match self
            .claude
            .refresh_discovery(&mut self.engine)
            .and_then(|_| self.claude.poll(&mut self.engine, observation))
        {
            Ok(stats) => {
                health.tracked_sources = stats.files_tracked;
                health.source_available = stats.sources_available > 0;
                health.source_degraded = stats.source_read_failures > 0;
                health.fatal = false;
                health.last_error_kind = if stats.source_read_failures > 0 {
                    RuntimeErrorKind::SourceUnavailable
                } else {
                    RuntimeErrorKind::None
                };
                if health.source_available && !health.source_degraded {
                    health.last_successful_poll_ms = observation.wall_timestamp_ms;
                }
            }
            Err(e) => {
                health.fatal = true;
                health.last_error_kind = claude_error_kind(e);
                self.fatal = Some(format!("claude: {}", health.last_error_kind));
                return Err(RuntimeError::AdapterFatal(format!(
                    "claude: {}",
                    health.last_error_kind
                )));
            }
        }
        self.claude_health = health.clone();
        Ok(health)
    }

    fn run_zcode(
        &mut self,
        observation: &ObservationTime,
    ) -> Result<RuntimeAdapterHealth, RuntimeError> {
        let mut health = self.zcode_health.clone();
        match self
            .zcode
            .refresh_discovery(&mut self.engine)
            .and_then(|_| self.zcode.poll(&mut self.engine, observation))
        {
            Ok(stats) => {
                health.tracked_sources = stats.sources_tracked;
                // External source failures are NOT runtime fatal — degraded health only (RT7).
                // tracked=0 -> not available (Task 03A-FIX §10).
                health.source_available = stats.sources_tracked > 0 && !stats.source_unavailable;
                health.source_degraded =
                    stats.source_unavailable || stats.health_unknown_status > 0;
                health.last_error_kind = if stats.source_unavailable {
                    RuntimeErrorKind::SourceUnavailable
                } else {
                    RuntimeErrorKind::None
                };
                if health.source_available && !health.source_degraded {
                    health.last_successful_poll_ms = observation.wall_timestamp_ms;
                }
            }
            Err(e) => {
                health.fatal = true;
                health.last_error_kind = zcode_error_kind(e);
                self.fatal = Some(format!("zcode: {}", health.last_error_kind));
                return Err(RuntimeError::AdapterFatal(format!(
                    "zcode: {}",
                    health.last_error_kind
                )));
            }
        }
        self.zcode_health = health.clone();
        Ok(health)
    }
}

fn codex_error_kind(e: CodexAdapterError) -> RuntimeErrorKind {
    match e {
        CodexAdapterError::CheckpointLoad => RuntimeErrorKind::CheckpointLoad,
        CodexAdapterError::CheckpointPersist => RuntimeErrorKind::CheckpointPersist,
        CodexAdapterError::EngineStorage => RuntimeErrorKind::EngineStorage,
        CodexAdapterError::FatalNeedsEngineRestart => RuntimeErrorKind::Fatal,
    }
}

fn claude_error_kind(e: ClaudeAdapterError) -> RuntimeErrorKind {
    match e {
        ClaudeAdapterError::CheckpointLoad => RuntimeErrorKind::CheckpointLoad,
        ClaudeAdapterError::CheckpointPersist => RuntimeErrorKind::CheckpointPersist,
        ClaudeAdapterError::EngineStorage => RuntimeErrorKind::EngineStorage,
        ClaudeAdapterError::FatalNeedsEngineRestart => RuntimeErrorKind::Fatal,
    }
}

fn zcode_error_kind(e: ZCodeAdapterError) -> RuntimeErrorKind {
    match e {
        ZCodeAdapterError::CheckpointLoad => RuntimeErrorKind::CheckpointLoad,
        ZCodeAdapterError::CheckpointPersist => RuntimeErrorKind::CheckpointPersist,
        ZCodeAdapterError::EngineStorage => RuntimeErrorKind::EngineStorage,
        ZCodeAdapterError::FatalNeedsEngineRestart => RuntimeErrorKind::Fatal,
    }
}
