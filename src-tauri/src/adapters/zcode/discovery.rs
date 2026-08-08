use std::path::{Path, PathBuf};

use crate::adapters::common::identity::stable_path_hash;

/// Discovered ZCode primary DB (metadata only).
#[derive(Debug, Clone)]
pub struct DiscoveredZCodeDb {
    pub path: PathBuf,
    pub db_hash: String,
    pub size: u64,
    pub modified_ms: i64,
}

/// Metadata-only environment info: rollout / log directories are DISCOVERED but never
/// opened as canonical sources by V1 (rollout = validation only, logs = ignored).
#[derive(Debug, Clone, Default)]
pub struct ZCodeEnvironmentInfo {
    pub rollout_files: usize,
    pub log_files: usize,
}

/// ZCode discovery (passive read only).
/// Canonical target: `%USERPROFILE%\.zcode\cli\db\db.sqlite`.
/// `tasks-index.sqlite` is NEVER a usage source.
pub struct ZCodeDiscovery {
    cli_dir: PathBuf,
}

impl ZCodeDiscovery {
    /// Dynamic home discovery via USERPROFILE / HOME. Never hardcodes a username or drive letter.
    pub fn new() -> Self {
        let home = std::env::var("USERPROFILE")
            .or_else(|_| std::env::var("HOME"))
            .unwrap_or_else(|_| ".".to_string());
        Self {
            cli_dir: PathBuf::from(home).join(".zcode").join("cli"),
        }
    }

    /// Explicit root (tests use a synthetic `~/.zcode/cli` layout).
    pub fn with_cli_root(root: PathBuf) -> Self {
        Self { cli_dir: root }
    }

    pub fn cli_dir(&self) -> &Path {
        &self.cli_dir
    }

    /// Discover the primary DB if it exists (metadata only).
    pub fn discover_db(&self) -> Option<DiscoveredZCodeDb> {
        let p = self.cli_dir.join("db").join("db.sqlite");
        let m = std::fs::metadata(&p).ok()?;
        if !m.is_file() {
            return None;
        }
        let modified_ms = m
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        Some(DiscoveredZCodeDb {
            path: p.clone(),
            db_hash: stable_path_hash(&p),
            size: m.len(),
            modified_ms,
        })
    }

    /// Metadata-only counts of rollout / log files (§13).
    pub fn discover_environment(&self) -> ZCodeEnvironmentInfo {
        let rollout_files = count_jsonl(&self.cli_dir.join("rollout"));
        let log_files = count_jsonl(&self.cli_dir.join("log"));
        ZCodeEnvironmentInfo {
            rollout_files,
            log_files,
        }
    }
}

fn count_jsonl(dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|e| {
            e.path().is_file()
                && e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.ends_with(".jsonl"))
                    .unwrap_or(false)
        })
        .count()
}

impl Default for ZCodeDiscovery {
    fn default() -> Self {
        Self::new()
    }
}
