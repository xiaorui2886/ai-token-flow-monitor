use std::path::{Path, PathBuf};

/// Discovered rollout JSONL file (metadata only, no content read).
#[derive(Debug, Clone)]
pub struct DiscoveredRollout {
    pub path: PathBuf,
    pub file_hash: String,
    pub size: u64,
    pub modified_ms: i64,
}

/// Discovered state SQLite file (metadata only).
#[derive(Debug, Clone)]
pub struct StateSqliteInfo {
    pub file_hash: String,
    pub size: u64,
}

/// Codex directory discovery (passive read only).
pub struct CodexDiscovery {
    codex_dir: PathBuf,
}

impl CodexDiscovery {
    /// Dynamic home discovery via USERPROFILE / HOME. Never hardcodes a username or drive letter.
    pub fn new() -> Self {
        let home = std::env::var("USERPROFILE")
            .or_else(|_| std::env::var("HOME"))
            .unwrap_or_else(|_| ".".to_string());
        Self {
            codex_dir: PathBuf::from(home).join(".codex"),
        }
    }

    pub fn codex_dir(&self) -> &Path {
        &self.codex_dir
    }

    /// Recursively discover `~/.codex/sessions/**/rollout-*.jsonl` using std::fs only.
    pub fn discover_rollouts(&self) -> Vec<DiscoveredRollout> {
        let sessions = self.codex_dir.join("sessions");
        let mut out = Vec::new();
        Self::walk(&sessions, &mut out);
        out.sort_by_key(|r| r.modified_ms);
        out
    }

    fn walk(dir: &Path, out: &mut Vec<DiscoveredRollout>) {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                Self::walk(&p, out);
            } else if p.is_file() {
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or_default();
                if name.starts_with("rollout-") && name.ends_with(".jsonl") {
                    if let Ok(m) = std::fs::metadata(&p) {
                        let modified_ms = m
                            .modified()
                            .ok()
                            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or(0);
                        out.push(DiscoveredRollout {
                            path: p.clone(),
                            file_hash: stable_path_hash(&p),
                            size: m.len(),
                            modified_ms,
                        });
                    }
                }
            }
        }
    }

    /// Metadata-only discovery of `~/.codex/state_*.sqlite`. Never opens with immutable=1, never reads content.
    pub fn discover_state_sqlite(&self) -> Vec<StateSqliteInfo> {
        let mut out = Vec::new();
        let entries = match std::fs::read_dir(&self.codex_dir) {
            Ok(e) => e,
            Err(_) => return out,
        };
        for entry in entries.flatten() {
            let p = entry.path();
            if !p.is_file() {
                continue;
            }
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or_default();
            if name.starts_with("state_") && name.ends_with(".sqlite") {
                if let Ok(m) = std::fs::metadata(&p) {
                    out.push(StateSqliteInfo {
                        file_hash: stable_path_hash(&p),
                        size: m.len(),
                    });
                }
            }
        }
        out
    }
}

impl Default for CodexDiscovery {
    fn default() -> Self {
        Self::new()
    }
}

/// Stable, irreversible path hash: absolute path -> Windows separator normalization
/// -> lowercase (case-insensitive) -> SHA-256 -> first 16 hex chars.
///
/// Never print the raw path in logs; only this hash.
pub fn stable_path_hash(path: &Path) -> String {
    let abs = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    let normalized = abs.to_string_lossy().replace('\\', "/").to_lowercase();

    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(normalized.as_bytes());
    let digest = hasher.finalize();
    digest
        .iter()
        .take(8)
        .map(|b| format!("{:02x}", b))
        .collect()
}
