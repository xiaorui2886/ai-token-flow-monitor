use std::path::{Path, PathBuf};

use crate::adapters::common::identity::stable_path_hash;

/// Discovered Claude transcript JSONL file (metadata only, no content read).
#[derive(Debug, Clone)]
pub struct DiscoveredTranscript {
    pub path: PathBuf,
    pub file_hash: String,
    pub size: u64,
    pub modified_ms: i64,
}

/// Claude Code transcript discovery (passive read only).
/// Walks `%USERPROFILE%\.claude\projects\**\*.jsonl` recursively.
/// `history.jsonl` lives outside `projects/` and is NOT a usage source — never scanned.
pub struct ClaudeDiscovery {
    projects_dir: PathBuf,
}

impl ClaudeDiscovery {
    /// Dynamic home discovery via USERPROFILE / HOME. Never hardcodes a username or drive letter.
    pub fn new() -> Self {
        let home = std::env::var("USERPROFILE")
            .or_else(|_| std::env::var("HOME"))
            .unwrap_or_else(|_| ".".to_string());
        Self {
            projects_dir: PathBuf::from(home).join(".claude").join("projects"),
        }
    }

    /// Explicit root (tests use a synthetic `~/.claude/projects` layout).
    pub fn with_projects_root(root: PathBuf) -> Self {
        Self { projects_dir: root }
    }

    pub fn projects_dir(&self) -> &Path {
        &self.projects_dir
    }

    /// Recursively discover `*.jsonl` under `~/.claude/projects` using std::fs only.
    pub fn discover_transcripts(&self) -> Vec<DiscoveredTranscript> {
        let mut out = Vec::new();
        Self::walk(&self.projects_dir, &mut out);
        out.sort_by_key(|t| t.modified_ms);
        out
    }

    fn walk(dir: &Path, out: &mut Vec<DiscoveredTranscript>) {
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
                if name.ends_with(".jsonl") {
                    if let Ok(m) = std::fs::metadata(&p) {
                        let modified_ms = m
                            .modified()
                            .ok()
                            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or(0);
                        out.push(DiscoveredTranscript {
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
}

impl Default for ClaudeDiscovery {
    fn default() -> Self {
        Self::new()
    }
}
