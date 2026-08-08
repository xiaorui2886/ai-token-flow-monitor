/// Stable irreversible identity hashing, shared by all adapters.
use std::path::Path;

use sha2::{Digest, Sha256};

/// SHA-256 of `input` -> first 16 hex chars.
/// Used for raw string identities (e.g. Claude sessionId / message.id) so no raw ID
/// ever enters logs, SQLite or reports.
pub fn stable_hash16(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let digest = hasher.finalize();
    digest
        .iter()
        .take(8)
        .map(|b| format!("{:02x}", b))
        .collect()
}

/// Stable, irreversible path hash: absolute path -> Windows separator normalization
/// -> lowercase (case-insensitive) -> SHA-256 -> first 16 hex chars.
///
/// Never print the raw path in logs; only this hash.
pub fn stable_path_hash(path: &Path) -> String {
    let abs = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    let normalized = abs.to_string_lossy().replace('\\', "/").to_lowercase();
    stable_hash16(&normalized)
}
