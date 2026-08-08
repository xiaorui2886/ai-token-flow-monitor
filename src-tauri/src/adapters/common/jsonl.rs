/// Generic byte-safe JSONL tailing + Safe EOF scanning, shared by all JSONL adapters
/// (Codex rollout, Claude transcript, ...). One authoritative implementation — no drift.
use std::path::Path;

/// One complete newline-terminated JSON record with byte offsets.
#[derive(Debug, Clone)]
pub struct JsonlLine {
    /// Start byte offset of this record (first byte).
    pub line_start_offset: u64,
    /// End byte offset AFTER the terminating newline.
    pub line_end_offset: u64,
    /// Record bytes (includes trailing newline).
    pub bytes: Vec<u8>,
}

/// Byte-safe JSONL reader. Only complete newline-terminated records are returned.
/// A partial line at EOF stays in the pending buffer until the next append completes it.
#[derive(Debug, Clone)]
pub struct JsonlTailer {
    /// Next unread file offset.
    pub offset: u64,
    pending: Vec<u8>,
    pending_start: u64,
}

impl JsonlTailer {
    pub fn new(start_offset: u64) -> Self {
        Self {
            offset: start_offset,
            pending: Vec::new(),
            pending_start: start_offset,
        }
    }

    /// Feed a chunk read from `[offset, offset+len)`. Returns complete records.
    /// `offset` is advanced by the chunk length regardless (all bytes are consumed
    /// into either returned lines or the pending partial buffer).
    pub fn feed(&mut self, chunk: &[u8]) -> Vec<JsonlLine> {
        let mut lines = Vec::new();
        self.pending.extend_from_slice(chunk);
        let mut consumed = 0usize;
        while let Some(pos) = self.pending[consumed..].iter().position(|&b| b == b'\n') {
            let end = consumed + pos + 1;
            lines.push(JsonlLine {
                line_start_offset: self.pending_start + consumed as u64,
                line_end_offset: self.pending_start + end as u64,
                bytes: self.pending[consumed..end].to_vec(),
            });
            consumed = end;
        }
        self.pending.drain(..consumed);
        self.pending_start += consumed as u64;
        self.offset += chunk.len() as u64;
        lines
    }

    /// Reset tailer (e.g. after truncation / safe recovery). Discards pending partial bytes.
    pub fn reset(&mut self, new_offset: u64) {
        self.offset = new_offset;
        self.pending.clear();
        self.pending_start = new_offset;
    }

    pub fn has_pending_partial(&self) -> bool {
        !self.pending.is_empty()
    }
}

/// Scan `data[..limit]` and return the byte offset just after the LAST complete
/// newline-terminated record. A partial line at EOF is never included, so the
/// returned offset is always a safe checkpoint position.
pub fn scan_safe_eof(data: &[u8], limit: usize) -> u64 {
    let mut safe_end = 0u64;
    for (i, &b) in data[..limit].iter().enumerate() {
        if b == b'\n' {
            safe_end = (i + 1) as u64;
        }
    }
    safe_end
}

/// Read `[0, end_offset)` of `path` and return the Safe EOF offset.
/// On read failure the file is treated as empty (offset 0) — callers re-try next poll.
pub fn read_scan_safe_eof(path: &Path, end_offset: u64) -> u64 {
    let data = std::fs::read(path).unwrap_or_default();
    let limit = (end_offset as usize).min(data.len());
    scan_safe_eof(&data, limit)
}
