//! Claude Code Transcript JSONL Adapter V1 - Local Ground Truth Probe.
//!
//! Passive read only. Never sends prompts, never modifies Claude files.
//!
//! Usage:
//!   cargo run --example claude_probe -- --validate-existing
//!   cargo run --example claude_probe -- --live-passive

use std::collections::HashMap;

use ai_token_flow_monitor_lib::adapters::claude::discovery::ClaudeDiscovery;
use ai_token_flow_monitor_lib::adapters::claude::parser::{parse_claude_line, ClaudeUsageFinality};
use ai_token_flow_monitor_lib::adapters::common::jsonl::JsonlTailer;

/// One message group: (input, output, cache_read, cache_creation, finality) per write.
type UsageGroup = Vec<(
    Option<u64>,
    Option<u64>,
    Option<u64>,
    Option<u64>,
    ClaudeUsageFinality,
)>;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args
        .iter()
        .find(|a| a.as_str() == "--validate-existing" || a.as_str() == "--live-passive")
        .map(|s| s.as_str())
        .unwrap_or("--validate-existing");

    match mode {
        "--validate-existing" => validate_existing(),
        "--live-passive" => live_passive(),
        _ => unreachable!(),
    }
}

fn validate_existing() {
    let discovery = ClaudeDiscovery::new();
    let transcripts = discovery.discover_transcripts();

    let mut files_with_usage = 0usize;
    let mut usage_events_checked = 0u64;
    let mut cache_read_field_present = false;
    let mut cache_creation_field_present = false;
    let mut model_field_present = false;
    // message_id (raw, local only) -> per-write usage tuples
    let mut groups: HashMap<String, UsageGroup> = HashMap::new();

    for t in &transcripts {
        let data = match std::fs::read(&t.path) {
            Ok(d) => d,
            Err(_) => continue,
        };
        let mut tailer = JsonlTailer::new(0);
        let lines = tailer.feed(&data);
        let mut file_has_usage = false;
        for line in &lines {
            match parse_claude_line(&line.bytes) {
                Ok(Some(record)) => {
                    file_has_usage = true;
                    usage_events_checked += 1;
                    cache_read_field_present |= record.cache_read_input_tokens.is_some();
                    cache_creation_field_present |= record.cache_creation_input_tokens.is_some();
                    model_field_present |= record.model.is_some();
                    if let Some(mid) = &record.message_id {
                        groups.entry(mid.clone()).or_default().push((
                            record.input_tokens,
                            record.output_tokens,
                            record.cache_read_input_tokens,
                            record.cache_creation_input_tokens,
                            record.finality,
                        ));
                    }
                }
                Ok(None) => {}
                Err(_) => {}
            }
        }
        if file_has_usage {
            files_with_usage += 1;
        }
    }

    let distinct_message_ids = groups.len();
    let mut identical_reemit_groups = 0u64;
    let mut two_phase_groups = 0u64;
    for v in groups.values() {
        if v.len() < 2 {
            continue;
        }
        let base = (
            v[0].0,
            v[0].1,
            v[0].2,
            v[0].3,
            v[0].4 == ClaudeUsageFinality::AuthoritativeFinal,
        );
        let all_identical = v.iter().all(|x| {
            (
                x.0,
                x.1,
                x.2,
                x.3,
                x.4 == ClaudeUsageFinality::AuthoritativeFinal,
            ) == base
        });
        if all_identical {
            identical_reemit_groups += 1;
        } else {
            two_phase_groups += 1;
        }
    }

    println!("files_found={}", transcripts.len());
    println!("files_with_usage={}", files_with_usage);
    println!("usage_events_checked={}", usage_events_checked);
    println!("distinct_message_ids={}", distinct_message_ids);
    println!("identical_reemit_groups={}", identical_reemit_groups);
    println!("two_phase_groups={}", two_phase_groups);
    println!("cache_read_field_present={}", cache_read_field_present);
    println!(
        "cache_creation_field_present={}",
        cache_creation_field_present
    );
    println!("model_field_present={}", model_field_present);
    println!("reasoning_counter_present=false");
}

fn live_passive() {
    let discovery = ClaudeDiscovery::new();
    let transcripts = discovery.discover_transcripts();

    if transcripts.is_empty() {
        println!("files_found=0");
        println!("LIVE EVENT NOT OBSERVED");
        return;
    }

    // Passive single observation: report whether the newest transcript has grown since attach.
    let newest = transcripts.iter().max_by_key(|t| t.modified_ms);
    let before = newest
        .map(|t| std::fs::metadata(&t.path).map(|m| m.len()).unwrap_or(0))
        .unwrap_or(0);
    std::thread::sleep(std::time::Duration::from_millis(600));
    let after = newest
        .map(|t| std::fs::metadata(&t.path).map(|m| m.len()).unwrap_or(0))
        .unwrap_or(0);

    println!("files_found={}", transcripts.len());
    if after > before {
        println!("LIVE EVENT OBSERVED (new bytes appended)");
    } else {
        println!("LIVE EVENT NOT OBSERVED");
    }
}
