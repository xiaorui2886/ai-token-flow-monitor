//! First Batch Runtime Probe — starts the real CollectorRuntime (Codex + Claude + ZCode)
//! against the real local sources and takes one passive tick.
//!
//! Passive read only. Never sends prompts, never creates tasks, never modifies external files.
//!
//! Usage:
//!   cargo run --example first_batch_runtime_probe -- --validate-existing
//!   cargo run --example first_batch_runtime_probe -- --live-passive

use ai_token_flow_monitor_lib::runtime::{CollectorRuntime, RuntimeConfig};

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

fn make_runtime() -> CollectorRuntime {
    // Monitor SQLite goes to a temp file — the probe never touches any real monitor DB.
    let db_path =
        std::env::temp_dir().join(format!("first_batch_probe_{}.sqlite", uuid::Uuid::new_v4()));
    let config = RuntimeConfig {
        monitor_db_path: Some(db_path),
        ..Default::default()
    };
    CollectorRuntime::new(config).expect("runtime startup")
}

fn validate_existing() {
    let mut runtime = make_runtime();
    let snapshot = match runtime.tick_once() {
        Ok(s) => s,
        Err(_) => {
            println!("runtime_tick=FAILED");
            println!("monitor_db_ok=false");
            return;
        }
    };

    let run_short: String = snapshot.collector_run_id.chars().take(12).collect();
    println!("collector_run_id={}", run_short);
    for h in &snapshot.adapter_health {
        println!("{}_sources={}", h.agent_id, h.tracked_sources);
        println!(
            "{}_health=available:{} degraded:{} fatal:{}",
            h.agent_id, h.source_available, h.source_degraded, h.fatal
        );
    }
    println!(
        "global_out_tps={:.1}",
        snapshot.global_metrics.global_out_tps
    );
    println!(
        "global_in_available={}",
        snapshot.global_metrics.global_in_tps.is_some()
    );
    println!("monitor_db_ok=true");
}

fn live_passive() {
    let mut runtime = make_runtime();
    // Attach + observe once.
    let _ = runtime.tick_once();

    let total_before = total_output(&runtime);
    std::thread::sleep(std::time::Duration::from_millis(1000));
    let _ = runtime.tick_once();
    let total_after = total_output(&runtime);

    if total_after > total_before {
        println!("LIVE EVENT OBSERVED (new canonical tokens)");
    } else {
        println!("LIVE EVENT NOT OBSERVED");
    }
}

/// Sum of canonical output across all agents in the monitor SQLite (passive read).
fn total_output(runtime: &CollectorRuntime) -> u64 {
    let storage = runtime.storage.clone();
    let guard = storage.lock();
    ["codex", "claude", "zcode"]
        .iter()
        .map(|a| guard.get_total_output_tokens(a).unwrap_or(0))
        .sum()
}
