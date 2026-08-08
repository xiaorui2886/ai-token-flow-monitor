pub mod adapters;
pub mod core;
pub mod runtime;

use tauri::Manager;

use runtime::host::{
    RuntimeHost, RuntimeHostConfig, RuntimeHostHandle, RuntimeHostState, RuntimePublicSnapshot,
};

/// Read-only snapshot command for future UI. Returns `None` until the collector worker
/// published its first tick.
#[tauri::command]
fn get_runtime_snapshot(
    host: tauri::State<'_, RuntimeHostHandle>,
) -> Result<Option<RuntimePublicSnapshot>, String> {
    Ok(host.snapshot())
}

/// Read-only host state command for future UI.
#[tauri::command]
fn get_runtime_host_state(
    host: tauri::State<'_, RuntimeHostHandle>,
) -> Result<RuntimeHostState, String> {
    Ok(host.state())
}

pub fn run() {
    tauri::Builder::default()
        .setup(|app| {
            // Task 03B §7: the official monitor DB lives in the Tauri app local data dir
            // (resolved dynamically — no hardcoded user name / C:\Users / install dir).
            let data_dir = app.path().app_local_data_dir()?;
            let monitor_dir = data_dir.join("ai-token-flow-monitor");
            std::fs::create_dir_all(&monitor_dir)?;
            let db_path = monitor_dir.join("monitor.sqlite");

            let config = RuntimeHostConfig {
                monitor_db_path: Some(db_path),
                ..Default::default()
            };
            let mut host =
                RuntimeHost::new(config).map_err(|e| std::io::Error::other(format!("{e:?}")))?;
            host.start()
                .map_err(|e| std::io::Error::other(format!("{e:?}")))?;

            // The handle (not the CollectorRuntime) is managed: the worker owns the runtime,
            // the frontend can only read sanitized snapshots.
            app.manage(host.handle());
            println!("AI Token Flow Monitor backend initialized.");
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            get_runtime_snapshot,
            get_runtime_host_state
        ])
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|app_handle, event| {
            // Task 03B §9: on real app exit, stop the collector worker and join it.
            // `stop()` is idempotent, so ExitRequested + Exit double-fire is safe.
            match event {
                tauri::RunEvent::ExitRequested { .. } | tauri::RunEvent::Exit => {
                    if let Some(host) = app_handle.try_state::<RuntimeHostHandle>() {
                        host.stop();
                    }
                }
                _ => {}
            }
        });
}
