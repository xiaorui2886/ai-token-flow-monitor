pub mod core;

pub fn run() {
    tauri::Builder::default()
        .setup(|_app| {
            println!("AI Token Flow Monitor backend initialized.");
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
