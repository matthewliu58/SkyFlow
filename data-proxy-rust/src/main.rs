mod config;
mod logger;
mod client-pool;
mod proxy-handler;
mod health;
mod congestion;
mod utils;

use axum::{routing::get, Router};
use crate::config::{BUFFER_SIZE, PORT};
use crate::congestion::{check_congestion};
use crate::health::{health, health_state_change};
use crate::logger::init_logger;
use crate::proxy-handler::proxy_handler;
use serde_json::json;
use tracing::{error, info};

use serde::Deserialize;
use std::error::Error;

// Limit process maximum memory (RLIMIT_AS)
// use libc::{rlimit, RLIMIT_AS, setrlimit};

// Config struct
#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub port: String,
    pub mem: u64,   // Unit: GB
}

/// Load configuration
pub fn load_config() -> Result<AppConfig, Box<dyn Error>> {
    let cfg = config::Config::builder()
        .add_source(config::File::with_name("config.toml"))
        .build()?;

    let config = cfg.try_deserialize::<AppConfig>()?;
    Ok(config)
}

// Set process maximum memory (read from config)
// fn set_process_max_memory(mem_gb: u64) {
//     let max_bytes = mem_gb * 1024 * 1024 * 1024; // Convert to bytes
//
//     let rlim = rlimit {
//         rlim_cur: max_bytes,
//         rlim_max: max_bytes,
//     };
//
//     unsafe {
//         setrlimit(RLIMIT_AS, &rlim);
//     }
//     info!("Memory limit set: {} GB", mem_gb);
// }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Logger
    if let Err(e) = init_logger() {
        panic!("Failed to init logger: {}", e);
    }

    let pre = "init";
    info!(%pre, "Starting data proxy service");

    // Load config
    let config = load_config()?;
    info!("Config loaded successfully: {:?}", config);

    // Port
    PORT.set(config.port.clone()).unwrap();

    // Set memory limit from config
//     set_process_max_memory(config.mem);
//     let max_mem = get_process_max_memory();
//     println!("Current process max memory: {} MB", max_mem / 1024 / 1024);

    // Routes
    let app = Router::new()
        .route("/healthStateChange", get(health_state_change))
        .route("/health", get(health))
        .route("/queueInfo", get(|| async {
            let status = check_congestion(2 * BUFFER_SIZE);
            json!(status).to_string()
        }))
        .fallback(proxy_handler);

    // Start server
    let port = PORT.get().unwrap();
    let addr = format!("0.0.0.0:{}", port);
    info!(%pre, "Listening on {}", addr);

    axum::Server::bind(&addr.parse()?)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}