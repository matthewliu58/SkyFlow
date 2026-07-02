use once_cell::sync::OnceCell;
use std::sync::{RwLock, LazyLock};

// Global config (simplified, extensible for YAML reading)
pub static PORT: OnceCell<String> = OnceCell::new();

// HTTP header constants
pub const HEADER_HOPS: &str = "x-hops";
pub const HEADER_INDEX: &str = "x-index";
pub const HEADER_DEST_TYPE: &str = "X-Dest-Type";
pub const REMOTE_DISK: &str = "remote-disk";
pub const DEFAULT_INDEX: &str = "1";
pub const SERVER_ERROR_CODE: u16 = 503;
pub const BUFFER_SIZE: usize = 64 * 1024;

// Congestion detection thresholds
pub const WARNING_LEVEL_FOR_BUFFER: f64 = 0.6;
// pub const CRITICAL_LEVEL_FOR_BUFFER: f64 = 0.8;

// Health status (global) - Fixed static initialization error
pub static STATUS: LazyLock<RwLock<String>> = LazyLock::new(|| {
    RwLock::new(String::from("on"))
});

// Active transfers count (atomic)
pub static ACTIVE_TRANSFERS: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);