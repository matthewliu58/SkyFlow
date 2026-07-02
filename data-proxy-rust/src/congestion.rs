use crate::config::{ACTIVE_TRANSFERS, WARNING_LEVEL_FOR_BUFFER};
use serde::Serialize;
// use sysinfo::{Pid, ProcessExt, System, SystemExt};
use procfs::process::Process;
use tracing::{error, info, warn};

// use libc;

/// Proxy status struct (equivalent to Go's ProxyStatus)
#[derive(Debug, Serialize)]
pub struct ProxyStatus {
    pub active_connections: i64,
    pub total_mem: u64,
    pub process_mem: u64,
    pub avg_cache_per_conn: f64,
    pub cache_usage_ratio: f64,
}

/// Check congestion status (equivalent to Go's CheckCongestion)
pub fn check_congestion(pre: &str, all_buffer_size: usize) -> ProxyStatus {
    let mut status = ProxyStatus {
        active_connections: 0,
        total_mem: 0,
        process_mem: 0,
        avg_cache_per_conn: 0.0,
        cache_usage_ratio: 0.0,
    };

    // Current process memory usage (RSS)
    status.process_mem = get_process_rss();

    // Active connection count
    let active = ACTIVE_TRANSFERS.load(std::sync::atomic::Ordering::SeqCst);
    if active <= 0 {
        return status;
    }
    status.active_connections = active;

    // Average memory per connection
    let per_conn_cache = all_buffer_size * 1024; // KB -> Bytes per connection
    let avg_cache = status.process_mem as f64 / active as f64;
    status.avg_cache_per_conn = avg_cache;
    status.cache_usage_ratio = avg_cache / per_conn_cache as f64;

    // Log output - info level
    info!(
        proxy_mem = status.process_mem,
        active_connections = active,
        avg_cache_per_connection = avg_cache,
        pre = pre,
        "Proxy avg cache"
    );

    // Congestion warning
    if status.cache_usage_ratio > WARNING_LEVEL_FOR_BUFFER as f64 {
        warn!(
            WarningLevelforBuffer = WARNING_LEVEL_FOR_BUFFER,
            pre = pre,
            "Potential congestion: average per-connection buffer near 128KB"
        );
    }

    // Full status log
    info!(
        pre = pre,
        status = ?status,
        "Proxy status"
    );

    status
}

// Read the maximum memory limit for the process (bytes)
// fn get_process_max_memory() -> u64 {
//     let mut rlim = libc::rlimit {
//         rlim_cur: 0,
//         rlim_max: 0,
//     };
//
//     unsafe {
//         libc::getrlimit(libc::RLIMIT_AS, &mut rlim);
//     }
//
//     // If unlimited, return 0 (avoid division by zero)
//     if rlim.rlim_cur == libc::RLIM_INFINITY {
//         0
//     } else {
//         rlim.rlim_cur as u64
//     }
// }

// Get current process memory usage (bytes)
// pub fn get_process_used_memory() -> u64 {
//     // Global singleton System to avoid repeated creation
//     use once_cell::sync::Lazy;
//     static SYS: Lazy<tokio::sync::Mutex<sysinfo::System>> =
//         Lazy::new(|| tokio::sync::Mutex::new(sysinfo::System::new_all()));
//
//     tokio::runtime::Handle::current().block_on(async {
//         let mut sys = SYS.lock().await;
//         sys.refresh_processes();
//
//         let pid = Pid::this();
//         if let Some(process) = sys.process(pid) {
//             process.memory() * 1024 // KB → Bytes
//         } else {
//             0
//         }
//     })
// }

pub fn get_process_rss() -> u64 {
    if let Ok(proc) = Process::myself() {
        if let Ok(statm) = proc.statm() {
            // statm.resident: number of resident pages
            let page_size = procfs::page_size().unwrap_or(4096) as u64;
            return statm.resident as u64 * page_size;
        }
    }
    0
}