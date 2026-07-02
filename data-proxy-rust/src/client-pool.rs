use hyper::Client;
use hyper_tls::HttpsConnector;
use std::collections::HashMap;
use std::sync::{Arc, RwLock, LazyLock};
use std::time::Duration;

type HttpClient = Client<HttpsConnector<hyper::client::HttpConnector>>;

// Client pool (global) - Fixed static initialization error
static CLIENT_POOL: LazyLock<RwLock<HashMap<String, Arc<HttpClient>>>> = LazyLock::new(|| {
    RwLock::new(HashMap::new())
});

/// Get HTTP/HTTPS client (connection pool reuse, equivalent to Go's getClient)
pub fn get_client(target: &str, _scheme: &str) -> Arc<HttpClient> {
    // Read lock to check existence
    if let Ok(read_lock) = CLIENT_POOL.read() {
        if let Some(client) = read_lock.get(target) {
            return Arc::clone(client);
        }
    }

    // Create new client
    let https = HttpsConnector::new();
    let client = Client::builder()
        .pool_max_idle_per_host(50)
        .pool_idle_timeout(Duration::from_secs(10))
        .http1_read_buf_exact_size(crate::config::BUFFER_SIZE)
        .http1_max_buf_size(crate::config::BUFFER_SIZE)
        .build::<_, hyper::Body>(https);

    let client_arc = Arc::new(client);

    // Write lock to insert into pool
    if let Ok(mut write_lock) = CLIENT_POOL.write() {
        write_lock.insert(target.to_string(), Arc::clone(&client_arc));
    }

    client_arc
}