use axum::{
    body::Body,
    http::{Method, Request, Response, StatusCode, Uri},
    response::IntoResponse,
};
use crate::client-pool::get_client;
use crate::config::{
    ACTIVE_TRANSFERS, HEADER_DEST_TYPE, HEADER_HOPS, HEADER_INDEX, DEFAULT_INDEX,
    REMOTE_DISK, SERVER_ERROR_CODE,
};
use crate::utils::{generate_random_letters, split_hops};
use tracing::{error, info, warn};
use std::str::FromStr;
// use bytes::Bytes;
use futures_util::stream::StreamExt;

/// Core proxy handler function (equivalent to Go's handler)
pub async fn proxy_handler(req: Request<Body>) -> impl IntoResponse {
    let pre = req.headers()
        .get("X-Pre")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| generate_random_letters(5));
    let headers = req.headers().clone();
    let uri = req.uri().clone();
    let method = req.method().clone();

    // Extract headers
    let hops_str = headers.get(HEADER_HOPS)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let index_str = headers.get(HEADER_INDEX)
        .and_then(|v| v.to_str().ok())
        .unwrap_or(DEFAULT_INDEX);

    let hops = split_hops(hops_str);
    let current_index = index_str.parse::<usize>().unwrap_or(1);
    let hops_len = hops.len();

    info!(
        %pre,
        hops = ?hops,
        current_index = current_index,
        method = %method,
        path = %uri.path(),
        "Received request"
    );

    // Validate hops
    if hops.is_empty() {
        warn!(%pre, "Missing x-hops header");
        return (StatusCode::BAD_REQUEST, "Missing x-hops header").into_response();
    }

    // Calculate new index
    let new_index = current_index + 1;
    if new_index > hops_len {
        warn!(%pre, new_index, hops_len, "Forward index out of range");
        return (StatusCode::from_u16(SERVER_ERROR_CODE).unwrap(), "Forward index out of range").into_response();
    }

    // Parse target hop
    let target_hop = &hops[new_index - 1];
    let parts: Vec<&str> = target_hop.split(':').collect();
    if parts.len() != 2 {
        warn!(%pre, target_hop, "Invalid target hop format");
        return (StatusCode::BAD_REQUEST, "Invalid target hop format").into_response();
    }
    let target_ip = parts[0];
    let target_port = parts[1];

    // Last hop logic: determine scheme and method
    let mut scheme = "http";
    let mut forward_method = method.clone();
    if new_index == hops_len {
        let dest_type = headers.get(HEADER_DEST_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if dest_type != REMOTE_DISK {
            scheme = "https";
            forward_method = Method::PUT;
        }
    }

    // Build target URL
    let target_url = format!(
        "{}://{}{}",
        scheme,
        target_ip,
//         target_port,
        uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("")
    );
    info!(%pre, target_url, "Forwarding to target");

    // Get client
    let target = format!("{}:{}", target_ip, target_port);
    let client = get_client(&target, scheme);

    // ====================== Fix 1: Preserve original logic completely ======================
    let mut forward_req = Request::new(req.into_body());
    *forward_req.uri_mut() = Uri::from_str(&target_url).unwrap();
    *forward_req.method_mut() = forward_method;

    // Copy headers + set new index
    let forward_headers = forward_req.headers_mut();
    forward_headers.extend(headers.iter().map(|(k, v)| (k.clone(), v.clone())));
    forward_headers.insert(
        HEADER_INDEX,
        new_index.to_string().try_into().unwrap(),
    );

    info!(%pre, headers = ?forward_headers, "Forwarded request headers");

    // Send request
    let resp = match client.request(forward_req).await {
        Ok(resp) => resp,
        Err(e) => {
            error!(%pre, err = ?e, "Failed to forward request");
            return (
                StatusCode::from_u16(SERVER_ERROR_CODE).unwrap(),
                "Failed to forward request",
            ).into_response();
        }
    };

    // Build response
    let mut response = Response::new(Body::empty());
    *response.status_mut() = resp.status();

    // Copy response headers
    let resp_headers = response.headers_mut();
    resp_headers.extend(resp.headers().iter().map(|(k, v)| (k.clone(), v.clone())));

    info!(%pre, headers = ?resp.headers(), "Forwarded response headers");

    // Process response body (count + copy)
    ACTIVE_TRANSFERS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let mut body = resp.into_body();
    let mut response_body = Vec::new();

    // ====================== Fix 2: Preserve original logic completely ======================
    while let Some(chunk) = body.next().await {
        match chunk {
            Ok(bytes) => response_body.extend_from_slice(&bytes),
            Err(e) => {
                error!(%pre, err = ?e, "Error reading response body");
                ACTIVE_TRANSFERS.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                return (StatusCode::INTERNAL_SERVER_ERROR, "Error reading response body").into_response();
            }
        }
    }

    ACTIVE_TRANSFERS.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    *response.body_mut() = Body::from(response_body);

    info!(
        %pre,
        target_hop,
        status = %response.status(),
        protocol = scheme,
        "Request completed"
    );

    response.into_response()
}