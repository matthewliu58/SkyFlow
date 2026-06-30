#!/bin/bash
set -euo pipefail

LUA_SCRIPT_PATH="/root/access_router.lua"

echo "Generating Lua script ${LUA_SCRIPT_PATH}..."
cat > "${LUA_SCRIPT_PATH}" << 'EOF'
-- ${ENVOY_HOME}/lua/port_bandwidth_limit.lua
-- Core config (reasonable interval)
local CHECK_INTERVAL = 5                     -- Bandwidth stats interval: 5s (balance accuracy and performance)
local DEFAULT_BW_LIMIT = 10 * 1024 * 1024    -- Fallback default limit: 10MB/s (used when x-rate header missing/invalid)
local port_in_stats = {}                     -- Port bandwidth stats cache (global var)

-- Core 3: Get bandwidth limit from x-rate header (prefer header, fallback to default)
local function get_port_bw_limit(request_handle, log_map)
    -- 1. Get x-rate header value (case-insensitive, compatible with X-Rate/x-rate)
    local req_headers = request_handle:headers()
    local x_rate_str = req_headers:get("x-rate") or req_headers:get("X-Rate")

    -- 2. Parse x-rate value (expected number, unit: MB/s, auto-convert to bytes/sec)
    local x_rate_mb = tonumber(x_rate_str)
    local bw_limit = DEFAULT_BW_LIMIT  -- Default fallback value

    if x_rate_mb and x_rate_mb > 0 then
        bw_limit = x_rate_mb * 1024 * 1024  -- Convert to bytes/sec (consistent with bandwidth stats unit)
        local info_msg = string.format("[Lua-INFO-1] Got limit from x-rate header: %.2fMB/s (converted: %d bytes/sec)", x_rate_mb, bw_limit)
        table.insert(log_map, info_msg)  -- Store in unified log_map
        request_handle:logErr(info_msg)  -- Unified logErr output with level tag
    else
        local warn_msg = string.format("[Lua-WARN-2] x-rate header missing/invalid (value: %s), using default limit: 10MB/s", x_rate_str or "nil")
        table.insert(log_map, warn_msg)  -- Store in unified log_map
        request_handle:logErr(warn_msg)  -- Unified logErr output with level tag
    end

    return bw_limit
end

-- Core 4: Get port from x-port header (simple and direct, replaces auto-detect)
local function get_port_from_header(request_handle, log_map)
    local current_port = nil
    local req_headers = request_handle:headers()
    -- Get x-port header (case-insensitive, compatible with X-Port/x-port)
    local x_port_str = req_headers:get("x-port") or req_headers:get("X-Port")

    -- Parse port number (must be numeric, range 1-65535 per TCP/IP spec)
    local port_num = tonumber(x_port_str)
    if port_num and port_num > 0 and port_num <= 65535 then
        current_port = tonumber(port_num)
    end

    -- Log port fetch result, store in unified log_map
    local info_msg = string.format("[Lua-INFO-3] Got port from x-port header: %s (raw: %s)", current_port or "fetch failed/invalid", x_port_str or "nil")
    table.insert(log_map, info_msg)
    request_handle:logErr(info_msg)
    return current_port
end

-- Core 5: Calculate real-time port ingress bandwidth (optimization: check time diff first, then fetch metrics)
local function calculate_port_in_bandwidth(request_handle, port, log_map)
    if not port_in_stats[port] then
        port_in_stats[port] = { last_bytes = 0, last_check_time = os.time() }
    end
    local stats = port_in_stats[port]

    local bandwidth = 0
    local now = os.time()
    local time_diff = now - stats.last_check_time

    -- Step 1: Check if time diff meets stats interval, return last bandwidth if not
    if time_diff < CHECK_INTERVAL or time_diff <= 0 then
        bandwidth = stats.last_bw or 0
        local info_msg = string.format("[Lua-INFO-4] Port %d not yet at stats interval (diff=%ds, need>=%ds), using last bandwidth: %.2fMB/s",
          port, time_diff, CHECK_INTERVAL, bandwidth/1024/1024)
        table.insert(log_map, info_msg)  -- Store in unified log_map
        request_handle:logErr(info_msg)
        return bandwidth -- Early return, skip subsequent logic
    end

    -- Step 2: Only when time diff is satisfied, fetch Envoy metrics and compute bandwidth
    local stat_prefix = "ingress_http_" .. port
    local current_bytes = 0
    local ok, counter = pcall(function()
        return request_handle:stats():counter(stat_prefix .. ".downstream_rq_bytes_total")
    end)
    if ok and counter then
        current_bytes = counter:value()
    else
        local warn_msg = string.format("[Lua-WARN-5] Cannot get bandwidth metrics for port %d: %s", port, counter or "metric not found")
        table.insert(log_map, warn_msg)  -- Store in unified log_map
        request_handle:logErr(warn_msg)
        return 0
    end

    -- Calculate real-time bandwidth and update cache
    local byte_diff = current_bytes - stats.last_bytes
    bandwidth = byte_diff / time_diff  -- bytes/sec
    stats.last_bytes = current_bytes
    stats.last_check_time = now
    stats.last_bw = bandwidth

    local info_msg = string.format("[Lua-INFO-6] Port %d bandwidth stats updated: time_diff=%ds, byte_diff=%d, real-time bw=%.2fMB/s",
      port, time_diff, byte_diff, bandwidth/1024/1024)
    table.insert(log_map, info_msg)  -- Store in unified log_map
    request_handle:logErr(info_msg)

    return bandwidth
end

-- Core 6: Request rate-limit logic (removed error_log_map, all logs in unified log_map)
function envoy_on_request(request_handle)
    -- 1. Init local log_map, all logs (info/warn/error) stored here, no more error_log_map
    local log_map = {}

    -- 1a. Print all request headers
    local headers = request_handle:headers()
    local header_log = "[Lua-INFO-7] Request Headers: "
    for key, value in pairs(headers) do
        header_log = header_log .. string.format("%s=%s; ", key, value)
    end
    table.insert(log_map, header_log)
    request_handle:logErr(header_log)

    local x_enable_str = headers:get("x-rate-limit-enable")
    if x_enable_str == nil then

        -- Concat all logs from log_map, write metadata (no separate error metadata needed)
        local full_log_msg = table.concat(log_map, "; ")
        request_handle:streamInfo():dynamicMetadata():set("lua_info", "msg", full_log_msg)

        -- Return 400 Bad Request response
        request_handle:respond(
            {
                [":status"] = "400",
                Content_Type = "text/plain; charset=utf-8",
                X_Error_Type = "Missing x-rate-limit-enable header"
            },
            "Bad Request: x-rate-limit-enable header is missing or invalid."
        )

        return
    end

    if x_enable_str == "false" then
        -- Concat all logs from log_map, write metadata (no separate error metadata needed)
        local full_log_msg = table.concat(log_map, "; ")
        request_handle:streamInfo():dynamicMetadata():set("lua_info", "msg", full_log_msg)
        return
    end

    -- 2. Get port from x-port header
    local current_port = get_port_from_header(request_handle, log_map)
    if not current_port then
        local err_msg = "[Lua-ERROR-8] Rate limit failed: x-port header missing/invalid (pass valid port 1-65535)"
        -- Only store in log_map, removed error_log_map related ops
        table.insert(log_map, err_msg)
        request_handle:logErr(err_msg)

        -- Concat all logs from log_map, write metadata (no separate error metadata needed)
        local full_log_msg = table.concat(log_map, "; ")
        request_handle:streamInfo():dynamicMetadata():set("lua_info", "msg", full_log_msg)

        -- Return 400 Bad Request response
        request_handle:respond(
            {
                [":status"] = "400",
                Content_Type = "text/plain; charset=utf-8",
                X_Error_Type = "Invalid Port (x-port header)"
            },
            "Bad Request: x-port header is missing or invalid. Please pass a valid port number (range: 1-65535)."
        )

        return
    end

    -- 3. Get rate-limit threshold from x-rate header
    local port_limit = get_port_bw_limit(request_handle, log_map)
    local port_limit_mb = port_limit / 1024 / 1024

    -- 4. Calculate real-time bandwidth for x-port port (optimized: check time diff first)
    local current_bw = calculate_port_in_bandwidth(request_handle, current_port, log_map)
    if current_bw <= 0 then
        local info_msg = string.format("[Lua-INFO-9] Port %d bandwidth calc error: %d bytes/sec", current_port, current_bw)
        table.insert(log_map, info_msg)
        request_handle:logErr(info_msg)
    end
    local current_bw_mb = current_bw / 1024 / 1024

    -- 5. Bandwidth limit check: trigger 503 response
    if current_bw > port_limit then

        local limit_msg = string.format("[Lua-INFO-10] Port %d rate limit triggered: %.2fMB/s > %.2fMB/s (threshold from x-rate header)",
          current_port, current_bw_mb, port_limit_mb)
        table.insert(log_map, limit_msg)
        request_handle:logErr(limit_msg)

        local full_log_msg = table.concat(log_map, "; ")
        request_handle:streamInfo():dynamicMetadata():set("lua_info", "msg", full_log_msg)

        request_handle:respond(
            {
                [":status"] = "503",
                X_Limit_Type = "Port In Bandwidth",
                X_Current_Port = tostring(current_port),
                X_Current_BW = string.format("%.2fMB/s", current_bw_mb),
                X_Max_BW = string.format("%.2fMB/s", port_limit_mb),
                X_Rate_Source = "Request Header x-rate",
                X_Port_Source = "Request Header x-port"
            },
            string.format("Port %d Bandwidth Limit Exceeded (Max: %.2fMB/s, From x-rate Header)", current_port, port_limit_mb)
        )

        return
    end

    -- 6. Bandwidth normal: log it
    local normal_msg = string.format("[Lua-INFO-11] Port %d bandwidth normal: %.2fMB/s (limit: %.2fMB/s, threshold from x-rate header)",
      current_port, current_bw_mb, port_limit_mb)
    table.insert(log_map, normal_msg)
    request_handle:logErr(normal_msg)


    -- 7. Core: Concat all logs from log_map (info/warn/error), write metadata once to avoid overwrites
    local full_info_msg = table.concat(log_map, "; ")
    request_handle:streamInfo():dynamicMetadata():set("lua_info", "msg", full_info_msg)

    -- Removed all error_log_map dead code
end

-- Response phase: no-op
function envoy_on_response(response_handle)
end
EOF

chmod 644 "${LUA_SCRIPT_PATH}"
echo "Lua script generated: ${LUA_SCRIPT_PATH}"
