#!/bin/bash
set -euo pipefail

LUA_SCRIPT_PATH="/home/matth/hop_router.lua"

echo "Generating Lua script ${LUA_SCRIPT_PATH}..."
cat > "${LUA_SCRIPT_PATH}" << 'EOF'
-- Envoy Lua Filter: Minimal hops dynamic routing (request forwarding + response pass-through)
-- Core: Dynamically set Envoy forward target, decouple from static cluster dependency
-- Optimizations: 1. Return 5xx when new_index > hops_len 2. Write dynamic_metadata only once 3. Remove all dead code 4. Pure response pass-through
-- ==============================================
-- Common constants (core essentials only)
-- ==============================================
local HEADER_CONST = {
    HOPS = "x-hops",          -- Forward chain: A1,A2,...An,S3 (core essential)
    INDEX = "x-index",        -- Cursor index (init=1, core essential)
    HOST = "Host",            -- Forward core header (core essential)
    STATUS = ":status"        -- Response status code (only for local error response)
}

local BUSINESS_RULE = {
    EMPTY_VALUE = "",               -- Empty value fallback
    SEPARATOR = ",",                -- Hops separator
    INIT_INDEX = "1",               -- Initial index=1
    SERVER_ERROR_CODE = "503"       -- Return 503 when new_index exceeds length (service unavailable, suitable for forwarding error scenarios)
}

-- ==============================================
-- Common utility functions (only essential string split)
-- ==============================================
-- Split string to array (parse hops, core essential)
local function split_str(str, sep)
    local arr = {}
    if str == nil or str == BUSINESS_RULE.EMPTY_VALUE then
        return arr
    end
    for val in string.gmatch(str, "[^" .. sep .. "]+") do
        table.insert(arr, val)
    end
    return arr
end

-- ==============================================
-- Request phase (core: parse x-hops, dynamically set Envoy forward target)
-- New: Return 5xx error when new_index > hops_len, abort forwarding
-- ==============================================
function envoy_on_request(request_handle)
    -- Init log_map, unified cache for all logs (info/warn/error)
    local log_map = {}

    -- Initial log
    local init_msg = "[Lua-INFO-1] Start processing hop router request"
    table.insert(log_map, init_msg)
    request_handle:logErr(init_msg)

    -- 1. Read request headers (only essential hops_str and index_str)
    local hops_str = request_handle:headers():get(HEADER_CONST.HOPS) or BUSINESS_RULE.EMPTY_VALUE
    local index_str = request_handle:headers():get(HEADER_CONST.INDEX) or BUSINESS_RULE.INIT_INDEX
    local read_header_msg = string.format("[Lua-INFO-2] Read request headers | x-hops=%s | x-index=%s",
        hops_str, index_str)
    table.insert(log_map, read_header_msg)
    request_handle:logErr(read_header_msg)

    -- 2. Format conversion (core essential: compute forward node base)
    local hops_arr = split_str(hops_str, BUSINESS_RULE.SEPARATOR)
    local current_index = tonumber(index_str) or tonumber(BUSINESS_RULE.INIT_INDEX)
    local hops_len = #hops_arr
    local format_msg = string.format("[Lua-INFO-3] Format convert success | hops length=%d | current_index=%d",
        hops_len, current_index)
    table.insert(log_map, format_msg)
    request_handle:logErr(format_msg)

    -- 3. Empty hops reject forwarding (core error handling: no forward chain)
    if hops_len == 0 then
        local err_msg = "[Lua-ERROR-4] Missing x-hops header, reject forwarding"
        table.insert(log_map, err_msg)
        request_handle:logErr(err_msg)
        -- Write error metadata once
        request_handle:streamInfo():dynamicMetadata():set("lua_error", "msg", table.concat(log_map, "; "))
        -- Return 400 response
        request_handle:respond({[HEADER_CONST.STATUS] = "400"}, "Missing x-hops header")
        return
    end

    -- 4. Compute forward target (core business logic: locate forward node by index)
    local target_hop = BUSINESS_RULE.EMPTY_VALUE
    local new_index = current_index + 1
    -- New: Check if new_index exceeds hops length first, return 5xx if so
    if new_index > hops_len then
        local err_msg = string.format("[Lua-ERROR-5] Forward index out of range | new_index=%d | hops length=%d | current_index=%d",
            new_index, hops_len, current_index)
        table.insert(log_map, err_msg)
        request_handle:logErr(err_msg)
        -- Write error metadata once
        request_handle:streamInfo():dynamicMetadata():set("lua_error","msg", table.concat(log_map, "; "))
        -- Return 5xx status (503 here, can change to 500/etc as needed)
        request_handle:respond({[HEADER_CONST.STATUS] = BUSINESS_RULE.SERVER_ERROR_CODE},
            "Forward index out of range (no valid target hop)")
        return
    end

    -- Normal forward: new_index <= hops_len → pick matching node
    target_hop = hops_arr[new_index]
    local forward_msg = string.format("[Lua-INFO-6] Normal forward | current_index=%d → target=%s | hops=%s",
        current_index, target_hop, hops_str)
    table.insert(log_map, forward_msg)
    request_handle:logErr(forward_msg)

    -- 5. Execute forward (core business logic: set dynamic target, modify headers)
    local target_ip, target_port = string.match(target_hop, "([^:]+):(%d+)")
    if target_ip and target_port then
        -- Header ops: set dynamic forward target (core essential)
        request_handle:headers():replace(":authority", target_ip..":"..target_port)
        request_handle:headers():replace("x-host", target_hop)
        request_handle:headers():replace(HEADER_CONST.HOST, target_hop)

        local set_target_msg = string.format("[Lua-INFO-7] Set dynamic forward target | IP=%s | Port=%s | target_hop=%s",
            target_ip, target_port, target_hop)
        table.insert(log_map, set_target_msg)
        request_handle:logErr(set_target_msg)
    else
        local err_msg = string.format("[Lua-ERROR-8] Invalid target hop format | target_hop=%s", target_hop)
        table.insert(log_map, err_msg)
        request_handle:logErr(err_msg)
        -- Write error metadata once
        request_handle:stream_info():dynamic_metadata():set("lua_error","msg", table.concat(log_map, "; "))
        -- Return 400 response
        request_handle:respond({[HEADER_CONST.STATUS] = "400"}, "Invalid target hop format (required: IP:Port)")
        return
    end

    -- 6. Update Index header (core essential: pass to next hop, advance forward chain)
    request_handle:headers():replace(HEADER_CONST.INDEX, tostring(new_index))
    local update_index_msg = string.format("[Lua-INFO-9] Update x-index header | old_index=%d → new_index=%d",
        current_index, new_index)
    table.insert(log_map, update_index_msg)
    request_handle:logErr(update_index_msg)

    -- Final log
    local final_msg = string.format("[Lua-INFO-10] Request processed | hops=%s | current_index=%d | new_index=%d",
        hops_str, current_index, new_index)
    table.insert(log_map, final_msg)
    request_handle:logErr(final_msg)

    -- Core optimization: Write lua_info metadata only once here (the only write in entire flow)
    request_handle:streamInfo():dynamicMetadata():set("lua_info","msg", table.concat(log_map, "; "))
end

-- ==============================================
-- Response phase (ultra-minimal: no operations, pure pass-through)
-- ==============================================
function envoy_on_response(response_handle)
    -- No response modifications → Envoy auto-returns all downstream responses as-is (including 400/500 errors)
end
EOF

chmod 644 "${LUA_SCRIPT_PATH}"
echo "Lua script generated: ${LUA_SCRIPT_PATH}"
