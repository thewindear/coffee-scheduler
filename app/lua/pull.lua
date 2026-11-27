-- KEYS: ready queue, lease queue, worker activity
-- ARGV: prefix, now, batch size, worker ID, default lease ms, activity TTL,
--       service name, activity JSON, allowed business/type pairs ("" means all), reserved queue flag
local prefix, now_ms, batch_size = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])
local worker_id, default_lease_ms, service = ARGV[4], tonumber(ARGV[5]), ARGV[7]
local scopes = ARGV[9] == "" and {} or cjson.decode(ARGV[9])

redis.call("SET", KEYS[3], ARGV[8], "EX", ARGV[6])

local results = {}
local offset = 0
local chunk_size = math.max(batch_size, 64)
while #results < batch_size do
    local candidates = redis.call("ZRANGE", KEYS[1], offset, offset + chunk_size - 1)
    if #candidates == 0 then break end
    for _, task_id in ipairs(candidates) do
        local task_key = prefix .. ":task:" .. task_id
        local vals = redis.call("HMGET", task_key, "queue", "priority", "payload", "retry_count",
            "max_retries", "lease_ttl", "scheduled_at", "business_id", "task_type", "collection_type",
            "recipient_service")
        local allowed = ARGV[9] == ""
        for _, scope in ipairs(scopes) do
            if (scope[1] == "" or scope[1] == vals[8])
                and (scope[2] == "" or scope[2] == (vals[9] or vals[10])) then
                allowed = true
                break
            end
        end
        if not vals[1] then
            redis.call("ZREM", KEYS[1], task_id)
        elseif allowed and (vals[11] == service or (not vals[11] and ARGV[10] == "0")) then
            local lease_ms = (tonumber(vals[6]) or 0) * 1000
            if lease_ms <= 0 then lease_ms = default_lease_ms end
            local expire_at = now_ms + lease_ms
            local gen = redis.call("HINCRBY", task_key, "lease_generation", 1)
            redis.call("ZREM", KEYS[1], task_id)
            redis.call("ZADD", KEYS[2], expire_at, task_id)
            redis.call("HSET", task_key, "status", "RUNNING", "worker_id", worker_id,
                "worker_service", service, "pulled_at", now_ms)
            table.insert(results, {task_id, vals[1], vals[2] or "5", vals[3] or "{}",
                vals[4] or "0", vals[5] or "3", tostring(expire_at),
                vals[7] or tostring(now_ms), tostring(gen), vals[9] or vals[10] or ""})
            if #results == batch_size then break end
        else
            offset = offset + 1
        end
    end
end
return results
