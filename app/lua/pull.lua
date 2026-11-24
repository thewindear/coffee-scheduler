-- KEYS[1]: cs:ready:{queue}
-- KEYS[2]: cs:lease:{queue}
-- KEYS[3]: cs:worker:{queue}:{worker_id}
-- ARGV[1]: prefix
-- ARGV[2]: now_ms
-- ARGV[3]: batch_size
-- ARGV[4]: worker_id
-- ARGV[5]: default_lease_ms
-- ARGV[6]: worker_ttl_sec
local prefix = ARGV[1]
local now_ms = tonumber(ARGV[2])
local batch_size = tonumber(ARGV[3])
local worker_id = ARGV[4]
local default_lease_ms = tonumber(ARGV[5])
local worker_ttl_sec = tonumber(ARGV[6])

redis.call("SET", KEYS[3], now_ms, "EX", worker_ttl_sec)

local popped = redis.call("ZPOPMIN", KEYS[1], batch_size)
if #popped == 0 then
    return {}
end

local results = {}
for i = 1, #popped, 2 do
    local task_id = popped[i]
    local task_key = prefix .. ":task:" .. task_id
    local vals = redis.call("HMGET", task_key, "queue", "priority", "payload", "retry_count", "max_retries", "lease_ttl", "scheduled_at")
    if vals[1] and vals[1] ~= false then
        local lease_sec = tonumber(vals[6])
        local lease_ms = default_lease_ms
        if lease_sec and lease_sec > 0 then
            lease_ms = lease_sec * 1000
        end
        local expire_at = now_ms + lease_ms
        redis.call("ZADD", KEYS[2], expire_at, task_id)
        redis.call("HSET", task_key, "status", "RUNNING", "worker_id", worker_id, "pulled_at", now_ms)
        table.insert(results, {
            task_id,
            vals[1],
            vals[2] or "5",
            vals[3] or "{}",
            vals[4] or "0",
            vals[5] or "3",
            tostring(expire_at),
            vals[7] or tostring(now_ms)
        })
    end
end

return results
