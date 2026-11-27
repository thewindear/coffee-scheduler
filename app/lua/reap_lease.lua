-- KEYS[1]: cs:lease:{queue}
-- KEYS[2]: cs:dlq:{queue}
-- ARGV[1]: prefix
-- ARGV[2]: queue
-- ARGV[3]: now_ms
-- ARGV[4]: limit
-- ARGV[5]: base_delay_ms
-- ARGV[6]: hash_ttl_sec
-- ARGV[7]: shard_count
local prefix = ARGV[1]
local queue = ARGV[2]
local now_ms = tonumber(ARGV[3])
local limit = tonumber(ARGV[4])
local base_delay_ms = tonumber(ARGV[5])
local hash_ttl_sec = tonumber(ARGV[6])
local shard_count = tonumber(ARGV[7])

local expired = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", now_ms, "LIMIT", 0, limit)
if #expired == 0 then
    return {}
end

redis.call("ZREM", KEYS[1], unpack(expired))

local results = {}
for _, task_id in ipairs(expired) do
    local task_key = prefix .. ":task:" .. task_id
    local vals = redis.call("HMGET", task_key, "status", "retry_count", "max_retries", "worker_id", "lease_generation", "worker_service")
    if vals[1] == "RUNNING" then
        local retry_count = tonumber(vals[2]) or 0
        local max_retries = tonumber(vals[3]) or 3
        local timeout_receipt = '{"error_code":"LEASE_TIMEOUT","error_message":"Worker lease expired without heartbeat or ack"}'

        if retry_count < max_retries then
            local next_retry = retry_count + 1
            local backoff = base_delay_ms * (2 ^ (next_retry - 1))
            local next_run_at = now_ms + backoff
            local byte_sum = 0
            for idx = 1, #task_id do
                byte_sum = byte_sum + string.byte(task_id, idx)
            end
            local shard = byte_sum % shard_count
            redis.call("HSET", task_key, "status", "DELAYED", "retry_count", next_retry, "receipt", timeout_receipt)
            redis.call("ZADD", prefix .. ":delay:" .. tostring(shard), next_run_at, task_id)
            table.insert(results, {task_id, "RETRY", tostring(next_retry), vals[4] or "", vals[5] or "0", tostring(next_run_at), vals[6] or ""})
        else
            redis.call("HSET", task_key, "status", "DEAD", "finished_at", now_ms, "receipt", timeout_receipt)
            redis.call("RPUSH", KEYS[2], task_id)
            redis.call("EXPIRE", task_key, hash_ttl_sec)
            table.insert(results, {task_id, "DEAD", tostring(retry_count), vals[4] or "", vals[5] or "0", tostring(now_ms), vals[6] or ""})
        end
    end
end

return results
