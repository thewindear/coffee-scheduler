-- KEYS[1]: cs:lease:{queue}
-- KEYS[2]: cs:task:{task_id}
-- KEYS[3]: cs:delay:{shard}
-- KEYS[4]: cs:dlq:{queue}
-- ARGV[1]: task_id
-- ARGV[2]: now_ms
-- ARGV[3]: retryable ("1" or "0")
-- ARGV[4]: base_delay_ms
-- ARGV[5]: hash_ttl_sec
-- ARGV[6]: receipt_json
-- ARGV[7]: worker_id
-- ARGV[8]: expected_generation (optional, "" to skip)
local task_id = ARGV[1]
local now_ms = tonumber(ARGV[2])
local retryable = ARGV[3]
local base_delay_ms = tonumber(ARGV[4])
local hash_ttl_sec = tonumber(ARGV[5])
local receipt_json = ARGV[6]
local worker_id = ARGV[7]
local expected_gen = ARGV[8]

local vals = redis.call("HMGET", KEYS[2], "status", "retry_count", "max_retries", "worker_id", "lease_generation")
local expires_at = tonumber(redis.call("ZSCORE", KEYS[1], task_id))
if vals[1] ~= "RUNNING" or vals[4] ~= worker_id or not expires_at or expires_at <= now_ms then
    return {"IGNORED", 0, 0}
end
if expected_gen and expected_gen ~= "" then
    local current_gen = tonumber(vals[5] or "0")
    if current_gen ~= tonumber(expected_gen) then
        return {"IGNORED", 0, 0}
    end
end

local retry_count = tonumber(vals[2]) or 0
local max_retries = tonumber(vals[3]) or 3

redis.call("ZREM", KEYS[1], task_id)

if retryable == "1" and retry_count < max_retries then
    local next_retry = retry_count + 1
    local backoff = base_delay_ms * (2 ^ (next_retry - 1))
    local next_run_at = now_ms + backoff
    redis.call("HSET", KEYS[2], "status", "DELAYED", "retry_count", next_retry, "receipt", receipt_json, "worker_id", worker_id)
    redis.call("ZADD", KEYS[3], next_run_at, task_id)
    return {"RETRY", next_retry, next_run_at}
else
    redis.call("HSET", KEYS[2], "status", "DEAD", "finished_at", now_ms, "receipt", receipt_json, "worker_id", worker_id)
    redis.call("RPUSH", KEYS[4], task_id)
    redis.call("EXPIRE", KEYS[2], hash_ttl_sec)
    return {"DEAD", retry_count, now_ms}
end
