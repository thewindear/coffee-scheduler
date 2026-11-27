-- KEYS[1]: cs:lease:{queue}
-- KEYS[2]: cs:task:{task_id}
-- ARGV[1]: task_id
-- ARGV[2]: now_ms
-- ARGV[3]: hash_ttl_sec
-- ARGV[4]: receipt_json
-- ARGV[5]: worker_id
-- ARGV[6]: expected_generation (optional, "" to skip)
local task_id = ARGV[1]
local now_ms = tonumber(ARGV[2])
local hash_ttl_sec = tonumber(ARGV[3])
local receipt_json = ARGV[4]
local worker_id = ARGV[5]
local expected_gen = ARGV[6]

local state = redis.call("HMGET", KEYS[2], "status", "worker_id", "lease_generation")
local expires_at = tonumber(redis.call("ZSCORE", KEYS[1], task_id))
if state[1] ~= "RUNNING" or state[2] ~= worker_id or not expires_at or expires_at <= now_ms then
    return 0
end
if expected_gen and expected_gen ~= "" then
    local current_gen = tonumber(state[3] or "0")
    if current_gen ~= tonumber(expected_gen) then
        return 0
    end
end

redis.call("ZREM", KEYS[1], task_id)
redis.call("HSET", KEYS[2], "status", "SUCCEEDED", "finished_at", now_ms, "receipt", receipt_json, "worker_id", worker_id)
redis.call("EXPIRE", KEYS[2], hash_ttl_sec)
return 1
