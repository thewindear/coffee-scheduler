-- KEYS[1]: cs:lease:{queue}
-- KEYS[2]: cs:task:{task_id}
-- ARGV[1]: task_id
-- ARGV[2]: now_ms
-- ARGV[3]: hash_ttl_sec
-- ARGV[4]: receipt_json
-- ARGV[5]: worker_id
local task_id = ARGV[1]
local now_ms = tonumber(ARGV[2])
local hash_ttl_sec = tonumber(ARGV[3])
local receipt_json = ARGV[4]
local worker_id = ARGV[5]

local status = redis.call("HGET", KEYS[2], "status")
if not status or status ~= "RUNNING" then
    return 0
end

redis.call("ZREM", KEYS[1], task_id)
redis.call("HSET", KEYS[2], "status", "SUCCEEDED", "finished_at", now_ms, "receipt", receipt_json, "worker_id", worker_id)
redis.call("EXPIRE", KEYS[2], hash_ttl_sec)
return 1
