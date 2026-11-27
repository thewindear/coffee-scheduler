-- KEYS: lease queue, task hash
-- ARGV: task ID, queue, service, worker ID, generation, now, hash TTL, receipt, receipt hash,
--       "1" if the database already accepted this result
local task_id, now_ms = ARGV[1], tonumber(ARGV[6])
local owned = lease_status(KEYS[1], KEYS[2], task_id, ARGV[2], ARGV[3], ARGV[4], ARGV[5], now_ms)
if owned ~= 1 then
    if owned == -1 then return -1 end
    local state = redis.call("HMGET", KEYS[2], "status", "worker_service", "worker_id", "lease_generation", "receipt_hash")
    if state[2] ~= ARGV[3] or state[3] ~= ARGV[4] or state[4] ~= ARGV[5] then return 0 end
    if state[1] == "SUCCEEDED" and state[5] == ARGV[9] then return 2 end
    if ARGV[10] ~= "1" then return 0 end
end

redis.call("ZREM", KEYS[1], task_id)
redis.call("HSET", KEYS[2], "status", "SUCCEEDED", "finished_at", now_ms, "receipt", ARGV[8], "receipt_hash", ARGV[9])
redis.call("EXPIRE", KEYS[2], tonumber(ARGV[7]))
return 1
