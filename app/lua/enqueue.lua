-- KEYS: task hash, ready ZSET, delay ZSET, active queues, DLQ, optional idempotency key
-- ARGV: prefix, task ID, queue, priority, now, scheduled time, mode, TTL, hash fields...
local prefix, task_id, queue = ARGV[1], ARGV[2], ARGV[3]
local priority, now_ms, scheduled_ms = tonumber(ARGV[4]), tonumber(ARGV[5]), tonumber(ARGV[6])

if ARGV[7] == "retry" then
    local status = redis.call("HGET", KEYS[1], "status")
    if status and status ~= "DEAD" and status ~= "SUCCEEDED" then
        return {task_id, status, 0}
    end
    redis.call("LREM", KEYS[5], 0, task_id)
    redis.call("PERSIST", KEYS[1])
    redis.call("HDEL", KEYS[1], "worker_id", "pulled_at", "finished_at")
elseif KEYS[6] then
    local original = redis.call("GET", KEYS[6])
    if original then
        local status = redis.call("HGET", prefix .. ":task:" .. original, "status")
        return {original, status or "UNKNOWN", 0}
    end
end

local status = "READY"
if scheduled_ms > now_ms then
    status = "DELAYED"
    redis.call("ZADD", KEYS[3], ARGV[6], task_id)
else
    enqueue_ready(KEYS[2], task_id, priority, now_ms)
end
redis.call("HSET", KEYS[1], "status", status, unpack(ARGV, 9))
redis.call("SADD", KEYS[4], queue)
if KEYS[6] then
    redis.call("SET", KEYS[6], task_id, "NX", "EX", ARGV[8])
end
return {task_id, status, 1}
