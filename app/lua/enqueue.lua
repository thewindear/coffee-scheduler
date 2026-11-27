-- KEYS: task hash, ready ZSET, delay ZSET, active queues, DLQ, optional idempotency key
-- ARGV: prefix, task ID, queue, priority, now, scheduled time, mode, TTL, hash fields...
local prefix, task_id, queue = ARGV[1], ARGV[2], ARGV[3]
local priority, now_ms, scheduled_ms = tonumber(ARGV[4]), tonumber(ARGV[5]), tonumber(ARGV[6])

if ARGV[7] == "retry" then
    local status = redis.call("HGET", KEYS[1], "status")
    if status and status ~= "DEAD" and status ~= "SUCCEEDED" then
        return {task_id, status, 0}
    end
    local current_gen = redis.call("HGET", KEYS[1], "lease_generation")
    if current_gen then
        for i = 9, #ARGV, 2 do
            if ARGV[i] == "lease_generation" and tonumber(current_gen) + 1 ~= tonumber(ARGV[i + 1]) then
                return {task_id, status, 0}
            end
        end
    end
    redis.call("LREM", KEYS[5], 0, task_id)
    redis.call("PERSIST", KEYS[1])
    redis.call("HDEL", KEYS[1], "worker_id", "worker_service", "pulled_at", "finished_at", "receipt_hash")
elseif ARGV[7] == "project" then
    local existing = redis.call("HGET", KEYS[1], "status")
    if existing then
        if existing == "READY" and not redis.call("ZSCORE", KEYS[2], task_id) then
            local old_priority = tonumber(redis.call("HGET", KEYS[1], "priority")) or priority
            enqueue_ready(KEYS[2], task_id, old_priority, now_ms)
            return {task_id, existing, 1}
        end
        return {task_id, existing, 0}
    end
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
