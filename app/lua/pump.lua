-- KEYS[1]: cs:delay:{shard}
-- ARGV[1]: prefix
-- ARGV[2]: now_ms
-- ARGV[3]: limit
local prefix = ARGV[1]
local now_ms = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])

local items = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", now_ms, "LIMIT", 0, limit)
if #items == 0 then
    return 0
end

redis.call("ZREM", KEYS[1], unpack(items))

for _, task_id in ipairs(items) do
    local task_key = prefix .. ":task:" .. task_id
    local fields = redis.call("HMGET", task_key, "queue", "priority")
    local queue = fields[1]
    if queue and queue ~= false then
        local prio = tonumber(fields[2]) or 5
        enqueue_ready(prefix .. ":ready:" .. queue, task_id, prio, now_ms)
        redis.call("SADD", prefix .. ":queues", queue)
        redis.call("HSET", task_key, "status", "READY")
    end
end

return #items
