-- KEYS: lease queue, task hash, worker activity
-- ARGV: task ID, queue, service, worker ID, generation, now, extension, default lease,
--       activity TTL, activity JSON
local task_id, now_ms = ARGV[1], tonumber(ARGV[6])
local owned = lease_status(KEYS[1], KEYS[2], task_id, ARGV[2], ARGV[3], ARGV[4], ARGV[5], now_ms)
if owned ~= 1 then return owned end

local lease_ms = tonumber(ARGV[7])
if lease_ms <= 0 then
    lease_ms = (tonumber(redis.call("HGET", KEYS[2], "lease_ttl")) or 0) * 1000
    if lease_ms <= 0 then lease_ms = tonumber(ARGV[8]) end
end

local expire_at = now_ms + lease_ms
redis.call("ZADD", KEYS[1], expire_at, task_id)
redis.call("SET", KEYS[3], ARGV[10], "EX", ARGV[9])
return expire_at
