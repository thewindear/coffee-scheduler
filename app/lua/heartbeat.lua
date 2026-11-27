-- KEYS[1]: cs:lease:{queue}
-- KEYS[2]: cs:task:{task_id}
-- KEYS[3]: cs:worker:{queue}:{worker_id}
-- ARGV[1]: task_id
-- ARGV[2]: worker_id
-- ARGV[3]: now_ms
-- ARGV[4]: extend_ms (0 = use task lease_ttl)
-- ARGV[5]: default_lease_ms
-- ARGV[6]: worker_ttl_sec
-- ARGV[7]: expected_generation (optional, "" to skip)
local task_id = ARGV[1]
local worker_id = ARGV[2]
local now_ms = tonumber(ARGV[3])
local extend_ms = tonumber(ARGV[4])
local default_lease_ms = tonumber(ARGV[5])
local worker_ttl_sec = tonumber(ARGV[6])
local expected_gen = ARGV[7]

local status = redis.call("HGET", KEYS[2], "status")
if status ~= "RUNNING" then
    return 0
end

local current_worker = redis.call("HGET", KEYS[2], "worker_id")
if current_worker ~= worker_id then
    return -1
end

if expected_gen and expected_gen ~= "" then
    local current_gen = tonumber(redis.call("HGET", KEYS[2], "lease_generation") or "0")
    if current_gen ~= tonumber(expected_gen) then
        return 0
    end
end

local expires_at = tonumber(redis.call("ZSCORE", KEYS[1], task_id))
if not expires_at or expires_at <= now_ms then
    return 0
end

local lease_ms = extend_ms
if not lease_ms or lease_ms <= 0 then
    local lease_sec = tonumber(redis.call("HGET", KEYS[2], "lease_ttl"))
    if lease_sec and lease_sec > 0 then
        lease_ms = lease_sec * 1000
    else
        lease_ms = default_lease_ms
    end
end

local expire_at = now_ms + lease_ms
redis.call("ZADD", KEYS[1], expire_at, task_id)
redis.call("SET", KEYS[3], now_ms, "EX", worker_ttl_sec)
return expire_at
