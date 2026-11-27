-- Discard a Redis projection rejected by the database without touching a newer lease.
local state = redis.call("HMGET", KEYS[2], "status", "lease_generation")
if state[1] == "RUNNING" and state[2] == ARGV[2] then
    redis.call("ZREM", KEYS[1], ARGV[1])
    redis.call("DEL", KEYS[2])
end
