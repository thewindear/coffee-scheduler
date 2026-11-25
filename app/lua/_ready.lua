local function enqueue_ready(key, task_id, priority, now_ms)
    local band = (9 - priority) * 10000000000000
    local score = band + now_ms
    local last = redis.call("ZREVRANGEBYSCORE", key, "(" .. (band + 10000000000000),
        band, "WITHSCORES", "LIMIT", 0, 1)
    if #last > 0 then
        -- 1/64 is representable across all priority bands and preserves existing scores.
        score = math.max(score, tonumber(last[2]) + 1 / 64)
    end
    redis.call("ZADD", key, string.format("%.6f", score), task_id)
end
