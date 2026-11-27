local function lease_status(lease_key, task_key, task_id, queue, service, worker_id, generation, now_ms)
    local state = redis.call("HMGET", task_key, "status", "queue", "worker_service", "worker_id", "lease_generation")
    if not state[1] then return 0 end
    if state[2] ~= queue or (state[3] and state[3] ~= service) then return -1 end
    if state[1] ~= "RUNNING" or state[3] ~= service or state[4] ~= worker_id
        or state[5] ~= generation then return 0 end
    local expires_at = tonumber(redis.call("ZSCORE", lease_key, task_id))
    if not expires_at or expires_at <= now_ms then return 0 end
    return 1
end
