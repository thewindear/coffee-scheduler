-- KEYS: lease queue, task hash, delay shard, dead letter queue
-- ARGV: task ID, queue, service, worker ID, generation, now, retryable, retry delay, hash TTL, receipt
local task_id, worker_id, now_ms = ARGV[1], ARGV[4], tonumber(ARGV[6])
local retryable, base_delay_ms, hash_ttl_sec, receipt_json = ARGV[7], tonumber(ARGV[8]), tonumber(ARGV[9]), ARGV[10]
local owned = lease_status(KEYS[1], KEYS[2], task_id, ARGV[2], ARGV[3], worker_id, ARGV[5], now_ms)
if owned ~= 1 then
    return {owned == -1 and "FORBIDDEN" or "IGNORED", 0, 0}
end
local vals = redis.call("HMGET", KEYS[2], "retry_count", "max_retries")
local retry_count = tonumber(vals[1]) or 0
local max_retries = tonumber(vals[2]) or 3

redis.call("ZREM", KEYS[1], task_id)

if retryable == "1" and retry_count < max_retries then
    local next_retry = retry_count + 1
    local backoff = base_delay_ms * (2 ^ (next_retry - 1))
    local next_run_at = now_ms + backoff
    redis.call("HSET", KEYS[2], "status", "DELAYED", "retry_count", next_retry, "receipt", receipt_json, "worker_id", worker_id)
    redis.call("ZADD", KEYS[3], next_run_at, task_id)
    return {"RETRY", next_retry, next_run_at}
else
    redis.call("HSET", KEYS[2], "status", "DEAD", "finished_at", now_ms, "receipt", receipt_json, "worker_id", worker_id)
    redis.call("RPUSH", KEYS[4], task_id)
    redis.call("EXPIRE", KEYS[2], hash_ttl_sec)
    return {"DEAD", retry_count, now_ms}
end
