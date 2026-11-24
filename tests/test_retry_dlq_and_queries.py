import asyncio
import time


async def test_nack_retry_dlq_and_batch_retry(app_bundle):
    app, client, _, _ = app_bundle

    # Submit task with max_retries=1
    r = await client.post(
        "/api/v1/tasks",
        json={"queue": "dlq_q", "payload": {"url": "bad"}, "max_retries": 1},
    )
    tid = r.json()["task_id"]

    # First pull -> nack(retryable=True) -> RETRY
    await client.post("/api/v1/tasks/pull", json={"queue": "dlq_q", "worker_id": "w1", "batch_size": 1})
    n1 = await client.post(
        f"/api/v1/tasks/{tid}/nack",
        json={
            "queue": "dlq_q",
            "worker_id": "w1",
            "error_code": "TIMEOUT",
            "error_message": "upstream 504",
            "retryable": True,
        },
    )
    assert n1.status_code == 200
    assert n1.json()["outcome"] == "RETRY"
    assert n1.json()["retry_count"] == 1

    # Pump retry back to ready
    await asyncio.sleep(0.03)
    await app.state.scheduler.pump_once()

    # Second pull -> nack -> DEAD
    await client.post("/api/v1/tasks/pull", json={"queue": "dlq_q", "worker_id": "w1", "batch_size": 1})
    n2 = await client.post(
        f"/api/v1/tasks/{tid}/nack",
        json={
            "queue": "dlq_q",
            "worker_id": "w1",
            "error_code": "TIMEOUT",
            "error_message": "still 504",
            "retryable": True,
        },
    )
    assert n2.json()["outcome"] == "DEAD"

    # Fast-fail task (retryable=False) directly into DEAD
    r_fast = await client.post(
        "/api/v1/tasks",
        json={"queue": "dlq_q", "payload": {"url": "404"}, "max_retries": 5},
    )
    tid_fast = r_fast.json()["task_id"]
    await client.post("/api/v1/tasks/pull", json={"queue": "dlq_q", "worker_id": "w1", "batch_size": 1})
    nf = await client.post(
        f"/api/v1/tasks/{tid_fast}/nack",
        json={
            "queue": "dlq_q",
            "worker_id": "w1",
            "error_code": "NOT_FOUND",
            "error_message": "404",
            "retryable": False,
        },
    )
    assert nf.json()["outcome"] == "DEAD"

    await app.state.flusher.drain_once()

    # Check queue stats
    stats = (await client.get("/api/v1/queues/dlq_q/stats")).json()
    assert stats["dlq_count"] == 2
    assert stats["active_workers"] == 1

    # Batch retry DLQ
    batch_retry = await client.post("/api/v1/dlq/dlq_q/retry", json={"limit": 10})
    assert batch_retry.status_code == 200
    assert len(batch_retry.json()) == 2

    stats_after = (await client.get("/api/v1/queues/dlq_q/stats")).json()
    assert stats_after["dlq_count"] == 0
    assert stats_after["ready_count"] == 2


async def test_lease_reap_cold_hot_lookup_and_cursor_pagination(app_bundle):
    app, client, fake_redis, _ = app_bundle

    # Create 3 tasks and complete them
    tids = []
    for idx in range(3):
        r = await client.post("/api/v1/tasks", json={"queue": "page_q", "payload": {"idx": idx}})
        tid = r.json()["task_id"]
        tids.append(tid)
        await client.post("/api/v1/tasks/pull", json={"queue": "page_q", "worker_id": "w1", "batch_size": 1})
        await client.post(
            f"/api/v1/tasks/{tid}/ack",
            json={"queue": "page_q", "worker_id": "w1", "summary": {"idx": idx}},
        )

    await app.state.flusher.drain_once()

    # Delete Redis key for first task to test DB fallback via UUIDv7 partition pruning
    await fake_redis.delete(app.state.redis_mgr.task_key(tids[0]))
    db_detail = (await client.get(f"/api/v1/tasks/{tids[0]}")).json()
    assert db_detail["source"] == "db"
    assert db_detail["status"] == "SUCCEEDED"

    # Cursor pagination with limit=2
    p1 = (await client.get("/api/v1/tasks?queue=page_q&status=SUCCEEDED&limit=2")).json()
    assert len(p1["items"]) == 2
    assert p1["next_cursor"] is not None

    p2 = (
        await client.get(f"/api/v1/tasks?queue=page_q&status=SUCCEEDED&limit=2&cursor={p1['next_cursor']}")
    ).json()
    assert len(p2["items"]) == 1
    assert p2["next_cursor"] is None

    # Lease expiration reap test
    r_exp = await client.post(
        "/api/v1/tasks",
        json={"queue": "reap_q", "payload": {}, "max_retries": 0, "lease_seconds": 1},
    )
    tid_exp = r_exp.json()["task_id"]
    await client.post("/api/v1/tasks/pull", json={"queue": "reap_q", "worker_id": "w-dead", "batch_size": 1})
    # Force lease score into the past
    await fake_redis.zadd(app.state.redis_mgr.lease_key("reap_q"), {tid_exp: int(time.time() * 1000) - 1000})
    reaped = await app.state.scheduler.reap_once()
    assert reaped == 1
    dead_detail = (await client.get(f"/api/v1/tasks/{tid_exp}")).json()
    assert dead_detail["status"] == "DEAD"
    assert dead_detail["receipt"]["error_code"] == "LEASE_TIMEOUT"


async def test_cron_job_crud_and_trigger(app_bundle):
    app, client, _, _ = app_bundle

    c_resp = await client.post(
        "/api/v1/cron-jobs",
        json={
            "name": "daily-crawl",
            "queue": "cron_q",
            "priority": 8,
            "cron_expr": "* * * * *",
            "payload": {"target": "news"},
        },
    )
    assert c_resp.status_code == 201
    job_id = c_resp.json()["id"]

    # Force next_run_at into past so scan_cron_once triggers immediately
    from sqlalchemy import update

    from app.models.tables import CronJob

    async with app.state.session_factory() as session:
        await session.execute(
            update(CronJob).where(CronJob.id == job_id).values(next_run_at=int(time.time() * 1000) - 1000)
        )
        await session.commit()

    triggered = await app.state.scheduler.scan_cron_once()
    assert triggered == 1

    pulled = (
        await client.post(
            "/api/v1/tasks/pull",
            json={"queue": "cron_q", "worker_id": "w-cron", "batch_size": 5},
        )
    ).json()["tasks"]
    assert len(pulled) == 1
    assert pulled[0]["priority"] == 8
    assert pulled[0]["payload"] == {"target": "news"}
