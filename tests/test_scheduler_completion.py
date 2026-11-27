import asyncio
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from prometheus_client.parser import text_string_to_metric_families
from sqlalchemy import update

from app.core.access import Identity
from app.models.tables import CronJob
from app.schemas.tasks import TaskSubmitRequest
from app.services.flusher import AsyncFlusher


async def test_scripts_are_preloaded_and_recover_after_script_flush(app_bundle):
    app, client, redis, _ = app_bundle
    manager = app.state.redis_mgr
    scripts = [
        manager.get_script(name).sha
        for name in ("enqueue", "pump", "pull", "ack", "nack", "reap_lease", "heartbeat")
    ]
    assert all(await redis.script_exists(*scripts))
    await redis.script_flush()
    response = await client.post("/api/v1/tasks", json={"queue": "reload"})
    assert response.status_code == 200
    assert await redis.script_exists(manager.get_script("enqueue").sha) == [True]


async def test_ready_fifo_with_equal_timestamps_and_legacy_scores(app_bundle, monkeypatch):
    app, client, redis, _ = app_bundle
    now_ms = int(time.time() * 1000)
    monkeypatch.setattr("app.services.dispatcher._now_ms", lambda: now_ms)
    entropy = iter((bytes([255]) * 10, bytes(10), bytes([128]) * 10))
    monkeypatch.setattr("app.core.uuidv7.os", SimpleNamespace(urandom=lambda size: next(entropy)))

    async def submit() -> str:
        return (await client.post("/api/v1/tasks", json={"queue": "fifo", "priority": 0})).json()[
            "task_id"
        ]

    legacy = await submit()
    ready_key = app.state.redis_mgr.ready_key("fifo")
    await redis.zadd(ready_key, {legacy: 90_000_000_000_000 + now_ms})
    first, second = await submit(), await submit()
    assert first < legacy and first < second
    response = await client.post("/api/v1/tasks/pull", json={"queue": "fifo", "worker_id": "w"})
    assert [task["task_id"] for task in response.json()["tasks"]] == [legacy, first, second]


async def test_concurrent_idempotent_submit_returns_one_task(app_bundle):
    app, _, redis, _ = app_bundle
    request = TaskSubmitRequest(queue="dedup", idempotency_key="once")
    identity = Identity("test-admin", None, True, ())
    results = await asyncio.gather(
        *(app.state.dispatcher.submit_task(request, identity=identity) for _ in range(12))
    )
    assert len({result.task_id for result in results}) == 1
    assert sum(not result.deduplicated for result in results) == 1
    assert await redis.zcard(app.state.redis_mgr.ready_key("dedup")) == 1


async def test_expired_lease_requeues_after_backoff(app_bundle, monkeypatch):
    app, client, redis, _ = app_bundle
    created = await client.post("/api/v1/tasks", json={"queue": "expiry", "max_retries": 1})
    task_id = created.json()["task_id"]

    async def pull(worker_id: str):
        result = await client.post(
            "/api/v1/tasks/pull", json={"queue": "expiry", "worker_id": worker_id}
        )
        return result.json()["tasks"]

    assert (await pull("initial"))[0]["task_id"] == task_id
    now_ms = int(time.time() * 1000)
    monkeypatch.setattr(
        "app.services.scheduler_loop.time", SimpleNamespace(time=lambda: now_ms / 1000)
    )
    manager = app.state.redis_mgr
    await redis.zadd(manager.lease_key("expiry"), {task_id: now_ms - 1})
    assert await app.state.scheduler.reap_once() == 1
    due = now_ms + app.state.settings.retry_base_delay_ms
    assert await redis.zscore(manager.delay_key(manager.delay_shard_for(task_id)), task_id) == due
    assert await app.state.scheduler.pump_once() == 0
    now_ms = due + 1
    assert await app.state.scheduler.pump_once() == 1
    (retried,) = await pull("replacement")
    assert (retried["task_id"], retried["retry_count"]) == (task_id, 1)
    for action in ("ack", "nack"):
        stale = await client.post(
            f"/api/v1/tasks/{task_id}/{action}",
            json={
                "queue": "expiry",
                "worker_id": "initial",
                "lease_generation": 1,
            },
        )
        assert stale.status_code == 409


@pytest.mark.parametrize("receipt", ["ack", "nack"])
async def test_receipt_limit_counts_raw_bytes_before_schema_validation(app_bundle, receipt):
    _, client, _, _ = app_bundle
    created = await client.post("/api/v1/tasks", json={"queue": "receipts"})
    await client.post("/api/v1/tasks/pull", json={"queue": "receipts", "worker_id": "w"})
    task_id = created.json()["task_id"]
    path = f"/api/v1/tasks/{task_id}/{receipt}"
    raw = b'{"queue":"receipts","worker_id":"w","lease_generation":1}'.rjust(16384)
    headers = {"content-type": "application/json"}

    async def chunks():
        yield raw
        yield b" "

    too_large = await client.post(path, content=chunks(), headers=headers)
    assert too_large.status_code == 413
    assert "artifact" in too_large.json()["detail"]
    exact_limit = await client.post(path, content=raw, headers=headers)
    assert exact_limit.status_code == 200


async def test_list_accepts_timezone_and_metrics_expose_queue_depth(app_bundle):
    app, client, _, _ = app_bundle
    await client.post("/api/v1/tasks", json={"queue": "observed", "max_retries": 0})
    start = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
    await app.state.flusher.drain_once()
    page = await client.get("/api/v1/tasks", params={"start_time": start, "queue": "observed"})
    assert len(page.json()["items"]) == 1

    async def metrics():
        response = await client.get("/metrics")
        assert response.status_code == 200
        return {
            (sample.name, sample.labels.get("queue") or sample.labels.get("handler")): sample.value
            for family in text_string_to_metric_families(response.text)
            for sample in family.samples
        }

    ready_metric = ("coffee_scheduler_queue_ready_size", "observed")
    dead_metric = ("coffee_scheduler_queue_dlq_size", "observed")
    assert (await metrics())[ready_metric] == 1
    pulled = await client.post("/api/v1/tasks/pull", json={"queue": "observed", "worker_id": "w"})
    task_id = pulled.json()["tasks"][0]["task_id"]
    await client.post(
        f"/api/v1/tasks/{task_id}/nack",
        json={"queue": "observed", "worker_id": "w", "lease_generation": 1},
    )
    stats = (await client.get("/api/v1/queues/observed/stats")).json()
    assert stats == {
        "queue": "observed",
        "ready_count": 0,
        "running_count": 0,
        "dlq_count": 1,
        "worker_count": 1,
    }
    values = await metrics()
    assert values[ready_metric] == 0
    assert values[dead_metric] == 1
    assert values[("coffee_scheduler_tasks_nacked_total", "observed")] >= 1
    assert values[("http_request_duration_seconds_count", "/api/v1/tasks/pull")] >= 1
    assert values[("coffee_scheduler_flusher_duration_seconds_count", None)] > 0


async def test_flusher_triggers_by_size_timeout_and_shutdown(monkeypatch):
    flusher = AsyncFlusher(None, None, batch_size=2, flush_interval_ms=200)
    flushed = asyncio.Queue()

    async def record_batch(batch):
        flushed.put_nowait(list(batch))

    monkeypatch.setattr(flusher, "flush_batch", record_batch)
    flusher.start()
    try:
        await flusher.enqueue({"task_id": "1"})
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(flushed.get(), timeout=0.02)
        await flusher.enqueue({"task_id": "2"})
        assert await asyncio.wait_for(flushed.get(), timeout=0.1) == [
            {"task_id": "1"},
            {"task_id": "2"},
        ]
        await flusher.enqueue({"task_id": "3"})
        assert await asyncio.wait_for(flushed.get(), timeout=1) == [{"task_id": "3"}]
        await flusher.enqueue({"task_id": "4"})
    finally:
        await flusher.stop()
    assert flushed.get_nowait() == [{"task_id": "4"}]


async def test_cron_lock_renews_and_releases(app_bundle, monkeypatch):
    app, _, _, _ = app_bundle
    scheduler = app.state.scheduler
    monkeypatch.setattr("app.services.scheduler_loop.CRON_LOCK_SECONDS", 0.15)
    started = asyncio.Event()
    finish = asyncio.Event()

    async def slow_scan():
        started.set()
        await finish.wait()
        return 3

    monkeypatch.setattr(scheduler, "_scan_due_jobs", slow_scan)
    scan = asyncio.create_task(scheduler.scan_cron_once())
    try:
        await started.wait()
        await asyncio.sleep(0.25)
        assert await asyncio.wait_for(scheduler.scan_cron_once(), timeout=0.1) == 0
    finally:
        finish.set()
        assert await scan == 3
    assert await scheduler.scan_cron_once() == 3


async def test_cron_retry_after_submission_does_not_duplicate_tasks(app_bundle, monkeypatch):
    app, client, redis, _ = app_bundle
    created = await client.post(
        "/api/v1/cron-jobs",
        json={"name": "retry-cron", "queue": "cron-retry", "cron_expr": "* * * * *"},
    )
    assert created.status_code == 201
    async with app.state.session_factory() as session:
        await session.execute(
            update(CronJob).where(CronJob.id == created.json()["id"]).values(next_run_at=1)
        )
        await session.commit()

    submit_task = app.state.dispatcher.submit_task

    async def submit_then_fail(*args, **kwargs):
        await submit_task(*args, **kwargs)
        raise RuntimeError("Cron update interrupted after enqueue")

    with monkeypatch.context() as patch:
        patch.setattr(app.state.dispatcher, "submit_task", submit_then_fail)
        with pytest.raises(ExceptionGroup) as error:
            await app.state.scheduler.scan_cron_once()
        assert isinstance(error.value.exceptions[0], RuntimeError)
    assert await app.state.scheduler.scan_cron_once() == 1
    assert await app.state.scheduler.scan_cron_once() == 0
    assert await redis.zcard(app.state.redis_mgr.ready_key("cron-retry")) == 1


async def test_flusher_retries_failed_batch_before_later_records(monkeypatch):
    flusher = AsyncFlusher(None, None, batch_size=1, flush_interval_ms=20)
    attempted = asyncio.Queue()

    async def record_batch(batch):
        if batch:
            attempted.put_nowait(batch[0]["task_id"])
            if attempted.qsize() == 1:
                raise RuntimeError("temporary database failure")

    monkeypatch.setattr(flusher, "flush_batch", record_batch)
    flusher.start()
    try:
        await flusher.enqueue({"task_id": "1"})
        await flusher.enqueue({"task_id": "2"})
        async with asyncio.timeout(1):
            while attempted.qsize() < 3:
                await asyncio.sleep(0.01)
    finally:
        await flusher.stop()
    assert [attempted.get_nowait() for _ in range(3)] == ["1", "1", "2"]
