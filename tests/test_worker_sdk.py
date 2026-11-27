import asyncio
import time

import httpx
import pytest
from coffee_scheduler_worker import SchedulerClient, Worker


class ObservedTransport(httpx.AsyncBaseTransport):
    def __init__(self, app):
        self.asgi = httpx.ASGITransport(app=app)
        self.requests = []
        self.lose_ack = False

    async def handle_async_request(self, request):
        self.requests.append((request.url.path, await request.aread()))
        response = await self.asgi.handle_async_request(request)
        if self.lose_ack and request.url.path.endswith("/ack"):
            self.lose_ack = False
            assert response.status_code == 200
            raise httpx.ReadError("ACK response lost after commit", request=request)
        return response


def sdk(app, http, transport):
    return SchedulerClient(
        "http://test", http.headers["Authorization"].split()[1], transport=transport
    )


async def test_fixed_lease_15_minutes_and_lost_ack_response(app_bundle, monkeypatch):
    app, http, _, _ = app_bundle
    transport = ObservedTransport(app)
    now_ms = [int(time.time() * 1000)]
    monkeypatch.setattr("app.services.dispatcher._now_ms", lambda: now_ms[0])
    start = now_ms[0]
    async with sdk(app, http, transport) as scheduler:
        task = await scheduler.submit(queue="fixed", lease_seconds=20 * 60)
        (pulled,) = await scheduler.pull("fixed", "fixed-1")
        assert pulled.task_id == task["task_id"]
        assert pulled.lease_expire_at_ms - start == 20 * 60 * 1000

        async def handler(job):
            now_ms[0] += 15 * 60 * 1000
            return {"summary": {"minutes": 15}}

        transport.lose_ack = True
        result = await Worker(scheduler, "fixed", "fixed-1").execute(pulled, handler)
        assert result["status"] == "SUCCEEDED" and result["replayed"] is True
        ack = [body for path, body in transport.requests if path.endswith("/ack")]
        assert len(ack) == 2 and ack[0] == ack[1]
        assert not any(path.endswith("/heartbeat") for path, _ in transport.requests)


async def test_blocked_IO_cancels_on_deadline_and_other_task_renews_independently(app_bundle):
    app, http, _, _ = app_bundle
    transport = ObservedTransport(app)
    operation_closed = asyncio.Event()

    async def external_server(request):
        try:
            await asyncio.Event().wait()
        finally:
            operation_closed.set()

    async with (
        sdk(app, http, transport) as scheduler,
        httpx.AsyncClient(transport=httpx.MockTransport(external_server)) as dependency,
    ):
        slow = await scheduler.submit(queue="concurrent", lease_seconds=1, payload={"slow": True})
        fast = await scheduler.submit(queue="concurrent", lease_seconds=1)
        worker_id = "shared-poller"
        tasks = await scheduler.pull("concurrent", worker_id, batch_size=2)
        worker = Worker(
            scheduler,
            "concurrent",
            worker_id,
            capacity=2,
            auto_renew=True,
            execution_timeout=lambda task: 0.9 if task.payload.get("slow") else 2,
        )

        async def handler(job):
            if job.payload.get("slow"):
                await dependency.get("https://dependency.test/report")
            await asyncio.sleep(1.1)
            return {"summary": {"complete": True}}

        results = await asyncio.gather(*(worker.execute(task, handler) for task in tasks))
        outcomes = {task.task_id: result for task, result in zip(tasks, results, strict=True)}
        assert operation_closed.is_set()
        assert outcomes[slow["task_id"]]["outcome"] == "RETRY"
        assert outcomes[fast["task_id"]]["status"] == "SUCCEEDED"
        heartbeats = {task.task_id: 0 for task in tasks}
        for path, _ in transport.requests:
            if path.endswith("/heartbeat"):
                heartbeats[path.split("/")[-2]] += 1
        assert all(count >= 2 for count in heartbeats.values())
        count = len(transport.requests)
        await asyncio.sleep(0.4)
        assert len(transport.requests) == count


async def test_lost_lease_cancels_inflight_work_without_receipt(app_bundle):
    app, http, redis, _ = app_bundle
    transport = ObservedTransport(app)
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def long_running(task):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async with sdk(app, http, transport) as scheduler:
        await scheduler.submit(queue="lost", lease_seconds=1)
        (task,) = await scheduler.pull("lost", "former")
        worker = Worker(scheduler, "lost", "former", auto_renew=True)
        runner = asyncio.create_task(worker.execute(task, long_running))
        await asyncio.wait_for(started.wait(), 2)
        await redis.zadd(app.state.redis_mgr.lease_key("lost"), {task.task_id: 1})
        assert await asyncio.wait_for(runner, 2) is None
        assert cancelled.is_set()
        assert any(path.endswith("/heartbeat") for path, _ in transport.requests)
        assert not any(path.endswith(("/ack", "/nack")) for path, _ in transport.requests)


async def test_worker_polls_empty_and_never_pulls_more_than_free_slots(app_bundle):
    app, http, _, _ = app_bundle
    transport = ObservedTransport(app)
    release = asyncio.Event()
    two_running = asyncio.Event()
    completed = asyncio.Event()
    running = 0
    maximum = 0
    seen = []

    async def handler(task):
        nonlocal running, maximum
        running += 1
        maximum = max(maximum, running)
        if running == 2:
            two_running.set()
        await release.wait()
        running -= 1
        seen.append(task.task_id)
        if len(seen) == 3:
            completed.set()
        return {}

    async with sdk(app, http, transport) as scheduler:
        worker = Worker(scheduler, "bounded", "pool", capacity=2, poll_interval=0.04)
        running_task = asyncio.create_task(worker.run(handler))
        try:
            async with asyncio.timeout(3):
                while not any(path.endswith("/pull") for path, _ in transport.requests):
                    await asyncio.sleep(0.01)
                for _ in range(3):
                    await scheduler.submit(queue="bounded")
                await two_running.wait()
                pulls = sum(path.endswith("/pull") for path, _ in transport.requests)
                await asyncio.sleep(0.1)
                assert sum(path.endswith("/pull") for path, _ in transport.requests) == pulls
                release.set()
                await completed.wait()
                while sum(path.endswith("/ack") for path, _ in transport.requests) != 3:
                    await asyncio.sleep(0.01)
        finally:
            running_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await running_task
        assert maximum == 2 and len(set(seen)) == 3
