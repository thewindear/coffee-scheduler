import asyncio
import logging
import time
from contextlib import suppress
from datetime import UTC, datetime

from croniter import croniter
from redis.exceptions import LockNotOwnedError
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.core.db import ensure_task_records_partitions
from app.core.metrics import TASKS_NACKED
from app.core.redis_client import RedisLuaManager
from app.models.tables import CronJob
from app.schemas.tasks import TaskSubmitRequest
from app.services.dispatcher import TaskDispatcher, _decode
from app.services.flusher import AsyncFlusher

logger = logging.getLogger(__name__)
CRON_LOCK_SECONDS = 5


class SchedulerEngine:
    """Background engine running Delay Pump, Lease Reaper, Cron Scanner, and Partition Maintainer."""

    def __init__(
        self,
        redis_mgr: RedisLuaManager,
        dispatcher: TaskDispatcher,
        flusher: AsyncFlusher,
        engine: AsyncEngine,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
    ) -> None:
        self.redis_mgr = redis_mgr
        self.redis = redis_mgr.redis
        self.dispatcher = dispatcher
        self.flusher = flusher
        self.engine = engine
        self.session_factory = session_factory
        self.settings = settings
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []

    async def pump_once(self) -> int:
        now_ms = int(time.time() * 1000)
        script = self.redis_mgr.get_script("pump")
        total = 0
        for shard in range(self.settings.delay_shard_count):
            moved = await script(
                keys=[self.redis_mgr.delay_key(shard)],
                args=[self.redis_mgr.prefix, str(now_ms), str(self.settings.pump_batch_size)],
            )
            total += int(moved)
        return total

    async def reap_once(self) -> int:
        now_ms = int(time.time() * 1000)
        queues = await self.redis.smembers(self.redis_mgr.active_queues_key())
        if not queues:
            return 0
        script = self.redis_mgr.get_script("reap_lease")
        reaped_total = 0
        for q_raw in queues:
            queue = _decode(q_raw)
            rows = await script(
                keys=[self.redis_mgr.lease_key(queue), self.redis_mgr.dlq_key(queue)],
                args=[
                    self.redis_mgr.prefix,
                    queue,
                    str(now_ms),
                    str(self.settings.pump_batch_size),
                    str(self.settings.retry_base_delay_ms),
                    str(self.settings.task_hash_ttl_seconds),
                    str(self.settings.delay_shard_count),
                ],
            )
            for row in rows or []:
                reaped_total += 1
                tid = _decode(row[0])
                outcome = _decode(row[1])
                TASKS_NACKED.labels(queue=queue, outcome=outcome).inc()
                rc = int(_decode(row[2]))
                wid = _decode(row[3]) or "unknown"
                if outcome == "DEAD":
                    snapshot = await self.redis.hgetall(self.redis_mgr.task_key(tid))
                    await self.dispatcher._enqueue_terminal_flush(
                        tid,
                        snapshot,
                        "DEAD",
                        {"error_code": "LEASE_TIMEOUT", "error_message": "Worker lease expired without heartbeat or ack"},
                        wid,
                        now_ms,
                        retry_count=rc,
                    )
        return reaped_total

    async def scan_cron_once(self) -> int:
        lock_key = f"{self.redis_mgr.prefix}:lock:cron_scanner"
        lock = self.redis.lock(lock_key, timeout=CRON_LOCK_SECONDS)
        if not await lock.acquire(blocking=False):
            return 0

        finished = asyncio.Event()

        async def renew_lock() -> None:
            while not finished.is_set():
                try:
                    await asyncio.wait_for(finished.wait(), timeout=CRON_LOCK_SECONDS / 3)
                except TimeoutError:
                    await lock.reacquire()

        try:
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(renew_lock())
                try:
                    return await self._scan_due_jobs()
                finally:
                    finished.set()
        finally:
            with suppress(LockNotOwnedError):
                await lock.release()

    async def _scan_due_jobs(self) -> int:
        now_ms = int(time.time() * 1000)
        now_dt = datetime.fromtimestamp(now_ms / 1000.0, tz=UTC)
        triggered = 0
        async with self.session_factory() as session:
            stmt = select(CronJob).where(CronJob.enabled.is_(True), CronJob.next_run_at <= now_ms)
            jobs = (await session.execute(stmt)).scalars().all()
            for job in jobs:
                next_dt = croniter(job.cron_expr, now_dt).get_next(datetime)
                next_ms = int(next_dt.timestamp() * 1000)
                upd = (
                    update(CronJob)
                    .where(CronJob.id == job.id, CronJob.version == job.version)
                    .values(next_run_at=next_ms, version=job.version + 1)
                )
                res = await session.execute(upd)
                if res.rowcount == 1:
                    await self.dispatcher.submit_task(
                        TaskSubmitRequest(
                            queue=job.queue,
                            payload=job.payload,
                            priority=job.priority,
                            max_retries=job.max_retries,
                            lease_seconds=job.lease_seconds,
                        ),
                        cron_occurrence=(job.id, job.next_run_at),
                    )
                    await session.commit()
                    triggered += 1
                else:
                    await session.rollback()
        return triggered

    async def _run_periodic(self, name: str, interval_ms: int, coro_fn) -> None:
        interval_s = interval_ms / 1000.0
        while not self._stop_event.is_set():
            try:
                await coro_fn()
            except Exception:
                logger.exception("Scheduler loop %s error", name)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval_s)
            except TimeoutError:
                pass

    async def _maintain_partitions(self) -> None:
        await ensure_task_records_partitions(self.engine, days_ahead=self.settings.partition_precreate_days)

    def start(self) -> None:
        if self._tasks:
            return
        self._stop_event.clear()
        self._tasks = [
            asyncio.create_task(
                self._run_periodic("pump", self.settings.pump_interval_ms, self.pump_once),
                name="sched-pump",
            ),
            asyncio.create_task(
                self._run_periodic("reap", self.settings.lease_reap_interval_ms, self.reap_once),
                name="sched-reap",
            ),
            asyncio.create_task(
                self._run_periodic("cron", self.settings.cron_scan_interval_ms, self.scan_cron_once),
                name="sched-cron",
            ),
            asyncio.create_task(
                self._run_periodic("partitions", 3600_000, self._maintain_partitions),
                name="sched-partitions",
            ),
        ]

    async def stop(self) -> None:
        self._stop_event.set()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()
