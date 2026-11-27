import asyncio
import logging
from typing import Any

from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.core.metrics import FLUSHER_BATCH_HISTOGRAM, FLUSHER_DURATION
from app.models.tables import TaskRecord

logger = logging.getLogger(__name__)


class AsyncFlusher:
    """Buffers task lifecycle events in memory and batch-upserts them to MySQL/SQLite."""

    def __init__(
        self,
        engine: AsyncEngine,
        session_factory: async_sessionmaker[AsyncSession],
        batch_size: int = 500,
        flush_interval_ms: int = 100,
    ) -> None:
        self.engine = engine
        self.session_factory = session_factory
        self.batch_size = batch_size
        self.flush_interval_s = flush_interval_ms / 1000.0
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=50000)
        self._pending: list[dict[str, Any]] = []
        self._flush_lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def enqueue(self, record: dict[str, Any]) -> None:
        await self.queue.put(record)
        self._wake.set()

    async def flush_batch(self, records: list[dict[str, Any]]) -> None:
        if not records:
            return
        # Deduplicate within batch by (task_id, created_at), keeping latest state
        merged: dict[tuple[str, Any], dict[str, Any]] = {}
        for rec in records:
            key = (rec["task_id"], rec["created_at"])
            merged[key] = rec

        rows = [
            rec for rec in merged.values() if rec.get("task_kind", "normal") == "normal"
        ]
        if not rows:
            return
        FLUSHER_BATCH_HISTOGRAM.observe(len(rows))

        update_cols = [
            "status",
            "priority",
            "payload",
            "receipt",
            "retry_count",
            "worker_id",
            "scheduled_at",
            "pulled_at",
            "finished_at",
        ]
        dialect = self.engine.dialect.name
        with FLUSHER_DURATION.time():
            async with self.session_factory() as session:
                if dialect == "mysql":
                    from sqlalchemy import case

                    stmt = mysql_insert(TaskRecord).values(rows)
                    is_protected = (TaskRecord.task_kind != "normal") | (
                        TaskRecord.status.in_(["SUCCEEDED", "DEAD"])
                    )
                    upsert_stmt = stmt.on_duplicate_key_update(
                        **{
                            col: case(
                                (is_protected, getattr(TaskRecord, col)),
                                else_=stmt.inserted[col],
                            )
                            for col in update_cols
                        }
                    )
                else:
                    stmt = sqlite_insert(TaskRecord).values(rows)
                    upsert_stmt = stmt.on_conflict_do_update(
                        index_elements=["task_id", "created_at"],
                        set_={col: getattr(stmt.excluded, col) for col in update_cols},
                        where=(TaskRecord.task_kind == "normal")
                        & TaskRecord.status.notin_(["SUCCEEDED", "DEAD"]),
                    )
                await session.execute(upsert_stmt)
                await session.commit()

    async def drain_once(self) -> int:
        async with self._flush_lock:
            if not self._pending:
                for _ in range(min(self.batch_size, self.queue.qsize())):
                    self._pending.append(self.queue.get_nowait())
            await self.flush_batch(self._pending)
            count = len(self._pending)
            self._pending.clear()
            return count

    async def _loop(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._stop_event.is_set() or not self.queue.empty() or self._pending:
            if self.queue.empty() and not self._pending:
                self._wake.clear()
                await self._wake.wait()
                if self.queue.empty():
                    continue

            deadline = loop.time() + self.flush_interval_s
            while (
                not self._pending
                and self.queue.qsize() < self.batch_size
                and not self._stop_event.is_set()
            ):
                self._wake.clear()
                try:
                    async with asyncio.timeout_at(deadline):
                        await self._wake.wait()
                except TimeoutError:
                    break

            try:
                await self.drain_once()
            except Exception:
                if self._stop_event.is_set():
                    raise
                logger.exception("AsyncFlusher will retry %d records", len(self._pending))
                await asyncio.sleep(self.flush_interval_s)

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop_event.clear()
            self._task = asyncio.create_task(self._loop(), name="async-flusher")

    async def stop(self) -> None:
        self._stop_event.set()
        self._wake.set()
        if self._task is not None:
            await self._task
            self._task = None
