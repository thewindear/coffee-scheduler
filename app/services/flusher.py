import asyncio
import logging
from typing import Any

from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.core.metrics import FLUSHER_BATCH_HISTOGRAM
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
        self._flush_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def enqueue(self, record: dict[str, Any]) -> None:
        try:
            self.queue.put_nowait(record)
        except asyncio.QueueFull:
            logger.warning("AsyncFlusher queue full; dropping record task_id=%s", record.get("task_id"))

    async def flush_batch(self, records: list[dict[str, Any]]) -> None:
        if not records:
            return
        # Deduplicate within batch by (task_id, created_at), keeping latest state
        merged: dict[tuple[str, Any], dict[str, Any]] = {}
        for rec in records:
            key = (rec["task_id"], rec["created_at"])
            if key in merged:
                merged[key].update({k: v for k, v in rec.items() if v is not None})
            else:
                merged[key] = dict(rec)

        rows = list(merged.values())
        FLUSHER_BATCH_HISTOGRAM.observe(len(rows))

        update_cols = [
            "status",
            "priority",
            "payload",
            "receipt",
            "retry_count",
            "worker_id",
            "pulled_at",
            "finished_at",
        ]
        dialect = self.engine.dialect.name
        async with self._flush_lock:
            async with self.session_factory() as session:
                if dialect == "mysql":
                    stmt = mysql_insert(TaskRecord).values(rows)
                    upsert_stmt = stmt.on_duplicate_key_update(
                        **{col: stmt.inserted[col] for col in update_cols}
                    )
                else:
                    stmt = sqlite_insert(TaskRecord).values(rows)
                    upsert_stmt = stmt.on_conflict_do_update(
                        index_elements=["task_id", "created_at"],
                        set_={col: getattr(stmt.excluded, col) for col in update_cols},
                    )
                await session.execute(upsert_stmt)
                await session.commit()

    async def drain_once(self) -> int:
        batch: list[dict[str, Any]] = []
        while len(batch) < self.batch_size:
            try:
                batch.append(self.queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if batch:
            await self.flush_batch(batch)
        else:
            # Wait for any in-flight background flush_batch to finish committing
            async with self._flush_lock:
                pass
        return len(batch)

    async def _loop(self) -> None:
        while not self._stop_event.is_set() or not self.queue.empty():
            batch: list[dict[str, Any]] = []
            try:
                first = await asyncio.wait_for(self.queue.get(), timeout=self.flush_interval_s)
                batch.append(first)
            except TimeoutError:
                continue

            while len(batch) < self.batch_size:
                try:
                    batch.append(self.queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            try:
                await self.flush_batch(batch)
            except Exception:
                logger.exception("AsyncFlusher failed to flush batch of %d records", len(batch))

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop_event.clear()
            self._task = asyncio.create_task(self._loop(), name="async-flusher")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            await self._task
            self._task = None
