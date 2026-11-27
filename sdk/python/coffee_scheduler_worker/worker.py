import asyncio
import time
from collections.abc import Awaitable, Callable

import httpx

from .client import SchedulerClient, Task, lease_lost, transient

Handler = Callable[[Task], Awaitable[dict | None]]


class JobError(Exception):
    def __init__(self, code: str, message: str, *, retryable: bool = True) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


class Worker:
    """Pull only into available slots and supervise each handler's lease and deadline."""

    def __init__(
        self,
        client: SchedulerClient,
        queue: str,
        worker_id: str,
        *,
        capacity: int = 1,
        poll_interval: float = 0.5,
        auto_renew: bool = False,
        extend_seconds: int | None = None,
        execution_timeout: float | Callable[[Task], float] | None = None,
    ) -> None:
        if capacity < 1 or poll_interval <= 0:
            raise ValueError("Capacity and polling interval must be positive")
        if extend_seconds is not None and not 1 <= extend_seconds <= 86400:
            raise ValueError("Heartbeat extension must be between 1 and 86400 seconds")
        self.client = client
        self.queue = queue
        self.worker_id = worker_id
        self.capacity = capacity
        self.poll_interval = poll_interval
        self.auto_renew = auto_renew
        self.extend_seconds = extend_seconds
        self.execution_timeout = execution_timeout

    async def _watch_lease(self, task: Task) -> None:
        expires = task.lease_expire_at_ms
        deadline = asyncio.get_running_loop().time() + max(0, (expires - time.time() * 1000) / 1000)
        if not self.auto_renew:
            await asyncio.sleep(max(0, deadline - asyncio.get_running_loop().time()))
            return
        loop = asyncio.get_running_loop()
        while (remaining := deadline - loop.time()) > 0:
            await asyncio.sleep(remaining / 3)
            while loop.time() < deadline:
                try:
                    async with asyncio.timeout_at(deadline):
                        receipt = await self.client.heartbeat(
                            task, self.worker_id, extend_seconds=self.extend_seconds
                        )
                    expires = receipt["lease_expire_at_ms"]
                    deadline = loop.time() + max(0, (expires - time.time() * 1000) / 1000)
                    break
                except TimeoutError:
                    return
                except httpx.HTTPError as exc:
                    if lease_lost(exc):
                        return
                    if not transient(exc):
                        raise
                    await asyncio.sleep(min(0.1, max(0, (deadline - loop.time()) / 3)))

    async def execute(self, task: Task, handler: Handler) -> dict | None:
        """Run one task. A lost lease cancels the handler and yields no receipt."""
        timeout = (
            self.execution_timeout(task)
            if callable(self.execution_timeout)
            else self.execution_timeout
        )

        async def work() -> tuple[dict, JobError | None]:
            try:
                async with asyncio.timeout(timeout):
                    return await handler(task) or {}, None
            except TimeoutError:
                return {}, JobError("EXECUTION_TIMEOUT", "Task execution deadline reached")
            except JobError as exc:
                return {}, exc
            except Exception as exc:
                return {}, JobError("EXECUTION_ERROR", type(exc).__name__)

        if task.lease_expire_at_ms <= time.time() * 1000:
            return None
        execution = asyncio.create_task(work())
        watcher = asyncio.create_task(self._watch_lease(task))
        try:
            done, _ = await asyncio.wait({execution, watcher}, return_when=asyncio.FIRST_COMPLETED)
            if watcher in done:
                watcher.result()
                return None
            fields, failure = execution.result()
        finally:
            for pending in (execution, watcher):
                if not pending.done():
                    pending.cancel()
            await asyncio.gather(execution, watcher, return_exceptions=True)

        try:
            if failure is not None:
                return await self.client.nack(
                    task,
                    self.worker_id,
                    error_code=failure.code,
                    error_message=str(failure),
                    retryable=failure.retryable,
                )
            return await self.client.ack(task, self.worker_id, **fields)
        except httpx.HTTPError as exc:
            if lease_lost(exc):
                return None
            raise

    async def run(self, handler: Handler) -> None:
        """Run until cancelled. Cancelled handlers leave leases for scheduler recovery."""
        async with asyncio.TaskGroup() as group:
            running = set()
            while True:
                running = {task for task in running if not task.done()}
                available = self.capacity - len(running)
                if available:
                    try:
                        jobs = await self.client.pull(
                            self.queue, self.worker_id, batch_size=min(100, available)
                        )
                    except httpx.HTTPError as exc:
                        if not transient(exc):
                            raise
                        jobs = []
                    for task in jobs:
                        running.add(group.create_task(self.execute(task, handler)))
                    if jobs:
                        continue
                if running:
                    await asyncio.wait(
                        running,
                        timeout=None if not available else self.poll_interval,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                else:
                    await asyncio.sleep(self.poll_interval)
