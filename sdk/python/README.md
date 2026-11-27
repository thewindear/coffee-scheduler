# Coffee Scheduler Worker SDK

Build the independent wheel from the repository root with `uv build --wheel sdk/python`.
Install the resulting `coffee_scheduler_worker-0.1.0-py3-none-any.whl` in the Worker
environment. The package uses the public JSON API and depends on HTTPX.

```python
import asyncio
import os

from coffee_scheduler_worker import SchedulerClient, Worker


async def handle(task):
    return {"summary": {"pages": task.payload["pages"]}}


async def main():
    async with SchedulerClient(
        os.environ["SCHEDULER_URL"], os.environ["SCHEDULER_TOKEN"]
    ) as client:
        await Worker(
            client,
            "document.inspect",
            "worker-1",
            capacity=4,
            auto_renew=True,
            execution_timeout=120,
        ).run(handle)


asyncio.run(main())
```

Use a stable instance ID for each independently polling process. A handler returns ACK
fields such as `summary`, `artifacts`, `extra`, or `processing_result`. Raise
`JobError(code, message, retryable=False)` for permanent failures.

Handlers are asynchronous. Use cancellable I/O with its own timeouts and release
resources in `finally`. For blocking code that cannot be cancelled, run it in a
subprocess and terminate and reap the subprocess when the handler is cancelled.
An execution timeout cancels the handler, stops renewal, and sends a retryable NACK
if the task is still owned. A lost lease cancels the handler and ends that execution.
Fixed mode uses the lease set at submission. Keep host clocks synchronized, since
the API returns absolute expiration timestamps.

Use the processing `task_id` as the idempotency key for any business write. Commit
the write before returning the ACK fields. An identical ACK is retried if its HTTP
response is lost. A retry of the ACK uses the same task ID and lease generation.
