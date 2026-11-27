import asyncio
from dataclasses import dataclass, field
from urllib.parse import quote

import httpx

_TRANSIENT = {408, 429, 500, 502, 503, 504}


@dataclass(frozen=True, slots=True)
class Task:
    task_id: str
    queue: str
    lease_generation: int
    lease_expire_at_ms: int
    payload: dict
    priority: int = 5
    retry_count: int = 0
    max_retries: int = 3
    scheduled_at_ms: int = 0
    task_kind: str = "normal"
    task_type: str | None = None
    source_task_id: str | None = None
    completion_id: str | None = None
    step: str | None = None
    recipient_service: str | None = None
    artifacts: list[dict] = field(default_factory=list)


def transient(error: httpx.HTTPError) -> bool:
    return isinstance(error, httpx.TransportError) or (
        isinstance(error, httpx.HTTPStatusError) and error.response.status_code in _TRANSIENT
    )


def lease_lost(error: httpx.HTTPError) -> bool:
    return isinstance(error, httpx.HTTPStatusError) and error.response.status_code in (
        403,
        404,
        409,
    )


class SchedulerClient:
    """Client for the public JSON API. Use HTTPS for remote scheduler connections."""

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float = 10,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._http = httpx.AsyncClient(
            base_url=f"{base_url.rstrip('/')}/api/v1/",
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout,
            transport=transport,
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()

    async def _request(self, method: str, path: str, body: dict | None = None) -> dict:
        response = await self._http.request(method, path, json=body)
        return response.raise_for_status().json()

    async def submit(self, **fields) -> dict:
        return await self._request("POST", "tasks", fields)

    async def pull(self, queue: str, worker_id: str, *, batch_size: int = 1) -> list[Task]:
        response = await self._request(
            "POST",
            "tasks/pull",
            {"queue": queue, "worker_id": worker_id, "batch_size": batch_size},
        )
        return [
            Task(**{name: entry[name] for name in Task.__dataclass_fields__ if name in entry})
            for entry in response["tasks"]
        ]

    async def _action(self, action: str, task: Task, worker_id: str, fields: dict) -> dict:
        return await self._request(
            "POST",
            f"tasks/{quote(task.task_id, safe='')}/{action}",
            {
                **fields,
                "queue": task.queue,
                "worker_id": worker_id,
                "lease_generation": task.lease_generation,
            },
        )

    async def heartbeat(
        self, task: Task, worker_id: str, *, extend_seconds: int | None = None
    ) -> dict:
        return await self._action("heartbeat", task, worker_id, {"extend_seconds": extend_seconds})

    async def ack(
        self,
        task: Task,
        worker_id: str,
        *,
        summary: dict | None = None,
        artifacts: list[dict] | None = None,
        extra: dict | None = None,
        processing_result: dict | None = None,
        duration_ms: int | None = None,
        attempts: int = 3,
    ) -> dict:
        if attempts < 1:
            raise ValueError("ACK attempts must be positive")
        fields = {
            "summary": summary or {},
            "artifacts": artifacts or [],
            "extra": extra or {},
            "processing_result": processing_result,
            "duration_ms": duration_ms,
        }
        for attempt in range(attempts):
            try:
                return await self._action("ack", task, worker_id, fields)
            except httpx.HTTPError as exc:
                if attempt == attempts - 1 or not transient(exc):
                    raise
                await asyncio.sleep(min(0.1 * 2**attempt, 1))

    async def nack(
        self,
        task: Task,
        worker_id: str,
        *,
        error_code: str = "EXECUTION_ERROR",
        error_message: str = "",
        retryable: bool = True,
    ) -> dict:
        return await self._action(
            "nack",
            task,
            worker_id,
            {
                "error_code": error_code,
                "error_message": error_message,
                "retryable": retryable,
            },
        )

    async def deliveries(self, task_id: str) -> dict:
        return await self._request("GET", f"tasks/{quote(task_id, safe='')}/deliveries")

    async def artifacts(self, task_id: str) -> dict:
        return await self._request("GET", f"tasks/{quote(task_id, safe='')}/artifacts")

    async def rerun_delivery(
        self,
        task_id: str,
        recipient_service: str,
        step: str,
        *,
        idempotency_key: str,
        priority: int | None = None,
    ) -> dict:
        task, recipient, step_name = (
            quote(part, safe="") for part in (task_id, recipient_service, step)
        )
        return await self._request(
            "POST",
            f"tasks/{task}/deliveries/{recipient}/{step_name}/rerun",
            {"idempotency_key": idempotency_key, "priority": priority},
        )

    async def rerun(self, task_id: str, **fields) -> dict:
        return await self._request("POST", f"tasks/{quote(task_id, safe='')}/rerun", fields)
