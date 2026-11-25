from collections.abc import Awaitable, Callable
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import Response
from fastapi.routing import APIRoute
from starlette.types import Message

from app.api.deps import get_dispatcher
from app.schemas.tasks import (
    DlqBatchRetryRequest,
    QueueStatsResponse,
    TaskAckRequest,
    TaskBatchSubmitRequest,
    TaskDetailResponse,
    TaskHeartbeatRequest,
    TaskHeartbeatResponse,
    TaskListResponse,
    TaskNackRequest,
    TaskNackResponse,
    TaskPullRequest,
    TaskPullResponse,
    TaskRetryRequest,
    TaskSubmitRequest,
    TaskSubmitResponse,
)
from app.services.dispatcher import TaskDispatcher

router = APIRouter(prefix="/api/v1", tags=["tasks"])


class ReceiptRoute(APIRoute):
    def get_route_handler(self) -> Callable[[Request], Awaitable[Response]]:
        handler = super().get_route_handler()

        async def limited_handler(request: Request) -> Response:
            limit = request.app.state.settings.max_receipt_bytes
            body = bytearray()
            async for chunk in request.stream():
                if len(body) + len(chunk) > limit:
                    raise HTTPException(
                        status_code=413,
                        detail=f"Receipt exceeds {limit} bytes. Store large data externally and pass artifact references.",
                    )
                body.extend(chunk)

            buffered: bytes | None = bytes(body)

            async def receive() -> Message:
                nonlocal buffered
                if buffered is None:
                    return await request.receive()
                chunk, buffered = buffered, None
                return {"type": "http.request", "body": chunk}

            return await handler(Request(request.scope, receive))

        return limited_handler


receipt_router = APIRouter(route_class=ReceiptRoute)


@router.post("/tasks", response_model=TaskSubmitResponse)
async def submit_task(
    req: TaskSubmitRequest,
    dispatcher: TaskDispatcher = Depends(get_dispatcher),
) -> TaskSubmitResponse:
    return await dispatcher.submit_task(req)


@router.post("/tasks/batch", response_model=list[TaskSubmitResponse])
async def submit_tasks_batch(
    req: TaskBatchSubmitRequest,
    dispatcher: TaskDispatcher = Depends(get_dispatcher),
) -> list[TaskSubmitResponse]:
    return await dispatcher.submit_batch(req.tasks)


@router.post("/tasks/pull", response_model=TaskPullResponse)
async def pull_tasks(
    req: TaskPullRequest,
    dispatcher: TaskDispatcher = Depends(get_dispatcher),
) -> TaskPullResponse:
    tasks = await dispatcher.pull_tasks(req)
    return TaskPullResponse(tasks=tasks)


@router.post("/tasks/{task_id}/heartbeat", response_model=TaskHeartbeatResponse)
async def heartbeat_task(
    task_id: str,
    req: TaskHeartbeatRequest,
    dispatcher: TaskDispatcher = Depends(get_dispatcher),
) -> TaskHeartbeatResponse:
    return await dispatcher.heartbeat(task_id, req)


@receipt_router.post("/tasks/{task_id}/ack")
async def ack_task(
    task_id: str,
    req: TaskAckRequest,
    dispatcher: TaskDispatcher = Depends(get_dispatcher),
) -> dict[str, str]:
    return await dispatcher.ack_task(task_id, req)


@receipt_router.post("/tasks/{task_id}/nack", response_model=TaskNackResponse)
async def nack_task(
    task_id: str,
    req: TaskNackRequest,
    dispatcher: TaskDispatcher = Depends(get_dispatcher),
) -> TaskNackResponse:
    return await dispatcher.nack_task(task_id, req)


@router.get("/tasks/{task_id}", response_model=TaskDetailResponse)
async def get_task(
    task_id: str,
    dispatcher: TaskDispatcher = Depends(get_dispatcher),
) -> TaskDetailResponse:
    return await dispatcher.get_task(task_id)


@router.get("/tasks", response_model=TaskListResponse)
async def list_tasks(
    queue: str | None = Query(default=None),
    status: str | None = Query(default=None),
    worker_id: str | None = Query(default=None),
    start_time: datetime | None = Query(default=None),
    end_time: datetime | None = Query(default=None),
    cursor: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=100),
    dispatcher: TaskDispatcher = Depends(get_dispatcher),
) -> TaskListResponse:
    return await dispatcher.list_tasks(
        queue=queue,
        status_filter=status,
        worker_id=worker_id,
        start_time=start_time,
        end_time=end_time,
        cursor=cursor,
        limit=limit,
    )


@router.get("/queues/{queue}/stats", response_model=QueueStatsResponse)
async def get_queue_stats(
    queue: str,
    dispatcher: TaskDispatcher = Depends(get_dispatcher),
) -> QueueStatsResponse:
    return await dispatcher.get_queue_stats(queue)


@router.post("/tasks/{task_id}/retry", response_model=TaskSubmitResponse)
async def retry_task(
    task_id: str,
    req: TaskRetryRequest | None = None,
    dispatcher: TaskDispatcher = Depends(get_dispatcher),
) -> TaskSubmitResponse:
    return await dispatcher.retry_task(task_id, req or TaskRetryRequest())


@router.post("/dlq/{queue}/retry", response_model=list[TaskSubmitResponse])
async def retry_dlq_batch(
    queue: str,
    req: DlqBatchRetryRequest | None = None,
    dispatcher: TaskDispatcher = Depends(get_dispatcher),
) -> list[TaskSubmitResponse]:
    return await dispatcher.retry_dlq_batch(queue, req or DlqBatchRetryRequest())


router.include_router(receipt_router)
