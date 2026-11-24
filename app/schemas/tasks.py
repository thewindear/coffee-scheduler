from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class TaskSubmitRequest(BaseModel):
    queue: str = Field(..., min_length=1, max_length=64)
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=5, ge=0, le=9)
    delay_seconds: int = Field(default=0, ge=0)
    run_at_ms: int | None = Field(default=None, ge=0)
    max_retries: int = Field(default=3, ge=0, le=20)
    lease_seconds: int = Field(default=60, ge=1, le=86400)
    idempotency_key: str | None = Field(default=None, max_length=128)


class TaskBatchSubmitRequest(BaseModel):
    tasks: list[TaskSubmitRequest] = Field(..., min_length=1, max_length=500)


class TaskSubmitResponse(BaseModel):
    task_id: str
    queue: str
    status: str
    deduplicated: bool = False


class TaskPullRequest(BaseModel):
    queue: str = Field(..., min_length=1, max_length=64)
    worker_id: str = Field(..., min_length=1, max_length=128)
    batch_size: int = Field(default=10, ge=1, le=100)


class PulledTask(BaseModel):
    task_id: str
    queue: str
    priority: int
    payload: dict[str, Any]
    retry_count: int
    max_retries: int
    lease_expire_at_ms: int
    scheduled_at_ms: int


class TaskPullResponse(BaseModel):
    tasks: list[PulledTask]


class TaskHeartbeatRequest(BaseModel):
    queue: str = Field(..., min_length=1, max_length=64)
    worker_id: str = Field(..., min_length=1, max_length=128)
    extend_seconds: int | None = Field(default=None, ge=1, le=86400)


class TaskHeartbeatResponse(BaseModel):
    task_id: str
    lease_expire_at_ms: int


class ArtifactRef(BaseModel):
    storage: str = Field(..., min_length=1, max_length=32)
    uri: str = Field(..., min_length=1, max_length=1024)
    meta: dict[str, Any] = Field(default_factory=dict)


class TaskAckRequest(BaseModel):
    queue: str = Field(..., min_length=1, max_length=64)
    worker_id: str = Field(..., min_length=1, max_length=128)
    duration_ms: int | None = Field(default=None, ge=0)
    summary: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[ArtifactRef] = Field(default_factory=list)
    extra: dict[str, Any] = Field(default_factory=dict)


class TaskNackRequest(BaseModel):
    queue: str = Field(..., min_length=1, max_length=64)
    worker_id: str = Field(..., min_length=1, max_length=128)
    error_code: str = Field(default="EXECUTION_ERROR", max_length=64)
    error_message: str = Field(default="", max_length=4096)
    retryable: bool = True


class TaskNackResponse(BaseModel):
    task_id: str
    outcome: str
    retry_count: int
    next_run_at_ms: int | None = None


class TaskRetryRequest(BaseModel):
    payload: dict[str, Any] | None = None
    priority: int | None = Field(default=None, ge=0, le=9)
    delay_seconds: int = Field(default=0, ge=0)


class DlqBatchRetryRequest(BaseModel):
    task_ids: list[str] | None = Field(default=None, max_length=500)
    limit: int = Field(default=100, ge=1, le=500)


class TaskDetailResponse(BaseModel):
    task_id: str
    queue: str
    priority: int
    status: str
    payload: dict[str, Any]
    receipt: dict[str, Any] | None = None
    retry_count: int
    worker_id: str | None = None
    scheduled_at_ms: int
    pulled_at_ms: int | None = None
    finished_at_ms: int | None = None
    created_at: datetime | None = None
    source: str


class TaskListResponse(BaseModel):
    items: list[TaskDetailResponse]
    next_cursor: str | None = None


class QueueStatsResponse(BaseModel):
    queue: str
    ready_count: int
    running_count: int
    dlq_count: int
    active_workers: int
