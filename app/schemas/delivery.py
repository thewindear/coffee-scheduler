from typing import Any

from pydantic import BaseModel, Field

from app.schemas.tasks import ArtifactRef


class ProcessingTaskTrace(BaseModel):
    task_id: str
    status: str
    retry_count: int
    worker_id: str | None = None
    parent_task_id: str | None = None
    rerun_of_task_id: str | None = None
    processing_result: dict[str, Any] | None = None
    receipt: dict[str, Any] | None = None
    finished_at_ms: int | None = None


class DispatchTraceItem(BaseModel):
    dispatch_id: str
    recipient_service: str
    step: str
    target_queue: str
    delivery_mode: str
    dispatch_kind: str
    status: str
    processing_task_id: str | None = None
    parent_dispatch_id: str | None = None
    rerun_idempotency_key: str | None = None
    kafka_delivery_id: str | None = None
    kafka_topic: str | None = None
    kafka_published_at_ms: int | None = None
    last_error: str | None = None
    processing_status: str = "UNKNOWN"
    processing_task: ProcessingTaskTrace | None = None


class CompletionTrace(BaseModel):
    completion_id: str
    lease_generation: int
    worker_id: str
    completed_at_ms: int
    summary: dict[str, Any]
    artifacts: list[ArtifactRef]
    artifact_expires_at_ms: int | None = None
    artifact_expired: bool = False
    artifact_deleted: bool = False


class TaskDeliveriesResponse(BaseModel):
    source_task_id: str
    business_id: str | None = None
    business_request_id: str | None = None
    task_type: str | None = None
    status: str
    parent_task_id: str | None = None
    child_source_task_ids: list[str] = Field(default_factory=list)
    completion: CompletionTrace | None = None
    deliveries: list[DispatchTraceItem] = Field(default_factory=list)


class RerunStepRequest(BaseModel):
    idempotency_key: str = Field(..., min_length=1, max_length=128)
    priority: int | None = Field(default=None, ge=0, le=9)


class RerunStepResponse(BaseModel):
    dispatch_id: str
    processing_task_id: str
    source_task_id: str
    completion_id: str
    recipient_service: str
    step: str
    status: str
    deduplicated: bool = False


class SourceRerunRequest(BaseModel):
    idempotency_key: str | None = Field(default=None, max_length=128)
    priority: int | None = Field(default=None, ge=0, le=9)
    delay_seconds: int = Field(default=0, ge=0)


class DeliveryStatsResponse(BaseModel):
    business_id: str | None = None
    recipient_service: str | None = None
    source_succeeded_count: int
    processing_succeeded_count: int
    processing_failed_count: int
    auto_retry_count: int
    manual_rerun_count: int
    kafka_published_count: int
    kafka_failed_count: int
    kafka_unknown_processing_count: int
    e2e_timeout_count: int


class BacklogSegment(BaseModel):
    count: int
    oldest_wait_ms: int


class DeliveryBacklogResponse(BaseModel):
    source_ready: BacklogSegment
    dispatch_pending: BacklogSegment
    processing_ready: BacklogSegment


class TaskArtifactsResponse(BaseModel):
    source_task_id: str
    completion_id: str
    business_id: str
    artifacts: list[ArtifactRef]
    artifact_expires_at_ms: int | None = None
    artifact_expired: bool = False
    artifact_deleted: bool = False
