from datetime import datetime

from fastapi import APIRouter, Depends, Query, Request

from app.api.deps import AuthenticatedRoute, get_identity
from app.core.access import Identity
from app.schemas.delivery import (
    DeliveryBacklogResponse,
    DeliveryStatsResponse,
    RerunStepRequest,
    RerunStepResponse,
    SourceRerunRequest,
    TaskArtifactsResponse,
    TaskDeliveriesResponse,
)
from app.schemas.tasks import TaskSubmitResponse
from app.services.delivery_dispatcher import DeliveryDispatcher

router = APIRouter(prefix="/api/v1", tags=["deliveries"], route_class=AuthenticatedRoute)


def get_delivery_dispatcher(request: Request) -> DeliveryDispatcher:
    return request.app.state.delivery_dispatcher


@router.get("/tasks/{task_id}/deliveries", response_model=TaskDeliveriesResponse)
async def get_task_deliveries(
    task_id: str,
    identity: Identity = Depends(get_identity),
    delivery_dispatcher: DeliveryDispatcher = Depends(get_delivery_dispatcher),
) -> TaskDeliveriesResponse:
    return await delivery_dispatcher.get_task_deliveries(task_id, identity)


@router.get("/tasks/{task_id}/artifacts", response_model=TaskArtifactsResponse)
async def get_task_artifacts(
    task_id: str,
    identity: Identity = Depends(get_identity),
    delivery_dispatcher: DeliveryDispatcher = Depends(get_delivery_dispatcher),
) -> TaskArtifactsResponse:
    return await delivery_dispatcher.get_task_artifacts(task_id, identity)


@router.post(
    "/tasks/{task_id}/deliveries/{recipient_service}/{step}/rerun",
    response_model=RerunStepResponse,
)
async def rerun_delivery_step(
    task_id: str,
    recipient_service: str,
    step: str,
    req: RerunStepRequest,
    identity: Identity = Depends(get_identity),
    delivery_dispatcher: DeliveryDispatcher = Depends(get_delivery_dispatcher),
) -> RerunStepResponse:
    return await delivery_dispatcher.rerun_step(task_id, recipient_service, step, req, identity)


@router.post("/tasks/{task_id}/rerun", response_model=TaskSubmitResponse)
async def rerun_source_task(
    task_id: str,
    req: SourceRerunRequest | None = None,
    identity: Identity = Depends(get_identity),
    delivery_dispatcher: DeliveryDispatcher = Depends(get_delivery_dispatcher),
) -> TaskSubmitResponse:
    return await delivery_dispatcher.rerun_source(task_id, req or SourceRerunRequest(), identity)


@router.get("/deliveries/stats", response_model=DeliveryStatsResponse)
async def get_delivery_stats(
    business_id: str | None = Query(default=None),
    recipient_service: str | None = Query(default=None),
    start_time: datetime | None = Query(default=None),
    end_time: datetime | None = Query(default=None),
    identity: Identity = Depends(get_identity),
    queue: str | None = Query(default=None),
    delivery_dispatcher: DeliveryDispatcher = Depends(get_delivery_dispatcher),
) -> DeliveryStatsResponse:
    return await delivery_dispatcher.get_stats(
        business_id, recipient_service, start_time, end_time, identity, queue
    )


@router.get("/deliveries/backlog", response_model=DeliveryBacklogResponse)
async def get_delivery_backlog(
    business_id: str | None = Query(default=None),
    identity: Identity = Depends(get_identity),
    queue: str | None = Query(default=None),
    delivery_dispatcher: DeliveryDispatcher = Depends(get_delivery_dispatcher),
) -> DeliveryBacklogResponse:
    return await delivery_dispatcher.get_backlog(business_id, identity, queue)
