from datetime import datetime

from fastapi import APIRouter, Depends, Query, Request

from app.api.deps import get_optional_identity
from app.core.delivery_config import IdentityConfig
from app.schemas.delivery import (
    CollectionArtifactsResponse,
    CollectionDeliveriesResponse,
    DeliveryBacklogResponse,
    DeliveryStatsResponse,
    RecollectRequest,
    RerunStepRequest,
    RerunStepResponse,
)
from app.schemas.tasks import TaskSubmitResponse
from app.services.delivery_dispatcher import DeliveryDispatcher

router = APIRouter(prefix="/api/v1", tags=["deliveries"])


def get_delivery_dispatcher(request: Request) -> DeliveryDispatcher:
    return request.app.state.delivery_dispatcher


@router.get("/collections/{task_id}/deliveries", response_model=CollectionDeliveriesResponse)
async def get_collection_deliveries(
    task_id: str,
    identity: IdentityConfig | None = Depends(get_optional_identity),
    delivery_dispatcher: DeliveryDispatcher = Depends(get_delivery_dispatcher),
) -> CollectionDeliveriesResponse:
    return await delivery_dispatcher.get_collection_deliveries(task_id, identity)


@router.get("/collections/{task_id}/artifacts", response_model=CollectionArtifactsResponse)
async def get_collection_artifacts(
    task_id: str,
    identity: IdentityConfig | None = Depends(get_optional_identity),
    delivery_dispatcher: DeliveryDispatcher = Depends(get_delivery_dispatcher),
) -> CollectionArtifactsResponse:
    return await delivery_dispatcher.get_collection_artifacts(task_id, identity)


@router.post(
    "/collections/{task_id}/deliveries/{recipient_service}/{step}/rerun",
    response_model=RerunStepResponse,
)
async def rerun_delivery_step(
    task_id: str,
    recipient_service: str,
    step: str,
    req: RerunStepRequest,
    identity: IdentityConfig | None = Depends(get_optional_identity),
    delivery_dispatcher: DeliveryDispatcher = Depends(get_delivery_dispatcher),
) -> RerunStepResponse:
    return await delivery_dispatcher.rerun_step(task_id, recipient_service, step, req, identity)


@router.post("/collections/{task_id}/recollect", response_model=TaskSubmitResponse)
async def recollect_task(
    task_id: str,
    req: RecollectRequest | None = None,
    identity: IdentityConfig | None = Depends(get_optional_identity),
    delivery_dispatcher: DeliveryDispatcher = Depends(get_delivery_dispatcher),
) -> TaskSubmitResponse:
    return await delivery_dispatcher.recollect(task_id, req or RecollectRequest(), identity)


@router.get("/deliveries/stats", response_model=DeliveryStatsResponse)
async def get_delivery_stats(
    business_id: str | None = Query(default=None),
    recipient_service: str | None = Query(default=None),
    start_time: datetime | None = Query(default=None),
    end_time: datetime | None = Query(default=None),
    identity: IdentityConfig | None = Depends(get_optional_identity),
    delivery_dispatcher: DeliveryDispatcher = Depends(get_delivery_dispatcher),
) -> DeliveryStatsResponse:
    return await delivery_dispatcher.get_stats(
        business_id, recipient_service, start_time, end_time, identity
    )


@router.get("/deliveries/backlog", response_model=DeliveryBacklogResponse)
async def get_delivery_backlog(
    business_id: str | None = Query(default=None),
    identity: IdentityConfig | None = Depends(get_optional_identity),
    delivery_dispatcher: DeliveryDispatcher = Depends(get_delivery_dispatcher),
) -> DeliveryBacklogResponse:
    return await delivery_dispatcher.get_backlog(business_id, identity)
