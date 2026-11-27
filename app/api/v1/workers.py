from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.api.deps import AuthenticatedRoute, get_dispatcher, get_identity
from app.core.access import Identity, forbidden
from app.services.dispatcher import TaskDispatcher

router = APIRouter(prefix="/api/v1/admin", tags=["admin"], route_class=AuthenticatedRoute)


class WorkerActivity(BaseModel):
    worker_id: str
    service_name: str
    queues: list[str]
    last_seen_ip: str | None
    last_seen_at: int


class WorkersResponse(BaseModel):
    online_count: int
    online_window_seconds: int
    workers: list[WorkerActivity]


@router.get("/workers", response_model=WorkersResponse)
async def list_workers(
    service_name: str | None = None,
    queue: str | None = None,
    identity: Identity = Depends(get_identity),
    dispatcher: TaskDispatcher = Depends(get_dispatcher),
) -> WorkersResponse:
    if not identity.administrator:
        raise forbidden()
    instances: dict[tuple[str, str], WorkerActivity] = {}
    for item in await dispatcher.worker_activity(queue):
        if service_name is not None and service_name != item["service_name"]:
            continue
        if queue is not None and queue != item["queue"]:
            continue
        key = (item["service_name"], item["worker_id"])
        worker = instances.get(key)
        if worker is None:
            instances[key] = WorkerActivity(**item, queues=[item["queue"]])
        else:
            worker.queues.append(item["queue"])
            if item["last_seen_at"] > worker.last_seen_at:
                worker.last_seen_at = item["last_seen_at"]
                worker.last_seen_ip = item["last_seen_ip"]
    for worker in instances.values():
        worker.queues.sort()
    return WorkersResponse(
        online_count=len(instances),
        online_window_seconds=dispatcher.settings.worker_heartbeat_ttl_seconds,
        workers=[instances[key] for key in sorted(instances)],
    )
