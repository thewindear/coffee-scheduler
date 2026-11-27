from datetime import UTC, datetime

from croniter import croniter
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.deps import AuthenticatedRoute, get_dispatcher, get_identity, get_session_factory
from app.core.access import Identity, forbidden, load_identity
from app.models.tables import CronJob, ServiceIdentity
from app.schemas.cron_jobs import CronJobCreate, CronJobResponse, CronJobUpdate
from app.schemas.tasks import TaskSubmitRequest
from app.services.dispatcher import TaskDispatcher

router = APIRouter(prefix="/api/v1/cron-jobs", tags=["cron-jobs"], route_class=AuthenticatedRoute)


def _next_run_ms(cron_expr: str) -> int:
    if not croniter.is_valid(cron_expr):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid cron expression"
        )
    now_dt = datetime.now(UTC)
    return int(croniter(cron_expr, now_dt).get_next(datetime).timestamp() * 1000)


@router.post("", response_model=CronJobResponse, status_code=status.HTTP_201_CREATED)
async def create_cron_job(
    req: CronJobCreate,
    dispatcher: TaskDispatcher = Depends(get_dispatcher),
    identity: Identity = Depends(get_identity),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
) -> CronJobResponse:
    business_id = req.business_id or identity.business_id
    identity.require("cron_manage", business_id, req.queue)
    next_ms = _next_run_ms(req.cron_expr)
    job = CronJob(
        name=req.name,
        owner_service=identity.service_name,
        business_id=business_id,
        queue=req.queue,
        priority=req.priority,
        cron_expr=req.cron_expr,
        payload=req.payload,
        max_retries=req.max_retries,
        lease_seconds=req.lease_seconds,
        enabled=req.enabled,
        next_run_at=next_ms,
    )
    async with session_factory() as session:
        await dispatcher.submission_route(
            TaskSubmitRequest(queue=req.queue, business_id=business_id), identity, session
        )
        session.add(job)
        try:
            await session.commit()
        except IntegrityError as exc:
            await session.rollback()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail="Cron job name already exists"
            ) from exc
        await session.refresh(job)
        return CronJobResponse.model_validate(job)


@router.get("", response_model=list[CronJobResponse])
async def list_cron_jobs(
    queue: str | None = Query(default=None),
    enabled: bool | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    identity: Identity = Depends(get_identity),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
) -> list[CronJobResponse]:
    conditions = [
        identity.condition("cron_read", business=CronJob.business_id, queue=CronJob.queue)
    ]
    if queue is not None:
        identity.queue_scopes("cron_read", queue)
        conditions.append(CronJob.queue == queue)
    if enabled is not None:
        conditions.append(CronJob.enabled.is_(enabled))
    stmt = (
        select(CronJob).where(*conditions).order_by(CronJob.id.desc()).limit(limit).offset(offset)
    )
    async with session_factory() as session:
        rows = (await session.execute(stmt)).scalars().all()
        return [CronJobResponse.model_validate(r) for r in rows]


@router.get("/{job_id}", response_model=CronJobResponse)
async def get_cron_job(
    job_id: int,
    identity: Identity = Depends(get_identity),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
) -> CronJobResponse:
    async with session_factory() as session:
        job = await session.get(CronJob, job_id)
        if not job:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cron job not found")
        identity.require("cron_read", job.business_id, job.queue)
        return CronJobResponse.model_validate(job)


@router.patch("/{job_id}", response_model=CronJobResponse)
async def update_cron_job(
    job_id: int,
    req: CronJobUpdate,
    dispatcher: TaskDispatcher = Depends(get_dispatcher),
    identity: Identity = Depends(get_identity),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
) -> CronJobResponse:
    async with session_factory() as session:
        job = await session.get(CronJob, job_id, with_for_update=True)
        if not job:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cron job not found")
        identity.require("cron_manage", job.business_id, job.queue)
        data = req.model_dump(exclude_unset=True)
        queue = data.get("queue", job.queue)
        identity.require("cron_manage", job.business_id, queue)
        if data.get("enabled") is True or queue != job.queue:
            if job.owner_service is None:
                if not identity.administrator:
                    raise forbidden()
                job.owner_service = identity.service_name
            owner = await session.get(ServiceIdentity, job.owner_service, with_for_update=True)
            if owner is None:
                raise forbidden()
            target = TaskSubmitRequest(queue=queue, business_id=job.business_id)
            await dispatcher.submission_route(target, identity, session, use_home_business=False)
            await dispatcher.submission_route(
                target,
                await load_identity(session, owner, lock=True),
                session,
                use_home_business=False,
            )
        if "cron_expr" in data:
            job.next_run_at = _next_run_ms(data["cron_expr"])
        for k, v in data.items():
            setattr(job, k, v)
        job.version += 1
        await session.commit()
        await session.refresh(job)
        return CronJobResponse.model_validate(job)


@router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_cron_job(
    job_id: int,
    identity: Identity = Depends(get_identity),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
) -> None:
    async with session_factory() as session:
        job = await session.get(CronJob, job_id, with_for_update=True)
        if not job:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cron job not found")
        identity.require("cron_manage", job.business_id, job.queue)
        await session.delete(job)
        await session.commit()
