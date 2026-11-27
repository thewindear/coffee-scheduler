import hashlib
import json
import logging
import time
from datetime import UTC, datetime
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.access import Identity, forbidden, insert_once
from app.core.config import Settings
from app.core.delivery_config import DeliveryRegistry
from app.core.metrics import TASKS_SUBMITTED
from app.core.redis_client import RedisLuaManager
from app.core.uuidv7 import extract_uuid7_timestamp_ms, uuid7
from app.models.tables import DeliveryDispatch, SourceSubmission, TaskCompletion, TaskRecord
from app.schemas.delivery import (
    BacklogSegment,
    CompletionTrace,
    DeliveryBacklogResponse,
    DeliveryStatsResponse,
    DispatchTraceItem,
    ProcessingTaskTrace,
    RerunStepRequest,
    RerunStepResponse,
    SourceRerunRequest,
    TaskArtifactsResponse,
    TaskDeliveriesResponse,
)
from app.schemas.tasks import ArtifactRef, TaskSubmitResponse
from app.services.dispatcher import TaskDispatcher, _ms_to_naive_utc, _naive_utc, _public_payload

logger = logging.getLogger(__name__)


class DeliveryDispatcher:
    """Handles background result dispatch, Redis reconciliation, lineage queries, reruns, and stats."""

    def __init__(
        self,
        redis_mgr: RedisLuaManager,
        dispatcher: TaskDispatcher,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        registry: DeliveryRegistry,
    ) -> None:
        self.redis_mgr = redis_mgr
        self.redis = redis_mgr.redis
        self.dispatcher = dispatcher
        self.session_factory = session_factory
        self.settings = settings
        self.registry = registry

    async def _project_task_to_redis(
        self,
        task_row: TaskRecord,
        artifacts: list[dict[str, Any]] | None = None,
    ) -> bool:
        """Project a persistent source/processing TaskRecord into Redis if not terminal or leased."""
        if task_row.status in ("SUCCEEDED", "DEAD"):
            return False
        lease_score = await self.redis.zscore(
            self.redis_mgr.lease_key(task_row.queue), task_row.task_id
        )
        now_ms = int(time.time() * 1000)
        if lease_score is not None and float(lease_score) > now_ms:
            return False

        created_ms = extract_uuid7_timestamp_ms(task_row.task_id) or int(
            task_row.created_at.replace(tzinfo=UTC).timestamp() * 1000
        )
        fields: dict[str, str] = {
            "task_id": task_row.task_id,
            "queue": task_row.queue,
            "priority": str(task_row.priority),
            "idempotency_key": task_row.idempotency_key or "",
            "payload": json.dumps(
                _public_payload(task_row.task_kind, task_row.payload),
                separators=(",", ":"),
                ensure_ascii=False,
            ),
            "retry_count": str(task_row.retry_count),
            "max_retries": str(
                task_row.max_retries
                if task_row.max_retries is not None
                else self.settings.default_max_retries
            ),
            "lease_ttl": str(task_row.lease_seconds or self.settings.default_lease_seconds),
            "scheduled_at": str(task_row.scheduled_at),
            "created_at_ms": str(created_ms),
            "task_kind": task_row.task_kind,
            "lease_generation": str(task_row.lease_generation),
        }
        if task_row.business_id:
            fields["business_id"] = task_row.business_id
        if task_row.task_type:
            fields["task_type"] = task_row.task_type
        if task_row.business_request_id:
            fields["business_request_id"] = task_row.business_request_id
        if task_row.source_task_id:
            fields["source_task_id"] = task_row.source_task_id
        if task_row.completion_id:
            fields["completion_id"] = task_row.completion_id
        if task_row.recipient_service:
            fields["recipient_service"] = task_row.recipient_service
        if task_row.step:
            fields["step"] = task_row.step
        if task_row.parent_task_id:
            fields["parent_task_id"] = task_row.parent_task_id
        if task_row.rerun_of_task_id:
            fields["rerun_of_task_id"] = task_row.rerun_of_task_id
        if artifacts is not None:
            fields["artifacts"] = json.dumps(artifacts, separators=(",", ":"), ensure_ascii=False)

        _, _, created = await self.dispatcher._enqueue_task(
            task_row.task_id,
            task_row.queue,
            task_row.priority,
            now_ms,
            task_row.scheduled_at,
            fields,
            mode="project",
        )
        return created

    async def dispatch_once(self) -> int:
        """Create processing tasks in MySQL for pending HTTP dispatches and project to Redis."""
        projections = []
        async with self.session_factory.begin() as session:
            pending = (
                DeliveryDispatch.status == "PENDING",
                DeliveryDispatch.delivery_mode == "http",
            )
            if session.get_bind().dialect.name == "sqlite":
                # SQLite serializes writers and has no SELECT FOR UPDATE.
                await session.execute(
                    update(DeliveryDispatch)
                    .where(
                        DeliveryDispatch.dispatch_id
                        == select(DeliveryDispatch.dispatch_id)
                        .where(*pending)
                        .limit(1)
                        .scalar_subquery()
                    )
                    .values(dispatch_id=DeliveryDispatch.dispatch_id)
                )
            stmt = (
                select(DeliveryDispatch)
                .where(*pending)
                .order_by(DeliveryDispatch.created_at.asc())
                .limit(self.settings.delivery_batch_size)
                .with_for_update(skip_locked=True)
            )
            pending_rows = (await session.execute(stmt)).scalars().all()

            for disp in pending_rows:
                completion = await session.get(TaskCompletion, disp.completion_id)
                if completion is None:
                    continue

                now_ms = int(time.time() * 1000)
                proc_task_id = disp.processing_task_id
                task_row: TaskRecord | None = None

                if proc_task_id:
                    task_row = await self.dispatcher._find_task_record(session, proc_task_id)

                if task_row is None:
                    proc_task_id = proc_task_id or uuid7(now_ms)
                    payload = {
                        "source_task_id": disp.source_task_id,
                        "completion_id": disp.completion_id,
                        "business_id": disp.business_id,
                        "business_request_id": completion.business_request_id,
                        "task_type": completion.task_type,
                        "recipient_service": disp.recipient_service,
                        "step": disp.step,
                        "summary": completion.summary,
                        "artifacts": completion.artifacts,
                    }
                    task_row = TaskRecord(
                        task_id=proc_task_id,
                        queue=disp.target_queue,
                        priority=5,
                        status="READY",
                        idempotency_key=None,
                        payload=payload,
                        receipt=None,
                        retry_count=0,
                        worker_id=None,
                        scheduled_at=now_ms,
                        pulled_at=None,
                        finished_at=None,
                        created_at=_ms_to_naive_utc(now_ms),
                        task_kind="processing",
                        business_id=disp.business_id,
                        business_request_id=completion.business_request_id,
                        task_type=completion.task_type,
                        lease_generation=0,
                        source_task_id=disp.source_task_id,
                        completion_id=disp.completion_id,
                        recipient_service=disp.recipient_service,
                        step=disp.step,
                    )
                    session.add(task_row)

                disp.processing_task_id = proc_task_id
                disp.status = "DISPATCHED"
                projections.append((task_row, completion.artifacts))
        for task_row, artifacts in projections:
            await self._project_task_to_redis(task_row, artifacts=artifacts)
            TASKS_SUBMITTED.labels(queue=task_row.queue).inc()
        return len(projections)

    async def reconcile_once(self) -> int:
        """Reconcile persistent READY/DISPATCHED tasks missing from Redis without reviving terminal or leased tasks."""
        repaired = 0
        async with self.session_factory() as session:
            stmt = (
                select(TaskRecord)
                .where(
                    TaskRecord.task_kind.in_(["source", "processing"]),
                    TaskRecord.status.in_(["READY", "DELAYED", "RUNNING"]),
                )
                .order_by(TaskRecord.created_at, TaskRecord.task_id)
                .execution_options(yield_per=self.settings.delivery_batch_size)
            )
            records = await session.stream_scalars(stmt)
            try:
                async for row in records:
                    async with self.redis.pipeline(transaction=False) as pipe:
                        pipe.hget(self.redis_mgr.task_key(row.task_id), "status")
                        pipe.zscore(self.redis_mgr.ready_key(row.queue), row.task_id)
                        status, ready_score = await pipe.execute()
                    if status is not None and (
                        status not in (b"READY", "READY") or ready_score is not None
                    ):
                        continue
                    artifacts = (
                        row.payload.get("artifacts", []) if row.task_kind == "processing" else None
                    )
                    if await self._project_task_to_redis(row, artifacts=artifacts):
                        repaired += 1
                    if repaired >= self.settings.delivery_batch_size:
                        break
            finally:
                await records.close()
        return repaired

    async def _load_source(
        self, session: AsyncSession, task_id: str, identity: Identity
    ) -> TaskRecord:
        identity.scopes("task_read")
        row = await self.dispatcher._find_task_record(session, task_id)
        if row is None:
            raise HTTPException(404, "Source task not found")
        identity.require("task_read", row.business_id, row.queue, row.task_type)
        if row.task_kind != "source":
            raise HTTPException(404, "Source task not found")
        return row

    async def get_task_deliveries(
        self, source_task_id: str, identity: Identity
    ) -> TaskDeliveriesResponse:
        now_ms = int(time.time() * 1000)
        async with self.session_factory() as session:
            source = await self._load_source(session, source_task_id, identity)

            comp = (
                await session.execute(
                    select(TaskCompletion).where(TaskCompletion.source_task_id == source_task_id)
                )
            ).scalar_one_or_none()

            child_rows = (
                (
                    await session.execute(
                        select(TaskRecord.task_id).where(
                            TaskRecord.parent_task_id == source_task_id,
                            TaskRecord.task_kind == "source",
                        )
                    )
                )
                .scalars()
                .all()
            )

            dispatches = (
                (
                    await session.execute(
                        select(DeliveryDispatch)
                        .where(DeliveryDispatch.source_task_id == source_task_id)
                        .order_by(DeliveryDispatch.created_at.asc())
                    )
                )
                .scalars()
                .all()
            )

            items: list[DispatchTraceItem] = []
            for d in dispatches:
                proc_trace: ProcessingTaskTrace | None = None
                proc_status = "UNKNOWN"
                if d.delivery_mode == "http":
                    if d.processing_task_id:
                        p_row = await self.dispatcher._find_task_record(
                            session, d.processing_task_id
                        )
                        if p_row is not None:
                            proc_status = p_row.status
                            proc_trace = ProcessingTaskTrace(
                                task_id=p_row.task_id,
                                status=p_row.status,
                                retry_count=p_row.retry_count,
                                worker_id=p_row.worker_id,
                                parent_task_id=p_row.parent_task_id,
                                rerun_of_task_id=p_row.rerun_of_task_id,
                                processing_result=p_row.processing_result,
                                receipt=p_row.receipt,
                                finished_at_ms=p_row.finished_at,
                            )
                    else:
                        proc_status = "PENDING_DISPATCH"
                else:
                    proc_status = "UNKNOWN"

                items.append(
                    DispatchTraceItem(
                        dispatch_id=d.dispatch_id,
                        recipient_service=d.recipient_service,
                        step=d.step,
                        target_queue=d.target_queue,
                        delivery_mode=d.delivery_mode,
                        dispatch_kind=d.dispatch_kind,
                        status=d.status,
                        processing_task_id=d.processing_task_id,
                        parent_dispatch_id=d.parent_dispatch_id,
                        rerun_idempotency_key=d.rerun_idempotency_key,
                        kafka_delivery_id=d.kafka_delivery_id,
                        kafka_topic=d.kafka_topic,
                        kafka_published_at_ms=d.kafka_published_at_ms,
                        last_error=d.last_error,
                        processing_status=proc_status,
                        processing_task=proc_trace,
                    )
                )

            comp_trace: CompletionTrace | None = None
            if comp is not None:
                expired = bool(
                    comp.artifact_expires_at_ms is not None and now_ms > comp.artifact_expires_at_ms
                )
                comp_trace = CompletionTrace(
                    completion_id=comp.completion_id,
                    lease_generation=comp.lease_generation,
                    worker_id=comp.worker_id,
                    completed_at_ms=comp.completed_at_ms,
                    summary=comp.summary,
                    artifacts=[ArtifactRef.model_validate(a) for a in comp.artifacts],
                    artifact_expires_at_ms=comp.artifact_expires_at_ms,
                    artifact_expired=expired,
                    artifact_deleted=comp.artifact_deleted,
                )

            return TaskDeliveriesResponse(
                source_task_id=source_task_id,
                business_id=source.business_id,
                business_request_id=source.business_request_id,
                task_type=source.task_type,
                status=source.status,
                parent_task_id=source.parent_task_id,
                child_source_task_ids=list(child_rows),
                completion=comp_trace,
                deliveries=items,
            )

    async def get_task_artifacts(
        self, source_task_id: str, identity: Identity
    ) -> TaskArtifactsResponse:
        now_ms = int(time.time() * 1000)
        async with self.session_factory() as session:
            await self._load_source(session, source_task_id, identity)
            comp = (
                await session.execute(
                    select(TaskCompletion).where(TaskCompletion.source_task_id == source_task_id)
                )
            ).scalar_one_or_none()
            if comp is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Completion record not found for source task",
                )
            expired = bool(
                comp.artifact_expires_at_ms is not None and now_ms > comp.artifact_expires_at_ms
            )
            return TaskArtifactsResponse(
                source_task_id=source_task_id,
                completion_id=comp.completion_id,
                business_id=comp.business_id,
                artifacts=[ArtifactRef.model_validate(a) for a in comp.artifacts],
                artifact_expires_at_ms=comp.artifact_expires_at_ms,
                artifact_expired=expired,
                artifact_deleted=comp.artifact_deleted,
            )

    async def rerun_step(
        self,
        source_task_id: str,
        recipient_service: str,
        step: str,
        req: RerunStepRequest,
        identity: Identity,
    ) -> RerunStepResponse:
        now_ms = int(time.time() * 1000)
        legacy_key = f"{source_task_id}:{recipient_service}:{step}:{req.idempotency_key}"
        scoped_idemp_key = hashlib.sha256(
            json.dumps([source_task_id, recipient_service, step, req.idempotency_key]).encode()
        ).hexdigest()

        async with self.session_factory() as session:
            source = await self._load_source(session, source_task_id, identity)

            comp = (
                await session.execute(
                    select(TaskCompletion).where(TaskCompletion.source_task_id == source_task_id)
                )
            ).scalar_one_or_none()
            if comp is None:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Source task has not completed yet",
                )

            completion_id = comp.completion_id
            orig_dispatch = (
                await session.execute(
                    select(DeliveryDispatch)
                    .where(
                        DeliveryDispatch.completion_id == completion_id,
                        DeliveryDispatch.recipient_service == recipient_service,
                        DeliveryDispatch.step == step,
                        DeliveryDispatch.dispatch_kind == "normal",
                    )
                    .limit(1)
                )
            ).scalar_one_or_none()
            if orig_dispatch is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No dispatch found for ({recipient_service}, {step})",
                )

            identity.require(
                "task_rerun", source.business_id, orig_dispatch.target_queue, source.task_type
            )

            # Resolve the original target before applying the write grant.
            existing_rerun = (
                await session.execute(
                    select(DeliveryDispatch).where(
                        DeliveryDispatch.rerun_idempotency_key.in_([scoped_idemp_key, legacy_key])
                    )
                )
            ).scalar_one_or_none()
            if existing_rerun is not None:
                return RerunStepResponse(
                    dispatch_id=existing_rerun.dispatch_id,
                    processing_task_id=existing_rerun.processing_task_id or "",
                    source_task_id=source_task_id,
                    completion_id=completion_id,
                    recipient_service=recipient_service,
                    step=step,
                    status=existing_rerun.status,
                    deduplicated=True,
                )

            if comp.artifact_deleted:
                raise HTTPException(
                    status_code=status.HTTP_410_GONE,
                    detail={
                        "code": "ARTIFACT_DELETED",
                        "message": "Artifact has been deleted; please rerun the source task",
                    },
                )
            if comp.artifact_expires_at_ms is not None and now_ms > comp.artifact_expires_at_ms:
                raise HTTPException(
                    status_code=status.HTTP_410_GONE,
                    detail={
                        "code": "ARTIFACT_EXPIRED",
                        "message": "Artifact retention window expired; please rerun the source task",
                    },
                )

            new_dispatch_id = uuid7(now_ms)
            new_proc_task_id = uuid7(now_ms)
            priority = req.priority if req.priority is not None else 5
            payload = {
                "source_task_id": source_task_id,
                "completion_id": completion_id,
                "business_id": comp.business_id,
                "business_request_id": comp.business_request_id,
                "task_type": comp.task_type,
                "recipient_service": recipient_service,
                "step": step,
                "summary": comp.summary,
                "artifacts": comp.artifacts,
                "rerun_of_task_id": orig_dispatch.processing_task_id,
            }
            new_task = TaskRecord(
                task_id=new_proc_task_id,
                queue=orig_dispatch.target_queue,
                priority=priority,
                status="READY",
                idempotency_key=None,
                payload=payload,
                receipt=None,
                retry_count=0,
                worker_id=None,
                scheduled_at=now_ms,
                pulled_at=None,
                finished_at=None,
                created_at=_ms_to_naive_utc(now_ms),
                task_kind="processing",
                business_id=comp.business_id,
                business_request_id=comp.business_request_id,
                task_type=comp.task_type,
                lease_generation=0,
                source_task_id=source_task_id,
                completion_id=completion_id,
                recipient_service=recipient_service,
                step=step,
                parent_task_id=orig_dispatch.processing_task_id,
                rerun_of_task_id=orig_dispatch.processing_task_id,
            )
            new_disp = DeliveryDispatch(
                dispatch_id=new_dispatch_id,
                completion_id=completion_id,
                source_task_id=source_task_id,
                business_id=comp.business_id,
                recipient_service=recipient_service,
                step=step,
                target_queue=orig_dispatch.target_queue,
                delivery_mode="http",
                dispatch_kind="rerun",
                normal_dedup_key=None,
                rerun_idempotency_key=scoped_idemp_key,
                status="DISPATCHED",
                processing_task_id=new_proc_task_id,
                parent_dispatch_id=orig_dispatch.dispatch_id,
                completed_at_ms=comp.completed_at_ms,
            )
            session.add(new_task)
            session.add(new_disp)
            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()
                dup = (
                    await session.execute(
                        select(DeliveryDispatch).where(
                            DeliveryDispatch.rerun_idempotency_key == scoped_idemp_key
                        )
                    )
                ).scalar_one()
                return RerunStepResponse(
                    dispatch_id=dup.dispatch_id,
                    processing_task_id=dup.processing_task_id or "",
                    source_task_id=source_task_id,
                    completion_id=completion_id,
                    recipient_service=recipient_service,
                    step=step,
                    status=dup.status,
                    deduplicated=True,
                )

        await self._project_task_to_redis(new_task, artifacts=comp.artifacts)
        return RerunStepResponse(
            dispatch_id=new_dispatch_id,
            processing_task_id=new_proc_task_id,
            source_task_id=source_task_id,
            completion_id=completion_id,
            recipient_service=recipient_service,
            step=step,
            status="DISPATCHED",
            deduplicated=False,
        )

    async def rerun_source(
        self,
        source_task_id: str,
        req: SourceRerunRequest,
        identity: Identity,
    ) -> TaskSubmitResponse:
        now_ms = int(time.time() * 1000)
        async with self.session_factory() as session:
            orig = await self._load_source(session, source_task_id, identity)
            identity.require("task_rerun", orig.business_id, orig.queue, orig.task_type)

            new_task_id = uuid7(now_ms)
            if req.idempotency_key:
                scope = ["rerun", identity.service_name, source_task_id, req.idempotency_key]
                key = hashlib.sha256(json.dumps(scope).encode()).hexdigest()
                await insert_once(
                    session, SourceSubmission, "scope_key", scope_key=key, task_id=new_task_id
                )
                submission = await session.get(SourceSubmission, key, with_for_update=True)
                if submission.task_id != new_task_id:
                    existing = await self.dispatcher._find_task_record(session, submission.task_id)
                    return TaskSubmitResponse(
                        task_id=existing.task_id,
                        queue=existing.queue,
                        status=existing.status,
                        deduplicated=True,
                        task_kind="source",
                        business_id=existing.business_id,
                        task_type=existing.task_type,
                    )
            priority = req.priority if req.priority is not None else orig.priority
            scheduled_at_ms = now_ms + req.delay_seconds * 1000
            initial_status = "DELAYED" if scheduled_at_ms > now_ms else "READY"
            new_row = TaskRecord(
                task_id=new_task_id,
                queue=orig.queue,
                priority=priority,
                status=initial_status,
                idempotency_key=req.idempotency_key,
                payload=orig.payload,
                receipt=None,
                retry_count=0,
                worker_id=None,
                scheduled_at=scheduled_at_ms,
                pulled_at=None,
                finished_at=None,
                created_at=_ms_to_naive_utc(now_ms),
                task_kind="source",
                business_id=orig.business_id,
                business_request_id=orig.business_request_id,
                task_type=orig.task_type,
                lease_generation=0,
                parent_task_id=source_task_id,
                requested_recipients=orig.requested_recipients,
                lease_seconds=orig.lease_seconds,
                max_retries=orig.max_retries,
            )
            session.add(new_row)
            await session.commit()

        await self._project_task_to_redis(new_row)
        return TaskSubmitResponse(
            task_id=new_task_id,
            queue=orig.queue,
            status=initial_status,
            task_kind="source",
            business_id=orig.business_id,
            task_type=orig.task_type,
        )

    async def get_stats(
        self,
        business_id: str | None,
        recipient_service: str | None,
        start_time: datetime | None,
        end_time: datetime | None,
        identity: Identity,
        queue: str | None = None,
    ) -> DeliveryStatsResponse:
        scopes = identity.queue_scopes("monitor", queue) if queue else identity.scopes("monitor")
        if business_id and not any(
            not grant.business_id or grant.business_id == business_id for grant in scopes
        ):
            raise forbidden()
        now_ms = int(time.time() * 1000)
        start_ms = (
            int(_naive_utc(start_time).replace(tzinfo=UTC).timestamp() * 1000)
            if start_time
            else None
        )
        end_ms = (
            int(_naive_utc(end_time).replace(tzinfo=UTC).timestamp() * 1000) if end_time else None
        )

        async with self.session_factory() as session:
            comp_conds = [
                identity.condition(
                    "monitor", business=TaskCompletion.business_id, queue=TaskRecord.queue
                )
            ]
            if business_id:
                comp_conds.append(TaskCompletion.business_id == business_id)
            if queue:
                comp_conds.append(TaskRecord.queue == queue)
            if start_ms is not None:
                comp_conds.append(TaskCompletion.completed_at_ms >= start_ms)
            if end_ms is not None:
                comp_conds.append(TaskCompletion.completed_at_ms <= end_ms)
            source_succeeded_count = (
                await session.scalar(
                    select(func.count(TaskCompletion.completion_id))
                    .join(TaskRecord, TaskRecord.task_id == TaskCompletion.source_task_id)
                    .where(*comp_conds)
                )
            ) or 0

            disp_conds = [
                identity.condition(
                    "monitor",
                    business=DeliveryDispatch.business_id,
                    queue=DeliveryDispatch.target_queue,
                )
            ]
            if business_id:
                disp_conds.append(DeliveryDispatch.business_id == business_id)
            if queue:
                disp_conds.append(DeliveryDispatch.target_queue == queue)
            if recipient_service:
                disp_conds.append(DeliveryDispatch.recipient_service == recipient_service)
            if start_ms is not None:
                disp_conds.append(DeliveryDispatch.completed_at_ms >= start_ms)
            if end_ms is not None:
                disp_conds.append(DeliveryDispatch.completed_at_ms <= end_ms)

            dispatches = (
                (await session.execute(select(DeliveryDispatch).where(*disp_conds))).scalars().all()
            )

            proc_succeeded = 0
            proc_failed = 0
            auto_retry = 0
            manual_rerun = 0
            kafka_published = 0
            kafka_failed = 0
            kafka_unknown = 0
            e2e_timeout = 0

            for d in dispatches:
                if d.dispatch_kind == "rerun":
                    manual_rerun += 1
                comp = await session.get(TaskCompletion, d.completion_id)
                ct_cfg = (
                    self.registry.get_task_type(d.business_id, comp.task_type) if comp else None
                )
                timeout_s = (
                    ct_cfg.e2e_timeout_seconds
                    if ct_cfg and ct_cfg.e2e_timeout_seconds is not None
                    else self.settings.default_e2e_timeout_seconds
                )
                timeout_ms = timeout_s * 1000

                if d.delivery_mode == "kafka":
                    if d.status == "KAFKA_PUBLISHED":
                        kafka_published += 1
                        kafka_unknown += 1
                    elif d.last_error:
                        kafka_failed += 1
                    if (now_ms - d.completed_at_ms) > timeout_ms and d.status != "KAFKA_PUBLISHED":
                        e2e_timeout += 1
                else:
                    if d.processing_task_id:
                        p_row = await self.dispatcher._find_task_record(
                            session, d.processing_task_id
                        )
                        if p_row is not None:
                            auto_retry += p_row.retry_count
                            if p_row.status == "SUCCEEDED":
                                proc_succeeded += 1
                                if (
                                    p_row.finished_at
                                    and (p_row.finished_at - d.completed_at_ms) > timeout_ms
                                ):
                                    e2e_timeout += 1
                            elif p_row.status == "DEAD":
                                proc_failed += 1
                                if (now_ms - d.completed_at_ms) > timeout_ms:
                                    e2e_timeout += 1
                            else:
                                if (now_ms - d.completed_at_ms) > timeout_ms:
                                    e2e_timeout += 1
                    else:
                        if (now_ms - d.completed_at_ms) > timeout_ms:
                            e2e_timeout += 1

            return DeliveryStatsResponse(
                business_id=business_id,
                recipient_service=recipient_service,
                source_succeeded_count=int(source_succeeded_count),
                processing_succeeded_count=proc_succeeded,
                processing_failed_count=proc_failed,
                auto_retry_count=auto_retry,
                manual_rerun_count=manual_rerun,
                kafka_published_count=kafka_published,
                kafka_failed_count=kafka_failed,
                kafka_unknown_processing_count=kafka_unknown,
                e2e_timeout_count=e2e_timeout,
            )

    async def get_backlog(
        self,
        business_id: str | None,
        identity: Identity,
        queue: str | None = None,
    ) -> DeliveryBacklogResponse:
        scopes = identity.queue_scopes("monitor", queue) if queue else identity.scopes("monitor")
        if business_id and not any(
            not grant.business_id or grant.business_id == business_id for grant in scopes
        ):
            raise forbidden()
        now_ms = int(time.time() * 1000)
        async with self.session_factory() as session:
            task_scope = [
                identity.condition(
                    "monitor", business=TaskRecord.business_id, queue=TaskRecord.queue
                )
            ]
            dispatch_scope = [
                identity.condition(
                    "monitor",
                    business=DeliveryDispatch.business_id,
                    queue=DeliveryDispatch.target_queue,
                )
            ]
            if business_id:
                task_scope.append(TaskRecord.business_id == business_id)
                dispatch_scope.append(DeliveryDispatch.business_id == business_id)
            if queue:
                task_scope.append(TaskRecord.queue == queue)
                dispatch_scope.append(DeliveryDispatch.target_queue == queue)

            async def summary(timestamp, *conditions) -> BacklogSegment:
                count, oldest = (
                    await session.execute(
                        select(func.count(), func.min(timestamp)).where(*conditions)
                    )
                ).one()
                return BacklogSegment(
                    count=count, oldest_wait_ms=max(now_ms - oldest, 0) if oldest is not None else 0
                )

            ready = [*task_scope, TaskRecord.status == "READY", TaskRecord.scheduled_at <= now_ms]
            sources = await summary(
                TaskRecord.scheduled_at, TaskRecord.task_kind == "source", *ready
            )
            processing = await summary(
                TaskRecord.scheduled_at, TaskRecord.task_kind == "processing", *ready
            )
            pending = await summary(
                DeliveryDispatch.completed_at_ms,
                DeliveryDispatch.status == "PENDING",
                *dispatch_scope,
            )
        return DeliveryBacklogResponse(
            source_ready=sources,
            dispatch_pending=pending,
            processing_ready=processing,
        )
