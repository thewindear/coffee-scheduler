import json
import logging
import time
from datetime import UTC, datetime
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.core.delivery_config import DeliveryRegistry, IdentityConfig
from app.core.metrics import TASKS_SUBMITTED
from app.core.redis_client import RedisLuaManager
from app.core.uuidv7 import extract_uuid7_timestamp_ms, uuid7
from app.models.tables import DeliveryDispatch, TaskCompletion, TaskRecord
from app.schemas.delivery import (
    BacklogSegment,
    CollectionArtifactsResponse,
    CollectionDeliveriesResponse,
    CompletionTrace,
    DeliveryBacklogResponse,
    DeliveryStatsResponse,
    DispatchTraceItem,
    ProcessingTaskTrace,
    RecollectRequest,
    RerunStepRequest,
    RerunStepResponse,
)
from app.schemas.tasks import ArtifactRef, TaskSubmitResponse
from app.services.dispatcher import TaskDispatcher, _ms_to_naive_utc, _naive_utc

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
        """Project a persistent collection/processing TaskRecord into Redis if not terminal or leased."""
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
            "payload": json.dumps(task_row.payload, separators=(",", ":"), ensure_ascii=False),
            "retry_count": str(task_row.retry_count),
            "max_retries": str(self.settings.default_max_retries),
            "lease_ttl": str(self.settings.default_lease_seconds),
            "scheduled_at": str(task_row.scheduled_at),
            "created_at_ms": str(created_ms),
            "task_kind": task_row.task_kind,
            "lease_generation": str(task_row.lease_generation),
        }
        if task_row.business_id:
            fields["business_id"] = task_row.business_id
        if task_row.collection_type:
            fields["collection_type"] = task_row.collection_type
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
        if artifacts is not None:
            fields["artifacts"] = json.dumps(artifacts, separators=(",", ":"), ensure_ascii=False)

        _, _, created = await self.dispatcher._enqueue_task(
            task_row.task_id,
            task_row.queue,
            task_row.priority,
            now_ms,
            task_row.scheduled_at,
            fields,
        )
        return created

    async def dispatch_once(self) -> int:
        """Create processing tasks in MySQL for pending HTTP dispatches and project to Redis."""
        dispatched = 0
        async with self.session_factory() as session:
            stmt = (
                select(DeliveryDispatch)
                .where(
                    DeliveryDispatch.status == "PENDING",
                    DeliveryDispatch.delivery_mode == "http",
                )
                .order_by(DeliveryDispatch.created_at.asc())
                .limit(self.settings.delivery_batch_size)
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
                        "source_task_id": disp.collection_task_id,
                        "completion_id": disp.completion_id,
                        "business_id": disp.business_id,
                        "business_request_id": completion.business_request_id,
                        "collection_type": completion.collection_type,
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
                        collection_type=completion.collection_type,
                        lease_generation=0,
                        source_task_id=disp.collection_task_id,
                        completion_id=disp.completion_id,
                        recipient_service=disp.recipient_service,
                        step=disp.step,
                    )
                    session.add(task_row)

                disp.processing_task_id = proc_task_id
                disp.status = "DISPATCHED"
                await session.commit()

                await self._project_task_to_redis(task_row, artifacts=completion.artifacts)
                TASKS_SUBMITTED.labels(queue=task_row.queue).inc()
                dispatched += 1
        return dispatched

    async def reconcile_once(self) -> int:
        """Reconcile persistent READY/DISPATCHED tasks missing from Redis without reviving terminal or leased tasks."""
        repaired = 0
        now_ms = int(time.time() * 1000)
        async with self.session_factory() as session:
            # 1. Reconcile dispatched HTTP processing tasks
            disp_stmt = (
                select(DeliveryDispatch)
                .where(
                    DeliveryDispatch.status == "DISPATCHED",
                    DeliveryDispatch.delivery_mode == "http",
                    DeliveryDispatch.processing_task_id.is_not(None),
                )
                .limit(self.settings.delivery_batch_size)
            )
            dispatches = (await session.execute(disp_stmt)).scalars().all()
            for disp in dispatches:
                assert disp.processing_task_id is not None
                task_row = await self.dispatcher._find_task_record(session, disp.processing_task_id)
                if task_row is None or task_row.status in ("SUCCEEDED", "DEAD"):
                    continue
                if await self._needs_redis_recovery(task_row, now_ms):
                    comp = await session.get(TaskCompletion, disp.completion_id)
                    artifacts = comp.artifacts if comp else []
                    await self._project_task_to_redis(task_row, artifacts=artifacts)
                    repaired += 1

            # 2. Reconcile collection tasks stuck in READY without Redis entry
            coll_stmt = (
                select(TaskRecord)
                .where(
                    TaskRecord.task_kind == "collection",
                    TaskRecord.status == "READY",
                )
                .limit(self.settings.delivery_batch_size)
            )
            coll_rows = (await session.execute(coll_stmt)).scalars().all()
            for row in coll_rows:
                if await self._needs_redis_recovery(row, now_ms):
                    await self._project_task_to_redis(row)
                    repaired += 1
        return repaired

    async def _needs_redis_recovery(self, task_row: TaskRecord, now_ms: int) -> bool:
        if task_row.status in ("SUCCEEDED", "DEAD"):
            return False
        lease_score = await self.redis.zscore(
            self.redis_mgr.lease_key(task_row.queue), task_row.task_id
        )
        if lease_score is not None and float(lease_score) > now_ms:
            return False
        exists_hash = await self.redis.exists(self.redis_mgr.task_key(task_row.task_id))
        if not exists_hash:
            return True
        if task_row.status == "READY":
            ready_score = await self.redis.zscore(
                self.redis_mgr.ready_key(task_row.queue), task_row.task_id
            )
            if ready_score is None:
                return True
        return False

    async def _load_task_or_404(self, session: AsyncSession, task_id: str) -> TaskRecord:
        row = await self.dispatcher._find_task_record(session, task_id)
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Collection task not found"
            )
        return row

    async def get_collection_deliveries(
        self, collection_task_id: str, identity: IdentityConfig | None
    ) -> CollectionDeliveriesResponse:
        now_ms = int(time.time() * 1000)
        async with self.session_factory() as session:
            coll_row = await self._load_task_or_404(session, collection_task_id)
            if coll_row.business_id:
                self.registry.authorize_business_access(identity, coll_row.business_id)

            comp = (
                await session.execute(
                    select(TaskCompletion).where(
                        TaskCompletion.collection_task_id == collection_task_id
                    )
                )
            ).scalar_one_or_none()

            child_rows = (
                await session.execute(
                    select(TaskRecord.task_id).where(TaskRecord.parent_task_id == collection_task_id)
                )
            ).scalars().all()

            dispatches = (
                await session.execute(
                    select(DeliveryDispatch)
                    .where(DeliveryDispatch.collection_task_id == collection_task_id)
                    .order_by(DeliveryDispatch.created_at.asc())
                )
            ).scalars().all()

            items: list[DispatchTraceItem] = []
            for d in dispatches:
                proc_trace: ProcessingTaskTrace | None = None
                proc_status = "UNKNOWN"
                if d.delivery_mode == "http":
                    if d.processing_task_id:
                        p_row = await self.dispatcher._find_task_record(session, d.processing_task_id)
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

            return CollectionDeliveriesResponse(
                collection_task_id=collection_task_id,
                business_id=coll_row.business_id,
                business_request_id=coll_row.business_request_id,
                collection_type=coll_row.collection_type,
                status=coll_row.status,
                parent_task_id=coll_row.parent_task_id,
                child_collection_task_ids=list(child_rows),
                completion=comp_trace,
                deliveries=items,
            )

    async def get_collection_artifacts(
        self, collection_task_id: str, identity: IdentityConfig | None
    ) -> CollectionArtifactsResponse:
        now_ms = int(time.time() * 1000)
        async with self.session_factory() as session:
            coll_row = await self._load_task_or_404(session, collection_task_id)
            comp = (
                await session.execute(
                    select(TaskCompletion).where(
                        TaskCompletion.collection_task_id == collection_task_id
                    )
                )
            ).scalar_one_or_none()
            if comp is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Completion record not found for collection task",
                )
            allowed_recipient = None
            if identity and identity.role == "service" and identity.service_name:
                for snap in comp.recipient_snapshot:
                    if snap.get("recipient_service") == identity.service_name:
                        allowed_recipient = identity.service_name
                        break
            self.registry.authorize_business_access(
                identity,
                coll_row.business_id or "",
                allow_service_recipient=allowed_recipient,
            )
            expired = bool(
                comp.artifact_expires_at_ms is not None and now_ms > comp.artifact_expires_at_ms
            )
            return CollectionArtifactsResponse(
                collection_task_id=collection_task_id,
                completion_id=comp.completion_id,
                business_id=comp.business_id,
                artifacts=[ArtifactRef.model_validate(a) for a in comp.artifacts],
                artifact_expires_at_ms=comp.artifact_expires_at_ms,
                artifact_expired=expired,
                artifact_deleted=comp.artifact_deleted,
            )

    async def rerun_step(
        self,
        collection_task_id: str,
        recipient_service: str,
        step: str,
        req: RerunStepRequest,
        identity: IdentityConfig | None,
    ) -> RerunStepResponse:
        now_ms = int(time.time() * 1000)
        scoped_idemp_key = f"{collection_task_id}:{recipient_service}:{step}:{req.idempotency_key}"

        async with self.session_factory() as session:
            coll_row = await self._load_task_or_404(session, collection_task_id)
            if coll_row.business_id:
                self.registry.authorize_business_access(identity, coll_row.business_id)

            comp = (
                await session.execute(
                    select(TaskCompletion).where(
                        TaskCompletion.collection_task_id == collection_task_id
                    )
                )
            ).scalar_one_or_none()
            if comp is None:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Collection task has not completed yet",
                )

            # Check idempotency key first
            existing_rerun = (
                await session.execute(
                    select(DeliveryDispatch).where(
                        DeliveryDispatch.rerun_idempotency_key == scoped_idemp_key
                    )
                )
            ).scalar_one_or_none()
            if existing_rerun is not None:
                return RerunStepResponse(
                    dispatch_id=existing_rerun.dispatch_id,
                    processing_task_id=existing_rerun.processing_task_id or "",
                    collection_task_id=collection_task_id,
                    completion_id=comp.completion_id,
                    recipient_service=recipient_service,
                    step=step,
                    status=existing_rerun.status,
                    deduplicated=True,
                )

            if comp.artifact_deleted:
                raise HTTPException(
                    status_code=status.HTTP_410_GONE,
                    detail={"code": "ARTIFACT_DELETED", "message": "Artifact has been deleted; please re-collect"},
                )
            if comp.artifact_expires_at_ms is not None and now_ms > comp.artifact_expires_at_ms:
                raise HTTPException(
                    status_code=status.HTTP_410_GONE,
                    detail={"code": "ARTIFACT_EXPIRED", "message": "Artifact retention window expired; please re-collect"},
                )

            orig_dispatch = (
                await session.execute(
                    select(DeliveryDispatch)
                    .where(
                        DeliveryDispatch.completion_id == comp.completion_id,
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

            new_dispatch_id = uuid7(now_ms)
            new_proc_task_id = uuid7(now_ms)
            priority = req.priority if req.priority is not None else 5
            payload = {
                "source_task_id": collection_task_id,
                "completion_id": comp.completion_id,
                "business_id": comp.business_id,
                "business_request_id": comp.business_request_id,
                "collection_type": comp.collection_type,
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
                collection_type=comp.collection_type,
                lease_generation=0,
                source_task_id=collection_task_id,
                completion_id=comp.completion_id,
                recipient_service=recipient_service,
                step=step,
                parent_task_id=orig_dispatch.processing_task_id,
                rerun_of_task_id=orig_dispatch.processing_task_id,
            )
            new_disp = DeliveryDispatch(
                dispatch_id=new_dispatch_id,
                completion_id=comp.completion_id,
                collection_task_id=collection_task_id,
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
                    collection_task_id=collection_task_id,
                    completion_id=comp.completion_id,
                    recipient_service=recipient_service,
                    step=step,
                    status=dup.status,
                    deduplicated=True,
                )

        await self._project_task_to_redis(new_task, artifacts=comp.artifacts)
        return RerunStepResponse(
            dispatch_id=new_dispatch_id,
            processing_task_id=new_proc_task_id,
            collection_task_id=collection_task_id,
            completion_id=comp.completion_id,
            recipient_service=recipient_service,
            step=step,
            status="DISPATCHED",
            deduplicated=False,
        )

    async def recollect(
        self,
        collection_task_id: str,
        req: RecollectRequest,
        identity: IdentityConfig | None,
    ) -> TaskSubmitResponse:
        now_ms = int(time.time() * 1000)
        async with self.session_factory() as session:
            orig = await self._load_task_or_404(session, collection_task_id)
            if orig.business_id:
                self.registry.authorize_business_access(identity, orig.business_id)
            if orig.task_kind != "collection":
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Only collection tasks can be re-collected",
                )

            new_task_id = uuid7(now_ms)
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
                task_kind="collection",
                business_id=orig.business_id,
                business_request_id=orig.business_request_id,
                collection_type=orig.collection_type,
                lease_generation=0,
                parent_task_id=collection_task_id,
            )
            session.add(new_row)
            await session.commit()

        await self._project_task_to_redis(new_row)
        return TaskSubmitResponse(
            task_id=new_task_id,
            queue=orig.queue,
            status=initial_status,
            task_kind="collection",
            business_id=orig.business_id,
            collection_type=orig.collection_type,
        )

    async def get_stats(
        self,
        business_id: str | None,
        recipient_service: str | None,
        start_time: datetime | None,
        end_time: datetime | None,
        identity: IdentityConfig | None,
    ) -> DeliveryStatsResponse:
        eff_business_id = business_id
        if identity is not None and identity.role == "business":
            if business_id and business_id != identity.business_id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Cross-business stats access is forbidden",
                )
            eff_business_id = identity.business_id
        elif eff_business_id:
            self.registry.authorize_business_access(identity, eff_business_id)
        elif identity is None or identity.role != "operator":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Operator identity or business_id required",
            )

        now_ms = int(time.time() * 1000)
        start_ms = int(_naive_utc(start_time).replace(tzinfo=UTC).timestamp() * 1000) if start_time else None
        end_ms = int(_naive_utc(end_time).replace(tzinfo=UTC).timestamp() * 1000) if end_time else None

        async with self.session_factory() as session:
            comp_conds = []
            if eff_business_id:
                comp_conds.append(TaskCompletion.business_id == eff_business_id)
            if start_ms is not None:
                comp_conds.append(TaskCompletion.completed_at_ms >= start_ms)
            if end_ms is not None:
                comp_conds.append(TaskCompletion.completed_at_ms <= end_ms)
            collection_succeeded_count = (
                await session.scalar(select(func.count(TaskCompletion.completion_id)).where(*comp_conds))
            ) or 0

            disp_conds = []
            if eff_business_id:
                disp_conds.append(DeliveryDispatch.business_id == eff_business_id)
            if recipient_service:
                disp_conds.append(DeliveryDispatch.recipient_service == recipient_service)
            if start_ms is not None:
                disp_conds.append(DeliveryDispatch.completed_at_ms >= start_ms)
            if end_ms is not None:
                disp_conds.append(DeliveryDispatch.completed_at_ms <= end_ms)

            dispatches = (
                await session.execute(select(DeliveryDispatch).where(*disp_conds))
            ).scalars().all()

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
                    self.registry.get_collection_type(d.business_id, comp.collection_type)
                    if comp
                    else None
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
                        p_row = await self.dispatcher._find_task_record(session, d.processing_task_id)
                        if p_row is not None:
                            auto_retry += p_row.retry_count
                            if p_row.status == "SUCCEEDED":
                                proc_succeeded += 1
                                if p_row.finished_at and (p_row.finished_at - d.completed_at_ms) > timeout_ms:
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
                business_id=eff_business_id,
                recipient_service=recipient_service,
                collection_succeeded_count=int(collection_succeeded_count),
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
        self, business_id: str | None, identity: IdentityConfig | None
    ) -> DeliveryBacklogResponse:
        eff_business_id = business_id
        if identity is not None and identity.role == "business":
            if business_id and business_id != identity.business_id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Cross-business backlog access is forbidden",
                )
            eff_business_id = identity.business_id
        elif eff_business_id:
            self.registry.authorize_business_access(identity, eff_business_id)
        elif identity is None or identity.role != "operator":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Operator identity or business_id required",
            )

        now_ms = int(time.time() * 1000)
        async with self.session_factory() as session:
            # 1. Due collection tasks waiting to be pulled
            c_conds = [
                TaskRecord.task_kind == "collection",
                TaskRecord.status == "READY",
                TaskRecord.scheduled_at <= now_ms,
            ]
            if eff_business_id:
                c_conds.append(TaskRecord.business_id == eff_business_id)
            c_rows = (await session.execute(select(TaskRecord.scheduled_at).where(*c_conds))).scalars().all()
            c_oldest = max((now_ms - s for s in c_rows), default=0)

            # 2. Collection succeeded, recipient still PENDING dispatch/publish
            d_conds = [DeliveryDispatch.status == "PENDING"]
            if eff_business_id:
                d_conds.append(DeliveryDispatch.business_id == eff_business_id)
            d_rows = (
                await session.execute(select(DeliveryDispatch.completed_at_ms).where(*d_conds))
            ).scalars().all()
            d_oldest = max((now_ms - c for c in d_rows), default=0)

            # 3. Ready HTTP processing tasks waiting for downstream pull
            p_conds = [
                TaskRecord.task_kind == "processing",
                TaskRecord.status == "READY",
                TaskRecord.scheduled_at <= now_ms,
            ]
            if eff_business_id:
                p_conds.append(TaskRecord.business_id == eff_business_id)
            p_rows = (await session.execute(select(TaskRecord.scheduled_at).where(*p_conds))).scalars().all()
            p_oldest = max((now_ms - s for s in p_rows), default=0)

        return DeliveryBacklogResponse(
            collection_ready=BacklogSegment(count=len(c_rows), oldest_wait_ms=c_oldest),
            dispatch_pending=BacklogSegment(count=len(d_rows), oldest_wait_ms=d_oldest),
            processing_ready=BacklogSegment(count=len(p_rows), oldest_wait_ms=p_oldest),
        )
