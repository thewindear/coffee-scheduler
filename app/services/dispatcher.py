import base64
import hashlib
import json
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import and_, desc, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.access import Identity, forbidden, insert_once
from app.core.config import Settings
from app.core.delivery_config import DeliveryRegistry
from app.core.metrics import TASKS_ACKED, TASKS_NACKED, TASKS_PULLED, TASKS_SUBMITTED
from app.core.redis_client import RedisLuaManager
from app.core.subscriptions import lock_subscription_scope, snapshot_recipients
from app.core.uuidv7 import day_bounds_utc_naive, extract_uuid7_timestamp_ms, uuid7
from app.models.tables import (
    DeliveryDispatch,
    SourceSubmission,
    SubscriptionQueue,
    TaskCompletion,
    TaskRecord,
)
from app.schemas.tasks import (
    ArtifactRef,
    DlqBatchRetryRequest,
    PulledTask,
    QueueStatsResponse,
    TaskAckRequest,
    TaskAckResponse,
    TaskDetailResponse,
    TaskHeartbeatRequest,
    TaskHeartbeatResponse,
    TaskListResponse,
    TaskNackRequest,
    TaskNackResponse,
    TaskPullRequest,
    TaskRetryRequest,
    TaskSubmitRequest,
    TaskSubmitResponse,
)
from app.services.flusher import AsyncFlusher


def _now_ms() -> int:
    return int(time.time() * 1000)


def _ms_to_naive_utc(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC).replace(tzinfo=None)


def _decode(val: Any) -> str:
    if isinstance(val, bytes):
        return val.decode("utf-8")
    return str(val)


def _task_kind(value: str) -> str:
    return "source" if value == "collection" else value


def _public_payload(kind: str, payload: dict) -> dict:
    if kind == "processing" and "collection_type" in payload:
        payload = dict(payload)
        payload.setdefault("task_type", payload.pop("collection_type"))
    return payload


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _encode_cursor(created_at: datetime, task_id: str) -> str:
    raw = f"{created_at.isoformat()}|{task_id}"
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")


def _decode_cursor(cursor: str) -> tuple[datetime, str]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
        ts_str, tid = raw.split("|", 1)
        return _naive_utc(datetime.fromisoformat(ts_str)), tid
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid cursor"
        ) from exc


def _compute_receipt_hash(
    summary: dict[str, Any],
    artifacts: list[dict[str, Any]],
    extra: dict[str, Any],
    processing_result: dict[str, Any] | None = None,
) -> str:
    canonical = json.dumps(
        {
            "summary": summary,
            "artifacts": artifacts,
            "extra": extra,
            "processing_result": processing_result,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class TaskDispatcher:
    """Core service handling task submission, priority pulling, leases, receipts, and queries."""

    def __init__(
        self,
        redis_mgr: RedisLuaManager,
        flusher: AsyncFlusher,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        registry: DeliveryRegistry | None = None,
    ) -> None:
        self.redis_mgr = redis_mgr
        self.redis = redis_mgr.redis
        self.flusher = flusher
        self.session_factory = session_factory
        self.settings = settings
        self.registry = registry or DeliveryRegistry()

    async def _enqueue_task(
        self,
        task_id: str,
        queue: str,
        priority: int,
        now_ms: int,
        scheduled_at_ms: int,
        fields: dict[str, str],
        *,
        idempotency_key: str | None = None,
        mode: str = "submit",
    ) -> tuple[str, str, bool]:
        keys = [
            self.redis_mgr.task_key(task_id),
            self.redis_mgr.ready_key(queue),
            self.redis_mgr.delay_key(self.redis_mgr.delay_shard_for(task_id)),
            self.redis_mgr.active_queues_key(),
            self.redis_mgr.dlq_key(queue),
        ]
        if idempotency_key:
            keys.append(idempotency_key)
        result = await self.redis_mgr.get_script("enqueue")(
            keys=keys,
            args=[
                self.redis_mgr.prefix,
                task_id,
                queue,
                priority,
                now_ms,
                scheduled_at_ms,
                mode,
                self.settings.idempotency_ttl_seconds,
                *(value for pair in fields.items() for value in pair),
            ],
        )
        return _decode(result[0]), _decode(result[1]), bool(result[2])

    async def submission_route(
        self,
        req: TaskSubmitRequest,
        identity: Identity,
        session: AsyncSession,
        *,
        use_home_business: bool = True,
    ) -> tuple[str, str | None, str]:
        business_id = req.business_id or (identity.business_id if use_home_business else None)
        if req.task_type is not None:
            cfg = self.registry.resolve_submission(identity, business_id, req.task_type)
            self.registry.validate_recipients(cfg, req.recipient_services)
            queue, kind = cfg.queue, "source" if cfg.delivery_enabled else "normal"
        else:
            assert req.queue is not None
            queue, kind = req.queue, "normal"
            identity.require("submit", business_id, queue)
        if self.registry.is_processing_queue(queue) or await session.get(SubscriptionQueue, queue):
            raise forbidden()
        return queue, business_id, kind

    async def submit_task(
        self,
        req: TaskSubmitRequest,
        *,
        identity: Identity,
        cron_occurrence: tuple[int, int] | None = None,
        route: tuple[str, str | None, str] | None = None,
    ) -> TaskSubmitResponse:
        if route is None:
            async with self.session_factory() as session:
                route = await self.submission_route(req, identity, session)
        queue, business_id, kind = route
        now_ms = _now_ms()
        task_id = uuid7(now_ms)
        scheduled = (
            req.run_at_ms if req.run_at_ms is not None else now_ms + req.delay_seconds * 1000
        )

        scope_key = None
        if req.idempotency_key:
            scope = [business_id, identity.service_name, queue, req.task_type, req.idempotency_key]
            scope_key = hashlib.sha256(json.dumps(scope).encode()).hexdigest()
        if cron_occurrence is not None:
            scope_key = f"cron_occurrence:{cron_occurrence[0]}:{cron_occurrence[1]}"

        fields = {
            "task_id": task_id,
            "queue": queue,
            "priority": str(req.priority),
            "idempotency_key": req.idempotency_key or "",
            "payload": json.dumps(req.payload, separators=(",", ":"), ensure_ascii=False),
            "retry_count": "0",
            "max_retries": str(req.max_retries),
            "lease_ttl": str(req.lease_seconds),
            "scheduled_at": str(scheduled),
            "created_at_ms": str(now_ms),
            "task_kind": kind,
            "lease_generation": "0",
            "business_id": business_id or "",
            "task_type": req.task_type or "",
            "business_request_id": req.business_request_id or "",
        }
        row_fields = {
            "task_id": task_id,
            "queue": queue,
            "priority": req.priority,
            "status": "READY" if scheduled <= now_ms else "DELAYED",
            "idempotency_key": req.idempotency_key,
            "payload": req.payload,
            "receipt": None,
            "retry_count": 0,
            "worker_id": None,
            "worker_service": None,
            "scheduled_at": scheduled,
            "pulled_at": None,
            "finished_at": None,
            "created_at": _ms_to_naive_utc(now_ms),
            "task_kind": kind,
            "business_id": business_id,
            "business_request_id": req.business_request_id,
            "task_type": req.task_type,
            "lease_generation": 0,
            "lease_seconds": req.lease_seconds,
            "max_retries": req.max_retries,
            "requested_recipients": req.recipient_services or [],
        }
        if kind == "source":
            async with self.session_factory.begin() as session:
                if scope_key:
                    await insert_once(
                        session, SourceSubmission, "scope_key", scope_key=scope_key, task_id=task_id
                    )
                    submission = await session.get(
                        SourceSubmission, scope_key, with_for_update=True
                    )
                    if submission.task_id != task_id:
                        original = await self._find_task_record(session, submission.task_id)
                        return TaskSubmitResponse(
                            task_id=original.task_id,
                            queue=original.queue,
                            status=original.status,
                            deduplicated=True,
                            task_kind=kind,
                            business_id=business_id,
                            task_type=req.task_type,
                        )
                session.add(TaskRecord(**row_fields))
            _, initial_status, _ = await self._enqueue_task(
                task_id, queue, req.priority, now_ms, scheduled, fields, mode="project"
            )
            created = True
        else:
            task_id, initial_status, created = await self._enqueue_task(
                task_id,
                queue,
                req.priority,
                now_ms,
                scheduled,
                fields,
                idempotency_key=self.redis_mgr.idemp_key(scope_key) if scope_key else None,
            )
            if created:
                await self.flusher.enqueue(row_fields)
        if created:
            TASKS_SUBMITTED.labels(queue=queue).inc()
        return TaskSubmitResponse(
            task_id=task_id,
            queue=queue,
            status=initial_status,
            deduplicated=not created,
            task_kind=kind,
            business_id=business_id,
            task_type=req.task_type,
        )

    async def submit_batch(
        self, items: list[TaskSubmitRequest], *, identity: Identity
    ) -> list[TaskSubmitResponse]:
        async with self.session_factory() as session:
            routes = [await self.submission_route(item, identity, session) for item in items]
        return [
            await self.submit_task(item, identity=identity, route=route)
            for item, route in zip(items, routes, strict=True)
        ]

    def _worker_activity_json(
        self, req, identity: Identity, scopes, now_ms: int, ip: str | None
    ) -> str:
        return json.dumps(
            {
                "queue": req.queue,
                "worker_id": req.worker_id,
                "service_name": identity.service_name,
                "last_seen_at": now_ms,
                "last_seen_ip": ip,
                "business_ids": sorted({grant.business_id for grant in scopes}),
            }
        )

    async def worker_activity(self, queue: str | None = None) -> list[dict]:
        keys = {
            key async for key in self.redis.scan_iter(match=self.redis_mgr.worker_pattern(queue))
        }
        return (
            [json.loads(value) for value in await self.redis.mget(list(keys)) if value]
            if keys
            else []
        )

    async def pull_tasks(
        self,
        req: TaskPullRequest,
        *,
        identity: Identity,
        ip: str | None = None,
    ) -> list[PulledTask]:
        scopes = identity.queue_scopes("pull", req.queue)
        async with self.session_factory() as session:
            owner = await session.get(SubscriptionQueue, req.queue)
        if owner:
            if owner.recipient_service != identity.service_name:
                raise forbidden()
            scopes = tuple(
                g for g in scopes if not g.business_id or g.business_id == owner.business_id
            )
            if not scopes:
                raise forbidden()
        now_ms = _now_ms()
        filters = [[g.business_id, g.task_type] for g in scopes]
        if owner:
            filters = [[owner.business_id, g.task_type] for g in scopes]
        raw_results = await self.redis_mgr.get_script("pull")(
            keys=[
                self.redis_mgr.ready_key(req.queue),
                self.redis_mgr.lease_key(req.queue),
                self.redis_mgr.worker_key(req.queue, identity.service_name, req.worker_id),
            ],
            args=[
                self.redis_mgr.prefix,
                now_ms,
                req.batch_size,
                req.worker_id,
                self.settings.default_lease_seconds * 1000,
                self.settings.worker_heartbeat_ttl_seconds,
                identity.service_name,
                self._worker_activity_json(req, identity, scopes, now_ms, ip),
                "" if ["", ""] in filters else json.dumps(filters),
                "1" if owner else "0",
            ],
        )
        if not raw_results:
            return []

        async with self.redis.pipeline(transaction=False) as pipe:
            for row in raw_results:
                tid = _decode(row[0])
                pipe.hmget(
                    self.redis_mgr.task_key(tid),
                    "task_kind",
                    "source_task_id",
                    "completion_id",
                    "step",
                    "recipient_service",
                    "artifacts",
                )
            meta_rows = await pipe.execute()

        tasks: list[PulledTask] = []
        for row, meta in zip(raw_results, meta_rows, strict=True):
            tid = _decode(row[0])
            queue = _decode(row[1])
            lease_gen = int(_decode(row[8])) if len(row) > 8 else 0
            task_kind = _task_kind(_decode(meta[0])) if meta[0] else "normal"
            source_task_id = _decode(meta[1]) if meta[1] else None
            completion_id = _decode(meta[2]) if meta[2] else None
            step = _decode(meta[3]) if meta[3] else None
            recipient_service = _decode(meta[4]) if meta[4] else None
            artifacts_raw = json.loads(_decode(meta[5])) if meta[5] else []
            artifacts = [ArtifactRef.model_validate(a) for a in artifacts_raw]

            if task_kind in ("source", "processing"):
                ts_ms = extract_uuid7_timestamp_ms(tid)
                async with self.session_factory() as session:
                    conds = [
                        TaskRecord.task_id == tid,
                        TaskRecord.status.notin_(["SUCCEEDED", "DEAD"]),
                        TaskRecord.lease_generation < lease_gen,
                    ]
                    if ts_ms is not None:
                        day_start, day_end = day_bounds_utc_naive(ts_ms)
                        conds.extend(
                            [TaskRecord.created_at >= day_start, TaskRecord.created_at < day_end]
                        )
                    upd = (
                        update(TaskRecord)
                        .where(*conds)
                        .values(
                            status="RUNNING",
                            worker_id=req.worker_id,
                            worker_service=identity.service_name,
                            pulled_at=now_ms,
                            retry_count=int(row[4]),
                            lease_generation=lease_gen,
                        )
                    )
                    res = await session.execute(upd)
                    await session.commit()
                    if res.rowcount == 0:
                        await self.redis_mgr.get_script("discard_lease")(
                            keys=[self.redis_mgr.lease_key(queue), self.redis_mgr.task_key(tid)],
                            args=[tid, lease_gen],
                        )
                        continue

            tasks.append(
                PulledTask(
                    task_id=tid,
                    queue=queue,
                    priority=int(_decode(row[2])),
                    payload=_public_payload(task_kind, json.loads(_decode(row[3]))),
                    retry_count=int(_decode(row[4])),
                    max_retries=int(_decode(row[5])),
                    lease_expire_at_ms=int(_decode(row[6])),
                    scheduled_at_ms=int(_decode(row[7])),
                    lease_generation=lease_gen,
                    task_kind=task_kind,
                    task_type=_decode(row[9]) or None,
                    source_task_id=source_task_id,
                    completion_id=completion_id,
                    step=step,
                    recipient_service=recipient_service,
                    artifacts=artifacts,
                )
            )
        if tasks:
            TASKS_PULLED.labels(queue=req.queue).inc(len(tasks))
        return tasks

    async def _authorize_receipt(self, task_id: str, req, identity: Identity) -> tuple[dict, str]:
        identity.queue_scopes("pull", req.queue)
        raw = await self.redis.hgetall(self.redis_mgr.task_key(task_id))
        snapshot = {_decode(k): _decode(v) for k, v in raw.items()}
        if snapshot:
            queue = snapshot["queue"]
            business = snapshot.get("business_id") or None
            task_type = snapshot.get("task_type") or snapshot.get("collection_type") or None
            service = snapshot.get("worker_service")
            recipient = snapshot.get("recipient_service")
            kind = _task_kind(snapshot.get("task_kind", "normal"))
        else:
            async with self.session_factory() as session:
                row = await self._find_task_record(session, task_id)
            if row is None:
                raise HTTPException(409, "Task has no active lease")
            queue, business, task_type = row.queue, row.business_id, row.task_type
            service, recipient, kind = row.worker_service, row.recipient_service, row.task_kind
        identity.require("pull", business, queue, task_type)
        if (
            queue != req.queue
            or (service and service != identity.service_name)
            or (recipient and recipient != identity.service_name)
        ):
            raise forbidden()
        return snapshot, kind

    def _lease_args(self, task_id: str, req, identity: Identity, now_ms: int) -> list:
        return [
            task_id,
            req.queue,
            identity.service_name,
            req.worker_id,
            req.lease_generation,
            now_ms,
        ]

    @staticmethod
    def _check_lease_result(value: int) -> None:
        if value == -1:
            raise forbidden()
        if value == 0:
            raise HTTPException(409, "Task has no active lease for this execution")

    async def heartbeat(
        self,
        task_id: str,
        req: TaskHeartbeatRequest,
        *,
        identity: Identity,
        ip: str | None = None,
    ) -> TaskHeartbeatResponse:
        await self._authorize_receipt(task_id, req, identity)
        now_ms = _now_ms()
        value = int(
            await self.redis_mgr.get_script("heartbeat")(
                keys=[
                    self.redis_mgr.lease_key(req.queue),
                    self.redis_mgr.task_key(task_id),
                    self.redis_mgr.worker_key(req.queue, identity.service_name, req.worker_id),
                ],
                args=[
                    *self._lease_args(task_id, req, identity, now_ms),
                    (req.extend_seconds or 0) * 1000,
                    self.settings.default_lease_seconds * 1000,
                    self.settings.worker_heartbeat_ttl_seconds,
                    self._worker_activity_json(
                        req, identity, identity.queue_scopes("pull", req.queue), now_ms, ip
                    ),
                ],
            )
        )
        self._check_lease_result(value)
        return TaskHeartbeatResponse(
            task_id=task_id, lease_expire_at_ms=value, lease_generation=req.lease_generation
        )

    async def _find_task_record(
        self, session: AsyncSession, task_id: str, *, lock: bool = False
    ) -> TaskRecord | None:
        ts_ms = extract_uuid7_timestamp_ms(task_id)
        conditions = [TaskRecord.task_id == task_id]
        if ts_ms is not None:
            day_start, day_end = day_bounds_utc_naive(ts_ms)
            conditions.extend([TaskRecord.created_at >= day_start, TaskRecord.created_at < day_end])
        stmt = select(TaskRecord).where(*conditions)
        if lock:
            if session.get_bind().dialect.name == "sqlite":
                await session.execute(
                    update(TaskRecord).where(*conditions).values(task_id=TaskRecord.task_id)
                )
            stmt = stmt.with_for_update()
        return (await session.execute(stmt)).scalar_one_or_none()

    async def ack_task(
        self, task_id: str, req: TaskAckRequest, *, identity: Identity
    ) -> TaskAckResponse:
        snapshot, kind = await self._authorize_receipt(task_id, req, identity)
        artifacts = [a.model_dump() for a in req.artifacts]
        processing_result = (
            req.processing_result if req.processing_result is not None else req.summary
        )
        receipt = {
            "worker_id": req.worker_id,
            "duration_ms": req.duration_ms,
            "summary": req.summary,
            "artifacts": artifacts,
            "extra": req.extra,
        }
        receipt_hash = _compute_receipt_hash(
            req.summary,
            artifacts,
            req.extra,
            processing_result if kind == "processing" else None,
        )
        now_ms = _now_ms()
        keys = [self.redis_mgr.lease_key(req.queue), self.redis_mgr.task_key(task_id)]
        args = self._lease_args(task_id, req, identity, now_ms)
        ack_args = [
            self.settings.task_hash_ttl_seconds,
            json.dumps(receipt, ensure_ascii=False, separators=(",", ":")),
            receipt_hash,
        ]

        if kind == "normal":
            if snapshot:
                result = int(
                    await self.redis_mgr.get_script("ack")(keys=keys, args=[*args, *ack_args, "0"])
                )
                self._check_lease_result(result)
                if result == 1:
                    await self._enqueue_terminal_flush(
                        task_id, snapshot, "SUCCEEDED", receipt, req.worker_id, now_ms
                    )
                    TASKS_ACKED.labels(queue=req.queue).inc()
            else:
                async with self.session_factory() as session:
                    row = await self._find_task_record(session, task_id)
                    self._require_same_ack(row, req, identity, receipt_hash)
                result = 2
            return TaskAckResponse(
                task_id=task_id,
                status="SUCCEEDED",
                lease_generation=req.lease_generation,
                replayed=result == 2,
            )

        completion_id = None
        dispatch_count = 0
        async with self.session_factory.begin() as session:
            row = await self._find_task_record(session, task_id, lock=True)
            if row is None or row.queue != req.queue:
                raise HTTPException(409, "Task has no active lease")
            identity.require("pull", row.business_id, row.queue, row.task_type)
            if row.worker_service and row.worker_service != identity.service_name:
                raise forbidden()
            replayed = row.status == "SUCCEEDED"
            if replayed:
                self._require_same_ack(row, req, identity, receipt_hash)
                completion_id = row.completion_id
                if kind == "source":
                    completion = (
                        await session.get(TaskCompletion, completion_id)
                        if completion_id
                        else (
                            await session.scalars(
                                select(TaskCompletion).where(
                                    TaskCompletion.source_task_id == task_id
                                )
                            )
                        ).one_or_none()
                    )
                    if completion is None:
                        raise HTTPException(409, "Completion record is missing")
                    completion_id = completion.completion_id
                    dispatch_count = len(completion.recipient_snapshot)
                # Redis was already finalized or can be repaired below after a lost response.
                now_ms = row.finished_at
            else:
                if (
                    row.status != "RUNNING"
                    or row.worker_id != req.worker_id
                    or row.lease_generation != req.lease_generation
                ):
                    raise HTTPException(409, "Task has no active lease for this execution")
                if kind == "source":
                    await lock_subscription_scope(session, row.business_id, row.task_type)
                now_ms = _now_ms()
                active = await self.redis_mgr.get_script("check_lease")(
                    keys=keys,
                    args=self._lease_args(task_id, req, identity, now_ms),
                )
                self._check_lease_result(int(active))
                row.status = "SUCCEEDED"
                row.finished_at = now_ms
                row.receipt = receipt
                if kind == "processing":
                    row.processing_result = processing_result
                    completion_id = row.completion_id
                else:
                    completion_id, dispatch_count = await self._complete_source(
                        session, row, req, receipt_hash, artifacts, now_ms
                    )

        await self.redis_mgr.get_script("ack")(
            keys=keys,
            args=[*self._lease_args(task_id, req, identity, now_ms), *ack_args, "1"],
        )
        if not replayed:
            TASKS_ACKED.labels(queue=req.queue).inc()
        return TaskAckResponse(
            task_id=task_id,
            status="SUCCEEDED",
            completion_id=completion_id,
            lease_generation=req.lease_generation,
            replayed=replayed,
            dispatch_count=dispatch_count,
        )

    @staticmethod
    def _require_same_ack(row, req: TaskAckRequest, identity: Identity, receipt_hash: str) -> None:
        if row is not None and row.worker_service and row.worker_service != identity.service_name:
            raise forbidden()
        if (
            row is None
            or row.status != "SUCCEEDED"
            or row.worker_service != identity.service_name
            or row.worker_id != req.worker_id
            or row.lease_generation != req.lease_generation
        ):
            raise HTTPException(409, "Task has no accepted ACK for this execution")
        receipt = row.receipt or {}
        if (
            _compute_receipt_hash(
                receipt.get("summary", {}),
                receipt.get("artifacts", []),
                receipt.get("extra", {}),
                row.processing_result,
            )
            != receipt_hash
        ):
            raise HTTPException(409, "ACK conflicts with the accepted result")

    async def _complete_source(
        self, session, row, req, receipt_hash, artifacts, now_ms
    ) -> tuple[str, int]:
        completion_id = uuid7(now_ms)
        requested = row.requested_recipients
        if requested is None and row.payload:
            legacy = row.payload.get("_requested_recipients")
            requested = legacy if isinstance(legacy, list) else None
        recipients = await snapshot_recipients(
            session, self.registry, row.business_id, row.task_type, requested
        )
        cfg = self.registry.get_task_type(row.business_id, row.task_type)
        retention = cfg.artifact_retention_seconds if cfg else None
        row.completion_id = completion_id
        session.add(
            TaskCompletion(
                completion_id=completion_id,
                source_task_id=row.task_id,
                lease_generation=req.lease_generation,
                business_id=row.business_id,
                task_type=row.task_type,
                business_request_id=row.business_request_id,
                worker_id=req.worker_id,
                worker_service=row.worker_service,
                completed_at_ms=now_ms,
                receipt_hash=receipt_hash,
                summary=req.summary,
                artifacts=artifacts,
                extra=req.extra,
                recipient_snapshot=[rule.model_dump() for rule in recipients],
                artifact_expires_at_ms=now_ms
                + (retention or self.settings.default_artifact_retention_seconds) * 1000,
                artifact_deleted=False,
            )
        )
        for rule in recipients:
            dedup = hashlib.sha256(
                json.dumps([completion_id, rule.recipient_service, rule.step]).encode()
            ).hexdigest()
            session.add(
                DeliveryDispatch(
                    dispatch_id=uuid7(now_ms),
                    completion_id=completion_id,
                    source_task_id=row.task_id,
                    business_id=row.business_id,
                    recipient_service=rule.recipient_service,
                    step=rule.step,
                    target_queue=rule.queue,
                    delivery_mode=rule.delivery_mode,
                    dispatch_kind="normal",
                    normal_dedup_key=dedup,
                    status="PENDING",
                    kafka_topic=rule.kafka_topic,
                    completed_at_ms=now_ms,
                )
            )
        return completion_id, len(recipients)

    async def nack_task(
        self, task_id: str, req: TaskNackRequest, *, identity: Identity
    ) -> TaskNackResponse:
        snapshot, kind = await self._authorize_receipt(task_id, req, identity)
        receipt = {
            "worker_id": req.worker_id,
            "error_code": req.error_code,
            "error_message": req.error_message,
            "retryable": req.retryable,
        }

        async def transition():
            now_ms = _now_ms()
            result = await self.redis_mgr.get_script("nack")(
                keys=[
                    self.redis_mgr.lease_key(req.queue),
                    self.redis_mgr.task_key(task_id),
                    self.redis_mgr.delay_key(self.redis_mgr.delay_shard_for(task_id)),
                    self.redis_mgr.dlq_key(req.queue),
                ],
                args=[
                    *self._lease_args(task_id, req, identity, now_ms),
                    "1" if req.retryable else "0",
                    self.settings.retry_base_delay_ms,
                    self.settings.task_hash_ttl_seconds,
                    json.dumps(receipt, ensure_ascii=False, separators=(",", ":")),
                ],
            )
            outcome, retry_count, timestamp = _decode(result[0]), int(result[1]), int(result[2])
            if outcome == "FORBIDDEN":
                raise forbidden()
            if outcome == "IGNORED":
                raise HTTPException(409, "Task has no active lease for this execution")
            return outcome, retry_count, timestamp

        if kind in ("source", "processing"):
            async with self.session_factory.begin() as session:
                row = await self._find_task_record(session, task_id, lock=True)
                if (
                    row is None
                    or row.status != "RUNNING"
                    or row.lease_generation != req.lease_generation
                ):
                    raise HTTPException(409, "Task has no active lease for this execution")
                outcome, retry_count, timestamp = await transition()
                row.retry_count = retry_count
                row.receipt = receipt
                if outcome == "DEAD":
                    row.status = "DEAD"
                    row.finished_at = timestamp
                else:
                    row.status = "DELAYED"
                    row.scheduled_at = timestamp
        else:
            outcome, retry_count, timestamp = await transition()
            if outcome == "DEAD":
                await self._enqueue_terminal_flush(
                    task_id, snapshot, "DEAD", receipt, req.worker_id, timestamp, retry_count
                )
        TASKS_NACKED.labels(queue=req.queue, outcome=outcome).inc()
        return TaskNackResponse(
            task_id=task_id,
            outcome=outcome,
            retry_count=retry_count,
            next_run_at_ms=timestamp if outcome == "RETRY" else None,
        )

    async def _enqueue_terminal_flush(
        self,
        task_id: str,
        snapshot: dict[Any, Any],
        final_status: str,
        receipt_dict: dict[str, Any],
        worker_id: str,
        finished_at_ms: int,
        retry_count: int | None = None,
    ) -> None:
        decoded = {_decode(k): _decode(v) for k, v in snapshot.items()}
        created_ms = int(
            decoded.get("created_at_ms") or extract_uuid7_timestamp_ms(task_id) or finished_at_ms
        )
        rc = retry_count if retry_count is not None else int(decoded.get("retry_count", 0))
        await self.flusher.enqueue(
            {
                "task_id": task_id,
                "queue": decoded.get("queue", "default"),
                "priority": int(decoded.get("priority", 5)),
                "status": final_status,
                "idempotency_key": decoded.get("idempotency_key") or None,
                "payload": json.loads(decoded.get("payload", "{}")),
                "receipt": receipt_dict,
                "retry_count": rc,
                "worker_id": worker_id,
                "worker_service": decoded.get("worker_service") or None,
                "business_id": decoded.get("business_id") or None,
                "business_request_id": decoded.get("business_request_id") or None,
                "task_type": decoded.get("task_type") or decoded.get("collection_type") or None,
                "lease_generation": int(decoded.get("lease_generation", 0)),
                "lease_seconds": int(decoded["lease_ttl"]) if decoded.get("lease_ttl") else None,
                "max_retries": int(decoded["max_retries"]) if decoded.get("max_retries") else None,
                "task_kind": "normal",
                "requested_recipients": [],
                "scheduled_at": int(decoded.get("scheduled_at", created_ms)),
                "pulled_at": int(decoded["pulled_at"]) if decoded.get("pulled_at") else None,
                "finished_at": finished_at_ms,
                "created_at": _ms_to_naive_utc(created_ms),
            }
        )

    def _row_to_detail(self, row: TaskRecord) -> TaskDetailResponse:
        return TaskDetailResponse(
            task_id=row.task_id,
            queue=row.queue,
            priority=row.priority,
            status=row.status,
            payload=_public_payload(row.task_kind, row.payload),
            receipt=row.receipt,
            retry_count=row.retry_count,
            worker_id=row.worker_id,
            scheduled_at_ms=row.scheduled_at,
            pulled_at_ms=row.pulled_at,
            finished_at_ms=row.finished_at,
            created_at=row.created_at,
            source="db",
            task_kind=row.task_kind,
            business_id=row.business_id,
            business_request_id=row.business_request_id,
            task_type=row.task_type,
            lease_generation=row.lease_generation,
            source_task_id=row.source_task_id,
            completion_id=row.completion_id,
            recipient_service=row.recipient_service,
            step=row.step,
            parent_task_id=row.parent_task_id,
            rerun_of_task_id=row.rerun_of_task_id,
            processing_result=row.processing_result,
        )

    async def _get_task(self, task_id: str) -> TaskDetailResponse:
        raw = await self.redis.hgetall(self.redis_mgr.task_key(task_id))
        if raw:
            d = {_decode(k): _decode(v) for k, v in raw.items()}
            task_kind = _task_kind(d.get("task_kind", "normal"))
            if task_kind in ("source", "processing"):
                async with self.session_factory() as session:
                    db_row = await self._find_task_record(session, task_id)
                    if db_row is not None and (
                        db_row.status in ("SUCCEEDED", "DEAD")
                        or db_row.lease_generation > int(d.get("lease_generation", 0))
                    ):
                        return self._row_to_detail(db_row)
            created_ms = int(d.get("created_at_ms") or extract_uuid7_timestamp_ms(task_id) or 0)
            return TaskDetailResponse(
                task_id=task_id,
                queue=d["queue"],
                priority=int(d.get("priority", 5)),
                status=d["status"],
                payload=_public_payload(task_kind, json.loads(d.get("payload", "{}"))),
                receipt=json.loads(d["receipt"]) if d.get("receipt") else None,
                retry_count=int(d.get("retry_count", 0)),
                worker_id=d.get("worker_id") or None,
                scheduled_at_ms=int(d.get("scheduled_at", created_ms)),
                pulled_at_ms=int(d["pulled_at"]) if d.get("pulled_at") else None,
                finished_at_ms=int(d["finished_at"]) if d.get("finished_at") else None,
                created_at=_ms_to_naive_utc(created_ms) if created_ms else None,
                source="redis",
                task_kind=task_kind,
                business_id=d.get("business_id") or None,
                business_request_id=d.get("business_request_id") or None,
                task_type=d.get("task_type") or d.get("collection_type") or None,
                lease_generation=int(d.get("lease_generation", 0)),
                source_task_id=d.get("source_task_id") or None,
                completion_id=d.get("completion_id") or None,
                recipient_service=d.get("recipient_service") or None,
                step=d.get("step") or None,
                parent_task_id=d.get("parent_task_id") or None,
                rerun_of_task_id=d.get("rerun_of_task_id") or None,
            )

        # Fallback to MySQL with UUIDv7 partition pruning
        async with self.session_factory() as session:
            row = await self._find_task_record(session, task_id)
            if row is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
            return self._row_to_detail(row)

    async def get_task(self, task_id: str, identity: Identity) -> TaskDetailResponse:
        identity.scopes("task_read")
        detail = await self._get_task(task_id)
        identity.require("task_read", detail.business_id, detail.queue, detail.task_type)
        return detail

    async def list_tasks(
        self,
        identity: Identity,
        queue: str | None = None,
        status_filter: str | None = None,
        worker_id: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> TaskListResponse:
        now_utc = datetime.now(UTC).replace(tzinfo=None)
        eff_end = _naive_utc(end_time) if end_time is not None else now_utc
        eff_start = (
            _naive_utc(start_time) if start_time is not None else eff_end - timedelta(hours=24)
        )
        if eff_start > eff_end:
            raise HTTPException(status_code=400, detail="start_time must precede end_time")
        if (eff_end - eff_start) > timedelta(days=self.settings.max_query_span_days):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Query time span cannot exceed {self.settings.max_query_span_days} days",
            )

        if queue:
            identity.queue_scopes("task_read", queue)
        conditions = [
            TaskRecord.created_at >= eff_start,
            TaskRecord.created_at <= eff_end,
            identity.condition(
                "task_read",
                business=TaskRecord.business_id,
                queue=TaskRecord.queue,
                task_type=TaskRecord.task_type,
            ),
        ]
        if queue:
            conditions.append(TaskRecord.queue == queue)
        if status_filter:
            conditions.append(TaskRecord.status == status_filter)
        if worker_id:
            conditions.append(TaskRecord.worker_id == worker_id)
        if cursor:
            c_time, c_tid = _decode_cursor(cursor)
            conditions.append(
                or_(
                    TaskRecord.created_at < c_time,
                    and_(TaskRecord.created_at == c_time, TaskRecord.task_id < c_tid),
                )
            )

        stmt = (
            select(TaskRecord)
            .where(*conditions)
            .order_by(desc(TaskRecord.created_at), desc(TaskRecord.task_id))
            .limit(limit + 1)
        )
        async with self.session_factory() as session:
            rows = (await session.execute(stmt)).scalars().all()

        has_more = len(rows) > limit
        page_rows = rows[:limit]
        items = [self._row_to_detail(r) for r in page_rows]
        next_cursor = (
            _encode_cursor(page_rows[-1].created_at, page_rows[-1].task_id) if has_more else None
        )
        return TaskListResponse(items=items, next_cursor=next_cursor)

    async def _scoped_details(self, ids, scopes, queue: str):
        seen = set()
        for tid in ids:
            task_id = _decode(tid)
            if task_id in seen:
                continue
            seen.add(task_id)
            try:
                detail = await self._get_task(task_id)
            except HTTPException as exc:
                if exc.status_code != 404:
                    raise
                continue
            if detail.queue == queue and any(
                not grant.business_id or detail.business_id == grant.business_id for grant in scopes
            ):
                yield detail

    async def get_queue_stats(self, queue: str, identity: Identity) -> QueueStatsResponse:
        scopes = identity.queue_scopes("monitor", queue)
        businesses = {g.business_id for g in scopes}
        pipe = self.redis.pipeline(transaction=False)
        if "" in businesses:
            pipe.zcard(self.redis_mgr.ready_key(queue))
            pipe.zcard(self.redis_mgr.lease_key(queue))
            pipe.llen(self.redis_mgr.dlq_key(queue))
            counts = await pipe.execute()
        else:
            pipe.zrange(self.redis_mgr.ready_key(queue), 0, -1)
            pipe.zrange(self.redis_mgr.lease_key(queue), 0, -1)
            pipe.lrange(self.redis_mgr.dlq_key(queue), 0, -1)
            counts = [
                sum([1 async for _ in self._scoped_details(ids, scopes, queue)])
                for ids in await pipe.execute()
            ]

        workers = await self.worker_activity(queue)
        worker_count = sum(
            worker["queue"] == queue
            and ("" in businesses or bool(businesses.intersection(worker["business_ids"])))
            for worker in workers
        )
        return QueueStatsResponse(
            queue=queue,
            ready_count=counts[0],
            running_count=counts[1],
            dlq_count=counts[2],
            worker_count=worker_count,
        )

    async def list_dlq(
        self, queue: str, limit: int, identity: Identity
    ) -> list[TaskDetailResponse]:
        scopes = identity.queue_scopes("dlq_read", queue)
        return await self._visible_dlq(queue, limit, scopes)

    async def _visible_dlq(self, queue: str, limit: int, scopes) -> list[TaskDetailResponse]:
        key = self.redis_mgr.dlq_key(queue)
        results = []
        for offset in range(0, await self.redis.llen(key), 100):
            ids = await self.redis.lrange(key, offset, offset + 99)
            async for detail in self._scoped_details(ids, scopes, queue):
                if detail.status == "DEAD":
                    results.append(detail)
                    if len(results) == limit:
                        return results
        return results

    async def retry_task(
        self, task_id: str, req: TaskRetryRequest, identity: Identity
    ) -> TaskSubmitResponse:
        detail = await self.get_task(task_id, identity)
        identity.require("task_retry", detail.business_id, detail.queue, detail.task_type)
        return await self._retry_task(detail, req)

    async def _retry_task(
        self, detail: TaskDetailResponse, req: TaskRetryRequest
    ) -> TaskSubmitResponse:
        if detail.status != "DEAD" and not (
            detail.status == "SUCCEEDED" and detail.task_kind == "normal"
        ):
            raise HTTPException(409, f"Cannot retry task in status {detail.status}")
        task_id = detail.task_id
        now_ms = _now_ms()
        priority = req.priority if req.priority is not None else detail.priority
        payload = req.payload if req.payload is not None else detail.payload
        scheduled_at = now_ms + req.delay_seconds * 1000
        created_ms = extract_uuid7_timestamp_ms(task_id) or int(
            detail.created_at.replace(tzinfo=UTC).timestamp() * 1000
        )
        async with self.session_factory.begin() as session:
            row = await self._find_task_record(session, task_id, lock=True)
            if row is not None and detail.task_kind != "normal" and row.status != "DEAD":
                raise HTTPException(409, f"Cannot retry task in status {row.status}")
            fields = {
                "task_id": task_id,
                "queue": detail.queue,
                "priority": str(priority),
                "payload": json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
                "retry_count": "0",
                "scheduled_at": str(scheduled_at),
                "created_at_ms": str(created_ms),
                "business_id": detail.business_id or "",
                "business_request_id": detail.business_request_id or "",
                "task_type": detail.task_type or "",
                "task_kind": detail.task_kind,
                "lease_generation": str(detail.lease_generation + 1),
            }
            if row and row.lease_seconds is not None:
                fields["lease_ttl"] = str(row.lease_seconds)
            if row and row.max_retries is not None:
                fields["max_retries"] = str(row.max_retries)
            for key in (
                "source_task_id",
                "completion_id",
                "step",
                "recipient_service",
                "parent_task_id",
                "rerun_of_task_id",
            ):
                value = getattr(detail, key)
                if value:
                    fields[key] = value
            if detail.task_kind == "processing":
                fields["artifacts"] = json.dumps(detail.payload.get("artifacts", []))
            _, new_status, enqueued = await self._enqueue_task(
                task_id,
                detail.queue,
                priority,
                now_ms,
                scheduled_at,
                fields,
                mode="retry",
            )
            if not enqueued:
                raise HTTPException(409, f"Cannot retry task in status {new_status}")
            if row:
                row.status, row.priority, row.payload, row.scheduled_at = (
                    new_status,
                    priority,
                    payload,
                    scheduled_at,
                )
                row.worker_id = row.worker_service = row.pulled_at = row.finished_at = None
                row.retry_count = 0
                row.lease_generation = detail.lease_generation + 1
            else:
                session.add(
                    TaskRecord(
                        task_id=task_id,
                        queue=detail.queue,
                        status=new_status,
                        priority=priority,
                        payload=payload,
                        receipt=detail.receipt,
                        idempotency_key=None,
                        retry_count=0,
                        scheduled_at=scheduled_at,
                        created_at=_ms_to_naive_utc(created_ms),
                        business_id=detail.business_id,
                        business_request_id=detail.business_request_id,
                        task_type=detail.task_type,
                        lease_generation=detail.lease_generation + 1,
                        task_kind=detail.task_kind,
                    )
                )
        return TaskSubmitResponse(
            task_id=task_id,
            queue=detail.queue,
            status=new_status,
            task_kind=detail.task_kind,
            task_type=detail.task_type,
            business_id=detail.business_id,
        )

    async def retry_dlq_batch(
        self, queue: str, req: DlqBatchRetryRequest, identity: Identity
    ) -> list[TaskSubmitResponse]:
        scopes = identity.queue_scopes("dlq_retry", queue)
        if req.task_ids is not None:
            details = []
            for task_id in dict.fromkeys(req.task_ids):
                detail = await self._get_task(task_id)
                if detail.queue != queue:
                    raise forbidden()
                identity.require("dlq_retry", detail.business_id, detail.queue)
                if detail.status != "DEAD":
                    raise HTTPException(409, "Only dead-letter tasks can be retried in a batch")
                details.append(detail)
        else:
            details = await self._visible_dlq(queue, req.limit, scopes)

        results = []
        for detail in details:
            try:
                results.append(await self._retry_task(detail, TaskRetryRequest()))
            except HTTPException as exc:
                if exc.status_code != 409:
                    raise
        return results
