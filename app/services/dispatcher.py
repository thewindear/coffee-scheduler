import base64
import hashlib
import json
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import and_, desc, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.core.delivery_config import DeliveryRegistry, IdentityConfig
from app.core.metrics import TASKS_ACKED, TASKS_NACKED, TASKS_PULLED, TASKS_SUBMITTED
from app.core.redis_client import RedisLuaManager
from app.core.uuidv7 import day_bounds_utc_naive, extract_uuid7_timestamp_ms, uuid7
from app.models.tables import DeliveryDispatch, TaskCompletion, TaskRecord
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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid cursor") from exc


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
        retry: bool = False,
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
                self.redis_mgr.prefix, task_id, queue, priority, now_ms, scheduled_at_ms,
                "retry" if retry else "submit", self.settings.idempotency_ttl_seconds,
                *(value for pair in fields.items() for value in pair),
            ],
        )
        return _decode(result[0]), _decode(result[1]), bool(result[2])

    async def submit_task(
        self,
        req: TaskSubmitRequest,
        *,
        cron_occurrence: tuple[int, int] | None = None,
        identity: IdentityConfig | None = None,
    ) -> TaskSubmitResponse:
        now_ms = _now_ms()
        task_id = uuid7(now_ms)
        scheduled_at_ms = (
            req.run_at_ms if req.run_at_ms is not None else now_ms + req.delay_seconds * 1000
        )

        task_kind = "normal"
        business_id: str | None = None
        collection_type: str | None = req.collection_type
        requested_recipients: list[str] | None = req.recipient_services
        target_queue = req.queue

        if collection_type is not None:
            cfg, _ = self.registry.resolve_submission(
                identity, collection_type, req.recipient_services
            )
            target_queue = cfg.queue
            business_id = cfg.business_id
            task_kind = "collection" if cfg.delivery_enabled else "normal"
        else:
            assert target_queue is not None
            if self.registry.is_protected_queue(target_queue):
                self.registry.authorize_queue_pull(identity, target_queue)

        assert target_queue is not None
        idempotency_key = (
            self.redis_mgr.idemp_key(req.idempotency_key) if req.idempotency_key else None
        )
        if cron_occurrence is not None:
            job_id, due_at = cron_occurrence
            idempotency_key = f"{self.redis_mgr.prefix}:cron_occurrence:{job_id}:{due_at}"

        redis_fields: dict[str, str] = {
            "task_id": task_id,
            "queue": target_queue,
            "priority": str(req.priority),
            "idempotency_key": req.idempotency_key or "",
            "payload": json.dumps(req.payload, separators=(",", ":"), ensure_ascii=False),
            "retry_count": "0",
            "max_retries": str(req.max_retries),
            "lease_ttl": str(req.lease_seconds),
            "scheduled_at": str(scheduled_at_ms),
            "created_at_ms": str(now_ms),
            "task_kind": task_kind,
            "lease_generation": "0",
        }
        if business_id:
            redis_fields["business_id"] = business_id
        if collection_type:
            redis_fields["collection_type"] = collection_type
        if req.business_request_id:
            redis_fields["business_request_id"] = req.business_request_id
        if requested_recipients:
            redis_fields["requested_recipients"] = json.dumps(requested_recipients)

        task_id, initial_status, created = await self._enqueue_task(
            task_id,
            target_queue,
            req.priority,
            now_ms,
            scheduled_at_ms,
            redis_fields,
            idempotency_key=idempotency_key,
        )
        if not created:
            return TaskSubmitResponse(
                task_id=task_id,
                queue=target_queue,
                status=initial_status,
                deduplicated=True,
                task_kind=task_kind,
                business_id=business_id,
                collection_type=collection_type,
            )

        if task_kind == "collection":
            db_payload = dict(req.payload)
            if requested_recipients:
                db_payload["_requested_recipients"] = requested_recipients
            async with self.session_factory() as session:
                session.add(
                    TaskRecord(
                        task_id=task_id,
                        queue=target_queue,
                        priority=req.priority,
                        status=initial_status,
                        idempotency_key=req.idempotency_key,
                        payload=db_payload,
                        receipt=None,
                        retry_count=0,
                        worker_id=None,
                        scheduled_at=scheduled_at_ms,
                        pulled_at=None,
                        finished_at=None,
                        created_at=_ms_to_naive_utc(now_ms),
                        task_kind=task_kind,
                        business_id=business_id,
                        business_request_id=req.business_request_id,
                        collection_type=collection_type,
                        lease_generation=0,
                    )
                )
                await session.commit()
        else:
            await self.flusher.enqueue(
                {
                    "task_id": task_id,
                    "queue": target_queue,
                    "priority": req.priority,
                    "status": initial_status,
                    "idempotency_key": req.idempotency_key,
                    "payload": req.payload,
                    "receipt": None,
                    "retry_count": 0,
                    "worker_id": None,
                    "scheduled_at": scheduled_at_ms,
                    "pulled_at": None,
                    "finished_at": None,
                    "created_at": _ms_to_naive_utc(now_ms),
                    "task_kind": "normal",
                }
            )
        TASKS_SUBMITTED.labels(queue=target_queue).inc()
        return TaskSubmitResponse(
            task_id=task_id,
            queue=target_queue,
            status=initial_status,
            task_kind=task_kind,
            business_id=business_id,
            collection_type=collection_type,
        )

    async def submit_batch(
        self, items: list[TaskSubmitRequest], *, identity: IdentityConfig | None = None
    ) -> list[TaskSubmitResponse]:
        results = []
        for item in items:
            results.append(await self.submit_task(item, identity=identity))
        return results

    async def pull_tasks(
        self, req: TaskPullRequest, *, identity: IdentityConfig | None = None
    ) -> list[PulledTask]:
        self.registry.authorize_queue_pull(identity, req.queue)
        now_ms = _now_ms()
        script = self.redis_mgr.get_script("pull")
        raw_results = await script(
            keys=[
                self.redis_mgr.ready_key(req.queue),
                self.redis_mgr.lease_key(req.queue),
                self.redis_mgr.worker_key(req.queue, req.worker_id),
            ],
            args=[
                self.redis_mgr.prefix,
                str(now_ms),
                str(req.batch_size),
                req.worker_id,
                str(self.settings.default_lease_seconds * 1000),
                str(self.settings.worker_heartbeat_ttl_seconds),
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
            task_kind = _decode(meta[0]) if meta[0] else "normal"
            source_task_id = _decode(meta[1]) if meta[1] else None
            completion_id = _decode(meta[2]) if meta[2] else None
            step = _decode(meta[3]) if meta[3] else None
            recipient_service = _decode(meta[4]) if meta[4] else None
            artifacts_raw = json.loads(_decode(meta[5])) if meta[5] else []
            artifacts = [ArtifactRef.model_validate(a) for a in artifacts_raw]

            if task_kind in ("collection", "processing"):
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
                            pulled_at=now_ms,
                            lease_generation=lease_gen,
                        )
                    )
                    res = await session.execute(upd)
                    await session.commit()
                    if res.rowcount == 0:
                        await self.redis.zrem(self.redis_mgr.lease_key(queue), tid)
                        await self.redis.delete(self.redis_mgr.task_key(tid))
                        continue

            tasks.append(
                PulledTask(
                    task_id=tid,
                    queue=queue,
                    priority=int(_decode(row[2])),
                    payload=json.loads(_decode(row[3])),
                    retry_count=int(_decode(row[4])),
                    max_retries=int(_decode(row[5])),
                    lease_expire_at_ms=int(_decode(row[6])),
                    scheduled_at_ms=int(_decode(row[7])),
                    lease_generation=lease_gen,
                    task_kind=task_kind,
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

    async def heartbeat(
        self,
        task_id: str,
        req: TaskHeartbeatRequest,
        *,
        identity: IdentityConfig | None = None,
    ) -> TaskHeartbeatResponse:
        self.registry.authorize_queue_pull(identity, req.queue)
        now_ms = _now_ms()
        extend_ms = (req.extend_seconds * 1000) if req.extend_seconds else 0
        expected_gen = str(req.lease_generation) if req.lease_generation is not None else ""
        script = self.redis_mgr.get_script("heartbeat")
        res = await script(
            keys=[
                self.redis_mgr.lease_key(req.queue),
                self.redis_mgr.task_key(task_id),
                self.redis_mgr.worker_key(req.queue, req.worker_id),
            ],
            args=[
                task_id,
                req.worker_id,
                str(now_ms),
                str(extend_ms),
                str(self.settings.default_lease_seconds * 1000),
                str(self.settings.worker_heartbeat_ttl_seconds),
                expected_gen,
            ],
        )
        val = int(res)
        if val == 0:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Task is not actively running, lease has expired, or generation is stale",
            )
        if val == -1:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Task lease is owned by a different worker_id",
            )
        current_gen_raw = await self.redis.hget(self.redis_mgr.task_key(task_id), "lease_generation")
        current_gen = int(_decode(current_gen_raw)) if current_gen_raw else (req.lease_generation or 0)
        return TaskHeartbeatResponse(
            task_id=task_id, lease_expire_at_ms=val, lease_generation=current_gen
        )

    async def _find_task_record(self, session: AsyncSession, task_id: str) -> TaskRecord | None:
        ts_ms = extract_uuid7_timestamp_ms(task_id)
        stmt = select(TaskRecord).where(TaskRecord.task_id == task_id)
        if ts_ms is not None:
            day_start, day_end = day_bounds_utc_naive(ts_ms)
            stmt = stmt.where(TaskRecord.created_at >= day_start, TaskRecord.created_at < day_end)
        return (await session.execute(stmt)).scalar_one_or_none()

    async def ack_task(
        self,
        task_id: str,
        req: TaskAckRequest,
        *,
        identity: IdentityConfig | None = None,
    ) -> TaskAckResponse:
        self.registry.authorize_queue_pull(identity, req.queue)
        now_ms = _now_ms()
        artifacts_list = [a.model_dump() for a in req.artifacts]
        receipt_dict = {
            "worker_id": req.worker_id,
            "duration_ms": req.duration_ms,
            "summary": req.summary,
            "artifacts": artifacts_list,
            "extra": req.extra,
        }
        receipt_json = json.dumps(receipt_dict, separators=(",", ":"), ensure_ascii=False)
        expected_gen = str(req.lease_generation) if req.lease_generation is not None else ""

        task_k = self.redis_mgr.task_key(task_id)
        snapshot = await self.redis.hgetall(task_k)
        decoded_snap = {_decode(k): _decode(v) for k, v in (snapshot or {}).items()}
        task_kind = decoded_snap.get("task_kind")

        if task_kind is None:
            async with self.session_factory() as session:
                db_row = await self._find_task_record(session, task_id)
                task_kind = db_row.task_kind if db_row is not None else "normal"

        if task_kind == "normal":
            script = self.redis_mgr.get_script("ack")
            ok = await script(
                keys=[self.redis_mgr.lease_key(req.queue), task_k],
                args=[
                    task_id,
                    str(now_ms),
                    str(self.settings.task_hash_ttl_seconds),
                    receipt_json,
                    req.worker_id,
                    expected_gen,
                ],
            )
            if int(ok) == 0:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Task is not in RUNNING state",
                )

            await self._enqueue_terminal_flush(
                task_id, snapshot, "SUCCEEDED", receipt_dict, req.worker_id, now_ms
            )
            TASKS_ACKED.labels(queue=req.queue).inc()
            return TaskAckResponse(task_id=task_id, status="SUCCEEDED")

        # Authoritative MySQL transaction for collection and processing tasks
        receipt_hash = _compute_receipt_hash(
            req.summary,
            artifacts_list,
            req.extra,
            req.processing_result if task_kind == "processing" else None,
        )
        completion_id: str | None = None
        replayed = False
        dispatch_count = 0
        accepted_gen = 0

        async with self.session_factory() as session:
            row = await self._find_task_record(session, task_id)
            if row is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
            if row.queue != req.queue:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Task does not belong to the specified queue",
                )

            if row.status == "SUCCEEDED":
                if row.task_kind == "collection":
                    comp = (
                        await session.execute(
                            select(TaskCompletion).where(
                                TaskCompletion.collection_task_id == task_id
                            )
                        )
                    ).scalar_one_or_none()
                    if (
                        comp is not None
                        and comp.receipt_hash == receipt_hash
                        and comp.worker_id == req.worker_id
                        and (
                            req.lease_generation is None
                            or comp.lease_generation == req.lease_generation
                        )
                    ):
                        completion_id = comp.completion_id
                        accepted_gen = comp.lease_generation
                        replayed = True
                    else:
                        raise HTTPException(
                            status_code=status.HTTP_409_CONFLICT,
                            detail="Conflicting or stale ACK for already completed collection task",
                        )
                else:
                    existing_receipt = row.receipt or {}
                    existing_hash = _compute_receipt_hash(
                        existing_receipt.get("summary", {}),
                        existing_receipt.get("artifacts", []),
                        existing_receipt.get("extra", {}),
                        row.processing_result,
                    )
                    if (
                        existing_hash == receipt_hash
                        and row.worker_id == req.worker_id
                        and (
                            req.lease_generation is None
                            or row.lease_generation == req.lease_generation
                        )
                    ):
                        completion_id = row.completion_id
                        accepted_gen = row.lease_generation
                        replayed = True
                    else:
                        raise HTTPException(
                            status_code=status.HTTP_409_CONFLICT,
                            detail="Conflicting or stale ACK for already completed processing task",
                        )
            elif row.status == "DEAD":
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Task is already in DEAD state",
                )
            else:
                if req.lease_generation is not None and row.lease_generation != req.lease_generation:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="Stale lease_generation",
                    )
                if row.worker_id != req.worker_id or row.status != "RUNNING":
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="Task is not actively owned by this worker_id",
                    )
                accepted_gen = row.lease_generation
                row.status = "SUCCEEDED"
                row.finished_at = now_ms
                row.receipt = receipt_dict
                row.worker_id = req.worker_id

                if row.task_kind == "processing":
                    row.processing_result = req.processing_result or req.summary
                    completion_id = row.completion_id
                elif row.task_kind == "collection":
                    completion_id = uuid7(now_ms)
                    req_recips = (row.payload or {}).get("_requested_recipients")
                    recipients = self.registry.snapshot_recipients_for_completion(
                        row.business_id or "",
                        row.collection_type or "",
                        req_recips,
                    )
                    ct_cfg = self.registry.get_collection_type(
                        row.business_id or "", row.collection_type or ""
                    )
                    retention_s = (
                        ct_cfg.artifact_retention_seconds
                        if ct_cfg and ct_cfg.artifact_retention_seconds is not None
                        else self.settings.default_artifact_retention_seconds
                    )
                    expires_at_ms = now_ms + retention_s * 1000
                    session.add(
                        TaskCompletion(
                            completion_id=completion_id,
                            collection_task_id=task_id,
                            lease_generation=accepted_gen,
                            business_id=row.business_id or "",
                            collection_type=row.collection_type or "",
                            business_request_id=row.business_request_id,
                            worker_id=req.worker_id,
                            completed_at_ms=now_ms,
                            receipt_hash=receipt_hash,
                            summary=req.summary,
                            artifacts=artifacts_list,
                            extra=req.extra,
                            recipient_snapshot=[r.model_dump() for r in recipients],
                            artifact_expires_at_ms=expires_at_ms,
                            artifact_deleted=False,
                        )
                    )
                    for rule in recipients:
                        dispatch_id = uuid7(now_ms)
                        session.add(
                            DeliveryDispatch(
                                dispatch_id=dispatch_id,
                                completion_id=completion_id,
                                collection_task_id=task_id,
                                business_id=row.business_id or "",
                                recipient_service=rule.recipient_service,
                                step=rule.step,
                                target_queue=rule.queue,
                                delivery_mode=rule.delivery_mode,
                                dispatch_kind="normal",
                                normal_dedup_key=f"{completion_id}:{rule.recipient_service}:{rule.step}",
                                status="PENDING",
                                kafka_topic=rule.kafka_topic,
                                completed_at_ms=now_ms,
                            )
                        )
                        dispatch_count += 1
                await session.commit()

        # Update Redis only AFTER MySQL transaction commits
        if not replayed:
            script = self.redis_mgr.get_script("ack")
            ok = await script(
                keys=[self.redis_mgr.lease_key(req.queue), task_k],
                args=[
                    task_id,
                    str(now_ms),
                    str(self.settings.task_hash_ttl_seconds),
                    receipt_json,
                    req.worker_id,
                    expected_gen,
                ],
            )
            if int(ok) == 0:
                await self.redis.zrem(self.redis_mgr.lease_key(req.queue), task_id)
                if await self.redis.exists(task_k):
                    await self.redis.hset(
                        task_k,
                        mapping={
                            "status": "SUCCEEDED",
                            "finished_at": str(now_ms),
                            "receipt": receipt_json,
                            "worker_id": req.worker_id,
                        },
                    )
                    await self.redis.expire(task_k, self.settings.task_hash_ttl_seconds)
            TASKS_ACKED.labels(queue=req.queue).inc()

        return TaskAckResponse(
            task_id=task_id,
            status="SUCCEEDED",
            completion_id=completion_id,
            lease_generation=accepted_gen,
            replayed=replayed,
            dispatch_count=dispatch_count,
        )

    async def nack_task(
        self,
        task_id: str,
        req: TaskNackRequest,
        *,
        identity: IdentityConfig | None = None,
    ) -> TaskNackResponse:
        self.registry.authorize_queue_pull(identity, req.queue)
        now_ms = _now_ms()
        receipt_dict = {
            "worker_id": req.worker_id,
            "error_code": req.error_code,
            "error_message": req.error_message,
            "retryable": req.retryable,
        }
        receipt_json = json.dumps(receipt_dict, separators=(",", ":"), ensure_ascii=False)
        expected_gen = str(req.lease_generation) if req.lease_generation is not None else ""

        task_k = self.redis_mgr.task_key(task_id)
        snapshot = await self.redis.hgetall(task_k)
        decoded_snap = {_decode(k): _decode(v) for k, v in (snapshot or {}).items()}
        task_kind = decoded_snap.get("task_kind", "normal")
        shard = self.redis_mgr.delay_shard_for(task_id)
        script = self.redis_mgr.get_script("nack")
        res = await script(
            keys=[
                self.redis_mgr.lease_key(req.queue),
                task_k,
                self.redis_mgr.delay_key(shard),
                self.redis_mgr.dlq_key(req.queue),
            ],
            args=[
                task_id,
                str(now_ms),
                "1" if req.retryable else "0",
                str(self.settings.retry_base_delay_ms),
                str(self.settings.task_hash_ttl_seconds),
                receipt_json,
                req.worker_id,
                expected_gen,
            ],
        )
        outcome = _decode(res[0])
        retry_count = int(_decode(res[1]))
        ts_val = int(_decode(res[2]))
        if outcome == "IGNORED":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Task is not in RUNNING state or lease_generation is stale",
            )

        if task_kind in ("collection", "processing"):
            async with self.session_factory() as session:
                row = await self._find_task_record(session, task_id)
                if row is not None and row.status not in ("SUCCEEDED", "DEAD"):
                    row.retry_count = retry_count
                    row.receipt = receipt_dict
                    row.worker_id = req.worker_id
                    if outcome == "DEAD":
                        row.status = "DEAD"
                        row.finished_at = now_ms
                    else:
                        row.status = "DELAYED"
                        row.scheduled_at = ts_val
                    await session.commit()
        elif outcome == "DEAD":
            await self._enqueue_terminal_flush(
                task_id, snapshot, "DEAD", receipt_dict, req.worker_id, now_ms, retry_count=retry_count
            )
        TASKS_NACKED.labels(queue=req.queue, outcome=outcome).inc()
        return TaskNackResponse(
            task_id=task_id,
            outcome=outcome,
            retry_count=retry_count,
            next_run_at_ms=ts_val if outcome == "RETRY" else None,
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
        decoded = {_decode(k): _decode(v) for k, v in (snapshot or {}).items()}
        created_ms = int(decoded.get("created_at_ms") or extract_uuid7_timestamp_ms(task_id) or finished_at_ms)
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
            payload=row.payload,
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
            collection_type=row.collection_type,
            lease_generation=row.lease_generation,
            source_task_id=row.source_task_id,
            completion_id=row.completion_id,
            recipient_service=row.recipient_service,
            step=row.step,
            parent_task_id=row.parent_task_id,
            rerun_of_task_id=row.rerun_of_task_id,
            processing_result=row.processing_result,
        )

    async def get_task(self, task_id: str) -> TaskDetailResponse:
        raw = await self.redis.hgetall(self.redis_mgr.task_key(task_id))
        if raw:
            d = {_decode(k): _decode(v) for k, v in raw.items()}
            task_kind = d.get("task_kind", "normal")
            if task_kind in ("collection", "processing"):
                async with self.session_factory() as session:
                    db_row = await self._find_task_record(session, task_id)
                    if db_row is not None and (
                        db_row.status in ("SUCCEEDED", "DEAD")
                        or db_row.lease_generation >= int(d.get("lease_generation", 0))
                    ):
                        return self._row_to_detail(db_row)
            created_ms = int(d.get("created_at_ms") or extract_uuid7_timestamp_ms(task_id) or 0)
            return TaskDetailResponse(
                task_id=task_id,
                queue=d["queue"],
                priority=int(d.get("priority", 5)),
                status=d["status"],
                payload=json.loads(d.get("payload", "{}")),
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
                collection_type=d.get("collection_type") or None,
                lease_generation=int(d.get("lease_generation", 0)),
                source_task_id=d.get("source_task_id") or None,
                completion_id=d.get("completion_id") or None,
                recipient_service=d.get("recipient_service") or None,
                step=d.get("step") or None,
            )

        # Fallback to MySQL with UUIDv7 partition pruning
        async with self.session_factory() as session:
            row = await self._find_task_record(session, task_id)
            if row is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
            return self._row_to_detail(row)

    async def list_tasks(
        self,
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
        eff_start = _naive_utc(start_time) if start_time is not None else eff_end - timedelta(hours=24)
        if eff_start > eff_end:
            raise HTTPException(status_code=400, detail="start_time must precede end_time")
        if (eff_end - eff_start) > timedelta(days=self.settings.max_query_span_days):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Query time span cannot exceed {self.settings.max_query_span_days} days",
            )

        conditions = [TaskRecord.created_at >= eff_start, TaskRecord.created_at <= eff_end]
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
        next_cursor = _encode_cursor(page_rows[-1].created_at, page_rows[-1].task_id) if has_more else None
        return TaskListResponse(items=items, next_cursor=next_cursor)

    async def get_queue_stats(self, queue: str) -> QueueStatsResponse:
        pipe = self.redis.pipeline(transaction=False)
        pipe.zcard(self.redis_mgr.ready_key(queue))
        pipe.zcard(self.redis_mgr.lease_key(queue))
        pipe.llen(self.redis_mgr.dlq_key(queue))
        ready_cnt, running_cnt, dlq_cnt = await pipe.execute()

        worker_pattern = f"{self.redis_mgr.prefix}:worker:{queue}:*"
        worker_count = 0
        async for _ in self.redis.scan_iter(match=worker_pattern, count=100):
            worker_count += 1

        return QueueStatsResponse(
            queue=queue,
            ready_count=int(ready_cnt),
            running_count=int(running_cnt),
            dlq_count=int(dlq_cnt),
            worker_count=worker_count,
        )

    async def retry_task(self, task_id: str, req: TaskRetryRequest) -> TaskSubmitResponse:
        detail = await self.get_task(task_id)
        if detail.status not in ("DEAD", "SUCCEEDED"):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Cannot retry task in status {detail.status}",
            )

        now_ms = _now_ms()
        new_priority = req.priority if req.priority is not None else detail.priority
        new_payload = req.payload if req.payload is not None else detail.payload
        scheduled_at_ms = now_ms + req.delay_seconds * 1000
        created_ms = extract_uuid7_timestamp_ms(task_id)
        if created_ms is None:
            created_ms = int(_naive_utc(detail.created_at).replace(tzinfo=UTC).timestamp() * 1000) if detail.created_at else now_ms
        _, new_status, enqueued = await self._enqueue_task(
            task_id, detail.queue, new_priority, now_ms, scheduled_at_ms,
            {
                "task_id": task_id,
                "queue": detail.queue,
                "priority": str(new_priority),
                "payload": json.dumps(new_payload, separators=(",", ":"), ensure_ascii=False),
                "retry_count": "0",
                "scheduled_at": str(scheduled_at_ms),
                "created_at_ms": str(created_ms),
            },
            retry=True,
        )
        if not enqueued:
            raise HTTPException(status_code=409, detail=f"Cannot retry task in status {new_status}")
        created_dt = _ms_to_naive_utc(created_ms)
        await self.flusher.enqueue(
            {
                "task_id": task_id,
                "queue": detail.queue,
                "priority": new_priority,
                "status": new_status,
                "idempotency_key": None,
                "payload": new_payload,
                "receipt": detail.receipt,
                "retry_count": 0,
                "worker_id": None,
                "scheduled_at": scheduled_at_ms,
                "pulled_at": None,
                "finished_at": None,
                "created_at": created_dt,
            }
        )
        return TaskSubmitResponse(task_id=task_id, queue=detail.queue, status=new_status)

    async def retry_dlq_batch(self, queue: str, req: DlqBatchRetryRequest) -> list[TaskSubmitResponse]:
        dlq_k = self.redis_mgr.dlq_key(queue)
        if req.task_ids:
            target_ids = req.task_ids
        else:
            raw_ids = await self.redis.lrange(dlq_k, 0, req.limit - 1)
            target_ids = [_decode(x) for x in raw_ids]

        results: list[TaskSubmitResponse] = []
        for tid in target_ids:
            try:
                res = await self.retry_task(tid, TaskRetryRequest())
                results.append(res)
            except HTTPException:
                continue
        return results
