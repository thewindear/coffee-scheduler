import base64
import json
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import and_, desc, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.core.metrics import TASKS_ACKED, TASKS_NACKED, TASKS_PULLED, TASKS_SUBMITTED
from app.core.redis_client import RedisLuaManager
from app.core.uuidv7 import day_bounds_utc_naive, extract_uuid7_timestamp_ms, uuid7
from app.models.tables import TaskRecord
from app.schemas.tasks import (
    DlqBatchRetryRequest,
    PulledTask,
    QueueStatsResponse,
    TaskAckRequest,
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


def _priority_score(priority: int, ready_at_ms: int) -> float:
    return float((9 - priority) * 10_000_000_000_000 + ready_at_ms)


def _encode_cursor(created_at: datetime, task_id: str) -> str:
    raw = f"{created_at.isoformat()}|{task_id}"
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")


def _decode_cursor(cursor: str) -> tuple[datetime, str]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
        ts_str, tid = raw.split("|", 1)
        return datetime.fromisoformat(ts_str), tid
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid cursor") from exc


class TaskDispatcher:
    """Core service handling task submission, priority pulling, leases, receipts, and queries."""

    def __init__(
        self,
        redis_mgr: RedisLuaManager,
        flusher: AsyncFlusher,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
    ) -> None:
        self.redis_mgr = redis_mgr
        self.redis = redis_mgr.redis
        self.flusher = flusher
        self.session_factory = session_factory
        self.settings = settings

    async def submit_task(self, req: TaskSubmitRequest) -> TaskSubmitResponse:
        now_ms = _now_ms()
        task_id = uuid7(now_ms)

        if req.idempotency_key:
            idemp_k = self.redis_mgr.idemp_key(req.idempotency_key)
            acquired = await self.redis.set(
                idemp_k, task_id, nx=True, ex=self.settings.idempotency_ttl_seconds
            )
            if not acquired:
                existing_id = await self.redis.get(idemp_k)
                if existing_id is not None:
                    existing_str = _decode(existing_id)
                    existing_status = await self.redis.hget(self.redis_mgr.task_key(existing_str), "status")
                    return TaskSubmitResponse(
                        task_id=existing_str,
                        queue=req.queue,
                        status=_decode(existing_status) if existing_status else "UNKNOWN",
                        deduplicated=True,
                    )

        scheduled_at_ms = req.run_at_ms if req.run_at_ms is not None else now_ms + req.delay_seconds * 1000
        is_delayed = scheduled_at_ms > now_ms
        initial_status = "DELAYED" if is_delayed else "READY"
        payload_json = json.dumps(req.payload, separators=(",", ":"), ensure_ascii=False)
        created_at = _ms_to_naive_utc(now_ms)

        pipe = self.redis.pipeline(transaction=False)
        task_k = self.redis_mgr.task_key(task_id)
        pipe.hset(
            task_k,
            mapping={
                "task_id": task_id,
                "queue": req.queue,
                "priority": str(req.priority),
                "status": initial_status,
                "idempotency_key": req.idempotency_key or "",
                "payload": payload_json,
                "retry_count": "0",
                "max_retries": str(req.max_retries),
                "lease_ttl": str(req.lease_seconds),
                "scheduled_at": str(scheduled_at_ms),
                "created_at_ms": str(now_ms),
            },
        )
        pipe.sadd(self.redis_mgr.active_queues_key(), req.queue)
        if is_delayed:
            shard = self.redis_mgr.delay_shard_for(task_id)
            pipe.zadd(self.redis_mgr.delay_key(shard), {task_id: scheduled_at_ms})
        else:
            score = _priority_score(req.priority, now_ms)
            pipe.zadd(self.redis_mgr.ready_key(req.queue), {task_id: score})
        await pipe.execute()

        self.flusher.enqueue(
            {
                "task_id": task_id,
                "queue": req.queue,
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
                "created_at": created_at,
            }
        )
        TASKS_SUBMITTED.labels(queue=req.queue).inc()
        return TaskSubmitResponse(task_id=task_id, queue=req.queue, status=initial_status)

    async def submit_batch(self, items: list[TaskSubmitRequest]) -> list[TaskSubmitResponse]:
        results = []
        for item in items:
            results.append(await self.submit_task(item))
        return results

    async def pull_tasks(self, req: TaskPullRequest) -> list[PulledTask]:
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
        tasks: list[PulledTask] = []
        for row in raw_results or []:
            tasks.append(
                PulledTask(
                    task_id=_decode(row[0]),
                    queue=_decode(row[1]),
                    priority=int(_decode(row[2])),
                    payload=json.loads(_decode(row[3])),
                    retry_count=int(_decode(row[4])),
                    max_retries=int(_decode(row[5])),
                    lease_expire_at_ms=int(_decode(row[6])),
                    scheduled_at_ms=int(_decode(row[7])),
                )
            )
        if tasks:
            TASKS_PULLED.labels(queue=req.queue).inc(len(tasks))
        return tasks

    async def heartbeat(self, task_id: str, req: TaskHeartbeatRequest) -> TaskHeartbeatResponse:
        now_ms = _now_ms()
        extend_ms = (req.extend_seconds * 1000) if req.extend_seconds else 0
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
            ],
        )
        val = int(res)
        if val == 0:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Task is not actively running or lease has expired",
            )
        if val == -1:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Task lease is owned by a different worker_id",
            )
        return TaskHeartbeatResponse(task_id=task_id, lease_expire_at_ms=val)

    async def ack_task(self, task_id: str, req: TaskAckRequest) -> dict[str, str]:
        now_ms = _now_ms()
        receipt_dict = {
            "worker_id": req.worker_id,
            "duration_ms": req.duration_ms,
            "summary": req.summary,
            "artifacts": [a.model_dump() for a in req.artifacts],
            "extra": req.extra,
        }
        receipt_json = json.dumps(receipt_dict, separators=(",", ":"), ensure_ascii=False)
        if len(receipt_json.encode("utf-8")) > self.settings.max_receipt_bytes:
            raise HTTPException(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                detail=f"Receipt payload exceeds {self.settings.max_receipt_bytes} bytes; store large data externally and pass artifacts URI",
            )

        task_k = self.redis_mgr.task_key(task_id)
        snapshot = await self.redis.hgetall(task_k)
        script = self.redis_mgr.get_script("ack")
        ok = await script(
            keys=[self.redis_mgr.lease_key(req.queue), task_k],
            args=[
                task_id,
                str(now_ms),
                str(self.settings.task_hash_ttl_seconds),
                receipt_json,
                req.worker_id,
            ],
        )
        if int(ok) == 0:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Task is not in RUNNING state",
            )

        self._enqueue_terminal_flush(task_id, snapshot, "SUCCEEDED", receipt_dict, req.worker_id, now_ms)
        TASKS_ACKED.labels(queue=req.queue).inc()
        return {"task_id": task_id, "status": "SUCCEEDED"}

    async def nack_task(self, task_id: str, req: TaskNackRequest) -> TaskNackResponse:
        now_ms = _now_ms()
        receipt_dict = {
            "worker_id": req.worker_id,
            "error_code": req.error_code,
            "error_message": req.error_message,
            "retryable": req.retryable,
        }
        receipt_json = json.dumps(receipt_dict, separators=(",", ":"), ensure_ascii=False)
        if len(receipt_json.encode("utf-8")) > self.settings.max_receipt_bytes:
            raise HTTPException(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                detail=f"Nack payload exceeds {self.settings.max_receipt_bytes} bytes",
            )

        task_k = self.redis_mgr.task_key(task_id)
        snapshot = await self.redis.hgetall(task_k)
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
            ],
        )
        outcome = _decode(res[0])
        retry_count = int(_decode(res[1]))
        ts_val = int(_decode(res[2]))
        if outcome == "IGNORED":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Task is not in RUNNING state",
            )

        if outcome == "DEAD":
            self._enqueue_terminal_flush(
                task_id, snapshot, "DEAD", receipt_dict, req.worker_id, now_ms, retry_count=retry_count
            )
        TASKS_NACKED.labels(queue=req.queue, outcome=outcome).inc()
        return TaskNackResponse(
            task_id=task_id,
            outcome=outcome,
            retry_count=retry_count,
            next_run_at_ms=ts_val if outcome == "RETRY" else None,
        )

    def _enqueue_terminal_flush(
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
        self.flusher.enqueue(
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

    async def get_task(self, task_id: str) -> TaskDetailResponse:
        raw = await self.redis.hgetall(self.redis_mgr.task_key(task_id))
        if raw:
            d = {_decode(k): _decode(v) for k, v in raw.items()}
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
            )

        # Fallback to MySQL with UUIDv7 partition pruning
        ts_ms = extract_uuid7_timestamp_ms(task_id)
        async with self.session_factory() as session:
            stmt = select(TaskRecord).where(TaskRecord.task_id == task_id)
            if ts_ms is not None:
                day_start, day_end = day_bounds_utc_naive(ts_ms)
                stmt = stmt.where(
                    TaskRecord.created_at >= day_start,
                    TaskRecord.created_at < day_end,
                )
            row = (await session.execute(stmt)).scalar_one_or_none()
            if row is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
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
            )

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
        eff_end = end_time or now_utc
        eff_start = start_time or (eff_end - timedelta(hours=24))
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
        items = [
            TaskDetailResponse(
                task_id=r.task_id,
                queue=r.queue,
                priority=r.priority,
                status=r.status,
                payload=r.payload,
                receipt=r.receipt,
                retry_count=r.retry_count,
                worker_id=r.worker_id,
                scheduled_at_ms=r.scheduled_at,
                pulled_at_ms=r.pulled_at,
                finished_at_ms=r.finished_at,
                created_at=r.created_at,
                source="db",
            )
            for r in page_rows
        ]
        next_cursor = _encode_cursor(page_rows[-1].created_at, page_rows[-1].task_id) if has_more else None
        return TaskListResponse(items=items, next_cursor=next_cursor)

    async def get_queue_stats(self, queue: str) -> QueueStatsResponse:
        pipe = self.redis.pipeline(transaction=False)
        pipe.zcard(self.redis_mgr.ready_key(queue))
        pipe.zcard(self.redis_mgr.lease_key(queue))
        pipe.llen(self.redis_mgr.dlq_key(queue))
        ready_cnt, running_cnt, dlq_cnt = await pipe.execute()

        worker_pattern = f"{self.redis_mgr.prefix}:worker:{queue}:*"
        active_workers = 0
        async for _ in self.redis.scan_iter(match=worker_pattern, count=100):
            active_workers += 1

        return QueueStatsResponse(
            queue=queue,
            ready_count=int(ready_cnt),
            running_count=int(running_cnt),
            dlq_count=int(dlq_cnt),
            active_workers=active_workers,
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
        payload_json = json.dumps(new_payload, separators=(",", ":"), ensure_ascii=False)
        scheduled_at_ms = now_ms + req.delay_seconds * 1000
        new_status = "DELAYED" if req.delay_seconds > 0 else "READY"

        task_k = self.redis_mgr.task_key(task_id)
        pipe = self.redis.pipeline(transaction=False)
        pipe.lrem(self.redis_mgr.dlq_key(detail.queue), 0, task_id)
        pipe.hset(
            task_k,
            mapping={
                "task_id": task_id,
                "queue": detail.queue,
                "priority": str(new_priority),
                "status": new_status,
                "payload": payload_json,
                "retry_count": "0",
                "scheduled_at": str(scheduled_at_ms),
                "created_at_ms": str(
                    int(detail.created_at.replace(tzinfo=UTC).timestamp() * 1000)
                    if detail.created_at
                    else now_ms
                ),
            },
        )
        pipe.persist(task_k)
        if req.delay_seconds > 0:
            shard = self.redis_mgr.delay_shard_for(task_id)
            pipe.zadd(self.redis_mgr.delay_key(shard), {task_id: scheduled_at_ms})
        else:
            pipe.zadd(self.redis_mgr.ready_key(detail.queue), {task_id: _priority_score(new_priority, now_ms)})
        await pipe.execute()

        created_dt = detail.created_at or _ms_to_naive_utc(now_ms)
        self.flusher.enqueue(
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
