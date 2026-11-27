import time
from contextlib import asynccontextmanager

import fakeredis.aioredis
import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.access import Grant
from app.core.delivery_config import (
    DeliveryRegistry,
    RecipientRule,
    TaskTypeConfig,
)
from app.core.subscriptions import open_subscription, set_subscription_enabled
from app.main import create_app
from app.models.tables import DeliveryDispatch, TaskCompletion
from tests.conftest import seed_service


def _build_registry() -> DeliveryRegistry:
    reg = DeliveryRegistry()
    reg.register_task_type(
        TaskTypeConfig(
            task_type="product_link",
            business_id="biz-goods",
            queue="collect.goods",
            delivery_enabled=True,
            artifact_retention_seconds=3600,
            e2e_timeout_seconds=60,
        )
    )
    return reg


@pytest.fixture
def credentials():
    return {}


@pytest.fixture
async def delivery_bundle(credentials, test_settings):
    registry = _build_registry()
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=False)
    engine = create_async_engine(test_settings.mysql_dsn)
    app = create_app(
        settings=test_settings,
        redis_client=fake_redis,
        db_engine=engine,
        delivery_registry=registry,
    )
    async with app.router.lifespan_context(app):
        services = [
            (
                "tok-biz-goods",
                "biz-goods-app",
                "biz-goods",
                [
                    Grant("submit", "biz-goods", task_type="product_link"),
                    Grant("task_read", "biz-goods"),
                    Grant("monitor", "biz-goods"),
                    Grant("task_rerun", "biz-goods"),
                ],
                False,
            ),
            (
                "tok-biz-other",
                "biz-other-app",
                "biz-other",
                [Grant("task_read", "biz-other")],
                False,
            ),
            (
                "tok-collector-goods",
                "collector",
                "biz-goods",
                [Grant("pull", "biz-goods", "collect.goods")],
                False,
            ),
            (
                "tok-svc-etl",
                "product-etl",
                None,
                [Grant("task_read", "biz-goods", "collect.goods")],
                False,
            ),
            ("tok-svc-qc", "quality-check", None, [], False),
            ("tok-svc-unrelated", "unrelated-svc", None, [], False),
            ("tok-operator", "ops-admin", None, [], True),
            ("tok-legacy", "legacy-svc", None, [], False),
        ]
        for alias, service, business, grants, admin in services:
            _, credentials[alias] = await seed_service(
                app, service, business, grants, administrator=admin
            )
        async with app.state.session_factory.begin() as session:
            await open_subscription(
                session, registry, "biz-goods", "product_link", "product-etl", "etl", "proc.etl"
            )
            await open_subscription(
                session, registry, "biz-goods", "product_link", "quality-check", "qc", "proc.qc"
            )
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield app, client, fake_redis, engine, registry
    await fake_redis.aclose()
    await engine.dispose()


async def test_4_1_full_chain_routing_auth_and_failure_isolation(delivery_bundle, credentials):
    app, client, _, _, _ = delivery_bundle

    # Unauthenticated submission with task_type is rejected
    r_unauth = await client.post(
        "/api/v1/tasks",
        json={"task_type": "product_link", "payload": {"url": "https://example.com/1"}},
    )
    assert r_unauth.status_code == 401

    # Business submits collection task by task_type
    r_sub = await client.post(
        "/api/v1/tasks",
        headers=credentials["tok-biz-goods"],
        json={
            "task_type": "product_link",
            "business_request_id": "req-1001",
            "payload": {"url": "https://example.com/item/1"},
            "max_retries": 0,
        },
    )
    assert r_sub.status_code == 200
    sub_data = r_sub.json()
    assert sub_data["queue"] == "collect.goods"
    assert sub_data["task_kind"] == "source"
    assert sub_data["business_id"] == "biz-goods"
    coll_id = sub_data["task_id"]

    # Collector pulls and ACKs with versioned artifact reference
    r_pull = await client.post(
        "/api/v1/tasks/pull",
        headers=credentials["tok-collector-goods"],
        json={"queue": "collect.goods", "worker_id": "cw-1", "batch_size": 1},
    )
    assert r_pull.status_code == 200
    pulled = r_pull.json()["tasks"][0]
    assert pulled["task_id"] == coll_id
    assert pulled["lease_generation"] == 1

    r_ack = await client.post(
        f"/api/v1/tasks/{coll_id}/ack",
        headers=credentials["tok-collector-goods"],
        json={
            "queue": "collect.goods",
            "worker_id": "cw-1",
            "lease_generation": 1,
            "summary": {"items": 1},
            "artifacts": [
                {
                    "storage": "oss",
                    "uri": "oss://bucket/goods/item_1.json",
                    "meta": {"format": "json", "version": "v1"},
                }
            ],
        },
    )
    assert r_ack.status_code == 200
    ack_data = r_ack.json()
    assert ack_data["status"] == "SUCCEEDED"
    assert ack_data["dispatch_count"] == 2
    comp_id = ack_data["completion_id"]

    # Background dispatcher creates 2 processing tasks and projects to Redis
    dispatched = await app.state.delivery_dispatcher.dispatch_once()
    assert dispatched == 2

    # Downstream 1 (product-etl) pulls, verifies lineage fields + artifact, and ACKs
    r_etl_pull = await client.post(
        "/api/v1/tasks/pull",
        headers=credentials["tok-svc-etl"],
        json={"queue": "proc.etl", "worker_id": "etl-w1", "batch_size": 5},
    )
    etl_tasks = r_etl_pull.json()["tasks"]
    assert len(etl_tasks) == 1
    etl_task = etl_tasks[0]
    assert etl_task["task_kind"] == "processing"
    assert etl_task["source_task_id"] == coll_id
    assert etl_task["completion_id"] == comp_id
    assert etl_task["step"] == "etl"
    assert etl_task["recipient_service"] == "product-etl"
    assert etl_task["artifacts"][0]["uri"] == "oss://bucket/goods/item_1.json"

    r_etl_ack = await client.post(
        f"/api/v1/tasks/{etl_task['task_id']}/ack",
        headers=credentials["tok-svc-etl"],
        json={
            "queue": "proc.etl",
            "worker_id": "etl-w1",
            "lease_generation": etl_task["lease_generation"],
            "processing_result": {"rows_written": 1},
        },
    )
    assert r_etl_ack.status_code == 200
    assert r_etl_ack.json()["dispatch_count"] == 0

    # Downstream 2 (quality-check) fails with non-retryable NACK -> DEAD (failure isolation)
    r_qc_pull = await client.post(
        "/api/v1/tasks/pull",
        headers=credentials["tok-svc-qc"],
        json={"queue": "proc.qc", "worker_id": "qc-w1", "batch_size": 5},
    )
    qc_task = r_qc_pull.json()["tasks"][0]
    r_qc_nack = await client.post(
        f"/api/v1/tasks/{qc_task['task_id']}/nack",
        headers=credentials["tok-svc-qc"],
        json={
            "queue": "proc.qc",
            "worker_id": "qc-w1",
            "lease_generation": qc_task["lease_generation"],
            "error_code": "QC_SCHEMA_ERR",
            "error_message": "Missing price field",
            "retryable": False,
        },
    )
    assert r_qc_nack.status_code == 200
    assert r_qc_nack.json()["outcome"] == "DEAD"

    # Cross-business rejection (403)
    assert (
        await client.get(
            f"/api/v1/tasks/{coll_id}/deliveries", headers=credentials["tok-biz-other"]
        )
    ).status_code == 403
    assert (
        await client.get(
            f"/api/v1/tasks/{coll_id}/artifacts", headers=credentials["tok-svc-unrelated"]
        )
    ).status_code == 403
    assert (
        await client.get(f"/api/v1/tasks/{coll_id}/artifacts", headers=credentials["tok-svc-etl"])
    ).status_code == 200

    # Lineage shows ETL=SUCCEEDED and QC=DEAD
    r_trace = await client.get(
        f"/api/v1/tasks/{coll_id}/deliveries", headers=credentials["tok-biz-goods"]
    )
    assert r_trace.status_code == 200
    trace = r_trace.json()
    by_svc = {d["recipient_service"]: d for d in trace["deliveries"]}
    assert by_svc["product-etl"]["processing_status"] == "SUCCEEDED"
    assert by_svc["quality-check"]["processing_status"] == "DEAD"


async def test_4_2_fault_recovery_idempotency_and_stale_lease(delivery_bundle, credentials):
    app, client, fake_redis, _, _ = delivery_bundle

    # 1. Submit collection task
    r_sub = await client.post(
        "/api/v1/tasks",
        headers=credentials["tok-biz-goods"],
        json={
            "task_type": "product_link",
            "payload": {"url": "https://example.com/2"},
            "lease_seconds": 1,
        },
    )
    coll_id = r_sub.json()["task_id"]

    # 2. Worker 1 pulls (gen=1), then lease expires & reaped, Worker 2 pulls (gen=2)
    p1 = (
        await client.post(
            "/api/v1/tasks/pull",
            headers=credentials["tok-collector-goods"],
            json={"queue": "collect.goods", "worker_id": "cw-1", "batch_size": 1},
        )
    ).json()["tasks"][0]
    assert p1["lease_generation"] == 1

    # Force lease expiry in Redis and reap
    await fake_redis.zadd(
        app.state.redis_mgr.lease_key("collect.goods"), {coll_id: time.time() * 1000 - 1000}
    )
    reaped = await app.state.scheduler.reap_once()
    assert reaped == 1
    # Pump retry back to ready
    await fake_redis.zadd(
        app.state.redis_mgr.delay_key(app.state.redis_mgr.delay_shard_for(coll_id)),
        {coll_id: time.time() * 1000 - 1000},
    )
    await app.state.scheduler.pump_once()

    p2 = (
        await client.post(
            "/api/v1/tasks/pull",
            headers=credentials["tok-collector-goods"],
            json={"queue": "collect.goods", "worker_id": "cw-2", "batch_size": 1},
        )
    ).json()["tasks"][0]
    assert p2["lease_generation"] == 2

    # Stale Worker 1 (gen=1) ACK is rejected with 409
    r_stale = await client.post(
        f"/api/v1/tasks/{coll_id}/ack",
        headers=credentials["tok-collector-goods"],
        json={"queue": "collect.goods", "worker_id": "cw-1", "lease_generation": 1},
    )
    assert r_stale.status_code == 409

    # Worker 2 (gen=2) ACKs -> succeeds
    ack_body = {
        "queue": "collect.goods",
        "worker_id": "cw-2",
        "lease_generation": 2,
        "summary": {"ok": True},
        "artifacts": [{"storage": "oss", "uri": "oss://bucket/2.json", "meta": {"version": "v1"}}],
    }
    r_ok = await client.post(
        f"/api/v1/tasks/{coll_id}/ack",
        headers=credentials["tok-collector-goods"],
        json=ack_body,
    )
    assert r_ok.status_code == 200
    assert r_ok.json()["replayed"] is False
    comp_id = r_ok.json()["completion_id"]

    # Lost ACK response: Worker 2 replays identical ACK -> 200 replayed=True
    r_replay = await client.post(
        f"/api/v1/tasks/{coll_id}/ack",
        headers=credentials["tok-collector-goods"],
        json=ack_body,
    )
    assert r_replay.status_code == 200
    assert r_replay.json()["replayed"] is True
    assert r_replay.json()["completion_id"] == comp_id

    # Conflicting receipt on already completed task -> 409
    r_conflict = await client.post(
        f"/api/v1/tasks/{coll_id}/ack",
        headers=credentials["tok-collector-goods"],
        json={**ack_body, "summary": {"ok": False}},
    )
    assert r_conflict.status_code == 409

    # 3. Dispatcher crash recovery & lost Redis ready item reconciliation
    await app.state.delivery_dispatcher.dispatch_once()
    async with app.state.session_factory() as session:
        disps = (
            (
                await session.execute(
                    select(DeliveryDispatch).where(DeliveryDispatch.source_task_id == coll_id)
                )
            )
            .scalars()
            .all()
        )
        etl_disp = next(d for d in disps if d.recipient_service == "product-etl")
        etl_tid = etl_disp.processing_task_id
        assert etl_tid is not None

    # Simulate Redis losing the ETL ready item
    await fake_redis.delete(app.state.redis_mgr.task_key(etl_tid))
    await fake_redis.zrem(app.state.redis_mgr.ready_key("proc.etl"), etl_tid)

    repaired = await app.state.delivery_dispatcher.reconcile_once()
    assert repaired >= 1

    # ETL pulls, writes DB, crashes before ACK -> lease reaped -> ETL2 pulls & ACKs
    etl_p1 = (
        await client.post(
            "/api/v1/tasks/pull",
            headers=credentials["tok-svc-etl"],
            json={"queue": "proc.etl", "worker_id": "etl-1", "batch_size": 1},
        )
    ).json()["tasks"][0]
    assert etl_p1["task_id"] == etl_tid
    assert etl_p1["lease_generation"] == 1

    # While ETL1 holds valid lease, reconcile_once must NOT re-enqueue it
    assert await app.state.delivery_dispatcher.reconcile_once() == 0

    await fake_redis.zadd(
        app.state.redis_mgr.lease_key("proc.etl"), {etl_tid: time.time() * 1000 - 1000}
    )
    await app.state.scheduler.reap_once()
    await fake_redis.zadd(
        app.state.redis_mgr.delay_key(app.state.redis_mgr.delay_shard_for(etl_tid)),
        {etl_tid: time.time() * 1000 - 1000},
    )
    await app.state.scheduler.pump_once()

    etl_p2 = (
        await client.post(
            "/api/v1/tasks/pull",
            headers=credentials["tok-svc-etl"],
            json={"queue": "proc.etl", "worker_id": "etl-2", "batch_size": 1},
        )
    ).json()["tasks"][0]
    assert etl_p2["task_id"] == etl_tid
    assert etl_p2["lease_generation"] == 2

    r_etl_ack = await client.post(
        f"/api/v1/tasks/{etl_tid}/ack",
        headers=credentials["tok-svc-etl"],
        json={
            "queue": "proc.etl",
            "worker_id": "etl-2",
            "lease_generation": 2,
            "processing_result": {"idempotent_upsert": True},
        },
    )
    assert r_etl_ack.status_code == 200
    # After terminal SUCCEEDED, reconcile_once must NEVER revive it
    await fake_redis.delete(app.state.redis_mgr.task_key(etl_tid))
    assert await app.state.delivery_dispatcher.reconcile_once() == 0


async def test_4_2_ack_db_rollback_does_not_confirm(delivery_bundle, credentials, monkeypatch):
    app, client, _, _, _ = delivery_bundle
    r_sub = await client.post(
        "/api/v1/tasks",
        headers=credentials["tok-biz-goods"],
        json={"task_type": "product_link", "payload": {"url": "https://example.com/rollback"}},
    )
    coll_id = r_sub.json()["task_id"]
    p1 = (
        await client.post(
            "/api/v1/tasks/pull",
            headers=credentials["tok-collector-goods"],
            json={"queue": "collect.goods", "worker_id": "cw-1", "batch_size": 1},
        )
    ).json()["tasks"][0]

    # Simulate DB failure during ACK commit
    orig_session_factory = app.state.dispatcher.session_factory

    original_begin = orig_session_factory.begin

    @asynccontextmanager
    async def fail_on_commit():
        async with original_begin() as session:
            yield session
            raise RuntimeError("Simulated MySQL failure")

    monkeypatch.setattr(orig_session_factory, "begin", fail_on_commit)
    with pytest.raises(RuntimeError, match="Simulated MySQL failure"):
        await client.post(
            f"/api/v1/tasks/{coll_id}/ack",
            headers=credentials["tok-collector-goods"],
            json={
                "queue": "collect.goods",
                "worker_id": "cw-1",
                "lease_generation": p1["lease_generation"],
            },
        )

    async with orig_session_factory() as session:
        comp = (
            await session.execute(
                select(TaskCompletion).where(TaskCompletion.source_task_id == coll_id)
            )
        ).scalar_one_or_none()
        assert comp is None


async def test_4_3_rerun_idempotency_expiry_rerun_source_and_backlog(delivery_bundle, credentials):
    app, client, _, _, _ = delivery_bundle

    # Submit 2 tasks: task1 will complete and be dispatched; task2 stays in source_ready
    t1 = (
        await client.post(
            "/api/v1/tasks",
            headers=credentials["tok-biz-goods"],
            json={"task_type": "product_link", "payload": {"url": "https://example.com/3a"}},
        )
    ).json()["task_id"]
    t2 = (
        await client.post(
            "/api/v1/tasks",
            headers=credentials["tok-biz-goods"],
            json={"task_type": "product_link", "payload": {"url": "https://example.com/3b"}},
        )
    ).json()["task_id"]

    # Pull & ACK t1 only
    p1 = (
        await client.post(
            "/api/v1/tasks/pull",
            headers=credentials["tok-collector-goods"],
            json={"queue": "collect.goods", "worker_id": "cw-1", "batch_size": 1},
        )
    ).json()["tasks"][0]
    assert p1["task_id"] == t1
    await client.post(
        f"/api/v1/tasks/{t1}/ack",
        headers=credentials["tok-collector-goods"],
        json={
            "queue": "collect.goods",
            "worker_id": "cw-1",
            "lease_generation": p1["lease_generation"],
            "artifacts": [
                {"storage": "oss", "uri": "oss://bucket/3a.json", "meta": {"version": "v1"}}
            ],
        },
    )

    # Backlog before dispatch_once: source_ready=1 (t2), dispatch_pending=2, processing_ready=0
    b1 = (
        await client.get("/api/v1/deliveries/backlog", headers=credentials["tok-biz-goods"])
    ).json()
    assert b1["source_ready"]["count"] == 1
    assert b1["dispatch_pending"]["count"] == 2
    assert b1["processing_ready"]["count"] == 0

    await app.state.delivery_dispatcher.dispatch_once()

    # Backlog after dispatch_once: dispatch_pending=0, processing_ready=2
    b2 = (
        await client.get("/api/v1/deliveries/backlog", headers=credentials["tok-biz-goods"])
    ).json()
    assert b2["source_ready"]["count"] == 1
    assert b2["dispatch_pending"]["count"] == 0
    assert b2["processing_ready"]["count"] == 2

    # Single-step manual rerun for quality-check/qc + idempotency
    r_rerun1 = await client.post(
        f"/api/v1/tasks/{t1}/deliveries/quality-check/qc/rerun",
        headers=credentials["tok-biz-goods"],
        json={"idempotency_key": "ops-rerun-1"},
    )
    assert r_rerun1.status_code == 200
    rr1 = r_rerun1.json()
    assert rr1["deduplicated"] is False

    r_rerun2 = await client.post(
        f"/api/v1/tasks/{t1}/deliveries/quality-check/qc/rerun",
        headers=credentials["tok-biz-goods"],
        json={"idempotency_key": "ops-rerun-1"},
    )
    assert r_rerun2.status_code == 200
    assert r_rerun2.json()["deduplicated"] is True
    assert r_rerun2.json()["processing_task_id"] == rr1["processing_task_id"]

    # Stats reflect manual_rerun_count=1
    stats = (
        await client.get("/api/v1/deliveries/stats", headers=credentials["tok-biz-goods"])
    ).json()
    assert stats["source_succeeded_count"] == 1
    assert stats["manual_rerun_count"] == 1

    # Expire artifact -> rerun returns 410 ARTIFACT_EXPIRED
    async with app.state.session_factory() as session:
        comp = (
            await session.execute(select(TaskCompletion).where(TaskCompletion.source_task_id == t1))
        ).scalar_one()
        comp.artifact_expires_at_ms = int(time.time() * 1000) - 1000
        await session.commit()

    r_exp = await client.post(
        f"/api/v1/tasks/{t1}/deliveries/quality-check/qc/rerun",
        headers=credentials["tok-biz-goods"],
        json={"idempotency_key": "ops-rerun-2"},
    )
    assert r_exp.status_code == 410
    assert r_exp.json()["detail"]["code"] == "ARTIFACT_EXPIRED"

    # Manual re-collect creates new collection task linked via parent_task_id
    r_recoll = await client.post(
        f"/api/v1/tasks/{t1}/rerun",
        headers=credentials["tok-biz-goods"],
        json={},
    )
    assert r_recoll.status_code == 200
    t1_child = r_recoll.json()["task_id"]
    assert t1_child != t1

    trace = (
        await client.get(f"/api/v1/tasks/{t1}/deliveries", headers=credentials["tok-biz-goods"])
    ).json()
    assert t1_child in trace["child_source_task_ids"]
    assert t2 != t1_child


async def test_4_4_kafka_migration_cutover_and_rollback(delivery_bundle, credentials):
    app, client, _, _, registry = delivery_bundle

    async with app.state.session_factory.begin() as session:
        for service, step in [("product-etl", "etl"), ("quality-check", "qc")]:
            await set_subscription_enabled(
                session, "biz-goods", "product_link", service, step, enabled=False
            )

    # Validate handoff guard: enabling kafka without kafka_handoff_confirmed raises ValueError
    with pytest.raises(ValueError, match="confirmed handoff"):
        registry.register_task_type(
            TaskTypeConfig(
                task_type="bad_kafka",
                business_id="biz-goods",
                queue="collect.goods",
                default_services=[
                    RecipientRule(
                        recipient_service="legacy-svc",
                        step="ingest",
                        queue="proc.legacy",
                        delivery_mode="kafka",
                        kafka_topic="topic.legacy",
                        kafka_handoff_confirmed=False,
                    )
                ],
            )
        )

    # Configure product_link with legacy-svc on Kafka (handoff confirmed)
    registry.register_task_type(
        TaskTypeConfig(
            task_type="product_link",
            business_id="biz-goods",
            queue="collect.goods",
            delivery_enabled=True,
            default_services=[
                RecipientRule(
                    recipient_service="legacy-svc",
                    step="ingest",
                    queue="proc.legacy",
                    delivery_mode="kafka",
                    kafka_topic="topic.goods.legacy",
                    kafka_handoff_confirmed=True,
                )
            ],
        )
    )

    async def _submit_and_collect(idx: int) -> str:
        tid = (
            await client.post(
                "/api/v1/tasks",
                headers=credentials["tok-biz-goods"],
                json={"task_type": "product_link", "payload": {"idx": idx}},
            )
        ).json()["task_id"]
        pulled = (
            await client.post(
                "/api/v1/tasks/pull",
                headers=credentials["tok-collector-goods"],
                json={"queue": "collect.goods", "worker_id": "cw-k", "batch_size": 1},
            )
        ).json()["tasks"][0]
        await client.post(
            f"/api/v1/tasks/{tid}/ack",
            headers=credentials["tok-collector-goods"],
            json={
                "queue": "collect.goods",
                "worker_id": "cw-k",
                "lease_generation": pulled["lease_generation"],
                "artifacts": [{"storage": "oss", "uri": f"oss://bucket/k_{idx}.json"}],
            },
        )
        return tid

    # Task 1 completed under Kafka rule; simulate 1 broker failure then retry with fixed delivery_id
    t1 = await _submit_and_collect(1)
    app.state.kafka_publisher.producer.fail_next = True
    assert await app.state.kafka_publisher.publish_pending_once() == 0
    assert await app.state.kafka_publisher.publish_pending_once() == 1
    assert len(app.state.kafka_publisher.producer.messages) == 1
    msg1 = app.state.kafka_publisher.producer.messages[0]
    assert msg1["topic"] == "topic.goods.legacy"
    assert msg1["value"]["source_task_id"] == t1

    # Cut over new completions to HTTP while preserving the Kafka backlog.
    t_old_backlog = await _submit_and_collect(2)
    async with app.state.session_factory.begin() as session:
        await open_subscription(
            session, registry, "biz-goods", "product_link", "legacy-svc", "ingest", "proc.legacy"
        )
    t_http = await _submit_and_collect(3)

    # Roll back the cutover using a distinct Kafka binding. The disabled HTTP binding still shadows its old rule.
    async with app.state.session_factory.begin() as session:
        await set_subscription_enabled(
            session, "biz-goods", "product_link", "legacy-svc", "ingest", enabled=False
        )
    cfg = registry.get_task_type("biz-goods", "product_link")
    rule = cfg.default_services[0].model_copy(update={"step": "ingest-v2"})
    registry.register_task_type(cfg.model_copy(update={"default_services": [rule]}))
    t_rollback = await _submit_and_collect(4)

    # Run both dispatchers: t_old_backlog & t_rollback go to Kafka; t_http goes to HTTP!
    assert await app.state.kafka_publisher.publish_pending_once() == 2
    assert await app.state.delivery_dispatcher.dispatch_once() == 1

    # Verify each completion has exactly 1 delivery path
    for tid, expected_mode in [
        (t1, "kafka"),
        (t_old_backlog, "kafka"),
        (t_http, "http"),
        (t_rollback, "kafka"),
    ]:
        trace = (
            await client.get(
                f"/api/v1/tasks/{tid}/deliveries", headers=credentials["tok-biz-goods"]
            )
        ).json()
        assert len(trace["deliveries"]) == 1
        assert trace["deliveries"][0]["delivery_mode"] == expected_mode
        if expected_mode == "kafka":
            assert trace["deliveries"][0]["status"] == "KAFKA_PUBLISHED"
            assert trace["deliveries"][0]["processing_status"] == "UNKNOWN"

    stats = (
        await client.get("/api/v1/deliveries/stats", headers=credentials["tok-biz-goods"])
    ).json()
    assert stats["kafka_published_count"] == 3
    assert stats["kafka_unknown_processing_count"] == 3
