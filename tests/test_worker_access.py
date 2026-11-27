import asyncio
import json
import os
import sqlite3
import time
from uuid import uuid4

import fakeredis.aioredis
import httpx
import pytest
from coffee_scheduler_worker import SchedulerClient, Task, Worker
from redis.asyncio import Redis
from sqlalchemy import select, text, update
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from app.admin import execute, parser
from app.core.config import Settings
from app.main import create_app
from app.models.tables import (
    CronJob,
    DeliveryDispatch,
    ResultSubscription,
    ServiceCredential,
    TaskCompletion,
)
from examples.document_worker import inspect_document, write_result

DOC_TYPE = "document.inspect"
SOURCE = "documents.inspect"
ARCHIVE = "documents.archive"
PREVIEW = "documents.preview"
BUSINESS = "editorial"


@pytest.fixture
async def access_app(tmp_path):
    mysql_dsn = os.getenv("COFFEE_TEST_MYSQL_DSN")
    redis_url = os.getenv("COFFEE_TEST_REDIS_URL")
    schema = f"coffee_access_{uuid4().hex}"
    if mysql_dsn is not None:
        control = create_async_engine(mysql_dsn)
        async with control.begin() as conn:
            await conn.execute(text(f"CREATE DATABASE {schema}"))
        dsn = make_url(mysql_dsn).set(database=schema).render_as_string(hide_password=False)
    else:
        control = None
        dsn = f"sqlite+aiosqlite:///{tmp_path / 'access.sqlite3'}"
    settings = Settings(
        app_env="test",
        require_https=False,
        mysql_dsn=dsn,
        embedded_scheduler=False,
        worker_heartbeat_ttl_seconds=1,
        retry_base_delay_ms=5,
        default_max_retries=1,
        trusted_proxy_ips=["10.10.0.0/24"],
        redis_key_prefix=schema,
        delivery_config_json=json.dumps(
            {
                "task_types": [
                    {
                        "task_type": DOC_TYPE,
                        "business_id": BUSINESS,
                        "queue": SOURCE,
                        "shared_grants": [
                            {"recipient_service": "archive", "step": "archive", "queue": ARCHIVE},
                        ],
                    },
                    {"task_type": DOC_TYPE, "business_id": "other", "queue": SOURCE},
                ]
            }
        ),
    )
    redis = Redis.from_url(redis_url) if redis_url else fakeredis.aioredis.FakeRedis()
    engine = create_async_engine(dsn)
    app = create_app(settings=settings, db_engine=engine, redis_client=redis)
    try:
        async with app.router.lifespan_context(app):
            yield app
    finally:
        if redis_url:
            keys = [key async for key in redis.scan_iter(match=f"{schema}:*")]
            if keys:
                await redis.delete(*keys)
        await redis.aclose()
        await engine.dispose()
        if control is not None:
            async with control.begin() as conn:
                await conn.execute(text(f"DROP DATABASE {schema}"))
            await control.dispose()


async def admin(app, *args):
    async with app.state.session_factory.begin() as session:
        return await execute(session, parser().parse_args(args), app.state.delivery_registry)


async def identity(app, service, business=None, *, administrator=False):
    options = ["--business", business] if business else []
    if administrator:
        options.append("--admin")
    await admin(app, "create-service", service, *options)
    return await admin(app, "issue", service)


def bearer(credential):
    return {"Authorization": f"Bearer {credential['token']}"}


def client(app, credential=None, ip="192.0.2.10"):
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=(ip, 12345)),
        base_url="http://test",
        headers=bearer(credential) if credential else None,
    )


def sdk(app, credential):
    return SchedulerClient(
        "http://test", credential["token"], transport=httpx.ASGITransport(app=app)
    )


async def response(client, method, path, *, status=200, **fields):
    result = await client.request(method, path, **fields)
    assert result.status_code == status, result.text
    return result.json() if result.content else None


async def grant(app, name, operation, *, business=None, queue=None, task_type=None):
    fields = [
        item
        for key, value in [("--business", business), ("--queue", queue), ("--task-type", task_type)]
        if value
        for item in [key, value]
    ]
    await admin(app, "grant", name, operation, *fields)


async def document_services(app):
    credentials = {}
    for name in ("author", "source", "archive", "preview"):
        credentials[name] = await identity(app, name, BUSINESS)
    await grant(app, "author", "submit", business=BUSINESS, task_type=DOC_TYPE)
    await grant(app, "author", "task_read", business=BUSINESS)
    await grant(app, "source", "pull", business=BUSINESS, queue=SOURCE)
    return credentials


async def open_doc_subscription(app, name):
    return await admin(
        app, "open-subscription", BUSINESS, DOC_TYPE, name, name, f"documents.{name}"
    )


async def test_authenticated_entrypoints_and_lease_owner(access_app):
    app = access_app
    writer = await identity(app, "writer", BUSINESS)
    worker = await identity(app, "worker", BUSINESS)
    impersonator = await identity(app, "another", BUSINESS)
    other = await identity(app, "other-writer", "other")
    for name in ("worker", "another"):
        await grant(app, name, "pull", business=BUSINESS, queue="plain")
    for name, business in [("writer", BUSINESS), ("other-writer", "other")]:
        await grant(app, name, "submit", business=business, queue="plain")
    await grant(app, "writer", "task_read", business=BUSINESS)

    async with (
        client(app) as anonymous,
        client(app, writer) as submitter,
        client(app, worker) as execution,
    ):
        assert await response(anonymous, "GET", "/health") == {"status": "ok"}
        for method, path in [
            ("POST", "/api/v1/tasks"),
            ("POST", "/api/v1/tasks/batch"),
            ("POST", "/api/v1/tasks/pull"),
            ("POST", "/api/v1/tasks/example/heartbeat"),
            ("POST", "/api/v1/tasks/example/ack"),
            ("POST", "/api/v1/tasks/example/nack"),
            ("POST", "/api/v1/tasks/example/retry"),
            ("GET", "/api/v1/tasks"),
            ("GET", "/api/v1/tasks/example"),
            ("GET", "/api/v1/tasks/example/deliveries"),
            ("GET", "/api/v1/tasks/example/artifacts"),
            ("POST", "/api/v1/tasks/example/rerun"),
            ("GET", "/api/v1/queues/plain/stats"),
            ("GET", "/api/v1/dlq/plain"),
            ("POST", "/api/v1/dlq/plain/retry"),
            ("GET", "/api/v1/deliveries/backlog"),
            ("GET", "/api/v1/deliveries/stats"),
            ("GET", "/api/v1/cron-jobs"),
            ("GET", "/api/v1/admin/workers"),
            ("GET", "/metrics"),
        ]:
            await response(anonymous, method, path, status=401)
        await response(
            anonymous, "GET", "/api/v1/tasks", status=401, headers=bearer({"token": "wrong"})
        )
        await response(execution, "POST", "/api/v1/tasks", status=403, json={"queue": "plain"})
        await response(
            submitter,
            "POST",
            "/api/v1/tasks/pull",
            status=403,
            json={"queue": "plain", "worker_id": "same"},
        )
        await response(
            submitter,
            "POST",
            "/api/v1/tasks/batch",
            status=403,
            json={
                "tasks": [{"queue": "plain"}, {"queue": "forbidden"}],
            },
        )
        await response(
            submitter,
            "POST",
            "/api/v1/tasks",
            status=403,
            json={"queue": "plain", "business_id": "other"},
        )
        assert await app.state.redis.zcard(app.state.redis_mgr.ready_key("plain")) == 0
        async with client(app, other) as foreign:
            foreign_task = await response(
                foreign,
                "POST",
                "/api/v1/tasks",
                json={
                    "queue": "plain",
                    "idempotency_key": "same",
                    "priority": 9,
                },
            )
        local = await response(
            submitter, "POST", "/api/v1/tasks", json={"queue": "plain", "idempotency_key": "same"}
        )
        assert foreign_task["task_id"] != local["task_id"]
        await response(submitter, "GET", f"/api/v1/tasks/{foreign_task['task_id']}", status=403)
        claim = {"queue": "plain", "worker_id": "same"}
        (pulled,) = (await response(execution, "POST", "/api/v1/tasks/pull", json=claim))["tasks"]
        assert pulled["task_id"] == local["task_id"]
        body = {**claim, "lease_generation": pulled["lease_generation"]}
        base = f"/api/v1/tasks/{pulled['task_id']}"
        async with client(app, impersonator) as attacker:
            for action in ("heartbeat", "ack", "nack"):
                await response(attacker, "POST", f"{base}/{action}", status=403, json=body)
        await response(execution, "POST", f"{base}/ack", status=422, json=claim)
        await response(
            execution, "POST", f"{base}/heartbeat", status=409, json={**body, "lease_generation": 2}
        )
        await response(execution, "POST", f"{base}/heartbeat", json=body)
        assert (await response(execution, "POST", f"{base}/ack", json=body))["replayed"] is False
        assert (await response(execution, "POST", f"{base}/ack", json=body))["replayed"] is True
        await app.state.flusher.drain_once()
        await app.state.redis.delete(app.state.redis_mgr.task_key(pulled["task_id"]))
        assert (await response(execution, "POST", f"{base}/ack", json=body))["replayed"] is True
        await response(
            execution,
            "POST",
            f"{base}/ack",
            status=409,
            json={**body, "summary": {"changed": True}},
        )
        assert await app.state.redis.zcard(app.state.redis_mgr.ready_key("plain")) == 1


async def test_scoped_queries_DLQ_and_metrics(access_app):
    app = access_app
    operator = await identity(app, "admin", administrator=True)
    reader = await identity(app, "reader", BUSINESS)
    retrier = await identity(app, "retrier", BUSINESS)
    scrape = await identity(app, "scraper")
    for operation in ("task_read", "dlq_read", "monitor"):
        await grant(app, "reader", operation, business=BUSINESS, queue="shared")
    await grant(app, "retrier", "dlq_retry", business=BUSINESS, queue="shared")
    await grant(app, "scraper", "metrics")

    async with client(app, operator) as admin_client:
        ids = {}
        # The case-variant business must be excluded on MySQL as well as SQLite.
        for business, queue, state in [
            (BUSINESS, "shared", "local"),
            (BUSINESS.upper(), "shared", "case"),
            ("other", "shared", "foreign"),
            (BUSINESS, "second", "wrong_queue"),
        ]:
            task = await response(
                admin_client,
                "POST",
                "/api/v1/tasks",
                json={
                    "queue": queue,
                    "business_id": business,
                    "max_retries": 0,
                },
            )
            ids[state] = task["task_id"]
            pull = {"queue": queue, "worker_id": "adm", "batch_size": 1}
            (item,) = (await response(admin_client, "POST", "/api/v1/tasks/pull", json=pull))[
                "tasks"
            ]
            await response(
                admin_client,
                "POST",
                f"/api/v1/tasks/{item['task_id']}/nack",
                json={
                    **pull,
                    "lease_generation": item["lease_generation"],
                },
            )
        await app.state.flusher.drain_once()

    async with (
        client(app, reader) as read,
        client(app, retrier) as retry,
        client(app, scrape) as metrics,
    ):
        page = await response(read, "GET", "/api/v1/tasks?limit=1")
        assert [item["task_id"] for item in page["items"]] == [ids["local"]]
        assert page["next_cursor"] is None
        assert (await response(read, "GET", "/api/v1/queues/shared/stats"))["dlq_count"] == 1
        assert [i["task_id"] for i in await response(read, "GET", "/api/v1/dlq/shared")] == [
            ids["local"]
        ]
        await response(read, "GET", "/api/v1/queues/second/stats", status=403)
        await response(read, "POST", f"/api/v1/tasks/{ids['local']}/retry", status=403)
        await response(read, "POST", "/api/v1/dlq/shared/retry", status=403)
        for forbidden_id in (ids["case"], ids["foreign"], ids["wrong_queue"]):
            await response(
                retry,
                "POST",
                "/api/v1/dlq/shared/retry",
                status=403,
                json={
                    "task_ids": [ids["local"], forbidden_id],
                },
            )
        assert (await response(read, "GET", f"/api/v1/tasks/{ids['local']}"))["status"] == "DEAD"
        assert [
            i["task_id"] for i in await response(retry, "POST", "/api/v1/dlq/shared/retry")
        ] == [ids["local"]]
        await response(read, "GET", "/metrics", status=403)
        assert (await metrics.get("/metrics")).status_code == 200
        await response(metrics, "GET", "/api/v1/admin/workers", status=403)


async def test_cron_rechecks_owner_submission_grant(access_app):
    app = access_app
    owner = await identity(app, "cron-owner", BUSINESS)
    foreign = await identity(app, "foreign-ops", "other")
    for operation in ("cron_manage", "cron_read"):
        await grant(app, "cron-owner", operation, business=BUSINESS)
        await grant(app, "foreign-ops", operation, business="other")
    rule = {"name": "daily", "queue": "daily", "cron_expr": "* * * * *"}
    async with client(app, owner) as local, client(app, foreign) as stranger:
        await response(local, "POST", "/api/v1/cron-jobs", status=403, json=rule)
        assert (await response(local, "GET", "/api/v1/cron-jobs")) == []
        await grant(app, "cron-owner", "submit", business=BUSINESS, queue="daily")
        created = await response(local, "POST", "/api/v1/cron-jobs", status=201, json=rule)
        path = f"/api/v1/cron-jobs/{created['id']}"
        assert created["owner_service"] == "cron-owner"
        assert created["business_id"] == BUSINESS
        assert await response(stranger, "GET", "/api/v1/cron-jobs") == []
        for method, fields in [
            ("GET", {}),
            ("PATCH", {"json": {"enabled": False}}),
            ("DELETE", {}),
        ]:
            await response(stranger, method, path, status=403, **fields)
        await response(local, "PATCH", path, status=403, json={"queue": "unauthorized"})
        await admin(
            app, "revoke-grant", "cron-owner", "submit", "--business", BUSINESS, "--queue", "daily"
        )
        assert (await response(local, "GET", path))["enabled"] is True
        async with app.state.session_factory.begin() as session:
            await session.execute(
                update(CronJob).where(CronJob.id == created["id"]).values(next_run_at=1)
            )
        assert await app.state.scheduler.scan_cron_once() == 0
        assert await app.state.redis.zcard(app.state.redis_mgr.ready_key("daily")) == 0
        await response(local, "PATCH", path, json={"enabled": False})
        await response(local, "PATCH", path, status=403, json={"enabled": True})
        await grant(app, "cron-owner", "submit", business=BUSINESS, queue="daily")
        assert (await response(local, "PATCH", path, json={"enabled": True}))["enabled"] is True
        await response(local, "DELETE", path, status=204)


async def test_subscription_zero_boundary_rotation_and_cross_instance(access_app):
    app = access_app
    credentials = await document_services(app)
    await identity(app, "different", BUSINESS)
    async with sdk(app, credentials["author"]) as author, sdk(app, credentials["source"]) as source:

        async def finish(*, requested=None):
            submitted = await author.submit(
                task_type=DOC_TYPE,
                payload={"title": "Report", "pages": [1]},
                recipient_services=requested,
            )
            (task,) = await source.pull(SOURCE, "source-one")
            assert task.task_id == submitted["task_id"]
            assert task.task_type == DOC_TYPE and task.task_kind == "source"
            return task, await source.ack(task, "source-one", summary={"page_count": 1})

        first, zero = await finish()
        assert zero["status"] == "SUCCEEDED"
        assert zero["dispatch_count"] == 0 and zero["completion_id"]
        replica = create_app(
            settings=app.state.settings,
            db_engine=app.state.engine,
            redis_client=app.state.redis,
        )
        async with replica.router.lifespan_context(replica):
            registrations = await asyncio.gather(
                *(open_doc_subscription(replica, "archive") for _ in range(3))
            )
            assert registrations == [registrations[0]] * 3
            assert await admin(app, "list-subscriptions", "--business", BUSINESS) == [
                registrations[0]
            ]
            with pytest.raises(ValueError, match="Queue is assigned"):
                await admin(app, "open-subscription", BUSINESS, DOC_TYPE, "different", "x", ARCHIVE)
            replay = await source.ack(first, "source-one", summary={"page_count": 1})
            assert replay["completion_id"] == zero["completion_id"]
            assert replay["dispatch_count"] == 0 and replay["replayed"]
            assert await replica.state.delivery_dispatcher.dispatch_once() == 0
            second, one = await finish(requested=["archive"])
            assert one["dispatch_count"] == 1
            await admin(replica, "disable-subscription", BUSINESS, DOC_TYPE, "archive", "archive")
            again = await open_doc_subscription(app, "archive")
            assert again["enabled"] is False
            third, stopped = await finish(requested=["archive"])
            assert stopped["dispatch_count"] == 0
            assert await replica.state.delivery_dispatcher.dispatch_once() == 1
            rotated = await admin(replica, "rotate", "archive")
            old_id = credentials["archive"]["credential_id"]
            assert len(await admin(app, "list-credentials", "archive")) == 2
            await admin(app, "revoke-credential", old_id)
            assert all(
                set(row) == {"credential_id", "revoked"}
                for row in await admin(replica, "list-credentials", "archive")
            )
            async with app.state.session_factory() as session:
                stored = await session.get(ServiceCredential, rotated["credential_id"])
                assert len(stored.digest) == 64 and stored.digest not in rotated["token"]
            for instance in (app, replica):
                async with client(instance, credentials["archive"]) as old:
                    await response(
                        old,
                        "POST",
                        "/api/v1/tasks/pull",
                        status=401,
                        json={"queue": ARCHIVE, "worker_id": "old"},
                    )
            async with sdk(replica, rotated) as subscriber:
                (item,) = await subscriber.pull(ARCHIVE, "replacement")
                assert (
                    item.source_task_id == second.task_id
                    and item.completion_id == one["completion_id"]
                )
                assert item.artifacts == [] and item.payload["summary"] == {"page_count": 1}
                await subscriber.ack(item, "replacement", processing_result={})
                assert await subscriber.pull(ARCHIVE, "replacement") == []
            assert (await author.deliveries(first.task_id))["deliveries"] == []
            assert (await author.deliveries(third.task_id))["deliveries"] == []
            await admin(
                app, "revoke-grant", "archive", "pull", "--business", BUSINESS, "--queue", ARCHIVE
            )
            async with client(replica, rotated) as revoked:
                await response(
                    revoked,
                    "POST",
                    "/api/v1/tasks/pull",
                    status=403,
                    json={"queue": ARCHIVE, "worker_id": "next"},
                )


async def test_ack_snapshot_serializes_with_open_and_disable(access_app, monkeypatch):
    app = access_app
    credentials = await document_services(app)
    # Start an open transaction before ACK. The resulting completion must include the new binding.
    async with sdk(app, credentials["author"]) as author, sdk(app, credentials["source"]) as source:

        async def pull():
            await author.submit(task_type=DOC_TYPE)
            (task,) = await source.pull(SOURCE, "serial")
            return task

        task = await pull()
        async with app.state.session_factory.begin() as session:
            await execute(
                session,
                parser().parse_args(
                    ["open-subscription", BUSINESS, DOC_TYPE, "archive", "archive", ARCHIVE]
                ),
                app.state.delivery_registry,
            )
            blocked_ack = asyncio.create_task(source.ack(task, "serial", summary={"pages": 1}))
            await asyncio.sleep(0.05)
            assert not blocked_ack.done()
        assert (await blocked_ack)["dispatch_count"] == 1

        second = await pull()
        import app.services.dispatcher as dispatcher_module

        original = dispatcher_module.snapshot_recipients
        snapshot_read = asyncio.Event()
        allow_commit = asyncio.Event()

        async def hold_snapshot(*args, **kwargs):
            result = await original(*args, **kwargs)
            snapshot_read.set()
            await allow_commit.wait()
            return result

        with monkeypatch.context() as patch:
            patch.setattr(dispatcher_module, "snapshot_recipients", hold_snapshot)
            ack = asyncio.create_task(source.ack(second, "serial", summary={"pages": 2}))
            await asyncio.wait_for(snapshot_read.wait(), 2)
            disable = asyncio.create_task(
                admin(app, "disable-subscription", BUSINESS, DOC_TYPE, "archive", "archive")
            )
            await asyncio.sleep(0.05)
            assert not disable.done()
            allow_commit.set()
            assert (await ack)["dispatch_count"] == 1
            await disable
        third = await pull()
        assert (await source.ack(third, "serial"))["dispatch_count"] == 0
        async with app.state.session_factory() as session:
            completions = (await session.scalars(select(TaskCompletion))).all()
            assert {row.source_task_id: len(row.recipient_snapshot) for row in completions} == {
                task.task_id: 1,
                second.task_id: 1,
                third.task_id: 0,
            }


async def test_two_subscribers_restart_idempotent_write_and_dead_letter(access_app, tmp_path):
    app = access_app
    credentials = await document_services(app)
    for service in ("archive", "preview"):
        await open_doc_subscription(app, service)
    admin_credential = await identity(app, "ops", administrator=True)
    archive_db = tmp_path / "archive.sqlite3"
    preview_db = tmp_path / "preview.sqlite3"
    async with sdk(app, credentials["author"]) as author, sdk(app, credentials["source"]) as source:
        submitted = await author.submit(
            task_type=DOC_TYPE,
            lease_seconds=1200,
            payload={
                "title": "Quarterly report",
                "pages": ["first", "last"],
                "rendered_pdf": {"uri": "https://example.test/report.pdf", "version": "2026-11"},
            },
        )
        (item,) = await source.pull(SOURCE, "source-fixed")
        result = await Worker(source, SOURCE, "source-fixed").execute(item, inspect_document)
        assert result["dispatch_count"] == 2
        assert await app.state.delivery_dispatcher.dispatch_once() == 2
        async with sdk(app, credentials["preview"]) as preview:
            (job,) = await preview.pull(PREVIEW, "preview-1")
            assert job.source_task_id == submitted["task_id"]
            assert write_result(preview_db, job)["inserted"]
            assert (await preview.ack(job, "preview-1"))["status"] == "SUCCEEDED"
        # Archive was offline while Preview confirmed its own delivery.
        async with sdk(app, credentials["archive"]) as archive:
            (old,) = await archive.pull(ARCHIVE, "archive-1")
            assert old.task_id != job.task_id and old.artifacts == job.artifacts
            assert (
                old.source_task_id == job.source_task_id and old.completion_id == job.completion_id
            )
            assert write_result(archive_db, old)["inserted"]

        # Instance exited after committing its business write. Advance the expired lease and backoff.
        async def expire(task):
            redis, keys = app.state.redis, app.state.redis_mgr
            await redis.zadd(
                keys.lease_key(task.queue), {task.task_id: int(time.time() * 1000) - 1}
            )
            assert await app.state.scheduler.reap_once() == 1
            await redis.zadd(
                keys.delay_key(keys.delay_shard_for(task.task_id)),
                {task.task_id: int(time.time() * 1000) - 1},
            )
            await app.state.scheduler.pump_once()

        await expire(old)
        replica = create_app(
            settings=app.state.settings, db_engine=app.state.engine, redis_client=app.state.redis
        )
        async with (
            replica.router.lifespan_context(replica),
            sdk(replica, credentials["archive"]) as archive,
        ):
            async with sdk(replica, credentials["preview"]) as preview:
                assert await preview.pull(PREVIEW, "preview-2") == []
            (tasks_a, tasks_b) = await asyncio.gather(
                archive.pull(ARCHIVE, "archive-2"), archive.pull(ARCHIVE, "archive-3")
            )
            assert len(tasks_a) + len(tasks_b) == 1
            replacement = (tasks_a or tasks_b)[0]
            worker_name = "archive-2" if tasks_a else "archive-3"
            assert replacement.task_id == old.task_id
            assert replacement.lease_generation == old.lease_generation + 1
            assert not write_result(archive_db, replacement)["inserted"]
            await archive.ack(replacement, worker_name, processing_result={"idempotent": True})
            assert await archive.pull(ARCHIVE, "archive-4") == []
            async with sdk(replica, admin_credential) as ops:
                rerun = await ops.rerun_delivery(
                    item.task_id, "archive", "archive", idempotency_key="one"
                )
                duplicate = await ops.rerun_delivery(
                    item.task_id, "archive", "archive", idempotency_key="one"
                )
                assert rerun["processing_task_id"] == duplicate["processing_task_id"]
                (failed,) = await archive.pull(ARCHIVE, "archive-2")
                assert (await archive.nack(failed, "archive-2"))["outcome"] == "RETRY"
                await app.state.redis.zadd(
                    app.state.redis_mgr.delay_key(
                        app.state.redis_mgr.delay_shard_for(failed.task_id)
                    ),
                    {failed.task_id: int(time.time() * 1000) - 1},
                )
                await app.state.scheduler.pump_once()
                (failed_again,) = await archive.pull(ARCHIVE, "archive-3")
                assert (await archive.nack(failed_again, "archive-3"))["outcome"] == "DEAD"
                same = await ops.rerun(item.task_id, idempotency_key="new-source")
                assert (await ops.rerun(item.task_id, idempotency_key="new-source"))[
                    "task_id"
                ] == same["task_id"]
                trace = await ops.deliveries(item.task_id)
                assert trace["child_source_task_ids"] == [same["task_id"]]
                assert (await ops.artifacts(item.task_id))["artifacts"] == old.artifacts
                async with client(replica, admin_credential) as ops_http:
                    (dlq,) = await response(ops_http, "GET", f"/api/v1/dlq/{ARCHIVE}")
                    assert dlq["task_id"] == failed.task_id
        for database in (archive_db, preview_db):
            with sqlite3.connect(database) as conn:
                rows = conn.execute(
                    "SELECT source_task_id, completion_id FROM processed_results"
                ).fetchall()
            assert rows == [(item.task_id, result["completion_id"])]
        async with app.state.session_factory() as session:
            rows = await session.scalars(
                select(DeliveryDispatch).where(DeliveryDispatch.dispatch_kind == "normal")
            )
            assert len(rows.all()) == 2


async def test_worker_activity_proxy_filters_and_expiry(access_app):
    app = access_app
    credentials = await document_services(app)
    await open_doc_subscription(app, "archive")
    ops = await identity(app, "ops", administrator=True)
    await grant(app, "source", "pull", queue="empty", business=BUSINESS)
    async with (
        sdk(app, credentials["author"]) as author,
        client(app, ops) as view,
        client(
            app,
            credentials["source"],
            ip="10.10.0.3",
        ) as proxy,
        client(app, credentials["source"], ip="192.0.2.1") as direct,
    ):
        submitted = await author.submit(task_type=DOC_TYPE, lease_seconds=1200)
        claim = {"queue": SOURCE, "worker_id": "pool"}
        headers = {"X-Forwarded-For": "198.51.100.8, 10.10.0.2"}
        (task,) = (
            await response(proxy, "POST", "/api/v1/tasks/pull", json=claim, headers=headers)
        )["tasks"]
        heartbeat = await response(
            proxy,
            "POST",
            f"/api/v1/tasks/{task['task_id']}/heartbeat",
            headers=headers,
            json={**claim, "lease_generation": task["lease_generation"]},
        )
        assert heartbeat["lease_expire_at_ms"] - int(time.time() * 1000) > 1_100_000
        # Three pull loops using the shared instance ID still count as one Worker.
        for _ in range(3):
            assert (
                await response(
                    proxy,
                    "POST",
                    "/api/v1/tasks/pull",
                    json={**claim, "queue": "empty"},
                    headers=headers,
                )
            )["tasks"] == []
        await response(
            proxy,
            "POST",
            "/api/v1/tasks/pull",
            json={**claim, "worker_id": "other"},
            headers=headers,
        )
        await response(
            direct,
            "POST",
            "/api/v1/tasks/pull",
            json={**claim, "worker_id": "direct"},
            headers={"X-Forwarded-For": "203.0.113.7"},
        )
        await response(direct, "GET", "/api/v1/admin/workers", status=403)
        workers = await response(view, "GET", "/api/v1/admin/workers")
        assert workers["online_count"] == 3
        assert workers["online_window_seconds"] == 1
        by_id = {worker["worker_id"]: worker for worker in workers["workers"]}
        assert by_id["pool"]["queues"] == [SOURCE, "empty"]
        assert by_id["other"]["last_seen_ip"] == by_id["pool"]["last_seen_ip"] == "198.51.100.8"
        assert by_id["direct"]["last_seen_ip"] == "192.0.2.1"
        assert all(worker["last_seen_at"] > 0 for worker in by_id.values())
        filtered = await response(
            view, "GET", "/api/v1/admin/workers?service_name=source&queue=empty"
        )
        assert [worker["worker_id"] for worker in filtered["workers"]] == ["pool"]
        for key in [
            key
            async for key in app.state.redis.scan_iter(
                match=f"{app.state.redis_mgr.prefix}:worker_activity:*"
            )
        ]:
            assert 0 < await app.state.redis.pttl(key) <= 1000
        await asyncio.sleep(1.05)
        assert (await response(view, "GET", "/api/v1/admin/workers"))["online_count"] == 0
        assert (await response(view, "GET", f"/api/v1/tasks/{submitted['task_id']}"))[
            "status"
        ] == "RUNNING"
        assert await app.state.scheduler.reap_once() == 0
        async with app.state.session_factory() as session:
            subscription = await session.get(
                ResultSubscription, (BUSINESS, DOC_TYPE, "archive", "archive")
            )
            assert subscription.enabled is True
        async with sdk(app, credentials["source"]) as source:
            assert (await source.ack(Task(**task), "pool"))["dispatch_count"] == 1


async def test_tls_requires_trusted_gateway_or_secure_transport(access_app):
    app = access_app
    ops = await identity(app, "ops", administrator=True)
    app.state.settings.require_https = True
    async with client(app, ops, ip="192.0.2.1") as untrusted:
        await response(
            untrusted, "GET", "/api/v1/tasks", status=403, headers={"X-Forwarded-Proto": "https"}
        )
        assert await response(untrusted, "GET", "/health") == {"status": "ok"}
    async with client(app, ops, ip="10.10.0.5") as trusted:
        await response(trusted, "GET", "/api/v1/tasks", headers={"X-Forwarded-Proto": "https"})
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="https://scheduler.test",
        headers=bearer(ops),
    ) as secure:
        assert await response(secure, "GET", "/api/v1/tasks") == {"items": [], "next_cursor": None}


async def test_monitoring_delivery_aggregates_and_rerun_target_are_scoped(access_app):
    app = access_app
    credentials = await document_services(app)
    await open_doc_subscription(app, "archive")
    await open_doc_subscription(app, "preview")
    other_author = await identity(app, "other-author", "other", administrator=True)
    reader = await identity(app, "delivery-reader", BUSINESS)
    await grant(app, "delivery-reader", "task_read", business=BUSINESS, queue=SOURCE)
    await grant(app, "delivery-reader", "monitor", business=BUSINESS, queue=SOURCE)
    await grant(app, "delivery-reader", "monitor", business=BUSINESS, queue=ARCHIVE)
    await grant(app, "delivery-reader", "task_rerun", business=BUSINESS, queue=SOURCE)
    async with sdk(app, credentials["author"]) as author, sdk(app, credentials["source"]) as source:
        task = await author.submit(task_type=DOC_TYPE)
        (item,) = await source.pull(SOURCE, "one")
        assert (await source.ack(item, "one"))["dispatch_count"] == 2
    async with sdk(app, other_author) as other:
        foreign = await other.submit(task_type=DOC_TYPE, business_id="other")
        (item,) = await other.pull(SOURCE, "other")
        await other.ack(item, "other")
        await other.submit(task_type=DOC_TYPE, business_id="other")
    async with client(app, reader) as monitor:
        stats = await response(monitor, "GET", "/api/v1/deliveries/stats")
        assert stats["source_succeeded_count"] == 1
        assert (await response(monitor, "GET", "/api/v1/deliveries/stats?queue=documents.archive"))[
            "source_succeeded_count"
        ] == 0
        backlog = await response(monitor, "GET", "/api/v1/deliveries/backlog")
        assert backlog["source_ready"]["count"] == 0
        assert backlog["dispatch_pending"]["count"] == 1
        await response(monitor, "GET", "/api/v1/deliveries/stats?business_id=other", status=403)
        await response(monitor, "GET", f"/api/v1/tasks/{foreign['task_id']}/deliveries", status=403)
        await response(
            monitor, "GET", "/api/v1/deliveries/backlog?queue=documents.preview", status=403
        )
        path = f"/api/v1/tasks/{task['task_id']}/deliveries/archive/archive/rerun"
        await response(monitor, "POST", path, status=403, json={"idempotency_key": "step"})
        await grant(app, "delivery-reader", "task_rerun", business=BUSINESS, queue=ARCHIVE)
        await response(monitor, "POST", path, json={"idempotency_key": "step"})
        stats = await response(monitor, "GET", "/api/v1/deliveries/stats")
        assert stats["manual_rerun_count"] == 1


async def test_openapi_exposes_only_generic_result_contract(access_app):
    async with client(access_app) as http:
        spec = await response(http, "GET", "/openapi.json")
    paths = spec["paths"]
    for method, path in [
        ("get", "/api/v1/tasks/{task_id}/deliveries"),
        ("get", "/api/v1/tasks/{task_id}/artifacts"),
        ("post", "/api/v1/tasks/{task_id}/rerun"),
        ("post", "/api/v1/tasks/{task_id}/deliveries/{recipient_service}/{step}/rerun"),
    ]:
        assert paths[path][method]["security"] == [{"HTTPBearer": []}]
    schema = spec["components"]["schemas"]
    assert "task_type" in schema["TaskSubmitRequest"]["properties"]
    assert "task_type" in schema["PulledTask"]["properties"]
    assert {"source_task_id", "child_source_task_ids"} <= schema["TaskDeliveriesResponse"][
        "properties"
    ].keys()
    assert "source_succeeded_count" in schema["DeliveryStatsResponse"]["properties"]
    assert "source_ready" in schema["DeliveryBacklogResponse"]["properties"]
    rendered = json.dumps(spec)
    for obsolete in ("/collections/", "collection_type", "collection_task_id", "recollect"):
        assert obsolete not in rendered


async def test_source_recovers_ready_projection_and_parallel_dispatch_is_single(access_app):
    app = access_app
    credentials = await document_services(app)
    await open_doc_subscription(app, "archive")
    await open_doc_subscription(app, "preview")
    async with sdk(app, credentials["author"]) as author, sdk(app, credentials["source"]) as source:
        submitted = await asyncio.gather(
            *(author.submit(task_type=DOC_TYPE, idempotency_key="persistent") for _ in range(4))
        )
        assert len({item["task_id"] for item in submitted}) == 1
        assert sum(not item["deduplicated"] for item in submitted) == 1
        task_id = submitted[0]["task_id"]
        await app.state.redis.zrem(app.state.redis_mgr.ready_key(SOURCE), task_id)
        assert await app.state.delivery_dispatcher.reconcile_once() == 1
        (claimed,) = await source.pull(SOURCE, "one")
        result = await source.ack(claimed, "one")
        assert result["dispatch_count"] == 2
        dispatched = await asyncio.gather(
            *(app.state.delivery_dispatcher.dispatch_once() for _ in range(3))
        )
        assert sum(dispatched) == 2
        trace = await author.deliveries(task_id)
        assert len({item["processing_task_id"] for item in trace["deliveries"]}) == 2
