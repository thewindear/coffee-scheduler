import asyncio
import time


async def test_submit_pull_heartbeat_ack_with_artifacts(app_bundle):
    app, client, _, _ = app_bundle

    # 1. Submit instant task
    resp = await client.post(
        "/api/v1/tasks",
        json={"queue": "spider", "payload": {"url": "https://example.com"}, "priority": 7},
    )
    assert resp.status_code == 200
    data = resp.json()
    task_id = data["task_id"]
    assert data["status"] == "READY"

    # 2. Pull task
    pull_resp = await client.post(
        "/api/v1/tasks/pull",
        json={"queue": "spider", "worker_id": "crawler-1", "batch_size": 5},
    )
    assert pull_resp.status_code == 200
    pulled = pull_resp.json()["tasks"]
    assert len(pulled) == 1
    assert pulled[0]["task_id"] == task_id
    assert pulled[0]["priority"] == 7

    # Verify RUNNING & worker_id marker via GET
    detail = (await client.get(f"/api/v1/tasks/{task_id}")).json()
    assert detail["status"] == "RUNNING"
    assert detail["worker_id"] == "crawler-1"
    assert detail["source"] == "redis"

    # 3. Heartbeat
    hb = await client.post(
        f"/api/v1/tasks/{task_id}/heartbeat",
        json={
            "queue": "spider",
            "worker_id": "crawler-1",
            "extend_seconds": 120,
            "lease_generation": pulled[0]["lease_generation"],
        },
    )
    assert hb.status_code == 200
    assert hb.json()["lease_expire_at_ms"] > int(time.time() * 1000)

    # 4. Ack with polymorphic artifacts (OSS, Kafka, Redis)
    ack_resp = await client.post(
        f"/api/v1/tasks/{task_id}/ack",
        json={
            "queue": "spider",
            "worker_id": "crawler-1",
            "lease_generation": pulled[0]["lease_generation"],
            "duration_ms": 340,
            "summary": {"count": 100},
            "artifacts": [
                {"storage": "oss", "uri": "oss://bucket/a.parquet", "meta": {"rows": 100}},
                {"storage": "kafka", "uri": "kafka://cluster/topic-1", "meta": {"offset": 42}},
                {"storage": "redis", "uri": "redis://cache/k1", "meta": {"ttl": 3600}},
            ],
        },
    )
    assert ack_resp.status_code == 200
    assert ack_resp.json()["status"] == "SUCCEEDED"

    await app.state.flusher.drain_once()
    detail_after = (await client.get(f"/api/v1/tasks/{task_id}")).json()
    assert detail_after["status"] == "SUCCEEDED"
    assert len(detail_after["receipt"]["artifacts"]) == 3


async def test_priority_ordering_and_concurrent_pull_isolation(app_bundle):
    _, client, _, _ = app_bundle

    # Submit tasks with priorities 1, 9, 5
    ids_by_prio = {}
    for prio in (1, 9, 5):
        r = await client.post(
            "/api/v1/tasks",
            json={"queue": "prio_q", "payload": {"prio": prio}, "priority": prio},
        )
        ids_by_prio[prio] = r.json()["task_id"]

    # Pull one by one to verify 9 -> 5 -> 1 order
    pulled_order = []
    for i in range(3):
        r = await client.post(
            "/api/v1/tasks/pull",
            json={"queue": "prio_q", "worker_id": f"w-{i}", "batch_size": 1},
        )
        pulled_order.append(r.json()["tasks"][0]["task_id"])
    assert pulled_order == [ids_by_prio[9], ids_by_prio[5], ids_by_prio[1]]

    # Concurrent pull isolation across 20 tasks and 4 workers
    batch_payload = [{"queue": "conc_q", "payload": {"i": i}} for i in range(20)]
    await client.post("/api/v1/tasks/batch", json={"tasks": batch_payload})

    results = await asyncio.gather(
        *[
            client.post(
                "/api/v1/tasks/pull",
                json={"queue": "conc_q", "worker_id": f"worker-{idx}", "batch_size": 10},
            )
            for idx in range(4)
        ]
    )
    all_pulled_ids = [t["task_id"] for r in results for t in r.json()["tasks"]]
    assert len(all_pulled_ids) == 20
    assert len(set(all_pulled_ids)) == 20


async def test_idempotency_and_delayed_pump(app_bundle):
    app, client, _, _ = app_bundle

    # Idempotency deduplication
    r1 = await client.post(
        "/api/v1/tasks",
        json={"queue": "dedup_q", "payload": {"x": 1}, "idempotency_key": "req-001"},
    )
    r2 = await client.post(
        "/api/v1/tasks",
        json={"queue": "dedup_q", "payload": {"x": 1}, "idempotency_key": "req-001"},
    )
    assert r1.json()["task_id"] == r2.json()["task_id"]
    assert r1.json()["deduplicated"] is False
    assert r2.json()["deduplicated"] is True

    # Delayed task with past-due run_at_ms -> pump moves to READY
    past_ms = int(time.time() * 1000) + 50
    dr = await client.post(
        "/api/v1/tasks",
        json={"queue": "delay_q", "payload": {"d": 1}, "run_at_ms": past_ms},
    )
    assert dr.json()["status"] == "DELAYED"
    await asyncio.sleep(0.08)
    moved = await app.state.scheduler.pump_once()
    assert moved >= 1

    pulled = (
        await client.post(
            "/api/v1/tasks/pull",
            json={"queue": "delay_q", "worker_id": "w1", "batch_size": 5},
        )
    ).json()["tasks"]
    assert len(pulled) == 1
    assert pulled[0]["task_id"] == dr.json()["task_id"]


async def test_ack_payload_size_limit(app_bundle):
    _, client, _, _ = app_bundle
    r = await client.post("/api/v1/tasks", json={"queue": "size_q", "payload": {}})
    tid = r.json()["task_id"]
    await client.post(
        "/api/v1/tasks/pull", json={"queue": "size_q", "worker_id": "w1", "batch_size": 1}
    )

    huge_ack = await client.post(
        f"/api/v1/tasks/{tid}/ack",
        json={
            "queue": "size_q",
            "worker_id": "w1",
            "extra": {"blob": "x" * 20000},
        },
    )
    assert huge_ack.status_code == 413
