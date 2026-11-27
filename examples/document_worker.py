"""Document inspection and two independent subscribers over the public API."""

import argparse
import asyncio
import json
import os
import sqlite3
from pathlib import Path

from coffee_scheduler_worker import SchedulerClient, Task, Worker

TASK_TYPE = "document.inspect"


async def inspect_document(task: Task) -> dict:
    pages = task.payload["pages"]
    summary = {"title": task.payload["title"], "page_count": len(pages)}
    artifact = task.payload.get("rendered_pdf")
    return {
        "summary": summary,
        "artifacts": [
            {"storage": "https", "uri": artifact["uri"], "meta": {"version": artifact["version"]}},
        ]
        if artifact
        else [],
    }


def write_result(database: Path, task: Task) -> dict:
    payload = task.payload
    if payload["step"] == "archive":
        output = {"document": payload["summary"]["title"], "pdfs": payload["artifacts"]}
    else:
        output = {"pages": payload["summary"]["page_count"], "title": payload["summary"]["title"]}
    with sqlite3.connect(database) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_results (
                task_id TEXT PRIMARY KEY, source_task_id TEXT NOT NULL,
                completion_id TEXT NOT NULL, output TEXT NOT NULL
            )
        """)
        cursor = conn.execute(
            "INSERT INTO processed_results VALUES (?, ?, ?, ?) ON CONFLICT(task_id) DO NOTHING",
            (task.task_id, task.source_task_id, task.completion_id, json.dumps(output)),
        )
        inserted = cursor.rowcount == 1
    return {"inserted": inserted}


async def main() -> None:
    args_parser = argparse.ArgumentParser(description=__doc__)
    subparsers = args_parser.add_subparsers(dest="command", required=True)
    submit = subparsers.add_parser("submit")
    submit.add_argument("--with-artifact", action="store_true")
    for name, queue in [
        ("inspect", "documents.inspect"),
        ("archive", "documents.archive"),
        ("preview", "documents.preview"),
    ]:
        worker = subparsers.add_parser(name)
        worker.add_argument("--queue", default=queue)
        worker.add_argument("--worker-id", required=True)
        worker.add_argument("--database", type=Path, default=Path(f"{name}.sqlite3"))
        worker.add_argument("--once", action="store_true")
    args = args_parser.parse_args()

    async with SchedulerClient(
        os.environ["SCHEDULER_URL"], os.environ["SCHEDULER_TOKEN"]
    ) as client:
        if args.command == "submit":
            payload = {"title": "Quarterly report", "pages": ["Summary", "Revenue"]}
            if args.with_artifact:
                payload["rendered_pdf"] = {
                    "uri": "https://example.com/documents/report.pdf",
                    "version": "v1",
                }
            print(
                json.dumps(
                    await client.submit(task_type=TASK_TYPE, payload=payload, lease_seconds=30)
                )
            )
            return

        async def consume(task: Task) -> dict:
            return {"processing_result": write_result(args.database, task)}

        handler = inspect_document if args.command == "inspect" else consume
        worker = Worker(client, args.queue, args.worker_id, auto_renew=True, execution_timeout=15)
        if args.once:
            tasks = await client.pull(args.queue, args.worker_id)
            if not tasks:
                raise SystemExit("No task was available")
            print(json.dumps(await worker.execute(tasks[0], handler)))
        else:
            await worker.run(handler)


if __name__ == "__main__":
    asyncio.run(main())
