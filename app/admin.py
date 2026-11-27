"""Deployment-side administration. Run with python -m app.admin."""

import argparse
import asyncio
import json

from sqlalchemy import select

from app.core.access import OPERATIONS, Grant, issue_credential, set_grant
from app.core.config import get_settings
from app.core.db import create_db_engine, create_session_factory
from app.core.delivery_config import DeliveryRegistry
from app.core.subscriptions import open_subscription, set_subscription_enabled
from app.models.tables import ResultSubscription, ServiceCredential, ServiceIdentity


def name(value: str) -> str:
    if not value or value != value.strip() or len(value) > 64:
        raise argparse.ArgumentTypeError(
            "Use a nonempty name of up to 64 characters without surrounding whitespace"
        )
    return value


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(description=__doc__)
    sub = cli.add_subparsers(dest="command", required=True)
    create = sub.add_parser("create-service")
    create.add_argument("service", type=name)
    create.add_argument("--business", type=name)
    create.add_argument("--admin", action="store_true")
    for command in ("issue", "rotate", "list-credentials"):
        sub.add_parser(command).add_argument("service", type=name)
    sub.add_parser("revoke-credential").add_argument("credential_id")
    for command in ("grant", "revoke-grant"):
        action = sub.add_parser(command)
        action.add_argument("service", type=name)
        action.add_argument("operation", choices=OPERATIONS)
        action.add_argument("--business", type=name)
        action.add_argument("--queue", type=name)
        action.add_argument("--task-type", type=name)
    for command in ("open-subscription", "disable-subscription", "enable-subscription"):
        action = sub.add_parser(command)
        action.add_argument("business", type=name)
        action.add_argument("task_type", type=name)
        action.add_argument("service", type=name)
        action.add_argument("step", type=name)
        if command == "open-subscription":
            action.add_argument("queue", type=name)
    listing = sub.add_parser("list-subscriptions")
    listing.add_argument("--business", type=name)
    listing.add_argument("--task-type", type=name)
    return cli


def subscription_view(row: ResultSubscription) -> dict:
    return {
        key: getattr(row, key)
        for key in ("business_id", "task_type", "recipient_service", "step", "queue", "enabled")
    }


async def execute(session, args, registry: DeliveryRegistry):
    command = args.command
    if command == "create-service":
        if await session.get(ServiceIdentity, args.service):
            raise ValueError("Service already exists")
        session.add(
            ServiceIdentity(name=args.service, business_id=args.business, administrator=args.admin)
        )
        return {"service_name": args.service}
    if command in ("issue", "rotate", "grant", "revoke-grant", "list-credentials"):
        if not await session.get(ServiceIdentity, args.service):
            raise ValueError("Service does not exist")
    if command in ("issue", "rotate"):
        credential_id, token = issue_credential(session, args.service)
        return {"credential_id": credential_id, "token": token}
    if command == "list-credentials":
        rows = await session.scalars(
            select(ServiceCredential).where(ServiceCredential.service_name == args.service)
        )
        return [{"credential_id": row.credential_id, "revoked": row.revoked} for row in rows]
    if command == "revoke-credential":
        row = await session.get(ServiceCredential, args.credential_id)
        if row is None:
            raise ValueError("Credential does not exist")
        row.revoked = True
        return {"credential_id": row.credential_id, "revoked": True}
    if command in ("grant", "revoke-grant"):
        if args.operation == "metrics" and (args.business or args.queue or args.task_type):
            raise ValueError("Metrics access is deployment-wide")
        if args.task_type and args.operation in {
            "monitor",
            "cron_read",
            "cron_manage",
            "dlq_read",
            "dlq_retry",
        }:
            raise ValueError("This operation is scoped by business and queue")
        await set_grant(
            session,
            args.service,
            Grant(args.operation, args.business or "", args.queue or "", args.task_type or ""),
            revoke=command == "revoke-grant",
        )
        return {
            "service_name": args.service,
            "operation": args.operation,
            "revoked": command == "revoke-grant",
        }
    if command == "list-subscriptions":
        stmt = select(ResultSubscription)
        if args.business:
            stmt = stmt.where(ResultSubscription.business_id == args.business)
        if args.task_type:
            stmt = stmt.where(ResultSubscription.task_type == args.task_type)
        return [subscription_view(row) for row in await session.scalars(stmt)]
    binding = (args.business, args.task_type, args.service, args.step)
    if command == "open-subscription":
        row = await open_subscription(session, registry, *binding, args.queue)
    else:
        row = await set_subscription_enabled(
            session, *binding, enabled=command == "enable-subscription"
        )
    return subscription_view(row)


async def run(args):
    cfg = get_settings()
    engine = create_db_engine(cfg)
    try:
        async with create_session_factory(engine).begin() as session:
            return await execute(session, args, DeliveryRegistry(cfg.delivery_config_json))
    finally:
        await engine.dispose()


def main() -> None:
    cli = parser()
    args = cli.parse_args()
    try:
        result = asyncio.run(run(args))
    except ValueError as exc:
        cli.exit(1, f"{exc}\n")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
