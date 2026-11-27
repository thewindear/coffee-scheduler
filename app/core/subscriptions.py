from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.access import Grant, insert_once, set_grant
from app.core.delivery_config import DeliveryRegistry, RecipientRule
from app.models.tables import (
    ResultSubscription,
    ServiceIdentity,
    SubscriptionQueue,
    SubscriptionScope,
)


async def lock_subscription_scope(session: AsyncSession, business_id: str, task_type: str) -> None:
    # The upsert takes a write lock for both existing and newly configured routes until commit.
    await insert_once(
        session, SubscriptionScope, "business_id", business_id=business_id, task_type=task_type
    )


async def open_subscription(
    session: AsyncSession,
    registry: DeliveryRegistry,
    business_id: str,
    task_type: str,
    recipient_service: str,
    step: str,
    queue: str,
) -> ResultSubscription:
    if "/" in recipient_service or "/" in step:
        raise ValueError("Recipient service and step must each fit in one HTTP path segment")
    cfg = registry.get_task_type(business_id, task_type)
    if not cfg or not cfg.delivery_enabled:
        raise ValueError("Task type must be configured with result delivery enabled")
    await lock_subscription_scope(session, business_id, task_type)
    key = (business_id, task_type, recipient_service, step)
    existing = await session.get(ResultSubscription, key, with_for_update=True)
    if existing is not None:
        if existing.queue != queue:
            raise ValueError("This subscription already uses a different queue")
        return existing
    if await session.get(ServiceIdentity, recipient_service) is None:
        raise ValueError("Recipient service does not exist")
    if not registry.subscription_queue_allowed(queue, business_id, recipient_service):
        raise ValueError("Queue is assigned to another route")
    await insert_once(
        session,
        SubscriptionQueue,
        "queue",
        queue=queue,
        business_id=business_id,
        recipient_service=recipient_service,
    )
    owner = await session.get(SubscriptionQueue, queue, with_for_update=True)
    if (owner.business_id, owner.recipient_service) != (business_id, recipient_service):
        raise ValueError("Queue is assigned to another business or recipient service")
    row = ResultSubscription(
        business_id=business_id,
        task_type=task_type,
        recipient_service=recipient_service,
        step=step,
        queue=queue,
        enabled=True,
    )
    session.add(row)
    await set_grant(session, recipient_service, Grant("pull", business_id, queue))
    return row


async def set_subscription_enabled(
    session: AsyncSession,
    business_id: str,
    task_type: str,
    recipient_service: str,
    step: str,
    *,
    enabled: bool,
) -> ResultSubscription:
    await lock_subscription_scope(session, business_id, task_type)
    row = await session.get(
        ResultSubscription,
        (business_id, task_type, recipient_service, step),
        with_for_update=True,
    )
    if row is None:
        raise ValueError("Subscription does not exist")
    row.enabled = enabled
    return row


async def snapshot_recipients(
    session: AsyncSession,
    registry: DeliveryRegistry,
    business_id: str,
    task_type: str,
    requested: list[str] | None,
) -> list[RecipientRule]:
    cfg = registry.get_task_type(business_id, task_type)
    rules = {
        (r.recipient_service, r.step): r for r in registry.configured_recipients(cfg, requested)
    }
    rows = (
        await session.scalars(
            select(ResultSubscription)
            .where(
                ResultSubscription.business_id == business_id,
                ResultSubscription.task_type == task_type,
            )
            .with_for_update()
        )
    ).all()
    for row in rows:
        key = (row.recipient_service, row.step)
        if row.enabled:
            rules[key] = RecipientRule(
                recipient_service=row.recipient_service, step=row.step, queue=row.queue
            )
        else:
            rules.pop(key, None)
    return list(rules.values())
