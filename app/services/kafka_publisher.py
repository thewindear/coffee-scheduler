import logging
import time
from typing import Any, Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.delivery_config import DeliveryRegistry
from app.core.uuidv7 import uuid7
from app.models.tables import DeliveryDispatch, TaskCompletion

logger = logging.getLogger(__name__)


class KafkaProducerProtocol(Protocol):
    async def publish(self, topic: str, key: str, value: dict[str, Any]) -> None: ...


class InMemoryKafkaProducer:
    """In-memory Kafka producer for testing and local verification."""

    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []
        self.fail_next: bool = False

    async def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("Simulated Kafka broker failure")
        self.messages.append({"topic": topic, "key": key, "value": value})


class KafkaCompatPublisher:
    """Publishes legacy Kafka messages for result-delivery dispatches configured with delivery_mode='kafka'."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        registry: DeliveryRegistry,
        producer: KafkaProducerProtocol | None = None,
        batch_size: int = 100,
    ) -> None:
        self.session_factory = session_factory
        self.registry = registry
        self.producer: KafkaProducerProtocol = producer or InMemoryKafkaProducer()
        self.batch_size = batch_size

    async def publish_pending_once(self) -> int:
        published_count = 0
        async with self.session_factory() as session:
            stmt = (
                select(DeliveryDispatch)
                .where(
                    DeliveryDispatch.status == "PENDING",
                    DeliveryDispatch.delivery_mode == "kafka",
                )
                .order_by(DeliveryDispatch.created_at.asc())
                .limit(self.batch_size)
            )
            dispatches = (await session.execute(stmt)).scalars().all()
            for dispatch in dispatches:
                completion = await session.get(TaskCompletion, dispatch.completion_id)
                if completion is None:
                    continue

                # Guard against dual publishing: confirm Kafka handoff on current rule if present
                ct_cfg = self.registry.get_collection_type(
                    dispatch.business_id, completion.collection_type
                )
                if ct_cfg is not None:
                    matching_rule = next(
                        (
                            r
                            for r in [*ct_cfg.default_services, *ct_cfg.shared_grants]
                            if r.recipient_service == dispatch.recipient_service
                            and r.step == dispatch.step
                        ),
                        None,
                    )
                    if (
                        matching_rule is not None
                        and matching_rule.delivery_mode == "kafka"
                        and not matching_rule.kafka_handoff_confirmed
                    ):
                        dispatch.last_error = "KAFKA_HANDOFF_NOT_CONFIRMED"
                        await session.commit()
                        continue

                if not dispatch.kafka_delivery_id:
                    dispatch.kafka_delivery_id = uuid7(int(time.time() * 1000))
                    await session.commit()

                topic = dispatch.kafka_topic or f"coffee.delivery.{dispatch.recipient_service}"
                message = {
                    "delivery_id": dispatch.kafka_delivery_id,
                    "collection_task_id": dispatch.collection_task_id,
                    "completion_id": dispatch.completion_id,
                    "business_id": dispatch.business_id,
                    "business_request_id": completion.business_request_id,
                    "collection_type": completion.collection_type,
                    "recipient_service": dispatch.recipient_service,
                    "step": dispatch.step,
                    "completed_at_ms": completion.completed_at_ms,
                    "summary": completion.summary,
                    "artifacts": completion.artifacts,
                }
                try:
                    await self.producer.publish(
                        topic=topic,
                        key=dispatch.collection_task_id,
                        value=message,
                    )
                except Exception as exc:
                    logger.warning(
                        "Kafka publish failed for dispatch %s: %s", dispatch.dispatch_id, exc
                    )
                    dispatch.last_error = str(exc)[:512]
                    await session.commit()
                    continue

                dispatch.status = "KAFKA_PUBLISHED"
                dispatch.kafka_published_at_ms = int(time.time() * 1000)
                dispatch.last_error = None
                await session.commit()
                published_count += 1
        return published_count
