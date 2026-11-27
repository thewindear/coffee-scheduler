import json
from typing import Literal

from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field

from app.core.access import Identity, forbidden


class RecipientRule(BaseModel):
    recipient_service: str = Field(..., min_length=1, max_length=64)
    step: str = Field(..., min_length=1, max_length=64)
    queue: str = Field(..., min_length=1, max_length=64)
    delivery_mode: Literal["http", "kafka"] = "http"
    kafka_topic: str | None = Field(default=None, max_length=128)
    kafka_handoff_confirmed: bool = False
    enabled: bool = True


class TaskTypeConfig(BaseModel):
    task_type: str = Field(..., min_length=1, max_length=64)
    business_id: str = Field(..., min_length=1, max_length=64)
    queue: str = Field(..., min_length=1, max_length=64)
    delivery_enabled: bool = True
    default_services: list[RecipientRule] = Field(default_factory=list)
    shared_grants: list[RecipientRule] = Field(default_factory=list)
    artifact_retention_seconds: int | None = Field(default=None, ge=1)
    e2e_timeout_seconds: int | None = Field(default=None, ge=1)


class DeliveryConfigPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    task_types: list[TaskTypeConfig] = Field(default_factory=list)


class DeliveryRegistry:
    def __init__(self, raw_json: str | None = None) -> None:
        self._task_types: dict[tuple[str, str], TaskTypeConfig] = {}
        if raw_json:
            payload = DeliveryConfigPayload.model_validate(json.loads(raw_json))
            for cfg in payload.task_types:
                self.register_task_type(cfg)

    def register_task_type(self, cfg: TaskTypeConfig) -> None:
        if any(rule.delivery_mode == "http" for rule in cfg.default_services):
            raise ValueError("Register default HTTP recipients with the subscription CLI")
        seen = set()
        for rule in [*cfg.default_services, *cfg.shared_grants]:
            pair = (rule.recipient_service, rule.step)
            if pair in seen:
                raise ValueError(f"Duplicate recipient/step {pair} for {cfg.task_type}")
            seen.add(pair)
            if (
                rule.delivery_mode == "kafka"
                and rule.enabled
                and not (rule.kafka_topic and rule.kafka_handoff_confirmed)
            ):
                raise ValueError(
                    f"Kafka delivery for {pair} requires a topic and confirmed handoff"
                )
            if rule.delivery_mode == "http" and (
                rule.queue == cfg.queue
                or not self.subscription_queue_allowed(
                    rule.queue, cfg.business_id, rule.recipient_service
                )
            ):
                raise ValueError(f"HTTP recipient queue {rule.queue} is assigned to another route")
        if self.is_processing_queue(cfg.queue):
            raise ValueError(f"Source queue {cfg.queue} is assigned to an HTTP recipient")
        http_owners: dict[str, str] = {}
        for rule in cfg.shared_grants:
            if (
                rule.delivery_mode == "http"
                and http_owners.setdefault(rule.queue, rule.recipient_service)
                != rule.recipient_service
            ):
                raise ValueError(
                    f"HTTP recipient queue {rule.queue} is assigned to multiple services"
                )
        self._task_types[(cfg.business_id, cfg.task_type)] = cfg

    def is_processing_queue(self, queue: str) -> bool:
        return any(
            r.delivery_mode == "http" and r.queue == queue
            for cfg in self._task_types.values()
            for r in cfg.shared_grants
        )

    def subscription_queue_allowed(self, queue: str, business_id: str, service: str) -> bool:
        return all(
            cfg.queue != queue
            and all(
                r.delivery_mode != "http"
                or r.queue != queue
                or (cfg.business_id == business_id and r.recipient_service == service)
                for r in cfg.shared_grants
            )
            for cfg in self._task_types.values()
        )

    def get_task_type(self, business_id: str, task_type: str) -> TaskTypeConfig | None:
        return self._task_types.get((business_id, task_type))

    def resolve_submission(
        self, identity: Identity, business_id: str | None, task_type: str
    ) -> TaskTypeConfig:
        cfg = self.get_task_type(business_id or "", task_type)
        if cfg is None:
            raise forbidden()
        identity.require("submit", business_id, cfg.queue, task_type)
        return cfg

    @staticmethod
    def configured_recipients(
        cfg: TaskTypeConfig | None, requested: list[str] | None
    ) -> list[RecipientRule]:
        if cfg is None or not cfg.delivery_enabled:
            return []
        return [r for r in cfg.default_services if r.enabled] + [
            r for r in cfg.shared_grants if r.enabled and r.recipient_service in (requested or [])
        ]

    @staticmethod
    def validate_recipients(cfg: TaskTypeConfig, requested: list[str] | None) -> None:
        allowed = {
            r.recipient_service for r in [*cfg.default_services, *cfg.shared_grants] if r.enabled
        }
        if requested and (not cfg.delivery_enabled or set(requested) - allowed):
            raise HTTPException(403, "An additional recipient is outside the configured grants")
