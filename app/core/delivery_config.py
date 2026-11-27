import json
from typing import Literal

from fastapi import HTTPException, status
from pydantic import BaseModel, Field


class RecipientRule(BaseModel):
    recipient_service: str = Field(..., min_length=1, max_length=64)
    step: str = Field(..., min_length=1, max_length=64)
    queue: str = Field(..., min_length=1, max_length=64)
    delivery_mode: Literal["http", "kafka"] = "http"
    kafka_topic: str | None = Field(default=None, max_length=128)
    kafka_handoff_confirmed: bool = False
    enabled: bool = True


class CollectionTypeConfig(BaseModel):
    collection_type: str = Field(..., min_length=1, max_length=64)
    business_id: str = Field(..., min_length=1, max_length=64)
    queue: str = Field(..., min_length=1, max_length=64)
    delivery_enabled: bool = True
    default_services: list[RecipientRule] = Field(default_factory=list)
    shared_grants: list[RecipientRule] = Field(default_factory=list)
    artifact_retention_seconds: int | None = Field(default=None, ge=1)
    e2e_timeout_seconds: int | None = Field(default=None, ge=1)


class IdentityConfig(BaseModel):
    token: str = Field(..., min_length=1, max_length=256)
    principal_id: str = Field(..., min_length=1, max_length=128)
    role: Literal["business", "service", "collector", "operator"]
    business_id: str | None = Field(default=None, max_length=64)
    service_name: str | None = Field(default=None, max_length=64)
    allowed_queues: list[str] = Field(default_factory=list)


class DeliveryConfigPayload(BaseModel):
    identities: list[IdentityConfig] = Field(default_factory=list)
    collection_types: list[CollectionTypeConfig] = Field(default_factory=list)


class DeliveryRegistry:
    """In-memory registry for business identities, collection routing, and authorization."""

    def __init__(self, raw_json: str | None = None) -> None:
        self._identities_by_token: dict[str, IdentityConfig] = {}
        self._collection_types: dict[tuple[str, str], CollectionTypeConfig] = {}
        self._types_by_name: dict[str, list[CollectionTypeConfig]] = {}
        self._protected_queues: set[str] = set()
        if raw_json:
            payload = DeliveryConfigPayload.model_validate(json.loads(raw_json))
            for ident in payload.identities:
                self.register_identity(ident)
            for ct in payload.collection_types:
                self.register_collection_type(ct)

    @staticmethod
    def validate_collection_type(cfg: CollectionTypeConfig) -> None:
        if not cfg.delivery_enabled:
            return
        enabled_defaults = [r for r in cfg.default_services if r.enabled]
        if not enabled_defaults:
            raise ValueError(
                f"Collection type '{cfg.collection_type}' must have at least one enabled default recipient service"
            )
        seen_pairs: set[tuple[str, str]] = set()
        for rule in [*cfg.default_services, *cfg.shared_grants]:
            pair = (rule.recipient_service, rule.step)
            if pair in seen_pairs:
                raise ValueError(
                    f"Duplicate recipient/step rule ({rule.recipient_service}, {rule.step}) for '{cfg.collection_type}'"
                )
            seen_pairs.add(pair)
            if rule.delivery_mode == "kafka" and rule.enabled:
                if not rule.kafka_topic:
                    raise ValueError(
                        f"Kafka delivery for ({rule.recipient_service}, {rule.step}) requires kafka_topic"
                    )
                if not rule.kafka_handoff_confirmed:
                    raise ValueError(
                        f"Kafka delivery for ({rule.recipient_service}, {rule.step}) requires kafka_handoff_confirmed=True"
                    )

    def register_identity(self, identity: IdentityConfig) -> None:
        self._identities_by_token[identity.token] = identity

    def register_collection_type(self, cfg: CollectionTypeConfig) -> None:
        self.validate_collection_type(cfg)
        key = (cfg.business_id, cfg.collection_type)
        self._collection_types[key] = cfg
        existing = [
            c
            for c in self._types_by_name.get(cfg.collection_type, [])
            if c.business_id != cfg.business_id
        ]
        existing.append(cfg)
        self._types_by_name[cfg.collection_type] = existing
        self._protected_queues.add(cfg.queue)
        for rule in [*cfg.default_services, *cfg.shared_grants]:
            self._protected_queues.add(rule.queue)

    def is_protected_queue(self, queue: str) -> bool:
        return queue in self._protected_queues

    def has_identities(self) -> bool:
        return bool(self._identities_by_token)

    def resolve_identity(self, token: str | None) -> IdentityConfig | None:
        if not token:
            return None
        ident = self._identities_by_token.get(token)
        if ident is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication credentials",
            )
        return ident

    def get_collection_type(
        self, business_id: str, collection_type: str
    ) -> CollectionTypeConfig | None:
        return self._collection_types.get((business_id, collection_type))

    def resolve_submission(
        self,
        identity: IdentityConfig | None,
        collection_type: str,
        requested_recipients: list[str] | None = None,
    ) -> tuple[CollectionTypeConfig, list[RecipientRule]]:
        if identity is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication is required to submit collection_type tasks",
            )
        if identity.role not in ("business", "operator") or not identity.business_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Caller identity is not authorized to submit business collection tasks",
            )
        cfg = self.get_collection_type(identity.business_id, collection_type)
        if cfg is None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Collection type '{collection_type}' is not configured for business '{identity.business_id}'",
            )
        selected = [r for r in cfg.default_services if r.enabled]
        if requested_recipients:
            grants_by_svc: dict[str, list[RecipientRule]] = {}
            for r in [*cfg.default_services, *cfg.shared_grants]:
                if r.enabled:
                    grants_by_svc.setdefault(r.recipient_service, []).append(r)
            existing_pairs = {(r.recipient_service, r.step) for r in selected}
            for svc in requested_recipients:
                if svc not in grants_by_svc:
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=f"Recipient service '{svc}' is not authorized for '{collection_type}'",
                    )
                for rule in grants_by_svc[svc]:
                    if (rule.recipient_service, rule.step) not in existing_pairs:
                        selected.append(rule)
                        existing_pairs.add((rule.recipient_service, rule.step))
        return cfg, selected

    def snapshot_recipients_for_completion(
        self,
        business_id: str,
        collection_type: str,
        requested_recipients: list[str] | None = None,
    ) -> list[RecipientRule]:
        cfg = self.get_collection_type(business_id, collection_type)
        if cfg is None or not cfg.delivery_enabled:
            return []
        selected = [r for r in cfg.default_services if r.enabled]
        if requested_recipients:
            grants_by_svc: dict[str, list[RecipientRule]] = {}
            for r in [*cfg.default_services, *cfg.shared_grants]:
                if r.enabled:
                    grants_by_svc.setdefault(r.recipient_service, []).append(r)
            existing_pairs = {(r.recipient_service, r.step) for r in selected}
            for svc in requested_recipients:
                for rule in grants_by_svc.get(svc, []):
                    if (rule.recipient_service, rule.step) not in existing_pairs:
                        selected.append(rule)
                        existing_pairs.add((rule.recipient_service, rule.step))
        return selected

    def authorize_queue_pull(self, identity: IdentityConfig | None, queue: str) -> None:
        if not self.is_protected_queue(queue):
            return
        if identity is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required for protected delivery queue",
            )
        if identity.role == "operator":
            return
        if identity.allowed_queues and queue in identity.allowed_queues:
            return
        if identity.role == "service" and identity.service_name:
            for cfg in self._collection_types.values():
                for r in [*cfg.default_services, *cfg.shared_grants]:
                    if r.queue == queue and r.recipient_service == identity.service_name:
                        return
        if identity.role == "collector":
            for cfg in self._collection_types.values():
                if cfg.queue == queue and (
                    identity.business_id is None or identity.business_id == cfg.business_id
                ):
                    return
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Identity '{identity.principal_id}' is not authorized for queue '{queue}'",
        )

    def authorize_business_access(
        self,
        identity: IdentityConfig | None,
        business_id: str,
        *,
        allow_service_recipient: str | None = None,
    ) -> None:
        if identity is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required",
            )
        if identity.role == "operator":
            return
        if identity.role == "business" and identity.business_id == business_id:
            return
        if (
            allow_service_recipient
            and identity.role == "service"
            and identity.service_name == allow_service_recipient
        ):
            return
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Cross-business or unauthorized access is forbidden",
        )
