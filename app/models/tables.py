from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    Index,
    Integer,
    PrimaryKeyConstraint,
    SmallInteger,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class CronJob(Base):
    __tablename__ = "cron_jobs"
    __table_args__ = (
        UniqueConstraint("name", name="uk_name"),
        Index("idx_scan", "enabled", "next_run_at"),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    queue: Mapped[str] = mapped_column(String(64), nullable=False)
    priority: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=5)
    cron_expr: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    max_retries: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=3)
    lease_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=60)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    next_run_at: Mapped[int] = mapped_column(BigInteger, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class TaskRecord(Base):
    __tablename__ = "task_records"
    __table_args__ = (
        PrimaryKeyConstraint("task_id", "created_at"),
        Index("idx_queue_status", "queue", "status", "created_at"),
        Index("idx_worker", "worker_id", "pulled_at"),
        Index("idx_business_created", "business_id", "created_at"),
        Index("idx_source_task", "source_task_id"),
        Index("idx_completion", "completion_id"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    task_id: Mapped[str] = mapped_column(String(36), nullable=False)
    queue: Mapped[str] = mapped_column(String(64), nullable=False)
    priority: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=5)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    idempotency_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    receipt: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    retry_count: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0)
    worker_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    scheduled_at: Mapped[int] = mapped_column(BigInteger, nullable=False)
    pulled_at: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    finished_at: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    # Result-delivery fields (defaulted/nullable for backward compatibility with normal tasks)
    task_kind: Mapped[str] = mapped_column(String(24), nullable=False, default="normal")
    business_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    business_request_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    collection_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    lease_generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    source_task_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    completion_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    recipient_service: Mapped[str | None] = mapped_column(String(64), nullable=True)
    step: Mapped[str | None] = mapped_column(String(64), nullable=True)
    parent_task_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    rerun_of_task_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    processing_result: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class TaskCompletion(Base):
    __tablename__ = "task_completions"
    __table_args__ = (
        UniqueConstraint("collection_task_id", name="uk_completion_collection_task"),
        Index("idx_comp_business_time", "business_id", "completed_at_ms"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    completion_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    collection_task_id: Mapped[str] = mapped_column(String(36), nullable=False)
    lease_generation: Mapped[int] = mapped_column(Integer, nullable=False)
    business_id: Mapped[str] = mapped_column(String(64), nullable=False)
    collection_type: Mapped[str] = mapped_column(String(64), nullable=False)
    business_request_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    worker_id: Mapped[str] = mapped_column(String(128), nullable=False)
    completed_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    receipt_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    summary: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    artifacts: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    extra: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    recipient_snapshot: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    artifact_expires_at_ms: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    artifact_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), nullable=False, server_default=func.now()
    )


class DeliveryDispatch(Base):
    __tablename__ = "delivery_dispatches"
    __table_args__ = (
        UniqueConstraint("normal_dedup_key", name="uk_normal_dispatch"),
        UniqueConstraint("rerun_idempotency_key", name="uk_rerun_idemp"),
        Index("idx_dispatch_pending", "status", "delivery_mode", "created_at"),
        Index("idx_dispatch_collection", "collection_task_id", "created_at"),
        Index("idx_dispatch_business", "business_id", "recipient_service", "status", "completed_at_ms"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    dispatch_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    completion_id: Mapped[str] = mapped_column(String(36), nullable=False)
    collection_task_id: Mapped[str] = mapped_column(String(36), nullable=False)
    business_id: Mapped[str] = mapped_column(String(64), nullable=False)
    recipient_service: Mapped[str] = mapped_column(String(64), nullable=False)
    step: Mapped[str] = mapped_column(String(64), nullable=False)
    target_queue: Mapped[str] = mapped_column(String(64), nullable=False)
    delivery_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    dispatch_kind: Mapped[str] = mapped_column(String(16), nullable=False, default="normal")
    normal_dedup_key: Mapped[str | None] = mapped_column(String(160), nullable=True)
    rerun_idempotency_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="PENDING")
    processing_task_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    parent_dispatch_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    kafka_delivery_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    kafka_topic: Mapped[str | None] = mapped_column(String(128), nullable=True)
    kafka_published_at_ms: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(512), nullable=True)
    completed_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), nullable=False, server_default=func.now(), onupdate=func.now()
    )
