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
