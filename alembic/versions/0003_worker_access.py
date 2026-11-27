"""Persist service access, subscriptions, and the owners of leases and Cron rules."""

import sqlalchemy as sa
from sqlalchemy.dialects import mysql

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None

TABLE_NAMES = (
    "service_identities",
    "service_credentials",
    "service_grants",
    "subscription_scopes",
    "subscription_queues",
    "result_subscriptions",
    "source_submissions",
)
OPTIONS = {"mysql_charset": "utf8mb4", "mysql_collate": "utf8mb4_0900_bin"}
RENAMES = (
    ("task_records", "collection_type", "task_type", 64, True),
    ("task_completions", "collection_type", "task_type", 64, False),
    ("task_completions", "collection_task_id", "source_task_id", 36, False),
    ("delivery_dispatches", "collection_task_id", "source_task_id", 36, False),
)

SCOPED_COLUMNS = (
    ("cron_jobs", "queue", False),
    ("cron_jobs", "business_id", True),
    ("task_records", "queue", False),
    ("task_records", "business_id", True),
    ("task_records", "task_type", True),
    ("task_records", "recipient_service", True),
    ("task_records", "step", True),
    ("task_completions", "business_id", False),
    ("task_completions", "task_type", False),
    ("delivery_dispatches", "business_id", False),
    ("delivery_dispatches", "recipient_service", False),
    ("delivery_dispatches", "target_queue", False),
    ("delivery_dispatches", "step", False),
)


def set_scope_collation(*, exact):
    if op.get_context().dialect.name == "mysql":
        name_type = (
            mysql.VARCHAR(64, charset="utf8mb4", collation="utf8mb4_0900_bin")
            if exact
            else sa.String(64)
        )
        for table, name, nullable in SCOPED_COLUMNS:
            op.alter_column(table, name, type_=name_type, existing_nullable=nullable)


def column(name, *, primary_key=False):
    return sa.Column(name, sa.String(64), primary_key=primary_key, nullable=False)


def service_name(name="service_name", *, primary_key=False):
    return sa.Column(
        name,
        sa.String(64),
        sa.ForeignKey("service_identities.name"),
        primary_key=primary_key,
        nullable=False,
    )


def upgrade() -> None:
    op.create_table(
        "service_identities",
        column("name", primary_key=True),
        sa.Column("business_id", sa.String(64)),
        sa.Column("administrator", sa.Boolean(), nullable=False),
        **OPTIONS,
    )
    op.create_table(
        "service_credentials",
        sa.Column("credential_id", sa.String(32), primary_key=True),
        service_name(),
        column("digest"),
        sa.Column("revoked", sa.Boolean(), nullable=False),
        **OPTIONS,
    )
    op.create_index("ix_service_credentials_service_name", "service_credentials", ["service_name"])
    op.create_table(
        "service_grants",
        service_name(primary_key=True),
        sa.Column("operation", sa.String(24), primary_key=True),
        *(column(name, primary_key=True) for name in ("business_id", "queue", "task_type")),
        **OPTIONS,
    )
    op.create_table(
        "subscription_scopes",
        column("business_id", primary_key=True),
        column("task_type", primary_key=True),
        **OPTIONS,
    )
    op.create_table(
        "subscription_queues",
        column("queue", primary_key=True),
        column("business_id"),
        service_name("recipient_service"),
        **OPTIONS,
    )
    op.create_table(
        "result_subscriptions",
        column("business_id", primary_key=True),
        column("task_type", primary_key=True),
        service_name("recipient_service", primary_key=True),
        column("step", primary_key=True),
        sa.Column(
            "queue", sa.String(64), sa.ForeignKey("subscription_queues.queue"), nullable=False
        ),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        **OPTIONS,
    )
    op.create_table(
        "source_submissions",
        column("scope_key", primary_key=True),
        sa.Column("task_id", sa.String(36), nullable=False),
        **OPTIONS,
    )
    for table, old, new, length, nullable in RENAMES:
        op.alter_column(
            table,
            old,
            new_column_name=new,
            existing_type=sa.String(length),
            existing_nullable=nullable,
        )
    op.execute("UPDATE task_records SET task_kind = 'source' WHERE task_kind = 'collection'")
    op.add_column("cron_jobs", sa.Column("owner_service", sa.String(64)))
    op.add_column("cron_jobs", sa.Column("business_id", sa.String(64)))
    op.add_column("task_records", sa.Column("worker_service", sa.String(64)))
    op.add_column("task_records", sa.Column("lease_seconds", sa.Integer()))
    op.add_column("task_records", sa.Column("max_retries", sa.SmallInteger()))
    op.add_column("task_records", sa.Column("requested_recipients", sa.JSON()))
    op.add_column("task_completions", sa.Column("worker_service", sa.String(64)))
    set_scope_collation(exact=True)


def downgrade() -> None:
    set_scope_collation(exact=False)
    op.drop_column("task_completions", "worker_service")
    for name in ("requested_recipients", "max_retries", "lease_seconds", "worker_service"):
        op.drop_column("task_records", name)
    op.drop_column("cron_jobs", "business_id")
    op.drop_column("cron_jobs", "owner_service")
    op.execute("UPDATE task_records SET task_kind = 'collection' WHERE task_kind = 'source'")
    for table, old, new, length, nullable in reversed(RENAMES):
        op.alter_column(
            table,
            new,
            new_column_name=old,
            existing_type=sa.String(length),
            existing_nullable=nullable,
        )
    for table in reversed(TABLE_NAMES):
        op.drop_table(table)
