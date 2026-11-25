"""Create the initial scheduler tables."""

import sqlalchemy as sa

from alembic import op
from app.core.config import get_settings
from app.core.db import (
    ensure_mysql_task_records_partitions,
    partition_task_records_sql,
    task_record_partition_definitions,
)

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    migration = op.get_context()
    connection = op.get_bind()
    existing = set() if migration.as_sql else set(sa.inspect(connection).get_table_names())
    if "cron_jobs" not in existing:
        op.create_table(
            "cron_jobs",
            sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), primary_key=True),
            sa.Column("name", sa.String(128), nullable=False),
            sa.Column("queue", sa.String(64), nullable=False),
            sa.Column("priority", sa.SmallInteger(), nullable=False),
            sa.Column("cron_expr", sa.String(64), nullable=False),
            sa.Column("payload", sa.JSON(), nullable=False),
            sa.Column("max_retries", sa.SmallInteger(), nullable=False),
            sa.Column("lease_seconds", sa.Integer(), nullable=False),
            sa.Column("enabled", sa.Boolean(), nullable=False),
            sa.Column("next_run_at", sa.BigInteger(), nullable=False),
            sa.Column("version", sa.Integer(), nullable=False),
            sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("name", name="uk_name"),
        )
        op.create_index("idx_scan", "cron_jobs", ["enabled", "next_run_at"])
    if "task_records" not in existing:
        op.create_table(
            "task_records",
            sa.Column("task_id", sa.String(36), nullable=False),
            sa.Column("queue", sa.String(64), nullable=False),
            sa.Column("priority", sa.SmallInteger(), nullable=False),
            sa.Column("status", sa.String(16), nullable=False),
            sa.Column("idempotency_key", sa.String(128)),
            sa.Column("payload", sa.JSON(), nullable=False),
            sa.Column("receipt", sa.JSON()),
            sa.Column("retry_count", sa.SmallInteger(), nullable=False),
            sa.Column("worker_id", sa.String(128)),
            sa.Column("scheduled_at", sa.BigInteger(), nullable=False),
            sa.Column("pulled_at", sa.BigInteger()),
            sa.Column("finished_at", sa.BigInteger()),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
            sa.PrimaryKeyConstraint("task_id", "created_at"),
            mysql_charset="utf8mb4",
            mysql_engine="InnoDB",
        )
        op.create_index("idx_queue_status", "task_records", ["queue", "status", "created_at"])
        op.create_index("idx_worker", "task_records", ["worker_id", "pulled_at"])
    if migration.dialect.name == "mysql":
        days_ahead = get_settings().partition_precreate_days
        if migration.as_sql:
            op.execute(partition_task_records_sql(task_record_partition_definitions(days_ahead)))
        else:
            ensure_mysql_task_records_partitions(connection, days_ahead)


def downgrade() -> None:
    op.drop_table("task_records")
    op.drop_table("cron_jobs")
