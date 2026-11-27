"""Add result delivery columns, task_completions, and delivery_dispatches tables."""

import sqlalchemy as sa

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "task_records",
        sa.Column("task_kind", sa.String(24), nullable=False, server_default="normal"),
    )
    op.add_column("task_records", sa.Column("business_id", sa.String(64), nullable=True))
    op.add_column("task_records", sa.Column("business_request_id", sa.String(128), nullable=True))
    op.add_column("task_records", sa.Column("collection_type", sa.String(64), nullable=True))
    op.add_column(
        "task_records",
        sa.Column("lease_generation", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column("task_records", sa.Column("source_task_id", sa.String(36), nullable=True))
    op.add_column("task_records", sa.Column("completion_id", sa.String(36), nullable=True))
    op.add_column("task_records", sa.Column("recipient_service", sa.String(64), nullable=True))
    op.add_column("task_records", sa.Column("step", sa.String(64), nullable=True))
    op.add_column("task_records", sa.Column("parent_task_id", sa.String(36), nullable=True))
    op.add_column("task_records", sa.Column("rerun_of_task_id", sa.String(36), nullable=True))
    op.add_column("task_records", sa.Column("processing_result", sa.JSON(), nullable=True))

    op.create_index("idx_business_created", "task_records", ["business_id", "created_at"])
    op.create_index("idx_source_task", "task_records", ["source_task_id"])
    op.create_index("idx_completion", "task_records", ["completion_id"])

    op.create_table(
        "task_completions",
        sa.Column("completion_id", sa.String(36), primary_key=True),
        sa.Column("collection_task_id", sa.String(36), nullable=False),
        sa.Column("lease_generation", sa.Integer(), nullable=False),
        sa.Column("business_id", sa.String(64), nullable=False),
        sa.Column("collection_type", sa.String(64), nullable=False),
        sa.Column("business_request_id", sa.String(128), nullable=True),
        sa.Column("worker_id", sa.String(128), nullable=False),
        sa.Column("completed_at_ms", sa.BigInteger(), nullable=False),
        sa.Column("receipt_hash", sa.String(64), nullable=False),
        sa.Column("summary", sa.JSON(), nullable=False),
        sa.Column("artifacts", sa.JSON(), nullable=False),
        sa.Column("extra", sa.JSON(), nullable=False),
        sa.Column("recipient_snapshot", sa.JSON(), nullable=False),
        sa.Column("artifact_expires_at_ms", sa.BigInteger(), nullable=True),
        sa.Column("artifact_deleted", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("collection_task_id", name="uk_completion_collection_task"),
        mysql_charset="utf8mb4",
        mysql_engine="InnoDB",
    )
    op.create_index("idx_comp_business_time", "task_completions", ["business_id", "completed_at_ms"])

    op.create_table(
        "delivery_dispatches",
        sa.Column("dispatch_id", sa.String(36), primary_key=True),
        sa.Column("completion_id", sa.String(36), nullable=False),
        sa.Column("collection_task_id", sa.String(36), nullable=False),
        sa.Column("business_id", sa.String(64), nullable=False),
        sa.Column("recipient_service", sa.String(64), nullable=False),
        sa.Column("step", sa.String(64), nullable=False),
        sa.Column("target_queue", sa.String(64), nullable=False),
        sa.Column("delivery_mode", sa.String(16), nullable=False),
        sa.Column("dispatch_kind", sa.String(16), nullable=False, server_default="normal"),
        sa.Column("normal_dedup_key", sa.String(160), nullable=True),
        sa.Column("rerun_idempotency_key", sa.String(128), nullable=True),
        sa.Column("status", sa.String(24), nullable=False, server_default="PENDING"),
        sa.Column("processing_task_id", sa.String(36), nullable=True),
        sa.Column("parent_dispatch_id", sa.String(36), nullable=True),
        sa.Column("kafka_delivery_id", sa.String(64), nullable=True),
        sa.Column("kafka_topic", sa.String(128), nullable=True),
        sa.Column("kafka_published_at_ms", sa.BigInteger(), nullable=True),
        sa.Column("last_error", sa.String(512), nullable=True),
        sa.Column("completed_at_ms", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("normal_dedup_key", name="uk_normal_dispatch"),
        sa.UniqueConstraint("rerun_idempotency_key", name="uk_rerun_idemp"),
        mysql_charset="utf8mb4",
        mysql_engine="InnoDB",
    )
    op.create_index("idx_dispatch_pending", "delivery_dispatches", ["status", "delivery_mode", "created_at"])
    op.create_index("idx_dispatch_collection", "delivery_dispatches", ["collection_task_id", "created_at"])
    op.create_index(
        "idx_dispatch_business",
        "delivery_dispatches",
        ["business_id", "recipient_service", "status", "completed_at_ms"],
    )


def downgrade() -> None:
    op.drop_table("delivery_dispatches")
    op.drop_table("task_completions")
    op.drop_index("idx_completion", table_name="task_records")
    op.drop_index("idx_source_task", table_name="task_records")
    op.drop_index("idx_business_created", table_name="task_records")
    for col in (
        "processing_result",
        "rerun_of_task_id",
        "parent_task_id",
        "step",
        "recipient_service",
        "completion_id",
        "source_task_id",
        "lease_generation",
        "collection_type",
        "business_request_id",
        "business_id",
        "task_kind",
    ):
        op.drop_column("task_records", col)
