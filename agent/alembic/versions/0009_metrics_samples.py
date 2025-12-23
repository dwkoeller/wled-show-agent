"""Metrics samples table."""

from alembic import op
import sqlalchemy as sa

revision = "0009_metrics_samples"
down_revision = "0008_agent_overrides_event_cursor"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "metrics_samples",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("agent_id", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.Float(), nullable=False),
        sa.Column("jobs_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("scheduler_ok", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("scheduler_running", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("scheduler_in_window", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("outbound_failures", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("outbound_retries", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("spool_dropped", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("spool_queued_events", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("spool_queued_bytes", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_index(
        "ix_metrics_samples_agent_created_at",
        "metrics_samples",
        ["agent_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_metrics_samples_agent_created_at", table_name="metrics_samples")
    op.drop_table("metrics_samples")
