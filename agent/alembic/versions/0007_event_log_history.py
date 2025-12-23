"""Persisted SSE event history."""

from alembic import op
import sqlalchemy as sa


revision = "0007_event_log_history"
down_revision = "0006_auth_security_reconcile"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "event_log",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("agent_id", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.Float(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("event", sa.String(length=128), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
    )
    op.create_index(
        "ix_event_log_agent_created_at", "event_log", ["agent_id", "created_at"]
    )
    op.create_index(
        "ix_event_log_type_created_at", "event_log", ["event_type", "created_at"]
    )
    op.create_index(
        "ix_event_log_event_created_at", "event_log", ["event", "created_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_event_log_event_created_at", table_name="event_log")
    op.drop_index("ix_event_log_type_created_at", table_name="event_log")
    op.drop_index("ix_event_log_agent_created_at", table_name="event_log")
    op.drop_table("event_log")
