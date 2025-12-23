"""Agent overrides and event cursor index."""

from alembic import op
import sqlalchemy as sa


revision = "0008_agent_overrides_event_cursor"
down_revision = "0007_event_log_history"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_overrides",
        sa.Column("agent_id", sa.String(length=128), primary_key=True, nullable=False),
        sa.Column("updated_at", sa.Float(), nullable=False),
        sa.Column("updated_by", sa.String(length=128), nullable=True),
        sa.Column("role", sa.String(length=128), nullable=True),
        sa.Column("tags", sa.JSON(), nullable=False),
    )
    op.create_index(
        "ix_agent_overrides_updated_at", "agent_overrides", ["updated_at"]
    )
    op.create_index("ix_event_log_created_id", "event_log", ["created_at", "id"])


def downgrade() -> None:
    op.drop_index("ix_event_log_created_id", table_name="event_log")
    op.drop_index("ix_agent_overrides_updated_at", table_name="agent_overrides")
    op.drop_table("agent_overrides")
