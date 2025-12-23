"""Add agent heartbeat history tag table."""

from alembic import op
import sqlalchemy as sa


revision = "0004_agent_heartbeat_history_tags"
down_revision = "0003_orchestration_step_peer_history"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_heartbeat_history_tags",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("history_id", sa.Integer(), nullable=False),
        sa.Column("agent_id", sa.String(length=128), nullable=False),
        sa.Column("tag", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.Float(), nullable=False),
    )
    op.create_index(
        "ix_agent_hb_tags_history_id",
        "agent_heartbeat_history_tags",
        ["history_id"],
    )
    op.create_index(
        "ix_agent_hb_tags_tag_created_at",
        "agent_heartbeat_history_tags",
        ["tag", "created_at"],
    )
    op.create_index(
        "ix_agent_hb_tags_agent_created_at",
        "agent_heartbeat_history_tags",
        ["agent_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_agent_hb_tags_agent_created_at",
        table_name="agent_heartbeat_history_tags",
    )
    op.drop_index(
        "ix_agent_hb_tags_tag_created_at",
        table_name="agent_heartbeat_history_tags",
    )
    op.drop_index(
        "ix_agent_hb_tags_history_id",
        table_name="agent_heartbeat_history_tags",
    )
    op.drop_table("agent_heartbeat_history_tags")
