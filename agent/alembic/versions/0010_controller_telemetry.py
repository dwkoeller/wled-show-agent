"""Controller telemetry history."""

from alembic import op
import sqlalchemy as sa

revision = "0010_controller_telemetry"
down_revision = "0009_metrics_samples"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "controller_telemetry",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("agent_id", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.Float(), nullable=False),
        sa.Column("ok", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("latency_ms", sa.Float(), nullable=True),
        sa.Column("uptime_s", sa.Float(), nullable=True),
        sa.Column("free_heap", sa.Integer(), nullable=True),
        sa.Column("rssi_dbm", sa.Integer(), nullable=True),
        sa.Column("temperature_c", sa.Float(), nullable=True),
        sa.Column("led_count", sa.Integer(), nullable=True),
        sa.Column("power_w", sa.Float(), nullable=True),
        sa.Column("fps", sa.Float(), nullable=True),
        sa.Column("error", sa.String(length=512), nullable=True),
        sa.Column("alerts", sa.JSON(), nullable=False),
    )
    op.create_index("ix_controller_telemetry_agent_created_at", "controller_telemetry", ["agent_id", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_controller_telemetry_agent_created_at", table_name="controller_telemetry")
    op.drop_table("controller_telemetry")
