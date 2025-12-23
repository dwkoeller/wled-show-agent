"""Add audit logs, orchestration runs, and fleet history."""

from alembic import op
import sqlalchemy as sa


revision = "0002_audit_orchestration_history"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_heartbeat_history",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("agent_id", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.Float(), nullable=False),
        sa.Column("updated_at", sa.Float(), nullable=False),
        sa.Column("name", sa.String(length=256), nullable=False),
        sa.Column("role", sa.String(length=128), nullable=False),
        sa.Column("controller_kind", sa.String(length=32), nullable=False),
        sa.Column("version", sa.String(length=64), nullable=False),
        sa.Column("base_url", sa.String(length=512), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
    )
    op.create_index(
        "ix_agent_heartbeat_history_agent_created_at",
        "agent_heartbeat_history",
        ["agent_id", "created_at"],
    )
    op.create_index(
        op.f("ix_agent_heartbeat_history_agent_id"),
        "agent_heartbeat_history",
        ["agent_id"],
    )
    op.create_index(
        op.f("ix_agent_heartbeat_history_created_at"),
        "agent_heartbeat_history",
        ["created_at"],
    )
    op.create_index(
        op.f("ix_agent_heartbeat_history_updated_at"),
        "agent_heartbeat_history",
        ["updated_at"],
    )

    op.create_table(
        "audit_log",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("agent_id", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.Float(), nullable=False),
        sa.Column("actor", sa.String(length=128), nullable=False),
        sa.Column("action", sa.String(length=128), nullable=False),
        sa.Column("resource", sa.String(length=256), nullable=True),
        sa.Column("ok", sa.Boolean(), nullable=False),
        sa.Column("error", sa.String(length=512), nullable=True),
        sa.Column("ip", sa.String(length=64), nullable=True),
        sa.Column("user_agent", sa.String(length=256), nullable=True),
        sa.Column("request_id", sa.String(length=64), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
    )
    op.create_index(
        "ix_audit_log_agent_created_at",
        "audit_log",
        ["agent_id", "created_at"],
    )
    op.create_index(
        "ix_audit_log_action_created_at",
        "audit_log",
        ["action", "created_at"],
    )
    op.create_index(op.f("ix_audit_log_action"), "audit_log", ["action"])
    op.create_index(op.f("ix_audit_log_actor"), "audit_log", ["actor"])
    op.create_index(op.f("ix_audit_log_agent_id"), "audit_log", ["agent_id"])
    op.create_index(op.f("ix_audit_log_created_at"), "audit_log", ["created_at"])
    op.create_index(op.f("ix_audit_log_ok"), "audit_log", ["ok"])

    op.create_table(
        "orchestration_runs",
        sa.Column("run_id", sa.String(length=64), primary_key=True, nullable=False),
        sa.Column("agent_id", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.Float(), nullable=False),
        sa.Column("updated_at", sa.Float(), nullable=False),
        sa.Column("started_at", sa.Float(), nullable=False),
        sa.Column("finished_at", sa.Float(), nullable=True),
        sa.Column("name", sa.String(length=256), nullable=True),
        sa.Column("scope", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("steps_total", sa.Integer(), nullable=False),
        sa.Column("loop", sa.Boolean(), nullable=False),
        sa.Column("include_self", sa.Boolean(), nullable=False),
        sa.Column("duration_s", sa.Float(), nullable=False),
        sa.Column("error", sa.String(length=512), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
    )
    op.create_index(
        "ix_orchestration_runs_agent_started_at",
        "orchestration_runs",
        ["agent_id", "started_at"],
    )
    op.create_index(
        "ix_orchestration_runs_scope_started_at",
        "orchestration_runs",
        ["scope", "started_at"],
    )
    op.create_index(
        "ix_orchestration_runs_status_started_at",
        "orchestration_runs",
        ["status", "started_at"],
    )
    op.create_index(
        op.f("ix_orchestration_runs_agent_id"),
        "orchestration_runs",
        ["agent_id"],
    )
    op.create_index(
        op.f("ix_orchestration_runs_created_at"),
        "orchestration_runs",
        ["created_at"],
    )
    op.create_index(
        op.f("ix_orchestration_runs_started_at"),
        "orchestration_runs",
        ["started_at"],
    )
    op.create_index(
        op.f("ix_orchestration_runs_status"),
        "orchestration_runs",
        ["status"],
    )


def downgrade() -> None:
    op.drop_index("ix_orchestration_runs_status", table_name="orchestration_runs")
    op.drop_index("ix_orchestration_runs_started_at", table_name="orchestration_runs")
    op.drop_index("ix_orchestration_runs_created_at", table_name="orchestration_runs")
    op.drop_index("ix_orchestration_runs_agent_id", table_name="orchestration_runs")
    op.drop_index("ix_orchestration_runs_status_started_at", table_name="orchestration_runs")
    op.drop_index("ix_orchestration_runs_scope_started_at", table_name="orchestration_runs")
    op.drop_index("ix_orchestration_runs_agent_started_at", table_name="orchestration_runs")
    op.drop_table("orchestration_runs")

    op.drop_index("ix_audit_log_ok", table_name="audit_log")
    op.drop_index("ix_audit_log_created_at", table_name="audit_log")
    op.drop_index("ix_audit_log_agent_id", table_name="audit_log")
    op.drop_index("ix_audit_log_actor", table_name="audit_log")
    op.drop_index("ix_audit_log_action", table_name="audit_log")
    op.drop_index("ix_audit_log_action_created_at", table_name="audit_log")
    op.drop_index("ix_audit_log_agent_created_at", table_name="audit_log")
    op.drop_table("audit_log")

    op.drop_index("ix_agent_heartbeat_history_updated_at", table_name="agent_heartbeat_history")
    op.drop_index("ix_agent_heartbeat_history_created_at", table_name="agent_heartbeat_history")
    op.drop_index("ix_agent_heartbeat_history_agent_id", table_name="agent_heartbeat_history")
    op.drop_index(
        "ix_agent_heartbeat_history_agent_created_at",
        table_name="agent_heartbeat_history",
    )
    op.drop_table("agent_heartbeat_history")
