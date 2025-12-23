"""Add orchestration step + peer result history tables."""

from alembic import op
import sqlalchemy as sa


revision = "0003_orchestration_step_peer_history"
down_revision = "0002_audit_orchestration_history"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "orchestration_steps",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("agent_id", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.Float(), nullable=False),
        sa.Column("updated_at", sa.Float(), nullable=False),
        sa.Column("started_at", sa.Float(), nullable=False),
        sa.Column("finished_at", sa.Float(), nullable=True),
        sa.Column("step_index", sa.Integer(), nullable=False),
        sa.Column("iteration", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("ok", sa.Boolean(), nullable=False),
        sa.Column("duration_s", sa.Float(), nullable=False),
        sa.Column("error", sa.String(length=512), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
    )
    op.create_index(
        "ix_orchestration_steps_run_created_at",
        "orchestration_steps",
        ["run_id", "created_at"],
    )
    op.create_index(
        "ix_orchestration_steps_agent_created_at",
        "orchestration_steps",
        ["agent_id", "created_at"],
    )
    op.create_index(
        "ix_orchestration_steps_status_created_at",
        "orchestration_steps",
        ["status", "created_at"],
    )
    op.create_index(
        op.f("ix_orchestration_steps_agent_id"),
        "orchestration_steps",
        ["agent_id"],
    )
    op.create_index(
        op.f("ix_orchestration_steps_created_at"),
        "orchestration_steps",
        ["created_at"],
    )
    op.create_index(
        op.f("ix_orchestration_steps_started_at"),
        "orchestration_steps",
        ["started_at"],
    )
    op.create_index(
        op.f("ix_orchestration_steps_step_index"),
        "orchestration_steps",
        ["step_index"],
    )
    op.create_index(
        op.f("ix_orchestration_steps_iteration"),
        "orchestration_steps",
        ["iteration"],
    )
    op.create_index(
        op.f("ix_orchestration_steps_run_id"),
        "orchestration_steps",
        ["run_id"],
    )
    op.create_index(
        op.f("ix_orchestration_steps_ok"),
        "orchestration_steps",
        ["ok"],
    )
    op.create_index(
        op.f("ix_orchestration_steps_status"),
        "orchestration_steps",
        ["status"],
    )

    op.create_table(
        "orchestration_peer_results",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("agent_id", sa.String(length=128), nullable=False),
        sa.Column("peer_id", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.Float(), nullable=False),
        sa.Column("updated_at", sa.Float(), nullable=False),
        sa.Column("started_at", sa.Float(), nullable=False),
        sa.Column("finished_at", sa.Float(), nullable=True),
        sa.Column("step_index", sa.Integer(), nullable=False),
        sa.Column("iteration", sa.Integer(), nullable=False),
        sa.Column("action", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("ok", sa.Boolean(), nullable=False),
        sa.Column("duration_s", sa.Float(), nullable=False),
        sa.Column("error", sa.String(length=512), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
    )
    op.create_index(
        "ix_orchestration_peer_run_created_at",
        "orchestration_peer_results",
        ["run_id", "created_at"],
    )
    op.create_index(
        "ix_orchestration_peer_peer_created_at",
        "orchestration_peer_results",
        ["peer_id", "created_at"],
    )
    op.create_index(
        "ix_orchestration_peer_status_created_at",
        "orchestration_peer_results",
        ["status", "created_at"],
    )
    op.create_index(
        op.f("ix_orchestration_peer_results_agent_id"),
        "orchestration_peer_results",
        ["agent_id"],
    )
    op.create_index(
        op.f("ix_orchestration_peer_results_created_at"),
        "orchestration_peer_results",
        ["created_at"],
    )
    op.create_index(
        op.f("ix_orchestration_peer_results_started_at"),
        "orchestration_peer_results",
        ["started_at"],
    )
    op.create_index(
        op.f("ix_orchestration_peer_results_step_index"),
        "orchestration_peer_results",
        ["step_index"],
    )
    op.create_index(
        op.f("ix_orchestration_peer_results_iteration"),
        "orchestration_peer_results",
        ["iteration"],
    )
    op.create_index(
        op.f("ix_orchestration_peer_results_run_id"),
        "orchestration_peer_results",
        ["run_id"],
    )
    op.create_index(
        op.f("ix_orchestration_peer_results_ok"),
        "orchestration_peer_results",
        ["ok"],
    )
    op.create_index(
        op.f("ix_orchestration_peer_results_peer_id"),
        "orchestration_peer_results",
        ["peer_id"],
    )
    op.create_index(
        op.f("ix_orchestration_peer_results_status"),
        "orchestration_peer_results",
        ["status"],
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_orchestration_peer_results_status"),
        table_name="orchestration_peer_results",
    )
    op.drop_index(
        op.f("ix_orchestration_peer_results_peer_id"),
        table_name="orchestration_peer_results",
    )
    op.drop_index(
        op.f("ix_orchestration_peer_results_ok"),
        table_name="orchestration_peer_results",
    )
    op.drop_index(
        op.f("ix_orchestration_peer_results_run_id"),
        table_name="orchestration_peer_results",
    )
    op.drop_index(
        op.f("ix_orchestration_peer_results_iteration"),
        table_name="orchestration_peer_results",
    )
    op.drop_index(
        op.f("ix_orchestration_peer_results_step_index"),
        table_name="orchestration_peer_results",
    )
    op.drop_index(
        op.f("ix_orchestration_peer_results_started_at"),
        table_name="orchestration_peer_results",
    )
    op.drop_index(
        op.f("ix_orchestration_peer_results_created_at"),
        table_name="orchestration_peer_results",
    )
    op.drop_index(
        op.f("ix_orchestration_peer_results_agent_id"),
        table_name="orchestration_peer_results",
    )
    op.drop_index(
        "ix_orchestration_peer_status_created_at",
        table_name="orchestration_peer_results",
    )
    op.drop_index(
        "ix_orchestration_peer_peer_created_at",
        table_name="orchestration_peer_results",
    )
    op.drop_index(
        "ix_orchestration_peer_run_created_at",
        table_name="orchestration_peer_results",
    )
    op.drop_table("orchestration_peer_results")

    op.drop_index(
        op.f("ix_orchestration_steps_status"),
        table_name="orchestration_steps",
    )
    op.drop_index(
        op.f("ix_orchestration_steps_ok"),
        table_name="orchestration_steps",
    )
    op.drop_index(
        op.f("ix_orchestration_steps_run_id"),
        table_name="orchestration_steps",
    )
    op.drop_index(
        op.f("ix_orchestration_steps_iteration"),
        table_name="orchestration_steps",
    )
    op.drop_index(
        op.f("ix_orchestration_steps_step_index"),
        table_name="orchestration_steps",
    )
    op.drop_index(
        op.f("ix_orchestration_steps_started_at"),
        table_name="orchestration_steps",
    )
    op.drop_index(
        op.f("ix_orchestration_steps_created_at"),
        table_name="orchestration_steps",
    )
    op.drop_index(
        op.f("ix_orchestration_steps_agent_id"),
        table_name="orchestration_steps",
    )
    op.drop_index(
        "ix_orchestration_steps_status_created_at",
        table_name="orchestration_steps",
    )
    op.drop_index(
        "ix_orchestration_steps_agent_created_at",
        table_name="orchestration_steps",
    )
    op.drop_index(
        "ix_orchestration_steps_run_created_at",
        table_name="orchestration_steps",
    )
    op.drop_table("orchestration_steps")
