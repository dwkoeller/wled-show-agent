"""Initial schema."""

from alembic import op
from sqlmodel import SQLModel

import sql_store  # noqa: F401


revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    exclude = {
        "agent_heartbeat_history",
        "audit_log",
        "orchestration_runs",
        "orchestration_steps",
        "orchestration_peer_results",
        "agent_heartbeat_history_tags",
        "auth_users",
        "auth_sessions",
        "auth_login_attempts",
        "orchestration_presets",
        "auth_api_keys",
        "auth_password_resets",
        "reconcile_runs",
        "event_log",
        "agent_overrides",
        "metrics_samples",
    }
    tables = [t for t in SQLModel.metadata.sorted_tables if t.name not in exclude]
    SQLModel.metadata.create_all(bind=bind, tables=tables)


def downgrade() -> None:
    bind = op.get_bind()
    exclude = {
        "agent_heartbeat_history",
        "audit_log",
        "orchestration_runs",
        "orchestration_steps",
        "orchestration_peer_results",
        "agent_heartbeat_history_tags",
        "auth_users",
        "auth_sessions",
        "auth_login_attempts",
        "orchestration_presets",
        "auth_api_keys",
        "auth_password_resets",
        "reconcile_runs",
        "event_log",
        "agent_overrides",
        "metrics_samples",
    }
    tables = [t for t in SQLModel.metadata.sorted_tables if t.name not in exclude]
    SQLModel.metadata.drop_all(bind=bind, tables=tables)
