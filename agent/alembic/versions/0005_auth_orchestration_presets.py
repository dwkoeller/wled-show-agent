"""Add auth tables and orchestration presets."""

from alembic import op
import sqlalchemy as sa


revision = "0005_auth_orchestration_presets"
down_revision = "0004_agent_heartbeat_history_tags"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "auth_users",
        sa.Column("username", sa.String(length=64), primary_key=True, nullable=False),
        sa.Column("password_hash", sa.String(length=256), nullable=False),
        sa.Column("totp_secret", sa.String(length=64), nullable=False),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("disabled", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("created_at", sa.Float(), nullable=False),
        sa.Column("updated_at", sa.Float(), nullable=False),
        sa.Column("last_login_at", sa.Float(), nullable=True),
    )
    op.create_index("ix_auth_users_role", "auth_users", ["role"])
    op.create_index("ix_auth_users_updated_at", "auth_users", ["updated_at"])
    op.create_index("ix_auth_users_created_at", "auth_users", ["created_at"])
    op.create_index("ix_auth_users_last_login_at", "auth_users", ["last_login_at"])

    op.create_table(
        "auth_sessions",
        sa.Column("jti", sa.String(length=64), primary_key=True, nullable=False),
        sa.Column("username", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.Float(), nullable=False),
        sa.Column("expires_at", sa.Float(), nullable=False),
        sa.Column("revoked_at", sa.Float(), nullable=True),
        sa.Column("last_seen_at", sa.Float(), nullable=True),
        sa.Column("ip", sa.String(length=64), nullable=True),
        sa.Column("user_agent", sa.String(length=256), nullable=True),
    )
    op.create_index(
        "ix_auth_sessions_user_created_at", "auth_sessions", ["username", "created_at"]
    )
    op.create_index("ix_auth_sessions_expires_at", "auth_sessions", ["expires_at"])
    op.create_index("ix_auth_sessions_revoked_at", "auth_sessions", ["revoked_at"])
    op.create_index("ix_auth_sessions_last_seen_at", "auth_sessions", ["last_seen_at"])

    op.create_table(
        "auth_login_attempts",
        sa.Column("username", sa.String(length=64), primary_key=True, nullable=False),
        sa.Column("ip", sa.String(length=64), primary_key=True, nullable=False),
        sa.Column("failed_count", sa.Integer(), nullable=False),
        sa.Column("first_failed_at", sa.Float(), nullable=False),
        sa.Column("last_failed_at", sa.Float(), nullable=False),
        sa.Column("locked_until", sa.Float(), nullable=True),
    )
    op.create_index(
        "ix_auth_login_attempts_username", "auth_login_attempts", ["username"]
    )
    op.create_index(
        "ix_auth_login_attempts_locked_until",
        "auth_login_attempts",
        ["locked_until"],
    )
    op.create_index(
        "ix_auth_login_attempts_last_failed_at",
        "auth_login_attempts",
        ["last_failed_at"],
    )

    op.create_table(
        "orchestration_presets",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("scope", sa.String(length=32), nullable=False),
        sa.Column("description", sa.String(length=256), nullable=True),
        sa.Column("created_at", sa.Float(), nullable=False),
        sa.Column("updated_at", sa.Float(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
    )
    op.create_index(
        "ix_orchestration_presets_scope_name",
        "orchestration_presets",
        ["scope", "name"],
        unique=True,
    )
    op.create_index(
        "ix_orchestration_presets_updated_at",
        "orchestration_presets",
        ["updated_at"],
    )
    op.create_index(
        "ix_orchestration_presets_created_at",
        "orchestration_presets",
        ["created_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_orchestration_presets_created_at", table_name="orchestration_presets"
    )
    op.drop_index(
        "ix_orchestration_presets_updated_at", table_name="orchestration_presets"
    )
    op.drop_index(
        "ix_orchestration_presets_scope_name", table_name="orchestration_presets"
    )
    op.drop_table("orchestration_presets")

    op.drop_index(
        "ix_auth_login_attempts_last_failed_at", table_name="auth_login_attempts"
    )
    op.drop_index(
        "ix_auth_login_attempts_locked_until", table_name="auth_login_attempts"
    )
    op.drop_index(
        "ix_auth_login_attempts_username", table_name="auth_login_attempts"
    )
    op.drop_table("auth_login_attempts")

    op.drop_index("ix_auth_sessions_last_seen_at", table_name="auth_sessions")
    op.drop_index("ix_auth_sessions_revoked_at", table_name="auth_sessions")
    op.drop_index("ix_auth_sessions_expires_at", table_name="auth_sessions")
    op.drop_index(
        "ix_auth_sessions_user_created_at", table_name="auth_sessions"
    )
    op.drop_table("auth_sessions")

    op.drop_index("ix_auth_users_last_login_at", table_name="auth_users")
    op.drop_index("ix_auth_users_created_at", table_name="auth_users")
    op.drop_index("ix_auth_users_updated_at", table_name="auth_users")
    op.drop_index("ix_auth_users_role", table_name="auth_users")
    op.drop_table("auth_users")
