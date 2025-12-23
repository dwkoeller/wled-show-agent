"""Auth security hardening, API keys, reconcile runs, and preset metadata."""

from alembic import op
import sqlalchemy as sa


revision = "0006_auth_security_reconcile"
down_revision = "0005_auth_orchestration_presets"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("auth_users", sa.Column("ip_allowlist", sa.JSON(), nullable=True))

    op.create_table(
        "auth_api_keys",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("username", sa.String(length=64), nullable=False),
        sa.Column("label", sa.String(length=128), nullable=True),
        sa.Column("prefix", sa.String(length=16), nullable=True),
        sa.Column("key_hash", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.Float(), nullable=False),
        sa.Column("last_used_at", sa.Float(), nullable=True),
        sa.Column("revoked_at", sa.Float(), nullable=True),
        sa.Column("expires_at", sa.Float(), nullable=True),
    )
    op.create_index("ix_auth_api_keys_username", "auth_api_keys", ["username"])
    op.create_index("ix_auth_api_keys_created_at", "auth_api_keys", ["created_at"])
    op.create_index("ix_auth_api_keys_last_used_at", "auth_api_keys", ["last_used_at"])
    op.create_index("ix_auth_api_keys_revoked_at", "auth_api_keys", ["revoked_at"])
    op.create_index("ix_auth_api_keys_expires_at", "auth_api_keys", ["expires_at"])
    op.create_index(
        "ix_auth_api_keys_key_hash", "auth_api_keys", ["key_hash"], unique=True
    )

    op.create_table(
        "auth_password_resets",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("username", sa.String(length=64), nullable=False),
        sa.Column("token_hash", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.Float(), nullable=False),
        sa.Column("expires_at", sa.Float(), nullable=False),
        sa.Column("used_at", sa.Float(), nullable=True),
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("used_ip", sa.String(length=64), nullable=True),
    )
    op.create_index(
        "ix_auth_password_resets_username", "auth_password_resets", ["username"]
    )
    op.create_index(
        "ix_auth_password_resets_created_at", "auth_password_resets", ["created_at"]
    )
    op.create_index(
        "ix_auth_password_resets_expires_at", "auth_password_resets", ["expires_at"]
    )
    op.create_index(
        "ix_auth_password_resets_used_at", "auth_password_resets", ["used_at"]
    )
    op.create_index(
        "ix_auth_password_resets_token_hash",
        "auth_password_resets",
        ["token_hash"],
        unique=True,
    )

    op.add_column("orchestration_presets", sa.Column("tags", sa.JSON(), nullable=True))
    op.add_column(
        "orchestration_presets",
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
    )

    op.create_table(
        "reconcile_runs",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("agent_id", sa.String(length=128), nullable=False),
        sa.Column("started_at", sa.Float(), nullable=False),
        sa.Column("finished_at", sa.Float(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("error", sa.String(length=512), nullable=True),
        sa.Column("cancel_requested", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("options", sa.JSON(), nullable=False),
        sa.Column("result", sa.JSON(), nullable=False),
    )
    op.create_index(
        "ix_reconcile_runs_agent_started_at",
        "reconcile_runs",
        ["agent_id", "started_at"],
    )
    op.create_index(
        "ix_reconcile_runs_status_started_at",
        "reconcile_runs",
        ["status", "started_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_reconcile_runs_status_started_at", table_name="reconcile_runs"
    )
    op.drop_index(
        "ix_reconcile_runs_agent_started_at", table_name="reconcile_runs"
    )
    op.drop_table("reconcile_runs")

    op.drop_column("orchestration_presets", "version")
    op.drop_column("orchestration_presets", "tags")

    op.drop_index(
        "ix_auth_password_resets_token_hash", table_name="auth_password_resets"
    )
    op.drop_index(
        "ix_auth_password_resets_used_at", table_name="auth_password_resets"
    )
    op.drop_index(
        "ix_auth_password_resets_expires_at", table_name="auth_password_resets"
    )
    op.drop_index(
        "ix_auth_password_resets_created_at", table_name="auth_password_resets"
    )
    op.drop_index(
        "ix_auth_password_resets_username", table_name="auth_password_resets"
    )
    op.drop_table("auth_password_resets")

    op.drop_index("ix_auth_api_keys_key_hash", table_name="auth_api_keys")
    op.drop_index("ix_auth_api_keys_expires_at", table_name="auth_api_keys")
    op.drop_index("ix_auth_api_keys_revoked_at", table_name="auth_api_keys")
    op.drop_index("ix_auth_api_keys_last_used_at", table_name="auth_api_keys")
    op.drop_index("ix_auth_api_keys_created_at", table_name="auth_api_keys")
    op.drop_index("ix_auth_api_keys_username", table_name="auth_api_keys")
    op.drop_table("auth_api_keys")

    op.drop_column("auth_users", "ip_allowlist")
