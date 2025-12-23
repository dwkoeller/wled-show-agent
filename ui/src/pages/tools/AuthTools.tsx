import SecurityIcon from "@mui/icons-material/Security";
import RefreshIcon from "@mui/icons-material/Refresh";
import DeleteIcon from "@mui/icons-material/Delete";
import KeyIcon from "@mui/icons-material/Key";
import LockResetIcon from "@mui/icons-material/LockReset";
import {
  Alert,
  Button,
  Card,
  CardActions,
  CardContent,
  Divider,
  FormControl,
  FormControlLabel,
  InputLabel,
  MenuItem,
  Select,
  Stack,
  Switch,
  TextField,
  Typography,
} from "@mui/material";
import React, { useEffect, useMemo, useState } from "react";
import { api } from "../../api";
import { useAuth } from "../../auth";
import { useEventRefresh } from "../../hooks/useEventRefresh";

type AuthUserRow = {
  username: string;
  role: string;
  disabled: boolean;
  ip_allowlist?: string[];
  created_at?: number;
  updated_at?: number;
  last_login_at?: number | null;
};

type AuthUsersRes = { ok: boolean; users: AuthUserRow[] };

type AuthSessionRow = {
  jti: string;
  username: string;
  created_at?: number;
  expires_at?: number;
  revoked_at?: number | null;
  last_seen_at?: number | null;
  ip?: string | null;
  user_agent?: string | null;
};

type AuthSessionsRes = {
  ok: boolean;
  sessions: AuthSessionRow[];
  count?: number;
  limit?: number;
  offset?: number;
  next_offset?: number | null;
};

type AuthLoginAttemptRow = {
  username: string;
  ip: string;
  failed_count: number;
  first_failed_at?: number;
  last_failed_at?: number;
  locked_until?: number | null;
};

type AuthLoginAttemptsRes = {
  ok: boolean;
  attempts: AuthLoginAttemptRow[];
  count?: number;
  limit?: number;
  offset?: number;
  next_offset?: number | null;
};

type AuthApiKeyRow = {
  id: number;
  username: string;
  label?: string | null;
  prefix?: string | null;
  created_at?: number;
  last_used_at?: number | null;
  revoked_at?: number | null;
  expires_at?: number | null;
};

type AuthApiKeysRes = {
  ok: boolean;
  api_keys: AuthApiKeyRow[];
  count?: number;
  limit?: number;
  offset?: number;
  next_offset?: number | null;
};

type AuthApiKeyCreateRes = {
  ok: boolean;
  api_key: string;
  record: AuthApiKeyRow;
};

type AuthUserCreateRes = {
  ok: boolean;
  user: AuthUserRow;
  totp_secret?: string;
  provisioning_uri?: string;
};

type AuthUserUpdateRes = {
  ok: boolean;
  user: AuthUserRow;
  totp_secret?: string;
  provisioning_uri?: string;
};

function fmtTs(ts?: number | null): string {
  if (!ts) return "—";
  try {
    return new Date(ts * 1000).toLocaleString();
  } catch {
    return String(ts);
  }
}

function parseList(raw: string): string[] | undefined {
  const out = raw
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
  return out.length ? out : undefined;
}

function toLimit(raw: string, fallback: number): number {
  const parsed = parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return parsed;
}

export function AuthTools() {
  const { user, config } = useAuth();
  const isAdmin = (user?.role || "") === "admin";
  const roleOptions = config?.roles?.length
    ? config.roles
    : ["admin", "user", "viewer"];

  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [users, setUsers] = useState<AuthUserRow[]>([]);
  const [sessions, setSessions] = useState<AuthSessionRow[]>([]);
  const [loginAttempts, setLoginAttempts] = useState<AuthLoginAttemptRow[]>([]);
  const [apiKeys, setApiKeys] = useState<AuthApiKeyRow[]>([]);

  const [activeOnly, setActiveOnly] = useState(true);
  const [sessionUserFilter, setSessionUserFilter] = useState("");
  const [sessionLimit, setSessionLimit] = useState("50");
  const [sessionOffset, setSessionOffset] = useState(0);
  const [sessionNextOffset, setSessionNextOffset] = useState<number | null>(null);

  const [attemptUserFilter, setAttemptUserFilter] = useState("");
  const [attemptIpFilter, setAttemptIpFilter] = useState("");
  const [attemptLockedOnly, setAttemptLockedOnly] = useState(true);
  const [attemptLimit, setAttemptLimit] = useState("50");
  const [attemptOffset, setAttemptOffset] = useState(0);
  const [attemptNextOffset, setAttemptNextOffset] = useState<number | null>(null);

  const [apiUserFilter, setApiUserFilter] = useState("");
  const [apiActiveOnly, setApiActiveOnly] = useState(true);
  const [apiLimit, setApiLimit] = useState("50");
  const [apiOffset, setApiOffset] = useState(0);
  const [apiNextOffset, setApiNextOffset] = useState<number | null>(null);

  const [newUsername, setNewUsername] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [newRole, setNewRole] = useState("user");
  const [newTotp, setNewTotp] = useState("");
  const [newIpAllowlist, setNewIpAllowlist] = useState("");
  const [createResult, setCreateResult] = useState<AuthUserCreateRes | null>(null);

  const [selectedUser, setSelectedUser] = useState("");
  const [updateRole, setUpdateRole] = useState("");
  const [updateDisabled, setUpdateDisabled] = useState(false);
  const [updatePassword, setUpdatePassword] = useState("");
  const [updateIpAllowlist, setUpdateIpAllowlist] = useState("");
  const [regenerateTotp, setRegenerateTotp] = useState(false);
  const [updateResult, setUpdateResult] = useState<AuthUserUpdateRes | null>(null);

  const [pwCurrent, setPwCurrent] = useState("");
  const [pwNew, setPwNew] = useState("");
  const [pwConfirm, setPwConfirm] = useState("");
  const [pwTotp, setPwTotp] = useState("");
  const [pwRevokeSessions, setPwRevokeSessions] = useState(true);
  const [pwRevokeKeys, setPwRevokeKeys] = useState(false);
  const [pwResult, setPwResult] = useState<string | null>(null);

  const [apiUsername, setApiUsername] = useState("");
  const [apiLabel, setApiLabel] = useState("");
  const [apiExpiresIn, setApiExpiresIn] = useState("3600");
  const [apiCreateResult, setApiCreateResult] = useState<AuthApiKeyCreateRes | null>(
    null,
  );
  const [resetUsername, setResetUsername] = useState("");
  const [resetTtl, setResetTtl] = useState("3600");
  const [resetToken, setResetToken] = useState<string | null>(null);

  const refreshAdmin = async () => {
    if (!isAdmin) return;
    setBusy(true);
    setError(null);
    try {
      const sessionsLimit = toLimit(sessionLimit, 50);
      const attemptsLimit = toLimit(attemptLimit, 50);
      const keysLimit = toLimit(apiLimit, 50);
      const [u, s, a, k] = await Promise.all([
        api<AuthUsersRes>("/v1/auth/users", { method: "GET" }),
        api<AuthSessionsRes>(
          `/v1/auth/sessions?active_only=${activeOnly ? "true" : "false"}&username=${encodeURIComponent(
            sessionUserFilter.trim(),
          )}&limit=${encodeURIComponent(String(sessionsLimit))}&offset=${sessionOffset}`,
          { method: "GET" },
        ),
        api<AuthLoginAttemptsRes>(
          `/v1/auth/login_attempts?locked_only=${attemptLockedOnly ? "true" : "false"}&username=${encodeURIComponent(
            attemptUserFilter.trim(),
          )}&ip=${encodeURIComponent(attemptIpFilter.trim())}&limit=${encodeURIComponent(
            String(attemptsLimit),
          )}&offset=${attemptOffset}`,
          { method: "GET" },
        ),
        api<AuthApiKeysRes>(
          `/v1/auth/api_keys?active_only=${apiActiveOnly ? "true" : "false"}&username=${encodeURIComponent(
            apiUserFilter.trim(),
          )}&limit=${encodeURIComponent(String(keysLimit))}&offset=${apiOffset}`,
          { method: "GET" },
        ),
      ]);
      setUsers(u.users || []);
      setSessions(s.sessions || []);
      setSessionNextOffset(s.next_offset ?? null);
      setLoginAttempts(a.attempts || []);
      setAttemptNextOffset(a.next_offset ?? null);
      setApiKeys(k.api_keys || []);
      setApiNextOffset(k.next_offset ?? null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  useEffect(() => {
    void refreshAdmin();
  }, [
    activeOnly,
    sessionUserFilter,
    sessionLimit,
    sessionOffset,
    attemptUserFilter,
    attemptIpFilter,
    attemptLockedOnly,
    attemptLimit,
    attemptOffset,
    apiUserFilter,
    apiActiveOnly,
    apiLimit,
    apiOffset,
    isAdmin,
  ]);

  useEventRefresh({
    types: ["auth", "tick"],
    refresh: refreshAdmin,
    minIntervalMs: 3000,
  });

  const createUser = async () => {
    setBusy(true);
    setError(null);
    setCreateResult(null);
    try {
      const res = await api<AuthUserCreateRes>("/v1/auth/users", {
        method: "POST",
        json: {
          username: newUsername.trim(),
          password: newPassword,
          role: newRole,
          totp_secret: newTotp.trim() || undefined,
          ip_allowlist: parseList(newIpAllowlist),
        },
      });
      setCreateResult(res);
      setNewUsername("");
      setNewPassword("");
      setNewTotp("");
      setNewIpAllowlist("");
      await refreshAdmin();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const applyUpdate = async () => {
    if (!selectedUser) return;
    setBusy(true);
    setError(null);
    setUpdateResult(null);
    try {
      const res = await api<AuthUserUpdateRes>(
        `/v1/auth/users/${encodeURIComponent(selectedUser)}`,
        {
          method: "PUT",
          json: {
            password: updatePassword || undefined,
            role: updateRole || undefined,
            disabled: updateDisabled,
            regenerate_totp: regenerateTotp,
            ip_allowlist: parseList(updateIpAllowlist),
          },
        },
      );
      setUpdateResult(res);
      setUpdatePassword("");
      setRegenerateTotp(false);
      await refreshAdmin();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const deleteUser = async (username: string) => {
    if (!window.confirm(`Delete user ${username}?`)) return;
    setBusy(true);
    setError(null);
    try {
      await api(`/v1/auth/users/${encodeURIComponent(username)}`, {
        method: "DELETE",
      });
      await refreshAdmin();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const revokeSession = async (jti: string) => {
    setBusy(true);
    setError(null);
    try {
      await api("/v1/auth/sessions/revoke", {
        method: "POST",
        json: { jti },
      });
      await refreshAdmin();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const revokeUserSessions = async (username: string) => {
    setBusy(true);
    setError(null);
    try {
      await api("/v1/auth/sessions/revoke", {
        method: "POST",
        json: { username },
      });
      await refreshAdmin();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const clearLoginAttempts = async (args: {
    username?: string;
    ip?: string;
    all?: boolean;
  }) => {
    setBusy(true);
    setError(null);
    try {
      await api("/v1/auth/login_attempts/clear", {
        method: "POST",
        json: args,
      });
      await refreshAdmin();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const createApiKey = async () => {
    setBusy(true);
    setError(null);
    setApiCreateResult(null);
    try {
      const expiresIn = Number(apiExpiresIn);
      const res = await api<AuthApiKeyCreateRes>("/v1/auth/api_keys", {
        method: "POST",
        json: {
          username: apiUsername.trim(),
          label: apiLabel.trim() || undefined,
          expires_in_s:
            apiExpiresIn && Number.isFinite(expiresIn) ? expiresIn : undefined,
        },
      });
      setApiCreateResult(res);
      setApiLabel("");
      await refreshAdmin();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const createResetToken = async () => {
    setBusy(true);
    setError(null);
    setResetToken(null);
    try {
      const ttl = Number(resetTtl);
      const res = await api<{ ok: boolean; token: string }>(
        "/v1/auth/password/reset_request",
        {
          method: "POST",
          json: {
            username: resetUsername.trim(),
            ttl_s: Number.isFinite(ttl) ? ttl : undefined,
          },
        },
      );
      setResetToken(res.token);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const revokeApiKey = async (id: number) => {
    setBusy(true);
    setError(null);
    try {
      await api("/v1/auth/api_keys/revoke", {
        method: "POST",
        json: { id },
      });
      await refreshAdmin();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const changePassword = async () => {
    setBusy(true);
    setError(null);
    setPwResult(null);
    try {
      if (pwNew !== pwConfirm) {
        throw new Error("New password confirmation does not match.");
      }
      await api("/v1/auth/password/change", {
        method: "POST",
        json: {
          current_password: pwCurrent,
          new_password: pwNew,
          totp: pwTotp || undefined,
          revoke_sessions: pwRevokeSessions,
          revoke_api_keys: pwRevokeKeys,
        },
      });
      setPwCurrent("");
      setPwNew("");
      setPwConfirm("");
      setPwTotp("");
      setPwResult("Password updated.");
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const selectedUserRow = useMemo(
    () => users.find((u) => u.username === selectedUser) || null,
    [selectedUser, users],
  );

  useEffect(() => {
    if (selectedUserRow) {
      setUpdateRole(selectedUserRow.role || "user");
      setUpdateDisabled(Boolean(selectedUserRow.disabled));
      setUpdateIpAllowlist((selectedUserRow.ip_allowlist || []).join(", "));
    }
  }, [selectedUserRow]);

  return (
    <Stack spacing={2}>
      {error ? <Alert severity="error">{error}</Alert> : null}

      <Card>
        <CardContent>
          <Stack direction="row" spacing={1} sx={{ alignItems: "center" }}>
            <SecurityIcon />
            <Typography variant="h6">Auth</Typography>
          </Stack>
          <Typography variant="body2" color="text.secondary">
            Manage users, 2FA secrets, API keys, and sessions.
          </Typography>
        </CardContent>
        <CardActions>
          <Button startIcon={<RefreshIcon />} onClick={refreshAdmin} disabled={busy}>
            Refresh
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Change password</Typography>
          <Stack spacing={1} sx={{ mt: 2 }}>
            <TextField
              label="Current password"
              type="password"
              value={pwCurrent}
              onChange={(e) => setPwCurrent(e.target.value)}
              disabled={busy}
            />
            <TextField
              label="New password"
              type="password"
              value={pwNew}
              onChange={(e) => setPwNew(e.target.value)}
              disabled={busy}
            />
            <TextField
              label="Confirm new password"
              type="password"
              value={pwConfirm}
              onChange={(e) => setPwConfirm(e.target.value)}
              disabled={busy}
            />
            {config?.totp_enabled ? (
              <TextField
                label="TOTP code"
                value={pwTotp}
                onChange={(e) => setPwTotp(e.target.value)}
                disabled={busy}
              />
            ) : null}
            <FormControlLabel
              control={
                <Switch
                  checked={pwRevokeSessions}
                  onChange={(e) => setPwRevokeSessions(e.target.checked)}
                />
              }
              label="Revoke other sessions"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={pwRevokeKeys}
                  onChange={(e) => setPwRevokeKeys(e.target.checked)}
                />
              }
              label="Revoke API keys"
            />
            {pwResult ? <Alert severity="success">{pwResult}</Alert> : null}
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            onClick={changePassword}
            disabled={
              busy ||
              !pwCurrent ||
              !pwNew ||
              !pwConfirm ||
              pwNew !== pwConfirm
            }
          >
            Update password
          </Button>
        </CardActions>
      </Card>

      {!isAdmin ? (
        <Alert severity="warning">
          Admin role required to manage users, sessions, and API keys.
        </Alert>
      ) : null}

      {isAdmin ? (
        <>
          {createResult?.totp_secret ? (
            <Alert severity="info">
              New user TOTP secret: <code>{createResult.totp_secret}</code>
              {createResult.provisioning_uri ? (
                <>
                  {" "}· URI: <code>{createResult.provisioning_uri}</code>
                </>
              ) : null}
            </Alert>
          ) : null}

          {updateResult?.totp_secret ? (
            <Alert severity="info">
              Rotated TOTP secret: <code>{updateResult.totp_secret}</code>
              {updateResult.provisioning_uri ? (
                <>
                  {" "}· URI: <code>{updateResult.provisioning_uri}</code>
                </>
              ) : null}
            </Alert>
          ) : null}

          {apiCreateResult?.api_key ? (
            <Alert severity="info">
              New API key: <code>{apiCreateResult.api_key}</code>
            </Alert>
          ) : null}
          {resetToken ? (
            <Alert severity="info">
              Password reset token: <code>{resetToken}</code>
            </Alert>
          ) : null}

          <Card>
            <CardContent>
              <Typography variant="h6">Create user</Typography>
              <Stack spacing={1} sx={{ mt: 2 }}>
                <TextField
                  label="Username"
                  value={newUsername}
                  onChange={(e) => setNewUsername(e.target.value)}
                  disabled={busy}
                />
                <TextField
                  label="Password"
                  type="password"
                  value={newPassword}
                  onChange={(e) => setNewPassword(e.target.value)}
                  disabled={busy}
                />
                <FormControl fullWidth>
                  <InputLabel>Role</InputLabel>
                  <Select
                    value={newRole}
                    label="Role"
                    onChange={(e) => setNewRole(String(e.target.value))}
                    disabled={busy}
                  >
                    {roleOptions.map((role) => (
                      <MenuItem key={role} value={role}>
                        {role}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
                <TextField
                  label="TOTP secret (optional)"
                  value={newTotp}
                  onChange={(e) => setNewTotp(e.target.value)}
                  disabled={busy}
                  helperText="Leave empty to auto-generate."
                />
                <TextField
                  label="IP allowlist (comma-separated)"
                  value={newIpAllowlist}
                  onChange={(e) => setNewIpAllowlist(e.target.value)}
                  disabled={busy}
                />
              </Stack>
            </CardContent>
            <CardActions>
              <Button
                variant="contained"
                startIcon={<KeyIcon />}
                onClick={createUser}
                disabled={busy || !newUsername.trim() || !newPassword}
              >
                Create
              </Button>
            </CardActions>
          </Card>

          <Card>
            <CardContent>
              <Typography variant="h6">Update user</Typography>
              <Stack spacing={1} sx={{ mt: 2 }}>
                <FormControl fullWidth>
                  <InputLabel>User</InputLabel>
                  <Select
                    value={selectedUser}
                    label="User"
                    onChange={(e) => setSelectedUser(String(e.target.value))}
                    disabled={busy}
                  >
                    <MenuItem value="">
                      <em>None</em>
                    </MenuItem>
                    {users.map((u) => (
                      <MenuItem key={u.username} value={u.username}>
                        {u.username}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
            <FormControl fullWidth>
              <InputLabel>Role</InputLabel>
              <Select
                value={updateRole}
                label="Role"
                onChange={(e) => setUpdateRole(String(e.target.value))}
                disabled={busy || !selectedUser}
              >
                <MenuItem value="">
                  <em>None</em>
                </MenuItem>
                {roleOptions.map((role) => (
                  <MenuItem key={role} value={role}>
                    {role}
                  </MenuItem>
                ))}
              </Select>
                </FormControl>
                <FormControlLabel
                  control={
                    <Switch
                      checked={updateDisabled}
                      onChange={(e) => setUpdateDisabled(e.target.checked)}
                      disabled={busy || !selectedUser}
                    />
                  }
                  label="Disabled"
                />
                <TextField
                  label="New password (optional)"
                  type="password"
                  value={updatePassword}
                  onChange={(e) => setUpdatePassword(e.target.value)}
                  disabled={busy || !selectedUser}
                />
                <TextField
                  label="IP allowlist (comma-separated)"
                  value={updateIpAllowlist}
                  onChange={(e) => setUpdateIpAllowlist(e.target.value)}
                  disabled={busy || !selectedUser}
                />
                <FormControlLabel
                  control={
                    <Switch
                      checked={regenerateTotp}
                      onChange={(e) => setRegenerateTotp(e.target.checked)}
                      disabled={busy || !selectedUser}
                    />
                  }
                  label="Regenerate TOTP secret"
                />
              </Stack>
            </CardContent>
            <CardActions>
              <Button
                variant="contained"
                onClick={applyUpdate}
                disabled={busy || !selectedUser}
              >
                Update
              </Button>
              <Button
                color="error"
                startIcon={<DeleteIcon />}
                onClick={() => deleteUser(selectedUser)}
                disabled={busy || !selectedUser}
              >
                Delete
              </Button>
            </CardActions>
          </Card>

          <Card>
            <CardContent>
              <Typography variant="h6">Users</Typography>
              <Stack spacing={1} sx={{ mt: 2 }}>
                {users.length ? (
                  users.map((u) => (
                    <Stack key={u.username} spacing={0.5}>
                      <Typography variant="body2">
                        <code>{u.username}</code> · role <code>{u.role}</code> ·
                        {u.disabled ? " disabled" : " active"}
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        last_login=<code>{fmtTs(u.last_login_at ?? null)}</code> ·
                        updated=<code>{fmtTs(u.updated_at ?? null)}</code>
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        ip_allowlist=<code>{
                          (u.ip_allowlist || []).join(", ") || "—"
                        }</code>
                      </Typography>
                      <Button
                        size="small"
                        onClick={() => revokeUserSessions(u.username)}
                        disabled={busy}
                      >
                        Revoke sessions
                      </Button>
                    </Stack>
                  ))
                ) : (
                  <Typography variant="body2" color="text.secondary">
                    No users.
                  </Typography>
                )}
              </Stack>
            </CardContent>
          </Card>

          <Card>
            <CardContent>
              <Typography variant="h6">Sessions</Typography>
              <Stack spacing={1} sx={{ mt: 2 }}>
                <FormControlLabel
                  control={
                    <Switch
                      checked={activeOnly}
                      onChange={(e) => {
                        setSessionOffset(0);
                        setActiveOnly(e.target.checked);
                      }}
                    />
                  }
                  label="Active only"
                />
                <TextField
                  label="Username filter"
                  value={sessionUserFilter}
                  onChange={(e) => {
                    setSessionOffset(0);
                    setSessionUserFilter(e.target.value);
                  }}
                  disabled={busy}
                />
                <TextField
                  label="Limit"
                  value={sessionLimit}
                  onChange={(e) => {
                    setSessionOffset(0);
                    setSessionLimit(e.target.value);
                  }}
                  inputMode="numeric"
                  disabled={busy}
                />
                <Stack direction="row" spacing={1}>
                  <Button
                    size="small"
                    onClick={() =>
                      setSessionOffset((prev) =>
                        Math.max(0, prev - toLimit(sessionLimit, 50)),
                      )
                    }
                    disabled={busy || sessionOffset <= 0}
                  >
                    Prev
                  </Button>
                  <Button
                    size="small"
                    onClick={() =>
                      setSessionOffset(sessionNextOffset ?? sessionOffset)
                    }
                    disabled={busy || sessionNextOffset == null}
                  >
                    Next
                  </Button>
                </Stack>
                {sessions.length ? (
                  sessions.map((s) => (
                    <Stack key={s.jti} spacing={0.5}>
                      <Typography variant="body2">
                        user=<code>{s.username}</code> · created=
                        <code>{fmtTs(s.created_at ?? null)}</code> · expires=
                        <code>{fmtTs(s.expires_at ?? null)}</code>
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        last_seen=<code>{fmtTs(s.last_seen_at ?? null)}</code> ·
                        revoked=<code>{fmtTs(s.revoked_at ?? null)}</code> · ip=
                        <code>{s.ip ?? "—"}</code>
                      </Typography>
                      <Button
                        size="small"
                        onClick={() => revokeSession(s.jti)}
                        disabled={busy}
                      >
                        Revoke session
                      </Button>
                    </Stack>
                  ))
                ) : (
                  <Typography variant="body2" color="text.secondary">
                    No sessions.
                  </Typography>
                )}
              </Stack>
            </CardContent>
          </Card>

          <Card>
            <CardContent>
              <Typography variant="h6">API keys</Typography>
              <Stack spacing={1} sx={{ mt: 2 }}>
                <FormControlLabel
                  control={
                    <Switch
                      checked={apiActiveOnly}
                      onChange={(e) => {
                        setApiOffset(0);
                        setApiActiveOnly(e.target.checked);
                      }}
                    />
                  }
                  label="Active only"
                />
                <TextField
                  label="Username filter"
                  value={apiUserFilter}
                  onChange={(e) => {
                    setApiOffset(0);
                    setApiUserFilter(e.target.value);
                  }}
                  disabled={busy}
                />
                <TextField
                  label="Limit"
                  value={apiLimit}
                  onChange={(e) => {
                    setApiOffset(0);
                    setApiLimit(e.target.value);
                  }}
                  inputMode="numeric"
                  disabled={busy}
                />
                <Stack direction="row" spacing={1}>
                  <Button
                    size="small"
                    onClick={() =>
                      setApiOffset((prev) =>
                        Math.max(0, prev - toLimit(apiLimit, 50)),
                      )
                    }
                    disabled={busy || apiOffset <= 0}
                  >
                    Prev
                  </Button>
                  <Button
                    size="small"
                    onClick={() => setApiOffset(apiNextOffset ?? apiOffset)}
                    disabled={busy || apiNextOffset == null}
                  >
                    Next
                  </Button>
                </Stack>
                {apiKeys.length ? (
                  apiKeys.map((k) => (
                    <Stack key={k.id} spacing={0.5}>
                      <Typography variant="body2">
                        user=<code>{k.username}</code> · label=
                        <code>{k.label || "—"}</code> · prefix=
                        <code>{k.prefix || "—"}</code>
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        created=<code>{fmtTs(k.created_at ?? null)}</code> ·
                        last_used=<code>{fmtTs(k.last_used_at ?? null)}</code> ·
                        expires=<code>{fmtTs(k.expires_at ?? null)}</code>
                      </Typography>
                      <Button
                        size="small"
                        onClick={() => revokeApiKey(k.id)}
                        disabled={busy || Boolean(k.revoked_at)}
                      >
                        Revoke key
                      </Button>
                    </Stack>
                  ))
                ) : (
                  <Typography variant="body2" color="text.secondary">
                    No API keys.
                  </Typography>
                )}
                <Divider />
                <Typography variant="subtitle2">Create API key</Typography>
                <FormControl fullWidth>
                  <InputLabel>User</InputLabel>
                  <Select
                    value={apiUsername}
                    label="User"
                    onChange={(e) => setApiUsername(String(e.target.value))}
                    disabled={busy}
                  >
                    <MenuItem value="">
                      <em>None</em>
                    </MenuItem>
                    {users.map((u) => (
                      <MenuItem key={u.username} value={u.username}>
                        {u.username}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
                <TextField
                  label="Label"
                  value={apiLabel}
                  onChange={(e) => setApiLabel(e.target.value)}
                  disabled={busy}
                />
                <TextField
                  label="Expires in (seconds, optional)"
                  value={apiExpiresIn}
                  onChange={(e) => setApiExpiresIn(e.target.value)}
                  disabled={busy}
                />
              </Stack>
            </CardContent>
            <CardActions>
              <Button
                variant="contained"
                startIcon={<KeyIcon />}
                onClick={createApiKey}
                disabled={busy || !apiUsername}
              >
                Create API key
              </Button>
            </CardActions>
          </Card>

          <Card>
            <CardContent>
              <Typography variant="h6">Password reset token</Typography>
              <Typography variant="body2" color="text.secondary">
                Generate a one-time token to reset a user password.
              </Typography>
              <Stack spacing={1} sx={{ mt: 2 }}>
                <FormControl fullWidth>
                  <InputLabel>User</InputLabel>
                  <Select
                    value={resetUsername}
                    label="User"
                    onChange={(e) => setResetUsername(String(e.target.value))}
                    disabled={busy}
                  >
                    <MenuItem value="">
                      <em>None</em>
                    </MenuItem>
                    {users.map((u) => (
                      <MenuItem key={u.username} value={u.username}>
                        {u.username}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
                <TextField
                  label="TTL (seconds)"
                  value={resetTtl}
                  onChange={(e) => setResetTtl(e.target.value)}
                  inputMode="numeric"
                  disabled={busy}
                />
              </Stack>
            </CardContent>
            <CardActions>
              <Button
                variant="contained"
                startIcon={<LockResetIcon />}
                onClick={createResetToken}
                disabled={busy || !resetUsername}
              >
                Create reset token
              </Button>
            </CardActions>
          </Card>

          <Card>
            <CardContent>
              <Typography variant="h6">Login attempts</Typography>
              <Stack spacing={1} sx={{ mt: 2 }}>
                <FormControlLabel
                  control={
                    <Switch
                      checked={attemptLockedOnly}
                      onChange={(e) => {
                        setAttemptOffset(0);
                        setAttemptLockedOnly(e.target.checked);
                      }}
                    />
                  }
                  label="Locked only"
                />
                <TextField
                  label="Username filter"
                  value={attemptUserFilter}
                  onChange={(e) => {
                    setAttemptOffset(0);
                    setAttemptUserFilter(e.target.value);
                  }}
                  disabled={busy}
                />
                <TextField
                  label="IP filter"
                  value={attemptIpFilter}
                  onChange={(e) => {
                    setAttemptOffset(0);
                    setAttemptIpFilter(e.target.value);
                  }}
                  disabled={busy}
                />
                <TextField
                  label="Limit"
                  value={attemptLimit}
                  onChange={(e) => {
                    setAttemptOffset(0);
                    setAttemptLimit(e.target.value);
                  }}
                  inputMode="numeric"
                  disabled={busy}
                />
                <Stack direction="row" spacing={1}>
                  <Button
                    size="small"
                    onClick={() =>
                      setAttemptOffset((prev) =>
                        Math.max(0, prev - toLimit(attemptLimit, 50)),
                      )
                    }
                    disabled={busy || attemptOffset <= 0}
                  >
                    Prev
                  </Button>
                  <Button
                    size="small"
                    onClick={() => setAttemptOffset(attemptNextOffset ?? attemptOffset)}
                    disabled={busy || attemptNextOffset == null}
                  >
                    Next
                  </Button>
                  <Button
                    size="small"
                    startIcon={<LockResetIcon />}
                    onClick={() => clearLoginAttempts({ all: true })}
                    disabled={busy}
                  >
                    Clear all
                  </Button>
                </Stack>
                {loginAttempts.length ? (
                  loginAttempts.map((a) => (
                    <Stack key={`${a.username}-${a.ip}`} spacing={0.5}>
                      <Typography variant="body2">
                        user=<code>{a.username}</code> · ip=<code>{a.ip}</code> ·
                        failed=<code>{a.failed_count}</code>
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        last_failed=<code>{fmtTs(a.last_failed_at ?? null)}</code> ·
                        locked_until=<code>{fmtTs(a.locked_until ?? null)}</code>
                      </Typography>
                      <Button
                        size="small"
                        onClick={() =>
                          clearLoginAttempts({
                            username: a.username,
                            ip: a.ip,
                          })
                        }
                        disabled={busy}
                      >
                        Clear
                      </Button>
                    </Stack>
                  ))
                ) : (
                  <Typography variant="body2" color="text.secondary">
                    No recent attempts.
                  </Typography>
                )}
              </Stack>
            </CardContent>
          </Card>
        </>
      ) : null}
    </Stack>
  );
}
