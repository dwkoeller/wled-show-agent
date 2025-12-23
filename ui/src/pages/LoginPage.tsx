import LockIcon from "@mui/icons-material/Lock";
import {
  Alert,
  Box,
  Button,
  Container,
  TextField,
  Typography,
} from "@mui/material";
import React, { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "../auth";

export function LoginPage() {
  const { config, login } = useAuth();
  const nav = useNavigate();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [totp, setTotp] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const needsTotp = useMemo(
    () => (config ? Boolean(config.totp_enabled) : true),
    [config],
  );

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setBusy(true);
    try {
      await login({ username, password, totp: totp || undefined });
      nav("/");
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Container maxWidth="xs">
      <Box sx={{ mt: 10, display: "flex", flexDirection: "column", gap: 2 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
          <LockIcon color="primary" />
          <Typography variant="h5">Sign in</Typography>
        </Box>
        {error ? <Alert severity="error">{error}</Alert> : null}
        <Box
          component="form"
          onSubmit={onSubmit}
          sx={{ display: "flex", flexDirection: "column", gap: 2 }}
        >
          <TextField
            label="Username"
            autoComplete="username"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            disabled={busy}
            required
          />
          <TextField
            label="Password"
            type="password"
            autoComplete="current-password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            disabled={busy}
            required
          />
          {needsTotp ? (
            <TextField
              label="TOTP (6 digits)"
              autoComplete="one-time-code"
              inputMode="numeric"
              value={totp}
              onChange={(e) => setTotp(e.target.value)}
              disabled={busy}
              required
            />
          ) : null}
          <Button type="submit" variant="contained" disabled={busy}>
            Sign in
          </Button>
          <Typography variant="body2" color="text.secondary">
            Auth uses an HttpOnly JWT cookie. Use HTTPS and set
            AUTH_COOKIE_SECURE=true for internet-facing deployments.
          </Typography>
        </Box>
      </Box>
    </Container>
  );
}
