import RefreshIcon from "@mui/icons-material/Refresh";
import {
  Alert,
  Box,
  Button,
  Card,
  CardActions,
  CardContent,
  Chip,
  Stack,
  Typography,
} from "@mui/material";
import React, { useEffect, useMemo, useState } from "react";
import { api } from "../../api";
import { useAuth } from "../../auth";
import { useEventRefresh } from "../../hooks/useEventRefresh";

type MqttStatus = {
  ok?: boolean;
  enabled?: boolean;
  running?: boolean;
  connected?: boolean;
  base_topic?: string;
  qos?: number;
  status_interval_s?: number;
  reconnect_interval_s?: number;
  broker?: {
    host?: string;
    port?: number;
    tls?: boolean;
    username?: string | null;
  } | null;
  broker_error?: string | null;
  ha_discovery?: {
    enabled?: boolean;
    prefix?: string;
    entity_prefix?: string;
  };
  last_error?: string | null;
  last_error_at?: number | null;
  last_connect_at?: number | null;
  last_disconnect_at?: number | null;
  last_message_at?: number | null;
  last_action?: string | null;
  counters?: {
    messages_received?: number;
    actions_ok?: number;
    actions_failed?: number;
  };
  topics?: {
    base?: string;
    commands?: string[];
    state?: string[];
  };
};

export function MqttTools() {
  const { config } = useAuth();
  const [status, setStatus] = useState<MqttStatus | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const enabled = Boolean(
    (status?.enabled ?? config?.mqtt_enabled) && status?.base_topic,
  );

  const pretty = useMemo(
    () => (status ? JSON.stringify(status, null, 2) : "-"),
    [status],
  );

  const refresh = async () => {
    setBusy(true);
    setError(null);
    try {
      const res = await api<MqttStatus>("/v1/mqtt/status", { method: "GET" });
      setStatus(res);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  useEffect(() => {
    void refresh();
  }, []);

  useEventRefresh({
    types: ["mqtt", "tick"],
    refresh,
    minIntervalMs: 3000,
  });

  return (
    <Stack spacing={2}>
      {error ? <Alert severity="error">{error}</Alert> : null}
      {!enabled ? (
        <Alert severity="info">
          MQTT bridge is not enabled. Set <code>MQTT_ENABLED=true</code> and{" "}
          <code>MQTT_URL</code>.
        </Alert>
      ) : null}
      <Card>
        <CardContent>
          <Typography variant="h6">MQTT Bridge</Typography>
          <Stack direction="row" spacing={1} sx={{ mt: 1, flexWrap: "wrap" }}>
            <Chip
              label={status?.connected ? "connected" : "disconnected"}
              color={status?.connected ? "success" : "default"}
              size="small"
            />
            <Chip
              label={status?.running ? "running" : "stopped"}
              color={status?.running ? "primary" : "default"}
              size="small"
            />
            <Chip
              label={`qos ${status?.qos ?? 0}`}
              size="small"
              variant="outlined"
            />
            {status?.base_topic ? (
              <Chip label={`topic ${status.base_topic}`} size="small" />
            ) : null}
          </Stack>
          <Stack spacing={1} sx={{ mt: 2 }}>
            <Typography variant="body2" color="text.secondary">
              Broker:{" "}
              <code>
                {status?.broker?.host || "-"}:{status?.broker?.port || "-"}
              </code>{" "}
              {status?.broker?.tls ? "(tls)" : "(plain)"}
            </Typography>
            {status?.broker_error ? (
              <Alert severity="warning">{status.broker_error}</Alert>
            ) : null}
            {status?.last_error ? (
              <Alert severity="warning">
                Last error: <code>{status.last_error}</code>
              </Alert>
            ) : null}
          </Stack>
          <Stack spacing={1} sx={{ mt: 2 }}>
            <Typography variant="subtitle2">Topics</Typography>
            <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap" }}>
              {(status?.topics?.commands || []).map((t) => (
                <Chip key={t} label={`${status?.base_topic}/${t}`} size="small" />
              ))}
            </Stack>
            <Typography variant="subtitle2" sx={{ mt: 1 }}>
              State topics
            </Typography>
            <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap" }}>
              {(status?.topics?.state || []).map((t) => (
                <Chip key={t} label={`${status?.base_topic}/${t}`} size="small" />
              ))}
            </Stack>
          </Stack>
          <Box
            component="pre"
            sx={{
              whiteSpace: "pre-wrap",
              wordBreak: "break-word",
              fontSize: 12,
              mt: 2,
            }}
          >
            {pretty}
          </Box>
        </CardContent>
        <CardActions>
          <Button startIcon={<RefreshIcon />} onClick={refresh} disabled={busy}>
            Refresh
          </Button>
        </CardActions>
      </Card>
    </Stack>
  );
}
