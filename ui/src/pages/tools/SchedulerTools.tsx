import RefreshIcon from "@mui/icons-material/Refresh";
import ScheduleIcon from "@mui/icons-material/Schedule";
import StopIcon from "@mui/icons-material/Stop";
import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import BoltIcon from "@mui/icons-material/Bolt";
import {
  Alert,
  Button,
  Card,
  CardActions,
  CardContent,
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

type SchedulerStatus = {
  ok: boolean;
  running: boolean;
  in_window: boolean;
  eligible?: boolean;
  leader_roles?: string[];
  leader?: boolean;
  lease?: {
    key?: string;
    owner_id?: string | null;
    expires_at?: number | null;
    ttl_s?: number | null;
  };
  last_action_at: number | null;
  last_action: string | null;
  last_error: string | null;
  next_action_in_s: number | null;
  config: any;
};

type SchedulerEvent = {
  id: number | null;
  agent_id: string;
  created_at: number;
  action: string;
  scope: string;
  reason: string;
  ok: boolean;
  duration_s: number;
  error: string | null;
};

function parseTargets(raw: string): string[] | null {
  const out = raw
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
  return out.length ? out : null;
}

function fmtTs(ts: number | null): string {
  if (!ts) return "—";
  try {
    return new Date(ts * 1000).toLocaleString();
  } catch {
    return String(ts);
  }
}

export function SchedulerTools() {
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState<SchedulerStatus | null>(null);
  const [events, setEvents] = useState<SchedulerEvent[]>([]);

  const [sequences, setSequences] = useState<string[]>([]);

  const cfg = status?.config ?? {};

  const [enabled, setEnabled] = useState(true);
  const [autostart, setAutostart] = useState(false);
  const [startHhmm, setStartHhmm] = useState("17:00");
  const [endHhmm, setEndHhmm] = useState("23:00");
  const [mode, setMode] = useState<"looks" | "sequence">("looks");
  const [scope, setScope] = useState<"local" | "fleet">("fleet");
  const [intervalS, setIntervalS] = useState("300");
  const [theme, setTheme] = useState("");
  const [brightness, setBrightness] = useState("");
  const [targets, setTargets] = useState("");
  const [includeSelf, setIncludeSelf] = useState(true);
  const [sequenceFile, setSequenceFile] = useState("");
  const [sequenceLoop, setSequenceLoop] = useState(true);
  const [stopAllOnEnd, setStopAllOnEnd] = useState(true);

  const nextActionHint = useMemo(() => {
    const s = status?.next_action_in_s;
    if (s == null) return null;
    if (s <= 0.5) return "Next action: now";
    if (s < 60) return `Next action in ${Math.round(s)}s`;
    return `Next action in ${Math.round(s / 60)}m`;
  }, [status?.next_action_in_s]);

  const refresh = async () => {
    setError(null);
    setBusy(true);
    try {
      const st = await api<SchedulerStatus>("/v1/scheduler/status", {
        method: "GET",
      });
      setStatus(st);
      try {
        const ev = await api<{ ok: boolean; events: SchedulerEvent[] }>(
          "/v1/scheduler/events?limit=20",
          { method: "GET" },
        );
        setEvents(ev.events || []);
      } catch {
        setEvents([]);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const refreshSequences = async () => {
    try {
      const res = await api<{ ok: boolean; files: string[] }>(
        "/v1/sequences/list",
        {
          method: "GET",
        },
      );
      setSequences(res.files || []);
    } catch {
      setSequences([]);
    }
  };

  useEffect(() => {
    void refresh();
    void refreshSequences();
  }, []);

  useEffect(() => {
    if (!status) return;
    setEnabled(Boolean(cfg.enabled));
    setAutostart(Boolean(cfg.autostart));
    setStartHhmm(String(cfg.start_hhmm ?? "17:00"));
    setEndHhmm(String(cfg.end_hhmm ?? "23:00"));
    setMode((cfg.mode === "sequence" ? "sequence" : "looks") as any);
    setScope((cfg.scope === "local" ? "local" : "fleet") as any);
    setIntervalS(String(cfg.interval_s ?? 300));
    setTheme(String(cfg.theme ?? ""));
    setBrightness(cfg.brightness == null ? "" : String(cfg.brightness));
    setTargets(Array.isArray(cfg.targets) ? cfg.targets.join(",") : "");
    setIncludeSelf(Boolean(cfg.include_self ?? true));
    setSequenceFile(String(cfg.sequence_file ?? ""));
    setSequenceLoop(Boolean(cfg.sequence_loop ?? true));
    setStopAllOnEnd(Boolean(cfg.stop_all_on_end ?? true));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [status?.config]);

  const save = async () => {
    setError(null);
    setBusy(true);
    try {
      await api("/v1/scheduler/config", {
        method: "POST",
        json: {
          enabled: Boolean(enabled),
          autostart: Boolean(autostart),
          start_hhmm: startHhmm,
          end_hhmm: endHhmm,
          mode,
          scope,
          interval_s: parseInt(intervalS || "300", 10),
          theme: theme.trim() || null,
          brightness: brightness.trim()
            ? parseInt(brightness.trim(), 10)
            : null,
          targets: parseTargets(targets),
          include_self: Boolean(includeSelf),
          sequence_file: sequenceFile.trim() || null,
          sequence_loop: Boolean(sequenceLoop),
          stop_all_on_end: Boolean(stopAllOnEnd),
        },
      });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const start = async () => {
    setError(null);
    setBusy(true);
    try {
      await api("/v1/scheduler/start", { method: "POST", json: {} });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const stop = async () => {
    setError(null);
    setBusy(true);
    try {
      await api("/v1/scheduler/stop", { method: "POST", json: {} });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const runOnce = async () => {
    setError(null);
    setBusy(true);
    try {
      await api("/v1/scheduler/run_once", { method: "POST", json: {} });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Stack spacing={2}>
      {error ? <Alert severity="error">{error}</Alert> : null}

      <Card>
        <CardContent>
          <Typography variant="h6">Scheduler Status</Typography>
          <Typography variant="body2" color="text.secondary">
            Basic show-window automation. Config is stored under{" "}
            <code>DATABASE_URL</code> (shared fleet config) and mirrored to{" "}
            <code>DATA_DIR/show/scheduler.json</code>.
          </Typography>
          {status ? (
            <Stack spacing={1} sx={{ mt: 2 }}>
              <Typography variant="body2">
                Running: <code>{String(status.running)}</code> · In window:{" "}
                <code>{String(status.in_window)}</code> · Eligible:{" "}
                <code>{String(status.eligible ?? true)}</code> · Leader:{" "}
                <code>{String(status.leader ?? false)}</code>
              </Typography>
              {status.leader_roles?.length ? (
                <Typography variant="body2">
                  Leader roles: <code>{status.leader_roles.join(",")}</code>
                </Typography>
              ) : null}
              {status.lease?.owner_id ? (
                <Typography variant="body2">
                  Lease owner: <code>{status.lease.owner_id}</code>
                </Typography>
              ) : null}
              <Typography variant="body2">
                Last action: <code>{status.last_action ?? "—"}</code> at{" "}
                <code>{fmtTs(status.last_action_at)}</code>
              </Typography>
              {nextActionHint ? (
                <Typography variant="body2">{nextActionHint}</Typography>
              ) : null}
              {status.last_error ? (
                <Alert severity="warning">{status.last_error}</Alert>
              ) : null}
            </Stack>
          ) : (
            <Typography variant="body2" sx={{ mt: 2 }}>
              No status yet.
            </Typography>
          )}
        </CardContent>
        <CardActions>
          <Button startIcon={<RefreshIcon />} onClick={refresh} disabled={busy}>
            Refresh
          </Button>
          <Button
            variant="contained"
            startIcon={<PlayArrowIcon />}
            onClick={start}
            disabled={busy}
          >
            Start
          </Button>
          <Button
            color="error"
            startIcon={<StopIcon />}
            onClick={stop}
            disabled={busy}
          >
            Stop
          </Button>
          <Button startIcon={<BoltIcon />} onClick={runOnce} disabled={busy}>
            Run once
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Recent events</Typography>
          {events.length === 0 ? (
            <Typography variant="body2" sx={{ mt: 1 }} color="text.secondary">
              No scheduler events yet.
            </Typography>
          ) : (
            <Stack spacing={1} sx={{ mt: 2 }}>
              {events.map((ev) => (
                <Typography
                  key={`${ev.id ?? "x"}-${ev.created_at}-${ev.action}`}
                  variant="body2"
                >
                  <code>{fmtTs(ev.created_at)}</code> · <code>{ev.action}</code>{" "}
                  (<code>{ev.scope}</code>) ·{" "}
                  <code>{ev.ok ? "ok" : "failed"}</code>
                  {ev.error ? (
                    <>
                      {" "}
                      · <code>{ev.error}</code>
                    </>
                  ) : null}
                </Typography>
              ))}
            </Stack>
          )}
        </CardContent>
        <CardActions>
          <Button startIcon={<RefreshIcon />} onClick={refresh} disabled={busy}>
            Refresh
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Config</Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <FormControlLabel
                control={
                  <Switch
                    checked={enabled}
                    onChange={(e) => setEnabled(e.target.checked)}
                  />
                }
                label="Enabled"
              />
              <FormControlLabel
                control={
                  <Switch
                    checked={autostart}
                    onChange={(e) => setAutostart(e.target.checked)}
                  />
                }
                label="Autostart on boot"
              />
              <FormControlLabel
                control={
                  <Switch
                    checked={stopAllOnEnd}
                    onChange={(e) => setStopAllOnEnd(e.target.checked)}
                  />
                }
                label="Stop all when window ends"
              />
            </Stack>

            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <TextField
                label="Start (HH:MM)"
                value={startHhmm}
                onChange={(e) => setStartHhmm(e.target.value)}
                disabled={busy}
                fullWidth
              />
              <TextField
                label="End (HH:MM)"
                value={endHhmm}
                onChange={(e) => setEndHhmm(e.target.value)}
                disabled={busy}
                fullWidth
              />
            </Stack>

            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <FormControl fullWidth>
                <InputLabel>Mode</InputLabel>
                <Select
                  value={mode}
                  label="Mode"
                  onChange={(e) =>
                    setMode((e.target.value as "looks" | "sequence") ?? "looks")
                  }
                  disabled={busy}
                >
                  <MenuItem value="looks">Apply random looks</MenuItem>
                  <MenuItem value="sequence">Keep a sequence running</MenuItem>
                </Select>
              </FormControl>
              <FormControl fullWidth>
                <InputLabel>Scope</InputLabel>
                <Select
                  value={scope}
                  label="Scope"
                  onChange={(e) =>
                    setScope((e.target.value as "local" | "fleet") ?? "fleet")
                  }
                  disabled={busy}
                >
                  <MenuItem value="fleet">Fleet</MenuItem>
                  <MenuItem value="local">Local only</MenuItem>
                </Select>
              </FormControl>
            </Stack>

            <FormControlLabel
              control={
                <Switch
                  checked={includeSelf}
                  onChange={(e) => setIncludeSelf(e.target.checked)}
                />
              }
              label="Include self (fleet mode)"
            />

            {scope === "fleet" ? (
              <TextField
                label="Targets (optional, comma-separated peer names)"
                value={targets}
                onChange={(e) => setTargets(e.target.value)}
                disabled={busy}
                helperText="Leave blank to target all configured peers."
              />
            ) : null}

            {mode === "looks" ? (
              <>
                <TextField
                  label="Interval (s)"
                  value={intervalS}
                  onChange={(e) => setIntervalS(e.target.value)}
                  disabled={busy}
                  inputMode="numeric"
                />
                <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
                  <TextField
                    label="Theme (optional)"
                    value={theme}
                    onChange={(e) => setTheme(e.target.value)}
                    disabled={busy}
                    fullWidth
                  />
                  <TextField
                    label="Brightness (optional)"
                    value={brightness}
                    onChange={(e) => setBrightness(e.target.value)}
                    disabled={busy}
                    inputMode="numeric"
                    fullWidth
                  />
                </Stack>
              </>
            ) : (
              <>
                <FormControl fullWidth>
                  <InputLabel>Sequence</InputLabel>
                  <Select
                    value={sequenceFile}
                    label="Sequence"
                    onChange={(e) => setSequenceFile(String(e.target.value))}
                    disabled={busy}
                  >
                    <MenuItem value="">(none)</MenuItem>
                    {sequences.map((f) => (
                      <MenuItem key={f} value={f}>
                        {f}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
                <FormControlLabel
                  control={
                    <Switch
                      checked={sequenceLoop}
                      onChange={(e) => setSequenceLoop(e.target.checked)}
                    />
                  }
                  label="Loop sequence"
                />
              </>
            )}
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            startIcon={<ScheduleIcon />}
            onClick={save}
            disabled={busy}
          >
            Save config
          </Button>
          <Button onClick={refreshSequences} disabled={busy}>
            Refresh sequences
          </Button>
        </CardActions>
      </Card>
    </Stack>
  );
}
