import RefreshIcon from "@mui/icons-material/Refresh";
import ScheduleIcon from "@mui/icons-material/Schedule";
import StopIcon from "@mui/icons-material/Stop";
import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import BoltIcon from "@mui/icons-material/Bolt";
import VisibilityIcon from "@mui/icons-material/Visibility";
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
import { useAuth } from "../../auth";
import { TargetPreviewDialog } from "../../components/TargetPreviewDialog";
import { useEventRefresh } from "../../hooks/useEventRefresh";

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

type SchedulerEventsRes = {
  ok: boolean;
  events: SchedulerEvent[];
  count?: number;
  limit?: number;
  offset?: number;
  next_offset?: number | null;
};

type RetentionRes = {
  ok?: boolean;
  stats?: { count?: number; oldest?: number | null; newest?: number | null };
  settings?: {
    max_rows?: number;
    max_days?: number;
    maintenance_interval_s?: number;
  };
  drift?: {
    excess_rows?: number;
    excess_age_s?: number;
    oldest_age_s?: number | null;
    drift?: boolean;
  };
  last_retention?: { at?: number; result?: Record<string, unknown> } | null;
};

type PageMeta = {
  count?: number;
  limit?: number;
  offset?: number;
  next_offset?: number | null;
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

function toEpochSeconds(value: string): string | null {
  if (!value) return null;
  const ts = Date.parse(value);
  if (Number.isNaN(ts)) return null;
  return String(Math.floor(ts / 1000));
}

export function SchedulerTools() {
  const { user } = useAuth();
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState<SchedulerStatus | null>(null);
  const [events, setEvents] = useState<SchedulerEvent[]>([]);
  const [eventsMeta, setEventsMeta] = useState<PageMeta | null>(null);
  const [retention, setRetention] = useState<RetentionRes | null>(null);
  const [retentionError, setRetentionError] = useState<string | null>(null);
  const [retentionBusy, setRetentionBusy] = useState(false);
  const [retentionOverrideRows, setRetentionOverrideRows] = useState("");
  const [retentionOverrideDays, setRetentionOverrideDays] = useState("");
  const [retentionResult, setRetentionResult] =
    useState<Record<string, unknown> | null>(null);

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

  const [eventsLimit, setEventsLimit] = useState("20");
  const [eventsOffset, setEventsOffset] = useState("0");
  const [eventsAgentFilter, setEventsAgentFilter] = useState("");
  const [eventsSinceFilter, setEventsSinceFilter] = useState("");
  const [eventsUntilFilter, setEventsUntilFilter] = useState("");

  const [previewOpen, setPreviewOpen] = useState(false);
  const [previewTargets, setPreviewTargets] = useState<string[] | null>(null);

  const nextActionHint = useMemo(() => {
    const s = status?.next_action_in_s;
    if (s == null) return null;
    if (s <= 0.5) return "Next action: now";
    if (s < 60) return `Next action in ${Math.round(s)}s`;
    return `Next action in ${Math.round(s / 60)}m`;
  }, [status?.next_action_in_s]);
  const retentionDrift = retention?.drift?.drift;
  const isAdmin = (user?.role || "") === "admin";

  const buildEventsQuery = (lim: number, off: number) => {
    const q = new URLSearchParams();
    q.set("limit", String(lim));
    if (off > 0) q.set("offset", String(off));
    if (eventsAgentFilter.trim()) q.set("agent_id", eventsAgentFilter.trim());
    const since = toEpochSeconds(eventsSinceFilter);
    const until = toEpochSeconds(eventsUntilFilter);
    if (since) q.set("since", since);
    if (until) q.set("until", until);
    return q;
  };

  const fetchRetention = async () => {
    setRetentionError(null);
    try {
      const res = await api<RetentionRes>("/v1/scheduler/retention", {
        method: "GET",
      });
      setRetention(res);
    } catch (e) {
      setRetentionError(e instanceof Error ? e.message : String(e));
    }
  };

  const runRetention = async () => {
    setRetentionBusy(true);
    setRetentionError(null);
    setRetentionResult(null);
    try {
      const params = new URLSearchParams();
      const rows = parseInt(retentionOverrideRows || "", 10);
      if (Number.isFinite(rows) && rows > 0) params.set("max_rows", String(rows));
      const days = parseInt(retentionOverrideDays || "", 10);
      if (Number.isFinite(days) && days > 0) params.set("max_days", String(days));
      const url =
        params.toString().length > 0
          ? `/v1/scheduler/retention?${params.toString()}`
          : "/v1/scheduler/retention";
      const res = await api<{ ok?: boolean; result?: Record<string, unknown> }>(
        url,
        { method: "POST", json: {} },
      );
      setRetentionResult(res.result ?? null);
      await fetchRetention();
    } catch (e) {
      setRetentionError(e instanceof Error ? e.message : String(e));
    } finally {
      setRetentionBusy(false);
    }
  };

  const refresh = async (opts?: { eventsOffset?: number }) => {
    setError(null);
    setBusy(true);
    try {
      const st = await api<SchedulerStatus>("/v1/scheduler/status", {
        method: "GET",
      });
      setStatus(st);
      try {
        const lim = parseInt(eventsLimit || "20", 10) || 20;
        const off = opts?.eventsOffset ?? (parseInt(eventsOffset || "0", 10) || 0);
        if (opts?.eventsOffset != null) setEventsOffset(String(off));
        const q = buildEventsQuery(lim, off);
        const ev = await api<SchedulerEventsRes>(
          `/v1/scheduler/events?${q.toString()}`,
          { method: "GET" },
        );
        setEvents(ev.events || []);
        setEventsMeta({
          count: ev.count ?? ev.events?.length ?? 0,
          limit: ev.limit ?? lim,
          offset: ev.offset ?? off,
          next_offset: ev.next_offset ?? null,
        });
      } catch {
        setEvents([]);
        setEventsMeta(null);
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
    void fetchRetention();
  }, []);

  useEventRefresh({
    types: ["scheduler", "sequences", "tick"],
    refresh,
    minIntervalMs: 3000,
  });

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

  const downloadExport = async (url: string, filename: string) => {
    setBusy(true);
    setError(null);
    try {
      const resp = await fetch(url, { credentials: "include" });
      if (!resp.ok) {
        const contentType = resp.headers.get("content-type") ?? "";
        if (contentType.includes("application/json")) {
          const data = (await resp.json()) as { detail?: string; error?: string };
          throw new Error(data.detail || data.error || `HTTP ${resp.status}`);
        }
        const text = await resp.text();
        throw new Error(text || `HTTP ${resp.status}`);
      }
      const blob = await resp.blob();
      const href = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = href;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(href);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const exportEvents = async (format: "csv" | "json") => {
    const lim = Math.min(parseInt(eventsLimit || "20", 10) || 20, 20000);
    const off = parseInt(eventsOffset || "0", 10) || 0;
    const q = buildEventsQuery(lim, off);
    q.set("format", format);
    const filename =
      format === "json" ? "scheduler_events.json" : "scheduler_events.csv";
    await downloadExport(`/v1/scheduler/events/export?${q.toString()}`, filename);
  };

  const pageEvents = async (dir: "prev" | "next") => {
    const limitVal = eventsMeta?.limit ?? (parseInt(eventsLimit || "20", 10) || 20);
    const current =
      eventsMeta?.offset ?? (parseInt(eventsOffset || "0", 10) || 0);
    const next =
      dir === "next"
        ? eventsMeta?.next_offset ?? null
        : Math.max(0, current - limitVal);
    if (next == null) return;
    await refresh({ eventsOffset: next });
  };

  return (
    <Stack spacing={2}>
      {error ? <Alert severity="error">{error}</Alert> : null}

      <Card>
        <CardContent>
          <Typography variant="h6">Scheduler Status</Typography>
          <Typography variant="body2" color="text.secondary">
            Basic show-window automation. Config is stored under{" "}
            <code>DATABASE_URL</code> (shared fleet config).
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
          <Button
            startIcon={<RefreshIcon />}
            onClick={() => void refresh()}
            disabled={busy}
          >
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
          <Stack spacing={1} sx={{ mt: 2 }}>
            <TextField
              label="Limit"
              value={eventsLimit}
              onChange={(e) => setEventsLimit(e.target.value)}
              disabled={busy}
              inputMode="numeric"
            />
            <TextField
              label="Offset"
              value={eventsOffset}
              onChange={(e) => setEventsOffset(e.target.value)}
              disabled={busy}
              inputMode="numeric"
            />
            <TextField
              label="Agent filter"
              value={eventsAgentFilter}
              onChange={(e) => setEventsAgentFilter(e.target.value)}
              disabled={busy}
              placeholder="agent-1"
            />
            <TextField
              label="Since"
              type="datetime-local"
              value={eventsSinceFilter}
              onChange={(e) => setEventsSinceFilter(e.target.value)}
              disabled={busy}
              InputLabelProps={{ shrink: true }}
            />
            <TextField
              label="Until"
              type="datetime-local"
              value={eventsUntilFilter}
              onChange={(e) => setEventsUntilFilter(e.target.value)}
              disabled={busy}
              InputLabelProps={{ shrink: true }}
            />
          </Stack>
          {eventsMeta ? (
            <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
              Showing <code>{eventsMeta.count ?? events.length}</code> · offset=
              <code>{eventsMeta.offset ?? 0}</code> · limit=
              <code>{eventsMeta.limit ?? eventsLimit}</code>
            </Typography>
          ) : null}
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
          <Button
            startIcon={<RefreshIcon />}
            onClick={() => void refresh()}
            disabled={busy}
          >
            Refresh
          </Button>
          <Button
            onClick={() => void pageEvents("prev")}
            disabled={busy || (eventsMeta?.offset ?? 0) <= 0}
          >
            Prev
          </Button>
          <Button
            onClick={() => void pageEvents("next")}
            disabled={busy || eventsMeta?.next_offset == null}
          >
            Next
          </Button>
          <Button onClick={() => void exportEvents("csv")} disabled={busy}>
            Export CSV
          </Button>
          <Button onClick={() => void exportEvents("json")} disabled={busy}>
            Export JSON
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Scheduler Retention</Typography>
          <Typography variant="body2" color="text.secondary">
            Retention status for <code>scheduler_events</code>.
          </Typography>
          {retentionDrift ? (
            <Alert severity="warning" sx={{ mt: 2 }}>
              Scheduler events exceed retention targets. Run cleanup or adjust{" "}
              <code>SCHEDULER_EVENTS_MAX_ROWS</code>/<code>SCHEDULER_EVENTS_MAX_DAYS</code>.
            </Alert>
          ) : null}
          {retentionError ? (
            <Alert severity="warning" sx={{ mt: 2 }}>
              {retentionError}
            </Alert>
          ) : null}
          {retentionResult ? (
            <Alert severity="success" sx={{ mt: 2 }}>
              Cleanup complete:{" "}
              <code>
                rows {String(retentionResult.deleted_by_rows ?? 0)} · days{" "}
                {String(retentionResult.deleted_by_days ?? 0)}
              </code>
            </Alert>
          ) : null}
          <Stack spacing={1} sx={{ mt: 2 }}>
            <Typography variant="body2">
              Count: <code>{retention?.stats?.count ?? 0}</code> · Oldest{" "}
              <code>{fmtTs(retention?.stats?.oldest ?? null)}</code> · Newest{" "}
              <code>{fmtTs(retention?.stats?.newest ?? null)}</code>
            </Typography>
            <Typography variant="body2">
              Max rows: <code>{retention?.settings?.max_rows ?? 0}</code> · Max days{" "}
              <code>{retention?.settings?.max_days ?? 0}</code>
            </Typography>
            <Typography variant="body2">
              Drift: rows <code>{retention?.drift?.excess_rows ?? 0}</code> · age{" "}
              <code>
                {retention?.drift?.excess_age_s
                  ? `${Math.round(retention.drift.excess_age_s)}s`
                  : "0s"}
              </code>
            </Typography>
            <TextField
              label="Override max rows"
              value={retentionOverrideRows}
              onChange={(e) => setRetentionOverrideRows(e.target.value)}
              helperText="Optional override for manual cleanup."
              disabled={!isAdmin || retentionBusy}
              inputMode="numeric"
            />
            <TextField
              label="Override max days"
              value={retentionOverrideDays}
              onChange={(e) => setRetentionOverrideDays(e.target.value)}
              helperText="Optional override for manual cleanup."
              disabled={!isAdmin || retentionBusy}
              inputMode="numeric"
            />
            <Typography variant="body2" color="text.secondary">
              Last cleanup:{" "}
              <code>
                {retention?.last_retention?.at
                  ? fmtTs(retention.last_retention.at)
                  : "—"}
              </code>
            </Typography>
          </Stack>
        </CardContent>
        <CardActions>
          <Button onClick={() => void fetchRetention()} disabled={retentionBusy}>
            Refresh retention
          </Button>
          <Button
            variant="contained"
            onClick={() => void runRetention()}
            disabled={!isAdmin || retentionBusy}
          >
            Run cleanup
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
              <Stack spacing={1}>
                <TextField
                  label="Targets (optional, comma-separated)"
                  value={targets}
                  onChange={(e) => setTargets(e.target.value)}
                  disabled={busy}
                  helperText='Examples: "roofline1", "role:roofline", "tag:outside", "*" (all online discovered). Leave blank = all configured peers.'
                />
                <Button
                  variant="outlined"
                  startIcon={<VisibilityIcon />}
                  onClick={() => {
                    setPreviewTargets(parseTargets(targets));
                    setPreviewOpen(true);
                  }}
                  disabled={busy}
                >
                  Preview targets
                </Button>
              </Stack>
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

      <TargetPreviewDialog
        open={previewOpen}
        title="Scheduler targets"
        targets={previewTargets}
        onClose={() => setPreviewOpen(false)}
      />
    </Stack>
  );
}
