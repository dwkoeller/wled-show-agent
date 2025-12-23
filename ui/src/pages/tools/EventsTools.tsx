import EventNoteIcon from "@mui/icons-material/EventNote";
import RefreshIcon from "@mui/icons-material/Refresh";
import {
  Alert,
  Box,
  Button,
  Card,
  CardActions,
  CardContent,
  Chip,
  FormControlLabel,
  MenuItem,
  Stack,
  Switch,
  TextField,
  Typography,
} from "@mui/material";
import React, { useEffect, useMemo, useState } from "react";
import { api } from "../../api";
import { useAuth } from "../../auth";
import { useFilteredEvents } from "../../hooks/useFilteredEvents";

const MAX_ENTRIES = 200;

type LogEntry = {
  id?: number;
  type: string;
  data?: unknown;
  ts?: number;
};

type EventHistoryRow = {
  id: number | null;
  agent_id: string;
  created_at: number;
  event_type: string;
  event?: string | null;
  payload?: Record<string, unknown> | null;
};

type PageMeta = {
  count?: number;
  limit?: number;
  offset?: number;
  next_offset?: number | null;
  after_id?: number | null;
  next_after_id?: number | null;
};

type EventHistoryRes = {
  ok: boolean;
  events: EventHistoryRow[];
  count?: number;
  limit?: number;
  offset?: number;
  next_offset?: number | null;
  after_id?: number | null;
  next_after_id?: number | null;
};

type EventStatsRes = {
  ok: boolean;
  bus?: {
    subscribers?: number;
    history?: number;
    max_history?: number;
    missed_total?: number;
    dropped_total?: number;
    clients?: Array<{
      id: number;
      connected_at: number;
      last_event_id?: number | null;
      last_seen_at?: number | null;
      missed?: number;
      dropped?: number;
      queue_max?: number;
      queue_size?: number;
    }>;
  };
  spool?: {
    queued_bytes?: number;
    queued_events?: number;
    dropped?: number;
  };
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

function fmtTs(ts?: number | null): string {
  if (!ts) return "—";
  try {
    return new Date(ts * 1000).toLocaleString();
  } catch {
    return String(ts);
  }
}

function fmtTsMs(ts?: number | null): string {
  if (!ts) return "—";
  try {
    return new Date(ts).toLocaleString();
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

function parseCsvList(value: string): string[] {
  return value
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
}

function parseAfterId(value: string): number | null {
  if (!value.trim()) return null;
  const id = parseInt(value, 10);
  if (!Number.isFinite(id) || id < 0) return null;
  return id;
}

function eventKind(data: unknown): string {
  if (!data || typeof data !== "object") return "";
  const raw = (data as { event?: unknown }).event;
  if (!raw) return "";
  return String(raw);
}

function formatPayload(data: unknown): string {
  if (data == null) return "";
  try {
    return JSON.stringify(data, null, 2);
  } catch {
    return String(data);
  }
}

export function EventsTools() {
  const { user } = useAuth();
  const [liveTypeFilter, setLiveTypeFilter] = useState("");
  const [liveEventFilter, setLiveEventFilter] = useState("");
  const liveTypes = useMemo(
    () => parseCsvList(liveTypeFilter),
    [liveTypeFilter],
  );
  const liveEvents = useMemo(
    () => parseCsvList(liveEventFilter),
    [liveEventFilter],
  );
  const {
    event,
    connected,
    enabled,
    lastEventId,
    lastEventAt,
    lastErrorAt,
    errorCount,
  } = useFilteredEvents({ types: liveTypes, events: liveEvents });
  const [entries, setEntries] = useState<LogEntry[]>([]);
  const [paused, setPaused] = useState(false);
  const [includeTicks, setIncludeTicks] = useState(false);
  const [includeReady, setIncludeReady] = useState(true);
  const [typeFilter, setTypeFilter] = useState("");
  const [eventFilter, setEventFilter] = useState("");
  const [searchFilter, setSearchFilter] = useState("");
  const [historyRows, setHistoryRows] = useState<EventHistoryRow[]>([]);
  const [historyMeta, setHistoryMeta] = useState<PageMeta | null>(null);
  const [historyBusy, setHistoryBusy] = useState(false);
  const [historyError, setHistoryError] = useState<string | null>(null);
  const [historyLimit, setHistoryLimit] = useState("200");
  const [historyOffset, setHistoryOffset] = useState("0");
  const [historyAfterId, setHistoryAfterId] = useState("");
  const [historyTypeFilter, setHistoryTypeFilter] = useState("");
  const [historyEventFilter, setHistoryEventFilter] = useState("");
  const [historyAgentFilter, setHistoryAgentFilter] = useState("");
  const [historySince, setHistorySince] = useState("");
  const [historyUntil, setHistoryUntil] = useState("");
  const [exportFormat, setExportFormat] = useState("csv");
  const [stats, setStats] = useState<EventStatsRes | null>(null);
  const [statsError, setStatsError] = useState<string | null>(null);
  const [retention, setRetention] = useState<RetentionRes | null>(null);
  const [retentionError, setRetentionError] = useState<string | null>(null);
  const [retentionBusy, setRetentionBusy] = useState(false);
  const [retentionOverrideRows, setRetentionOverrideRows] = useState("");
  const [retentionOverrideDays, setRetentionOverrideDays] = useState("");
  const [retentionResult, setRetentionResult] =
    useState<Record<string, unknown> | null>(null);

  useEffect(() => {
    if (!event || paused) return;
    if (!includeTicks && event.type === "tick") return;
    if (!includeReady && event.type === "ready") return;
    setEntries((prev) => {
      const next = [...prev, event];
      if (next.length > MAX_ENTRIES) {
        next.splice(0, next.length - MAX_ENTRIES);
      }
      return next;
    });
  }, [event, paused, includeTicks, includeReady]);

  useEffect(() => {
    let active = true;
    const fetchStats = async () => {
      try {
        const res = await api<EventStatsRes>("/v1/events/stats?include_clients=true", {
          method: "GET",
        });
        if (!active) return;
        setStats(res);
        setStatsError(null);
      } catch (e) {
        if (!active) return;
        setStatsError(e instanceof Error ? e.message : String(e));
      }
    };
    fetchStats();
    const id = window.setInterval(fetchStats, 10000);
    return () => {
      active = false;
      window.clearInterval(id);
    };
  }, []);

  const fetchRetention = async () => {
    setRetentionError(null);
    try {
      const res = await api<RetentionRes>("/v1/events/retention", {
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
          ? `/v1/events/retention?${params.toString()}`
          : "/v1/events/retention";
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

  useEffect(() => {
    void fetchRetention();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const filtered = useMemo(() => {
    const typeNeedle = typeFilter.trim().toLowerCase();
    const eventNeedle = eventFilter.trim().toLowerCase();
    const searchNeedle = searchFilter.trim().toLowerCase();
    return entries.filter((entry) => {
      if (typeNeedle && !entry.type.toLowerCase().includes(typeNeedle)) {
        return false;
      }
      const kind = eventKind(entry.data).toLowerCase();
      if (eventNeedle && !kind.includes(eventNeedle)) return false;
      if (searchNeedle) {
        const payload = formatPayload(entry.data).toLowerCase();
        if (!payload.includes(searchNeedle)) return false;
      }
      return true;
    });
  }, [entries, typeFilter, eventFilter, searchFilter]);

  const buildHistoryQuery = (
    lim: number,
    opts?: { offset?: number; afterId?: number | null },
  ) => {
    const q = new URLSearchParams();
    q.set("limit", String(lim));
    const afterOverride = opts?.afterId ?? null;
    const afterIdVal = afterOverride ?? parseAfterId(historyAfterId);
    const parsed = parseInt(historyOffset || "0", 10);
    const fallbackOffset = Number.isFinite(parsed) ? parsed : 0;
    const off = afterIdVal == null ? opts?.offset ?? fallbackOffset : 0;
    if (afterIdVal != null) {
      q.set("after_id", String(afterIdVal));
    } else if (off > 0) {
      q.set("offset", String(off));
    }
    if (historyTypeFilter.trim()) {
      q.set("event_type", historyTypeFilter.trim());
    }
    if (historyEventFilter.trim()) {
      q.set("event", historyEventFilter.trim());
    }
    if (historyAgentFilter.trim()) {
      q.set("agent_id", historyAgentFilter.trim());
    }
    const since = toEpochSeconds(historySince);
    if (since) q.set("since", since);
    const until = toEpochSeconds(historyUntil);
    if (until) q.set("until", until);
    return { q, off, afterId: afterIdVal };
  };

  const refreshHistory = async (opts?: { offset?: number; afterId?: number }) => {
    setHistoryBusy(true);
    setHistoryError(null);
    try {
      const lim = Math.min(parseInt(historyLimit || "200", 10) || 200, 2000);
      const { q, off, afterId } = buildHistoryQuery(lim, {
        offset: opts?.offset,
        afterId: opts?.afterId,
      });
      if (opts?.offset != null) setHistoryOffset(String(off));
      if (opts?.afterId != null) setHistoryAfterId(String(opts.afterId));
      const res = await api<EventHistoryRes>(
        `/v1/events/history?${q.toString()}`,
        { method: "GET" },
      );
      setHistoryRows(res.events || []);
      setHistoryMeta({
        count: res.count ?? res.events?.length ?? 0,
        limit: res.limit ?? lim,
        offset: res.offset ?? off,
        next_offset: res.next_offset ?? null,
        after_id: res.after_id ?? afterId ?? null,
        next_after_id: res.next_after_id ?? null,
      });
    } catch (e) {
      setHistoryError(e instanceof Error ? e.message : String(e));
    } finally {
      setHistoryBusy(false);
    }
  };

  const downloadExport = async () => {
    const lim = Math.min(parseInt(historyLimit || "200", 10) || 200, 20000);
    const { q } = buildHistoryQuery(lim);
    q.set("format", exportFormat);
    const filename =
      exportFormat === "json"
        ? "event_history.json"
        : exportFormat === "ndjson" || exportFormat === "jsonl"
          ? "event_history.jsonl"
          : "event_history.csv";
    const resp = await fetch(`/v1/events/history/export?${q.toString()}`, {
      method: "GET",
      credentials: "include",
    });
    if (!resp.ok) {
      const msg = await resp.text().catch(() => "Export failed");
      throw new Error(msg);
    }
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
  };

  const handleExport = async () => {
    setHistoryError(null);
    try {
      await downloadExport();
    } catch (e) {
      setHistoryError(e instanceof Error ? e.message : String(e));
    }
  };

  const statusLabel = enabled
    ? connected
      ? "events connected"
      : "events disconnected"
    : "events disabled";
  const retentionDrift = retention?.drift?.drift;
  const isAdmin = (user?.role || "") === "admin";

  const liveTypeLabel = liveTypes.length
    ? `types ${liveTypes.join(", ")}`
    : "types all";
  const liveEventLabel = liveEvents.length
    ? `event ${liveEvents.join(", ")}`
    : "event all";

  const historyOffsetVal =
    historyMeta?.offset ?? (parseInt(historyOffset || "0", 10) || 0);
  const historyLimitVal =
    historyMeta?.limit ?? (parseInt(historyLimit || "200", 10) || 200);
  const historyPrevOffset = Math.max(0, historyOffsetVal - historyLimitVal);
  const historyNextOffset = historyMeta?.next_offset ?? null;
  const historyAfterVal = parseAfterId(historyAfterId);
  const historyNextAfter = historyMeta?.next_after_id ?? null;
  const usingAfter = historyAfterVal != null;

  const copyFiltered = async () => {
    try {
      const payload = JSON.stringify(filtered, null, 2);
      await navigator.clipboard.writeText(payload);
    } catch {
      // ignore
    }
  };

  const exportLiveBuffer = () => {
    try {
      const payload = {
        exported_at: new Date().toISOString(),
        count: filtered.length,
        filters: {
          live_types: liveTypes,
          live_events: liveEvents,
          type_filter: typeFilter.trim() || null,
          event_filter: eventFilter.trim() || null,
          search_filter: searchFilter.trim() || null,
          include_ticks: includeTicks,
          include_ready: includeReady,
        },
        events: filtered,
      };
      const blob = new Blob([JSON.stringify(payload, null, 2)], {
        type: "application/json",
      });
      const stamp = new Date().toISOString().replace(/[:.]/g, "-");
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = `events_live_buffer_${stamp}.json`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    } catch {
      // ignore
    }
  };

  return (
    <Stack spacing={2}>
      <Card>
        <CardContent>
          <Stack direction="row" spacing={1} sx={{ alignItems: "center" }}>
            <EventNoteIcon color="action" />
            <Typography variant="h6">Events</Typography>
            <Chip
              size="small"
              label={statusLabel}
              color={enabled ? (connected ? "success" : "warning") : "default"}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`${filtered.length}/${entries.length} buffered`}
              variant="outlined"
            />
          </Stack>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            Real-time SSE diagnostics for troubleshooting integrations and
            automation workflows.
          </Typography>
          <Stack
            direction="row"
            spacing={1}
            sx={{ mt: 1, flexWrap: "wrap", alignItems: "center" }}
          >
            <Chip
              size="small"
              label={`last id ${lastEventId ?? "—"}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`last event ${fmtTsMs(lastEventAt)}`}
              variant="outlined"
            />
            <Chip size="small" label={`errors ${errorCount}`} variant="outlined" />
            <Chip
              size="small"
              label={`last error ${fmtTsMs(lastErrorAt)}`}
              variant="outlined"
            />
            <Chip size="small" label={liveTypeLabel} variant="outlined" />
            <Chip size="small" label={liveEventLabel} variant="outlined" />
          </Stack>
        </CardContent>
        <CardActions sx={{ flexWrap: "wrap", gap: 1 }}>
          <Button
            startIcon={<RefreshIcon />}
            onClick={() => setEntries([])}
          >
            Clear
          </Button>
          <Button onClick={() => void copyFiltered()}>
            Copy filtered
          </Button>
          <Button onClick={() => exportLiveBuffer()} disabled={filtered.length === 0}>
            Export live buffer
          </Button>
          <FormControlLabel
            control={
              <Switch
                checked={paused}
                onChange={(e) => setPaused(e.target.checked)}
              />
            }
            label={paused ? "Paused" : "Live"}
          />
          <FormControlLabel
            control={
              <Switch
                checked={includeTicks}
                onChange={(e) => setIncludeTicks(e.target.checked)}
              />
            }
            label="Include ticks"
          />
          <FormControlLabel
            control={
              <Switch
                checked={includeReady}
                onChange={(e) => setIncludeReady(e.target.checked)}
              />
            }
            label="Include ready"
          />
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">SSE diagnostics</Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            Live bus + disk spool health for troubleshooting reconnects.
          </Typography>
          <Stack direction="row" spacing={1} sx={{ mt: 2, flexWrap: "wrap" }}>
            <Chip
              size="small"
              label={`clients ${stats?.bus?.subscribers ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`history ${stats?.bus?.history ?? 0}/${stats?.bus?.max_history ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`missed ${stats?.bus?.missed_total ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`dropped ${stats?.bus?.dropped_total ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`spool events ${stats?.spool?.queued_events ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`spool bytes ${stats?.spool?.queued_bytes ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`spool drops ${stats?.spool?.dropped ?? 0}`}
              variant="outlined"
            />
          </Stack>
          {statsError ? (
            <Alert severity="warning" sx={{ mt: 2 }}>
              {statsError}
            </Alert>
          ) : null}
          {stats?.bus?.clients?.length ? (
            <Stack spacing={1} sx={{ mt: 2 }}>
              <Typography variant="subtitle2">Clients</Typography>
              {stats.bus.clients.map((c) => (
                <Stack
                  key={`client-${c.id}`}
                  spacing={0.25}
                  sx={{
                    border: "1px solid rgba(255,255,255,0.12)",
                    borderRadius: 1,
                    p: 1,
                  }}
                >
                  <Typography variant="body2">
                    client <code>{c.id}</code> · last_id{" "}
                    <code>{c.last_event_id ?? "—"}</code> · missed{" "}
                    <code>{c.missed ?? 0}</code> · dropped{" "}
                    <code>{c.dropped ?? 0}</code>
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    connected <code>{fmtTs(c.connected_at)}</code> · last_seen{" "}
                    <code>{fmtTs(c.last_seen_at ?? undefined)}</code> · queue{" "}
                    <code>{c.queue_size ?? 0}</code>/
                    <code>{c.queue_max ?? 0}</code>
                  </Typography>
                </Stack>
              ))}
            </Stack>
          ) : null}
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Event Retention</Typography>
          <Typography variant="body2" color="text.secondary">
            Retention status for <code>event_log</code>.
          </Typography>
          {retentionDrift ? (
            <Alert severity="warning" sx={{ mt: 2 }}>
              Event history exceeds retention targets. Run cleanup or adjust{" "}
              <code>EVENTS_HISTORY_MAX_ROWS</code>/<code>EVENTS_HISTORY_MAX_DAYS</code>.
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
          <Typography variant="h6">Live SSE filters</Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            Filters apply server-side to reduce the stream volume.
          </Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <TextField
              label="Types (comma-separated)"
              value={liveTypeFilter}
              onChange={(e) => setLiveTypeFilter(e.target.value)}
              placeholder="jobs, fleet, scheduler"
              size="small"
            />
            <TextField
              label="Event kinds (comma-separated)"
              value={liveEventFilter}
              onChange={(e) => setLiveEventFilter(e.target.value)}
              placeholder="created, updated, status"
              size="small"
            />
          </Stack>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Local filters</Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            Client-side filters for the buffered event list below.
          </Typography>
          <Stack spacing={2}>
            <TextField
              label="Type filter"
              value={typeFilter}
              onChange={(e) => setTypeFilter(e.target.value)}
              placeholder="jobs, fleet, scheduler"
              size="small"
            />
            <TextField
              label="Event filter"
              value={eventFilter}
              onChange={(e) => setEventFilter(e.target.value)}
              placeholder="event field in payload"
              size="small"
            />
            <TextField
              label="Search payload"
              value={searchFilter}
              onChange={(e) => setSearchFilter(e.target.value)}
              placeholder="match JSON payload"
              size="small"
            />
          </Stack>
        </CardContent>
      </Card>

      {filtered.length === 0 ? (
        <Alert severity="info">No events captured yet.</Alert>
      ) : null}

      {filtered.map((entry, idx) => {
        const payload = formatPayload(entry.data);
        const kind = eventKind(entry.data);
        return (
          <Card key={`${entry.id ?? "noid"}-${idx}`} variant="outlined">
            <CardContent>
              <Stack
                direction="row"
                spacing={1}
                sx={{ alignItems: "center", flexWrap: "wrap" }}
              >
                <Chip size="small" label={entry.type} />
                {kind ? <Chip size="small" label={kind} /> : null}
                {entry.id != null ? (
                  <Chip size="small" label={`id ${entry.id}`} />
                ) : null}
                <Typography variant="caption" color="text.secondary">
                  {fmtTs(entry.ts)}
                </Typography>
              </Stack>
              {payload ? (
                <Box
                  component="pre"
                  sx={{
                    mt: 1,
                    mb: 0,
                    fontSize: 12,
                    whiteSpace: "pre-wrap",
                    wordBreak: "break-word",
                    maxHeight: 240,
                    overflow: "auto",
                  }}
                >
                  {payload}
                </Box>
              ) : (
                <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                  No payload.
                </Typography>
              )}
            </CardContent>
          </Card>
        );
      })}

      <Card>
        <CardContent>
          <Stack direction="row" spacing={1} sx={{ alignItems: "center" }}>
            <EventNoteIcon color="action" />
            <Typography variant="h6">Server history</Typography>
            {historyMeta ? (
              <Chip
                size="small"
                label={`showing ${historyMeta.count ?? historyRows.length}`}
                variant="outlined"
              />
            ) : null}
          </Stack>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            Persisted SSE events stored in SQL for deeper diagnostics.
          </Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <TextField
              label="Limit"
              type="number"
              value={historyLimit}
              onChange={(e) => setHistoryLimit(e.target.value)}
              size="small"
            />
            <TextField
              label="Offset"
              type="number"
              value={historyOffset}
              onChange={(e) => setHistoryOffset(e.target.value)}
              disabled={usingAfter}
              helperText={
                usingAfter ? "Disabled when using after_id cursor." : undefined
              }
              size="small"
            />
            <TextField
              label="After ID (cursor)"
              type="number"
              value={historyAfterId}
              onChange={(e) => setHistoryAfterId(e.target.value)}
              helperText="Set to enable cursor paging; disables offset/prev."
              size="small"
            />
            <TextField
              label="Type"
              value={historyTypeFilter}
              onChange={(e) => setHistoryTypeFilter(e.target.value)}
              placeholder="jobs, fleet, scheduler"
              size="small"
            />
            <TextField
              label="Event"
              value={historyEventFilter}
              onChange={(e) => setHistoryEventFilter(e.target.value)}
              placeholder="created, updated, status"
              size="small"
            />
            <TextField
              label="Agent ID"
              value={historyAgentFilter}
              onChange={(e) => setHistoryAgentFilter(e.target.value)}
              placeholder="local agent_id"
              size="small"
            />
            <TextField
              label="Since"
              type="datetime-local"
              value={historySince}
              onChange={(e) => setHistorySince(e.target.value)}
              size="small"
              InputLabelProps={{ shrink: true }}
            />
            <TextField
              label="Until"
              type="datetime-local"
              value={historyUntil}
              onChange={(e) => setHistoryUntil(e.target.value)}
              size="small"
              InputLabelProps={{ shrink: true }}
            />
            <TextField
              label="Export format"
              select
              value={exportFormat}
              onChange={(e) => setExportFormat(e.target.value)}
              size="small"
            >
              <MenuItem value="csv">CSV</MenuItem>
              <MenuItem value="json">JSON</MenuItem>
              <MenuItem value="ndjson">NDJSON</MenuItem>
            </TextField>
          </Stack>
        </CardContent>
        <CardActions sx={{ flexWrap: "wrap", gap: 1 }}>
          <Button
            startIcon={<RefreshIcon />}
            onClick={() => refreshHistory()}
            disabled={historyBusy}
          >
            Load history
          </Button>
          <Button onClick={handleExport} disabled={historyBusy}>
            Export
          </Button>
          <Button
            disabled={historyBusy || usingAfter || historyOffsetVal <= 0}
            onClick={() => refreshHistory({ offset: historyPrevOffset })}
          >
            Prev
          </Button>
          <Button
            disabled={
              historyBusy ||
              (usingAfter ? historyNextAfter == null : historyNextOffset == null)
            }
            onClick={() =>
              usingAfter
                ? refreshHistory({ afterId: historyNextAfter ?? historyAfterVal ?? 0 })
                : refreshHistory({ offset: historyNextOffset ?? historyOffsetVal })
            }
          >
            Next
          </Button>
        </CardActions>
      </Card>

      {historyError ? <Alert severity="error">{historyError}</Alert> : null}

      {historyRows.length === 0 ? (
        <Alert severity="info">No server history loaded yet.</Alert>
      ) : null}

      {historyRows.map((row, idx) => {
        const payload = formatPayload(row.payload);
        return (
          <Card key={`history-${row.id ?? idx}`} variant="outlined">
            <CardContent>
              <Stack
                direction="row"
                spacing={1}
                sx={{ alignItems: "center", flexWrap: "wrap" }}
              >
                <Chip size="small" label={row.event_type} />
                {row.agent_id ? (
                  <Chip size="small" label={row.agent_id} />
                ) : null}
                {row.event ? <Chip size="small" label={row.event} /> : null}
                {row.id != null ? (
                  <Chip size="small" label={`id ${row.id}`} />
                ) : null}
                <Typography variant="caption" color="text.secondary">
                  {fmtTs(row.created_at)}
                </Typography>
              </Stack>
              {payload ? (
                <Box
                  component="pre"
                  sx={{
                    mt: 1,
                    mb: 0,
                    fontSize: 12,
                    whiteSpace: "pre-wrap",
                    wordBreak: "break-word",
                    maxHeight: 240,
                    overflow: "auto",
                  }}
                >
                  {payload}
                </Box>
              ) : (
                <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                  No payload.
                </Typography>
              )}
            </CardContent>
          </Card>
        );
      })}
    </Stack>
  );
}
