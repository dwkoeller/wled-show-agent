import EqualizerIcon from "@mui/icons-material/Equalizer";
import {
  Alert,
  Box,
  Button,
  Card,
  CardActions,
  CardContent,
  Chip,
  Stack,
  TextField,
  Typography,
} from "@mui/material";
import React, { useEffect, useMemo, useState } from "react";
import { api } from "../../api";
import { useAuth } from "../../auth";

type MetricsRes = {
  ok: boolean;
  uptime_s?: number;
  jobs?: { count?: number };
  scheduler?: { ok?: boolean; running?: boolean; in_window?: boolean };
  outbound?: {
    failures_total?: number;
    retries_total?: number;
    by_target_kind?: Record<
      string,
      { failures?: number; retries?: number; avg_latency_s?: number }
    >;
  };
  events?: {
    spool?: {
      dropped?: number;
      queued_events?: number;
      queued_bytes?: number;
    };
  };
};

type MetricsSample = {
  id?: number;
  created_at: number;
  jobs_count?: number;
  scheduler_ok?: boolean;
  scheduler_running?: boolean;
  scheduler_in_window?: boolean;
  outbound_failures?: number;
  outbound_retries?: number;
  spool_dropped?: number;
  spool_queued_events?: number;
  spool_queued_bytes?: number;
};

type MetricsHistoryRes = {
  ok: boolean;
  samples: MetricsSample[];
  count?: number;
  limit?: number;
  offset?: number;
  next_offset?: number | null;
};

type MetricsRetentionStatus = {
  ok?: boolean;
  stats?: {
    count?: number;
    oldest?: number | null;
    newest?: number | null;
  };
  settings?: {
    interval_s?: number;
    maintenance_interval_s?: number;
    max_rows?: number;
    max_days?: number;
  };
  drift?: {
    excess_rows?: number;
    excess_age_s?: number;
    oldest_age_s?: number | null;
    drift?: boolean;
  };
  last_retention?: {
    at?: number;
    result?: Record<string, unknown>;
  } | null;
};

type SeriesPoint = { ts: number; value: number };

const MAX_POINTS = 60;

function appendSeries(prev: SeriesPoint[], value: number): SeriesPoint[] {
  const next = [...prev, { ts: Date.now(), value }];
  if (next.length > MAX_POINTS) next.splice(0, next.length - MAX_POINTS);
  return next;
}

function Sparkline(props: { data: SeriesPoint[]; color?: string }) {
  const { data, color } = props;
  const width = 180;
  const height = 48;
  if (!data.length) {
    return (
      <Box
        sx={{
          width: "100%",
          height,
          border: "1px solid rgba(255,255,255,0.12)",
          borderRadius: 1,
        }}
      />
    );
  }
  const values = data.map((d) => d.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const points = values.map((v, i) => {
    const x = (i / Math.max(1, values.length - 1)) * (width - 4) + 2;
    const y = height - 2 - ((v - min) / range) * (height - 4);
    return [x, y];
  });
  const d = points
    .map(([x, y], i) => `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`)
    .join(" ");
  return (
    <Box
      component="svg"
      viewBox={`0 0 ${width} ${height}`}
      sx={{
        width: "100%",
        height,
        border: "1px solid rgba(255,255,255,0.12)",
        borderRadius: 1,
      }}
    >
      <path
        d={d}
        stroke={color ?? "rgba(144,202,249,0.9)"}
        strokeWidth="2"
        fill="none"
      />
    </Box>
  );
}

function fmtTs(tsSeconds: number | null | undefined): string {
  if (!tsSeconds) return "—";
  try {
    return new Date(tsSeconds * 1000).toLocaleString();
  } catch {
    return String(tsSeconds);
  }
}

function toEpochSeconds(value: string): string | null {
  if (!value) return null;
  const ts = Date.parse(value);
  if (Number.isNaN(ts)) return null;
  return String(Math.floor(ts / 1000));
}

export function MetricsTools() {
  const { user } = useAuth();
  const [metrics, setMetrics] = useState<MetricsRes | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [series, setSeries] = useState<{
    jobs: SeriesPoint[];
    scheduler: SeriesPoint[];
    outbound: SeriesPoint[];
    spool: SeriesPoint[];
  }>({
    jobs: [],
    scheduler: [],
    outbound: [],
    spool: [],
  });
  const [historySamples, setHistorySamples] = useState<MetricsSample[]>([]);
  const [historyError, setHistoryError] = useState<string | null>(null);
  const [historyBusy, setHistoryBusy] = useState(false);
  const [historyWindowHours, setHistoryWindowHours] = useState("6");
  const [historySince, setHistorySince] = useState("");
  const [historyUntil, setHistoryUntil] = useState("");
  const [historyLimit, setHistoryLimit] = useState("300");
  const [retentionStatus, setRetentionStatus] =
    useState<MetricsRetentionStatus | null>(null);
  const [retentionError, setRetentionError] = useState<string | null>(null);
  const [retentionBusy, setRetentionBusy] = useState(false);
  const [retentionOverrideRows, setRetentionOverrideRows] = useState("");
  const [retentionOverrideDays, setRetentionOverrideDays] = useState("");
  const [retentionResult, setRetentionResult] =
    useState<Record<string, unknown> | null>(null);

  useEffect(() => {
    let active = true;
    const fetchMetrics = async () => {
      try {
        const res = await api<MetricsRes>("/v1/metrics", { method: "GET" });
        if (!active) return;
        setMetrics(res);
        setError(null);
        const jobsCount = Number(res.jobs?.count ?? 0);
        const schedulerRunning = res.scheduler?.running ? 1 : 0;
        const outboundFailures = Number(res.outbound?.failures_total ?? 0);
        const spoolDrops = Number(res.events?.spool?.dropped ?? 0);
        setSeries((prev) => ({
          jobs: appendSeries(prev.jobs, jobsCount),
          scheduler: appendSeries(prev.scheduler, schedulerRunning),
          outbound: appendSeries(prev.outbound, outboundFailures),
          spool: appendSeries(prev.spool, spoolDrops),
        }));
      } catch (e) {
        if (!active) return;
        setError(e instanceof Error ? e.message : String(e));
      }
    };
    fetchMetrics();
    const id = window.setInterval(fetchMetrics, 5000);
    return () => {
      active = false;
      window.clearInterval(id);
    };
  }, []);

  const fetchHistory = async () => {
    setHistoryBusy(true);
    setHistoryError(null);
    try {
      const lim = Math.max(1, Math.min(5000, parseInt(historyLimit, 10) || 300));
      const hours = parseFloat(historyWindowHours || "");
      const q = new URLSearchParams();
      q.set("limit", String(lim));
      q.set("order", "asc");
      const sinceOverride = toEpochSeconds(historySince);
      const untilOverride = toEpochSeconds(historyUntil);
      if (sinceOverride) q.set("since", sinceOverride);
      if (untilOverride) q.set("until", untilOverride);
      if (!sinceOverride && !untilOverride && Number.isFinite(hours) && hours > 0) {
        const since = Math.floor(Date.now() / 1000 - hours * 3600);
        q.set("since", String(since));
      }
      const res = await api<MetricsHistoryRes>(`/v1/metrics/history?${q}`, {
        method: "GET",
      });
      setHistorySamples(res.samples ?? []);
    } catch (e) {
      setHistoryError(e instanceof Error ? e.message : String(e));
    } finally {
      setHistoryBusy(false);
    }
  };

  const fetchRetention = async () => {
    setRetentionError(null);
    try {
      const res = await api<MetricsRetentionStatus>(
        "/v1/metrics/history/retention",
        { method: "GET" },
      );
      setRetentionStatus(res);
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
          ? `/v1/metrics/history/retention?${params.toString()}`
          : "/v1/metrics/history/retention";
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

  const exportHistory = async (format: "csv" | "json") => {
    try {
      const lim = Math.max(1, Math.min(20000, parseInt(historyLimit, 10) || 300));
      const hours = parseFloat(historyWindowHours || "");
      const q = new URLSearchParams();
      q.set("format", format);
      q.set("limit", String(lim));
      q.set("order", "asc");
      const sinceOverride = toEpochSeconds(historySince);
      const untilOverride = toEpochSeconds(historyUntil);
      if (sinceOverride) q.set("since", sinceOverride);
      if (untilOverride) q.set("until", untilOverride);
      if (!sinceOverride && !untilOverride && Number.isFinite(hours) && hours > 0) {
        const since = Math.floor(Date.now() / 1000 - hours * 3600);
        q.set("since", String(since));
      }
      const resp = await fetch(`/v1/metrics/history/export?${q.toString()}`, {
        method: "GET",
        credentials: "include",
      });
      if (!resp.ok) {
        const contentType = resp.headers.get("content-type") ?? "";
        if (contentType.includes("application/json")) {
          const payload = (await resp.json().catch(() => null)) as {
            detail?: string;
            error?: string;
          } | null;
          throw new Error(payload?.detail || payload?.error || `HTTP ${resp.status}`);
        }
        const text = await resp.text();
        throw new Error(text || `HTTP ${resp.status}`);
      }
      const blob = await resp.blob();
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download =
        format === "json" ? "metrics_history.json" : "metrics_history.csv";
      anchor.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      setHistoryError(e instanceof Error ? e.message : String(e));
    }
  };

  useEffect(() => {
    void fetchHistory();
    void fetchRetention();
  }, []);

  const outboundKinds = useMemo(() => {
    const byKind = metrics?.outbound?.by_target_kind;
    if (!byKind) return [];
    return Object.entries(byKind);
  }, [metrics]);

  const historySeries = useMemo(() => {
    const rows = historySamples ?? [];
    const toSeries = (fn: (row: MetricsSample) => number) =>
      rows.map((row) => ({
        ts: row.created_at * 1000,
        value: fn(row),
      }));
    return {
      jobs: toSeries((row) => Number(row.jobs_count ?? 0)),
      scheduler: toSeries((row) => (row.scheduler_running ? 1 : 0)),
      outbound: toSeries((row) => Number(row.outbound_failures ?? 0)),
      spool: toSeries((row) => Number(row.spool_dropped ?? 0)),
    };
  }, [historySamples]);

  const lastSample = historySamples.length
    ? historySamples[historySamples.length - 1]
    : null;
  const retentionDrift = retentionStatus?.drift?.drift;
  const isAdmin = (user?.role || "") === "admin";

  return (
    <Stack spacing={2}>
      <Card>
        <CardContent>
          <Stack direction="row" spacing={1} sx={{ alignItems: "center" }}>
            <EqualizerIcon />
            <Typography variant="h6">Metrics</Typography>
          </Stack>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            Live snapshots from <code>/v1/metrics</code> (updated every 5s).
          </Typography>
          {error ? (
            <Alert severity="warning" sx={{ mt: 2 }}>
              {error}
            </Alert>
          ) : null}
        </CardContent>
      </Card>

      <Box
        sx={{
          display: "grid",
          gridTemplateColumns: { xs: "1fr", sm: "1fr 1fr" },
          gap: 2,
        }}
      >
        <Card>
          <CardContent>
            <Typography variant="subtitle1">Jobs</Typography>
            <Typography variant="h5">{metrics?.jobs?.count ?? 0}</Typography>
            <Sparkline data={series.jobs} />
          </CardContent>
        </Card>

        <Card>
          <CardContent>
            <Typography variant="subtitle1">Scheduler</Typography>
            <Typography variant="h5">
              {metrics?.scheduler?.running ? "running" : "stopped"}
            </Typography>
            <Stack direction="row" spacing={1} sx={{ mt: 1, flexWrap: "wrap" }}>
              <Chip
                size="small"
                label={`ok ${metrics?.scheduler?.ok ? "yes" : "no"}`}
                variant="outlined"
              />
              <Chip
                size="small"
                label={`in_window ${metrics?.scheduler?.in_window ? "yes" : "no"}`}
                variant="outlined"
              />
            </Stack>
            <Sparkline data={series.scheduler} color="rgba(129,199,132,0.9)" />
          </CardContent>
        </Card>

        <Card>
          <CardContent>
            <Typography variant="subtitle1">Outbound errors</Typography>
            <Typography variant="h5">
              {metrics?.outbound?.failures_total ?? 0}
            </Typography>
            <Stack direction="row" spacing={1} sx={{ mt: 1, flexWrap: "wrap" }}>
              <Chip
                size="small"
                label={`retries ${metrics?.outbound?.retries_total ?? 0}`}
                variant="outlined"
              />
              {outboundKinds.map(([kind, entry]) => (
                <Chip
                  key={kind}
                  size="small"
                  label={`${kind} ${entry?.failures ?? 0}`}
                  variant="outlined"
                />
              ))}
            </Stack>
            <Sparkline data={series.outbound} color="rgba(244,67,54,0.9)" />
          </CardContent>
        </Card>

        <Card>
          <CardContent>
            <Typography variant="subtitle1">Event spool drops</Typography>
            <Typography variant="h5">
              {metrics?.events?.spool?.dropped ?? 0}
            </Typography>
            <Stack direction="row" spacing={1} sx={{ mt: 1, flexWrap: "wrap" }}>
              <Chip
                size="small"
                label={`queued ${metrics?.events?.spool?.queued_events ?? 0}`}
                variant="outlined"
              />
              <Chip
                size="small"
                label={`bytes ${metrics?.events?.spool?.queued_bytes ?? 0}`}
                variant="outlined"
              />
            </Stack>
            <Sparkline data={series.spool} color="rgba(255,193,7,0.9)" />
          </CardContent>
        </Card>
      </Box>

      <Card>
        <CardContent>
          <Stack direction="row" spacing={1} sx={{ alignItems: "center" }}>
            <EqualizerIcon />
            <Typography variant="h6">Metrics Charts</Typography>
          </Stack>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            SQL-backed samples from <code>/v1/metrics/history</code> for charts.
          </Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <TextField
              label="Window hours"
              value={historyWindowHours}
              onChange={(e) => setHistoryWindowHours(e.target.value)}
              helperText="How far back to query when no date range is set."
              disabled={historyBusy}
              inputMode="decimal"
            />
            <TextField
              label="Since"
              type="datetime-local"
              value={historySince}
              onChange={(e) => setHistorySince(e.target.value)}
              helperText="Overrides window when set."
              disabled={historyBusy}
              InputLabelProps={{ shrink: true }}
            />
            <TextField
              label="Until"
              type="datetime-local"
              value={historyUntil}
              onChange={(e) => setHistoryUntil(e.target.value)}
              helperText="Optional upper bound."
              disabled={historyBusy}
              InputLabelProps={{ shrink: true }}
            />
            <TextField
              label="Limit"
              value={historyLimit}
              onChange={(e) => setHistoryLimit(e.target.value)}
              helperText="Max samples to fetch (1-5000)."
              disabled={historyBusy}
              inputMode="numeric"
            />
            <Typography variant="body2" color="text.secondary">
              Samples: <code>{historySamples.length}</code> · Last{" "}
              <code>{fmtTs(lastSample?.created_at ?? null)}</code>
            </Typography>
            {historyError ? (
              <Alert severity="warning">{historyError}</Alert>
            ) : null}
          </Stack>
          <Box
            sx={{
              display: "grid",
              gridTemplateColumns: { xs: "1fr", sm: "1fr 1fr" },
              gap: 2,
              mt: 3,
            }}
          >
            <Card variant="outlined">
              <CardContent>
                <Typography variant="subtitle1">Jobs (history)</Typography>
                <Typography variant="h5">{lastSample?.jobs_count ?? 0}</Typography>
                <Sparkline data={historySeries.jobs} />
              </CardContent>
            </Card>

            <Card variant="outlined">
              <CardContent>
                <Typography variant="subtitle1">Scheduler (history)</Typography>
                <Typography variant="h5">
                  {lastSample?.scheduler_running ? "running" : "stopped"}
                </Typography>
                <Sparkline
                  data={historySeries.scheduler}
                  color="rgba(129,199,132,0.9)"
                />
              </CardContent>
            </Card>

            <Card variant="outlined">
              <CardContent>
                <Typography variant="subtitle1">Outbound failures (history)</Typography>
                <Typography variant="h5">{lastSample?.outbound_failures ?? 0}</Typography>
                <Sparkline data={historySeries.outbound} color="rgba(244,67,54,0.9)" />
              </CardContent>
            </Card>

            <Card variant="outlined">
              <CardContent>
                <Typography variant="subtitle1">Spool drops (history)</Typography>
                <Typography variant="h5">{lastSample?.spool_dropped ?? 0}</Typography>
                <Sparkline data={historySeries.spool} color="rgba(255,193,7,0.9)" />
              </CardContent>
            </Card>
          </Box>
        </CardContent>
        <CardActions>
          <Button onClick={() => void fetchHistory()} disabled={historyBusy}>
            Refresh history
          </Button>
          <Button onClick={() => void exportHistory("csv")} disabled={historyBusy}>
            Export CSV
          </Button>
          <Button onClick={() => void exportHistory("json")} disabled={historyBusy}>
            Export JSON
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Retention</Typography>
          <Typography variant="body2" color="text.secondary">
            Configured sampling/retention for metrics history.
          </Typography>
          {retentionDrift ? (
            <Alert severity="warning" sx={{ mt: 2 }}>
              Metrics history exceeds retention targets. Run cleanup or increase{" "}
              <code>METRICS_HISTORY_MAX_ROWS</code>/<code>METRICS_HISTORY_MAX_DAYS</code>.
            </Alert>
          ) : null}
          {retentionError ? (
            <Alert severity="warning" sx={{ mt: 2 }}>
              {retentionError}
            </Alert>
          ) : null}
          <Stack spacing={1} sx={{ mt: 2 }}>
            <Typography variant="body2">
              Samples: <code>{retentionStatus?.stats?.count ?? 0}</code> · Oldest{" "}
              <code>{fmtTs(retentionStatus?.stats?.oldest ?? null)}</code> · Newest{" "}
              <code>{fmtTs(retentionStatus?.stats?.newest ?? null)}</code>
            </Typography>
            <Typography variant="body2">
              Drift: rows{" "}
              <code>{retentionStatus?.drift?.excess_rows ?? 0}</code> · age{" "}
              <code>
                {retentionStatus?.drift?.excess_age_s
                  ? `${Math.round(retentionStatus.drift.excess_age_s)}s`
                  : "0s"}
              </code>
            </Typography>
            <Typography variant="body2">
              Interval: <code>{retentionStatus?.settings?.interval_s ?? 0}</code>s ·
              Maintenance:{" "}
              <code>
                {retentionStatus?.settings?.maintenance_interval_s ?? 0}
              </code>
              s
            </Typography>
            <Typography variant="body2">
              Max rows: <code>{retentionStatus?.settings?.max_rows ?? 0}</code> ·
              Max days: <code>{retentionStatus?.settings?.max_days ?? 0}</code>
            </Typography>
            <Typography variant="body2">
              Last cleanup:{" "}
              <code>
                {retentionStatus?.last_retention?.at
                  ? fmtTs(retentionStatus.last_retention.at)
                  : "—"}
              </code>
            </Typography>
          </Stack>
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
    </Stack>
  );
}
