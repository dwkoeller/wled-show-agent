import CancelIcon from "@mui/icons-material/Cancel";
import RefreshIcon from "@mui/icons-material/Refresh";
import {
  Alert,
  Box,
  Button,
  Card,
  CardActions,
  CardContent,
  Chip,
  Collapse,
  FormControl,
  InputLabel,
  LinearProgress,
  MenuItem,
  Select,
  Stack,
  TextField,
  Typography,
} from "@mui/material";
import React, { useEffect, useMemo, useState } from "react";
import { api } from "../api";
import { useAuth } from "../auth";
import { useEventRefresh } from "../hooks/useEventRefresh";
import { useServerEvents } from "../hooks/useServerEvents";

type Job = {
  id: string;
  kind: string;
  status: string;
  created_at: number;
  started_at: number | null;
  finished_at: number | null;
  progress: {
    current: number | null;
    total: number | null;
    message: string | null;
  };
  result: any;
  error: string | null;
  logs: string[];
  cancel_requested: boolean;
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

function fmtTs(ts: number | null): string {
  if (!ts) return "—";
  try {
    return new Date(ts * 1000).toLocaleString();
  } catch {
    return String(ts);
  }
}

function progressPct(job: Job): number | null {
  const c = job.progress?.current;
  const t = job.progress?.total;
  if (c == null || t == null || t <= 0) return null;
  return Math.max(0, Math.min(100, (c / t) * 100));
}

function outFileFromResult(job: Job): string | null {
  const r = job.result;
  if (!r) return null;
  if (typeof r === "object" && typeof r.out_file === "string")
    return r.out_file;
  if (
    typeof r === "object" &&
    typeof r.summary === "object" &&
    typeof r.summary.file === "string"
  )
    return `looks/${r.summary.file}`;
  return null;
}

export function JobsPage() {
  const { user } = useAuth();
  const [jobsById, setJobsById] = useState<Record<string, Job>>({});
  const [error, setError] = useState<string | null>(null);
  const [retention, setRetention] = useState<RetentionRes | null>(null);
  const [retentionError, setRetentionError] = useState<string | null>(null);
  const [retentionBusy, setRetentionBusy] = useState(false);
  const [retentionOverrideRows, setRetentionOverrideRows] = useState("");
  const [retentionOverrideDays, setRetentionOverrideDays] = useState("");
  const [retentionResult, setRetentionResult] =
    useState<Record<string, unknown> | null>(null);
  const [jobLimit, setJobLimit] = useState("200");
  const [statusFilter, setStatusFilter] = useState("all");
  const [kindFilter, setKindFilter] = useState("");
  const [searchFilter, setSearchFilter] = useState("");
  const [expandedJobs, setExpandedJobs] = useState<Record<string, boolean>>({});
  const { connected: eventsConnected, enabled: eventsEnabled } = useServerEvents();
  const isAdmin = (user?.role || "") === "admin";

  const jobs = useMemo(() => {
    const arr = Object.values(jobsById);
    arr.sort((a, b) => (b.created_at ?? 0) - (a.created_at ?? 0));
    return arr;
  }, [jobsById]);

  const filteredJobs = useMemo(() => {
    const statusNeedle = statusFilter.trim().toLowerCase();
    const kindNeedle = kindFilter.trim().toLowerCase();
    const searchNeedle = searchFilter.trim().toLowerCase();
    return jobs.filter((job) => {
      if (statusNeedle && statusNeedle !== "all") {
        if (job.status?.toLowerCase() !== statusNeedle) return false;
      }
      if (kindNeedle && !job.kind.toLowerCase().includes(kindNeedle)) {
        return false;
      }
      if (searchNeedle) {
        const hay = [
          job.id,
          job.kind,
          job.status,
          job.error ?? "",
          job.progress?.message ?? "",
          ...(job.logs || []),
        ]
          .join(" ")
          .toLowerCase();
        if (!hay.includes(searchNeedle)) return false;
      }
      return true;
    });
  }, [jobs, statusFilter, kindFilter, searchFilter]);

  const refresh = async () => {
    setError(null);
    try {
      const lim = Math.max(1, Math.min(1000, parseInt(jobLimit, 10) || 100));
      const res = await api<{ ok: boolean; jobs: Job[] }>(
        `/v1/jobs?limit=${lim}`,
        { method: "GET" },
      );
      const m: Record<string, Job> = {};
      for (const j of res.jobs || []) m[j.id] = j;
      setJobsById(m);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const fetchRetention = async () => {
    setRetentionError(null);
    try {
      const res = await api<RetentionRes>("/v1/jobs/retention", {
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
          ? `/v1/jobs/retention?${params.toString()}`
          : "/v1/jobs/retention";
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

  const cancel = async (jobId: string) => {
    try {
      await api(`/v1/jobs/${encodeURIComponent(jobId)}/cancel`, {
        method: "POST",
        json: {},
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const toggleJobDetails = (jobId: string) => {
    setExpandedJobs((prev) => ({ ...prev, [jobId]: !prev[jobId] }));
  };

  const exportJobs = (format: "json" | "csv") => {
    try {
      if (format === "json") {
        const payload = JSON.stringify({ jobs: filteredJobs }, null, 2);
        const blob = new Blob([payload], { type: "application/json" });
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = "jobs.json";
        link.click();
        URL.revokeObjectURL(url);
        return;
      }
      const header = [
        "id",
        "kind",
        "status",
        "created_at",
        "started_at",
        "finished_at",
        "progress_current",
        "progress_total",
        "progress_message",
        "error",
        "out_file",
      ];
      const rows = filteredJobs.map((job) => {
        const outFile = outFileFromResult(job);
        return [
          job.id,
          job.kind,
          job.status,
          job.created_at,
          job.started_at ?? "",
          job.finished_at ?? "",
          job.progress?.current ?? "",
          job.progress?.total ?? "",
          job.progress?.message ?? "",
          job.error ?? "",
          outFile ?? "",
        ];
      });
      const esc = (val: unknown) => {
        const s = String(val ?? "");
        if (s.includes(",") || s.includes("\"") || s.includes("\n")) {
          return `"${s.replace(/\"/g, "\"\"")}"`;
        }
        return s;
      };
      const csv = [header.map(esc).join(",")]
        .concat(rows.map((row) => row.map(esc).join(",")))
        .join("\n");
      const blob = new Blob([csv], { type: "text/csv" });
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = "jobs.csv";
      link.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  useEffect(() => {
    void refresh();
    void fetchRetention();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEventRefresh({
    types: ["jobs", "tick"],
    refresh,
    minIntervalMs: 2000,
    fallbackIntervalMs: 5000,
  });

  return (
    <Stack spacing={2}>
      {error ? <Alert severity="error">{error}</Alert> : null}
      <Card>
        <CardContent>
          <Stack direction="row" spacing={1} sx={{ alignItems: "center" }}>
            <Typography variant="h6">Jobs</Typography>
            <Chip
              size="small"
              label={
                eventsEnabled
                  ? eventsConnected
                    ? "events connected"
                    : "events disconnected"
                  : "events disabled"
              }
              color={
                eventsEnabled
                  ? eventsConnected
                    ? "success"
                    : "warning"
                  : "default"
              }
              variant="outlined"
            />
          </Stack>
          <Typography variant="body2" color="text.secondary">
            Long-running tasks run in the background. This page updates live via
            SSE.
          </Typography>
        </CardContent>
        <CardActions>
          <Button startIcon={<RefreshIcon />} onClick={refresh}>
            Refresh
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Job Retention</Typography>
          <Typography variant="body2" color="text.secondary">
            Retention status for job history.
          </Typography>
          {retention?.drift?.drift ? (
            <Alert severity="warning" sx={{ mt: 2 }}>
              Job history exceeds retention targets. Run cleanup or adjust{" "}
              <code>JOB_HISTORY_MAX_ROWS</code>/<code>JOB_HISTORY_MAX_DAYS</code>.
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
          <Typography variant="h6">Filters</Typography>
          <Typography variant="body2" color="text.secondary">
            Filter job history and export the current view.
          </Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <TextField
              label="Fetch limit"
              value={jobLimit}
              onChange={(e) => setJobLimit(e.target.value)}
              helperText="Applied on refresh (1-1000)."
              inputMode="numeric"
            />
            <FormControl size="small">
              <InputLabel id="job-status-label">Status</InputLabel>
              <Select
                labelId="job-status-label"
                label="Status"
                value={statusFilter}
                onChange={(e) => setStatusFilter(String(e.target.value))}
              >
                <MenuItem value="all">All</MenuItem>
                <MenuItem value="queued">Queued</MenuItem>
                <MenuItem value="running">Running</MenuItem>
                <MenuItem value="succeeded">Succeeded</MenuItem>
                <MenuItem value="failed">Failed</MenuItem>
                <MenuItem value="canceled">Canceled</MenuItem>
              </Select>
            </FormControl>
            <TextField
              label="Kind filter"
              value={kindFilter}
              onChange={(e) => setKindFilter(e.target.value)}
              placeholder="looks_generate, audio_analyze"
            />
            <TextField
              label="Search (id, kind, status, error, logs)"
              value={searchFilter}
              onChange={(e) => setSearchFilter(e.target.value)}
              placeholder="job id, error text"
            />
            <Typography variant="body2" color="text.secondary">
              Showing <code>{filteredJobs.length}</code> of{" "}
              <code>{jobs.length}</code>
            </Typography>
          </Stack>
        </CardContent>
        <CardActions>
          <Button onClick={refresh}>Refresh</Button>
          <Button onClick={() => exportJobs("csv")}>Export CSV</Button>
          <Button onClick={() => exportJobs("json")}>Export JSON</Button>
        </CardActions>
      </Card>

      {jobs.length === 0 ? (
        <Card>
          <CardContent>
            <Typography>No jobs yet.</Typography>
          </CardContent>
        </Card>
      ) : null}

      {jobs.length > 0 && filteredJobs.length === 0 ? (
        <Card>
          <CardContent>
            <Typography>No jobs match the current filters.</Typography>
          </CardContent>
        </Card>
      ) : null}

      {filteredJobs.map((j) => {
        const pct = progressPct(j);
        const outFile = outFileFromResult(j);
        const canCancel = j.status === "queued" || j.status === "running";
        const expanded = Boolean(expandedJobs[j.id]);
        return (
          <Card key={j.id} variant="outlined">
            <CardContent>
              <Stack
                direction="row"
                spacing={1}
                sx={{ alignItems: "center", flexWrap: "wrap" }}
              >
                <Typography variant="subtitle1" sx={{ flexGrow: 1 }}>
                  {j.kind}
                </Typography>
                <Chip
                  size="small"
                  label={j.status}
                  color={
                    j.status === "succeeded"
                      ? "success"
                      : j.status === "failed"
                        ? "error"
                        : j.status === "canceled"
                          ? "warning"
                          : "default"
                  }
                />
              </Stack>

              {j.progress?.message ? (
                <Typography
                  variant="body2"
                  color="text.secondary"
                  sx={{ mt: 0.5 }}
                >
                  {j.progress.message}
                </Typography>
              ) : null}

              {pct != null ? (
                <Box sx={{ mt: 1 }}>
                  <LinearProgress variant="determinate" value={pct} />
                  <Typography variant="caption" color="text.secondary">
                    {Math.round(pct)}% ({j.progress.current}/{j.progress.total})
                  </Typography>
                </Box>
              ) : j.status === "running" ? (
                <Box sx={{ mt: 1 }}>
                  <LinearProgress />
                </Box>
              ) : null}

              <Typography
                variant="caption"
                color="text.secondary"
                sx={{ display: "block", mt: 1 }}
              >
                Created: {fmtTs(j.created_at)} · Started: {fmtTs(j.started_at)}{" "}
                · Finished: {fmtTs(j.finished_at)}
              </Typography>

              {j.error ? (
                <Alert
                  severity={j.status === "canceled" ? "warning" : "error"}
                  sx={{ mt: 1 }}
                >
                  {j.error}
                </Alert>
              ) : null}

              <Collapse in={expanded} timeout="auto" unmountOnExit>
                <Typography variant="subtitle2" sx={{ mt: 1 }}>
                  Result
                </Typography>
                {j.result ? (
                  <Box
                    component="pre"
                    sx={{
                      whiteSpace: "pre-wrap",
                      wordBreak: "break-word",
                      fontSize: 12,
                      mt: 1,
                    }}
                  >
                    {JSON.stringify(j.result, null, 2)}
                  </Box>
                ) : (
                  <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{ mt: 1 }}
                  >
                    No result payload.
                  </Typography>
                )}
                <Typography variant="subtitle2" sx={{ mt: 2 }}>
                  Logs
                </Typography>
                {j.logs?.length ? (
                  <Box
                    component="pre"
                    sx={{
                      whiteSpace: "pre-wrap",
                      wordBreak: "break-word",
                      fontSize: 12,
                      mt: 1,
                    }}
                  >
                    {j.logs.join("\n")}
                  </Box>
                ) : (
                  <Typography variant="body2" color="text.secondary">
                    No logs captured.
                  </Typography>
                )}
              </Collapse>
            </CardContent>
            <CardActions>
              <Button onClick={() => toggleJobDetails(j.id)}>
                {expanded ? "Hide details" : "Show details"}
              </Button>
              {outFile ? (
                <Button
                  component="a"
                  href={`/v1/files/download?path=${encodeURIComponent(outFile)}`}
                  target="_blank"
                  rel="noreferrer"
                >
                  Download output
                </Button>
              ) : null}
              {canCancel ? (
                <Button
                  color="warning"
                  startIcon={<CancelIcon />}
                  onClick={() => cancel(j.id)}
                >
                  Cancel
                </Button>
              ) : null}
            </CardActions>
          </Card>
        );
      })}
    </Stack>
  );
}
