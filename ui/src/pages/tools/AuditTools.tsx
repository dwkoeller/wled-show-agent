import RefreshIcon from "@mui/icons-material/Refresh";
import HistoryIcon from "@mui/icons-material/History";
import {
  Alert,
  Button,
  Card,
  CardActions,
  CardContent,
  Chip,
  Collapse,
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

type AuditLogRow = {
  id: number | null;
  agent_id: string;
  created_at: number;
  actor: string;
  action: string;
  resource?: string | null;
  ok: boolean;
  error?: string | null;
  ip?: string | null;
  user_agent?: string | null;
  request_id?: string | null;
  payload?: Record<string, unknown> | null;
};

type AuditLogsRes = {
  ok: boolean;
  logs: AuditLogRow[];
  count?: number;
  limit?: number;
  offset?: number;
  next_offset?: number | null;
};

type PageMeta = {
  count?: number;
  limit?: number;
  offset?: number;
  next_offset?: number | null;
};

type OrchestrationRunRow = {
  run_id: string;
  agent_id: string;
  created_at: number;
  updated_at: number;
  started_at: number;
  finished_at?: number | null;
  name?: string | null;
  scope: string;
  status: string;
  steps_total: number;
  loop: boolean;
  include_self: boolean;
  duration_s?: number;
  error?: string | null;
  payload?: Record<string, unknown> | null;
};

type OrchestrationRunsRes = {
  ok: boolean;
  runs: OrchestrationRunRow[];
  count?: number;
  limit?: number;
  offset?: number;
  next_offset?: number | null;
};

type OrchestrationStepRow = {
  id?: number | null;
  run_id: string;
  agent_id: string;
  step_index: number;
  iteration: number;
  kind: string;
  status: string;
  ok: boolean;
  started_at: number;
  finished_at?: number | null;
  duration_s?: number;
  error?: string | null;
  payload?: Record<string, unknown> | null;
};

type OrchestrationPeerRow = {
  id?: number | null;
  run_id: string;
  agent_id: string;
  peer_id: string;
  step_index: number;
  iteration: number;
  action: string;
  status: string;
  ok: boolean;
  started_at: number;
  finished_at?: number | null;
  duration_s?: number;
  error?: string | null;
  payload?: Record<string, unknown> | null;
};

type OrchestrationRunDetailRes = {
  ok: boolean;
  run: OrchestrationRunRow;
  steps: OrchestrationStepRow[];
  peers: OrchestrationPeerRow[];
  steps_meta?: {
    count?: number;
    limit?: number;
    offset?: number;
    next_offset?: number | null;
    status?: string | null;
    ok?: boolean | null;
  };
  peers_meta?: {
    count?: number;
    limit?: number;
    offset?: number;
    next_offset?: number | null;
    status?: string | null;
    ok?: boolean | null;
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

function fmtTs(ts: number | null | undefined): string {
  if (!ts) return "—";
  try {
    return new Date(ts * 1000).toLocaleString();
  } catch {
    return String(ts);
  }
}

function fmtDuration(run: OrchestrationRunRow): string {
  const dur = Number(run.duration_s ?? 0);
  if (Number.isFinite(dur) && dur > 0) return `${dur.toFixed(1)}s`;
  if (!run.started_at || !run.finished_at) return "—";
  const delta = Math.max(0, run.finished_at - run.started_at);
  return `${delta.toFixed(1)}s`;
}

function statusColor(status: string): "success" | "error" | "warning" | "info" {
  const s = status.toLowerCase();
  if (s === "completed") return "success";
  if (s === "failed") return "error";
  if (s === "stopped") return "warning";
  return "info";
}

function toEpochSeconds(value: string): string | null {
  if (!value) return null;
  const ts = Date.parse(value);
  if (Number.isNaN(ts)) return null;
  return String(Math.floor(ts / 1000));
}

export function AuditTools() {
  const { user } = useAuth();
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [retention, setRetention] = useState<RetentionRes | null>(null);
  const [retentionError, setRetentionError] = useState<string | null>(null);
  const [retentionBusy, setRetentionBusy] = useState(false);
  const [retentionOverrideRows, setRetentionOverrideRows] = useState("");
  const [retentionOverrideDays, setRetentionOverrideDays] = useState("");
  const [retentionResult, setRetentionResult] =
    useState<Record<string, unknown> | null>(null);
  const [orchestrationRetention, setOrchestrationRetention] =
    useState<RetentionRes | null>(null);
  const [orchestrationRetentionError, setOrchestrationRetentionError] =
    useState<string | null>(null);
  const [orchestrationRetentionBusy, setOrchestrationRetentionBusy] =
    useState(false);
  const [orchestrationRetentionOverrideRows, setOrchestrationRetentionOverrideRows] =
    useState("");
  const [orchestrationRetentionOverrideDays, setOrchestrationRetentionOverrideDays] =
    useState("");
  const [orchestrationRetentionResult, setOrchestrationRetentionResult] =
    useState<Record<string, unknown> | null>(null);

  const [limit, setLimit] = useState("200");
  const [offset, setOffset] = useState("0");
  const [actionFilter, setActionFilter] = useState("");
  const [actorFilter, setActorFilter] = useState("");
  const [agentFilter, setAgentFilter] = useState("");
  const [resourceFilter, setResourceFilter] = useState("");
  const [ipFilter, setIpFilter] = useState("");
  const [errorFilter, setErrorFilter] = useState("");
  const [onlyErrors, setOnlyErrors] = useState(false);
  const [sinceFilter, setSinceFilter] = useState("");
  const [untilFilter, setUntilFilter] = useState("");
  const [scopeFilter, setScopeFilter] = useState("all");
  const [runOffset, setRunOffset] = useState("0");
  const [runStatusFilter, setRunStatusFilter] = useState("");
  const [runAgentFilter, setRunAgentFilter] = useState("");
  const [runSinceFilter, setRunSinceFilter] = useState("");
  const [runUntilFilter, setRunUntilFilter] = useState("");
  const [detailStepsLimit, setDetailStepsLimit] = useState("50");
  const [detailPeersLimit, setDetailPeersLimit] = useState("50");
  const [detailStepsOffset, setDetailStepsOffset] = useState("0");
  const [detailPeersOffset, setDetailPeersOffset] = useState("0");
  const [detailStepStatus, setDetailStepStatus] = useState("");
  const [detailPeerStatus, setDetailPeerStatus] = useState("");
  const [detailStepsFailuresOnly, setDetailStepsFailuresOnly] = useState(false);
  const [detailPeersFailuresOnly, setDetailPeersFailuresOnly] = useState(true);

  const [logs, setLogs] = useState<AuditLogRow[]>([]);
  const [logsMeta, setLogsMeta] = useState<PageMeta | null>(null);
  const [runs, setRuns] = useState<OrchestrationRunRow[]>([]);
  const [runsMeta, setRunsMeta] = useState<PageMeta | null>(null);
  const [runDetails, setRunDetails] = useState<
    Record<string, OrchestrationRunDetailRes>
  >({});
  const [expandedRunId, setExpandedRunId] = useState<string | null>(null);
  const [detailBusyId, setDetailBusyId] = useState<string | null>(null);

  const buildLogQuery = (lim: number, offsetOverride?: number) => {
    const q = new URLSearchParams();
    q.set("limit", String(lim));
    const parsed = parseInt(offset || "0", 10);
    const off = offsetOverride ?? (Number.isFinite(parsed) ? parsed : 0);
    if (off > 0) q.set("offset", String(off));
    if (actionFilter.trim()) q.set("action", actionFilter.trim());
    if (actorFilter.trim()) q.set("actor", actorFilter.trim());
    if (agentFilter.trim()) q.set("agent_id", agentFilter.trim());
    if (resourceFilter.trim()) q.set("resource", resourceFilter.trim());
    if (ipFilter.trim()) q.set("ip", ipFilter.trim());
    if (errorFilter.trim()) q.set("error", errorFilter.trim());
    if (onlyErrors) q.set("ok", "false");
    const since = toEpochSeconds(sinceFilter);
    const until = toEpochSeconds(untilFilter);
    if (since) q.set("since", since);
    if (until) q.set("until", until);
    return q;
  };

  const buildRunQuery = (lim: number, offsetOverride?: number) => {
    const q = new URLSearchParams();
    q.set("limit", String(lim));
    const parsed = parseInt(runOffset || "0", 10);
    const off = offsetOverride ?? (Number.isFinite(parsed) ? parsed : 0);
    if (off > 0) q.set("offset", String(off));
    if (scopeFilter !== "all") q.set("scope", scopeFilter);
    if (runStatusFilter.trim()) q.set("status", runStatusFilter.trim());
    if (runAgentFilter.trim()) q.set("agent_id", runAgentFilter.trim());
    const since = toEpochSeconds(runSinceFilter);
    const until = toEpochSeconds(runUntilFilter);
    if (since) q.set("since", since);
    if (until) q.set("until", until);
    return q;
  };

  const fetchRetention = async () => {
    setRetentionError(null);
    try {
      const res = await api<RetentionRes>("/v1/audit/retention", {
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
          ? `/v1/audit/retention?${params.toString()}`
          : "/v1/audit/retention";
      const res = await api<{ ok?: boolean; result?: Record<string, unknown> }>(
        url,
        { method: "POST", json: {} },
      );
      setRetentionResult(res.result ?? null);
      await fetchRetention();
      await fetchOrchestrationRetention();
    } catch (e) {
      setRetentionError(e instanceof Error ? e.message : String(e));
    } finally {
      setRetentionBusy(false);
    }
  };

  const fetchOrchestrationRetention = async () => {
    setOrchestrationRetentionError(null);
    try {
      const res = await api<RetentionRes>("/v1/orchestration/retention", {
        method: "GET",
      });
      setOrchestrationRetention(res);
    } catch (e) {
      setOrchestrationRetentionError(e instanceof Error ? e.message : String(e));
    }
  };

  const runOrchestrationRetention = async () => {
    setOrchestrationRetentionBusy(true);
    setOrchestrationRetentionError(null);
    setOrchestrationRetentionResult(null);
    try {
      const params = new URLSearchParams();
      const rows = parseInt(orchestrationRetentionOverrideRows || "", 10);
      if (Number.isFinite(rows) && rows > 0) params.set("max_rows", String(rows));
      const days = parseInt(orchestrationRetentionOverrideDays || "", 10);
      if (Number.isFinite(days) && days > 0) params.set("max_days", String(days));
      const url =
        params.toString().length > 0
          ? `/v1/orchestration/retention?${params.toString()}`
          : "/v1/orchestration/retention";
      const res = await api<{ ok?: boolean; result?: Record<string, unknown> }>(
        url,
        { method: "POST", json: {} },
      );
      setOrchestrationRetentionResult(res.result ?? null);
      await fetchOrchestrationRetention();
    } catch (e) {
      setOrchestrationRetentionError(e instanceof Error ? e.message : String(e));
    } finally {
      setOrchestrationRetentionBusy(false);
    }
  };

  const refresh = async (opts?: { logOffset?: number; runOffset?: number }) => {
    setBusy(true);
    setError(null);
    try {
      const lim = parseInt(limit || "200", 10) || 200;
      const runLim = Math.min(lim, 500);
      const logOff = opts?.logOffset ?? (parseInt(offset || "0", 10) || 0);
      const runOff = opts?.runOffset ?? (parseInt(runOffset || "0", 10) || 0);
      if (opts?.logOffset != null) setOffset(String(logOff));
      if (opts?.runOffset != null) setRunOffset(String(runOff));
      const q = buildLogQuery(lim, logOff);
      const qRuns = buildRunQuery(runLim, runOff);

      const [logsRes, runsRes] = await Promise.all([
        api<AuditLogsRes>(`/v1/audit/logs?${q.toString()}`, { method: "GET" }),
        api<OrchestrationRunsRes>(
          `/v1/orchestration/runs?${qRuns.toString()}`,
          { method: "GET" },
        ),
      ]);
      setLogs(logsRes.logs ?? []);
      setLogsMeta({
        count: logsRes.count ?? logsRes.logs?.length ?? 0,
        limit: logsRes.limit ?? lim,
        offset: logsRes.offset ?? logOff,
        next_offset: logsRes.next_offset ?? null,
      });
      setRuns(runsRes.runs ?? []);
      setRunsMeta({
        count: runsRes.count ?? runsRes.runs?.length ?? 0,
        limit: runsRes.limit ?? runLim,
        offset: runsRes.offset ?? runOff,
        next_offset: runsRes.next_offset ?? null,
      });
      await fetchRetention();
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

  const exportLogs = async (format: "csv" | "json") => {
    const lim = parseInt(limit || "200", 10) || 200;
    const q = buildLogQuery(Math.min(lim, 20000));
    q.set("format", format);
    const filename = format === "json" ? "audit_logs.json" : "audit_logs.csv";
    await downloadExport(`/v1/audit/logs/export?${q.toString()}`, filename);
  };

  const exportRuns = async (format: "csv" | "json") => {
    const lim = parseInt(limit || "200", 10) || 200;
    const q = buildRunQuery(Math.min(lim, 20000));
    q.set("format", format);
    const filename =
      format === "json" ? "orchestration_runs.json" : "orchestration_runs.csv";
    await downloadExport(`/v1/orchestration/runs/export?${q.toString()}`, filename);
  };

  const exportRunSteps = async (runId: string, format: "csv" | "json") => {
    const q = new URLSearchParams();
    const lim = Math.max(1, parseInt(detailStepsLimit || "50", 10) || 50);
    const off = Math.max(0, parseInt(detailStepsOffset || "0", 10) || 0);
    q.set("limit", String(lim));
    if (off > 0) q.set("offset", String(off));
    if (detailStepStatus.trim()) q.set("status", detailStepStatus.trim());
    if (detailStepsFailuresOnly) q.set("ok", "false");
    q.set("format", format);
    const filename =
      format === "json"
        ? `orchestration_steps_${runId}.json`
        : `orchestration_steps_${runId}.csv`;
    await downloadExport(
      `/v1/orchestration/runs/${encodeURIComponent(runId)}/steps/export?${q.toString()}`,
      filename,
    );
  };

  const exportRunPeers = async (runId: string, format: "csv" | "json") => {
    const q = new URLSearchParams();
    const lim = Math.max(1, parseInt(detailPeersLimit || "50", 10) || 50);
    const off = Math.max(0, parseInt(detailPeersOffset || "0", 10) || 0);
    q.set("limit", String(lim));
    if (off > 0) q.set("offset", String(off));
    if (detailPeerStatus.trim()) q.set("status", detailPeerStatus.trim());
    if (detailPeersFailuresOnly) q.set("ok", "false");
    q.set("format", format);
    const filename =
      format === "json"
        ? `orchestration_peers_${runId}.json`
        : `orchestration_peers_${runId}.csv`;
    await downloadExport(
      `/v1/orchestration/runs/${encodeURIComponent(runId)}/peers/export?${q.toString()}`,
      filename,
    );
  };

  const buildRunDetailQuery = (opts?: {
    stepsOffset?: number;
    peersOffset?: number;
  }) => {
    const q = new URLSearchParams();
    const stepsLimit = Math.max(1, parseInt(detailStepsLimit || "50", 10) || 50);
    const peersLimit = Math.max(1, parseInt(detailPeersLimit || "50", 10) || 50);
    const stepsOffset = Math.max(
      0,
      opts?.stepsOffset ?? (parseInt(detailStepsOffset || "0", 10) || 0),
    );
    const peersOffset = Math.max(
      0,
      opts?.peersOffset ?? (parseInt(detailPeersOffset || "0", 10) || 0),
    );
    q.set("steps_limit", String(stepsLimit));
    q.set("peers_limit", String(peersLimit));
    if (stepsOffset > 0) q.set("steps_offset", String(stepsOffset));
    if (peersOffset > 0) q.set("peers_offset", String(peersOffset));
    if (detailStepStatus.trim()) q.set("step_status", detailStepStatus.trim());
    if (detailPeerStatus.trim()) q.set("peer_status", detailPeerStatus.trim());
    if (detailStepsFailuresOnly) q.set("step_ok", "false");
    if (detailPeersFailuresOnly) q.set("peer_ok", "false");
    return { q, stepsOffset, peersOffset };
  };

  const fetchRunDetails = async (
    runId: string,
    opts?: { stepsOffset?: number; peersOffset?: number },
  ) => {
    setDetailBusyId(runId);
    setError(null);
    try {
      const { q, stepsOffset, peersOffset } = buildRunDetailQuery(opts);
      if (opts?.stepsOffset != null) setDetailStepsOffset(String(stepsOffset));
      if (opts?.peersOffset != null) setDetailPeersOffset(String(peersOffset));
      const detail = await api<OrchestrationRunDetailRes>(
        `/v1/orchestration/runs/${encodeURIComponent(runId)}?${q.toString()}`,
        { method: "GET" },
      );
      setRunDetails((prev) => ({ ...prev, [runId]: detail }));
      if (typeof detail.steps_meta?.offset === "number") {
        setDetailStepsOffset(String(detail.steps_meta.offset));
      }
      if (typeof detail.peers_meta?.offset === "number") {
        setDetailPeersOffset(String(detail.peers_meta.offset));
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setDetailBusyId(null);
    }
  };

  const pageRunSteps = async (runId: string, dir: "prev" | "next") => {
    const detail = runDetails[runId];
    if (!detail) return;
    const meta = detail.steps_meta;
    const limitVal = meta?.limit ?? (parseInt(detailStepsLimit || "50", 10) || 50);
    const current = meta?.offset ?? (parseInt(detailStepsOffset || "0", 10) || 0);
    const next =
      dir === "next"
        ? meta?.next_offset ?? null
        : Math.max(0, current - limitVal);
    if (next == null) return;
    setDetailStepsOffset(String(next));
    await fetchRunDetails(runId, { stepsOffset: next });
  };

  const pageRunPeers = async (runId: string, dir: "prev" | "next") => {
    const detail = runDetails[runId];
    if (!detail) return;
    const meta = detail.peers_meta;
    const limitVal = meta?.limit ?? (parseInt(detailPeersLimit || "50", 10) || 50);
    const current = meta?.offset ?? (parseInt(detailPeersOffset || "0", 10) || 0);
    const next =
      dir === "next"
        ? meta?.next_offset ?? null
        : Math.max(0, current - limitVal);
    if (next == null) return;
    setDetailPeersOffset(String(next));
    await fetchRunDetails(runId, { peersOffset: next });
  };

  const pageLogs = async (dir: "prev" | "next") => {
    const limitVal = logsMeta?.limit ?? (parseInt(limit || "200", 10) || 200);
    const current = logsMeta?.offset ?? (parseInt(offset || "0", 10) || 0);
    const next =
      dir === "next" ? logsMeta?.next_offset ?? null : Math.max(0, current - limitVal);
    if (next == null) return;
    await refresh({ logOffset: next });
  };

  const pageRuns = async (dir: "prev" | "next") => {
    const limitVal =
      runsMeta?.limit ??
      Math.min(parseInt(limit || "200", 10) || 200, 500);
    const current = runsMeta?.offset ?? (parseInt(runOffset || "0", 10) || 0);
    const next =
      dir === "next" ? runsMeta?.next_offset ?? null : Math.max(0, current - limitVal);
    if (next == null) return;
    await refresh({ runOffset: next });
  };

  const toggleRunDetails = async (runId: string) => {
    if (expandedRunId === runId) {
      setExpandedRunId(null);
      return;
    }
    setDetailStepsOffset("0");
    setDetailPeersOffset("0");
    setExpandedRunId(runId);
    await fetchRunDetails(runId, { stepsOffset: 0, peersOffset: 0 });
  };

  useEffect(() => {
    void refresh();
  }, []);

  useEventRefresh({
    types: ["audit", "orchestration", "tick"],
    refresh,
    minIntervalMs: 3000,
  });

  const logRows = useMemo(() => logs ?? [], [logs]);
  const runRows = useMemo(() => runs ?? [], [runs]);
  const retentionDrift = retention?.drift?.drift;
  const orchestrationRetentionDrift = orchestrationRetention?.drift?.drift;
  const isAdmin = (user?.role || "") === "admin";

  return (
    <Stack spacing={2}>
      {error ? <Alert severity="error">{error}</Alert> : null}

      <Card>
        <CardContent>
          <Stack direction="row" spacing={1} sx={{ alignItems: "center" }}>
            <HistoryIcon />
            <Typography variant="h6">Audit Log</Typography>
          </Stack>
          <Typography variant="body2" color="text.secondary">
            Auth/admin actions from <code>/v1/audit/logs</code>.
          </Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap" }}>
              <Chip
                label="Fleet overrides"
                variant="outlined"
                onClick={() => {
                  setActionFilter("fleet.overrides");
                  setOffset("0");
                }}
              />
              <Chip
                label="Auth events"
                variant="outlined"
                onClick={() => {
                  setActionFilter("auth.");
                  setOffset("0");
                }}
              />
              <Chip
                label="Scheduler"
                variant="outlined"
                onClick={() => {
                  setActionFilter("scheduler.");
                  setOffset("0");
                }}
              />
              <Chip
                label="Clear filter"
                variant="outlined"
                onClick={() => {
                  setActionFilter("");
                  setOffset("0");
                }}
              />
            </Stack>
            <TextField
              label="Limit"
              value={limit}
              onChange={(e) => setLimit(e.target.value)}
              disabled={busy}
            />
            <TextField
              label="Offset"
              value={offset}
              onChange={(e) => setOffset(e.target.value)}
              disabled={busy}
            />
            <TextField
              label="Action filter"
              value={actionFilter}
              onChange={(e) => setActionFilter(e.target.value)}
              placeholder="auth.login, fleet.overrides.*, scheduler."
              disabled={busy}
            />
            <TextField
              label="Actor filter"
              value={actorFilter}
              onChange={(e) => setActorFilter(e.target.value)}
              placeholder="admin"
              disabled={busy}
            />
            <TextField
              label="Agent filter"
              value={agentFilter}
              onChange={(e) => setAgentFilter(e.target.value)}
              placeholder="agent-1"
              disabled={busy}
            />
            <TextField
              label="Resource filter"
              value={resourceFilter}
              onChange={(e) => setResourceFilter(e.target.value)}
              placeholder="files/..."
              disabled={busy}
            />
            <TextField
              label="IP filter"
              value={ipFilter}
              onChange={(e) => setIpFilter(e.target.value)}
              placeholder="127.0.0.1"
              disabled={busy}
            />
            <TextField
              label="Error contains"
              value={errorFilter}
              onChange={(e) => setErrorFilter(e.target.value)}
              placeholder="timeout"
              disabled={busy}
            />
            <FormControlLabel
              control={
                <Switch
                  checked={onlyErrors}
                  onChange={(e) => setOnlyErrors(e.target.checked)}
                />
              }
              label="Only errors"
            />
            <TextField
              label="Since"
              type="datetime-local"
              value={sinceFilter}
              onChange={(e) => setSinceFilter(e.target.value)}
              disabled={busy}
              InputLabelProps={{ shrink: true }}
            />
            <TextField
              label="Until"
              type="datetime-local"
              value={untilFilter}
              onChange={(e) => setUntilFilter(e.target.value)}
              disabled={busy}
              InputLabelProps={{ shrink: true }}
            />
          </Stack>
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
            onClick={() => void pageLogs("prev")}
            disabled={busy || (logsMeta?.offset ?? 0) <= 0}
          >
            Prev
          </Button>
          <Button
            onClick={() => void pageLogs("next")}
            disabled={busy || logsMeta?.next_offset == null}
          >
            Next
          </Button>
          <Button onClick={() => void exportLogs("csv")} disabled={busy}>
            Export CSV
          </Button>
          <Button onClick={() => void exportLogs("json")} disabled={busy}>
            Export JSON
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Audit Retention</Typography>
          <Typography variant="body2" color="text.secondary">
            Retention status for <code>audit_log</code>.
          </Typography>
          {retentionDrift ? (
            <Alert severity="warning" sx={{ mt: 2 }}>
              Audit log exceeds retention targets. Run cleanup or adjust{" "}
              <code>AUDIT_LOG_MAX_ROWS</code>/<code>AUDIT_LOG_MAX_DAYS</code>.
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
          <Typography variant="h6">Recent Audit Entries</Typography>
          {logsMeta ? (
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
              Showing <code>{logsMeta.count ?? logRows.length}</code> · offset=
              <code>{logsMeta.offset ?? 0}</code> · limit=
              <code>{logsMeta.limit ?? limit}</code>
            </Typography>
          ) : null}
          <Stack spacing={1} sx={{ mt: 2 }}>
            {logRows.length ? (
              logRows.map((row) => (
                <Stack
                  key={`${row.id ?? "na"}-${row.created_at}`}
                  spacing={0.5}
                  sx={{
                    border: "1px solid rgba(255,255,255,0.12)",
                    borderRadius: 1,
                    p: 1,
                  }}
                >
                  <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap" }}>
                    <Chip
                      size="small"
                      label={row.ok ? "ok" : "error"}
                      color={row.ok ? "success" : "error"}
                      variant="outlined"
                    />
                    <Typography variant="body2">
                      <code>{row.action}</code>
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      actor=<code>{row.actor}</code>
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      ts=<code>{fmtTs(row.created_at)}</code>
                    </Typography>
                  </Stack>
                  <Typography variant="body2" color="text.secondary">
                    agent=<code>{row.agent_id}</code>
                    {row.resource ? (
                      <> · resource=<code>{row.resource}</code></>
                    ) : null}
                    {row.request_id ? (
                      <> · req=<code>{row.request_id}</code></>
                    ) : null}
                  </Typography>
                  {row.error ? (
                    <Typography variant="body2" color="error">
                      {row.error}
                    </Typography>
                  ) : null}
                  {row.payload && Object.keys(row.payload).length ? (
                    <Typography variant="body2" color="text.secondary">
                      payload=<code>{JSON.stringify(row.payload)}</code>
                    </Typography>
                  ) : null}
                </Stack>
              ))
            ) : (
              <Typography variant="body2">No audit entries.</Typography>
            )}
          </Stack>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Orchestration Retention</Typography>
          <Typography variant="body2" color="text.secondary">
            Retention status for <code>orchestration_runs</code>.
          </Typography>
          {orchestrationRetentionDrift ? (
            <Alert severity="warning" sx={{ mt: 2 }}>
              Orchestration runs exceed retention targets. Run cleanup or adjust{" "}
              <code>ORCHESTRATION_RUNS_MAX_ROWS</code>/
              <code>ORCHESTRATION_RUNS_MAX_DAYS</code>.
            </Alert>
          ) : null}
          {orchestrationRetentionError ? (
            <Alert severity="warning" sx={{ mt: 2 }}>
              {orchestrationRetentionError}
            </Alert>
          ) : null}
          {orchestrationRetentionResult ? (
            <Alert severity="success" sx={{ mt: 2 }}>
              Cleanup complete:{" "}
              <code>
                rows {String(orchestrationRetentionResult.deleted_by_rows ?? 0)} ·
                days {String(orchestrationRetentionResult.deleted_by_days ?? 0)}
              </code>
            </Alert>
          ) : null}
          <Stack spacing={1} sx={{ mt: 2 }}>
            <Typography variant="body2">
              Count: <code>{orchestrationRetention?.stats?.count ?? 0}</code> · Oldest{" "}
              <code>{fmtTs(orchestrationRetention?.stats?.oldest ?? null)}</code> ·
              Newest{" "}
              <code>{fmtTs(orchestrationRetention?.stats?.newest ?? null)}</code>
            </Typography>
            <Typography variant="body2">
              Max rows: <code>{orchestrationRetention?.settings?.max_rows ?? 0}</code> ·
              Max days: <code>{orchestrationRetention?.settings?.max_days ?? 0}</code>
            </Typography>
            <Typography variant="body2">
              Drift: rows{" "}
              <code>{orchestrationRetention?.drift?.excess_rows ?? 0}</code> · age{" "}
              <code>
                {orchestrationRetention?.drift?.excess_age_s
                  ? `${Math.round(orchestrationRetention.drift.excess_age_s)}s`
                  : "0s"}
              </code>
            </Typography>
            <TextField
              label="Override max rows"
              value={orchestrationRetentionOverrideRows}
              onChange={(e) => setOrchestrationRetentionOverrideRows(e.target.value)}
              helperText="Optional override for manual cleanup."
              disabled={!isAdmin || orchestrationRetentionBusy}
              inputMode="numeric"
            />
            <TextField
              label="Override max days"
              value={orchestrationRetentionOverrideDays}
              onChange={(e) => setOrchestrationRetentionOverrideDays(e.target.value)}
              helperText="Optional override for manual cleanup."
              disabled={!isAdmin || orchestrationRetentionBusy}
              inputMode="numeric"
            />
            <Typography variant="body2" color="text.secondary">
              Last cleanup:{" "}
              <code>
                {orchestrationRetention?.last_retention?.at
                  ? fmtTs(orchestrationRetention.last_retention.at)
                  : "—"}
              </code>
            </Typography>
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            onClick={() => void fetchOrchestrationRetention()}
            disabled={orchestrationRetentionBusy}
          >
            Refresh retention
          </Button>
          <Button
            variant="contained"
            onClick={() => void runOrchestrationRetention()}
            disabled={!isAdmin || orchestrationRetentionBusy}
          >
            Run cleanup
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Orchestration Runs</Typography>
          {runsMeta ? (
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
              Showing <code>{runsMeta.count ?? runRows.length}</code> · offset=
              <code>{runsMeta.offset ?? 0}</code> · limit=
              <code>{runsMeta.limit ?? Math.min(parseInt(limit || "200", 10) || 200, 500)}</code>
            </Typography>
          ) : null}
          <Stack spacing={2} sx={{ mt: 2 }}>
            <FormControl fullWidth>
              <InputLabel id="audit-scope-label">Scope</InputLabel>
              <Select
                labelId="audit-scope-label"
                value={scopeFilter}
                label="Scope"
                onChange={(e) => setScopeFilter(String(e.target.value))}
                disabled={busy}
              >
                <MenuItem value="all">All</MenuItem>
                <MenuItem value="local">Local</MenuItem>
                <MenuItem value="fleet">Fleet</MenuItem>
              </Select>
            </FormControl>
            <TextField
              label="Status filter"
              value={runStatusFilter}
              onChange={(e) => setRunStatusFilter(e.target.value)}
              placeholder="completed, failed"
              disabled={busy}
            />
            <TextField
              label="Offset"
              value={runOffset}
              onChange={(e) => setRunOffset(e.target.value)}
              disabled={busy}
            />
            <TextField
              label="Agent filter"
              value={runAgentFilter}
              onChange={(e) => setRunAgentFilter(e.target.value)}
              placeholder="agent-1"
              disabled={busy}
            />
            <TextField
              label="Since"
              type="datetime-local"
              value={runSinceFilter}
              onChange={(e) => setRunSinceFilter(e.target.value)}
              disabled={busy}
              InputLabelProps={{ shrink: true }}
            />
            <TextField
              label="Until"
              type="datetime-local"
              value={runUntilFilter}
              onChange={(e) => setRunUntilFilter(e.target.value)}
              disabled={busy}
              InputLabelProps={{ shrink: true }}
            />
          </Stack>
          <Stack spacing={1} sx={{ mt: 2 }}>
            {runRows.length ? (
              runRows.map((run) => {
                const detail = runDetails[run.run_id];
                const steps = detail?.steps ?? [];
                const peers = detail?.peers ?? [];
                const stepsMeta = detail?.steps_meta;
                const peersMeta = detail?.peers_meta;
                const stepsOffsetVal =
                  stepsMeta?.offset ?? (parseInt(detailStepsOffset || "0", 10) || 0);
                const peersOffsetVal =
                  peersMeta?.offset ?? (parseInt(detailPeersOffset || "0", 10) || 0);
                const detailBusy = detailBusyId === run.run_id;
                const stepsPrevDisabled = busy || detailBusy || stepsOffsetVal <= 0;
                const peersPrevDisabled = busy || detailBusy || peersOffsetVal <= 0;
                const stepsNextDisabled =
                  busy || detailBusy || stepsMeta?.next_offset == null;
                const peersNextDisabled =
                  busy || detailBusy || peersMeta?.next_offset == null;
                return (
                  <Stack
                    key={run.run_id}
                    spacing={0.5}
                    sx={{
                      border: "1px solid rgba(255,255,255,0.12)",
                      borderRadius: 1,
                      p: 1,
                    }}
                  >
                    <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap" }}>
                      <Chip
                        size="small"
                        label={run.status}
                        color={statusColor(run.status)}
                        variant="outlined"
                      />
                      <Typography variant="body2">
                        <code>{run.scope}</code>
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        agent=<code>{run.agent_id}</code>
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        steps=<code>{run.steps_total}</code>
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        duration=<code>{fmtDuration(run)}</code>
                      </Typography>
                      <Button
                        size="small"
                        onClick={() => void toggleRunDetails(run.run_id)}
                        disabled={busy || detailBusyId === run.run_id}
                      >
                        {expandedRunId === run.run_id ? "Hide" : "Details"}
                      </Button>
                    </Stack>
                    <Typography variant="body2" color="text.secondary">
                      name=<code>{run.name ?? "—"}</code> · start=
                      <code>{fmtTs(run.started_at)}</code>
                      {run.finished_at ? (
                        <> · end=<code>{fmtTs(run.finished_at)}</code></>
                      ) : null}
                    </Typography>
                    {run.error ? (
                      <Typography variant="body2" color="error">
                        {run.error}
                      </Typography>
                    ) : null}
                    <Collapse in={expandedRunId === run.run_id}>
                      {detail ? (
                        <Stack spacing={1} sx={{ mt: 1 }}>
                          <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap" }}>
                            <TextField
                              label="Steps limit"
                              value={detailStepsLimit}
                              onChange={(e) => setDetailStepsLimit(e.target.value)}
                              disabled={busy}
                              inputMode="numeric"
                            />
                            <TextField
                              label="Steps offset"
                              value={detailStepsOffset}
                              onChange={(e) => setDetailStepsOffset(e.target.value)}
                              disabled={busy}
                              inputMode="numeric"
                            />
                            <TextField
                              label="Step status"
                              value={detailStepStatus}
                              onChange={(e) => setDetailStepStatus(e.target.value)}
                              placeholder="completed, failed"
                              disabled={busy}
                            />
                            <FormControlLabel
                              control={
                                <Switch
                                  checked={detailStepsFailuresOnly}
                                  onChange={(e) =>
                                    setDetailStepsFailuresOnly(e.target.checked)
                                  }
                                />
                              }
                              label="Step failures only"
                            />
                            <TextField
                              label="Peers limit"
                              value={detailPeersLimit}
                              onChange={(e) => setDetailPeersLimit(e.target.value)}
                              disabled={busy}
                              inputMode="numeric"
                            />
                            <TextField
                              label="Peers offset"
                              value={detailPeersOffset}
                              onChange={(e) => setDetailPeersOffset(e.target.value)}
                              disabled={busy}
                              inputMode="numeric"
                            />
                            <TextField
                              label="Peer status"
                              value={detailPeerStatus}
                              onChange={(e) => setDetailPeerStatus(e.target.value)}
                              placeholder="completed, failed"
                              disabled={busy}
                            />
                            <FormControlLabel
                              control={
                                <Switch
                                  checked={detailPeersFailuresOnly}
                                  onChange={(e) =>
                                    setDetailPeersFailuresOnly(e.target.checked)
                                  }
                                />
                              }
                              label="Peer failures only"
                            />
                            <Button
                              size="small"
                              onClick={() => void fetchRunDetails(run.run_id)}
                              disabled={busy || detailBusyId === run.run_id}
                            >
                              Refresh details
                            </Button>
                            <Button
                              size="small"
                              onClick={() => void exportRunSteps(run.run_id, "csv")}
                              disabled={busy}
                            >
                              Export steps CSV
                            </Button>
                            <Button
                              size="small"
                              onClick={() => void exportRunSteps(run.run_id, "json")}
                              disabled={busy}
                            >
                              Export steps JSON
                            </Button>
                            <Button
                              size="small"
                              onClick={() => void exportRunPeers(run.run_id, "csv")}
                              disabled={busy}
                            >
                              Export peers CSV
                            </Button>
                            <Button
                              size="small"
                              onClick={() => void exportRunPeers(run.run_id, "json")}
                              disabled={busy}
                            >
                              Export peers JSON
                            </Button>
                          </Stack>
                          <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap" }}>
                            <Button
                              size="small"
                              onClick={() => void pageRunSteps(run.run_id, "prev")}
                              disabled={stepsPrevDisabled}
                            >
                              Prev steps
                            </Button>
                            <Button
                              size="small"
                              onClick={() => void pageRunSteps(run.run_id, "next")}
                              disabled={stepsNextDisabled}
                            >
                              Next steps
                            </Button>
                            <Button
                              size="small"
                              onClick={() => void pageRunPeers(run.run_id, "prev")}
                              disabled={peersPrevDisabled}
                            >
                              Prev peers
                            </Button>
                            <Button
                              size="small"
                              onClick={() => void pageRunPeers(run.run_id, "next")}
                              disabled={peersNextDisabled}
                            >
                              Next peers
                            </Button>
                          </Stack>
                          <Typography variant="body2">
                            Steps: <code>{stepsMeta?.count ?? steps.length}</code> ·
                            offset=<code>{stepsMeta?.offset ?? 0}</code> · limit=
                            <code>{stepsMeta?.limit ?? detailStepsLimit}</code>
                          </Typography>
                          <Typography variant="body2">
                            Peer results: <code>{peersMeta?.count ?? peers.length}</code>{" "}
                            · offset=<code>{peersMeta?.offset ?? 0}</code> · limit=
                            <code>{peersMeta?.limit ?? detailPeersLimit}</code>
                          </Typography>
                          {steps.length ? (
                            <Stack spacing={0.5}>
                              {steps.map((step) => (
                                <Typography
                                  key={`${step.id ?? "s"}-${step.step_index}-${step.iteration}`}
                                  variant="body2"
                                  color={step.ok ? "text.secondary" : "error"}
                                >
                                  step=<code>{step.step_index}</code> · kind=
                                  <code>{step.kind}</code> · status=
                                  <code>{step.status}</code> · dur=
                                  <code>{Number(step.duration_s ?? 0).toFixed(2)}s</code>
                                  {step.error ? (
                                    <>
                                      {" "}
                                      · err=<code>{step.error}</code>
                                    </>
                                  ) : null}
                                </Typography>
                              ))}
                            </Stack>
                          ) : (
                            <Typography variant="body2" color="text.secondary">
                              No steps.
                            </Typography>
                          )}
                          {peers.length ? (
                            <Stack spacing={0.5}>
                              <Typography variant="body2">
                                {detailPeersFailuresOnly
                                  ? "Peer failures"
                                  : "Peer results"}
                              </Typography>
                              {peers.map((peer) => (
                                <Typography
                                  key={`${peer.id ?? "p"}-${peer.peer_id}-${peer.step_index}-${peer.iteration}`}
                                  variant="body2"
                                  color={peer.ok ? "text.secondary" : "error"}
                                >
                                  peer=<code>{peer.peer_id}</code> · action=
                                  <code>{peer.action}</code> · status=
                                  <code>{peer.status}</code>
                                  {peer.error ? (
                                    <>
                                      {" "}
                                      · err=<code>{peer.error}</code>
                                    </>
                                  ) : null}
                                </Typography>
                              ))}
                            </Stack>
                          ) : (
                            <Typography variant="body2" color="text.secondary">
                              No peer results.
                            </Typography>
                          )}
                        </Stack>
                      ) : (
                        <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                          Loading details…
                        </Typography>
                      )}
                    </Collapse>
                  </Stack>
                );
              })
            ) : (
              <Typography variant="body2">No orchestration runs.</Typography>
            )}
          </Stack>
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
            onClick={() => void pageRuns("prev")}
            disabled={busy || (runsMeta?.offset ?? 0) <= 0}
          >
            Prev
          </Button>
          <Button
            onClick={() => void pageRuns("next")}
            disabled={busy || runsMeta?.next_offset == null}
          >
            Next
          </Button>
          <Button onClick={() => void exportRuns("csv")} disabled={busy}>
            Export CSV
          </Button>
          <Button onClick={() => void exportRuns("json")} disabled={busy}>
            Export JSON
          </Button>
        </CardActions>
      </Card>
    </Stack>
  );
}
