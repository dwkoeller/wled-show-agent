import RefreshIcon from "@mui/icons-material/Refresh";
import HubIcon from "@mui/icons-material/Hub";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import {
  Alert,
  Button,
  Card,
  CardActions,
  CardContent,
  Chip,
  Collapse,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
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
import { api, csrfHeaders } from "../../api";
import { useAuth } from "../../auth";
import { useEventRefresh } from "../../hooks/useEventRefresh";

type FleetStatusRes = {
  ok: boolean;
  now: number;
  stale_after_s: number;
  summary: { agents: number; online: number; configured: number };
  agents: Array<{
    agent_id: string;
    configured: boolean;
    online: boolean;
    age_s: number | null;
    updated_at: number | null;
    started_at: number | null;
    name: string | null;
    role: string | null;
    controller_kind: string | null;
    version: string | null;
    base_url: string | null;
    tags: string[] | null;
    role_override?: string | null;
    tags_override?: string[] | null;
    role_effective?: string | null;
    tags_effective?: string[] | null;
    capabilities?: any;
  }>;
};

type FleetHistoryRow = {
  id: number | null;
  agent_id: string;
  created_at: number;
  updated_at: number;
  name: string;
  role: string;
  controller_kind: string;
  version: string;
  base_url: string | null;
  payload?: Record<string, unknown> | null;
};

type FleetHistoryRes = {
  ok: boolean;
  history: FleetHistoryRow[];
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

type OrchestrationRunRow = {
  run_id: string;
  agent_id: string;
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

type LedfxFleetRes = {
  ok: boolean;
  cached?: boolean;
  generated_at?: number;
  ttl_s?: number;
  summary?: {
    total?: number;
    enabled?: number;
    healthy?: number;
    agents?: Record<string, unknown>;
  };
};

type FleetHealthRes = {
  ok: boolean;
  cached?: boolean;
  generated_at?: number;
  ttl_s?: number;
  summary?: {
    total?: number;
    online?: number;
    wled_ok?: number;
    fpp_ok?: number;
    ledfx_ok?: number;
  };
  agents?: Array<{
    agent_id: string;
    name?: string | null;
    base_url?: string | null;
    role?: string | null;
    tags?: string[] | null;
    role_override?: string | null;
    tags_override?: string[] | null;
    online?: boolean | null;
    updated_at?: number | null;
    wled?: Record<string, any> | null;
    fpp?: Record<string, any> | null;
    ledfx?: Record<string, any> | null;
    last_applied?: Record<string, any> | null;
    error?: string | null;
  }>;
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

type OverridesImportRes = {
  ok?: boolean;
  processed?: number;
  upserted?: number;
  errors?: string[];
  dry_run?: boolean;
  changes?: Array<{
    agent_id?: string;
    action?: string;
    role_before?: string | null;
    tags_before?: string[] | null;
    role_after?: string | null;
    tags_after?: string[] | null;
  }>;
  change_summary?: Record<string, number>;
};

function fmtAge(ageS: number | null): string {
  if (ageS == null) return "—";
  if (ageS < 60) return `${Math.round(ageS)}s`;
  if (ageS < 3600) return `${Math.round(ageS / 60)}m`;
  return `${Math.round(ageS / 3600)}h`;
}

function fmtTs(ts: number | null | undefined): string {
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

function healthColor(
  ok: boolean | null | undefined,
): "success" | "error" | "default" {
  if (ok === true) return "success";
  if (ok === false) return "error";
  return "default";
}

export function FleetTools() {
  const { user } = useAuth();
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [data, setData] = useState<FleetStatusRes | null>(null);
  const [history, setHistory] = useState<FleetHistoryRow[]>([]);
  const [historyMeta, setHistoryMeta] = useState<PageMeta | null>(null);
  const [historyRetention, setHistoryRetention] = useState<RetentionRes | null>(
    null,
  );
  const [historyRetentionError, setHistoryRetentionError] = useState<string | null>(
    null,
  );
  const [historyRetentionBusy, setHistoryRetentionBusy] = useState(false);
  const [historyRetentionOverrideRows, setHistoryRetentionOverrideRows] =
    useState("");
  const [historyRetentionOverrideDays, setHistoryRetentionOverrideDays] =
    useState("");
  const [historyRetentionResult, setHistoryRetentionResult] =
    useState<Record<string, unknown> | null>(null);
  const [runs, setRuns] = useState<OrchestrationRunRow[]>([]);
  const [runsMeta, setRunsMeta] = useState<PageMeta | null>(null);
  const [ledfxFleet, setLedfxFleet] = useState<LedfxFleetRes | null>(null);
  const [health, setHealth] = useState<FleetHealthRes | null>(null);
  const [healthError, setHealthError] = useState<string | null>(null);
  const [runDetails, setRunDetails] = useState<
    Record<string, OrchestrationRunDetailRes>
  >({});
  const [expandedRunId, setExpandedRunId] = useState<string | null>(null);
  const [detailBusyId, setDetailBusyId] = useState<string | null>(null);

  const [overrideOpen, setOverrideOpen] = useState(false);
  const [overrideAgent, setOverrideAgent] =
    useState<FleetStatusRes["agents"][0] | null>(null);
  const [overrideRole, setOverrideRole] = useState("");
  const [overrideTags, setOverrideTags] = useState("");
  const [overrideBusy, setOverrideBusy] = useState(false);
  const [overrideError, setOverrideError] = useState<string | null>(null);
  const [overrideImportFormat, setOverrideImportFormat] = useState<"csv" | "json">(
    "csv",
  );
  const [overrideImportFile, setOverrideImportFile] = useState<File | null>(null);
  const [overrideImportBusy, setOverrideImportBusy] = useState(false);
  const [overrideImportError, setOverrideImportError] = useState<string | null>(
    null,
  );
  const [overrideImportResult, setOverrideImportResult] =
    useState<OverridesImportRes | null>(null);
  const [overrideExportBusy, setOverrideExportBusy] = useState(false);
  const [overrideExportError, setOverrideExportError] = useState<string | null>(
    null,
  );

  const [query, setQuery] = useState("");
  const [onlineOnly, setOnlineOnly] = useState(false);
  const [configuredOnly, setConfiguredOnly] = useState(false);
  const [historyLimit, setHistoryLimit] = useState("100");
  const [historyOffset, setHistoryOffset] = useState("0");
  const [historyAgentFilter, setHistoryAgentFilter] = useState("");
  const [historyRoleFilter, setHistoryRoleFilter] = useState("");
  const [historyTagFilter, setHistoryTagFilter] = useState("");
  const [historySinceFilter, setHistorySinceFilter] = useState("");
  const [historyUntilFilter, setHistoryUntilFilter] = useState("");
  const [runsLimit, setRunsLimit] = useState("100");
  const [runsOffset, setRunsOffset] = useState("0");
  const [detailStepsLimit, setDetailStepsLimit] = useState("50");
  const [detailPeersLimit, setDetailPeersLimit] = useState("50");
  const [detailStepsOffset, setDetailStepsOffset] = useState("0");
  const [detailPeersOffset, setDetailPeersOffset] = useState("0");
  const [detailStepStatus, setDetailStepStatus] = useState("");
  const [detailPeerStatus, setDetailPeerStatus] = useState("");
  const [detailStepsFailuresOnly, setDetailStepsFailuresOnly] = useState(false);
  const [detailPeersFailuresOnly, setDetailPeersFailuresOnly] = useState(true);

  const buildHistoryQuery = (lim: number, off: number) => {
    const q = new URLSearchParams();
    q.set("limit", String(lim));
    if (off > 0) q.set("offset", String(off));
    if (historyAgentFilter.trim()) {
      q.set("agent_id", historyAgentFilter.trim());
    }
    if (historyRoleFilter.trim()) {
      q.set("role", historyRoleFilter.trim());
    }
    if (historyTagFilter.trim()) {
      q.set("tag", historyTagFilter.trim());
    }
    const since = toEpochSeconds(historySinceFilter);
    const until = toEpochSeconds(historyUntilFilter);
    if (since) q.set("since", since);
    if (until) q.set("until", until);
    return q;
  };

  const fetchHistoryRetention = async () => {
    setHistoryRetentionError(null);
    try {
      const res = await api<RetentionRes>("/v1/fleet/history/retention", {
        method: "GET",
      });
      setHistoryRetention(res);
    } catch (e) {
      setHistoryRetentionError(e instanceof Error ? e.message : String(e));
    }
  };

  const runHistoryRetention = async () => {
    setHistoryRetentionBusy(true);
    setHistoryRetentionError(null);
    setHistoryRetentionResult(null);
    try {
      const params = new URLSearchParams();
      const rows = parseInt(historyRetentionOverrideRows || "", 10);
      if (Number.isFinite(rows) && rows > 0) params.set("max_rows", String(rows));
      const days = parseInt(historyRetentionOverrideDays || "", 10);
      if (Number.isFinite(days) && days > 0) params.set("max_days", String(days));
      const url =
        params.toString().length > 0
          ? `/v1/fleet/history/retention?${params.toString()}`
          : "/v1/fleet/history/retention";
      const res = await api<{ ok?: boolean; result?: Record<string, unknown> }>(
        url,
        { method: "POST", json: {} },
      );
      setHistoryRetentionResult(res.result ?? null);
      await fetchHistoryRetention();
    } catch (e) {
      setHistoryRetentionError(e instanceof Error ? e.message : String(e));
    } finally {
      setHistoryRetentionBusy(false);
    }
  };

  const refresh = async (opts?: { historyOffset?: number; runsOffset?: number }) => {
    setBusy(true);
    setError(null);
    try {
      const res = await api<FleetStatusRes>("/v1/fleet/status", {
        method: "GET",
      });
      setData(res);
      try {
        const lim = parseInt(historyLimit || "100", 10) || 100;
        const off = opts?.historyOffset ?? (parseInt(historyOffset || "0", 10) || 0);
        if (opts?.historyOffset != null) setHistoryOffset(String(off));
        const q = buildHistoryQuery(lim, off);
        const hist = await api<FleetHistoryRes>(
          `/v1/fleet/history?${q.toString()}`,
          {
            method: "GET",
          },
        );
        setHistory(hist.history ?? []);
        setHistoryMeta({
          count: hist.count ?? hist.history?.length ?? 0,
          limit: hist.limit ?? lim,
          offset: hist.offset ?? off,
          next_offset: hist.next_offset ?? null,
        });
      } catch {
        setHistory([]);
        setHistoryMeta(null);
      }
      try {
        const runLim = parseInt(runsLimit || "100", 10) || 100;
        const runOff = opts?.runsOffset ?? (parseInt(runsOffset || "0", 10) || 0);
        if (opts?.runsOffset != null) setRunsOffset(String(runOff));
        const q = new URLSearchParams();
        q.set("limit", String(runLim));
        q.set("scope", "fleet");
        if (runOff > 0) q.set("offset", String(runOff));
        const runsRes = await api<OrchestrationRunsRes>(
          `/v1/orchestration/runs?${q.toString()}`,
          { method: "GET" },
        );
        setRuns(runsRes.runs ?? []);
        setRunsMeta({
          count: runsRes.count ?? runsRes.runs?.length ?? 0,
          limit: runsRes.limit ?? runLim,
          offset: runsRes.offset ?? runOff,
          next_offset: runsRes.next_offset ?? null,
        });
      } catch {
        setRuns([]);
        setRunsMeta(null);
      }
      try {
        const ledfxRes = await api<LedfxFleetRes>("/v1/ledfx/fleet", {
          method: "GET",
        });
        setLedfxFleet(ledfxRes);
      } catch {
        setLedfxFleet(null);
      }
      try {
        const healthRes = await api<FleetHealthRes>("/v1/fleet/health", {
          method: "GET",
        });
        setHealth(healthRes);
        setHealthError(null);
      } catch (e) {
        setHealth(null);
        setHealthError(e instanceof Error ? e.message : String(e));
      }
    } catch (e) {
      setData(null);
      setHistory([]);
      setRuns([]);
      setHealth(null);
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

  const exportRuns = async (format: "csv" | "json") => {
    const q = new URLSearchParams();
    const lim = Math.min(parseInt(runsLimit || "100", 10) || 100, 20000);
    const off = parseInt(runsOffset || "0", 10) || 0;
    q.set("limit", String(lim));
    if (off > 0) q.set("offset", String(off));
    q.set("scope", "fleet");
    q.set("format", format);
    const filename =
      format === "json" ? "fleet_orchestration_runs.json" : "fleet_orchestration_runs.csv";
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
        ? `fleet_orchestration_steps_${runId}.json`
        : `fleet_orchestration_steps_${runId}.csv`;
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
        ? `fleet_orchestration_peers_${runId}.json`
        : `fleet_orchestration_peers_${runId}.csv`;
    await downloadExport(
      `/v1/orchestration/runs/${encodeURIComponent(runId)}/peers/export?${q.toString()}`,
      filename,
    );
  };

  const exportHistory = async (format: "csv" | "json") => {
    const lim = Math.min(parseInt(historyLimit || "100", 10) || 100, 20000);
    const off = parseInt(historyOffset || "0", 10) || 0;
    const q = buildHistoryQuery(lim, off);
    q.set("format", format);
    const filename = format === "json" ? "fleet_history.json" : "fleet_history.csv";
    await downloadExport(`/v1/fleet/history/export?${q.toString()}`, filename);
  };

  const pageHistory = async (dir: "prev" | "next") => {
    const limitVal =
      historyMeta?.limit ?? (parseInt(historyLimit || "100", 10) || 100);
    const current =
      historyMeta?.offset ?? (parseInt(historyOffset || "0", 10) || 0);
    const next =
      dir === "next"
        ? historyMeta?.next_offset ?? null
        : Math.max(0, current - limitVal);
    if (next == null) return;
    await refresh({ historyOffset: next });
  };

  const pageRuns = async (dir: "prev" | "next") => {
    const limitVal =
      runsMeta?.limit ?? (parseInt(runsLimit || "100", 10) || 100);
    const current = runsMeta?.offset ?? (parseInt(runsOffset || "0", 10) || 0);
    const next =
      dir === "next" ? runsMeta?.next_offset ?? null : Math.max(0, current - limitVal);
    if (next == null) return;
    await refresh({ runsOffset: next });
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
    const current =
      meta?.offset ?? (parseInt(detailStepsOffset || "0", 10) || 0);
    const next =
      dir === "next" ? meta?.next_offset ?? null : Math.max(0, current - limitVal);
    if (next == null) return;
    setDetailStepsOffset(String(next));
    await fetchRunDetails(runId, { stepsOffset: next });
  };

  const pageRunPeers = async (runId: string, dir: "prev" | "next") => {
    const detail = runDetails[runId];
    if (!detail) return;
    const meta = detail.peers_meta;
    const limitVal = meta?.limit ?? (parseInt(detailPeersLimit || "50", 10) || 50);
    const current =
      meta?.offset ?? (parseInt(detailPeersOffset || "0", 10) || 0);
    const next =
      dir === "next" ? meta?.next_offset ?? null : Math.max(0, current - limitVal);
    if (next == null) return;
    setDetailPeersOffset(String(next));
    await fetchRunDetails(runId, { peersOffset: next });
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
    void fetchHistoryRetention();
  }, []);

  useEventRefresh({
    types: ["fleet", "orchestration", "tick"],
    refresh,
    minIntervalMs: 3000,
  });

  const agents = useMemo(() => {
    const q = query.trim().toLowerCase();
    const rows = data?.agents ?? [];
    return rows.filter((a) => {
      if (onlineOnly && !a.online) return false;
      if (configuredOnly && !a.configured) return false;
      if (!q) return true;
      const roleVal = a.role_effective ?? a.role ?? "";
      const tagVals = (a.tags_effective ?? a.tags ?? []).map(String);
      const hay = [
        a.agent_id,
        a.name ?? "",
        roleVal,
        a.base_url ?? "",
        ...tagVals,
      ]
        .join(" ")
        .toLowerCase();
      return hay.includes(q);
    });
  }, [configuredOnly, data?.agents, onlineOnly, query]);

  const roles = useMemo(() => {
    const s = new Set<string>();
    for (const a of data?.agents ?? []) {
      const roleVal = a.role_effective ?? a.role;
      if (roleVal) s.add(String(roleVal));
    }
    return Array.from(s).sort();
  }, [data?.agents]);

  const tags = useMemo(() => {
    const s = new Set<string>();
    for (const a of data?.agents ?? []) {
      for (const t of a.tags_effective ?? a.tags ?? []) s.add(String(t));
    }
    return Array.from(s).sort();
  }, [data?.agents]);

  const ledfxSummary = ledfxFleet?.summary;

  const ledfxAgents = useMemo(() => {
    const agents = ledfxFleet?.summary?.agents;
    if (!agents || typeof agents !== "object") return [];
    return Object.entries(agents);
  }, [ledfxFleet]);

  const healthSummary = health?.summary;
  const healthAgents = useMemo(() => health?.agents ?? [], [health]);

  const historyRows = useMemo(() => history ?? [], [history]);
  const historyRetentionDrift = historyRetention?.drift?.drift;
  const isAdmin = (user?.role || "") === "admin";
  const runRows = useMemo(() => runs ?? [], [runs]);

  const copy = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
    } catch {
      // ignore
    }
  };

  const openOverrideDialog = (agent: FleetStatusRes["agents"][0]) => {
    setOverrideAgent(agent);
    setOverrideRole(String(agent.role_override ?? ""));
    const tagOverride = agent.tags_override ?? null;
    setOverrideTags(tagOverride ? tagOverride.join(",") : "");
    setOverrideError(null);
    setOverrideOpen(true);
  };

  const closeOverrideDialog = () => {
    if (overrideBusy) return;
    setOverrideOpen(false);
    setOverrideAgent(null);
    setOverrideError(null);
  };

  const saveOverride = async () => {
    if (!overrideAgent) return;
    setOverrideBusy(true);
    setOverrideError(null);
    try {
      const roleVal = overrideRole.trim();
      const tagsVal = overrideTags
        .split(",")
        .map((t) => t.trim())
        .filter(Boolean);
      await api(`/v1/fleet/overrides/${encodeURIComponent(overrideAgent.agent_id)}`, {
        method: "PUT",
        json: {
          role: roleVal ? roleVal : null,
          tags: tagsVal.length ? tagsVal : null,
        },
      });
      await refresh();
      setOverrideOpen(false);
    } catch (e) {
      setOverrideError(e instanceof Error ? e.message : String(e));
    } finally {
      setOverrideBusy(false);
    }
  };

  const clearOverride = async () => {
    if (!overrideAgent) return;
    setOverrideBusy(true);
    setOverrideError(null);
    try {
      await api(
        `/v1/fleet/overrides/${encodeURIComponent(overrideAgent.agent_id)}`,
        {
          method: "DELETE",
        },
      );
      await refresh();
      setOverrideOpen(false);
    } catch (e) {
      setOverrideError(e instanceof Error ? e.message : String(e));
    } finally {
      setOverrideBusy(false);
    }
  };

  const onImportFile = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0] ?? null;
    setOverrideImportFile(file);
    if (!file) return;
    const lower = file.name.toLowerCase();
    if (lower.endsWith(".json")) {
      setOverrideImportFormat("json");
    } else if (lower.endsWith(".csv")) {
      setOverrideImportFormat("csv");
    }
  };

  const importOverrides = async (dryRun: boolean) => {
    if (!overrideImportFile) return;
    setOverrideImportBusy(true);
    setOverrideImportError(null);
    setOverrideImportResult(null);
    try {
      const text = await overrideImportFile.text();
      const format = overrideImportFormat;
      const q = new URLSearchParams();
      q.set("format", format);
      if (dryRun) q.set("dry_run", "true");
      const resp = await fetch(
        `/v1/fleet/overrides/import?${q.toString()}`,
        {
          method: "POST",
          credentials: "include",
          headers: {
            "Content-Type": format === "json" ? "application/json" : "text/csv",
            ...csrfHeaders("POST"),
          },
          body: text,
        },
      );
      const contentType = resp.headers.get("content-type") ?? "";
      const payload = contentType.includes("application/json")
        ? await resp.json().catch(() => null)
        : await resp.text().catch(() => "");
      if (!resp.ok) {
        const msg =
          (payload && (payload.detail || payload.error)) ||
          (typeof payload === "string" && payload.trim()) ||
          `HTTP ${resp.status}`;
        throw new Error(msg);
      }
      setOverrideImportResult(payload as OverridesImportRes);
      await refresh();
    } catch (e) {
      setOverrideImportError(e instanceof Error ? e.message : String(e));
    } finally {
      setOverrideImportBusy(false);
    }
  };

  const exportOverrides = async (format: "csv" | "json") => {
    setOverrideExportBusy(true);
    setOverrideExportError(null);
    try {
      const resp = await fetch(
        `/v1/fleet/overrides/export?format=${encodeURIComponent(format)}`,
        {
          method: "GET",
          credentials: "include",
        },
      );
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
        format === "json" ? "fleet_overrides.json" : "fleet_overrides.csv";
      anchor.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      setOverrideExportError(e instanceof Error ? e.message : String(e));
    } finally {
      setOverrideExportBusy(false);
    }
  };

  const downloadTemplate = async () => {
    setOverrideExportBusy(true);
    setOverrideExportError(null);
    try {
      const resp = await fetch("/v1/fleet/overrides/template", {
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
      anchor.download = "fleet_overrides_template.csv";
      anchor.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      setOverrideExportError(e instanceof Error ? e.message : String(e));
    } finally {
      setOverrideExportBusy(false);
    }
  };

  return (
    <Stack spacing={2}>
      {error ? <Alert severity="error">{error}</Alert> : null}

      <Card>
        <CardContent>
          <Stack direction="row" spacing={1} sx={{ alignItems: "center" }}>
            <HubIcon />
            <Typography variant="h6">Fleet</Typography>
          </Stack>
          <Typography variant="body2" color="text.secondary">
            Status is derived from SQL heartbeats (<code>/v1/fleet/status</code>
            ).
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            Online threshold:{" "}
            <code>{Math.round(data?.stale_after_s ?? 0)}</code>s · Agents:{" "}
            <code>{data?.summary?.agents ?? 0}</code> · Online:{" "}
            <code>{data?.summary?.online ?? 0}</code> · Configured:{" "}
            <code>{data?.summary?.configured ?? 0}</code>
          </Typography>
        </CardContent>
        <CardActions>
          <Button
            startIcon={<RefreshIcon />}
            onClick={() => void refresh()}
            disabled={busy}
          >
            Refresh
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Fleet health</Typography>
          <Typography variant="body2" color="text.secondary">
            Cached WLED/FPP/LedFx health from <code>/v1/fleet/health</code>.
          </Typography>
          <Stack direction="row" spacing={1} sx={{ mt: 2, flexWrap: "wrap" }}>
            <Chip
              size="small"
              label={`total ${healthSummary?.total ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`online ${healthSummary?.online ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`wled ok ${healthSummary?.wled_ok ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`fpp ok ${healthSummary?.fpp_ok ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`ledfx ok ${healthSummary?.ledfx_ok ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`cached ${health?.cached ? "yes" : "no"}`}
              variant="outlined"
            />
          </Stack>
          {health ? (
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
              snapshot=<code>{fmtTs(health.generated_at ?? null)}</code> · ttl=
              <code>{Math.round(health.ttl_s ?? 0)}</code>s
            </Typography>
          ) : null}
          {healthError ? (
            <Alert severity="warning" sx={{ mt: 2 }}>
              {healthError}
            </Alert>
          ) : null}
          <Stack spacing={1} sx={{ mt: 2 }}>
            {healthAgents.length ? (
              healthAgents.map((entry) => {
                const wledEnabled = Boolean(entry.wled?.enabled);
                const wledOk = entry.wled?.ok;
                const fppEnabled = Boolean(entry.fpp?.enabled);
                const fppOk = entry.fpp?.ok;
                const ledfxEnabled = Boolean(
                  entry.ledfx?.ledfx_enabled ?? entry.ledfx?.enabled,
                );
                const ledfxOk = entry.ledfx?.health ?? entry.ledfx?.ok;
                const onlineLabel =
                  entry.online === true
                    ? "online"
                    : entry.online === false
                      ? "offline"
                      : "unknown";
                const onlineColor =
                  entry.online === true
                    ? "success"
                    : entry.online === false
                      ? "error"
                      : "default";
                const seq = entry.last_applied?.sequence ?? null;
                const look = entry.last_applied?.look ?? null;
                const lastSeq = seq?.name || seq?.file;
                const lastLook = look?.name || look?.file;
                return (
                  <Stack
                    key={`${entry.agent_id}-${entry.base_url ?? "na"}`}
                    spacing={0.5}
                    sx={{
                      border: "1px solid rgba(255,255,255,0.12)",
                      borderRadius: 1,
                      p: 1,
                    }}
                  >
                    <Stack
                      direction="row"
                      spacing={1}
                      sx={{ alignItems: "center", flexWrap: "wrap" }}
                    >
                      <Typography variant="body2">
                        <code>{entry.agent_id}</code>
                      </Typography>
                      <Chip
                        size="small"
                        label={onlineLabel}
                        color={onlineColor}
                        variant="outlined"
                      />
                      <Chip
                        size="small"
                        label={
                          wledEnabled
                            ? wledOk
                              ? "wled ok"
                              : "wled err"
                            : "wled off"
                        }
                        color={wledEnabled ? healthColor(wledOk) : "default"}
                        variant="outlined"
                      />
                      <Chip
                        size="small"
                        label={
                          fppEnabled
                            ? fppOk
                              ? "fpp ok"
                              : "fpp err"
                            : "fpp off"
                        }
                        color={fppEnabled ? healthColor(fppOk) : "default"}
                        variant="outlined"
                      />
                      <Chip
                        size="small"
                        label={
                          ledfxEnabled
                            ? ledfxOk
                              ? "ledfx ok"
                              : "ledfx err"
                            : "ledfx off"
                        }
                        color={ledfxEnabled ? healthColor(ledfxOk) : "default"}
                        variant="outlined"
                      />
                    </Stack>
                    <Typography variant="body2" color="text.secondary">
                      name=<code>{entry.name ?? "—"}</code> · base_url=
                      <code>{entry.base_url ?? "—"}</code>
                    </Typography>
                    {entry.role || entry.tags?.length ? (
                      <Typography variant="body2" color="text.secondary">
                        role=<code>{entry.role ?? "—"}</code> · tags=
                        <code>
                          {entry.tags?.length ? entry.tags.join(",") : "—"}
                        </code>
                      </Typography>
                    ) : null}
                    {lastSeq || lastLook ? (
                      <Typography variant="body2" color="text.secondary">
                        last_seq=<code>{lastSeq ?? "—"}</code> · last_look=
                        <code>{lastLook ?? "—"}</code>
                      </Typography>
                    ) : null}
                    {entry.error ? (
                      <Typography variant="body2" color="error">
                        {entry.error}
                      </Typography>
                    ) : null}
                  </Stack>
                );
              })
            ) : (
              <Typography variant="body2" color="text.secondary">
                No health entries.
              </Typography>
            )}
          </Stack>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Overrides import/export</Typography>
          <Typography variant="body2" color="text.secondary">
            Bulk manage role/tag overrides (CSV or JSON).
          </Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            {overrideImportError ? (
              <Alert severity="error">{overrideImportError}</Alert>
            ) : null}
            {overrideExportError ? (
              <Alert severity="error">{overrideExportError}</Alert>
            ) : null}
            {overrideImportResult ? (
              <Alert
                severity={
                  overrideImportResult.errors?.length ? "warning" : "success"
                }
              >
                {overrideImportResult.dry_run ? "Dry run" : "Imported"}: processed{" "}
                <code>{overrideImportResult.processed ?? 0}</code>
                {" · "}
                upserted <code>{overrideImportResult.upserted ?? 0}</code>
                {" · "}
                errors <code>{overrideImportResult.errors?.length ?? 0}</code>
                {overrideImportResult.change_summary ? (
                  <>
                    {" · "}insert{" "}
                    <code>{overrideImportResult.change_summary.insert ?? 0}</code>{" "}
                    · update{" "}
                    <code>{overrideImportResult.change_summary.update ?? 0}</code>{" "}
                    · noop{" "}
                    <code>{overrideImportResult.change_summary.noop ?? 0}</code>
                  </>
                ) : null}
              </Alert>
            ) : null}

            <Stack direction="row" spacing={1} sx={{ alignItems: "center" }}>
              <Button
                variant="outlined"
                component="label"
                disabled={overrideImportBusy}
              >
                Choose file
                <input
                  type="file"
                  hidden
                  accept=".csv,.json"
                  onChange={onImportFile}
                />
              </Button>
              <Typography variant="body2" color="text.secondary">
                {overrideImportFile ? overrideImportFile.name : "No file selected"}
              </Typography>
            </Stack>

            <FormControl size="small" fullWidth>
              <InputLabel>Format</InputLabel>
              <Select
                label="Format"
                value={overrideImportFormat}
                onChange={(e) =>
                  setOverrideImportFormat(e.target.value as "csv" | "json")
                }
                disabled={overrideImportBusy}
              >
                <MenuItem value="csv">CSV</MenuItem>
                <MenuItem value="json">JSON</MenuItem>
              </Select>
            </FormControl>

            <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap" }}>
              <Button
                variant="contained"
                onClick={() => void importOverrides(false)}
                disabled={overrideImportBusy || !overrideImportFile}
              >
                Import overrides
              </Button>
              <Button
                variant="outlined"
                onClick={() => void importOverrides(true)}
                disabled={overrideImportBusy || !overrideImportFile}
              >
                Preview changes
              </Button>
              <Button onClick={() => void downloadTemplate()} disabled={overrideExportBusy}>
                Download template
              </Button>
              <Button
                onClick={() => void exportOverrides("csv")}
                disabled={overrideExportBusy}
              >
                Export CSV
              </Button>
              <Button
                onClick={() => void exportOverrides("json")}
                disabled={overrideExportBusy}
              >
                Export JSON
              </Button>
            </Stack>

            {overrideImportResult?.errors?.length ? (
              <Stack spacing={0.5}>
                {overrideImportResult.errors.slice(0, 5).map((err, idx) => (
                  <Typography key={idx} variant="body2" color="error">
                    {err}
                  </Typography>
                ))}
                {overrideImportResult.errors.length > 5 ? (
                  <Typography variant="body2" color="text.secondary">
                    ...and {overrideImportResult.errors.length - 5} more
                  </Typography>
                ) : null}
              </Stack>
            ) : null}
            {overrideImportResult?.dry_run &&
            overrideImportResult.changes?.length ? (
              <Stack spacing={0.5}>
                {overrideImportResult.changes.slice(0, 5).map((change, idx) => (
                  <Typography key={idx} variant="body2">
                    <code>{change.agent_id ?? "—"}</code> ·{" "}
                    <code>{change.action ?? "noop"}</code> · role{" "}
                    <code>{change.role_before ?? "—"}</code> →{" "}
                    <code>{change.role_after ?? "—"}</code> · tags{" "}
                    <code>
                      {(change.tags_before || []).length
                        ? (change.tags_before || []).join(",")
                        : "—"}
                    </code>{" "}
                    →{" "}
                    <code>
                      {(change.tags_after || []).length
                        ? (change.tags_after || []).join(",")
                        : "—"}
                    </code>
                  </Typography>
                ))}
                {overrideImportResult.changes.length > 5 ? (
                  <Typography variant="body2" color="text.secondary">
                    ...and {overrideImportResult.changes.length - 5} more
                  </Typography>
                ) : null}
              </Stack>
            ) : null}
          </Stack>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Target selectors</Typography>
          <Typography variant="body2" color="text.secondary">
            Copy/paste into <code>targets</code> fields (Dashboard / Scheduler).
          </Typography>

          <Stack spacing={1} sx={{ mt: 2 }}>
            <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap" }}>
              <Chip
                label="*"
                variant="outlined"
                onClick={() => void copy("*")}
                onDelete={() => void copy("*")}
                deleteIcon={<ContentCopyIcon />}
              />
              {roles.map((r) => (
                <Chip
                  key={`role:${r}`}
                  label={`role:${r}`}
                  variant="outlined"
                  onClick={() => void copy(`role:${r}`)}
                  onDelete={() => void copy(`role:${r}`)}
                  deleteIcon={<ContentCopyIcon />}
                />
              ))}
              {tags.map((t) => (
                <Chip
                  key={`tag:${t}`}
                  label={`tag:${t}`}
                  variant="outlined"
                  onClick={() => void copy(`tag:${t}`)}
                  onDelete={() => void copy(`tag:${t}`)}
                  deleteIcon={<ContentCopyIcon />}
                />
              ))}
            </Stack>
          </Stack>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Filters</Typography>
          <Stack spacing={1} sx={{ mt: 2 }}>
            <TextField
              label="Search"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              disabled={busy}
              helperText="Search by agent_id, name, role, base_url, or tags."
            />
            <FormControlLabel
              control={
                <Switch
                  checked={onlineOnly}
                  onChange={(e) => setOnlineOnly(e.target.checked)}
                />
              }
              label="Online only"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={configuredOnly}
                  onChange={(e) => setConfiguredOnly(e.target.checked)}
                />
              }
              label="Configured only"
            />
          </Stack>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Agents</Typography>
          <Stack spacing={1} sx={{ mt: 2 }}>
            {agents.length ? (
              agents.map((a) => {
                const roleVal = a.role_effective ?? a.role;
                const tagsVal = a.tags_effective ?? a.tags ?? [];
                const hasOverride = a.role_override != null || a.tags_override != null;
                return (
                  <Stack
                    key={a.agent_id}
                    spacing={0.5}
                    sx={{
                      border: "1px solid rgba(255,255,255,0.12)",
                      borderRadius: 1,
                      p: 1,
                    }}
                  >
                    <Stack
                      direction="row"
                      spacing={1}
                      sx={{ alignItems: "center", flexWrap: "wrap" }}
                    >
                      <Typography variant="body2">
                        <code>{a.agent_id}</code>
                      </Typography>
                      {roleVal ? (
                        <Chip size="small" label={roleVal} variant="outlined" />
                      ) : null}
                      {hasOverride ? (
                        <Chip size="small" label="override" variant="outlined" />
                      ) : null}
                      <Chip
                        size="small"
                        label={a.configured ? "configured" : "discovered"}
                        variant="outlined"
                      />
                      <Chip
                        size="small"
                        color={a.online ? "success" : "error"}
                        label={a.online ? "online" : "offline"}
                        variant="outlined"
                      />
                      <Typography variant="body2" color="text.secondary">
                        age=<code>{fmtAge(a.age_s)}</code>
                      </Typography>
                      <Button
                        size="small"
                        onClick={() => openOverrideDialog(a)}
                      >
                        Edit override
                      </Button>
                    </Stack>

                    <Typography variant="body2" color="text.secondary">
                      name=<code>{a.name ?? "—"}</code> · base_url=
                      <code>{a.base_url ?? "—"}</code>
                    </Typography>
                    {tagsVal.length ? (
                      <Typography variant="body2" color="text.secondary">
                        tags=<code>{tagsVal.join(",")}</code>
                      </Typography>
                    ) : null}
                    {a.role_override != null || a.tags_override != null ? (
                      <Typography variant="body2" color="text.secondary">
                        override_role=<code>{a.role_override ?? "—"}</code> ·
                        override_tags=
                        <code>
                          {(a.tags_override ?? []).length
                            ? (a.tags_override ?? []).join(",")
                            : "—"}
                        </code>
                      </Typography>
                    ) : null}
                  </Stack>
                );
              })
            ) : (
              <Typography variant="body2">No agents.</Typography>
            )}
          </Stack>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">LedFx Fleet</Typography>
          <Typography variant="body2" color="text.secondary">
            Snapshot of LedFx health and last applied scenes/effects from peers.
          </Typography>
          <Stack direction="row" spacing={1} sx={{ mt: 2, flexWrap: "wrap" }}>
            <Chip
              size="small"
              label={`total ${ledfxSummary?.total ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`enabled ${ledfxSummary?.enabled ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`healthy ${ledfxSummary?.healthy ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`cached ${ledfxFleet?.cached ? "yes" : "no"}`}
              variant="outlined"
            />
          </Stack>
          {ledfxFleet ? (
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
              snapshot=<code>{fmtTs(ledfxFleet.generated_at ?? null)}</code> · ttl=
              <code>{Math.round(ledfxFleet.ttl_s ?? 0)}</code>s
            </Typography>
          ) : null}
          <Stack spacing={1} sx={{ mt: 2 }}>
            {ledfxAgents.length ? (
              ledfxAgents.map(([name, raw]) => {
                const entry = raw as any;
                const lastScene = entry?.last_scene?.name || entry?.last_scene?.file;
                const lastEffect =
                  entry?.last_effect?.name || entry?.last_effect?.file;
                return (
                  <Stack
                    key={name}
                    spacing={0.5}
                    sx={{
                      border: "1px solid rgba(255,255,255,0.12)",
                      borderRadius: 1,
                      p: 1,
                    }}
                  >
                    <Stack
                      direction="row"
                      spacing={1}
                      sx={{ alignItems: "center", flexWrap: "wrap" }}
                    >
                      <Typography variant="body2">
                        <code>{name}</code>
                      </Typography>
                      <Chip
                        size="small"
                        label={entry?.health ? "healthy" : "unhealthy"}
                        color={entry?.health ? "success" : "warning"}
                        variant="outlined"
                      />
                      <Chip
                        size="small"
                        label={entry?.ledfx_enabled ? "enabled" : "disabled"}
                        variant="outlined"
                      />
                    </Stack>
                    <Typography variant="body2" color="text.secondary">
                      scene=<code>{lastScene ?? "—"}</code> · effect=
                      <code>{lastEffect ?? "—"}</code>
                    </Typography>
                  </Stack>
                );
              })
            ) : (
              <Typography variant="body2" color="text.secondary">
                No LedFx fleet snapshot yet.
              </Typography>
            )}
          </Stack>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">History Retention</Typography>
          <Typography variant="body2" color="text.secondary">
            Retention status for <code>agent_heartbeat_history</code>.
          </Typography>
          {historyRetentionDrift ? (
            <Alert severity="warning" sx={{ mt: 2 }}>
              Fleet history exceeds retention targets. Run cleanup or adjust{" "}
              <code>AGENT_HISTORY_MAX_ROWS</code>/<code>AGENT_HISTORY_MAX_DAYS</code>.
            </Alert>
          ) : null}
          {historyRetentionError ? (
            <Alert severity="warning" sx={{ mt: 2 }}>
              {historyRetentionError}
            </Alert>
          ) : null}
          {historyRetentionResult ? (
            <Alert severity="success" sx={{ mt: 2 }}>
              Cleanup complete:{" "}
              <code>
                rows {String(historyRetentionResult.deleted_by_rows ?? 0)} · days{" "}
                {String(historyRetentionResult.deleted_by_days ?? 0)}
              </code>
            </Alert>
          ) : null}
          <Stack spacing={1} sx={{ mt: 2 }}>
            <Typography variant="body2">
              Count: <code>{historyRetention?.stats?.count ?? 0}</code> · Oldest{" "}
              <code>{fmtTs(historyRetention?.stats?.oldest ?? null)}</code> · Newest{" "}
              <code>{fmtTs(historyRetention?.stats?.newest ?? null)}</code>
            </Typography>
            <Typography variant="body2">
              Max rows: <code>{historyRetention?.settings?.max_rows ?? 0}</code> · Max
              days: <code>{historyRetention?.settings?.max_days ?? 0}</code>
            </Typography>
            <Typography variant="body2">
              Drift: rows <code>{historyRetention?.drift?.excess_rows ?? 0}</code> ·
              age{" "}
              <code>
                {historyRetention?.drift?.excess_age_s
                  ? `${Math.round(historyRetention.drift.excess_age_s)}s`
                  : "0s"}
              </code>
            </Typography>
            <TextField
              label="Override max rows"
              value={historyRetentionOverrideRows}
              onChange={(e) => setHistoryRetentionOverrideRows(e.target.value)}
              helperText="Optional override for manual cleanup."
              disabled={!isAdmin || historyRetentionBusy}
              inputMode="numeric"
            />
            <TextField
              label="Override max days"
              value={historyRetentionOverrideDays}
              onChange={(e) => setHistoryRetentionOverrideDays(e.target.value)}
              helperText="Optional override for manual cleanup."
              disabled={!isAdmin || historyRetentionBusy}
              inputMode="numeric"
            />
            <Typography variant="body2" color="text.secondary">
              Last cleanup:{" "}
              <code>
                {historyRetention?.last_retention?.at
                  ? fmtTs(historyRetention.last_retention.at)
                  : "—"}
              </code>
            </Typography>
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            onClick={() => void fetchHistoryRetention()}
            disabled={historyRetentionBusy}
          >
            Refresh retention
          </Button>
          <Button
            variant="contained"
            onClick={() => void runHistoryRetention()}
            disabled={!isAdmin || historyRetentionBusy}
          >
            Run cleanup
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">History</Typography>
          <Typography variant="body2" color="text.secondary">
            Periodic fleet snapshots from <code>/v1/fleet/history</code>.
          </Typography>
          <Stack spacing={1} sx={{ mt: 2 }}>
            <TextField
              label="Limit"
              value={historyLimit}
              onChange={(e) => setHistoryLimit(e.target.value)}
              disabled={busy}
            />
            <TextField
              label="Offset"
              value={historyOffset}
              onChange={(e) => setHistoryOffset(e.target.value)}
              disabled={busy}
            />
            <TextField
              label="Agent filter"
              value={historyAgentFilter}
              onChange={(e) => setHistoryAgentFilter(e.target.value)}
              disabled={busy}
              placeholder="agent-1"
            />
            <TextField
              label="Role filter"
              value={historyRoleFilter}
              onChange={(e) => setHistoryRoleFilter(e.target.value)}
              disabled={busy}
              placeholder="coordinator"
            />
            <TextField
              label="Tag filter"
              value={historyTagFilter}
              onChange={(e) => setHistoryTagFilter(e.target.value)}
              disabled={busy}
              placeholder="outdoor"
            />
            <TextField
              label="Since"
              type="datetime-local"
              value={historySinceFilter}
              onChange={(e) => setHistorySinceFilter(e.target.value)}
              disabled={busy}
              InputLabelProps={{ shrink: true }}
            />
            <TextField
              label="Until"
              type="datetime-local"
              value={historyUntilFilter}
              onChange={(e) => setHistoryUntilFilter(e.target.value)}
              disabled={busy}
              InputLabelProps={{ shrink: true }}
            />
          </Stack>
          {historyMeta ? (
            <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
              Showing <code>{historyMeta.count ?? historyRows.length}</code> · offset=
              <code>{historyMeta.offset ?? 0}</code> · limit=
              <code>{historyMeta.limit ?? historyLimit}</code>
            </Typography>
          ) : null}
          <Stack spacing={1} sx={{ mt: 2 }}>
            {historyRows.length ? (
              historyRows.map((row) => {
                const tags = Array.isArray(row.payload?.tags)
                  ? row.payload?.tags
                  : [];
                return (
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
                      <Typography variant="body2">
                        <code>{row.agent_id}</code>
                      </Typography>
                      {row.role ? (
                        <Chip size="small" label={row.role} variant="outlined" />
                      ) : null}
                      <Typography variant="body2" color="text.secondary">
                        ts=<code>{fmtTs(row.created_at)}</code>
                      </Typography>
                    </Stack>
                    <Typography variant="body2" color="text.secondary">
                      name=<code>{row.name || "—"}</code> · base_url=
                      <code>{row.base_url ?? "—"}</code>
                    </Typography>
                    {tags.length ? (
                      <Typography variant="body2" color="text.secondary">
                        tags=<code>{tags.join(",")}</code>
                      </Typography>
                    ) : null}
                  </Stack>
                );
              })
            ) : (
              <Typography variant="body2">No history entries.</Typography>
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
            onClick={() => void pageHistory("prev")}
            disabled={busy || (historyMeta?.offset ?? 0) <= 0}
          >
            Prev
          </Button>
          <Button
            onClick={() => void pageHistory("next")}
            disabled={busy || historyMeta?.next_offset == null}
          >
            Next
          </Button>
          <Button onClick={() => void exportHistory("csv")} disabled={busy}>
            Export CSV
          </Button>
          <Button onClick={() => void exportHistory("json")} disabled={busy}>
            Export JSON
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Orchestration Runs</Typography>
          <Typography variant="body2" color="text.secondary">
            Fleet orchestration history from <code>/v1/orchestration/runs</code>.
          </Typography>
          <Stack spacing={1} sx={{ mt: 2 }}>
            <TextField
              label="Limit"
              value={runsLimit}
              onChange={(e) => setRunsLimit(e.target.value)}
              disabled={busy}
              inputMode="numeric"
            />
            <TextField
              label="Offset"
              value={runsOffset}
              onChange={(e) => setRunsOffset(e.target.value)}
              disabled={busy}
              inputMode="numeric"
            />
          </Stack>
          {runsMeta ? (
            <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
              Showing <code>{runsMeta.count ?? runRows.length}</code> · offset=
              <code>{runsMeta.offset ?? 0}</code> · limit=
              <code>{runsMeta.limit ?? runsLimit}</code>
            </Typography>
          ) : null}
          <Stack spacing={1} sx={{ mt: 2 }}>
            {runRows.length ? (
              runRows.map((run) => {
                const detail = runDetails[run.run_id];
                const detailBusy = detailBusyId === run.run_id;
                const steps = detail?.steps ?? [];
                const peers = detail?.peers ?? [];
                const stepsMeta = detail?.steps_meta;
                const peersMeta = detail?.peers_meta;
                const stepsOffsetVal =
                  stepsMeta?.offset ?? (parseInt(detailStepsOffset || "0", 10) || 0);
                const peersOffsetVal =
                  peersMeta?.offset ?? (parseInt(detailPeersOffset || "0", 10) || 0);
                const stepsPrevDisabled =
                  busy || detailBusy || stepsOffsetVal <= 0;
                const peersPrevDisabled =
                  busy || detailBusy || peersOffsetVal <= 0;
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
                        disabled={busy || detailBusy}
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
                              disabled={busy || detailBusy}
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

      <Dialog
        open={overrideOpen}
        onClose={closeOverrideDialog}
        fullWidth
        maxWidth="sm"
      >
        <DialogTitle>Fleet override</DialogTitle>
        <DialogContent>
          {overrideError ? <Alert severity="error">{overrideError}</Alert> : null}
          <Stack spacing={2} sx={{ mt: 2 }}>
            <Typography variant="body2" color="text.secondary">
              Agent: <code>{overrideAgent?.agent_id ?? "—"}</code>
            </Typography>
            <TextField
              label="Role override"
              value={overrideRole}
              onChange={(e) => setOverrideRole(e.target.value)}
              helperText="Leave blank to inherit heartbeat role."
              placeholder={overrideAgent?.role_effective ?? overrideAgent?.role ?? ""}
              disabled={overrideBusy}
            />
            <TextField
              label="Tags override (comma-separated)"
              value={overrideTags}
              onChange={(e) => setOverrideTags(e.target.value)}
              helperText="Leave blank to inherit heartbeat tags."
              placeholder={
                overrideAgent?.tags_effective?.join(",") ??
                overrideAgent?.tags?.join(",") ??
                ""
              }
              disabled={overrideBusy}
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={clearOverride} disabled={overrideBusy || !overrideAgent}>
            Clear override
          </Button>
          <Button onClick={saveOverride} disabled={overrideBusy || !overrideAgent}>
            Save
          </Button>
          <Button onClick={closeOverrideDialog} disabled={overrideBusy}>
            Close
          </Button>
        </DialogActions>
      </Dialog>
    </Stack>
  );
}
