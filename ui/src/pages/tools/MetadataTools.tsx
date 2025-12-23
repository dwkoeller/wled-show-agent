import RefreshIcon from "@mui/icons-material/Refresh";
import StorageIcon from "@mui/icons-material/Storage";
import SyncIcon from "@mui/icons-material/Sync";
import {
  Alert,
  Button,
  Card,
  CardActions,
  CardContent,
  FormControlLabel,
  Stack,
  Switch,
  TextField,
  Typography,
} from "@mui/material";
import React, { useEffect, useMemo, useState } from "react";
import { api } from "../../api";
import { useAuth } from "../../auth";
import { AudioWaveformDialog } from "../../components/AudioWaveformDialog";
import { SequencePreviewDialog } from "../../components/SequencePreviewDialog";
import { useEventRefresh } from "../../hooks/useEventRefresh";

type LastApplied = {
  ok?: boolean;
  last_applied?: Record<string, any>;
};

type PackRow = {
  agent_id: string;
  dest_dir: string;
  created_at: number;
  updated_at: number;
  source_name: string | null;
  manifest_path: string | null;
  uploaded_bytes: number;
  unpacked_bytes: number;
  file_count: number;
};

type PacksRes = { ok: boolean; packs: PackRow[] };

type SequenceRow = {
  agent_id: string;
  file: string;
  created_at: number;
  updated_at: number;
  duration_s: number;
  steps_total: number;
};

type SequencesRes = { ok: boolean; sequences: SequenceRow[] };

type AudioRow = {
  agent_id: string;
  id: string;
  created_at: number;
  updated_at: number;
  source_path: string | null;
  beats_path: string | null;
  prefer_ffmpeg: boolean;
  bpm: number | null;
  beat_count: number | null;
  error: string | null;
};

type AudioRes = { ok: boolean; audio_analyses: AudioRow[] };

type ShowConfigRow = {
  agent_id: string;
  file: string;
  created_at: number;
  updated_at: number;
  name: string;
  props_total: number;
  groups_total: number;
  coordinator_base_url: string | null;
  fpp_base_url: string | null;
};

type ShowConfigsRes = { ok: boolean; show_configs: ShowConfigRow[] };

type FseqExportRow = {
  agent_id: string;
  file: string;
  created_at: number;
  updated_at: number;
  source_sequence: string | null;
  bytes_written: number;
  frames: number | null;
  channels: number | null;
  step_ms: number | null;
  duration_s: number | null;
};

type FseqExportsRes = { ok: boolean; fseq_exports: FseqExportRow[] };

type FppScriptRow = {
  agent_id: string;
  file: string;
  created_at: number;
  updated_at: number;
  kind: string;
  bytes_written: number;
};

type FppScriptsRes = { ok: boolean; fpp_scripts: FppScriptRow[] };

type RetentionRes = {
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

type MetaRetentionRes = {
  ok?: boolean;
  tables?: Record<string, RetentionRes>;
};

type ReconcileRes = {
  ok?: boolean;
  skipped?: boolean;
  reason?: string;
  scanned?: Record<string, number>;
  upserted?: Record<string, number>;
};

type ReconcileStatusRes = {
  ok?: boolean;
  exists?: boolean;
  status?: {
    ok?: boolean;
    running?: boolean;
    mode?: string;
    run_id?: number | null;
    cancel_requested?: boolean;
    phase?: string | null;
    started_at?: number | null;
    finished_at?: number | null;
    duration_s?: number | null;
    last_success_at?: number | null;
    last_error?: string | null;
    params?: Record<string, any>;
    last_result?: Record<string, any>;
  };
};

type ReconcileRunRow = {
  id: number;
  started_at: number;
  finished_at?: number | null;
  status: string;
  source?: string | null;
  error?: string | null;
  cancel_requested?: boolean;
  options?: Record<string, any>;
  result?: Record<string, any>;
};

type ReconcileHistoryRes = {
  ok?: boolean;
  runs?: ReconcileRunRow[];
  count?: number;
  limit?: number;
  offset?: number;
  next_offset?: number | null;
};

type JobRow = {
  id: string;
  kind: string;
  status: string;
  created_at: number;
  started_at: number | null;
  finished_at: number | null;
  progress?: { current?: number | null; total?: number | null; message?: string | null };
};

type JobsRes = {
  ok?: boolean;
  jobs?: JobRow[];
  queue?: { size?: number; max?: number; workers?: number };
};

type PreviewCacheRes = {
  ok?: boolean;
  files?: number;
  bytes?: number;
  max_mb?: number;
  max_days?: number;
};

type PreviewCachePurgeRes = {
  ok?: boolean;
  purge?: boolean;
  deleted_files?: number;
  deleted_bytes?: number;
  before_bytes?: number;
  after_bytes?: number;
};

function fmtTs(ts: number | null | undefined): string {
  if (!ts) return "—";
  try {
    return new Date(ts * 1000).toLocaleString();
  } catch {
    return String(ts);
  }
}

const META_RETENTION_TABLES: Array<{ key: string; label: string }> = [
  { key: "pack_ingests", label: "Pack ingests" },
  { key: "sequence_meta", label: "Sequence metadata" },
  { key: "audio_analyses", label: "Audio analyses" },
  { key: "show_configs", label: "Show configs" },
  { key: "fseq_exports", label: "FSEQ exports" },
  { key: "fpp_scripts", label: "FPP scripts" },
];

function fmtBytes(n: number | null | undefined): string {
  const b = Number(n ?? 0);
  if (!Number.isFinite(b) || b <= 0) return "0 B";
  const kb = 1024;
  const mb = kb * 1024;
  const gb = mb * 1024;
  if (b >= gb) return `${(b / gb).toFixed(2)} GB`;
  if (b >= mb) return `${(b / mb).toFixed(2)} MB`;
  if (b >= kb) return `${(b / kb).toFixed(2)} KB`;
  return `${Math.round(b)} B`;
}

export function MetadataTools() {
  const { user } = useAuth();
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const [lastApplied, setLastApplied] = useState<LastApplied | null>(null);
  const [packs, setPacks] = useState<PackRow[]>([]);
  const [sequences, setSequences] = useState<SequenceRow[]>([]);
  const [audio, setAudio] = useState<AudioRow[]>([]);
  const [showConfigs, setShowConfigs] = useState<ShowConfigRow[]>([]);
  const [fseqExports, setFseqExports] = useState<FseqExportRow[]>([]);
  const [fppScripts, setFppScripts] = useState<FppScriptRow[]>([]);
  const [sequencePreview, setSequencePreview] = useState<string | null>(null);
  const [audioPreview, setAudioPreview] = useState<{
    audioPath: string;
    beatsPath?: string | null;
    bpm?: number | null;
  } | null>(null);

  const [reconcilePacks, setReconcilePacks] = useState(true);
  const [reconcileSequences, setReconcileSequences] = useState(true);
  const [reconcileAudio, setReconcileAudio] = useState(false);
  const [reconcileShowConfigs, setReconcileShowConfigs] = useState(true);
  const [reconcileFseqExports, setReconcileFseqExports] = useState(true);
  const [reconcileFppScripts, setReconcileFppScripts] = useState(true);
  const [precomputePreviews, setPrecomputePreviews] = useState(true);
  const [precomputeWaveforms, setPrecomputeWaveforms] = useState(true);
  const [scanLimit, setScanLimit] = useState("5000");
  const [lastReconcile, setLastReconcile] = useState<ReconcileRes | null>(null);
  const [reconcileStatus, setReconcileStatus] =
    useState<ReconcileStatusRes | null>(null);
  const [reconcileHistory, setReconcileHistory] = useState<ReconcileRunRow[]>([]);
  const [historyLimit, setHistoryLimit] = useState("10");
  const [historyOffset, setHistoryOffset] = useState(0);
  const [historyNextOffset, setHistoryNextOffset] = useState<number | null>(null);

  const [precomputeJobs, setPrecomputeJobs] = useState<JobRow[]>([]);
  const [jobQueue, setJobQueue] = useState<JobsRes["queue"] | null>(null);
  const [manualPrecomputeFormats, setManualPrecomputeFormats] = useState("gif");
  const [manualPrecomputeLimit, setManualPrecomputeLimit] = useState("5000");
  const [manualPrecomputePreviews, setManualPrecomputePreviews] = useState(true);
  const [manualPrecomputeWaveforms, setManualPrecomputeWaveforms] = useState(true);
  const [manualPrecomputeResult, setManualPrecomputeResult] = useState<any | null>(
    null,
  );

  const [previewCache, setPreviewCache] = useState<PreviewCacheRes | null>(null);
  const [cacheOverrideMb, setCacheOverrideMb] = useState("");
  const [cacheOverrideDays, setCacheOverrideDays] = useState("");
  const [cacheAction, setCacheAction] = useState<PreviewCachePurgeRes | null>(null);
  const [metaRetention, setMetaRetention] = useState<MetaRetentionRes | null>(null);
  const [metaRetentionError, setMetaRetentionError] = useState<string | null>(null);
  const [metaRetentionBusy, setMetaRetentionBusy] = useState(false);
  const [metaRetentionOverrides, setMetaRetentionOverrides] = useState<
    Record<string, { rows: string; days: string }>
  >({});
  const [metaRetentionResult, setMetaRetentionResult] = useState<
    Record<string, Record<string, unknown> | null>
  >({});

  const needsDb = useMemo(() => {
    const msg = (error ?? "").toLowerCase();
    return (
      msg.includes("database_url is not configured") || msg.includes("database")
    );
  }, [error]);
  const isAdmin = (user?.role || "") === "admin";

  const refresh = async () => {
    setBusy(true);
    setError(null);
    try {
      const histLimit = parseInt(historyLimit || "10", 10) || 10;
      const [la, pk, sq, au, sc, fe, fs, rs, rh, pc, metaRet, jobs] =
        await Promise.all([
          api<LastApplied>("/v1/meta/last_applied?_", { method: "GET" }),
          api<PacksRes>("/v1/meta/packs?limit=50", { method: "GET" }),
          api<SequencesRes>("/v1/meta/sequences?limit=100", { method: "GET" }),
          api<AudioRes>("/v1/meta/audio_analyses?limit=50", { method: "GET" }),
          api<ShowConfigsRes>("/v1/meta/show_configs?limit=50", { method: "GET" }),
          api<FseqExportsRes>("/v1/meta/fseq_exports?limit=50", { method: "GET" }),
          api<FppScriptsRes>("/v1/meta/fpp_scripts?limit=50", { method: "GET" }),
          api<ReconcileStatusRes>("/v1/meta/reconcile/status", {
            method: "GET",
          }).catch(() => null),
          api<ReconcileHistoryRes>(
            `/v1/meta/reconcile/history?limit=${histLimit}&offset=${historyOffset}`,
            { method: "GET" },
          ).catch(() => null),
          api<PreviewCacheRes>("/v1/sequences/preview/cache", {
            method: "GET",
          }).catch(() => null),
          api<MetaRetentionRes>("/v1/meta/retention", { method: "GET" }).catch(
            () => null,
          ),
          api<JobsRes>("/v1/jobs?limit=100", { method: "GET" }).catch(() => null),
        ]);
      setLastApplied(la);
      setPacks(pk.packs ?? []);
      setSequences(sq.sequences ?? []);
      setAudio(au.audio_analyses ?? []);
      setShowConfigs(sc.show_configs ?? []);
      setFseqExports(fe.fseq_exports ?? []);
      setFppScripts(fs.fpp_scripts ?? []);
      if (rs) setReconcileStatus(rs);
      if (rh?.runs) {
        setReconcileHistory(rh.runs ?? []);
        setHistoryNextOffset(rh.next_offset ?? null);
      }
      if (pc) setPreviewCache(pc);
      if (metaRet) setMetaRetention(metaRet);
      if (jobs?.jobs) {
        const filtered = jobs.jobs.filter((j) =>
          String(j.kind || "").startsWith("precompute"),
        );
        setPrecomputeJobs(filtered);
      } else {
        setPrecomputeJobs([]);
      }
      setJobQueue(jobs?.queue ?? null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const fetchMetaRetention = async () => {
    setMetaRetentionError(null);
    try {
      const res = await api<MetaRetentionRes>("/v1/meta/retention", {
        method: "GET",
      });
      setMetaRetention(res);
    } catch (e) {
      setMetaRetentionError(e instanceof Error ? e.message : String(e));
    }
  };

  const setRetentionOverride = (key: string, field: "rows" | "days", value: string) => {
    setMetaRetentionOverrides((prev) => {
      const current = prev[key] ?? { rows: "", days: "" };
      return {
        ...prev,
        [key]: {
          rows: field === "rows" ? value : current.rows,
          days: field === "days" ? value : current.days,
        },
      };
    });
  };

  const runMetaRetention = async (key: string) => {
    setMetaRetentionBusy(true);
    setMetaRetentionError(null);
    setMetaRetentionResult((prev) => ({ ...prev, [key]: null }));
    try {
      const params = new URLSearchParams();
      params.set("table", key);
      const rows = parseInt(metaRetentionOverrides[key]?.rows || "", 10);
      if (Number.isFinite(rows) && rows > 0) params.set("max_rows", String(rows));
      const days = parseInt(metaRetentionOverrides[key]?.days || "", 10);
      if (Number.isFinite(days) && days > 0) params.set("max_days", String(days));
      const res = await api<{ ok?: boolean; result?: Record<string, unknown> }>(
        `/v1/meta/retention?${params.toString()}`,
        { method: "POST", json: {} },
      );
      setMetaRetentionResult((prev) => ({ ...prev, [key]: res.result ?? null }));
      await fetchMetaRetention();
    } catch (e) {
      setMetaRetentionError(e instanceof Error ? e.message : String(e));
    } finally {
      setMetaRetentionBusy(false);
    }
  };

  useEffect(() => {
    void refresh();
  }, [historyLimit, historyOffset]);

  useEventRefresh({
    types: ["meta", "files", "packs", "jobs", "tick"],
    refresh,
    minIntervalMs: 4000,
    ignoreEvents: ["list", "status"],
  });

  const doReconcile = async () => {
    setBusy(true);
    setError(null);
    setLastReconcile(null);
    try {
      const q = new URLSearchParams();
      q.set("packs", reconcilePacks ? "true" : "false");
      q.set("sequences", reconcileSequences ? "true" : "false");
      q.set("audio", reconcileAudio ? "true" : "false");
      q.set("show_configs", reconcileShowConfigs ? "true" : "false");
      q.set("fseq_exports", reconcileFseqExports ? "true" : "false");
      q.set("fpp_scripts", reconcileFppScripts ? "true" : "false");
      q.set("precompute_previews", precomputePreviews ? "true" : "false");
      q.set("precompute_waveforms", precomputeWaveforms ? "true" : "false");
      q.set("scan_limit", String(parseInt(scanLimit || "5000", 10) || 5000));
      const res = await api<ReconcileRes>(
        `/v1/meta/reconcile?${q.toString()}`,
        {
          method: "POST",
          json: {},
        },
      );
      setLastReconcile(res);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const cancelReconcile = async () => {
    setBusy(true);
    setError(null);
    try {
      await api("/v1/meta/reconcile/cancel", { method: "POST", json: {} });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const runPrecompute = async () => {
    setBusy(true);
    setError(null);
    setManualPrecomputeResult(null);
    try {
      const params = new URLSearchParams();
      params.set("previews", manualPrecomputePreviews ? "true" : "false");
      params.set("waveforms", manualPrecomputeWaveforms ? "true" : "false");
      if (manualPrecomputeFormats.trim()) {
        params.set("formats", manualPrecomputeFormats.trim());
      }
      const lim = parseInt(manualPrecomputeLimit || "5000", 10);
      if (Number.isFinite(lim) && lim > 0) {
        params.set("scan_limit", String(lim));
      }
      const res = await api<any>(`/v1/meta/precompute?${params.toString()}`, {
        method: "POST",
        json: {},
      });
      setManualPrecomputeResult(res);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const prunePreviewCache = async (purgeAll: boolean) => {
    setBusy(true);
    setError(null);
    setCacheAction(null);
    try {
      const params = new URLSearchParams();
      if (purgeAll) {
        params.set("all", "true");
      } else {
        const mb = parseInt(cacheOverrideMb || "", 10);
        if (Number.isFinite(mb) && mb > 0) {
          params.set("max_mb", String(mb));
        }
        const days = parseFloat(cacheOverrideDays || "");
        if (Number.isFinite(days) && days > 0) {
          params.set("max_days", String(days));
        }
      }
      const url =
        params.toString().length > 0
          ? `/v1/sequences/preview/purge?${params.toString()}`
          : "/v1/sequences/preview/purge";
      const res = await api<PreviewCachePurgeRes>(url, {
        method: "POST",
        json: {},
      });
      setCacheAction(res);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const look = lastApplied?.last_applied?.look ?? null;
  const seq = lastApplied?.last_applied?.sequence ?? null;
  const status = reconcileStatus?.status ?? null;
  const cacheLimits = previewCache
    ? `max ${previewCache.max_mb ?? 0} MB / ${previewCache.max_days ?? 0} days`
    : "—";
  const cacheMaxBytes =
    previewCache?.max_mb && previewCache.max_mb > 0
      ? previewCache.max_mb * 1024 * 1024
      : null;
  const cacheUsage =
    cacheMaxBytes && previewCache?.bytes
      ? previewCache.bytes / cacheMaxBytes
      : null;
  const cachePressureLabel =
    cacheUsage != null ? `${Math.round(cacheUsage * 100)}%` : "—";

  return (
    <Stack spacing={2}>
      {error ? (
        <Alert severity={needsDb ? "warning" : "error"}>{error}</Alert>
      ) : null}
      {needsDb ? (
        <Alert severity="info">
          These endpoints require <code>DATABASE_URL</code>. Enable SQL and
          restart the API container to use metadata features.
        </Alert>
      ) : null}

      {lastReconcile ? (
        <Alert severity={lastReconcile.ok ? "success" : "warning"}>
          Reconcile:{" "}
          <code>
            {lastReconcile.ok
              ? `upserted=${JSON.stringify(lastReconcile.upserted || {})}`
              : lastReconcile.reason || "not ok"}
          </code>
        </Alert>
      ) : null}

      {cacheAction ? (
        <Alert severity={cacheAction.ok ? "success" : "warning"}>
          Preview cache:{" "}
          <code>
            deleted={cacheAction.deleted_files ?? 0} · bytes{" "}
            {fmtBytes(cacheAction.deleted_bytes ?? 0)} · before{" "}
            {fmtBytes(cacheAction.before_bytes ?? 0)} · after{" "}
            {fmtBytes(cacheAction.after_bytes ?? 0)}
          </code>
        </Alert>
      ) : null}

      <Card>
        <CardContent>
          <Typography variant="h6">Reconcile Status</Typography>
          <Typography variant="body2" color="text.secondary">
            Latest reconcile run (startup, scheduled, or manual).
          </Typography>
          <Stack spacing={1} sx={{ mt: 2 }}>
            {reconcileStatus?.exists && status ? (
              <>
                <Typography variant="body2">
                  running=<code>{status.running ? "yes" : "no"}</code> · mode=
                  <code>{status.mode ?? "—"}</code> · run_id=
                  <code>{status.run_id ?? "—"}</code>
                </Typography>
                <Typography variant="body2">
                  phase=<code>{status.phase ?? "—"}</code> · cancel_requested=
                  <code>{status.cancel_requested ? "yes" : "no"}</code>
                </Typography>
                <Typography variant="body2">
                  started=<code>{fmtTs(status.started_at ?? null)}</code> · finished=
                  <code>{fmtTs(status.finished_at ?? null)}</code> · duration=
                  <code>
                    {status.duration_s != null
                      ? `${Number(status.duration_s).toFixed(1)}s`
                      : "—"}
                  </code>
                </Typography>
                <Typography variant="body2">
                  last success=<code>{fmtTs(status.last_success_at ?? null)}</code>
                </Typography>
                {status.last_error ? (
                  <Typography variant="body2" color="error">
                    {status.last_error}
                  </Typography>
                ) : null}
                {status.last_result?.upserted ? (
                  <Typography variant="body2">
                    upserted=<code>{JSON.stringify(status.last_result.upserted)}</code>
                  </Typography>
                ) : null}
              </>
            ) : (
              <Typography variant="body2" color="text.secondary">
                No reconcile status recorded yet.
              </Typography>
            )}
          </Stack>
        </CardContent>
        <CardActions>
          <Button startIcon={<RefreshIcon />} onClick={refresh} disabled={busy}>
            Refresh
          </Button>
          {status?.running ? (
            <Button
              color="error"
              startIcon={<SyncIcon />}
              onClick={cancelReconcile}
              disabled={busy}
            >
              Cancel
            </Button>
          ) : null}
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Precompute</Typography>
          <Typography variant="body2" color="text.secondary">
            Preview/waveform jobs queued after pack ingest or reconcile.
          </Typography>
          <Stack spacing={1} sx={{ mt: 2 }}>
            <Typography variant="body2">
              Queue:{" "}
              <code>
                {jobQueue?.size ?? 0}/{jobQueue?.max ?? 0}
              </code>{" "}
              · workers <code>{jobQueue?.workers ?? 0}</code>
            </Typography>
            {manualPrecomputeResult ? (
              <Alert
                severity={manualPrecomputeResult.ok ? "success" : "warning"}
              >
                {manualPrecomputeResult.job ? (
                  <>
                    Scheduled job <code>{manualPrecomputeResult.job.id}</code>
                  </>
                ) : manualPrecomputeResult.skipped ? (
                  <>Skipped: {manualPrecomputeResult.reason || "no files"}</>
                ) : (
                  <>Precompute started.</>
                )}
              </Alert>
            ) : null}
            {precomputeJobs.length ? (
              precomputeJobs.slice(0, 3).map((job) => {
                const current = job.progress?.current ?? null;
                const total = job.progress?.total ?? null;
                const pct =
                  current != null && total != null && total > 0
                    ? Math.round((current / total) * 100)
                    : null;
                return (
                  <Stack key={job.id} spacing={0.5}>
                    <Typography variant="body2">
                      <code>{job.kind}</code> · <code>{job.status}</code> ·{" "}
                      <code>{fmtTs(job.created_at)}</code>
                    </Typography>
                    {job.progress?.message ? (
                      <Typography variant="body2" color="text.secondary">
                        {job.progress.message}
                        {pct != null ? ` (${pct}%)` : ""}
                      </Typography>
                    ) : null}
                  </Stack>
                );
              })
            ) : (
              <Typography variant="body2" color="text.secondary">
                No recent precompute jobs.
              </Typography>
            )}
            <FormControlLabel
              control={
                <Switch
                  checked={manualPrecomputePreviews}
                  onChange={(e) => setManualPrecomputePreviews(e.target.checked)}
                />
              }
              label="Precompute previews"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={manualPrecomputeWaveforms}
                  onChange={(e) => setManualPrecomputeWaveforms(e.target.checked)}
                />
              }
              label="Precompute waveforms"
            />
            <TextField
              label="Formats (comma-separated)"
              value={manualPrecomputeFormats}
              onChange={(e) => setManualPrecomputeFormats(e.target.value)}
              helperText="e.g. gif,mp4 (defaults to gif)."
              disabled={busy}
            />
            <TextField
              label="Scan limit"
              value={manualPrecomputeLimit}
              onChange={(e) => setManualPrecomputeLimit(e.target.value)}
              helperText="Max files to scan when discovering content."
              disabled={busy}
              inputMode="numeric"
            />
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            onClick={runPrecompute}
            disabled={
              busy ||
              (!manualPrecomputePreviews && !manualPrecomputeWaveforms)
            }
          >
            Run precompute
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Preview Cache</Typography>
          <Typography variant="body2" color="text.secondary">
            Sequence previews cached under <code>DATA_DIR/cache/previews</code>.
          </Typography>
          {cacheUsage != null && cacheUsage >= 0.9 ? (
            <Alert severity={cacheUsage >= 1 ? "error" : "warning"} sx={{ mt: 2 }}>
              Cache pressure high ({cachePressureLabel}). Consider pruning or raising{" "}
              <code>SEQUENCE_PREVIEW_CACHE_MAX_MB</code>.
            </Alert>
          ) : null}
          <Stack spacing={1} sx={{ mt: 2 }}>
            <Typography variant="body2">
              Files: <code>{previewCache?.files ?? 0}</code> · Size{" "}
              <code>{fmtBytes(previewCache?.bytes ?? 0)}</code> · Policy{" "}
              <code>{cacheLimits}</code> · usage <code>{cachePressureLabel}</code>
            </Typography>
            <TextField
              label="Override max MB"
              value={cacheOverrideMb}
              onChange={(e) => setCacheOverrideMb(e.target.value)}
              helperText="Optional per-run limit."
              disabled={busy}
              inputMode="numeric"
            />
            <TextField
              label="Override max days"
              value={cacheOverrideDays}
              onChange={(e) => setCacheOverrideDays(e.target.value)}
              helperText="Optional per-run retention window."
              disabled={busy}
              inputMode="decimal"
            />
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            onClick={() => void prunePreviewCache(false)}
            disabled={busy}
          >
            Prune
          </Button>
          <Button
            color="error"
            onClick={() => void prunePreviewCache(true)}
            disabled={busy}
          >
            Purge all
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Metadata Retention</Typography>
          <Typography variant="body2" color="text.secondary">
            Retention status for SQL metadata tables.
          </Typography>
          {metaRetentionError ? (
            <Alert severity="warning" sx={{ mt: 2 }}>
              {metaRetentionError}
            </Alert>
          ) : null}
          <Stack spacing={2} sx={{ mt: 2 }}>
            {META_RETENTION_TABLES.map((entry) => {
              const table = metaRetention?.tables?.[entry.key];
              const overrides = metaRetentionOverrides[entry.key] ?? {
                rows: "",
                days: "",
              };
              const result = metaRetentionResult[entry.key];
              const drift = table?.drift?.drift;
              return (
                <Stack
                  key={entry.key}
                  spacing={1}
                  sx={{
                    border: "1px solid rgba(255,255,255,0.12)",
                    borderRadius: 1,
                    p: 2,
                  }}
                >
                  <Typography variant="subtitle2">{entry.label}</Typography>
                  {drift ? (
                    <Alert severity="warning">
                      Exceeds retention targets. Consider cleanup.
                    </Alert>
                  ) : null}
                  {result ? (
                    <Alert severity="success">
                      Cleanup complete:{" "}
                      <code>
                        rows {String(result.deleted_by_rows ?? 0)} · days{" "}
                        {String(result.deleted_by_days ?? 0)}
                      </code>
                    </Alert>
                  ) : null}
                  <Typography variant="body2">
                    Count: <code>{table?.stats?.count ?? 0}</code> · Oldest{" "}
                    <code>{fmtTs(table?.stats?.oldest ?? null)}</code> · Newest{" "}
                    <code>{fmtTs(table?.stats?.newest ?? null)}</code>
                  </Typography>
                  <Typography variant="body2">
                    Max rows: <code>{table?.settings?.max_rows ?? 0}</code> · Max
                    days: <code>{table?.settings?.max_days ?? 0}</code>
                  </Typography>
                  <Typography variant="body2">
                    Drift: rows <code>{table?.drift?.excess_rows ?? 0}</code> · age{" "}
                    <code>
                      {table?.drift?.excess_age_s
                        ? `${Math.round(table.drift.excess_age_s)}s`
                        : "0s"}
                    </code>
                  </Typography>
                  <TextField
                    label="Override max rows"
                    value={overrides.rows}
                    onChange={(e) =>
                      setRetentionOverride(entry.key, "rows", e.target.value)
                    }
                    helperText="Optional override for manual cleanup."
                    disabled={!isAdmin || metaRetentionBusy}
                    inputMode="numeric"
                  />
                  <TextField
                    label="Override max days"
                    value={overrides.days}
                    onChange={(e) =>
                      setRetentionOverride(entry.key, "days", e.target.value)
                    }
                    helperText="Optional override for manual cleanup."
                    disabled={!isAdmin || metaRetentionBusy}
                    inputMode="numeric"
                  />
                  <Typography variant="body2" color="text.secondary">
                    Last cleanup:{" "}
                    <code>
                      {table?.last_retention?.at
                        ? fmtTs(table.last_retention.at)
                        : "—"}
                    </code>
                  </Typography>
                  <Button
                    variant="contained"
                    onClick={() => void runMetaRetention(entry.key)}
                    disabled={!isAdmin || metaRetentionBusy}
                  >
                    Run cleanup
                  </Button>
                </Stack>
              );
            })}
          </Stack>
        </CardContent>
        <CardActions>
          <Button onClick={() => void fetchMetaRetention()} disabled={metaRetentionBusy}>
            Refresh retention
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Reconcile History</Typography>
          <Typography variant="body2" color="text.secondary">
            Recent reconcile runs (manual, startup, scheduled).
          </Typography>
          <Stack spacing={1} sx={{ mt: 2 }}>
            <TextField
              label="Limit"
              value={historyLimit}
              onChange={(e) => {
                setHistoryOffset(0);
                setHistoryLimit(e.target.value);
              }}
              inputMode="numeric"
              disabled={busy}
            />
            <Stack direction="row" spacing={1}>
              <Button
                size="small"
                onClick={() =>
                  setHistoryOffset((prev) =>
                    Math.max(0, prev - (parseInt(historyLimit, 10) || 10)),
                  )
                }
                disabled={busy || historyOffset <= 0}
              >
                Prev
              </Button>
              <Button
                size="small"
                onClick={() =>
                  setHistoryOffset(historyNextOffset ?? historyOffset)
                }
                disabled={busy || historyNextOffset == null}
              >
                Next
              </Button>
            </Stack>
            {reconcileHistory.length ? (
              reconcileHistory.map((run) => (
                <Stack key={run.id} spacing={0.5}>
                  <Typography variant="body2">
                    id=<code>{run.id}</code> · status=<code>{run.status}</code>{" "}
                    · source=<code>{run.source ?? "—"}</code>
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    started=<code>{fmtTs(run.started_at)}</code> · finished=
                    <code>{fmtTs(run.finished_at ?? null)}</code>
                  </Typography>
                  {run.error ? (
                    <Typography variant="body2" color="error">
                      {run.error}
                    </Typography>
                  ) : null}
                </Stack>
              ))
            ) : (
              <Typography variant="body2" color="text.secondary">
                No reconcile history.
              </Typography>
            )}
          </Stack>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Metadata</Typography>
          <Typography variant="body2" color="text.secondary">
            SQL-backed metadata for what&apos;s in <code>DATA_DIR</code>.
          </Typography>
        </CardContent>
        <CardActions>
          <Button startIcon={<RefreshIcon />} onClick={refresh} disabled={busy}>
            Refresh
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Last Applied</Typography>
          <Typography variant="body2" color="text.secondary">
            Updated when a look or sequence is applied.
          </Typography>

          <Stack spacing={1} sx={{ mt: 2 }}>
            <Typography variant="body2">
              Look: <code>{look?.name ?? "—"}</code> · Pack:{" "}
              <code>{look?.file ?? "—"}</code> · Updated:{" "}
              <code>{fmtTs(look?.updated_at ?? null)}</code>
            </Typography>
            <Typography variant="body2">
              Sequence: <code>{seq?.file ?? "—"}</code> · Updated:{" "}
              <code>{fmtTs(seq?.updated_at ?? null)}</code>
            </Typography>
          </Stack>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Reconcile</Typography>
          <Typography variant="body2" color="text.secondary">
            Scans <code>DATA_DIR</code> and upserts metadata tables (useful if
            you copied files directly into the volume).
          </Typography>

          <Stack spacing={1} sx={{ mt: 2 }}>
            <FormControlLabel
              control={
                <Switch
                  checked={reconcilePacks}
                  onChange={(e) => setReconcilePacks(e.target.checked)}
                />
              }
              label="Packs (manifest.json)"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={reconcileSequences}
                  onChange={(e) => setReconcileSequences(e.target.checked)}
                />
              }
              label="Sequences (DATA_DIR/sequences)"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={reconcileAudio}
                  onChange={(e) => setReconcileAudio(e.target.checked)}
                />
              }
              label="Audio analyses (DATA_DIR/audio/*.json)"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={reconcileShowConfigs}
                  onChange={(e) => setReconcileShowConfigs(e.target.checked)}
                />
              }
              label="Show configs (DATA_DIR/show/*.json)"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={reconcileFseqExports}
                  onChange={(e) => setReconcileFseqExports(e.target.checked)}
                />
              }
              label="FSEQ exports (DATA_DIR/fseq/*.fseq)"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={reconcileFppScripts}
                  onChange={(e) => setReconcileFppScripts(e.target.checked)}
                />
              }
              label="FPP scripts (DATA_DIR/fpp/scripts/*.sh)"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={precomputePreviews}
                  onChange={(e) => setPrecomputePreviews(e.target.checked)}
                />
              }
              label="Precompute sequence previews"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={precomputeWaveforms}
                  onChange={(e) => setPrecomputeWaveforms(e.target.checked)}
                />
              }
              label="Precompute audio waveforms"
            />
            <TextField
              label="Scan limit"
              value={scanLimit}
              onChange={(e) => setScanLimit(e.target.value)}
              helperText="Upper bound on files scanned per category."
              disabled={busy}
            />
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            startIcon={<SyncIcon />}
            onClick={doReconcile}
            disabled={
              busy ||
              (!reconcilePacks &&
                !reconcileSequences &&
                !reconcileAudio &&
                !reconcileShowConfigs &&
                !reconcileFseqExports &&
                !reconcileFppScripts)
            }
          >
            Reconcile now
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Packs</Typography>
          <Typography variant="body2" color="text.secondary">
            Most recent pack ingests (from <code>manifest.json</code>).
          </Typography>
          <Stack spacing={0.75} sx={{ mt: 2 }}>
            {packs.length ? (
              packs.map((p) => (
                <Typography key={p.dest_dir} variant="body2">
                  <code>{p.dest_dir}</code> · files <code>{p.file_count}</code>{" "}
                  · unpacked <code>{fmtBytes(p.unpacked_bytes)}</code> · updated{" "}
                  <code>{fmtTs(p.updated_at)}</code>
                </Typography>
              ))
            ) : (
              <Typography variant="body2" color="text.secondary">
                No pack records.
              </Typography>
            )}
          </Stack>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Sequences</Typography>
          <Typography variant="body2" color="text.secondary">
            Most recent sequence metadata (duration + step counts).
          </Typography>
          <Stack spacing={0.75} sx={{ mt: 2 }}>
            {sequences.length ? (
              sequences.map((s) => (
                <Stack
                  key={s.file}
                  direction={{ xs: "column", sm: "row" }}
                  spacing={1}
                  sx={{ alignItems: { sm: "center" } }}
                >
                  <Typography variant="body2" sx={{ flex: 1 }}>
                    <code>{s.file}</code> · steps <code>{s.steps_total}</code> ·
                    duration <code>{Math.round(s.duration_s)}s</code> · updated{" "}
                    <code>{fmtTs(s.updated_at)}</code>
                  </Typography>
                  <Button
                    size="small"
                    onClick={() => setSequencePreview(s.file)}
                  >
                    Preview
                  </Button>
                </Stack>
              ))
            ) : (
              <Typography variant="body2" color="text.secondary">
                No sequence records.
              </Typography>
            )}
          </Stack>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Audio Analyses</Typography>
          <Typography variant="body2" color="text.secondary">
            Recent beat/BPM analysis runs.
          </Typography>
          <Stack spacing={0.75} sx={{ mt: 2 }}>
            {audio.length ? (
              audio.map((a) => (
                <Stack
                  key={a.id}
                  direction={{ xs: "column", sm: "row" }}
                  spacing={1}
                  sx={{ alignItems: { sm: "center" } }}
                >
                  <Typography variant="body2" sx={{ flex: 1 }}>
                    <code>{a.beats_path ?? a.id}</code> · bpm{" "}
                    <code>{a.bpm ?? "—"}</code> · beats{" "}
                    <code>{a.beat_count ?? "—"}</code> · created{" "}
                    <code>{fmtTs(a.created_at)}</code>
                  </Typography>
                  <Button
                    size="small"
                    onClick={() =>
                      setAudioPreview({
                        audioPath: a.source_path || "",
                        beatsPath: a.beats_path,
                        bpm: a.bpm,
                      })
                    }
                    disabled={!a.source_path}
                  >
                    Waveform
                  </Button>
                </Stack>
              ))
            ) : (
              <Typography variant="body2" color="text.secondary">
                No audio analysis records.
              </Typography>
            )}
          </Stack>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Show Configs</Typography>
          <Typography variant="body2" color="text.secondary">
            Recent show configuration files.
          </Typography>
          <Stack spacing={0.75} sx={{ mt: 2 }}>
            {showConfigs.length ? (
              showConfigs.map((s) => (
                <Typography key={s.file} variant="body2">
                  <code>{s.file}</code> · props <code>{s.props_total}</code> ·
                  groups <code>{s.groups_total}</code> · updated{" "}
                  <code>{fmtTs(s.updated_at)}</code>
                </Typography>
              ))
            ) : (
              <Typography variant="body2" color="text.secondary">
                No show config records.
              </Typography>
            )}
          </Stack>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">FSEQ Exports</Typography>
          <Typography variant="body2" color="text.secondary">
            Recent FSEQ render outputs.
          </Typography>
          <Stack spacing={0.75} sx={{ mt: 2 }}>
            {fseqExports.length ? (
              fseqExports.map((f) => (
                <Typography key={f.file} variant="body2">
                  <code>{f.file}</code> · frames <code>{f.frames ?? "—"}</code>{" "}
                  · channels <code>{f.channels ?? "—"}</code> · size{" "}
                  <code>{fmtBytes(f.bytes_written)}</code>
                </Typography>
              ))
            ) : (
              <Typography variant="body2" color="text.secondary">
                No FSEQ export records.
              </Typography>
            )}
          </Stack>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">FPP Scripts</Typography>
          <Typography variant="body2" color="text.secondary">
            Helper scripts generated for Falcon Player.
          </Typography>
          <Stack spacing={0.75} sx={{ mt: 2 }}>
            {fppScripts.length ? (
              fppScripts.map((s) => (
                <Typography key={s.file} variant="body2">
                  <code>{s.file}</code> · kind{" "}
                  <code>{s.kind || "custom"}</code> · size{" "}
                  <code>{fmtBytes(s.bytes_written)}</code>
                </Typography>
              ))
            ) : (
              <Typography variant="body2" color="text.secondary">
                No script records.
              </Typography>
            )}
          </Stack>
        </CardContent>
      </Card>

      <SequencePreviewDialog
        open={Boolean(sequencePreview)}
        file={sequencePreview}
        onClose={() => setSequencePreview(null)}
      />
      <AudioWaveformDialog
        open={Boolean(audioPreview)}
        audioPath={audioPreview?.audioPath ?? null}
        beatsPath={audioPreview?.beatsPath ?? null}
        bpm={audioPreview?.bpm ?? null}
        onClose={() => setAudioPreview(null)}
      />
    </Stack>
  );
}
