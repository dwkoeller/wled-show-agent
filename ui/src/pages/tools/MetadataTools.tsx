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

type ReconcileRes = {
  ok?: boolean;
  skipped?: boolean;
  reason?: string;
  scanned?: Record<string, number>;
  upserted?: Record<string, number>;
};

function fmtTs(ts: number | null | undefined): string {
  if (!ts) return "—";
  try {
    return new Date(ts * 1000).toLocaleString();
  } catch {
    return String(ts);
  }
}

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
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const [lastApplied, setLastApplied] = useState<LastApplied | null>(null);
  const [packs, setPacks] = useState<PackRow[]>([]);
  const [sequences, setSequences] = useState<SequenceRow[]>([]);
  const [audio, setAudio] = useState<AudioRow[]>([]);

  const [reconcilePacks, setReconcilePacks] = useState(true);
  const [reconcileSequences, setReconcileSequences] = useState(true);
  const [reconcileAudio, setReconcileAudio] = useState(false);
  const [scanLimit, setScanLimit] = useState("5000");
  const [lastReconcile, setLastReconcile] = useState<ReconcileRes | null>(null);

  const needsDb = useMemo(() => {
    const msg = (error ?? "").toLowerCase();
    return (
      msg.includes("database_url is not configured") || msg.includes("database")
    );
  }, [error]);

  const refresh = async () => {
    setBusy(true);
    setError(null);
    try {
      const [la, pk, sq, au] = await Promise.all([
        api<LastApplied>("/v1/meta/last_applied?_", { method: "GET" }),
        api<PacksRes>("/v1/meta/packs?limit=50", { method: "GET" }),
        api<SequencesRes>("/v1/meta/sequences?limit=100", { method: "GET" }),
        api<AudioRes>("/v1/meta/audio_analyses?limit=50", { method: "GET" }),
      ]);
      setLastApplied(la);
      setPacks(pk.packs ?? []);
      setSequences(sq.sequences ?? []);
      setAudio(au.audio_analyses ?? []);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  useEffect(() => {
    void refresh();
  }, []);

  const doReconcile = async () => {
    setBusy(true);
    setError(null);
    setLastReconcile(null);
    try {
      const q = new URLSearchParams();
      q.set("packs", reconcilePacks ? "true" : "false");
      q.set("sequences", reconcileSequences ? "true" : "false");
      q.set("audio", reconcileAudio ? "true" : "false");
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

  const look = lastApplied?.last_applied?.look ?? null;
  const seq = lastApplied?.last_applied?.sequence ?? null;

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
              (!reconcilePacks && !reconcileSequences && !reconcileAudio)
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
                <Typography key={s.file} variant="body2">
                  <code>{s.file}</code> · steps <code>{s.steps_total}</code> ·
                  duration <code>{Math.round(s.duration_s)}s</code> · updated{" "}
                  <code>{fmtTs(s.updated_at)}</code>
                </Typography>
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
                <Typography key={a.id} variant="body2">
                  <code>{a.beats_path ?? a.id}</code> · bpm{" "}
                  <code>{a.bpm ?? "—"}</code> · beats{" "}
                  <code>{a.beat_count ?? "—"}</code> · created{" "}
                  <code>{fmtTs(a.created_at)}</code>
                </Typography>
              ))
            ) : (
              <Typography variant="body2" color="text.secondary">
                No audio analysis records.
              </Typography>
            )}
          </Stack>
        </CardContent>
      </Card>
    </Stack>
  );
}
