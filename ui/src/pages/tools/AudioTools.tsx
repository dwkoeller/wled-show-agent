import GraphicEqIcon from "@mui/icons-material/GraphicEq";
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
import { useNavigate } from "react-router-dom";
import { api } from "../../api";
import { useEventRefresh } from "../../hooks/useEventRefresh";

type WaveformCacheRes = {
  ok?: boolean;
  files?: number;
  bytes?: number;
  max_mb?: number;
  max_days?: number;
};

type WaveformCachePurgeRes = {
  ok?: boolean;
  purge?: boolean;
  deleted_files?: number;
  deleted_bytes?: number;
  before_bytes?: number;
  after_bytes?: number;
};

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

export function AudioTools() {
  const nav = useNavigate();
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [musicFiles, setMusicFiles] = useState<string[]>([]);
  const [waveformCache, setWaveformCache] = useState<WaveformCacheRes | null>(
    null,
  );
  const [cacheOverrideMb, setCacheOverrideMb] = useState("");
  const [cacheOverrideDays, setCacheOverrideDays] = useState("");
  const [cacheAction, setCacheAction] = useState<WaveformCachePurgeRes | null>(null);

  const [audioFile, setAudioFile] = useState("music/song.wav");
  const [outFile, setOutFile] = useState("audio/beats.json");
  const [minBpm, setMinBpm] = useState("60");
  const [maxBpm, setMaxBpm] = useState("200");
  const [hopMs, setHopMs] = useState("10");
  const [windowMs, setWindowMs] = useState("50");
  const [peakThreshold, setPeakThreshold] = useState("1.35");
  const [minIntervalS, setMinIntervalS] = useState("0.20");
  const [preferFfmpeg, setPreferFfmpeg] = useState(true);

  const refreshMusic = async () => {
    try {
      const [res, cache] = await Promise.all([
        api<{ ok: boolean; files: string[] }>(
          "/v1/files/list?dir=music&recursive=true&limit=200",
          { method: "GET" },
        ),
        api<WaveformCacheRes>("/v1/audio/waveform/cache", {
          method: "GET",
        }).catch(() => null),
      ]);
      setMusicFiles(res.files || []);
      if (cache) setWaveformCache(cache);
    } catch {
      setMusicFiles([]);
    }
  };

  useEffect(() => {
    void refreshMusic();
  }, []);

  useEventRefresh({
    types: ["audio", "files", "tick"],
    refresh: refreshMusic,
    minIntervalMs: 4000,
    ignoreEvents: ["list", "status"],
  });

  const hint = useMemo(() => {
    if (!musicFiles.length) return null;
    const first =
      musicFiles.find((f) => f.toLowerCase().endsWith(".wav")) ?? musicFiles[0];
    return first ? `Example: ${first}` : null;
  }, [musicFiles]);

  const submit = async () => {
    setBusy(true);
    setError(null);
    try {
      await api("/v1/jobs/audio/analyze", {
        method: "POST",
        json: {
          audio_file: audioFile,
          out_file: outFile,
          min_bpm: parseInt(minBpm || "60", 10),
          max_bpm: parseInt(maxBpm || "200", 10),
          hop_ms: parseInt(hopMs || "10", 10),
          window_ms: parseInt(windowMs || "50", 10),
          peak_threshold: parseFloat(peakThreshold || "1.35"),
          min_interval_s: parseFloat(minIntervalS || "0.2"),
          prefer_ffmpeg: Boolean(preferFfmpeg),
        },
      });
      nav("/jobs");
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const pruneWaveformCache = async (purgeAll: boolean) => {
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
          ? `/v1/audio/waveform/purge?${params.toString()}`
          : "/v1/audio/waveform/purge";
      const res = await api<WaveformCachePurgeRes>(url, {
        method: "POST",
        json: {},
      });
      setCacheAction(res);
      await refreshMusic();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const cacheMaxBytes =
    waveformCache?.max_mb && waveformCache.max_mb > 0
      ? waveformCache.max_mb * 1024 * 1024
      : null;
  const cacheUsage =
    cacheMaxBytes && waveformCache?.bytes
      ? waveformCache.bytes / cacheMaxBytes
      : null;
  const cachePressureLabel =
    cacheUsage != null ? `${Math.round(cacheUsage * 100)}%` : "—";

  return (
    <Stack spacing={2}>
      {error ? <Alert severity="error">{error}</Alert> : null}
      {cacheAction ? (
        <Alert severity={cacheAction.ok ? "success" : "warning"}>
          Waveform cache:{" "}
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
          <Typography variant="h6">Audio Beat/BPM Analyzer</Typography>
          <Typography variant="body2" color="text.secondary">
            Writes a JSON beat grid (for beat-aligned sequence generation).
            Without <code>ffmpeg</code> in the API container, only{" "}
            <code>.wav</code> is supported.
          </Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <TextField
              label="Audio file (relative to DATA_DIR)"
              value={audioFile}
              onChange={(e) => setAudioFile(e.target.value)}
              disabled={busy}
              helperText={
                hint ??
                "Place files under DATA_DIR/music/ (mounted from ./data/.../music/)."
              }
            />
            <TextField
              label="Output file"
              value={outFile}
              onChange={(e) => setOutFile(e.target.value)}
              disabled={busy}
            />

            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <TextField
                label="Min BPM"
                value={minBpm}
                onChange={(e) => setMinBpm(e.target.value)}
                disabled={busy}
                fullWidth
              />
              <TextField
                label="Max BPM"
                value={maxBpm}
                onChange={(e) => setMaxBpm(e.target.value)}
                disabled={busy}
                fullWidth
              />
            </Stack>

            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <TextField
                label="Hop (ms)"
                value={hopMs}
                onChange={(e) => setHopMs(e.target.value)}
                disabled={busy}
                fullWidth
              />
              <TextField
                label="Window (ms)"
                value={windowMs}
                onChange={(e) => setWindowMs(e.target.value)}
                disabled={busy}
                fullWidth
              />
            </Stack>

            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <TextField
                label="Peak threshold"
                value={peakThreshold}
                onChange={(e) => setPeakThreshold(e.target.value)}
                disabled={busy}
                fullWidth
              />
              <TextField
                label="Min interval (s)"
                value={minIntervalS}
                onChange={(e) => setMinIntervalS(e.target.value)}
                disabled={busy}
                fullWidth
              />
            </Stack>

            <FormControlLabel
              control={
                <Switch
                  checked={preferFfmpeg}
                  onChange={(e) => setPreferFfmpeg(e.target.checked)}
                />
              }
              label="Prefer ffmpeg for non-WAV"
            />
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            startIcon={<GraphicEqIcon />}
            onClick={submit}
            disabled={busy || !audioFile.trim()}
          >
            Analyze (job)
          </Button>
          <Button onClick={refreshMusic} disabled={busy}>
            Refresh music list
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Waveform Cache</Typography>
          <Typography variant="body2" color="text.secondary">
            Cached waveform summaries under <code>DATA_DIR/cache/waveforms</code>.
          </Typography>
          {cacheUsage != null && cacheUsage >= 0.9 ? (
            <Alert severity={cacheUsage >= 1 ? "error" : "warning"} sx={{ mt: 2 }}>
              Cache pressure high ({cachePressureLabel}). Consider pruning or raising{" "}
              <code>WAVEFORM_CACHE_MAX_MB</code>.
            </Alert>
          ) : null}
          <Stack spacing={1} sx={{ mt: 2 }}>
            <Typography variant="body2">
              Files: <code>{waveformCache?.files ?? 0}</code> · Size{" "}
              <code>{fmtBytes(waveformCache?.bytes ?? 0)}</code> · Policy{" "}
              <code>
                {waveformCache
                  ? `max ${waveformCache.max_mb ?? 0} MB / ${
                      waveformCache.max_days ?? 0
                    } days`
                  : "—"}
              </code>{" "}
              · usage <code>{cachePressureLabel}</code>
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
          <Button onClick={() => void pruneWaveformCache(false)} disabled={busy}>
            Prune
          </Button>
          <Button
            color="error"
            onClick={() => void pruneWaveformCache(true)}
            disabled={busy}
          >
            Purge all
          </Button>
        </CardActions>
      </Card>
    </Stack>
  );
}
