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

export function AudioTools() {
  const nav = useNavigate();
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [musicFiles, setMusicFiles] = useState<string[]>([]);

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
      const res = await api<{ ok: boolean; files: string[] }>(
        "/v1/files/list?dir=music&recursive=true&limit=200",
        { method: "GET" },
      );
      setMusicFiles(res.files || []);
    } catch {
      setMusicFiles([]);
    }
  };

  useEffect(() => {
    void refreshMusic();
  }, []);

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

  return (
    <Stack spacing={2}>
      {error ? <Alert severity="error">{error}</Alert> : null}

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
    </Stack>
  );
}
