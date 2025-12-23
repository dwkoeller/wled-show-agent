import {
  Alert,
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  LinearProgress,
  Stack,
  Typography,
} from "@mui/material";
import React, { useEffect, useMemo, useRef, useState } from "react";

type WaveSample = { min: number; max: number };

const CANVAS_WIDTH = 640;
const CANVAS_HEIGHT = 180;

function parseBeats(payload: any): number[] {
  if (!payload || typeof payload !== "object") return [];
  if (Array.isArray(payload.beats_s)) {
    return payload.beats_s.map((x: any) => Number(x)).filter(Number.isFinite);
  }
  if (Array.isArray(payload.beats_ms)) {
    return payload.beats_ms
      .map((x: any) => Number(x) / 1000)
      .filter(Number.isFinite);
  }
  return [];
}

export function AudioWaveformDialog(props: {
  open: boolean;
  audioPath: string | null;
  beatsPath?: string | null;
  bpm?: number | null;
  onClose: () => void;
}) {
  const { open, audioPath, beatsPath, bpm, onClose } = props;
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [duration, setDuration] = useState<number | null>(null);
  const [samples, setSamples] = useState<WaveSample[]>([]);
  const [beats, setBeats] = useState<number[]>([]);

  const audioKey = useMemo(() => String(audioPath ?? ""), [audioPath]);
  const beatsKey = useMemo(() => String(beatsPath ?? ""), [beatsPath]);

  useEffect(() => {
    if (!open || !audioKey) return;
    let active = true;
    setBusy(true);
    setError(null);
    setSamples([]);
    setBeats([]);
    setDuration(null);

    const fetchBeats = async () => {
      if (!beatsKey) return [];
      const params = new URLSearchParams();
      params.set("path", beatsKey);
      const resp = await fetch(`/v1/files/download?${params.toString()}`, {
        credentials: "include",
      });
      if (!resp.ok) return [];
      const data = await resp.json();
      return parseBeats(data);
    };

    const fetchWaveform = async () => {
      const params = new URLSearchParams();
      params.set("file", audioKey);
      params.set("points", String(Math.min(800, CANVAS_WIDTH)));
      const resp = await fetch(`/v1/audio/waveform?${params.toString()}`, {
        credentials: "include",
      });
      if (!resp.ok) {
        const contentType = resp.headers.get("content-type") ?? "";
        if (contentType.includes("application/json")) {
          const data = (await resp.json()) as { detail?: string; error?: string };
          throw new Error(data.detail || data.error || `HTTP ${resp.status}`);
        }
        const text = await resp.text();
        throw new Error(text || `HTTP ${resp.status}`);
      }
      return resp.json() as Promise<{
        duration_s?: number;
        points?: Array<{ min: number; max: number }>;
      }>;
    };

    const load = async () => {
      const [beatsList, waveform] = await Promise.all([
        fetchBeats(),
        fetchWaveform(),
      ]);
      const trimmedBeats =
        beatsList.length > 2000 ? beatsList.slice(0, 2000) : beatsList;
      const nextSamples = Array.isArray(waveform.points)
        ? waveform.points.map((p) => ({
            min: Number(p.min ?? 0),
            max: Number(p.max ?? 0),
          }))
        : [];
      if (!active) return;
      setSamples(nextSamples);
      setDuration(
        Number.isFinite(waveform.duration_s ?? NaN)
          ? Number(waveform.duration_s)
          : null,
      );
      setBeats(trimmedBeats);
    };

    load()
      .catch((e) => {
        if (!active) return;
        setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => {
        if (!active) return;
        setBusy(false);
      });

    return () => {
      active = false;
    };
  }, [open, audioKey, beatsKey]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    const width = canvas.width;
    const height = canvas.height;
    ctx.clearRect(0, 0, width, height);
    if (!samples.length) return;

    const mid = height / 2;
    ctx.strokeStyle = "rgba(144,202,249,0.9)";
    ctx.lineWidth = 1;
    samples.forEach((s, idx) => {
      const x = Math.round((idx / Math.max(1, samples.length - 1)) * width);
      const y1 = mid - s.max * mid;
      const y2 = mid - s.min * mid;
      ctx.beginPath();
      ctx.moveTo(x, y1);
      ctx.lineTo(x, y2);
      ctx.stroke();
    });

    if (duration && beats.length) {
      ctx.strokeStyle = "rgba(255,193,7,0.6)";
      ctx.lineWidth = 1;
      beats.forEach((t) => {
        const x = Math.round((t / duration) * width);
        ctx.beginPath();
        ctx.moveTo(x, 0);
        ctx.lineTo(x, height);
        ctx.stroke();
      });
    }
  }, [samples, beats, duration]);

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>Audio waveform</DialogTitle>
      <DialogContent>
        {busy ? <LinearProgress sx={{ mb: 2 }} /> : null}
        {error ? <Alert severity="error">{error}</Alert> : null}

        <Stack spacing={2} sx={{ mt: 1 }}>
          <Typography variant="body2" color="text.secondary">
            Audio: <code>{audioKey || "—"}</code>
          </Typography>
          {beatsKey ? (
            <Typography variant="body2" color="text.secondary">
              Beats: <code>{beatsKey}</code>
            </Typography>
          ) : null}
          <Typography variant="body2" color="text.secondary">
            Duration: <code>{duration ? `${duration.toFixed(2)}s` : "—"}</code> ·
            BPM: <code>{bpm ?? "—"}</code> · beats:{" "}
            <code>{beats.length || "—"}</code>
          </Typography>

          <Box
            sx={{
              border: "1px solid rgba(255,255,255,0.12)",
              borderRadius: 1,
              p: 1,
            }}
          >
            <canvas
              ref={canvasRef}
              width={CANVAS_WIDTH}
              height={CANVAS_HEIGHT}
              style={{ width: "100%", height: "auto", display: "block" }}
            />
          </Box>
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
      </DialogActions>
    </Dialog>
  );
}
