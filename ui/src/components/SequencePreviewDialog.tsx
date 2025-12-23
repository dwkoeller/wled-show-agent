import {
  Alert,
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControl,
  InputLabel,
  LinearProgress,
  MenuItem,
  Select,
  Stack,
  Typography,
} from "@mui/material";
import React, { useEffect, useMemo, useState } from "react";

type PreviewFormat = "gif" | "mp4";

export function SequencePreviewDialog(props: {
  open: boolean;
  file: string | null;
  onClose: () => void;
}) {
  const { open, file, onClose } = props;
  const [format, setFormat] = useState<PreviewFormat>("gif");
  const [previewUrl, setPreviewUrl] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const [forceRefresh, setForceRefresh] = useState(false);

  const fileKey = useMemo(() => String(file ?? ""), [file]);

  useEffect(() => {
    if (!open) {
      setPreviewUrl(null);
      setError(null);
      setBusy(false);
    }
  }, [open]);

  useEffect(() => {
    if (!open || !fileKey) return;
    let active = true;
    setBusy(true);
    setError(null);
    const params = new URLSearchParams();
    params.set("file", fileKey);
    params.set("format", format);
    if (forceRefresh) params.set("refresh", "1");
    params.set("_", String(refreshKey || Date.now()));
    const url = `/v1/sequences/preview?${params.toString()}`;

    fetch(url, { credentials: "include" })
      .then(async (resp) => {
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
        const urlObj = URL.createObjectURL(blob);
        if (!active) {
          URL.revokeObjectURL(urlObj);
          return;
        }
        setPreviewUrl((prev) => {
          if (prev) URL.revokeObjectURL(prev);
          return urlObj;
        });
      })
      .catch((e) => {
        if (!active) return;
        setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => {
        if (!active) return;
        setBusy(false);
        setForceRefresh(false);
      });

    return () => {
      active = false;
    };
  }, [open, fileKey, format, refreshKey, forceRefresh]);

  useEffect(
    () => () => {
      if (previewUrl) URL.revokeObjectURL(previewUrl);
    },
    [previewUrl],
  );

  const triggerRefresh = () => {
    setForceRefresh(true);
    setRefreshKey(Date.now());
  };

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>Sequence preview</DialogTitle>
      <DialogContent>
        {busy ? <LinearProgress sx={{ mb: 2 }} /> : null}
        {error ? <Alert severity="error">{error}</Alert> : null}

        <Stack spacing={2} sx={{ mt: 1 }}>
          <Typography variant="body2" color="text.secondary">
            File: <code>{fileKey || "—"}</code>
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Requires <code>ffmpeg</code> in the API container.
          </Typography>
          <FormControl fullWidth size="small">
            <InputLabel>Format</InputLabel>
            <Select
              label="Format"
              value={format}
              onChange={(e) => setFormat(e.target.value as PreviewFormat)}
              disabled={busy}
            >
              <MenuItem value="gif">GIF</MenuItem>
              <MenuItem value="mp4">MP4</MenuItem>
            </Select>
          </FormControl>

          {previewUrl ? (
            format === "gif" ? (
              <Box
                component="img"
                src={previewUrl}
                alt="Sequence preview"
                sx={{ width: "100%", borderRadius: 1, border: "1px solid rgba(255,255,255,0.12)" }}
              />
            ) : (
              <Box
                component="video"
                src={previewUrl}
                controls
                sx={{ width: "100%", borderRadius: 1, border: "1px solid rgba(255,255,255,0.12)" }}
              />
            )
          ) : (
            <Typography variant="body2" color="text.secondary">
              Preview not available yet.
            </Typography>
          )}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={triggerRefresh} disabled={busy || !fileKey}>
          Re-render
        </Button>
        <Button onClick={onClose}>Close</Button>
      </DialogActions>
    </Dialog>
  );
}
