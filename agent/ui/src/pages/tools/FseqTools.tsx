import DownloadIcon from "@mui/icons-material/Download";
import MovieFilterIcon from "@mui/icons-material/MovieFilter";
import {
  Alert,
  Button,
  Card,
  CardActions,
  CardContent,
  FormControl,
  InputLabel,
  MenuItem,
  Select,
  Stack,
  TextField,
  Typography,
} from "@mui/material";
import React, { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "../../api";

export function FseqTools() {
  const nav = useNavigate();
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const [sequences, setSequences] = useState<string[]>([]);
  const [fseqFiles, setFseqFiles] = useState<string[]>([]);

  const [sequenceFile, setSequenceFile] = useState("");
  const [outFile, setOutFile] = useState("fseq/export.fseq");
  const [stepMs, setStepMs] = useState("50");
  const [channelStart, setChannelStart] = useState("1");
  const [channelsTotal, setChannelsTotal] = useState("");
  const [ledCount, setLedCount] = useState("");
  const [defaultBri, setDefaultBri] = useState("128");

  const refresh = async () => {
    try {
      const seq = await api<{ ok: boolean; files: string[] }>(
        "/v1/sequences/list",
        { method: "GET" },
      );
      setSequences(seq.files || []);
      setSequenceFile((prev) => prev || (seq.files || [])[0] || "");
    } catch {
      setSequences([]);
    }
    try {
      const res = await api<{ ok: boolean; files: string[] }>(
        "/v1/files/list?dir=fseq&recursive=true&glob=*.fseq&limit=200",
        { method: "GET" },
      );
      setFseqFiles(res.files || []);
    } catch {
      setFseqFiles([]);
    }
  };

  useEffect(() => {
    void refresh();
  }, []);

  const suggestedOut = useMemo(() => {
    if (!sequenceFile) return null;
    const base = sequenceFile
      .replace(/^sequence_/, "")
      .replace(/\\.json$/i, "");
    return `fseq/${base}.fseq`;
  }, [sequenceFile]);

  const submit = async () => {
    setBusy(true);
    setError(null);
    try {
      await api("/v1/jobs/fseq/export", {
        method: "POST",
        json: {
          sequence_file: sequenceFile,
          out_file: outFile,
          step_ms: parseInt(stepMs || "50", 10),
          channel_start: parseInt(channelStart || "1", 10),
          channels_total: channelsTotal.trim()
            ? parseInt(channelsTotal.trim(), 10)
            : null,
          led_count: ledCount.trim() ? parseInt(ledCount.trim(), 10) : null,
          default_brightness: parseInt(defaultBri || "128", 10),
        },
      });
      await refresh();
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
          <Typography variant="h6">Export .fseq</Typography>
          <Typography variant="body2" color="text.secondary">
            Exports a renderable sequence (procedural <code>ddp</code> steps
            only) to an uncompressed v1 <code>.fseq</code>.
          </Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <FormControl fullWidth>
              <InputLabel>Sequence file</InputLabel>
              <Select
                value={sequenceFile}
                label="Sequence file"
                onChange={(e) => setSequenceFile(String(e.target.value))}
                disabled={busy}
              >
                {sequences.map((f) => (
                  <MenuItem key={f} value={f}>
                    {f}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            <TextField
              label="Output file"
              value={outFile}
              onChange={(e) => setOutFile(e.target.value)}
              disabled={busy}
              helperText={
                suggestedOut ? `Suggested: ${suggestedOut}` : undefined
              }
            />
            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <TextField
                label="Step (ms)"
                value={stepMs}
                onChange={(e) => setStepMs(e.target.value)}
                disabled={busy}
                fullWidth
              />
              <TextField
                label="Channel start"
                value={channelStart}
                onChange={(e) => setChannelStart(e.target.value)}
                disabled={busy}
                fullWidth
              />
            </Stack>
            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <TextField
                label="Channels total (optional)"
                value={channelsTotal}
                onChange={(e) => setChannelsTotal(e.target.value)}
                disabled={busy}
                fullWidth
              />
              <TextField
                label="LED count (optional)"
                value={ledCount}
                onChange={(e) => setLedCount(e.target.value)}
                disabled={busy}
                fullWidth
              />
            </Stack>
            <TextField
              label="Default brightness"
              value={defaultBri}
              onChange={(e) => setDefaultBri(e.target.value)}
              disabled={busy}
            />
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            startIcon={<MovieFilterIcon />}
            onClick={submit}
            disabled={busy || !sequenceFile.trim() || !outFile.trim()}
          >
            Export (job)
          </Button>
          <Button onClick={refresh} disabled={busy}>
            Refresh
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Existing .fseq Files</Typography>
          <Typography variant="body2" color="text.secondary">
            Downloads are served from <code>DATA_DIR</code>.
          </Typography>
          <Stack spacing={1} sx={{ mt: 2 }}>
            {fseqFiles.length ? (
              fseqFiles.map((f) => (
                <Stack
                  key={f}
                  direction="row"
                  spacing={1}
                  sx={{ alignItems: "center" }}
                >
                  <Typography variant="body2" sx={{ flexGrow: 1 }}>
                    <code>{f}</code>
                  </Typography>
                  <Button
                    size="small"
                    startIcon={<DownloadIcon />}
                    component="a"
                    href={`/v1/files/download?path=${encodeURIComponent(f)}`}
                    target="_blank"
                    rel="noreferrer"
                  >
                    Download
                  </Button>
                </Stack>
              ))
            ) : (
              <Typography variant="body2">
                No .fseq files found under <code>DATA_DIR/fseq</code>.
              </Typography>
            )}
          </Stack>
        </CardContent>
      </Card>
    </Stack>
  );
}
