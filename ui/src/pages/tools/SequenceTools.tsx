import AutoFixHighIcon from "@mui/icons-material/AutoFixHigh";
import {
  Alert,
  Button,
  Card,
  CardActions,
  CardContent,
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
import { useNavigate } from "react-router-dom";
import { api } from "../../api";
import { useEventRefresh } from "../../hooks/useEventRefresh";

export function SequenceTools() {
  const nav = useNavigate();
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const [packs, setPacks] = useState<string[]>([]);
  const [beatsFiles, setBeatsFiles] = useState<string[]>([]);

  const [name, setName] = useState("BeatMix");
  const [packFile, setPackFile] = useState<string>("");
  const [durationS, setDurationS] = useState("120");
  const [stepS, setStepS] = useState("8");
  const [includeDdp, setIncludeDdp] = useState(true);
  const [renderableOnly, setRenderableOnly] = useState(false);
  const [seed, setSeed] = useState("1337");

  const [beatsFile, setBeatsFile] = useState<string>("");
  const [beatsPerStep, setBeatsPerStep] = useState("4");
  const [beatOffsetS, setBeatOffsetS] = useState("0.0");

  const beatsEnabled = useMemo(() => Boolean(beatsFile.trim()), [beatsFile]);

  const refresh = async () => {
    try {
      const res = await api<{
        ok: boolean;
        packs: string[];
        latest: string | null;
      }>("/v1/looks/packs", { method: "GET" });
      const all = res.packs || [];
      setPacks(all);
      setPackFile((prev) => prev || res.latest || all[all.length - 1] || "");
    } catch {
      setPacks([]);
    }
    try {
      const res = await api<{ ok: boolean; files: string[] }>(
        "/v1/files/list?dir=audio&recursive=true&glob=*.json&limit=200",
        { method: "GET" },
      );
      const all = (res.files || []).filter((f) =>
        f.toLowerCase().includes("beats"),
      );
      setBeatsFiles(all);
    } catch {
      setBeatsFiles([]);
    }
  };

  useEffect(() => {
    void refresh();
  }, []);

  useEventRefresh({
    types: ["sequences", "looks", "files", "tick"],
    refresh,
    minIntervalMs: 4000,
    ignoreEvents: ["list", "status"],
  });

  const submit = async () => {
    setBusy(true);
    setError(null);
    try {
      await api("/v1/jobs/sequences/generate", {
        method: "POST",
        json: {
          name,
          pack_file: packFile || null,
          duration_s: parseInt(durationS || "120", 10),
          step_s: parseInt(stepS || "8", 10),
          include_ddp: Boolean(includeDdp),
          renderable_only: Boolean(renderableOnly),
          beats_file: beatsEnabled ? beatsFile : null,
          beats_per_step: parseInt(beatsPerStep || "4", 10),
          beat_offset_s: parseFloat(beatOffsetS || "0"),
          seed: parseInt(seed || "1337", 10),
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
          <Typography variant="h6">Sequence Generator</Typography>
          <Typography variant="body2" color="text.secondary">
            Generates a deterministic cue list under{" "}
            <code>DATA_DIR/sequences</code>. Optionally align steps to a beat
            grid.
          </Typography>

          <Stack spacing={2} sx={{ mt: 2 }}>
            <TextField
              label="Name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              disabled={busy}
            />

            <FormControl fullWidth>
              <InputLabel>Looks pack</InputLabel>
              <Select
                value={packFile}
                label="Looks pack"
                onChange={(e) => setPackFile(String(e.target.value))}
                disabled={busy}
              >
                {packs.map((p) => (
                  <MenuItem key={p} value={p}>
                    {p}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <TextField
                label="Duration (s)"
                value={durationS}
                onChange={(e) => setDurationS(e.target.value)}
                disabled={busy}
                fullWidth
              />
              <TextField
                label={
                  beatsEnabled
                    ? "Step (s) (ignored when beats_file set)"
                    : "Step (s)"
                }
                value={stepS}
                onChange={(e) => setStepS(e.target.value)}
                disabled={busy}
                fullWidth
              />
            </Stack>

            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <FormControlLabel
                control={
                  <Switch
                    checked={includeDdp}
                    onChange={(e) => setIncludeDdp(e.target.checked)}
                  />
                }
                label="Include DDP steps"
              />
              <FormControlLabel
                control={
                  <Switch
                    checked={renderableOnly}
                    onChange={(e) => setRenderableOnly(e.target.checked)}
                  />
                }
                label="Renderable-only (DDP only)"
              />
            </Stack>

            <TextField
              label="Seed"
              value={seed}
              onChange={(e) => setSeed(e.target.value)}
              disabled={busy}
            />

            <Card variant="outlined">
              <CardContent>
                <Typography variant="subtitle1">
                  Beat-Aligned Options
                </Typography>
                <Stack spacing={2} sx={{ mt: 1 }}>
                  <FormControl fullWidth>
                    <InputLabel>beats_file (optional)</InputLabel>
                    <Select
                      value={beatsFile}
                      label="beats_file (optional)"
                      onChange={(e) => setBeatsFile(String(e.target.value))}
                      disabled={busy}
                    >
                      <MenuItem value="">(none)</MenuItem>
                      {beatsFiles.map((f) => (
                        <MenuItem key={f} value={f}>
                          {f}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>

                  <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
                    <TextField
                      label="Beats per step"
                      value={beatsPerStep}
                      onChange={(e) => setBeatsPerStep(e.target.value)}
                      disabled={busy || !beatsEnabled}
                      fullWidth
                    />
                    <TextField
                      label="Beat offset (s)"
                      value={beatOffsetS}
                      onChange={(e) => setBeatOffsetS(e.target.value)}
                      disabled={busy || !beatsEnabled}
                      fullWidth
                    />
                  </Stack>
                </Stack>
              </CardContent>
            </Card>
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            startIcon={<AutoFixHighIcon />}
            onClick={submit}
            disabled={busy || !name.trim()}
          >
            Generate (job)
          </Button>
          <Button onClick={refresh} disabled={busy}>
            Refresh lists
          </Button>
        </CardActions>
      </Card>
    </Stack>
  );
}
