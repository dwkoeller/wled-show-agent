import RefreshIcon from "@mui/icons-material/Refresh";
import StopIcon from "@mui/icons-material/Stop";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import {
  Alert,
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Box,
  Button,
  Card,
  CardActions,
  CardContent,
  Chip,
  FormControl,
  InputLabel,
  MenuItem,
  Select,
  Stack,
  TextField,
  Typography,
} from "@mui/material";
import React, { useEffect, useMemo, useState } from "react";
import { api } from "../api";
import { useAuth } from "../auth";

type Json = unknown;

function parseTargets(raw: string): string[] | null {
  const out = raw
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
  return out.length ? out : null;
}

export function DashboardPage() {
  const { config } = useAuth();
  const [status, setStatus] = useState<Json | null>(null);
  const [patterns, setPatterns] = useState<string[]>([]);
  const [sequences, setSequences] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const [lookTheme, setLookTheme] = useState("");
  const [lookBri, setLookBri] = useState<string>("");
  const [lookScope, setLookScope] = useState<"local" | "fleet">("fleet");
  const [lookTargets, setLookTargets] = useState("");

  const [pat, setPat] = useState("");
  const [dur, setDur] = useState("30");
  const [bri, setBri] = useState("128");
  const [fps, setFps] = useState("");
  const [direction, setDirection] = useState("");
  const [startPos, setStartPos] = useState("");
  const [patScope, setPatScope] = useState<"local" | "fleet">("fleet");
  const [patTargets, setPatTargets] = useState("");

  const [seqFile, setSeqFile] = useState("");
  const [seqLoop, setSeqLoop] = useState(false);
  const [seqTargets, setSeqTargets] = useState("");

  const pretty = useMemo(() => JSON.stringify(status, null, 2), [status]);

  const refresh = async () => {
    setError(null);
    setBusy(true);
    try {
      const out: Record<string, unknown> = {};
      out.health = await api("/v1/health", { method: "GET" });
      out.wled = await api("/v1/wled/info", { method: "GET" });
      out.ddp = await api("/v1/ddp/status", { method: "GET" });
      out.sequence = await api("/v1/sequences/status", { method: "GET" });
      try {
        out.fleet = await api("/v1/fleet/peers", { method: "GET" });
        out.fleet_status = await api("/v1/fleet/invoke", {
          method: "POST",
          json: { action: "status", params: {}, include_self: true },
        });
      } catch (e) {
        out.fleet = {
          ok: false,
          error: e instanceof Error ? e.message : String(e),
        };
      }
      setStatus(out);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const loadLists = async () => {
    try {
      const pats = await api<{ ok: boolean; patterns: string[] }>(
        "/v1/ddp/patterns",
        { method: "GET" },
      );
      setPatterns(pats.patterns || []);
      setPat((pats.patterns || [])[0] ?? "");
    } catch {
      setPatterns([]);
    }
    try {
      const seq = await api<{ ok: boolean; files: string[] }>(
        "/v1/sequences/list",
        { method: "GET" },
      );
      setSequences(seq.files || []);
      setSeqFile((seq.files || [])[0] ?? "");
    } catch {
      setSequences([]);
    }
  };

  useEffect(() => {
    void loadLists().then(refresh);
  }, []);

  const stopAll = async () => {
    setBusy(true);
    setError(null);
    try {
      await api("/v1/fleet/stop_all", { method: "POST", json: {} });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const applyLook = async () => {
    setBusy(true);
    setError(null);
    try {
      const theme = lookTheme.trim() || null;
      const brightness = lookBri.trim() ? parseInt(lookBri.trim(), 10) : null;
      const targets = parseTargets(lookTargets);
      if (lookScope === "fleet") {
        await api("/v1/fleet/apply_random_look", {
          method: "POST",
          json: { theme, brightness, targets, include_self: true },
        });
      } else {
        await api("/v1/looks/apply_random", {
          method: "POST",
          json: { theme, brightness },
        });
      }
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const startPattern = async () => {
    setBusy(true);
    setError(null);
    try {
      const payload: Record<string, unknown> = {
        pattern: pat,
        duration_s: parseFloat(dur || "30"),
        brightness: parseInt(bri || "128", 10),
      };
      if (fps.trim()) payload.fps = parseFloat(fps.trim());
      if (direction.trim()) payload.direction = direction.trim();
      if (startPos.trim()) payload.start_pos = startPos.trim();
      const targets = parseTargets(patTargets);

      if (patScope === "fleet") {
        await api("/v1/fleet/invoke", {
          method: "POST",
          json: {
            action: "start_ddp_pattern",
            params: payload,
            targets,
            include_self: true,
          },
        });
      } else {
        await api("/v1/ddp/start", { method: "POST", json: payload });
      }
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const stopPattern = async () => {
    setBusy(true);
    setError(null);
    try {
      const targets = parseTargets(patTargets);
      if (patScope === "fleet") {
        await api("/v1/fleet/invoke", {
          method: "POST",
          json: { action: "stop_ddp", params: {}, targets, include_self: true },
        });
      } else {
        await api("/v1/ddp/stop", { method: "POST", json: {} });
      }
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const startSequence = async () => {
    setBusy(true);
    setError(null);
    try {
      const targets = parseTargets(seqTargets);
      await api("/v1/fleet/sequences/start", {
        method: "POST",
        json: { file: seqFile, loop: seqLoop, targets, include_self: true },
      });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const stopSequence = async () => {
    setBusy(true);
    setError(null);
    try {
      await api("/v1/fleet/sequences/stop", { method: "POST", json: {} });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Stack spacing={2}>
      {error ? <Alert severity="error">{error}</Alert> : null}

      <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap" }}>
        <Chip
          label={`OpenAI: ${config?.openai_enabled ? "on" : "off"}`}
          size="small"
        />
        <Chip
          label={`FPP: ${config?.fpp_enabled ? "on" : "off"}`}
          size="small"
        />
        <Chip label={`Peers: ${config?.peers_configured ?? 0}`} size="small" />
      </Stack>

      <Card>
        <CardContent>
          <Typography variant="h6">Status</Typography>
          <Box
            component="pre"
            sx={{
              whiteSpace: "pre-wrap",
              wordBreak: "break-word",
              fontSize: 12,
              mt: 1,
            }}
          >
            {pretty || "Loading…"}
          </Box>
        </CardContent>
        <CardActions>
          <Button startIcon={<RefreshIcon />} onClick={refresh} disabled={busy}>
            Refresh
          </Button>
          <Button
            color="error"
            startIcon={<StopIcon />}
            onClick={stopAll}
            disabled={busy}
          >
            Stop all
          </Button>
        </CardActions>
      </Card>

      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Quick Look</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Stack spacing={2}>
            <TextField
              label="Theme (optional)"
              value={lookTheme}
              onChange={(e) => setLookTheme(e.target.value)}
            />
            <TextField
              label="Brightness (optional)"
              inputMode="numeric"
              value={lookBri}
              onChange={(e) => setLookBri(e.target.value)}
            />
            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <FormControl fullWidth>
                <InputLabel>Scope</InputLabel>
                <Select
                  value={lookScope}
                  label="Scope"
                  onChange={(e) => setLookScope(e.target.value as any)}
                >
                  <MenuItem value="local">local</MenuItem>
                  <MenuItem value="fleet">fleet</MenuItem>
                </Select>
              </FormControl>
              <TextField
                label="Targets (comma-separated, optional)"
                value={lookTargets}
                onChange={(e) => setLookTargets(e.target.value)}
                fullWidth
              />
            </Stack>
            <Button variant="contained" onClick={applyLook} disabled={busy}>
              Apply random look
            </Button>
          </Stack>
        </AccordionDetails>
      </Accordion>

      <Accordion>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Realtime Pattern (DDP)</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Stack spacing={2}>
            <FormControl fullWidth>
              <InputLabel>Pattern</InputLabel>
              <Select
                value={pat}
                label="Pattern"
                onChange={(e) => setPat(e.target.value)}
              >
                {patterns.map((p) => (
                  <MenuItem key={p} value={p}>
                    {p}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <TextField
                label="Duration (s)"
                value={dur}
                onChange={(e) => setDur(e.target.value)}
                fullWidth
              />
              <TextField
                label="Brightness"
                value={bri}
                onChange={(e) => setBri(e.target.value)}
                fullWidth
              />
            </Stack>
            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <TextField
                label="FPS (optional)"
                value={fps}
                onChange={(e) => setFps(e.target.value)}
                fullWidth
              />
              <TextField
                label="Direction (cw/ccw, optional)"
                value={direction}
                onChange={(e) => setDirection(e.target.value)}
                fullWidth
              />
              <TextField
                label="Start pos (front/right/back/left, optional)"
                value={startPos}
                onChange={(e) => setStartPos(e.target.value)}
                fullWidth
              />
            </Stack>
            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <FormControl fullWidth>
                <InputLabel>Scope</InputLabel>
                <Select
                  value={patScope}
                  label="Scope"
                  onChange={(e) => setPatScope(e.target.value as any)}
                >
                  <MenuItem value="local">local</MenuItem>
                  <MenuItem value="fleet">fleet</MenuItem>
                </Select>
              </FormControl>
              <TextField
                label="Targets (comma-separated, optional)"
                value={patTargets}
                onChange={(e) => setPatTargets(e.target.value)}
                fullWidth
              />
            </Stack>
            <Stack direction="row" spacing={2}>
              <Button
                variant="contained"
                onClick={startPattern}
                disabled={busy}
              >
                Start
              </Button>
              <Button onClick={stopPattern} disabled={busy}>
                Stop
              </Button>
            </Stack>
          </Stack>
        </AccordionDetails>
      </Accordion>

      <Accordion>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Fleet Sequence</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Stack spacing={2}>
            <FormControl fullWidth>
              <InputLabel>Sequence file</InputLabel>
              <Select
                value={seqFile}
                label="Sequence file"
                onChange={(e) => setSeqFile(e.target.value)}
              >
                {sequences.map((f) => (
                  <MenuItem key={f} value={f}>
                    {f}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <FormControl fullWidth>
                <InputLabel>Loop</InputLabel>
                <Select
                  value={seqLoop ? "true" : "false"}
                  label="Loop"
                  onChange={(e) => setSeqLoop(e.target.value === "true")}
                >
                  <MenuItem value="false">false</MenuItem>
                  <MenuItem value="true">true</MenuItem>
                </Select>
              </FormControl>
              <TextField
                label="Targets (comma-separated, optional)"
                value={seqTargets}
                onChange={(e) => setSeqTargets(e.target.value)}
                fullWidth
              />
            </Stack>
            <Stack direction="row" spacing={2}>
              <Button
                variant="contained"
                onClick={startSequence}
                disabled={busy}
              >
                Start
              </Button>
              <Button onClick={stopSequence} disabled={busy}>
                Stop
              </Button>
            </Stack>
          </Stack>
        </AccordionDetails>
      </Accordion>
    </Stack>
  );
}
