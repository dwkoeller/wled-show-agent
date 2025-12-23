import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import RefreshIcon from "@mui/icons-material/Refresh";
import SettingsIcon from "@mui/icons-material/Settings";
import StopIcon from "@mui/icons-material/Stop";
import TuneIcon from "@mui/icons-material/Tune";
import {
  Alert,
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
import { api } from "../../api";
import { useAuth } from "../../auth";
import { useEventRefresh } from "../../hooks/useEventRefresh";

type Json = unknown;

type LedfxItem = {
  id?: string;
  name?: string;
  [key: string]: unknown;
};

type FleetSummary = {
  ok?: boolean;
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

export function LedfxTools() {
  const { config } = useAuth();
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const [status, setStatus] = useState<Json | null>(null);
  const [virtuals, setVirtuals] = useState<LedfxItem[]>([]);
  const [scenes, setScenes] = useState<LedfxItem[]>([]);
  const [effects, setEffects] = useState<LedfxItem[]>([]);
  const [lastApplied, setLastApplied] = useState<Record<string, unknown> | null>(
    null,
  );
  const [fleetSummary, setFleetSummary] = useState<FleetSummary | null>(null);
  const [actionResult, setActionResult] = useState<Json | null>(null);

  const [sceneId, setSceneId] = useState("");
  const [virtualId, setVirtualId] = useState("");
  const [effectName, setEffectName] = useState("");
  const [effectConfig, setEffectConfig] = useState("{}");
  const [brightness, setBrightness] = useState("0.6");

  const [reqMethod, setReqMethod] = useState("GET");
  const [reqPath, setReqPath] = useState("/api/virtuals");
  const [reqBody, setReqBody] = useState("");
  const [reqResult, setReqResult] = useState<Json | null>(null);

  const enabled = Boolean(config?.ledfx_enabled);

  const virtualIds = useMemo(
    () =>
      virtuals
        .map((v) => String(v.id || v.name || "").trim())
        .filter(Boolean),
    [virtuals],
  );
  const sceneIds = useMemo(
    () =>
      scenes
        .map((s) => String(s.id || s.name || "").trim())
        .filter(Boolean),
    [scenes],
  );
  const effectIds = useMemo(
    () =>
      effects
        .map((e) => String(e.id || e.name || "").trim())
        .filter(Boolean),
    [effects],
  );
  const selectedEffect = useMemo(() => {
    if (!effectName.trim()) return null;
    const target = effectName.trim().toLowerCase();
    return (
      effects.find((e) => String(e.id || "").toLowerCase() === target) ||
      effects.find((e) => String(e.name || "").toLowerCase() === target) ||
      null
    );
  }, [effects, effectName]);

  const fleetAgents = useMemo(() => {
    const agents = fleetSummary?.summary?.agents;
    if (!agents || typeof agents !== "object") return [];
    return Object.entries(agents);
  }, [fleetSummary]);

  const refresh = async () => {
    setBusy(true);
    setError(null);
    try {
      const out: any = {};
      try {
        out.status = await api("/v1/ledfx/status", { method: "GET" });
      } catch (e) {
        out.status = {
          ok: false,
          error: e instanceof Error ? e.message : String(e),
        };
      }
      try {
        const res = await api<{ ok: boolean; virtuals: LedfxItem[] }>(
          "/v1/ledfx/virtuals",
          { method: "GET" },
        );
        setVirtuals(res.virtuals || []);
        out.virtuals = res;
      } catch (e) {
        setVirtuals([]);
        out.virtuals = {
          ok: false,
          error: e instanceof Error ? e.message : String(e),
        };
      }
      try {
        const res = await api<{ ok: boolean; scenes: LedfxItem[] }>(
          "/v1/ledfx/scenes",
          { method: "GET" },
        );
        setScenes(res.scenes || []);
        out.scenes = res;
      } catch (e) {
        setScenes([]);
        out.scenes = {
          ok: false,
          error: e instanceof Error ? e.message : String(e),
        };
      }
      try {
        const res = await api<{ ok: boolean; effects: LedfxItem[] }>(
          "/v1/ledfx/effects",
          { method: "GET" },
        );
        setEffects(res.effects || []);
        out.effects = res;
      } catch (e) {
        setEffects([]);
        out.effects = {
          ok: false,
          error: e instanceof Error ? e.message : String(e),
        };
      }
      try {
        const res = await api<{ ok: boolean; last_applied: Record<string, any> }>(
          "/v1/meta/last_applied",
          { method: "GET" },
        );
        const all = res.last_applied || {};
        const filtered: Record<string, unknown> = {};
        Object.entries(all).forEach(([key, value]) => {
          if (key.startsWith("ledfx")) {
            filtered[key] = value;
          }
        });
        setLastApplied(filtered);
        out.last_applied = res;
      } catch (e) {
        setLastApplied(null);
        out.last_applied = {
          ok: false,
          error: e instanceof Error ? e.message : String(e),
        };
      }
      try {
        const res = await api<FleetSummary>("/v1/ledfx/fleet", { method: "GET" });
        setFleetSummary(res);
        out.fleet = res;
      } catch (e) {
        setFleetSummary(null);
        out.fleet = {
          ok: false,
          error: e instanceof Error ? e.message : String(e),
        };
      }
      setStatus(out.status);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  useEffect(() => {
    void refresh();
  }, []);

  useEventRefresh({
    types: ["ledfx", "meta", "tick"],
    refresh,
    minIntervalMs: 3000,
  });

  const activateScene = async () => {
    setBusy(true);
    setError(null);
    try {
      const res = await api("/v1/ledfx/scene/activate", {
        method: "POST",
        json: { scene_id: sceneId },
      });
      setActionResult(res);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const deactivateScene = async () => {
    setBusy(true);
    setError(null);
    try {
      const res = await api("/v1/ledfx/scene/deactivate", {
        method: "POST",
        json: { scene_id: sceneId },
      });
      setActionResult(res);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const setEffect = async () => {
    setBusy(true);
    setError(null);
    try {
      let cfg: Record<string, unknown> = {};
      if (effectConfig.trim()) {
        cfg = JSON.parse(effectConfig);
      }
      const res = await api("/v1/ledfx/virtual/effect", {
        method: "POST",
        json: {
          virtual_id: virtualId.trim() ? virtualId.trim() : null,
          effect: effectName.trim(),
          config: cfg,
        },
      });
      setActionResult(res);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const setVirtualBrightness = async () => {
    setBusy(true);
    setError(null);
    try {
      const value = parseFloat(brightness || "0");
      if (Number.isNaN(value)) {
        throw new Error("Brightness must be a number");
      }
      const res = await api("/v1/ledfx/virtual/brightness", {
        method: "POST",
        json: {
          virtual_id: virtualId.trim() ? virtualId.trim() : null,
          brightness: value,
        },
      });
      setActionResult(res);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const sendRaw = async () => {
    setBusy(true);
    setError(null);
    try {
      let payload: unknown = null;
      if (reqBody.trim()) {
        payload = JSON.parse(reqBody);
      }
      const res = await api("/v1/ledfx/request", {
        method: "POST",
        json: {
          method: reqMethod,
          path: reqPath,
          json_body: payload,
        },
      });
      setReqResult(res);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Stack spacing={2}>
      {error ? <Alert severity="error">{error}</Alert> : null}
      {!enabled ? (
        <Alert severity="info">
          LedFx is not configured. Set <code>LEDFX_BASE_URL</code> in your
          agent environment.
        </Alert>
      ) : null}

      {virtualIds.length ? (
        <datalist id="ledfx-virtuals">
          {virtualIds.map((id) => (
            <option key={id} value={id} />
          ))}
        </datalist>
      ) : null}
      {sceneIds.length ? (
        <datalist id="ledfx-scenes">
          {sceneIds.map((id) => (
            <option key={id} value={id} />
          ))}
        </datalist>
      ) : null}
      {effectIds.length ? (
        <datalist id="ledfx-effects">
          {effectIds.map((id) => (
            <option key={id} value={id} />
          ))}
        </datalist>
      ) : null}

      <Card>
        <CardContent>
          <Typography variant="h6">LedFx Status</Typography>
          <Stack direction="row" spacing={1} sx={{ mt: 1, flexWrap: "wrap" }}>
            <Chip
              label={enabled ? "configured" : "not configured"}
              size="small"
              color={enabled ? "success" : "default"}
            />
            <Chip
              label={`virtuals ${virtualIds.length}`}
              size="small"
              variant="outlined"
            />
            <Chip
              label={`scenes ${sceneIds.length}`}
              size="small"
              variant="outlined"
            />
            <Chip
              label={`effects ${effectIds.length}`}
              size="small"
              variant="outlined"
            />
          </Stack>
          <Box
            component="pre"
            sx={{
              whiteSpace: "pre-wrap",
              wordBreak: "break-word",
              fontSize: 12,
              mt: 2,
            }}
          >
            {status ? JSON.stringify(status, null, 2) : "-"}
          </Box>
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
          <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
            Persists the most recent LedFx scene/effect/brightness changes.
          </Typography>
          <Box
            component="pre"
            sx={{
              whiteSpace: "pre-wrap",
              wordBreak: "break-word",
              fontSize: 12,
              mt: 2,
            }}
          >
            {lastApplied ? JSON.stringify(lastApplied, null, 2) : "-"}
          </Box>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Fleet summary</Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
            Aggregated from peer A2A calls (cached briefly).
          </Typography>
          <Stack direction="row" spacing={1} sx={{ mt: 2, flexWrap: "wrap" }}>
            <Chip
              size="small"
              label={`total ${fleetSummary?.summary?.total ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`enabled ${fleetSummary?.summary?.enabled ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`healthy ${fleetSummary?.summary?.healthy ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`cached ${fleetSummary?.cached ? "yes" : "no"}`}
              variant="outlined"
            />
          </Stack>
          <Stack spacing={1} sx={{ mt: 2 }}>
            {fleetAgents.length ? (
              fleetAgents.map(([name, raw]) => {
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
                    <Typography variant="caption" color="text.secondary">
                      scene={lastScene ?? "—"} · effect={lastEffect ?? "—"}
                    </Typography>
                  </Stack>
                );
              })
            ) : (
              <Typography variant="body2" color="text.secondary">
                No fleet data yet.
              </Typography>
            )}
          </Stack>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Scenes</Typography>
          <Stack direction="row" spacing={2} sx={{ mt: 2, flexWrap: "wrap" }}>
            <TextField
              label="Scene id/name"
              value={sceneId}
              onChange={(e) => setSceneId(e.target.value)}
              size="small"
              inputProps={{ list: "ledfx-scenes" }}
            />
            <Button
              startIcon={<PlayArrowIcon />}
              onClick={activateScene}
              disabled={busy || !sceneId.trim()}
            >
              Activate
            </Button>
            <Button
              startIcon={<StopIcon />}
              onClick={deactivateScene}
              disabled={busy || !sceneId.trim()}
            >
              Deactivate
            </Button>
          </Stack>
          {sceneIds.length ? (
            <Stack direction="row" spacing={1} sx={{ mt: 2, flexWrap: "wrap" }}>
              {sceneIds.map((id) => (
                <Chip key={id} label={id} size="small" />
              ))}
            </Stack>
          ) : null}
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Virtuals</Typography>
          <Stack direction="row" spacing={2} sx={{ mt: 2, flexWrap: "wrap" }}>
            <TextField
              label="Virtual id/name"
              value={virtualId}
              onChange={(e) => setVirtualId(e.target.value)}
              size="small"
              inputProps={{ list: "ledfx-virtuals" }}
            />
            <TextField
              label="Effect"
              value={effectName}
              onChange={(e) => setEffectName(e.target.value)}
              size="small"
              inputProps={{ list: "ledfx-effects" }}
            />
            <TextField
              label="Effect config (JSON)"
              value={effectConfig}
              onChange={(e) => setEffectConfig(e.target.value)}
              size="small"
              fullWidth
            />
            <Button
              startIcon={<TuneIcon />}
              onClick={setEffect}
              disabled={busy || !effectName.trim()}
            >
              Set Effect
            </Button>
          </Stack>
          {selectedEffect ? (
            <Box
              component="pre"
              sx={{
                whiteSpace: "pre-wrap",
                wordBreak: "break-word",
                fontSize: 12,
                mt: 2,
              }}
            >
              {JSON.stringify(selectedEffect, null, 2)}
            </Box>
          ) : null}
          <Stack direction="row" spacing={2} sx={{ mt: 2, flexWrap: "wrap" }}>
            <TextField
              label="Brightness (0..1 or 0..255)"
              value={brightness}
              onChange={(e) => setBrightness(e.target.value)}
              size="small"
            />
            <Button
              startIcon={<SettingsIcon />}
              onClick={setVirtualBrightness}
              disabled={busy}
            >
              Set Brightness
            </Button>
          </Stack>
          {virtualIds.length ? (
            <Stack direction="row" spacing={1} sx={{ mt: 2, flexWrap: "wrap" }}>
              {virtualIds.map((id) => (
                <Chip key={id} label={id} size="small" />
              ))}
            </Stack>
          ) : null}
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Raw Request</Typography>
          <Stack direction="row" spacing={2} sx={{ mt: 2, flexWrap: "wrap" }}>
            <FormControl size="small" sx={{ minWidth: 120 }}>
              <InputLabel id="ledfx-method-label">Method</InputLabel>
              <Select
                labelId="ledfx-method-label"
                value={reqMethod}
                label="Method"
                onChange={(e) => setReqMethod(e.target.value)}
              >
                <MenuItem value="GET">GET</MenuItem>
                <MenuItem value="POST">POST</MenuItem>
                <MenuItem value="PUT">PUT</MenuItem>
                <MenuItem value="PATCH">PATCH</MenuItem>
                <MenuItem value="DELETE">DELETE</MenuItem>
              </Select>
            </FormControl>
            <TextField
              label="Path"
              value={reqPath}
              onChange={(e) => setReqPath(e.target.value)}
              size="small"
              fullWidth
            />
          </Stack>
          <TextField
            label="JSON body (optional)"
            value={reqBody}
            onChange={(e) => setReqBody(e.target.value)}
            size="small"
            fullWidth
            multiline
            minRows={3}
            sx={{ mt: 2 }}
          />
          <Box
            component="pre"
            sx={{
              whiteSpace: "pre-wrap",
              wordBreak: "break-word",
              fontSize: 12,
              mt: 2,
            }}
          >
            {reqResult ? JSON.stringify(reqResult, null, 2) : "-"}
          </Box>
        </CardContent>
        <CardActions>
          <Button startIcon={<RefreshIcon />} onClick={sendRaw} disabled={busy}>
            Send
          </Button>
        </CardActions>
      </Card>

      {actionResult ? (
        <Card>
          <CardContent>
            <Typography variant="h6">Last Action</Typography>
            <Box
              component="pre"
              sx={{
                whiteSpace: "pre-wrap",
                wordBreak: "break-word",
                fontSize: 12,
                mt: 2,
              }}
            >
              {JSON.stringify(actionResult, null, 2)}
            </Box>
          </CardContent>
        </Card>
      ) : null}
    </Stack>
  );
}
