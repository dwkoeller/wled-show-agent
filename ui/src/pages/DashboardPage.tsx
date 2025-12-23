import RefreshIcon from "@mui/icons-material/Refresh";
import StopIcon from "@mui/icons-material/Stop";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import VisibilityIcon from "@mui/icons-material/Visibility";
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
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControl,
  FormControlLabel,
  InputLabel,
  ListSubheader,
  MenuItem,
  Select,
  Stack,
  Switch,
  TextField,
  Typography,
} from "@mui/material";
import React, { useEffect, useMemo, useState } from "react";
import { api } from "../api";
import { useAuth } from "../auth";
import { TargetPreviewDialog } from "../components/TargetPreviewDialog";
import { useEventRefresh } from "../hooks/useEventRefresh";

type Json = unknown;

function parseTargets(raw: string): string[] | null {
  const out = raw
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
  return out.length ? out : null;
}

function parseHexColor(raw: string): [number, number, number] | null {
  const s = (raw || "").trim();
  if (!s) return null;
  const hex = s.startsWith("#") ? s.slice(1) : s;
  if (!/^[0-9a-fA-F]{6}$/.test(hex)) return null;
  const r = parseInt(hex.slice(0, 2), 16);
  const g = parseInt(hex.slice(2, 4), 16);
  const b = parseInt(hex.slice(4, 6), 16);
  if (![r, g, b].every((v) => Number.isFinite(v))) return null;
  return [r, g, b];
}

function rgbToHex(raw: unknown): string | null {
  if (!Array.isArray(raw) || raw.length < 3) return null;
  const [r, g, b] = raw;
  if (![r, g, b].every((v) => Number.isFinite(v))) return null;
  const clamp = (v: number) => Math.max(0, Math.min(255, Math.round(v)));
  const hex = [clamp(Number(r)), clamp(Number(g)), clamp(Number(b))]
    .map((v) => v.toString(16).padStart(2, "0"))
    .join("");
  return `#${hex}`;
}

function fmtTs(tsSeconds: number | null | undefined): string {
  if (!tsSeconds) return "—";
  try {
    return new Date(tsSeconds * 1000).toLocaleString();
  } catch {
    return String(tsSeconds);
  }
}

type CrossfadePreset = {
  id: string;
  label: string;
  look?: {
    name?: string;
    theme?: string;
    effect?: string;
    palette?: string;
    colors?: [string, string?, string?];
    speed?: string;
    intensity?: string;
    segment?: string;
    on?: boolean;
    reverse?: boolean;
  };
  state?: {
    on?: boolean;
    brightness?: string;
    effect?: string;
    palette?: string;
    colors?: [string, string?, string?];
    speed?: string;
    intensity?: string;
    segment?: string;
    reverse?: boolean;
  };
  brightness?: string;
  transition_ms?: string;
};

type OrchestrationPreset = {
  id: number;
  name: string;
  scope: string;
  description?: string | null;
  tags?: string[] | null;
  version?: number | null;
  payload?: Record<string, unknown> | null;
};

type FleetHealthRes = {
  ok: boolean;
  cached?: boolean;
  generated_at?: number;
  ttl_s?: number;
  summary?: {
    total?: number;
    online?: number;
    wled_ok?: number;
    fpp_ok?: number;
    ledfx_ok?: number;
  };
};

const CROSSFADE_PRESETS: CrossfadePreset[] = [
  {
    id: "warm_glow",
    label: "Warm glow",
    look: {
      name: "Warm glow",
      theme: "warm",
      effect: "Solid",
      palette: "Default",
      colors: ["#ffcc88", "#ff9966", "#ffddbb"],
      on: true,
    },
    brightness: "180",
    transition_ms: "1200",
  },
  {
    id: "candy_cane",
    label: "Candy cane",
    look: {
      name: "Candy cane",
      theme: "holiday",
      effect: "Solid",
      palette: "Default",
      colors: ["#ff0000", "#ffffff", "#ff0000"],
      on: true,
    },
    brightness: "200",
    transition_ms: "1200",
  },
  {
    id: "icy_blue",
    label: "Icy blue",
    look: {
      name: "Icy blue",
      theme: "icy",
      effect: "Solid",
      palette: "Default",
      colors: ["#66ccff", "#ffffff", "#cceeff"],
      on: true,
    },
    brightness: "170",
    transition_ms: "1400",
  },
  {
    id: "blackout",
    label: "Blackout",
    state: {
      on: false,
    },
    transition_ms: "1000",
  },
];

export function DashboardPage() {
  const { config } = useAuth();
  const [status, setStatus] = useState<Json | null>(null);
  const [patterns, setPatterns] = useState<string[]>([]);
  const [sequences, setSequences] = useState<string[]>([]);
  const [wledEffects, setWledEffects] = useState<string[]>([]);
  const [wledPalettes, setWledPalettes] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [fleetHealth, setFleetHealth] = useState<FleetHealthRes | null>(null);
  const [fleetHealthError, setFleetHealthError] = useState<string | null>(null);

  const [lookTheme, setLookTheme] = useState("");
  const [lookBri, setLookBri] = useState<string>("");
  const [lookScope, setLookScope] = useState<"local" | "fleet">("fleet");
  const [lookTargets, setLookTargets] = useState("");

  const [fadeLookJson, setFadeLookJson] = useState("");
  const [fadeStateJson, setFadeStateJson] = useState("");
  const [fadeBrightness, setFadeBrightness] = useState("");
  const [fadeTransitionMs, setFadeTransitionMs] = useState("1000");
  const [fadeScope, setFadeScope] = useState<"local" | "fleet">("fleet");
  const [fadeTargets, setFadeTargets] = useState("");
  const [fadeIncludeSelf, setFadeIncludeSelf] = useState(true);
  const [fadeUseLookBuilder, setFadeUseLookBuilder] = useState(false);
  const [fadeUseStateBuilder, setFadeUseStateBuilder] = useState(false);
  const [fadePreset, setFadePreset] = useState("");
  const [fadePresetName, setFadePresetName] = useState("");
  const [savedCrossfadePresets, setSavedCrossfadePresets] = useState<
    OrchestrationPreset[]
  >([]);
  const [fadeLookName, setFadeLookName] = useState("");
  const [fadeLookTheme, setFadeLookTheme] = useState("");
  const [fadeLookEffect, setFadeLookEffect] = useState("Solid");
  const [fadeLookPalette, setFadeLookPalette] = useState("Default");
  const [fadeLookColor1, setFadeLookColor1] = useState("#ff0000");
  const [fadeLookColor2, setFadeLookColor2] = useState("#00ff00");
  const [fadeLookColor3, setFadeLookColor3] = useState("#0000ff");
  const [fadeLookSpeed, setFadeLookSpeed] = useState("");
  const [fadeLookIntensity, setFadeLookIntensity] = useState("");
  const [fadeLookReverse, setFadeLookReverse] = useState(false);
  const [fadeLookSegment, setFadeLookSegment] = useState("");
  const [fadeLookOn, setFadeLookOn] = useState(true);
  const [fadeStateOn, setFadeStateOn] = useState(true);
  const [fadeStateBrightness, setFadeStateBrightness] = useState("");
  const [fadeStateEffect, setFadeStateEffect] = useState("");
  const [fadeStatePalette, setFadeStatePalette] = useState("");
  const [fadeStateColor1, setFadeStateColor1] = useState("#ff0000");
  const [fadeStateColor2, setFadeStateColor2] = useState("#00ff00");
  const [fadeStateColor3, setFadeStateColor3] = useState("#0000ff");
  const [fadeStateSpeed, setFadeStateSpeed] = useState("");
  const [fadeStateIntensity, setFadeStateIntensity] = useState("");
  const [fadeStateReverse, setFadeStateReverse] = useState(false);
  const [fadeStateSegment, setFadeStateSegment] = useState("");

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
  const [seqStaggerS, setSeqStaggerS] = useState("0.5");
  const [seqStartDelayS, setSeqStartDelayS] = useState("0");

  const [previewOpen, setPreviewOpen] = useState(false);
  const [previewTitle, setPreviewTitle] = useState("Targets preview");
  const [previewTargets, setPreviewTargets] = useState<string[] | null>(null);
  const [fadePreviewOpen, setFadePreviewOpen] = useState(false);
  const [fadePreviewJson, setFadePreviewJson] = useState("");

  const pretty = useMemo(() => JSON.stringify(status, null, 2), [status]);
  const ledfxPretty = useMemo(() => {
    if (!status || typeof status !== "object") return "";
    const raw = (status as { ledfx?: unknown }).ledfx;
    if (!raw) return "";
    try {
      return JSON.stringify(raw, null, 2);
    } catch {
      return String(raw);
    }
  }, [status]);

  const openPreview = (title: string, raw: string) => {
    setPreviewTitle(title);
    setPreviewTargets(parseTargets(raw));
    setPreviewOpen(true);
  };

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
        out.runtime_state = await api("/v1/runtime_state", { method: "GET" });
      } catch (e) {
        out.runtime_state = {
          ok: false,
          error: e instanceof Error ? e.message : String(e),
        };
      }
      try {
        out.scheduler = await api("/v1/scheduler/status", { method: "GET" });
      } catch (e) {
        out.scheduler = {
          ok: false,
          error: e instanceof Error ? e.message : String(e),
        };
      }
      try {
        out.fleet = await api("/v1/fleet/peers", { method: "GET" });
        try {
          out.fleet_db = await api("/v1/fleet/status", { method: "GET" });
        } catch (e) {
          out.fleet_db = {
            ok: false,
            error: e instanceof Error ? e.message : String(e),
          };
        }
        out.fleet_status = await api("/v1/fleet/invoke", {
          method: "POST",
          json: { action: "status", params: {}, include_self: true },
        });
        try {
          const health = await api<FleetHealthRes>("/v1/fleet/health", {
            method: "GET",
          });
          setFleetHealth(health);
          setFleetHealthError(null);
        } catch (e) {
          setFleetHealth(null);
          setFleetHealthError(e instanceof Error ? e.message : String(e));
        }
      } catch (e) {
        out.fleet = {
          ok: false,
          error: e instanceof Error ? e.message : String(e),
        };
        setFleetHealth(null);
        setFleetHealthError(null);
      }
      try {
        out.last_applied = await api("/v1/meta/last_applied", {
          method: "GET",
        });
      } catch (e) {
        out.last_applied = {
          ok: false,
          error: e instanceof Error ? e.message : String(e),
        };
      }
      if (config?.ledfx_enabled) {
        try {
          out.ledfx = await api("/v1/ledfx/status", { method: "GET" });
        } catch (e) {
          out.ledfx = {
            ok: false,
            error: e instanceof Error ? e.message : String(e),
          };
        }
      }
      await refreshCrossfadePresets();
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
    try {
      const eff = await api<{ ok: boolean; effects: string[] }>(
        "/v1/wled/effects",
        { method: "GET" },
      );
      setWledEffects(eff.effects || []);
    } catch {
      setWledEffects([]);
    }
    try {
      const pal = await api<{ ok: boolean; palettes: string[] }>(
        "/v1/wled/palettes",
        { method: "GET" },
      );
      setWledPalettes(pal.palettes || []);
    } catch {
      setWledPalettes([]);
    }
  };

  useEffect(() => {
    void loadLists().then(refresh);
  }, []);

  useEventRefresh({
    types: [
      "tick",
      "orchestration",
      "scheduler",
      "fleet",
      "ddp",
      "sequences",
      "ledfx",
      "meta",
    ],
    refresh,
    minIntervalMs: 5000,
  });

  const resolveIndex = (
    raw: string,
    label: string,
    list: string[],
    errors: string[],
  ) => {
    const trimmed = raw.trim();
    if (!trimmed) return undefined;
    const pos = list.indexOf(trimmed);
    if (pos >= 0) return pos;
    const num = parseInt(trimmed, 10);
    if (Number.isFinite(num)) return num;
    errors.push(`${label} must be a name or numeric id.`);
    return undefined;
  };

  const buildLookSpec = (errors: string[]) => {
    const effect = fadeLookEffect.trim() || "Solid";
    const palette = fadeLookPalette.trim() || "Default";
    if (wledEffects.length && fadeLookEffect.trim()) {
      if (!wledEffects.includes(effect)) {
        errors.push(`look effect '${effect}' not found.`);
      }
    }
    if (wledPalettes.length && fadeLookPalette.trim()) {
      if (!wledPalettes.includes(palette)) {
        errors.push(`look palette '${palette}' not found.`);
      }
    }

    const colors: number[][] = [];
    const c1 = parseHexColor(fadeLookColor1);
    const c2 = parseHexColor(fadeLookColor2);
    const c3 = parseHexColor(fadeLookColor3);
    if (fadeLookColor1.trim() && !c1) errors.push("look color 1 is invalid.");
    if (fadeLookColor2.trim() && !c2) errors.push("look color 2 is invalid.");
    if (fadeLookColor3.trim() && !c3) errors.push("look color 3 is invalid.");
    if (c1) colors.push(c1);
    if (c2) colors.push(c2);
    if (c3) colors.push(c3);

    const seg: Record<string, any> = {
      id: 0,
      fx: effect,
      pal: palette,
      on: Boolean(fadeLookOn),
    };
    if (fadeLookSegment.trim()) {
      const id = parseInt(fadeLookSegment, 10);
      if (!Number.isFinite(id)) {
        errors.push("look segment is not a number.");
      } else {
        seg.id = id;
      }
    }
    if (colors.length) seg.col = colors;
    if (fadeLookSpeed.trim()) {
      const sx = parseInt(fadeLookSpeed, 10);
      if (!Number.isFinite(sx)) errors.push("look speed is not a number.");
      else seg.sx = sx;
    }
    if (fadeLookIntensity.trim()) {
      const ix = parseInt(fadeLookIntensity, 10);
      if (!Number.isFinite(ix)) errors.push("look intensity is not a number.");
      else seg.ix = ix;
    }
    if (fadeLookReverse) seg.rev = 1;

    return {
      type: "wled_look",
      name: fadeLookName.trim() || undefined,
      theme: fadeLookTheme.trim() || undefined,
      seg,
    };
  };

  const buildStateSpec = (errors: string[]) => {
    const payload: Record<string, any> = {};
    payload.on = Boolean(fadeStateOn);
    if (fadeStateBrightness.trim()) {
      const bri = parseInt(fadeStateBrightness, 10);
      if (!Number.isFinite(bri)) errors.push("state brightness is not a number.");
      else payload.bri = bri;
    }

    const seg: Record<string, any> = {};
    const fx = resolveIndex(
      fadeStateEffect,
      "state effect",
      wledEffects,
      errors,
    );
    const pal = resolveIndex(
      fadeStatePalette,
      "state palette",
      wledPalettes,
      errors,
    );
    if (fx != null) seg.fx = fx;
    if (pal != null) seg.pal = pal;
    if (fadeStateSegment.trim()) {
      const id = parseInt(fadeStateSegment, 10);
      if (!Number.isFinite(id)) errors.push("state segment is not a number.");
      else seg.id = id;
    }

    const colors: number[][] = [];
    const c1 = parseHexColor(fadeStateColor1);
    const c2 = parseHexColor(fadeStateColor2);
    const c3 = parseHexColor(fadeStateColor3);
    if (fadeStateColor1.trim() && !c1) errors.push("state color 1 is invalid.");
    if (fadeStateColor2.trim() && !c2) errors.push("state color 2 is invalid.");
    if (fadeStateColor3.trim() && !c3) errors.push("state color 3 is invalid.");
    if (c1) colors.push(c1);
    if (c2) colors.push(c2);
    if (c3) colors.push(c3);
    if (colors.length) seg.col = colors;

    if (fadeStateSpeed.trim()) {
      const sx = parseInt(fadeStateSpeed, 10);
      if (!Number.isFinite(sx)) errors.push("state speed is not a number.");
      else seg.sx = sx;
    }
    if (fadeStateIntensity.trim()) {
      const ix = parseInt(fadeStateIntensity, 10);
      if (!Number.isFinite(ix)) errors.push("state intensity is not a number.");
      else seg.ix = ix;
    }
    if (fadeStateReverse) seg.rev = 1;

    if (Object.keys(seg).length) {
      if (seg.id == null) seg.id = 0;
      payload.seg = [seg];
    }
    return payload;
  };

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

  const applyCrossfadePreset = (preset: CrossfadePreset) => {
    if (preset.look) {
      setFadeUseLookBuilder(true);
      setFadeLookJson("");
      setFadeLookName(preset.look.name ?? "");
      setFadeLookTheme(preset.look.theme ?? "");
      setFadeLookEffect(preset.look.effect ?? "Solid");
      setFadeLookPalette(preset.look.palette ?? "Default");
      setFadeLookColor1(preset.look.colors?.[0] ?? "#ff0000");
      setFadeLookColor2(preset.look.colors?.[1] ?? "#00ff00");
      setFadeLookColor3(preset.look.colors?.[2] ?? "#0000ff");
      setFadeLookSpeed(preset.look.speed ?? "");
      setFadeLookIntensity(preset.look.intensity ?? "");
      setFadeLookSegment(preset.look.segment ?? "");
      setFadeLookOn(preset.look.on ?? true);
      setFadeLookReverse(preset.look.reverse ?? false);
    } else {
      setFadeUseLookBuilder(false);
      setFadeLookJson("");
    }

    if (preset.state) {
      setFadeUseStateBuilder(true);
      setFadeStateJson("");
      setFadeStateOn(preset.state.on ?? true);
      setFadeStateBrightness(preset.state.brightness ?? "");
      setFadeStateEffect(preset.state.effect ?? "");
      setFadeStatePalette(preset.state.palette ?? "");
      setFadeStateColor1(preset.state.colors?.[0] ?? "#ff0000");
      setFadeStateColor2(preset.state.colors?.[1] ?? "#00ff00");
      setFadeStateColor3(preset.state.colors?.[2] ?? "#0000ff");
      setFadeStateSpeed(preset.state.speed ?? "");
      setFadeStateIntensity(preset.state.intensity ?? "");
      setFadeStateSegment(preset.state.segment ?? "");
      setFadeStateReverse(preset.state.reverse ?? false);
    } else {
      setFadeUseStateBuilder(false);
      setFadeStateJson("");
    }

    if (preset.brightness != null) {
      setFadeBrightness(preset.brightness);
    } else {
      setFadeBrightness("");
    }
    if (preset.transition_ms != null) {
      setFadeTransitionMs(preset.transition_ms);
    }
    setFadePresetName(preset.label);
  };

  const refreshCrossfadePresets = async () => {
    try {
      const res = await api<{
        ok: boolean;
        presets: OrchestrationPreset[];
      }>("/v1/orchestration/presets?limit=200&scope=crossfade", {
        method: "GET",
      });
      setSavedCrossfadePresets(res.presets ?? []);
    } catch {
      setSavedCrossfadePresets([]);
    }
  };

  const applyCrossfadePayload = (
    payload: Record<string, unknown>,
    scopeOverride?: "local" | "fleet",
  ) => {
    const scopeVal = scopeOverride ?? fadeScope;
    const look = payload.look;
    const state = payload.state;

    const pickSeg = (seg: unknown) => {
      if (!seg) return null;
      if (Array.isArray(seg)) {
        return seg.find((s) => s && typeof s === "object") ?? null;
      }
      if (typeof seg === "object") return seg;
      return null;
    };

    const applyColors = (
      cols: unknown,
      set1: (v: string) => void,
      set2: (v: string) => void,
      set3: (v: string) => void,
    ) => {
      const colors: string[] = [];
      if (Array.isArray(cols)) {
        if (cols.length >= 3 && cols.every((c) => Number.isFinite(c))) {
          const hex = rgbToHex(cols);
          if (hex) colors.push(hex);
        } else {
          for (const item of cols) {
            const hex = rgbToHex(item);
            if (hex) colors.push(hex);
            if (colors.length >= 3) break;
          }
        }
      }
      if (colors[0]) set1(colors[0]);
      if (colors[1]) set2(colors[1]);
      if (colors[2]) set3(colors[2]);
    };

    if (look && typeof look === "object" && !Array.isArray(look)) {
      const seg = pickSeg((look as any).seg);
      const canBuild =
        Boolean(seg) ||
        "effect" in (look as any) ||
        "palette" in (look as any) ||
        "name" in (look as any);
      if (canBuild) {
        setFadeUseLookBuilder(true);
        setFadeLookJson("");
        setFadeLookName(String((look as any).name || ""));
        setFadeLookTheme(String((look as any).theme || ""));
        const effect = seg?.fx ?? (look as any).effect ?? "Solid";
        const palette = seg?.pal ?? (look as any).palette ?? "Default";
        setFadeLookEffect(String(effect || "Solid"));
        setFadeLookPalette(String(palette || "Default"));
        applyColors(
          seg?.col,
          setFadeLookColor1,
          setFadeLookColor2,
          setFadeLookColor3,
        );
        setFadeLookSpeed(seg?.sx != null ? String(seg.sx) : "");
        setFadeLookIntensity(seg?.ix != null ? String(seg.ix) : "");
        setFadeLookSegment(seg?.id != null ? String(seg.id) : "");
        setFadeLookOn(seg?.on != null ? Boolean(seg.on) : true);
        setFadeLookReverse(seg?.rev != null ? Boolean(seg.rev) : false);
      } else {
        setFadeUseLookBuilder(false);
        setFadeLookJson(JSON.stringify(look, null, 2));
      }
    } else if (look != null) {
      setFadeUseLookBuilder(false);
      setFadeLookJson(JSON.stringify(look, null, 2));
    } else {
      setFadeUseLookBuilder(false);
      setFadeLookJson("");
    }

    if (state && typeof state === "object" && !Array.isArray(state)) {
      const seg = pickSeg((state as any).seg);
      const canBuild =
        Boolean(seg) || "fx" in (state as any) || "pal" in (state as any);
      if (canBuild) {
        setFadeUseStateBuilder(true);
        setFadeStateJson("");
        if ("on" in (state as any)) {
          setFadeStateOn(Boolean((state as any).on));
        } else {
          setFadeStateOn(true);
        }
        if ((state as any).bri != null) {
          setFadeStateBrightness(String((state as any).bri));
        } else {
          setFadeStateBrightness("");
        }
        const fx = seg?.fx ?? (state as any).fx ?? "";
        const pal = seg?.pal ?? (state as any).pal ?? "";
        setFadeStateEffect(fx != null ? String(fx) : "");
        setFadeStatePalette(pal != null ? String(pal) : "");
        applyColors(
          seg?.col,
          setFadeStateColor1,
          setFadeStateColor2,
          setFadeStateColor3,
        );
        setFadeStateSpeed(seg?.sx != null ? String(seg.sx) : "");
        setFadeStateIntensity(seg?.ix != null ? String(seg.ix) : "");
        setFadeStateSegment(seg?.id != null ? String(seg.id) : "");
        setFadeStateReverse(seg?.rev != null ? Boolean(seg.rev) : false);
      } else {
        setFadeUseStateBuilder(false);
        setFadeStateJson(JSON.stringify(state, null, 2));
      }
    } else if (state != null) {
      setFadeUseStateBuilder(false);
      setFadeStateJson(JSON.stringify(state, null, 2));
    } else {
      setFadeUseStateBuilder(false);
      setFadeStateJson("");
    }

    if (payload.brightness != null) {
      setFadeBrightness(String(payload.brightness));
    } else {
      setFadeBrightness("");
    }
    if (payload.transition_ms != null) {
      setFadeTransitionMs(String(payload.transition_ms));
    } else {
      setFadeTransitionMs("");
    }
    if (payload.targets != null && Array.isArray(payload.targets)) {
      setFadeTargets(payload.targets.map(String).join(", "));
    } else if (payload.targets == null) {
      setFadeTargets("");
    }
    if (payload.include_self != null) {
      setFadeIncludeSelf(Boolean(payload.include_self));
    } else if (scopeVal === "fleet") {
      setFadeIncludeSelf(true);
    }

    if (scopeVal === "local" || scopeVal === "fleet") {
      setFadeScope(scopeVal);
    }
  };

  const applyStoredCrossfadePreset = (preset: OrchestrationPreset) => {
    const payload = (preset.payload || {}) as Record<string, unknown>;
    const scoped = payload.crossfade;
    const scopeRaw = payload.scope;
    const scope =
      typeof scopeRaw === "string" && (scopeRaw === "local" || scopeRaw === "fleet")
        ? scopeRaw
        : undefined;
    if (scoped && typeof scoped === "object" && !Array.isArray(scoped)) {
      applyCrossfadePayload(scoped as Record<string, unknown>, scope);
    } else {
      applyCrossfadePayload(payload, scope);
    }
    setFadePresetName(preset.name);
  };

  const saveCrossfadePreset = async () => {
    setBusy(true);
    setError(null);
    try {
      const name = fadePresetName.trim();
      if (!name) throw new Error("Preset name is required.");
      const { payload, errors } = buildCrossfadePayload();
      if (errors.length) {
        throw new Error(errors.join(" "));
      }
      const res = await api<{ ok: boolean; preset: OrchestrationPreset }>(
        "/v1/orchestration/presets",
        {
          method: "POST",
          json: {
            name,
            scope: "crossfade",
            description: null,
            tags: ["crossfade"],
            payload: { scope: fadeScope, crossfade: payload },
          },
        },
      );
      await refreshCrossfadePresets();
      if (res?.preset?.id != null) {
        setFadePreset(`saved:${res.preset.id}`);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const deleteCrossfadePreset = async () => {
    const current = fadePreset.startsWith("saved:")
      ? savedCrossfadePresets.find(
          (p) => String(p.id) === fadePreset.slice("saved:".length),
        )
      : null;
    if (!current) return;
    setBusy(true);
    setError(null);
    try {
      await api(`/v1/orchestration/presets/${current.id}`, {
        method: "DELETE",
      });
      setFadePreset("");
      setFadePresetName("");
      await refreshCrossfadePresets();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const buildCrossfadePayload = () => {
    const payload: Record<string, unknown> = {};
    const errors: string[] = [];
    const lookRaw = fadeLookJson.trim();
    const stateRaw = fadeStateJson.trim();

    if (fadeUseLookBuilder) {
      const spec = buildLookSpec(errors);
      payload.look = spec;
    } else if (lookRaw) {
      try {
        const parsed = JSON.parse(lookRaw);
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
          errors.push("Look JSON must be an object.");
        } else {
          payload.look = parsed;
        }
      } catch {
        errors.push("Look JSON is invalid.");
      }
    }

    if (fadeUseStateBuilder) {
      const spec = buildStateSpec(errors);
      payload.state = spec;
    } else if (stateRaw) {
      try {
        const parsed = JSON.parse(stateRaw);
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
          errors.push("State JSON must be an object.");
        } else {
          payload.state = parsed;
        }
      } catch {
        errors.push("State JSON is invalid.");
      }
    }

    if (!payload.look && !payload.state) {
      errors.push("Provide a look or state for crossfade.");
    }

    if (fadeBrightness.trim()) {
      const val = parseInt(fadeBrightness.trim(), 10);
      if (!Number.isFinite(val) || val < 1) {
        errors.push("Brightness must be a positive number.");
      } else {
        payload.brightness = val;
      }
    }

    if (fadeTransitionMs.trim()) {
      const val = parseInt(fadeTransitionMs.trim(), 10);
      if (!Number.isFinite(val) || val < 0) {
        errors.push("transition_ms must be >= 0.");
      } else {
        payload.transition_ms = val;
      }
    }

    if (fadeScope === "fleet") {
      payload.targets = parseTargets(fadeTargets);
      payload.include_self = fadeIncludeSelf;
    }

    return { payload, errors };
  };

  const previewCrossfade = () => {
    setError(null);
    const { payload, errors } = buildCrossfadePayload();
    if (errors.length) {
      setError(errors.join(" "));
      return;
    }
    setFadePreviewJson(JSON.stringify(payload, null, 2));
    setFadePreviewOpen(true);
  };

  const copyCrossfadePayload = async () => {
    if (!fadePreviewJson) return;
    if (!navigator.clipboard?.writeText) {
      setError("Clipboard API unavailable; copy manually.");
      return;
    }
    try {
      await navigator.clipboard.writeText(fadePreviewJson);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const applyCrossfade = async () => {
    setBusy(true);
    setError(null);
    try {
      const { payload, errors } = buildCrossfadePayload();
      if (errors.length) {
        throw new Error(errors.join(" "));
      }

      if (fadeScope === "fleet") {
        await api("/v1/fleet/crossfade", { method: "POST", json: payload });
      } else {
        await api("/v1/orchestration/crossfade", {
          method: "POST",
          json: payload,
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

  const startSequenceStaggered = async () => {
    setBusy(true);
    setError(null);
    try {
      const targets = parseTargets(seqTargets);
      await api("/v1/fleet/sequences/start_staggered", {
        method: "POST",
        json: {
          file: seqFile,
          loop: seqLoop,
          targets,
          include_self: true,
          stagger_s: parseFloat(seqStaggerS || "0"),
          start_delay_s: parseFloat(seqStartDelayS || "0"),
        },
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
        <Chip
          label={`LedFx: ${config?.ledfx_enabled ? "on" : "off"}`}
          size="small"
        />
        <Chip label={`Peers: ${config?.peers_configured ?? 0}`} size="small" />
      </Stack>
      {wledEffects.length ? (
        <datalist id="wled-effects">
          {wledEffects.map((e) => (
            <option key={e} value={e} />
          ))}
        </datalist>
      ) : null}
      {wledPalettes.length ? (
        <datalist id="wled-palettes">
          {wledPalettes.map((p) => (
            <option key={p} value={p} />
          ))}
        </datalist>
      ) : null}

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

      <Card>
        <CardContent>
          <Typography variant="h6">Fleet health</Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            Cached summary from <code>/v1/fleet/health</code>.
          </Typography>
          <Stack direction="row" spacing={1} sx={{ mt: 2, flexWrap: "wrap" }}>
            <Chip
              size="small"
              label={`total ${fleetHealth?.summary?.total ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`online ${fleetHealth?.summary?.online ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`wled ok ${fleetHealth?.summary?.wled_ok ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`fpp ok ${fleetHealth?.summary?.fpp_ok ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`ledfx ok ${fleetHealth?.summary?.ledfx_ok ?? 0}`}
              variant="outlined"
            />
            <Chip
              size="small"
              label={`cached ${fleetHealth?.cached ? "yes" : "no"}`}
              variant="outlined"
            />
          </Stack>
          {fleetHealth ? (
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
              snapshot=<code>{fmtTs(fleetHealth.generated_at ?? null)}</code> · ttl=
              <code>{Math.round(fleetHealth.ttl_s ?? 0)}</code>s
            </Typography>
          ) : null}
          {fleetHealthError ? (
            <Alert severity="warning" sx={{ mt: 2 }}>
              {fleetHealthError}
            </Alert>
          ) : null}
        </CardContent>
      </Card>

      {config?.ledfx_enabled ? (
        <Card>
          <CardContent>
            <Typography variant="h6">LedFx status</Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
              Current LedFx state from <code>/v1/ledfx/status</code>.
            </Typography>
            <Box
              component="pre"
              sx={{
                whiteSpace: "pre-wrap",
                wordBreak: "break-word",
                fontSize: 12,
                mt: 1,
              }}
            >
              {ledfxPretty || "Loading…"}
            </Box>
          </CardContent>
        </Card>
      ) : null}

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
                helperText='Examples: "roofline1", "role:roofline", "tag:outside", "*"'
                fullWidth
              />
            </Stack>
            {lookScope === "fleet" ? (
              <Button
                variant="outlined"
                startIcon={<VisibilityIcon />}
                onClick={() => openPreview("Quick Look targets", lookTargets)}
                disabled={busy}
              >
                Preview targets
              </Button>
            ) : null}
            <Button variant="contained" onClick={applyLook} disabled={busy}>
              Apply random look
            </Button>
          </Stack>
        </AccordionDetails>
      </Accordion>

      <Accordion>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Crossfade</Typography>
        </AccordionSummary>
        <AccordionDetails>
            <Stack spacing={2}>
              <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
                <FormControl fullWidth>
                  <InputLabel id="crossfade-preset-label">Preset</InputLabel>
                  <Select
                    id="crossfade-preset"
                    labelId="crossfade-preset-label"
                    value={fadePreset}
                    label="Preset"
                    onChange={(e) => {
                      const val = String(e.target.value || "");
                      setFadePreset(val);
                      if (val.startsWith("builtin:")) {
                        const key = val.slice("builtin:".length);
                        const preset = CROSSFADE_PRESETS.find(
                          (item) => item.id === key,
                        );
                        if (preset) applyCrossfadePreset(preset);
                        return;
                      }
                      if (val.startsWith("saved:")) {
                        const key = val.slice("saved:".length);
                        const preset = savedCrossfadePresets.find(
                          (item) => String(item.id) === key,
                        );
                        if (preset) applyStoredCrossfadePreset(preset);
                      }
                    }}
                  >
                    <MenuItem value="">
                      <em>Custom</em>
                    </MenuItem>
                    {savedCrossfadePresets.length ? (
                      <ListSubheader>Saved presets</ListSubheader>
                    ) : null}
                    {savedCrossfadePresets.map((preset) => (
                      <MenuItem
                        key={`saved:${preset.id}`}
                        value={`saved:${preset.id}`}
                      >
                        {preset.name}
                      </MenuItem>
                    ))}
                    <ListSubheader>Built-in presets</ListSubheader>
                    {CROSSFADE_PRESETS.map((preset) => (
                      <MenuItem
                        key={`builtin:${preset.id}`}
                        value={`builtin:${preset.id}`}
                      >
                        {preset.label}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Stack>
              <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
                <TextField
                  label="Preset name"
                  value={fadePresetName}
                  onChange={(e) => setFadePresetName(e.target.value)}
                  fullWidth
                />
                <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
                  <Button
                    variant="outlined"
                    onClick={saveCrossfadePreset}
                    disabled={busy || !fadePresetName.trim()}
                  >
                    Save preset
                  </Button>
                  <Button
                    variant="outlined"
                    onClick={deleteCrossfadePreset}
                    disabled={busy || !fadePreset.startsWith("saved:")}
                  >
                    Delete preset
                  </Button>
                  <Button
                    variant="outlined"
                    onClick={refreshCrossfadePresets}
                    disabled={busy}
                  >
                    Refresh presets
                  </Button>
                </Stack>
              </Stack>
              <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
                <FormControlLabel
                  control={
                  <Switch
                    checked={fadeUseLookBuilder}
                    onChange={(e) => setFadeUseLookBuilder(e.target.checked)}
                  />
                }
                label="Use look builder"
              />
              <FormControlLabel
                control={
                  <Switch
                    checked={fadeUseStateBuilder}
                    onChange={(e) => setFadeUseStateBuilder(e.target.checked)}
                  />
                }
                label="Use state builder"
              />
            </Stack>
            <Typography variant="caption" color="text.secondary">
              If both look and state are provided, the look is applied.
            </Typography>
            {fadeUseLookBuilder ? (
              <Stack spacing={2}>
                <Typography variant="subtitle2">Look builder</Typography>
                <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
                  <TextField
                    label="Name (optional)"
                    value={fadeLookName}
                    onChange={(e) => setFadeLookName(e.target.value)}
                    fullWidth
                  />
                  <TextField
                    label="Theme (optional)"
                    value={fadeLookTheme}
                    onChange={(e) => setFadeLookTheme(e.target.value)}
                    fullWidth
                  />
                </Stack>
                <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
                  <TextField
                    label="Effect"
                    value={fadeLookEffect}
                    onChange={(e) => setFadeLookEffect(e.target.value)}
                    inputProps={{ list: "wled-effects" }}
                    helperText="Effect name (defaults to Solid)"
                    fullWidth
                  />
                  <TextField
                    label="Palette"
                    value={fadeLookPalette}
                    onChange={(e) => setFadeLookPalette(e.target.value)}
                    inputProps={{ list: "wled-palettes" }}
                    helperText="Palette name (defaults to Default)"
                    fullWidth
                  />
                </Stack>
                <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
                  <TextField
                    label="Color 1"
                    type="color"
                    value={fadeLookColor1}
                    onChange={(e) => setFadeLookColor1(e.target.value)}
                    fullWidth
                  />
                  <TextField
                    label="Color 2"
                    type="color"
                    value={fadeLookColor2}
                    onChange={(e) => setFadeLookColor2(e.target.value)}
                    fullWidth
                  />
                  <TextField
                    label="Color 3"
                    type="color"
                    value={fadeLookColor3}
                    onChange={(e) => setFadeLookColor3(e.target.value)}
                    fullWidth
                  />
                </Stack>
                <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
                  <TextField
                    label="Speed (sx, optional)"
                    value={fadeLookSpeed}
                    onChange={(e) => setFadeLookSpeed(e.target.value)}
                    inputMode="numeric"
                    fullWidth
                  />
                  <TextField
                    label="Intensity (ix, optional)"
                    value={fadeLookIntensity}
                    onChange={(e) => setFadeLookIntensity(e.target.value)}
                    inputMode="numeric"
                    fullWidth
                  />
                  <TextField
                    label="Segment id (optional)"
                    value={fadeLookSegment}
                    onChange={(e) => setFadeLookSegment(e.target.value)}
                    inputMode="numeric"
                    fullWidth
                  />
                </Stack>
                <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={fadeLookReverse}
                        onChange={(e) => setFadeLookReverse(e.target.checked)}
                      />
                    }
                    label="Reverse"
                  />
                  <FormControlLabel
                    control={
                      <Switch
                        checked={fadeLookOn}
                        onChange={(e) => setFadeLookOn(e.target.checked)}
                      />
                    }
                    label="On"
                  />
                </Stack>
              </Stack>
            ) : (
              <TextField
                label="Look JSON (optional)"
                value={fadeLookJson}
                onChange={(e) => setFadeLookJson(e.target.value)}
                placeholder='{"type":"wled_look","seg":{"fx":"Solid","pal":"Default"}}'
                multiline
                minRows={3}
              />
            )}
            {fadeUseStateBuilder ? (
              <Stack spacing={2}>
                <Typography variant="subtitle2">State builder</Typography>
                <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={fadeStateOn}
                        onChange={(e) => setFadeStateOn(e.target.checked)}
                      />
                    }
                    label="On"
                  />
                  <TextField
                    label="Brightness (bri, optional)"
                    value={fadeStateBrightness}
                    onChange={(e) => setFadeStateBrightness(e.target.value)}
                    inputMode="numeric"
                    fullWidth
                  />
                </Stack>
                <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
                  <TextField
                    label="Effect (name or id)"
                    value={fadeStateEffect}
                    onChange={(e) => setFadeStateEffect(e.target.value)}
                    inputProps={{ list: "wled-effects" }}
                    helperText="Use name or numeric id"
                    fullWidth
                  />
                  <TextField
                    label="Palette (name or id)"
                    value={fadeStatePalette}
                    onChange={(e) => setFadeStatePalette(e.target.value)}
                    inputProps={{ list: "wled-palettes" }}
                    helperText="Use name or numeric id"
                    fullWidth
                  />
                </Stack>
                <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
                  <TextField
                    label="Color 1"
                    type="color"
                    value={fadeStateColor1}
                    onChange={(e) => setFadeStateColor1(e.target.value)}
                    fullWidth
                  />
                  <TextField
                    label="Color 2"
                    type="color"
                    value={fadeStateColor2}
                    onChange={(e) => setFadeStateColor2(e.target.value)}
                    fullWidth
                  />
                  <TextField
                    label="Color 3"
                    type="color"
                    value={fadeStateColor3}
                    onChange={(e) => setFadeStateColor3(e.target.value)}
                    fullWidth
                  />
                </Stack>
                <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
                  <TextField
                    label="Speed (sx, optional)"
                    value={fadeStateSpeed}
                    onChange={(e) => setFadeStateSpeed(e.target.value)}
                    inputMode="numeric"
                    fullWidth
                  />
                  <TextField
                    label="Intensity (ix, optional)"
                    value={fadeStateIntensity}
                    onChange={(e) => setFadeStateIntensity(e.target.value)}
                    inputMode="numeric"
                    fullWidth
                  />
                  <TextField
                    label="Segment id (optional)"
                    value={fadeStateSegment}
                    onChange={(e) => setFadeStateSegment(e.target.value)}
                    inputMode="numeric"
                    fullWidth
                  />
                </Stack>
                <FormControlLabel
                  control={
                    <Switch
                      checked={fadeStateReverse}
                      onChange={(e) => setFadeStateReverse(e.target.checked)}
                    />
                  }
                  label="Reverse"
                />
              </Stack>
            ) : (
              <TextField
                label="State JSON (optional)"
                value={fadeStateJson}
                onChange={(e) => setFadeStateJson(e.target.value)}
                placeholder='{"on": true, "bri": 128, "fx": 0, "pal": 0}'
                multiline
                minRows={3}
              />
            )}
            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <TextField
                label="Brightness (optional)"
                inputMode="numeric"
                value={fadeBrightness}
                onChange={(e) => setFadeBrightness(e.target.value)}
                fullWidth
              />
              <TextField
                label="transition_ms (optional)"
                inputMode="numeric"
                value={fadeTransitionMs}
                onChange={(e) => setFadeTransitionMs(e.target.value)}
                fullWidth
              />
            </Stack>
            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <FormControl fullWidth>
                <InputLabel>Scope</InputLabel>
                <Select
                  value={fadeScope}
                  label="Scope"
                  onChange={(e) => setFadeScope(e.target.value as any)}
                >
                  <MenuItem value="local">local</MenuItem>
                  <MenuItem value="fleet">fleet</MenuItem>
                </Select>
              </FormControl>
              <TextField
                label="Targets (comma-separated, optional)"
                value={fadeTargets}
                onChange={(e) => setFadeTargets(e.target.value)}
                helperText='Examples: "roofline1", "role:roofline", "tag:outside", "*"'
                fullWidth
              />
            </Stack>
            {fadeScope === "fleet" ? (
              <Stack
                direction={{ xs: "column", sm: "row" }}
                spacing={2}
                alignItems={{ xs: "stretch", sm: "center" }}
              >
                <FormControlLabel
                  control={
                    <Switch
                      checked={fadeIncludeSelf}
                      onChange={(e) => setFadeIncludeSelf(e.target.checked)}
                    />
                  }
                  label="Include self"
                />
                <Button
                  variant="outlined"
                  startIcon={<VisibilityIcon />}
                  onClick={() => openPreview("Crossfade targets", fadeTargets)}
                  disabled={busy}
                >
                  Preview targets
                </Button>
              </Stack>
            ) : null}
            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <Button variant="outlined" onClick={previewCrossfade} disabled={busy}>
                Preview payload
              </Button>
              <Button variant="contained" onClick={applyCrossfade} disabled={busy}>
                Crossfade
              </Button>
            </Stack>
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
                helperText='Examples: "roofline1", "role:roofline", "tag:outside", "*"'
                fullWidth
              />
            </Stack>
            {patScope === "fleet" ? (
              <Button
                variant="outlined"
                startIcon={<VisibilityIcon />}
                onClick={() => openPreview("Pattern targets", patTargets)}
                disabled={busy}
              >
                Preview targets
              </Button>
            ) : null}
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
                helperText='Examples: "roofline1", "role:roofline", "tag:outside", "*"'
                fullWidth
              />
            </Stack>
            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <TextField
                label="Stagger delay (s)"
                value={seqStaggerS}
                onChange={(e) => setSeqStaggerS(e.target.value)}
                inputMode="decimal"
                fullWidth
              />
              <TextField
                label="Start delay (s)"
                value={seqStartDelayS}
                onChange={(e) => setSeqStartDelayS(e.target.value)}
                inputMode="decimal"
                fullWidth
              />
            </Stack>
            <Button
              variant="outlined"
              startIcon={<VisibilityIcon />}
              onClick={() => openPreview("Fleet sequence targets", seqTargets)}
              disabled={busy}
            >
              Preview targets
            </Button>
            <Stack direction="row" spacing={2}>
              <Button
                variant="contained"
                onClick={startSequence}
                disabled={busy}
              >
                Start
              </Button>
              <Button
                variant="outlined"
                onClick={startSequenceStaggered}
                disabled={busy}
              >
                Start staggered
              </Button>
              <Button onClick={stopSequence} disabled={busy}>
                Stop
              </Button>
            </Stack>
          </Stack>
        </AccordionDetails>
      </Accordion>

      <TargetPreviewDialog
        open={previewOpen}
        title={previewTitle}
        targets={previewTargets}
        onClose={() => setPreviewOpen(false)}
      />

      <Dialog
        open={fadePreviewOpen}
        onClose={() => setFadePreviewOpen(false)}
        fullWidth
        maxWidth="md"
      >
        <DialogTitle>Crossfade payload</DialogTitle>
        <DialogContent>
          <TextField
            value={fadePreviewJson}
            fullWidth
            multiline
            minRows={8}
            inputProps={{ readOnly: true }}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={copyCrossfadePayload} disabled={!fadePreviewJson}>
            Copy
          </Button>
          <Button onClick={() => setFadePreviewOpen(false)}>Close</Button>
        </DialogActions>
      </Dialog>
    </Stack>
  );
}
