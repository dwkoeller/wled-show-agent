import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import RefreshIcon from "@mui/icons-material/Refresh";
import StopIcon from "@mui/icons-material/Stop";
import AddIcon from "@mui/icons-material/Add";
import DeleteIcon from "@mui/icons-material/Delete";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import SaveIcon from "@mui/icons-material/Save";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import {
  Alert,
  Box,
  Button,
  Card,
  CardActions,
  CardContent,
  Divider,
  FormControl,
  FormControlLabel,
  IconButton,
  InputLabel,
  MenuItem,
  Select,
  Stack,
  Switch,
  TextField,
  Typography,
} from "@mui/material";
import React, { useEffect, useMemo, useState } from "react";
import { api } from "../../api";
import { useEventRefresh } from "../../hooks/useEventRefresh";

const stepKinds = [
  "look",
  "state",
  "crossfade",
  "sequence",
  "preset",
  "ddp",
  "blackout",
  "pause",
  "ledfx_scene",
  "ledfx_effect",
  "ledfx_brightness",
] as const;

type StepKind = (typeof stepKinds)[number];

type BuilderStep = {
  id: string;
  kind: StepKind;
  duration_s: string;
  transition_ms: string;
  stagger_s: string;
  start_delay_s: string;
  brightness: string;
  loop: boolean;
  sequence_file: string;
  preset_id: string;
  pattern: string;
  fps: string;
  look_json: string;
  state_json: string;
  params_json: string;
  use_look_builder: boolean;
  look_name: string;
  look_theme: string;
  look_effect: string;
  look_palette: string;
  look_color1: string;
  look_color2: string;
  look_color3: string;
  look_speed: string;
  look_intensity: string;
  look_reverse: boolean;
  look_segment: string;
  look_on: boolean;
  use_state_builder: boolean;
  state_on: boolean;
  state_brightness: string;
  state_effect: string;
  state_palette: string;
  state_color1: string;
  state_color2: string;
  state_color3: string;
  state_speed: string;
  state_intensity: string;
  state_reverse: boolean;
  state_segment: string;
  ledfx_scene_id: string;
  ledfx_scene_action: string;
  ledfx_virtual_id: string;
  ledfx_effect: string;
  ledfx_config_json: string;
  ledfx_brightness: string;
};

type OrchestrationPreset = {
  id: number;
  name: string;
  scope: string;
  description?: string | null;
  tags?: string[];
  version?: number;
  payload: any;
  created_at?: number;
  updated_at?: number;
};

type PresetsRes = {
  ok: boolean;
  presets: OrchestrationPreset[];
  count?: number;
  limit?: number;
  offset?: number;
  next_offset?: number | null;
};

type SequenceListRes = { ok: boolean; files: string[] };
type DdpPatternsRes = { ok: boolean; patterns: string[] };
type WledPresetsRes = { ok: boolean; presets: Record<string, any> };
type WledEffectsRes = { ok: boolean; effects: string[] };
type WledPalettesRes = { ok: boolean; palettes: string[] };
type LastAppliedRes = { ok: boolean; last_applied?: Record<string, any> };

type Json = unknown;

function parseTargets(raw: string): string[] | null {
  const out = raw
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
  return out.length ? out : null;
}

function parseTags(raw: string): string[] {
  return raw
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
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

const defaultPayload = JSON.stringify(
  {
    name: "Evening",
    loop: false,
    steps: [
      {
        kind: "sequence",
        sequence_file: "sequence_ShowMix_*.json",
        duration_s: 30,
      },
      { kind: "blackout", transition_ms: 1000, duration_s: 2 },
    ],
  },
  null,
  2,
);

function makeId(): string {
  return Math.random().toString(36).slice(2, 10);
}

function newStep(kind: StepKind): BuilderStep {
  return {
    id: makeId(),
    kind,
    duration_s: "",
    transition_ms: "",
    stagger_s: "",
    start_delay_s: "",
    brightness: "",
    loop: false,
    sequence_file: "",
    preset_id: "",
    pattern: "",
    fps: "",
    look_json: "",
    state_json: "",
    params_json: "",
    use_look_builder: false,
    look_name: "",
    look_theme: "",
    look_effect: "",
    look_palette: "",
    look_color1: "#ff0000",
    look_color2: "#00ff00",
    look_color3: "#0000ff",
    look_speed: "",
    look_intensity: "",
    look_reverse: false,
    look_segment: "",
    look_on: true,
    use_state_builder: false,
    state_on: true,
    state_brightness: "",
    state_effect: "",
    state_palette: "",
    state_color1: "#ff0000",
    state_color2: "#00ff00",
    state_color3: "#0000ff",
    state_speed: "",
    state_intensity: "",
    state_reverse: false,
    state_segment: "",
    ledfx_scene_id: "",
    ledfx_scene_action: "activate",
    ledfx_virtual_id: "",
    ledfx_effect: "",
    ledfx_config_json: "",
    ledfx_brightness: "",
  };
}

function payloadToBuilder(payload: any): {
  name: string;
  loop: boolean;
  steps: BuilderStep[];
  targets: string;
  includeSelf: boolean;
} | null {
  if (!payload || typeof payload !== "object") return null;
  const steps = Array.isArray(payload.steps) ? payload.steps : null;
  if (!steps) return null;

  const outSteps: BuilderStep[] = steps.map((step: any) => {
    const kindRaw = String(step?.kind || "look").toLowerCase();
    const kind = (stepKinds.includes(kindRaw as StepKind)
      ? kindRaw
      : "look") as StepKind;
    const out = newStep(kind);
    out.duration_s = step?.duration_s != null ? String(step.duration_s) : "";
    out.transition_ms = step?.transition_ms != null ? String(step.transition_ms) : "";
    out.stagger_s = step?.stagger_s != null ? String(step.stagger_s) : "";
    out.start_delay_s =
      step?.start_delay_s != null ? String(step.start_delay_s) : "";
    out.brightness = step?.brightness != null ? String(step.brightness) : "";
    out.loop = Boolean(step?.loop);
    out.sequence_file = step?.sequence_file ? String(step.sequence_file) : "";
    out.preset_id = step?.preset_id != null ? String(step.preset_id) : "";
    out.pattern = step?.pattern ? String(step.pattern) : "";
    out.fps = step?.fps != null ? String(step.fps) : "";
    if (step?.look && typeof step.look === "object") {
      out.look_json = JSON.stringify(step.look, null, 2);
    }
    if (step?.state && typeof step.state === "object") {
      out.state_json = JSON.stringify(step.state, null, 2);
    }
    if (step?.params && typeof step.params === "object") {
      out.params_json = JSON.stringify(step.params, null, 2);
    }
    out.ledfx_scene_id =
      step?.ledfx_scene_id != null ? String(step.ledfx_scene_id) : "";
    out.ledfx_scene_action =
      step?.ledfx_scene_action != null
        ? String(step.ledfx_scene_action)
        : "activate";
    out.ledfx_virtual_id =
      step?.ledfx_virtual_id != null ? String(step.ledfx_virtual_id) : "";
    out.ledfx_effect =
      step?.ledfx_effect != null ? String(step.ledfx_effect) : "";
    if (step?.ledfx_config && typeof step.ledfx_config === "object") {
      out.ledfx_config_json = JSON.stringify(step.ledfx_config, null, 2);
    }
    out.ledfx_brightness =
      step?.ledfx_brightness != null ? String(step.ledfx_brightness) : "";
    return out;
  });

  const targets = Array.isArray(payload.targets)
    ? payload.targets.join(", ")
    : "";
  const includeSelf = payload.include_self != null ? Boolean(payload.include_self) : true;

  return {
    name: payload.name ? String(payload.name) : "",
    loop: Boolean(payload.loop),
    steps: outSteps,
    targets,
    includeSelf,
  };
}

export function OrchestrationTools() {
  const [mode, setMode] = useState<"local" | "fleet">("local");
  const [editorMode, setEditorMode] = useState<"builder" | "json">("builder");
  const [payloadText, setPayloadText] = useState(defaultPayload);
  const [targets, setTargets] = useState("");
  const [includeSelf, setIncludeSelf] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [statusLocal, setStatusLocal] = useState<Json | null>(null);
  const [statusFleet, setStatusFleet] = useState<Json | null>(null);
  const [sequenceFiles, setSequenceFiles] = useState<string[]>([]);
  const [ddpPatterns, setDdpPatterns] = useState<string[]>([]);
  const [wledPresets, setWledPresets] = useState<{ id: number; name: string }[]>(
    [],
  );
  const [wledEffects, setWledEffects] = useState<string[]>([]);
  const [wledPalettes, setWledPalettes] = useState<string[]>([]);
  const [lastApplied, setLastApplied] = useState<Record<string, any> | null>(null);

  const [builderName, setBuilderName] = useState("Evening");
  const [builderLoop, setBuilderLoop] = useState(false);
  const [builderSteps, setBuilderSteps] = useState<BuilderStep[]>([
    {
      id: makeId(),
      kind: "sequence",
      duration_s: "30",
      transition_ms: "",
      stagger_s: "",
      start_delay_s: "",
      brightness: "",
      loop: false,
      sequence_file: "sequence_ShowMix_*.json",
      preset_id: "",
      pattern: "",
      fps: "",
      look_json: "",
      state_json: "",
      params_json: "",
      use_look_builder: false,
      look_name: "",
      look_theme: "",
      look_effect: "",
      look_palette: "",
      look_color1: "#ff0000",
      look_color2: "#00ff00",
      look_color3: "#0000ff",
      look_speed: "",
      look_intensity: "",
      look_reverse: false,
      look_segment: "",
      look_on: true,
      use_state_builder: false,
      state_on: true,
      state_brightness: "",
      state_effect: "",
      state_palette: "",
      state_color1: "#ff0000",
      state_color2: "#00ff00",
      state_color3: "#0000ff",
      state_speed: "",
      state_intensity: "",
      state_reverse: false,
      state_segment: "",
      ledfx_scene_id: "",
      ledfx_scene_action: "activate",
      ledfx_virtual_id: "",
      ledfx_effect: "",
      ledfx_config_json: "",
      ledfx_brightness: "",
    },
    {
      id: makeId(),
      kind: "blackout",
      duration_s: "2",
      transition_ms: "1000",
      stagger_s: "",
      start_delay_s: "",
      brightness: "",
      loop: false,
      sequence_file: "",
      preset_id: "",
      pattern: "",
      fps: "",
      look_json: "",
      state_json: "",
      params_json: "",
      use_look_builder: false,
      look_name: "",
      look_theme: "",
      look_effect: "",
      look_palette: "",
      look_color1: "#ff0000",
      look_color2: "#00ff00",
      look_color3: "#0000ff",
      look_speed: "",
      look_intensity: "",
      look_reverse: false,
      look_segment: "",
      look_on: true,
      use_state_builder: false,
      state_on: true,
      state_brightness: "",
      state_effect: "",
      state_palette: "",
      state_color1: "#ff0000",
      state_color2: "#00ff00",
      state_color3: "#0000ff",
      state_speed: "",
      state_intensity: "",
      state_reverse: false,
      state_segment: "",
      ledfx_scene_id: "",
      ledfx_scene_action: "activate",
      ledfx_virtual_id: "",
      ledfx_effect: "",
      ledfx_config_json: "",
      ledfx_brightness: "",
    },
  ]);
  const [newStepKind, setNewStepKind] = useState<StepKind>("sequence");

  const [presets, setPresets] = useState<OrchestrationPreset[]>([]);
  const [selectedPresetId, setSelectedPresetId] = useState("");
  const [presetName, setPresetName] = useState("");
  const [presetDescription, setPresetDescription] = useState("");
  const [presetTags, setPresetTags] = useState("");
  const [presetVersion, setPresetVersion] = useState("");
  const [presetScope, setPresetScope] = useState<
    "local" | "fleet" | "crossfade" | "all"
  >("local");
  const [presetExport, setPresetExport] = useState("");
  const [presetImportText, setPresetImportText] = useState("");

  const prettyLocal = useMemo(
    () => (statusLocal ? JSON.stringify(statusLocal, null, 2) : "-"),
    [statusLocal],
  );
  const prettyFleet = useMemo(
    () => (statusFleet ? JSON.stringify(statusFleet, null, 2) : "-"),
    [statusFleet],
  );

  const refreshPresets = async () => {
    try {
      const scopeParam =
        presetScope === "all"
          ? ""
          : `&scope=${encodeURIComponent(presetScope)}`;
      const res = await api<PresetsRes>(
        `/v1/orchestration/presets?limit=200${scopeParam}`,
        {
          method: "GET",
        },
      );
      setPresets(res.presets ?? []);
    } catch {
      setPresets([]);
    }
  };

  const refresh = async () => {
    setBusy(true);
    setError(null);
    try {
      const [
        local,
        fleet,
        seqRes,
        ddpRes,
        wledRes,
        effectsRes,
        palettesRes,
        lastRes,
      ] = await Promise.all([
        api("/v1/orchestration/status", { method: "GET" }).catch((e) => ({
          ok: false,
          error: e instanceof Error ? e.message : String(e),
        })),
        api("/v1/fleet/orchestration/status", { method: "GET" }).catch((e) => ({
          ok: false,
          error: e instanceof Error ? e.message : String(e),
        })),
        api<SequenceListRes>("/v1/sequences/list", { method: "GET" }).catch(
          () => null,
        ),
        api<DdpPatternsRes>("/v1/ddp/patterns", { method: "GET" }).catch(() => null),
        api<WledPresetsRes>("/v1/wled/presets", { method: "GET" }).catch(() => null),
        api<WledEffectsRes>("/v1/wled/effects", { method: "GET" }).catch(() => null),
        api<WledPalettesRes>("/v1/wled/palettes", { method: "GET" }).catch(() => null),
        api<LastAppliedRes>("/v1/meta/last_applied", { method: "GET" }).catch(
          () => null,
        ),
      ]);
      setStatusLocal(local as Json);
      setStatusFleet(fleet as Json);
      await refreshPresets();
      if (seqRes?.files) setSequenceFiles((seqRes.files || []).slice().sort());
      if (ddpRes?.patterns)
        setDdpPatterns((ddpRes.patterns || []).slice().sort());
      if (wledRes?.presets) {
        const items = Object.entries(wledRes.presets || {})
          .map(([id, row]) => ({
            id: Number(id),
            name: String((row as any)?.n || id),
          }))
          .filter((item) => Number.isFinite(item.id))
          .sort((a, b) => a.id - b.id);
        setWledPresets(items);
      }
      if (effectsRes?.effects) {
        setWledEffects((effectsRes.effects || []).slice());
      }
      if (palettesRes?.palettes) {
        setWledPalettes((palettesRes.palettes || []).slice());
      }
      if (lastRes?.last_applied) setLastApplied(lastRes.last_applied || null);
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
    types: ["orchestration", "tick"],
    refresh,
    minIntervalMs: 3000,
  });

  useEffect(() => {
    void refreshPresets();
  }, [presetScope]);

  const resolveIndex = (
    raw: string,
    label: string,
    idx: number,
    list: string[],
    errors: string[],
  ) => {
    const trimmed = raw.trim();
    if (!trimmed) return undefined;
    const pos = list.indexOf(trimmed);
    if (pos >= 0) return pos;
    const num = parseInt(trimmed, 10);
    if (Number.isFinite(num)) return num;
    errors.push(`Step ${idx + 1}: ${label} must be a name or numeric id.`);
    return undefined;
  };

  const buildLookSpec = (
    step: BuilderStep,
    idx: number,
    errors: string[],
  ): Record<string, any> | null => {
    const effect = step.look_effect.trim() || "Solid";
    const palette = step.look_palette.trim() || "Default";
    if (wledEffects.length && step.look_effect.trim()) {
      if (!wledEffects.includes(effect)) {
        errors.push(`Step ${idx + 1}: effect '${effect}' not found.`);
      }
    }
    if (wledPalettes.length && step.look_palette.trim()) {
      if (!wledPalettes.includes(palette)) {
        errors.push(`Step ${idx + 1}: palette '${palette}' not found.`);
      }
    }

    const colors: number[][] = [];
    const c1 = parseHexColor(step.look_color1);
    const c2 = parseHexColor(step.look_color2);
    const c3 = parseHexColor(step.look_color3);
    if (step.look_color1.trim() && !c1) {
      errors.push(`Step ${idx + 1}: look color 1 is invalid.`);
    }
    if (step.look_color2.trim() && !c2) {
      errors.push(`Step ${idx + 1}: look color 2 is invalid.`);
    }
    if (step.look_color3.trim() && !c3) {
      errors.push(`Step ${idx + 1}: look color 3 is invalid.`);
    }
    if (c1) colors.push(c1);
    if (c2) colors.push(c2);
    if (c3) colors.push(c3);

    const seg: Record<string, any> = {
      id: 0,
      fx: effect,
      pal: palette,
      on: Boolean(step.look_on),
    };
    if (step.look_segment.trim()) {
      const id = parseInt(step.look_segment, 10);
      if (!Number.isFinite(id)) {
        errors.push(`Step ${idx + 1}: look segment is not a number.`);
      } else {
        seg.id = id;
      }
    }
    if (colors.length) seg.col = colors;
    if (step.look_speed.trim()) {
      const sx = parseInt(step.look_speed, 10);
      if (!Number.isFinite(sx)) {
        errors.push(`Step ${idx + 1}: look speed is not a number.`);
      } else {
        seg.sx = sx;
      }
    }
    if (step.look_intensity.trim()) {
      const ix = parseInt(step.look_intensity, 10);
      if (!Number.isFinite(ix)) {
        errors.push(`Step ${idx + 1}: look intensity is not a number.`);
      } else {
        seg.ix = ix;
      }
    }
    if (step.look_reverse) {
      seg.rev = 1;
    }

    return {
      type: "wled_look",
      name: step.look_name.trim() || undefined,
      theme: step.look_theme.trim() || undefined,
      seg,
    };
  };

  const buildStateSpec = (
    step: BuilderStep,
    idx: number,
    errors: string[],
  ): Record<string, any> | null => {
    const payload: Record<string, any> = {};
    payload.on = Boolean(step.state_on);
    if (step.state_brightness.trim()) {
      const bri = parseInt(step.state_brightness, 10);
      if (!Number.isFinite(bri)) {
        errors.push(`Step ${idx + 1}: state brightness is not a number.`);
      } else {
        payload.bri = bri;
      }
    }

    const seg: Record<string, any> = {};
    const fx = resolveIndex(
      step.state_effect,
      "state effect",
      idx,
      wledEffects,
      errors,
    );
    const pal = resolveIndex(
      step.state_palette,
      "state palette",
      idx,
      wledPalettes,
      errors,
    );
    if (fx != null) seg.fx = fx;
    if (pal != null) seg.pal = pal;
    if (step.state_segment.trim()) {
      const id = parseInt(step.state_segment, 10);
      if (!Number.isFinite(id)) {
        errors.push(`Step ${idx + 1}: state segment is not a number.`);
      } else {
        seg.id = id;
      }
    }

    const colors: number[][] = [];
    const c1 = parseHexColor(step.state_color1);
    const c2 = parseHexColor(step.state_color2);
    const c3 = parseHexColor(step.state_color3);
    if (step.state_color1.trim() && !c1) {
      errors.push(`Step ${idx + 1}: state color 1 is invalid.`);
    }
    if (step.state_color2.trim() && !c2) {
      errors.push(`Step ${idx + 1}: state color 2 is invalid.`);
    }
    if (step.state_color3.trim() && !c3) {
      errors.push(`Step ${idx + 1}: state color 3 is invalid.`);
    }
    if (c1) colors.push(c1);
    if (c2) colors.push(c2);
    if (c3) colors.push(c3);
    if (colors.length) seg.col = colors;

    if (step.state_speed.trim()) {
      const sx = parseInt(step.state_speed, 10);
      if (!Number.isFinite(sx)) {
        errors.push(`Step ${idx + 1}: state speed is not a number.`);
      } else {
        seg.sx = sx;
      }
    }
    if (step.state_intensity.trim()) {
      const ix = parseInt(step.state_intensity, 10);
      if (!Number.isFinite(ix)) {
        errors.push(`Step ${idx + 1}: state intensity is not a number.`);
      } else {
        seg.ix = ix;
      }
    }
    if (step.state_reverse) {
      seg.rev = 1;
    }

    if (Object.keys(seg).length) {
      if (seg.id == null) seg.id = 0;
      payload.seg = [seg];
    }

    return payload;
  };

  const getLookPreview = (step: BuilderStep, idx: number) => {
    const errs: string[] = [];
    const spec = buildLookSpec(step, idx, errs);
    return {
      errors: errs,
      text: spec ? JSON.stringify(spec, null, 2) : "",
    };
  };

  const getStatePreview = (step: BuilderStep, idx: number) => {
    const errs: string[] = [];
    const spec = buildStateSpec(step, idx, errs);
    return {
      errors: errs,
      text: spec ? JSON.stringify(spec, null, 2) : "",
    };
  };

  const builderResult = useMemo(() => {
    const errors: string[] = [];
    const steps: any[] = [];

    const parseNum = (
      raw: string,
      label: string,
      idx: number,
      opts?: { integer?: boolean; min?: number },
    ) => {
      if (!raw.trim()) return undefined;
      const num = opts?.integer ? parseInt(raw, 10) : parseFloat(raw);
      if (!Number.isFinite(num)) {
        errors.push(`Step ${idx + 1}: ${label} is not a number.`);
        return undefined;
      }
      if (opts?.min != null && num < opts.min) {
        errors.push(`Step ${idx + 1}: ${label} must be >= ${opts.min}.`);
        return undefined;
      }
      return num;
    };

    const parseJsonObject = (
      raw: string,
      label: string,
      idx: number,
      required: boolean,
    ) => {
      const trimmed = raw.trim();
      if (!trimmed) {
        if (required) errors.push(`Step ${idx + 1}: ${label} is required.`);
        return undefined;
      }
      try {
        const obj = JSON.parse(trimmed);
        if (!obj || typeof obj !== "object" || Array.isArray(obj)) {
          errors.push(`Step ${idx + 1}: ${label} must be a JSON object.`);
          return undefined;
        }
        return obj;
      } catch (e) {
        errors.push(`Step ${idx + 1}: ${label} JSON is invalid.`);
        return undefined;
      }
    };

    builderSteps.forEach((step, idx) => {
      const payload: any = { kind: step.kind };
      const duration = parseNum(step.duration_s, "duration_s", idx, { min: 0 });
      const transition = parseNum(step.transition_ms, "transition_ms", idx, {
        integer: true,
        min: 0,
      });
      const stagger = parseNum(step.stagger_s, "stagger_s", idx, { min: 0 });
      const startDelay = parseNum(step.start_delay_s, "start_delay_s", idx, {
        min: 0,
      });
      const brightness = parseNum(step.brightness, "brightness", idx, {
        integer: true,
        min: 1,
      });
      const fps = parseNum(step.fps, "fps", idx, { min: 1 });

      if (duration != null) payload.duration_s = duration;
      if (transition != null) payload.transition_ms = transition;
      if (stagger != null) payload.stagger_s = stagger;
      if (startDelay != null) payload.start_delay_s = startDelay;
      if (brightness != null) payload.brightness = brightness;
      if (fps != null) payload.fps = fps;

      if (step.kind === "look") {
        const look = step.use_look_builder
          ? buildLookSpec(step, idx, errors)
          : parseJsonObject(step.look_json, "look", idx, true);
        if (look) payload.look = look;
      }
      if (step.kind === "state") {
        const state = step.use_state_builder
          ? buildStateSpec(step, idx, errors)
          : parseJsonObject(step.state_json, "state", idx, true);
        if (state) payload.state = state;
      }
      if (step.kind === "crossfade") {
        const look = step.use_look_builder
          ? buildLookSpec(step, idx, errors)
          : parseJsonObject(step.look_json, "look", idx, false);
        const state = step.use_state_builder
          ? buildStateSpec(step, idx, errors)
          : parseJsonObject(step.state_json, "state", idx, false);
        if (!look && !state) {
          errors.push(`Step ${idx + 1}: look or state is required for crossfade.`);
        }
        if (look) payload.look = look;
        if (state) payload.state = state;
      }
      if (step.kind === "sequence") {
        if (!step.sequence_file.trim()) {
          errors.push(`Step ${idx + 1}: sequence_file is required.`);
        } else {
          payload.sequence_file = step.sequence_file.trim();
        }
        payload.loop = Boolean(step.loop);
        if (payload.loop && payload.duration_s == null) {
          errors.push(`Step ${idx + 1}: duration_s is required when loop=true.`);
        }
      }
      if (step.kind === "preset") {
        const presetId = parseNum(step.preset_id, "preset_id", idx, {
          integer: true,
          min: 1,
        });
        if (presetId == null) {
          errors.push(`Step ${idx + 1}: preset_id is required.`);
        } else {
          payload.preset_id = presetId;
        }
      }
      if (step.kind === "ddp") {
        if (!step.pattern.trim()) {
          errors.push(`Step ${idx + 1}: pattern is required.`);
        } else {
          payload.pattern = step.pattern.trim();
        }
        const params = parseJsonObject(step.params_json, "params", idx, false);
        if (params) payload.params = params;
      }
      if (step.kind === "ledfx_scene") {
        if (!step.ledfx_scene_id.trim()) {
          errors.push(`Step ${idx + 1}: ledfx_scene_id is required.`);
        } else {
          payload.ledfx_scene_id = step.ledfx_scene_id.trim();
        }
        if (step.ledfx_scene_action.trim()) {
          payload.ledfx_scene_action = step.ledfx_scene_action.trim();
        }
      }
      if (step.kind === "ledfx_effect") {
        if (!step.ledfx_effect.trim()) {
          errors.push(`Step ${idx + 1}: ledfx_effect is required.`);
        } else {
          payload.ledfx_effect = step.ledfx_effect.trim();
        }
        if (step.ledfx_virtual_id.trim()) {
          payload.ledfx_virtual_id = step.ledfx_virtual_id.trim();
        }
        const config = parseJsonObject(
          step.ledfx_config_json,
          "ledfx_config",
          idx,
          false,
        );
        if (config) payload.ledfx_config = config;
      }
      if (step.kind === "ledfx_brightness") {
        const bri = parseNum(step.ledfx_brightness, "ledfx_brightness", idx, {
          min: 0,
        });
        if (bri == null) {
          errors.push(`Step ${idx + 1}: ledfx_brightness is required.`);
        } else {
          payload.ledfx_brightness = bri;
        }
        if (step.ledfx_virtual_id.trim()) {
          payload.ledfx_virtual_id = step.ledfx_virtual_id.trim();
        }
      }
      if (step.kind === "pause") {
        if (payload.duration_s == null) {
          errors.push(`Step ${idx + 1}: duration_s is required for pause.`);
        }
      }

      steps.push(payload);
    });

    const payload: any = {
      name: builderName.trim() || undefined,
      loop: Boolean(builderLoop),
      steps,
    };
    if (mode === "fleet") {
      payload.targets = parseTargets(targets);
      payload.include_self = includeSelf;
    }

    return { payload, errors };
  }, [
    builderSteps,
    builderName,
    builderLoop,
    includeSelf,
    mode,
    targets,
    wledEffects,
    wledPalettes,
  ]);

  const durationSummary = useMemo(() => {
    let total = 0;
    let unknown = false;
    builderSteps.forEach((step) => {
      const raw = step.duration_s.trim();
      if (!raw) {
        if (step.loop || step.kind === "pause" || step.kind === "sequence") {
          unknown = true;
        }
        return;
      }
      const num = parseFloat(raw);
      if (!Number.isFinite(num)) {
        unknown = true;
        return;
      }
      total += num;
    });
    return { total, unknown };
  }, [builderSteps]);

  useEffect(() => {
    if (editorMode === "builder") {
      setPayloadText(JSON.stringify(builderResult.payload, null, 2));
    }
  }, [builderResult.payload, editorMode]);

  const start = async () => {
    setBusy(true);
    setError(null);
    try {
      let payload: any;
      if (editorMode === "builder") {
        if (builderResult.errors.length) {
          throw new Error(builderResult.errors.join(" "));
        }
        payload = builderResult.payload;
      } else {
        payload = JSON.parse(payloadText || "{}");
        if (mode === "fleet") {
          payload.targets = parseTargets(targets);
          payload.include_self = includeSelf;
        }
      }
      await api(
        mode === "fleet"
          ? "/v1/fleet/orchestration/start"
          : "/v1/orchestration/start",
        { method: "POST", json: payload },
      );
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const stop = async () => {
    setBusy(true);
    setError(null);
    try {
      await api(
        mode === "fleet"
          ? "/v1/fleet/orchestration/stop"
          : "/v1/orchestration/stop",
        { method: "POST", json: {} },
      );
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const addStep = (kind: StepKind) => {
    setBuilderSteps((prev) => [...prev, newStep(kind)]);
  };

  const updateStep = (id: string, patch: Partial<BuilderStep>) => {
    setBuilderSteps((prev) =>
      prev.map((s) => (s.id === id ? { ...s, ...patch } : s)),
    );
  };

  const moveStep = (id: string, dir: -1 | 1) => {
    setBuilderSteps((prev) => {
      const idx = prev.findIndex((s) => s.id === id);
      if (idx < 0) return prev;
      const next = idx + dir;
      if (next < 0 || next >= prev.length) return prev;
      const copy = [...prev];
      const [removed] = copy.splice(idx, 1);
      copy.splice(next, 0, removed);
      return copy;
    });
  };

  const removeStep = (id: string) => {
    setBuilderSteps((prev) => prev.filter((s) => s.id !== id));
  };

  const duplicateStep = (id: string) => {
    setBuilderSteps((prev) => {
      const idx = prev.findIndex((s) => s.id === id);
      if (idx < 0) return prev;
      const copy = [...prev];
      const base = copy[idx];
      copy.splice(idx + 1, 0, { ...base, id: makeId() });
      return copy;
    });
  };

  const applyLastLook = (id: string) => {
    const look = lastApplied?.look?.payload?.look;
    if (!look) {
      setError("No last applied look found.");
      return;
    }
    updateStep(id, { look_json: JSON.stringify(look, null, 2) });
  };

  const applyCurrentState = async (id: string) => {
    try {
      const res = await api<{ ok: boolean; state?: any }>("/v1/wled/state", {
        method: "GET",
      });
      if (res?.state) {
        updateStep(id, { state_json: JSON.stringify(res.state, null, 2) });
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const toggleEditor = () => {
    if (editorMode === "builder") {
      setEditorMode("json");
      return;
    }
    try {
      const parsed = JSON.parse(payloadText || "{}");
      const built = payloadToBuilder(parsed);
      if (!built) throw new Error("Payload JSON is missing a steps array.");
      setBuilderName(built.name);
      setBuilderLoop(built.loop);
      setBuilderSteps(built.steps);
      if (built.targets) setTargets(built.targets);
      setIncludeSelf(built.includeSelf);
      setEditorMode("builder");
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const savePreset = async () => {
    setBusy(true);
    setError(null);
    try {
      const name = presetName.trim();
      if (!name) throw new Error("Preset name is required.");
      const scopeToSave = presetScope === "all" ? mode : presetScope;
      const versionNum = parseInt(presetVersion, 10);
      const version = Number.isFinite(versionNum) ? versionNum : undefined;
      let payload: any;
      if (editorMode === "builder") {
        if (builderResult.errors.length) {
          throw new Error(builderResult.errors.join(" "));
        }
        payload = { ...builderResult.payload };
        if (scopeToSave !== "fleet") {
          delete payload.targets;
          delete payload.include_self;
        }
      } else {
        payload = JSON.parse(payloadText || "{}");
        if (mode === "fleet" && scopeToSave === "fleet") {
          payload.targets = parseTargets(targets);
          payload.include_self = includeSelf;
        }
      }
      const res = await api<{ ok: boolean; preset: OrchestrationPreset }>(
        "/v1/orchestration/presets",
        {
          method: "POST",
          json: {
            name,
            scope: scopeToSave,
            description: presetDescription.trim() || null,
            tags: parseTags(presetTags),
            version,
            payload,
          },
        },
      );
      setSelectedPresetId(String(res.preset?.id ?? ""));
      await refreshPresets();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const loadPreset = () => {
    const preset = presets.find((p) => String(p.id) === String(selectedPresetId));
    if (!preset) return;
    const payload = preset.payload || {};
    const built = payloadToBuilder(payload);
    if (preset.scope === "fleet" || preset.scope === "local") {
      setMode(preset.scope);
    }
    if (
      preset.scope === "local" ||
      preset.scope === "fleet" ||
      preset.scope === "crossfade"
    ) {
      setPresetScope(preset.scope);
    }
    setPresetName(preset.name);
    setPresetDescription(preset.description || "");
    setPresetTags((preset.tags || []).join(", "));
    setPresetVersion(
      preset.version != null && Number.isFinite(preset.version)
        ? String(preset.version)
        : ""
    );
    if (built) {
      setBuilderName(built.name);
      setBuilderLoop(built.loop);
      setBuilderSteps(built.steps);
      if (built.targets) setTargets(built.targets);
      setIncludeSelf(built.includeSelf);
      setEditorMode("builder");
    } else {
      setEditorMode("json");
      setPayloadText(JSON.stringify(payload, null, 2));
    }
  };

  const deletePreset = async () => {
    const preset = presets.find((p) => String(p.id) === String(selectedPresetId));
    if (!preset) return;
    setBusy(true);
    setError(null);
    try {
      await api(`/v1/orchestration/presets/${preset.id}`, {
        method: "DELETE",
      });
      setSelectedPresetId("");
      await refreshPresets();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const exportPresets = async () => {
    setBusy(true);
    setError(null);
    try {
      const scopeParam =
        presetScope === "all"
          ? ""
          : `&scope=${encodeURIComponent(presetScope)}`;
      const res = await api<PresetsRes>(
        `/v1/orchestration/presets/export?limit=2000${scopeParam}`,
        { method: "GET" },
      );
      setPresetExport(JSON.stringify(res.presets || [], null, 2));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const importPresets = async () => {
    setBusy(true);
    setError(null);
    try {
      const raw = presetImportText.trim();
      if (!raw) throw new Error("Paste preset JSON to import.");
      const parsed = JSON.parse(raw);
      const presetsList = Array.isArray(parsed)
        ? parsed
        : Array.isArray(parsed?.presets)
          ? parsed.presets
          : null;
      if (!presetsList) throw new Error("JSON must be an array or {presets:[...]}.");
      await api("/v1/orchestration/presets/import", {
        method: "POST",
        json: { presets: presetsList },
      });
      setPresetImportText("");
      await refreshPresets();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const filteredPresets = useMemo(
    () =>
      presets.filter((p) =>
        presetScope === "all" ? true : p.scope === presetScope,
      ),
    [presetScope, presets],
  );

  return (
    <Stack spacing={2}>
      {error ? <Alert severity="error">{error}</Alert> : null}

      <Card>
        <CardContent>
          <Typography variant="h6">Preset Library</Typography>
          <Typography variant="body2" color="text.secondary">
            Save and reuse orchestration payloads.
          </Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <FormControl fullWidth>
              <InputLabel>Preset</InputLabel>
              <Select
                value={selectedPresetId}
                label="Preset"
                onChange={(e) => setSelectedPresetId(String(e.target.value))}
                disabled={busy}
              >
                <MenuItem value="">
                  <em>None</em>
                </MenuItem>
                {filteredPresets.map((p) => (
                  <MenuItem key={p.id} value={String(p.id)}>
                    {p.name}
                    {p.version ? ` (v${p.version})` : ""}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            <FormControl fullWidth>
              <InputLabel>Preset scope</InputLabel>
              <Select
                value={presetScope}
                label="Preset scope"
                onChange={(e) =>
                  setPresetScope(
                    e.target.value as "local" | "fleet" | "crossfade" | "all",
                  )
                }
                disabled={busy}
              >
                <MenuItem value="local">local</MenuItem>
                <MenuItem value="fleet">fleet</MenuItem>
                <MenuItem value="crossfade">crossfade</MenuItem>
                <MenuItem value="all">all</MenuItem>
              </Select>
            </FormControl>
            <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap" }}>
              <Button onClick={loadPreset} disabled={busy || !selectedPresetId}>
                Load preset
              </Button>
              <Button
                color="error"
                onClick={deletePreset}
                disabled={busy || !selectedPresetId}
              >
                Delete preset
              </Button>
            </Stack>
            <Divider />
            <TextField
              label="Preset name"
              value={presetName}
              onChange={(e) => setPresetName(e.target.value)}
              disabled={busy}
            />
            <TextField
              label="Description"
              value={presetDescription}
              onChange={(e) => setPresetDescription(e.target.value)}
              disabled={busy}
            />
            <TextField
              label="Tags (comma-separated)"
              value={presetTags}
              onChange={(e) => setPresetTags(e.target.value)}
              disabled={busy}
            />
            <TextField
              label="Version (optional)"
              value={presetVersion}
              onChange={(e) => setPresetVersion(e.target.value)}
              disabled={busy}
              inputMode="numeric"
            />
            <Divider />
            <Typography variant="subtitle2">Import / Export</Typography>
            <TextField
              label="Exported presets JSON"
              value={presetExport}
              disabled
              multiline
              minRows={3}
            />
            <TextField
              label="Import presets JSON"
              value={presetImportText}
              onChange={(e) => setPresetImportText(e.target.value)}
              disabled={busy}
              multiline
              minRows={3}
            />
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            startIcon={<SaveIcon />}
            onClick={savePreset}
            disabled={busy}
          >
            Save preset
          </Button>
          <Button onClick={exportPresets} disabled={busy}>
            Export
          </Button>
          <Button onClick={importPresets} disabled={busy || !presetImportText.trim()}>
            Import
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Orchestration</Typography>
          <Typography variant="body2" color="text.secondary">
            Run a scene or playlist (looks, crossfade, sequences, DDP, blackout) locally or
            across the fleet.
          </Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <FormControl fullWidth>
              <InputLabel>Mode</InputLabel>
              <Select
                value={mode}
                label="Mode"
                onChange={(e) => setMode(e.target.value as "local" | "fleet")}
                disabled={busy}
              >
                <MenuItem value="local">local</MenuItem>
                <MenuItem value="fleet">fleet</MenuItem>
              </Select>
            </FormControl>
            {mode === "fleet" ? (
              <Stack spacing={2}>
                <TextField
                  label="Targets (comma-separated, optional)"
                  value={targets}
                  onChange={(e) => setTargets(e.target.value)}
                  helperText='Examples: "roofline1", "role:roofline", "tag:outside", "*"'
                  disabled={busy}
                />
                <FormControlLabel
                  control={
                    <Switch
                      checked={includeSelf}
                      onChange={(e) => setIncludeSelf(e.target.checked)}
                    />
                  }
                  label="Include self"
                />
              </Stack>
            ) : null}
            <FormControlLabel
              control={
                <Switch
                  checked={editorMode === "json"}
                  onChange={toggleEditor}
                />
              }
              label="JSON editor"
            />
          </Stack>
          {editorMode === "builder" ? (
            <Stack spacing={2} sx={{ mt: 2 }}>
              {builderResult.errors.length ? (
                <Alert severity="warning">{builderResult.errors.join(" ")}</Alert>
              ) : null}
              <TextField
                label="Name"
                value={builderName}
                onChange={(e) => setBuilderName(e.target.value)}
                disabled={busy}
              />
              <FormControlLabel
                control={
                  <Switch
                    checked={builderLoop}
                    onChange={(e) => setBuilderLoop(e.target.checked)}
                  />
                }
                label="Loop playlist"
              />
              <Typography variant="body2" color="text.secondary">
                Estimated duration:{" "}
                <code>
                  {durationSummary.unknown
                    ? "unknown"
                    : `${durationSummary.total.toFixed(1)}s`}
                </code>
              </Typography>
              <Stack spacing={2}>
                {builderSteps.map((step, idx) => {
                  const lookPreview = step.use_look_builder
                    ? getLookPreview(step, idx)
                    : null;
                  const statePreview = step.use_state_builder
                    ? getStatePreview(step, idx)
                    : null;
                  return (
                    <Card
                      key={step.id}
                      variant="outlined"
                      sx={{ p: 1, borderColor: "divider" }}
                    >
                      <Stack spacing={1}>
                      <Stack direction="row" spacing={1} alignItems="center">
                        <Typography variant="subtitle2">Step {idx + 1}</Typography>
                        <FormControl size="small">
                          <Select
                            value={step.kind}
                            onChange={(e) =>
                              updateStep(step.id, {
                                kind: e.target.value as StepKind,
                              })
                            }
                            disabled={busy}
                          >
                            {stepKinds.map((k) => (
                              <MenuItem key={k} value={k}>
                                {k}
                              </MenuItem>
                            ))}
                          </Select>
                        </FormControl>
                        <IconButton
                          size="small"
                          onClick={() => moveStep(step.id, -1)}
                          disabled={busy || idx === 0}
                        >
                          <ArrowUpwardIcon fontSize="small" />
                        </IconButton>
                        <IconButton
                          size="small"
                          onClick={() => moveStep(step.id, 1)}
                          disabled={busy || idx === builderSteps.length - 1}
                        >
                          <ArrowDownwardIcon fontSize="small" />
                        </IconButton>
                        <IconButton
                          size="small"
                          onClick={() => duplicateStep(step.id)}
                          disabled={busy}
                        >
                          <ContentCopyIcon fontSize="small" />
                        </IconButton>
                        <IconButton
                          size="small"
                          onClick={() => removeStep(step.id)}
                          disabled={busy}
                        >
                          <DeleteIcon fontSize="small" />
                        </IconButton>
                      </Stack>
                      <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap" }}>
                        <TextField
                          label="duration_s"
                          value={step.duration_s}
                          onChange={(e) =>
                            updateStep(step.id, { duration_s: e.target.value })
                          }
                          inputMode="numeric"
                          disabled={busy}
                          size="small"
                        />
                        <TextField
                          label="transition_ms"
                          value={step.transition_ms}
                          onChange={(e) =>
                            updateStep(step.id, { transition_ms: e.target.value })
                          }
                          inputMode="numeric"
                          disabled={busy}
                          size="small"
                        />
                        <TextField
                          label="brightness"
                          value={step.brightness}
                          onChange={(e) =>
                            updateStep(step.id, { brightness: e.target.value })
                          }
                          inputMode="numeric"
                          disabled={busy}
                          size="small"
                        />
                        {step.kind === "sequence" ? (
                          <FormControl size="small" sx={{ minWidth: 220 }}>
                            <InputLabel>Sequence</InputLabel>
                            <Select
                              value={
                                sequenceFiles.includes(step.sequence_file)
                                  ? step.sequence_file
                                  : ""
                              }
                              label="Sequence"
                              onChange={(e) =>
                                updateStep(step.id, {
                                  sequence_file: String(e.target.value),
                                })
                              }
                              disabled={busy || sequenceFiles.length === 0}
                            >
                              <MenuItem value="">
                                <em>Custom</em>
                              </MenuItem>
                              {sequenceFiles.map((f) => (
                                <MenuItem key={f} value={f}>
                                  {f}
                                </MenuItem>
                              ))}
                            </Select>
                          </FormControl>
                        ) : null}
                        {step.kind === "sequence" ? (
                          <TextField
                            label="sequence_file"
                            value={step.sequence_file}
                            onChange={(e) =>
                              updateStep(step.id, {
                                sequence_file: e.target.value,
                              })
                            }
                            disabled={busy}
                            size="small"
                            sx={{ minWidth: 240 }}
                          />
                        ) : null}
                        {step.kind === "sequence" ? (
                          <FormControlLabel
                            control={
                              <Switch
                                checked={step.loop}
                                onChange={(e) =>
                                  updateStep(step.id, { loop: e.target.checked })
                                }
                              />
                            }
                            label="Loop"
                          />
                        ) : null}
                        {step.kind === "sequence" && mode === "fleet" ? (
                          <>
                            <TextField
                              label="stagger_s"
                              value={step.stagger_s}
                              onChange={(e) =>
                                updateStep(step.id, {
                                  stagger_s: e.target.value,
                                })
                              }
                              inputMode="numeric"
                              disabled={busy}
                              size="small"
                            />
                            <TextField
                              label="start_delay_s"
                              value={step.start_delay_s}
                              onChange={(e) =>
                                updateStep(step.id, {
                                  start_delay_s: e.target.value,
                                })
                              }
                              inputMode="numeric"
                              disabled={busy}
                              size="small"
                            />
                          </>
                        ) : null}
                        {step.kind === "preset" ? (
                          <FormControl size="small" sx={{ minWidth: 180 }}>
                            <InputLabel>Preset</InputLabel>
                            <Select
                              value={
                                wledPresets.some(
                                  (p) => String(p.id) === String(step.preset_id),
                                )
                                  ? String(step.preset_id)
                                  : ""
                              }
                              label="Preset"
                              onChange={(e) =>
                                updateStep(step.id, {
                                  preset_id: String(e.target.value),
                                })
                              }
                              disabled={busy || wledPresets.length === 0}
                            >
                              <MenuItem value="">
                                <em>Custom</em>
                              </MenuItem>
                              {wledPresets.map((p) => (
                                <MenuItem key={p.id} value={String(p.id)}>
                                  {p.id} - {p.name}
                                </MenuItem>
                              ))}
                            </Select>
                          </FormControl>
                        ) : null}
                        {step.kind === "preset" ? (
                          <TextField
                            label="preset_id"
                            value={step.preset_id}
                            onChange={(e) =>
                              updateStep(step.id, { preset_id: e.target.value })
                            }
                            inputMode="numeric"
                            disabled={busy}
                            size="small"
                          />
                        ) : null}
                        {step.kind === "ddp" ? (
                          <>
                            <FormControl size="small" sx={{ minWidth: 180 }}>
                              <InputLabel>Pattern</InputLabel>
                              <Select
                                value={
                                  ddpPatterns.includes(step.pattern)
                                    ? step.pattern
                                    : ""
                                }
                                label="Pattern"
                                onChange={(e) =>
                                  updateStep(step.id, {
                                    pattern: String(e.target.value),
                                  })
                                }
                                disabled={busy || ddpPatterns.length === 0}
                              >
                                <MenuItem value="">
                                  <em>Custom</em>
                                </MenuItem>
                                {ddpPatterns.map((p) => (
                                  <MenuItem key={p} value={p}>
                                    {p}
                                  </MenuItem>
                                ))}
                              </Select>
                            </FormControl>
                            <TextField
                              label="pattern"
                              value={step.pattern}
                              onChange={(e) =>
                                updateStep(step.id, { pattern: e.target.value })
                              }
                              disabled={busy}
                              size="small"
                            />
                            <TextField
                              label="fps"
                              value={step.fps}
                              onChange={(e) =>
                                updateStep(step.id, { fps: e.target.value })
                              }
                              inputMode="numeric"
                              disabled={busy}
                              size="small"
                            />
                          </>
                        ) : null}
                        {step.kind === "ledfx_scene" ? (
                          <>
                            <TextField
                              label="ledfx_scene_id"
                              value={step.ledfx_scene_id}
                              onChange={(e) =>
                                updateStep(step.id, {
                                  ledfx_scene_id: e.target.value,
                                })
                              }
                              disabled={busy}
                              size="small"
                              sx={{ minWidth: 220 }}
                            />
                            <FormControl size="small" sx={{ minWidth: 180 }}>
                              <InputLabel>Scene action</InputLabel>
                              <Select
                                value={step.ledfx_scene_action || "activate"}
                                label="Scene action"
                                onChange={(e) =>
                                  updateStep(step.id, {
                                    ledfx_scene_action: String(e.target.value),
                                  })
                                }
                                disabled={busy}
                              >
                                <MenuItem value="activate">activate</MenuItem>
                                <MenuItem value="deactivate">deactivate</MenuItem>
                              </Select>
                            </FormControl>
                          </>
                        ) : null}
                        {step.kind === "ledfx_effect" ? (
                          <>
                            <TextField
                              label="ledfx_virtual_id (optional)"
                              value={step.ledfx_virtual_id}
                              onChange={(e) =>
                                updateStep(step.id, {
                                  ledfx_virtual_id: e.target.value,
                                })
                              }
                              disabled={busy}
                              size="small"
                              sx={{ minWidth: 200 }}
                            />
                            <TextField
                              label="ledfx_effect"
                              value={step.ledfx_effect}
                              onChange={(e) =>
                                updateStep(step.id, {
                                  ledfx_effect: e.target.value,
                                })
                              }
                              disabled={busy}
                              size="small"
                            />
                          </>
                        ) : null}
                        {step.kind === "ledfx_brightness" ? (
                          <>
                            <TextField
                              label="ledfx_virtual_id (optional)"
                              value={step.ledfx_virtual_id}
                              onChange={(e) =>
                                updateStep(step.id, {
                                  ledfx_virtual_id: e.target.value,
                                })
                              }
                              disabled={busy}
                              size="small"
                              sx={{ minWidth: 200 }}
                            />
                            <TextField
                              label="ledfx_brightness"
                              value={step.ledfx_brightness}
                              onChange={(e) =>
                                updateStep(step.id, {
                                  ledfx_brightness: e.target.value,
                                })
                              }
                              inputMode="numeric"
                              disabled={busy}
                              size="small"
                            />
                          </>
                        ) : null}
                      </Stack>
                      {step.kind === "ledfx_effect" ? (
                        <TextField
                          label="ledfx_config (JSON)"
                          value={step.ledfx_config_json}
                          onChange={(e) =>
                            updateStep(step.id, {
                              ledfx_config_json: e.target.value,
                            })
                          }
                          disabled={busy}
                          size="small"
                          fullWidth
                          multiline
                          minRows={3}
                        />
                      ) : null}
                      {step.kind === "look" || step.kind === "crossfade" ? (
                        <>
                          <FormControlLabel
                            control={
                              <Switch
                                checked={step.use_look_builder}
                                onChange={(e) =>
                                  updateStep(step.id, {
                                    use_look_builder: e.target.checked,
                                  })
                                }
                              />
                            }
                            label="Use visual look builder"
                          />
                          {step.use_look_builder ? (
                            <Stack spacing={1}>
                              {lookPreview?.errors.length ? (
                                <Alert severity="warning">
                                  {lookPreview.errors.join(" ")}
                                </Alert>
                              ) : null}
                              <Stack
                                direction="row"
                                spacing={1}
                                sx={{ flexWrap: "wrap" }}
                              >
                                <TextField
                                  label="look name"
                                  value={step.look_name}
                                  onChange={(e) =>
                                    updateStep(step.id, {
                                      look_name: e.target.value,
                                    })
                                  }
                                  size="small"
                                />
                                <TextField
                                  label="look theme"
                                  value={step.look_theme}
                                  onChange={(e) =>
                                    updateStep(step.id, {
                                      look_theme: e.target.value,
                                    })
                                  }
                                  size="small"
                                />
                                <FormControl size="small" sx={{ minWidth: 160 }}>
                                  <InputLabel>Effect</InputLabel>
                                  <Select
                                    value={
                                      wledEffects.includes(step.look_effect)
                                        ? step.look_effect
                                        : ""
                                    }
                                    label="Effect"
                                    onChange={(e) =>
                                      updateStep(step.id, {
                                        look_effect: String(e.target.value),
                                      })
                                    }
                                    disabled={busy || wledEffects.length === 0}
                                  >
                                    <MenuItem value="">
                                      <em>Custom</em>
                                    </MenuItem>
                                    {wledEffects.map((eff) => (
                                      <MenuItem key={eff} value={eff}>
                                        {eff}
                                      </MenuItem>
                                    ))}
                                  </Select>
                                </FormControl>
                                <TextField
                                  label="effect"
                                  value={step.look_effect}
                                  onChange={(e) =>
                                    updateStep(step.id, {
                                      look_effect: e.target.value,
                                    })
                                  }
                                  size="small"
                                />
                                <FormControl size="small" sx={{ minWidth: 160 }}>
                                  <InputLabel>Palette</InputLabel>
                                  <Select
                                    value={
                                      wledPalettes.includes(step.look_palette)
                                        ? step.look_palette
                                        : ""
                                    }
                                    label="Palette"
                                    onChange={(e) =>
                                      updateStep(step.id, {
                                        look_palette: String(e.target.value),
                                      })
                                    }
                                    disabled={busy || wledPalettes.length === 0}
                                  >
                                    <MenuItem value="">
                                      <em>Custom</em>
                                    </MenuItem>
                                    {wledPalettes.map((pal) => (
                                      <MenuItem key={pal} value={pal}>
                                        {pal}
                                      </MenuItem>
                                    ))}
                                  </Select>
                                </FormControl>
                                <TextField
                                  label="palette"
                                  value={step.look_palette}
                                  onChange={(e) =>
                                    updateStep(step.id, {
                                      look_palette: e.target.value,
                                    })
                                  }
                                  size="small"
                                />
                                <TextField
                                  label="segment id"
                                  value={step.look_segment}
                                  onChange={(e) =>
                                    updateStep(step.id, {
                                      look_segment: e.target.value,
                                    })
                                  }
                                  inputMode="numeric"
                                  size="small"
                                />
                                <TextField
                                  label="speed (sx)"
                                  value={step.look_speed}
                                  onChange={(e) =>
                                    updateStep(step.id, {
                                      look_speed: e.target.value,
                                    })
                                  }
                                  inputMode="numeric"
                                  size="small"
                                />
                                <TextField
                                  label="intensity (ix)"
                                  value={step.look_intensity}
                                  onChange={(e) =>
                                    updateStep(step.id, {
                                      look_intensity: e.target.value,
                                    })
                                  }
                                  inputMode="numeric"
                                  size="small"
                                />
                                <FormControlLabel
                                  control={
                                    <Switch
                                      checked={step.look_reverse}
                                      onChange={(e) =>
                                        updateStep(step.id, {
                                          look_reverse: e.target.checked,
                                        })
                                      }
                                    />
                                  }
                                  label="Reverse"
                                />
                                <FormControlLabel
                                  control={
                                    <Switch
                                      checked={step.look_on}
                                      onChange={(e) =>
                                        updateStep(step.id, {
                                          look_on: e.target.checked,
                                        })
                                      }
                                    />
                                  }
                                  label="On"
                                />
                              </Stack>
                              <Stack
                                direction="row"
                                spacing={1}
                                sx={{ flexWrap: "wrap" }}
                              >
                                <TextField
                                  label="color 1"
                                  type="color"
                                  value={step.look_color1}
                                  onChange={(e) =>
                                    updateStep(step.id, {
                                      look_color1: e.target.value,
                                    })
                                  }
                                  size="small"
                                  InputLabelProps={{ shrink: true }}
                                />
                                <TextField
                                  label="color 2"
                                  type="color"
                                  value={step.look_color2}
                                  onChange={(e) =>
                                    updateStep(step.id, {
                                      look_color2: e.target.value,
                                    })
                                  }
                                  size="small"
                                  InputLabelProps={{ shrink: true }}
                                />
                                <TextField
                                  label="color 3"
                                  type="color"
                                  value={step.look_color3}
                                  onChange={(e) =>
                                    updateStep(step.id, {
                                      look_color3: e.target.value,
                                    })
                                  }
                                  size="small"
                                  InputLabelProps={{ shrink: true }}
                                />
                              </Stack>
                              <TextField
                                label="Generated look JSON"
                                value={lookPreview?.text || ""}
                                disabled
                                multiline
                                minRows={3}
                              />
                            </Stack>
                          ) : (
                            <>
                              <Stack direction="row" spacing={1}>
                                <Button
                                  size="small"
                                  onClick={() => applyLastLook(step.id)}
                                  disabled={busy}
                                >
                                  Use last applied look
                                </Button>
                              </Stack>
                              <TextField
                                label="look JSON"
                                value={step.look_json}
                                onChange={(e) =>
                                  updateStep(step.id, { look_json: e.target.value })
                                }
                                disabled={busy}
                                multiline
                                minRows={3}
                                placeholder='{"name":"Warm","colors":["#ffcc88"]}'
                              />
                            </>
                          )}
                        </>
                      ) : null}
                      {step.kind === "state" || step.kind === "crossfade" ? (
                        <>
                          <FormControlLabel
                            control={
                              <Switch
                                checked={step.use_state_builder}
                                onChange={(e) =>
                                  updateStep(step.id, {
                                    use_state_builder: e.target.checked,
                                  })
                                }
                              />
                            }
                            label="Use visual state builder"
                          />
                          {step.use_state_builder ? (
                            <Stack spacing={1}>
                              {statePreview?.errors.length ? (
                                <Alert severity="warning">
                                  {statePreview.errors.join(" ")}
                                </Alert>
                              ) : null}
                              <Stack
                                direction="row"
                                spacing={1}
                                sx={{ flexWrap: "wrap" }}
                              >
                                <FormControlLabel
                                  control={
                                    <Switch
                                      checked={step.state_on}
                                      onChange={(e) =>
                                        updateStep(step.id, {
                                          state_on: e.target.checked,
                                        })
                                      }
                                    />
                                  }
                                  label="On"
                                />
                                <TextField
                                  label="brightness"
                                  value={step.state_brightness}
                                  onChange={(e) =>
                                    updateStep(step.id, {
                                      state_brightness: e.target.value,
                                    })
                                  }
                                  inputMode="numeric"
                                  size="small"
                                />
                                <FormControl size="small" sx={{ minWidth: 160 }}>
                                  <InputLabel>Effect</InputLabel>
                                  <Select
                                    value={
                                      wledEffects.includes(step.state_effect)
                                        ? step.state_effect
                                        : ""
                                    }
                                    label="Effect"
                                    onChange={(e) =>
                                      updateStep(step.id, {
                                        state_effect: String(e.target.value),
                                      })
                                    }
                                    disabled={busy || wledEffects.length === 0}
                                  >
                                    <MenuItem value="">
                                      <em>Custom</em>
                                    </MenuItem>
                                    {wledEffects.map((eff) => (
                                      <MenuItem key={eff} value={eff}>
                                        {eff}
                                      </MenuItem>
                                    ))}
                                  </Select>
                                </FormControl>
                                <TextField
                                  label="effect"
                                  value={step.state_effect}
                                  onChange={(e) =>
                                    updateStep(step.id, {
                                      state_effect: e.target.value,
                                    })
                                  }
                                  size="small"
                                />
                                <FormControl size="small" sx={{ minWidth: 160 }}>
                                  <InputLabel>Palette</InputLabel>
                                  <Select
                                    value={
                                      wledPalettes.includes(step.state_palette)
                                        ? step.state_palette
                                        : ""
                                    }
                                    label="Palette"
                                    onChange={(e) =>
                                      updateStep(step.id, {
                                        state_palette: String(e.target.value),
                                      })
                                    }
                                    disabled={busy || wledPalettes.length === 0}
                                  >
                                    <MenuItem value="">
                                      <em>Custom</em>
                                    </MenuItem>
                                    {wledPalettes.map((pal) => (
                                      <MenuItem key={pal} value={pal}>
                                        {pal}
                                      </MenuItem>
                                    ))}
                                  </Select>
                                </FormControl>
                                <TextField
                                  label="palette"
                                  value={step.state_palette}
                                  onChange={(e) =>
                                    updateStep(step.id, {
                                      state_palette: e.target.value,
                                    })
                                  }
                                  size="small"
                                />
                                <TextField
                                  label="segment id"
                                  value={step.state_segment}
                                  onChange={(e) =>
                                    updateStep(step.id, {
                                      state_segment: e.target.value,
                                    })
                                  }
                                  inputMode="numeric"
                                  size="small"
                                />
                                <TextField
                                  label="speed (sx)"
                                  value={step.state_speed}
                                  onChange={(e) =>
                                    updateStep(step.id, {
                                      state_speed: e.target.value,
                                    })
                                  }
                                  inputMode="numeric"
                                  size="small"
                                />
                                <TextField
                                  label="intensity (ix)"
                                  value={step.state_intensity}
                                  onChange={(e) =>
                                    updateStep(step.id, {
                                      state_intensity: e.target.value,
                                    })
                                  }
                                  inputMode="numeric"
                                  size="small"
                                />
                                <FormControlLabel
                                  control={
                                    <Switch
                                      checked={step.state_reverse}
                                      onChange={(e) =>
                                        updateStep(step.id, {
                                          state_reverse: e.target.checked,
                                        })
                                      }
                                    />
                                  }
                                  label="Reverse"
                                />
                              </Stack>
                              <Stack
                                direction="row"
                                spacing={1}
                                sx={{ flexWrap: "wrap" }}
                              >
                                <TextField
                                  label="color 1"
                                  type="color"
                                  value={step.state_color1}
                                  onChange={(e) =>
                                    updateStep(step.id, {
                                      state_color1: e.target.value,
                                    })
                                  }
                                  size="small"
                                  InputLabelProps={{ shrink: true }}
                                />
                                <TextField
                                  label="color 2"
                                  type="color"
                                  value={step.state_color2}
                                  onChange={(e) =>
                                    updateStep(step.id, {
                                      state_color2: e.target.value,
                                    })
                                  }
                                  size="small"
                                  InputLabelProps={{ shrink: true }}
                                />
                                <TextField
                                  label="color 3"
                                  type="color"
                                  value={step.state_color3}
                                  onChange={(e) =>
                                    updateStep(step.id, {
                                      state_color3: e.target.value,
                                    })
                                  }
                                  size="small"
                                  InputLabelProps={{ shrink: true }}
                                />
                              </Stack>
                              <TextField
                                label="Generated state JSON"
                                value={statePreview?.text || ""}
                                disabled
                                multiline
                                minRows={3}
                              />
                            </Stack>
                          ) : (
                            <>
                              <Stack direction="row" spacing={1}>
                                <Button
                                  size="small"
                                  onClick={() => applyCurrentState(step.id)}
                                  disabled={busy}
                                >
                                  Use current WLED state
                                </Button>
                              </Stack>
                              <TextField
                                label="state JSON"
                                value={step.state_json}
                                onChange={(e) =>
                                  updateStep(step.id, { state_json: e.target.value })
                                }
                                disabled={busy}
                                multiline
                                minRows={3}
                                placeholder='{"on":true,"bri":128}'
                              />
                            </>
                          )}
                        </>
                      ) : null}
                      {step.kind === "ddp" ? (
                        <TextField
                          label="params JSON"
                          value={step.params_json}
                          onChange={(e) =>
                            updateStep(step.id, { params_json: e.target.value })
                          }
                          disabled={busy}
                          multiline
                          minRows={2}
                          placeholder='{"speed":1.0}'
                        />
                      ) : null}
                      </Stack>
                    </Card>
                  );
                })}
              </Stack>
              <Stack direction="row" spacing={1} sx={{ alignItems: "center" }}>
                <FormControl size="small">
                  <Select
                    value={newStepKind}
                    onChange={(e) => setNewStepKind(e.target.value as StepKind)}
                    disabled={busy}
                    inputProps={{ "aria-label": "New step kind" }}
                  >
                    {stepKinds.map((k) => (
                      <MenuItem key={k} value={k}>
                        {k}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
                <Button
                  startIcon={<AddIcon />}
                  onClick={() => addStep(newStepKind)}
                  disabled={busy}
                >
                  Add step
                </Button>
              </Stack>
              <TextField
                label="Generated JSON"
                value={payloadText}
                disabled
                multiline
                minRows={6}
              />
            </Stack>
          ) : (
            <Stack spacing={2} sx={{ mt: 2 }}>
              <TextField
                label="Payload JSON"
                value={payloadText}
                onChange={(e) => setPayloadText(e.target.value)}
                disabled={busy}
                multiline
                minRows={6}
              />
            </Stack>
          )}
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            startIcon={<PlayArrowIcon />}
            onClick={start}
            disabled={busy}
          >
            Start
          </Button>
          <Button
            color="error"
            startIcon={<StopIcon />}
            onClick={stop}
            disabled={busy}
          >
            Stop
          </Button>
          <Button startIcon={<RefreshIcon />} onClick={refresh} disabled={busy}>
            Refresh
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Local status</Typography>
          <Box
            component="pre"
            sx={{
              whiteSpace: "pre-wrap",
              wordBreak: "break-word",
              fontSize: 12,
              mt: 1,
            }}
          >
            {prettyLocal}
          </Box>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Fleet status</Typography>
          <Box
            component="pre"
            sx={{
              whiteSpace: "pre-wrap",
              wordBreak: "break-word",
              fontSize: 12,
              mt: 1,
            }}
          >
            {prettyFleet}
          </Box>
        </CardContent>
      </Card>
    </Stack>
  );
}
