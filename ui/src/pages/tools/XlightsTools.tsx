import UploadFileIcon from "@mui/icons-material/UploadFile";
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

export function XlightsTools() {
  const nav = useNavigate();
  const [files, setFiles] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [lastJobId, setLastJobId] = useState<string | null>(null);

  // Project import
  const [projectDir, setProjectDir] = useState("xlights");
  const [projectOut, setProjectOut] = useState(
    "show/show_config_xlights_project.json",
  );
  const [projectName, setProjectName] = useState("xlights-project");
  const [includeControllers, setIncludeControllers] = useState(true);
  const [includeModels, setIncludeModels] = useState(true);

  // Networks-only import
  const [networksFile, setNetworksFile] = useState(
    "xlights/xlights_networks.xml",
  );
  const [networksOut, setNetworksOut] = useState(
    "show/show_config_xlights_networks.json",
  );
  const [networksName, setNetworksName] = useState("xlights-networks");

  // xsq timing import
  const [xsqFile, setXsqFile] = useState("xlights/song.xsq");
  const [timingTrack, setTimingTrack] = useState("Beat");
  const [xsqOut, setXsqOut] = useState("audio/beats_xlights.json");

  const refreshFiles = async () => {
    try {
      const res = await api<{ ok: boolean; files: string[] }>(
        "/v1/files/list?dir=xlights&recursive=true&limit=500",
        { method: "GET" },
      );
      setFiles(res.files || []);
    } catch {
      setFiles([]);
    }
  };

  useEffect(() => {
    void refreshFiles();
  }, []);

  useEventRefresh({
    types: ["files", "tick"],
    refresh: refreshFiles,
    minIntervalMs: 5000,
    ignoreEvents: ["list", "status"],
  });

  const networksCandidates = useMemo(
    () => files.filter((f) => f.toLowerCase().endsWith("xlights_networks.xml")),
    [files],
  );
  const xsqCandidates = useMemo(
    () => files.filter((f) => f.toLowerCase().endsWith(".xsq")),
    [files],
  );

  const submitJob = async (path: string, json: any) => {
    setBusy(true);
    setError(null);
    try {
      const res = await api<{ ok: boolean; job: { id: string } }>(path, {
        method: "POST",
        json,
      });
      setLastJobId(res.job.id);
      await refreshFiles();
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
      {lastJobId ? (
        <Alert severity="success">
          Job submitted: <code>{lastJobId}</code>
        </Alert>
      ) : null}

      <Card>
        <CardContent>
          <Typography variant="h6">Project Import</Typography>
          <Typography variant="body2" color="text.secondary">
            Imports networks + model channel ranges from an xLights project
            folder under <code>DATA_DIR</code>.
          </Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <TextField
              label="Project dir (relative to DATA_DIR)"
              value={projectDir}
              onChange={(e) => setProjectDir(e.target.value)}
              disabled={busy}
            />
            <TextField
              label="Show name"
              value={projectName}
              onChange={(e) => setProjectName(e.target.value)}
              disabled={busy}
            />
            <TextField
              label="Output file"
              value={projectOut}
              onChange={(e) => setProjectOut(e.target.value)}
              disabled={busy}
            />
            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <FormControlLabel
                control={
                  <Switch
                    checked={includeControllers}
                    onChange={(e) => setIncludeControllers(e.target.checked)}
                  />
                }
                label="Include controllers"
              />
              <FormControlLabel
                control={
                  <Switch
                    checked={includeModels}
                    onChange={(e) => setIncludeModels(e.target.checked)}
                  />
                }
                label="Include models"
              />
            </Stack>
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            startIcon={<UploadFileIcon />}
            disabled={busy}
            onClick={() =>
              submitJob("/v1/jobs/xlights/import_project", {
                project_dir: projectDir,
                out_file: projectOut,
                show_name: projectName,
                include_controllers: includeControllers,
                include_models: includeModels,
              })
            }
          >
            Import project (job)
          </Button>
          <Button onClick={refreshFiles} disabled={busy}>
            Refresh file list
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Networks Import</Typography>
          <Typography variant="body2" color="text.secondary">
            Imports <code>xlights_networks.xml</code> only (controllers).
          </Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <TextField
              label="Networks file (relative to DATA_DIR)"
              value={networksFile}
              onChange={(e) => setNetworksFile(e.target.value)}
              disabled={busy}
              helperText={
                networksCandidates.length
                  ? `Found: ${networksCandidates[0]}`
                  : "Tip: put xLights files under data/xlights/ on the coordinator."
              }
            />
            <TextField
              label="Show name"
              value={networksName}
              onChange={(e) => setNetworksName(e.target.value)}
              disabled={busy}
            />
            <TextField
              label="Output file"
              value={networksOut}
              onChange={(e) => setNetworksOut(e.target.value)}
              disabled={busy}
            />
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            startIcon={<UploadFileIcon />}
            disabled={busy}
            onClick={() =>
              submitJob("/v1/jobs/xlights/import_networks", {
                networks_file: networksFile,
                out_file: networksOut,
                show_name: networksName,
              })
            }
          >
            Import networks (job)
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Sequence Timing Import (.xsq)</Typography>
          <Typography variant="body2" color="text.secondary">
            Extracts a beat/timing grid from an xLights <code>.xsq</code> (no
            xLights effect import).
          </Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <TextField
              label="XSQ file (relative to DATA_DIR)"
              value={xsqFile}
              onChange={(e) => setXsqFile(e.target.value)}
              disabled={busy}
              helperText={
                xsqCandidates.length
                  ? `Found ${xsqCandidates.length} .xsq file(s) under xlights/`
                  : "Place your .xsq under DATA_DIR/xlights/."
              }
            />
            <TextField
              label="Timing track (optional)"
              value={timingTrack}
              onChange={(e) => setTimingTrack(e.target.value)}
              disabled={busy}
            />
            <TextField
              label="Output beats file"
              value={xsqOut}
              onChange={(e) => setXsqOut(e.target.value)}
              disabled={busy}
            />
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            startIcon={<UploadFileIcon />}
            disabled={busy}
            onClick={() =>
              submitJob("/v1/jobs/xlights/import_sequence", {
                xsq_file: xsqFile,
                timing_track: timingTrack || null,
                out_file: xsqOut,
              })
            }
          >
            Import timing grid (job)
          </Button>
        </CardActions>
      </Card>
    </Stack>
  );
}
