import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import RefreshIcon from "@mui/icons-material/Refresh";
import StopIcon from "@mui/icons-material/Stop";
import UploadFileIcon from "@mui/icons-material/UploadFile";
import {
  Alert,
  Box,
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
import { api } from "../../api";
import { useAuth } from "../../auth";
import { useEventRefresh } from "../../hooks/useEventRefresh";

type Json = unknown;

export function FppTools() {
  const { config } = useAuth();
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const [status, setStatus] = useState<Json | null>(null);
  const [discover, setDiscover] = useState<Json | null>(null);
  const [playlists, setPlaylists] = useState<string[]>([]);

  const [playlistName, setPlaylistName] = useState("");
  const [playlistRepeat, setPlaylistRepeat] = useState(false);

  const [eventId, setEventId] = useState("1");
  const [eventScriptId, setEventScriptId] = useState("1");
  const [eventScriptPath, setEventScriptPath] = useState(
    "/v1/fleet/sequences/start",
  );
  const [eventScriptPayload, setEventScriptPayload] = useState(
    '{"file":"sequence_ShowMix_*.json","loop":false}',
  );
  const [eventScriptFilename, setEventScriptFilename] = useState("");
  const [eventScriptIncludeKey, setEventScriptIncludeKey] = useState(false);
  const [eventScriptResult, setEventScriptResult] = useState<Json | null>(null);

  const [uploadLocalFile, setUploadLocalFile] = useState("fseq/export.fseq");
  const [uploadDir, setUploadDir] = useState("sequences");
  const [uploadSubdir, setUploadSubdir] = useState("");
  const [uploadDestFilename, setUploadDestFilename] = useState("");

  const [syncName, setSyncName] = useState("");
  const [syncSequences, setSyncSequences] = useState("");
  const [syncRepeat, setSyncRepeat] = useState(false);
  const [syncUpload, setSyncUpload] = useState(true);
  const [syncResult, setSyncResult] = useState<Json | null>(null);
  const [importResult, setImportResult] = useState<Json | null>(null);

  const fppEnabled = useMemo(() => Boolean(config?.fpp_enabled), [config]);

  const parseSequenceList = (raw: string) =>
    raw
      .split(/[\n,]+/g)
      .map((x) => x.trim())
      .filter(Boolean);
  const syncSequenceCount = useMemo(
    () => parseSequenceList(syncSequences).length,
    [syncSequences],
  );

  const refresh = async () => {
    setBusy(true);
    setError(null);
    try {
      const out: any = {};
      try {
        out.status = await api("/v1/fpp/status", { method: "GET" });
      } catch (e) {
        out.status = {
          ok: false,
          error: e instanceof Error ? e.message : String(e),
        };
      }
      try {
        out.discover = await api("/v1/fpp/discover", { method: "GET" });
      } catch (e) {
        out.discover = {
          ok: false,
          error: e instanceof Error ? e.message : String(e),
        };
      }
      try {
        const pls = await api<{ ok: boolean; playlists: string[] }>(
          "/v1/fpp/playlists",
          { method: "GET" },
        );
        setPlaylists(pls.playlists || []);
        setPlaylistName((prev) => prev || (pls.playlists || [])[0] || "");
        out.playlists = pls;
      } catch (e) {
        setPlaylists([]);
        out.playlists = {
          ok: false,
          error: e instanceof Error ? e.message : String(e),
        };
      }
      setStatus(out.status);
      setDiscover(out.discover);
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
    types: ["fpp", "tick"],
    refresh,
    minIntervalMs: 3000,
  });

  const startPlaylist = async () => {
    setBusy(true);
    setError(null);
    try {
      await api("/v1/fpp/playlist/start", {
        method: "POST",
        json: { name: playlistName, repeat: playlistRepeat },
      });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const stopPlaylist = async () => {
    setBusy(true);
    setError(null);
    try {
      await api("/v1/fpp/playlist/stop", { method: "POST", json: {} });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const triggerEvent = async () => {
    setBusy(true);
    setError(null);
    try {
      await api("/v1/fpp/event/trigger", {
        method: "POST",
        json: { event_id: parseInt(eventId || "1", 10) },
      });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const uploadFile = async () => {
    setBusy(true);
    setError(null);
    try {
      await api("/v1/fpp/upload_file", {
        method: "POST",
        json: {
          local_file: uploadLocalFile,
          dir: uploadDir,
          subdir: uploadSubdir.trim() ? uploadSubdir.trim() : null,
          dest_filename: uploadDestFilename.trim()
            ? uploadDestFilename.trim()
            : null,
        },
      });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const syncPlaylist = async () => {
    setBusy(true);
    setError(null);
    try {
      const sequences = parseSequenceList(syncSequences);
      const res = await api("/v1/fpp/playlists/sync", {
        method: "POST",
        json: {
          name: syncName.trim(),
          sequence_files: sequences,
          repeat: syncRepeat,
          upload: syncUpload,
          write_local: true,
        },
      });
      setSyncResult(res as Json);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const importPlaylist = async () => {
    setBusy(true);
    setError(null);
    try {
      const res = await api("/v1/fpp/playlists/import", {
        method: "POST",
        json: { name: syncName.trim(), from_fpp: true, write_local: true },
      });
      setImportResult(res as Json);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const exportEventScript = async () => {
    setBusy(true);
    setError(null);
    try {
      const payload = eventScriptPayload.trim()
        ? JSON.parse(eventScriptPayload)
        : {};
      const res = await api("/v1/fpp/export/event_script", {
        method: "POST",
        json: {
          event_id: parseInt(eventScriptId || "1", 10),
          path: eventScriptPath.trim() || "/v1/fleet/sequences/start",
          payload,
          out_filename: eventScriptFilename.trim()
            ? eventScriptFilename.trim()
            : null,
          include_a2a_key: eventScriptIncludeKey,
        },
      });
      setEventScriptResult(res as Json);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Stack spacing={2}>
      {!fppEnabled ? (
        <Alert severity="info">
          FPP is not configured. Set <code>FPP_BASE_URL</code> in your agent
          env.
        </Alert>
      ) : null}
      {error ? <Alert severity="error">{error}</Alert> : null}

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
            {status ? JSON.stringify(status, null, 2) : "—"}
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
          <Typography variant="h6">Playlists</Typography>
          <Stack spacing={2} sx={{ mt: 1 }}>
            <FormControl fullWidth>
              <InputLabel>Playlist</InputLabel>
              <Select
                value={playlistName}
                label="Playlist"
                onChange={(e) => setPlaylistName(String(e.target.value))}
                disabled={busy}
              >
                {playlists.map((p) => (
                  <MenuItem key={p} value={p}>
                    {p}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            <FormControlLabel
              control={
                <Switch
                  checked={playlistRepeat}
                  onChange={(e) => setPlaylistRepeat(e.target.checked)}
                />
              }
              label="Repeat"
            />
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            startIcon={<PlayArrowIcon />}
            onClick={startPlaylist}
            disabled={busy || !playlistName.trim()}
          >
            Start
          </Button>
          <Button
            color="error"
            startIcon={<StopIcon />}
            onClick={stopPlaylist}
            disabled={busy}
          >
            Stop
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Trigger Event</Typography>
          <Stack spacing={2} sx={{ mt: 1 }}>
            <TextField
              label="Event ID"
              value={eventId}
              onChange={(e) => setEventId(e.target.value)}
              disabled={busy}
              inputMode="numeric"
            />
          </Stack>
        </CardContent>
        <CardActions>
          <Button variant="contained" onClick={triggerEvent} disabled={busy}>
            Trigger
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Export Event Script</Typography>
          <Typography variant="body2" color="text.secondary">
            Generates <code>event-&lt;id&gt;.sh</code> under{" "}
            <code>DATA_DIR/fpp/scripts</code>.
          </Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <TextField
              label="Event ID"
              value={eventScriptId}
              onChange={(e) => setEventScriptId(e.target.value)}
              disabled={busy}
              inputMode="numeric"
            />
            <TextField
              label="Coordinator path"
              value={eventScriptPath}
              onChange={(e) => setEventScriptPath(e.target.value)}
              disabled={busy}
            />
            <TextField
              label="Payload JSON"
              value={eventScriptPayload}
              onChange={(e) => setEventScriptPayload(e.target.value)}
              disabled={busy}
              multiline
              minRows={3}
            />
            <TextField
              label="Output filename (optional)"
              value={eventScriptFilename}
              onChange={(e) => setEventScriptFilename(e.target.value)}
              disabled={busy}
            />
            <FormControlLabel
              control={
                <Switch
                  checked={eventScriptIncludeKey}
                  onChange={(e) => setEventScriptIncludeKey(e.target.checked)}
                />
              }
              label="Include A2A key"
            />
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            onClick={exportEventScript}
            disabled={busy || !eventScriptId.trim()}
          >
            Export script
          </Button>
        </CardActions>
        {Boolean(eventScriptResult) && (
          <CardContent>
            <Typography variant="subtitle2">Result</Typography>
            <Box
              component="pre"
              sx={{
                whiteSpace: "pre-wrap",
                wordBreak: "break-word",
                fontSize: 12,
                mt: 1,
              }}
            >
              {JSON.stringify(eventScriptResult, null, 2)}
            </Box>
          </CardContent>
        )}
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Upload File to FPP</Typography>
          <Typography variant="body2" color="text.secondary">
            Uploads a local file under <code>DATA_DIR</code> to FPP (e.g. an
            exported <code>.fseq</code>).
          </Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <TextField
              label="Local file (relative to DATA_DIR)"
              value={uploadLocalFile}
              onChange={(e) => setUploadLocalFile(e.target.value)}
              disabled={busy}
            />
            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <TextField
                label="FPP dir"
                value={uploadDir}
                onChange={(e) => setUploadDir(e.target.value)}
                disabled={busy}
                fullWidth
              />
              <TextField
                label="FPP subdir (optional)"
                value={uploadSubdir}
                onChange={(e) => setUploadSubdir(e.target.value)}
                disabled={busy}
                fullWidth
              />
            </Stack>
            <TextField
              label="Destination filename (optional)"
              value={uploadDestFilename}
              onChange={(e) => setUploadDestFilename(e.target.value)}
              disabled={busy}
            />
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            startIcon={<UploadFileIcon />}
            onClick={uploadFile}
            disabled={busy || !uploadLocalFile.trim()}
          >
            Upload
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Playlist Sync / Import</Typography>
          <Typography variant="body2" color="text.secondary">
            Build an FPP playlist from sequence filenames and sync it to the
            FPP host. You can also import an existing playlist from FPP.
          </Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <TextField
              label="Playlist name"
              value={syncName}
              onChange={(e) => setSyncName(e.target.value)}
              disabled={busy}
            />
            <TextField
              label="Sequence filenames (one per line or comma-separated)"
              value={syncSequences}
              onChange={(e) => setSyncSequences(e.target.value)}
              disabled={busy}
              multiline
              minRows={3}
            />
            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <FormControlLabel
                control={
                  <Switch
                    checked={syncRepeat}
                    onChange={(e) => setSyncRepeat(e.target.checked)}
                  />
                }
                label="Repeat"
              />
              <FormControlLabel
                control={
                  <Switch
                    checked={syncUpload}
                    onChange={(e) => setSyncUpload(e.target.checked)}
                  />
                }
                label="Upload to FPP"
              />
            </Stack>
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            onClick={syncPlaylist}
            disabled={busy || !syncName.trim() || syncSequenceCount === 0}
          >
            Sync playlist
          </Button>
          <Button
            variant="outlined"
            onClick={importPlaylist}
            disabled={busy || !syncName.trim()}
          >
            Import playlist
          </Button>
        </CardActions>
        {Boolean(syncResult || importResult) && (
          <CardContent>
            <Typography variant="subtitle2">Result</Typography>
            <Box
              component="pre"
              sx={{
                whiteSpace: "pre-wrap",
                wordBreak: "break-word",
                fontSize: 12,
                mt: 1,
              }}
            >
              {JSON.stringify(syncResult ?? importResult, null, 2)}
            </Box>
          </CardContent>
        )}
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Discovery</Typography>
          <Box
            component="pre"
            sx={{
              whiteSpace: "pre-wrap",
              wordBreak: "break-word",
              fontSize: 12,
              mt: 1,
            }}
          >
            {discover ? JSON.stringify(discover, null, 2) : "—"}
          </Box>
        </CardContent>
      </Card>
    </Stack>
  );
}
