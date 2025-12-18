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

  const [uploadLocalFile, setUploadLocalFile] = useState("fseq/export.fseq");
  const [uploadDir, setUploadDir] = useState("sequences");
  const [uploadSubdir, setUploadSubdir] = useState("");
  const [uploadDestFilename, setUploadDestFilename] = useState("");

  const fppEnabled = useMemo(() => Boolean(config?.fpp_enabled), [config]);

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
