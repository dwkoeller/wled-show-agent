import DeleteIcon from "@mui/icons-material/Delete";
import DownloadIcon from "@mui/icons-material/Download";
import RefreshIcon from "@mui/icons-material/Refresh";
import UploadFileIcon from "@mui/icons-material/UploadFile";
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
import { api } from "../../api";

const commonDirs = [
  { label: "(root)", value: "" },
  { label: "music/", value: "music" },
  { label: "xlights/", value: "xlights" },
  { label: "audio/", value: "audio" },
  { label: "looks/", value: "looks" },
  { label: "sequences/", value: "sequences" },
  { label: "fseq/", value: "fseq" },
  { label: "show/", value: "show" },
];

type UploadResult = { ok?: boolean; path?: string; bytes?: number };

export function FilesTools() {
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [files, setFiles] = useState<string[]>([]);

  const [dir, setDir] = useState("music");
  const [glob, setGlob] = useState("*");
  const [recursive, setRecursive] = useState(true);

  const [uploadDir, setUploadDir] = useState("music");
  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [uploadName, setUploadName] = useState("");
  const [overwrite, setOverwrite] = useState(false);
  const [lastUpload, setLastUpload] = useState<UploadResult | null>(null);

  const uploadPath = useMemo(() => {
    const name = uploadName.trim();
    if (!name) return "";
    const d = uploadDir.trim();
    return d ? `${d.replace(/\/+$/, "")}/${name.replace(/^\/+/, "")}` : name;
  }, [uploadDir, uploadName]);

  const refresh = async () => {
    setError(null);
    setBusy(true);
    try {
      const q = new URLSearchParams();
      q.set("dir", dir);
      q.set("glob", glob.trim() || "*");
      q.set("recursive", recursive ? "true" : "false");
      q.set("limit", "500");
      const res = await api<{ ok: boolean; files: string[] }>(
        `/v1/files/list?${q.toString()}`,
        { method: "GET" },
      );
      setFiles(res.files || []);
    } catch (e) {
      setFiles([]);
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  useEffect(() => {
    void refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const doUpload = async () => {
    if (!uploadFile) return;
    if (!uploadPath.trim()) {
      setError("Choose a destination filename.");
      return;
    }
    setBusy(true);
    setError(null);
    setLastUpload(null);
    try {
      const url = new URL("/v1/files/upload", window.location.origin);
      url.searchParams.set("path", uploadPath);
      if (overwrite) url.searchParams.set("overwrite", "true");

      const resp = await fetch(url.toString(), {
        method: "PUT",
        credentials: "include",
        headers: {
          "Content-Type": uploadFile.type || "application/octet-stream",
        },
        body: uploadFile,
      });

      const contentType = resp.headers.get("content-type") ?? "";
      const body = contentType.includes("application/json")
        ? await resp.json().catch(() => null)
        : await resp.text().catch(() => "");

      if (!resp.ok) {
        const msg =
          (body &&
            typeof body === "object" &&
            (body.detail || body.error || body.message)) ||
          (typeof body === "string" && body.trim()) ||
          `HTTP ${resp.status}`;
        throw new Error(String(msg));
      }

      setLastUpload(body as UploadResult);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const doDelete = async (path: string) => {
    if (!window.confirm(`Delete ${path}?`)) return;
    setBusy(true);
    setError(null);
    try {
      await api(`/v1/files/delete?path=${encodeURIComponent(path)}`, {
        method: "DELETE",
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
      {error ? <Alert severity="error">{error}</Alert> : null}
      {lastUpload?.path ? (
        <Alert severity="success">
          Uploaded <code>{lastUpload.path}</code> ({lastUpload.bytes ?? 0}{" "}
          bytes)
        </Alert>
      ) : null}

      <Card>
        <CardContent>
          <Typography variant="h6">Upload</Typography>
          <Typography variant="body2" color="text.secondary">
            Uploads a file into <code>DATA_DIR</code> (coordinator container
            volume).
          </Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <Button
              variant="outlined"
              component="label"
              startIcon={<UploadFileIcon />}
              disabled={busy}
            >
              Choose file
              <input
                hidden
                type="file"
                onChange={(e) => {
                  const f = e.target.files?.[0] ?? null;
                  setUploadFile(f);
                  setLastUpload(null);
                  if (f && !uploadName.trim()) setUploadName(f.name);
                }}
              />
            </Button>
            <Typography variant="body2" color="text.secondary">
              {uploadFile ? (
                <>
                  Selected: <code>{uploadFile.name}</code> ({uploadFile.size}{" "}
                  bytes)
                </>
              ) : (
                "No file selected."
              )}
            </Typography>

            <FormControl fullWidth>
              <InputLabel>Destination dir</InputLabel>
              <Select
                value={uploadDir}
                label="Destination dir"
                onChange={(e) => setUploadDir(String(e.target.value))}
                disabled={busy}
              >
                {commonDirs.map((d) => (
                  <MenuItem key={d.value} value={d.value}>
                    {d.label}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            <TextField
              label="Destination filename"
              value={uploadName}
              onChange={(e) => setUploadName(e.target.value)}
              disabled={busy}
              helperText={uploadPath ? `Will write: ${uploadPath}` : undefined}
            />

            <FormControlLabel
              control={
                <Switch
                  checked={overwrite}
                  onChange={(e) => setOverwrite(e.target.checked)}
                />
              }
              label="Overwrite if exists"
            />
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            startIcon={<UploadFileIcon />}
            onClick={doUpload}
            disabled={busy || !uploadFile || !uploadPath.trim()}
          >
            Upload
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Browse</Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <FormControl fullWidth>
              <InputLabel>Dir</InputLabel>
              <Select
                value={dir}
                label="Dir"
                onChange={(e) => setDir(String(e.target.value))}
                disabled={busy}
              >
                {commonDirs.map((d) => (
                  <MenuItem key={d.value} value={d.value}>
                    {d.label}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            <TextField
              label="Glob"
              value={glob}
              onChange={(e) => setGlob(e.target.value)}
              disabled={busy}
              helperText="Examples: *.wav, *.xsq, *.json, *"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={recursive}
                  onChange={(e) => setRecursive(e.target.checked)}
                />
              }
              label="Recursive"
            />
          </Stack>
        </CardContent>
        <CardActions>
          <Button startIcon={<RefreshIcon />} onClick={refresh} disabled={busy}>
            Refresh
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Files</Typography>
          <Typography variant="body2" color="text.secondary">
            Downloads are served from <code>DATA_DIR</code>.
          </Typography>
          <Stack spacing={1} sx={{ mt: 2 }}>
            {files.length ? (
              files.map((f) => (
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
                  <Button
                    size="small"
                    color="error"
                    startIcon={<DeleteIcon />}
                    onClick={() => doDelete(f)}
                    disabled={busy}
                  >
                    Delete
                  </Button>
                </Stack>
              ))
            ) : (
              <Typography variant="body2">No files found.</Typography>
            )}
          </Stack>
        </CardContent>
      </Card>
    </Stack>
  );
}
