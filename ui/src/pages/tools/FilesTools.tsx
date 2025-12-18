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
  LinearProgress,
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

const uploadDirs = [
  { label: "audio/", value: "audio" },
  { label: "music/", value: "music" },
  { label: "xlights/", value: "xlights" },
  { label: "sequences/", value: "sequences" },
];

const uploadAllowExts: Record<string, string[]> = {
  audio: [".wav", ".mp3", ".ogg", ".flac", ".m4a", ".aac"],
  music: [".wav", ".mp3", ".ogg", ".flac", ".m4a", ".aac"],
  xlights: [".xsq"],
  sequences: [".json"],
};

type UploadResult = { ok?: boolean; path?: string; bytes?: number };

export function FilesTools() {
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [files, setFiles] = useState<string[]>([]);

  const [dir, setDir] = useState("music");
  const [glob, setGlob] = useState("*");
  const [recursive, setRecursive] = useState(true);

  const [uploadDir, setUploadDir] = useState("audio");
  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [uploadName, setUploadName] = useState("");
  const [overwrite, setOverwrite] = useState(false);
  const [lastUpload, setLastUpload] = useState<UploadResult | null>(null);
  const [uploadProgress, setUploadProgress] = useState<number | null>(null);

  const uploadPath = useMemo(() => {
    const name = uploadName.trim() || uploadFile?.name || "";
    if (!name) return "";
    const d = uploadDir.trim();
    return d ? `${d.replace(/\/+$/, "")}/${name.replace(/^\/+/, "")}` : name;
  }, [uploadDir, uploadFile, uploadName]);

  const uploadExt = useMemo(() => {
    const n = uploadName.trim() || uploadFile?.name || "";
    const m = n.toLowerCase().match(/\.[a-z0-9]+$/);
    return m ? m[0] : "";
  }, [uploadName, uploadFile]);

  const uploadValidationError = useMemo(() => {
    const name = (uploadName.trim() || uploadFile?.name || "").trim();
    if (!name) return "Choose a file and destination filename.";
    const allowed = uploadAllowExts[uploadDir] ?? [];
    if (!uploadExt) return "Filename must include an extension.";
    if (allowed.length && !allowed.includes(uploadExt)) {
      return `File type ${uploadExt} is not allowed for ${uploadDir}/ (allowed: ${allowed.join(", ")})`;
    }
    return null;
  }, [uploadDir, uploadExt, uploadFile, uploadName]);

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
    if (uploadValidationError) {
      setError(uploadValidationError);
      return;
    }
    setBusy(true);
    setError(null);
    setLastUpload(null);
    setUploadProgress(0);
    try {
      const name = uploadName.trim() || uploadFile.name;

      const res = await new Promise<UploadResult>((resolve, reject) => {
        const form = new FormData();
        form.append("file", uploadFile, uploadFile.name);
        form.append("dir", uploadDir);
        form.append("filename", name);
        form.append("overwrite", overwrite ? "true" : "false");

        const xhr = new XMLHttpRequest();
        xhr.open("POST", "/v1/files/upload");
        xhr.withCredentials = true;

        xhr.upload.onprogress = (ev) => {
          if (!ev.lengthComputable || ev.total <= 0) {
            setUploadProgress(null);
            return;
          }
          setUploadProgress(Math.round((ev.loaded / ev.total) * 100));
        };

        xhr.onerror = () => reject(new Error("Upload failed (network error)."));
        xhr.onload = () => {
          const text = xhr.responseText ?? "";
          let body: any = null;
          try {
            body = JSON.parse(text);
          } catch {
            body = text;
          }

          if (xhr.status >= 200 && xhr.status < 300) {
            resolve(body as UploadResult);
            return;
          }

          const msg =
            (body &&
              typeof body === "object" &&
              (body.detail || body.error || body.message)) ||
            (typeof body === "string" && body.trim()) ||
            `HTTP ${xhr.status}`;
          reject(new Error(String(msg)));
        };

        xhr.send(form);
      });

      setLastUpload(res);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
      setUploadProgress(null);
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
                {uploadDirs.map((d) => (
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
              error={!!uploadValidationError}
              helperText={
                uploadValidationError
                  ? uploadValidationError
                  : uploadPath
                    ? `Will write: ${uploadPath}`
                    : undefined
              }
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

            {uploadProgress != null ? (
              <Stack spacing={0.5}>
                <LinearProgress
                  variant="determinate"
                  value={Math.max(0, Math.min(100, uploadProgress))}
                />
                <Typography variant="body2" color="text.secondary">
                  Uploading… {uploadProgress}%
                </Typography>
              </Stack>
            ) : null}
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            startIcon={<UploadFileIcon />}
            onClick={doUpload}
            disabled={busy || !uploadFile || !!uploadValidationError}
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
