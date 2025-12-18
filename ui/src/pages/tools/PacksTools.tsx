import ArchiveIcon from "@mui/icons-material/Archive";
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
import React, { useMemo, useState } from "react";

type IngestResult = {
  ok?: boolean;
  dest_dir?: string;
  uploaded_bytes?: number;
  unpacked_bytes?: number;
  files?: string[];
  manifest?: string;
};

export function PacksTools() {
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const [zipFile, setZipFile] = useState<File | null>(null);
  const [destDir, setDestDir] = useState("packs");
  const [overwrite, setOverwrite] = useState(false);
  const [lastResult, setLastResult] = useState<IngestResult | null>(null);

  const effectiveDestDir = useMemo(() => {
    const d = destDir.trim().replace(/^\/+/, "");
    if (!zipFile) return d;
    const base = zipFile.name.replace(/\.zip$/i, "").trim();
    if (!base) return d;
    if (!d) return `packs/${base}`;
    return `${d.replace(/\/+$/, "")}/${base}`;
  }, [destDir, zipFile]);

  const doUpload = async () => {
    if (!zipFile) return;
    const into = effectiveDestDir.trim();
    if (!into) {
      setError("Choose a destination folder.");
      return;
    }
    setBusy(true);
    setError(null);
    setLastResult(null);
    try {
      const url = new URL("/v1/packs/ingest", window.location.origin);
      url.searchParams.set("dest_dir", into);
      if (overwrite) url.searchParams.set("overwrite", "true");

      const resp = await fetch(url.toString(), {
        method: "PUT",
        credentials: "include",
        headers: { "Content-Type": zipFile.type || "application/zip" },
        body: zipFile,
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

      setLastResult(body as IngestResult);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Stack spacing={2}>
      {error ? <Alert severity="error">{error}</Alert> : null}
      {lastResult?.dest_dir ? (
        <Alert severity="success">
          Ingested into <code>{lastResult.dest_dir}</code> (
          {lastResult.files?.length ?? 0} files)
        </Alert>
      ) : null}

      <Card>
        <CardContent>
          <Typography variant="h6">Pack ingestion</Typography>
          <Typography variant="body2" color="text.secondary">
            Upload a <code>.zip</code> and unpack it into a dedicated folder
            under <code>DATA_DIR</code>.
          </Typography>

          <Stack spacing={2} sx={{ mt: 2 }}>
            <Button
              variant="outlined"
              component="label"
              startIcon={<UploadFileIcon />}
              disabled={busy}
            >
              Choose zip
              <input
                hidden
                type="file"
                accept=".zip,application/zip"
                onChange={(e) => {
                  const f = e.target.files?.[0] ?? null;
                  setZipFile(f);
                  setLastResult(null);
                }}
              />
            </Button>
            <Typography variant="body2" color="text.secondary">
              {zipFile ? (
                <>
                  Selected: <code>{zipFile.name}</code> ({zipFile.size} bytes)
                </>
              ) : (
                "No zip selected."
              )}
            </Typography>

            <TextField
              label="Destination base folder (under DATA_DIR)"
              value={destDir}
              onChange={(e) => setDestDir(e.target.value)}
              disabled={busy}
              helperText={
                effectiveDestDir
                  ? `Will unpack into: ${effectiveDestDir}`
                  : undefined
              }
              InputProps={{ startAdornment: <ArchiveIcon sx={{ mr: 1 }} /> }}
            />

            <FormControlLabel
              control={
                <Switch
                  checked={overwrite}
                  onChange={(e) => setOverwrite(e.target.checked)}
                />
              }
              label="Overwrite destination folder if it exists"
            />
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            startIcon={<ArchiveIcon />}
            onClick={doUpload}
            disabled={busy || !zipFile || !effectiveDestDir.trim()}
          >
            Ingest pack
          </Button>
        </CardActions>
      </Card>

      {lastResult?.files?.length ? (
        <Card>
          <CardContent>
            <Typography variant="h6">Extracted files</Typography>
            <Stack spacing={0.5} sx={{ mt: 1 }}>
              {lastResult.files.slice(0, 50).map((f) => (
                <Typography key={f} variant="body2">
                  <code>{f}</code>
                </Typography>
              ))}
              {lastResult.files.length > 50 ? (
                <Typography variant="body2" color="text.secondary">
                  …and {lastResult.files.length - 50} more
                </Typography>
              ) : null}
            </Stack>
          </CardContent>
        </Card>
      ) : null}
    </Stack>
  );
}
