import BackupIcon from "@mui/icons-material/Backup";
import RestoreIcon from "@mui/icons-material/Restore";
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
import React, { useState } from "react";
import { csrfHeaders } from "../../api";

type ImportResult = {
  ok?: boolean;
  db?: Record<string, unknown> | null;
  data?: Record<string, unknown> | null;
  warnings?: string[];
  detail?: string;
  error?: string;
};

function parseGlobs(raw: string): string[] {
  return raw
    .split(/[\n,]+/)
    .map((v) => v.trim())
    .filter(Boolean);
}

export function BackupTools() {
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [includeDb, setIncludeDb] = useState(true);
  const [includeData, setIncludeData] = useState(true);
  const [includeAuth, setIncludeAuth] = useState(false);
  const [exportExclude, setExportExclude] = useState("");

  const [restoreDb, setRestoreDb] = useState(true);
  const [restoreData, setRestoreData] = useState(true);
  const [restoreAuth, setRestoreAuth] = useState(false);
  const [dbMode, setDbMode] = useState<"merge" | "replace">("merge");
  const [overwriteData, setOverwriteData] = useState(false);
  const [importFile, setImportFile] = useState<File | null>(null);
  const [importResult, setImportResult] = useState<ImportResult | null>(null);
  const [importExclude, setImportExclude] = useState("");

  const downloadBackup = async () => {
    setBusy(true);
    setError(null);
    try {
      const url = new URL("/v1/backup/export", window.location.origin);
      url.searchParams.set("include_db", includeDb ? "true" : "false");
      url.searchParams.set("include_data", includeData ? "true" : "false");
      url.searchParams.set("include_auth", includeAuth ? "true" : "false");
      for (const glob of parseGlobs(exportExclude)) {
        url.searchParams.append("exclude_globs", glob);
      }
      const resp = await fetch(url.toString(), {
        method: "GET",
        credentials: "include",
        headers: csrfHeaders("GET"),
      });
      if (!resp.ok) {
        const text = await resp.text().catch(() => "");
        throw new Error(text || `HTTP ${resp.status}`);
      }
      const blob = await resp.blob();
      const href = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = href;
      link.download = `wsa_backup_${Date.now()}.zip`;
      link.click();
      URL.revokeObjectURL(href);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const doImport = async () => {
    if (!importFile) return;
    setBusy(true);
    setError(null);
    setImportResult(null);
    try {
      const url = new URL("/v1/backup/import", window.location.origin);
      url.searchParams.set("restore_db", restoreDb ? "true" : "false");
      url.searchParams.set("restore_data", restoreData ? "true" : "false");
      url.searchParams.set("restore_auth", restoreAuth ? "true" : "false");
      url.searchParams.set("db_mode", dbMode);
      url.searchParams.set("overwrite_data", overwriteData ? "true" : "false");
      for (const glob of parseGlobs(importExclude)) {
        url.searchParams.append("exclude_globs", glob);
      }

      const form = new FormData();
      form.append("file", importFile, importFile.name);
      const resp = await fetch(url.toString(), {
        method: "POST",
        credentials: "include",
        headers: csrfHeaders("POST"),
        body: form,
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
      setImportResult(body as ImportResult);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Stack spacing={2}>
      {error ? <Alert severity="error">{error}</Alert> : null}
      {importResult?.warnings && importResult.warnings.length ? (
        <Alert severity="warning">
          {importResult.warnings.map((warn) => (
            <div key={warn}>{warn}</div>
          ))}
        </Alert>
      ) : null}
      {importResult?.ok ? (
        <Alert severity="success">Restore completed successfully.</Alert>
      ) : null}

      <Card>
        <CardContent>
          <Typography variant="h6">Backup</Typography>
          <Typography variant="body2" color="text.secondary">
            Export DB rows and/or <code>DATA_DIR</code> files into a zip
            archive.
          </Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <FormControlLabel
              control={
                <Switch
                  checked={includeDb}
                  onChange={(e) => setIncludeDb(e.target.checked)}
                />
              }
              label="Include database tables"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={includeAuth}
                  onChange={(e) => setIncludeAuth(e.target.checked)}
                />
              }
              label="Include auth tables (users, API keys, sessions)"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={includeData}
                  onChange={(e) => setIncludeData(e.target.checked)}
                />
              }
              label="Include DATA_DIR files"
            />
            <TextField
              label="Exclude globs (comma or newline)"
              value={exportExclude}
              onChange={(e) => setExportExclude(e.target.value)}
              placeholder="**/*.tmp, **/.DS_Store"
              helperText="Applies to DATA_DIR exports only."
              disabled={busy}
            />
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            startIcon={<BackupIcon />}
            onClick={downloadBackup}
            disabled={busy || (!includeDb && !includeData)}
          >
            Download backup
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Restore</Typography>
          <Typography variant="body2" color="text.secondary">
            Restore DB rows and/or files from a backup zip.
          </Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <Button variant="outlined" component="label" disabled={busy}>
              Choose backup zip
              <input
                hidden
                type="file"
                accept=".zip,application/zip"
                onChange={(e) => setImportFile(e.target.files?.[0] ?? null)}
              />
            </Button>
            <Typography variant="body2" color="text.secondary">
              {importFile ? (
                <>
                  Selected: <code>{importFile.name}</code> ({importFile.size}{" "}
                  bytes)
                </>
              ) : (
                "No backup selected."
              )}
            </Typography>
            <FormControlLabel
              control={
                <Switch
                  checked={restoreDb}
                  onChange={(e) => setRestoreDb(e.target.checked)}
                />
              }
              label="Restore database tables"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={restoreAuth}
                  onChange={(e) => setRestoreAuth(e.target.checked)}
                />
              }
              label="Restore auth tables"
            />
            <FormControl fullWidth>
              <InputLabel>DB mode</InputLabel>
              <Select
                value={dbMode}
                label="DB mode"
                onChange={(e) => setDbMode(e.target.value as "merge" | "replace")}
              >
                <MenuItem value="merge">merge (upsert)</MenuItem>
                <MenuItem value="replace">replace (truncate + insert)</MenuItem>
              </Select>
            </FormControl>
            <FormControlLabel
              control={
                <Switch
                  checked={restoreData}
                  onChange={(e) => setRestoreData(e.target.checked)}
                />
              }
              label="Restore DATA_DIR files"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={overwriteData}
                  onChange={(e) => setOverwriteData(e.target.checked)}
                />
              }
              label="Overwrite existing files"
            />
            <TextField
              label="Exclude globs (comma or newline)"
              value={importExclude}
              onChange={(e) => setImportExclude(e.target.value)}
              placeholder="music/**, **/*.bak"
              helperText="Skipped paths are not restored."
              disabled={busy}
            />
            {importResult ? (
              <TextField
                label="Restore result"
                value={JSON.stringify(importResult, null, 2)}
                multiline
                minRows={4}
                disabled
              />
            ) : null}
          </Stack>
        </CardContent>
        <CardActions>
          <Button
            variant="contained"
            startIcon={<RestoreIcon />}
            onClick={doImport}
            disabled={busy || !importFile || (!restoreDb && !restoreData)}
          >
            Restore backup
          </Button>
        </CardActions>
      </Card>
    </Stack>
  );
}
