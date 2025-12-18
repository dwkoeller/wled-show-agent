import CancelIcon from "@mui/icons-material/Cancel";
import RefreshIcon from "@mui/icons-material/Refresh";
import {
  Alert,
  Box,
  Button,
  Card,
  CardActions,
  CardContent,
  Chip,
  LinearProgress,
  Stack,
  Typography,
} from "@mui/material";
import React, { useEffect, useMemo, useRef, useState } from "react";
import { api } from "../api";

type Job = {
  id: string;
  kind: string;
  status: string;
  created_at: number;
  started_at: number | null;
  finished_at: number | null;
  progress: {
    current: number | null;
    total: number | null;
    message: string | null;
  };
  result: any;
  error: string | null;
  logs: string[];
  cancel_requested: boolean;
};

function fmtTs(ts: number | null): string {
  if (!ts) return "—";
  try {
    return new Date(ts * 1000).toLocaleString();
  } catch {
    return String(ts);
  }
}

function progressPct(job: Job): number | null {
  const c = job.progress?.current;
  const t = job.progress?.total;
  if (c == null || t == null || t <= 0) return null;
  return Math.max(0, Math.min(100, (c / t) * 100));
}

function outFileFromResult(job: Job): string | null {
  const r = job.result;
  if (!r) return null;
  if (typeof r === "object" && typeof r.out_file === "string")
    return r.out_file;
  if (
    typeof r === "object" &&
    typeof r.summary === "object" &&
    typeof r.summary.file === "string"
  )
    return `looks/${r.summary.file}`;
  return null;
}

export function JobsPage() {
  const [jobsById, setJobsById] = useState<Record<string, Job>>({});
  const [error, setError] = useState<string | null>(null);
  const esRef = useRef<EventSource | null>(null);
  const receivedRef = useRef(false);

  const jobs = useMemo(() => {
    const arr = Object.values(jobsById);
    arr.sort((a, b) => (b.created_at ?? 0) - (a.created_at ?? 0));
    return arr;
  }, [jobsById]);

  const refresh = async () => {
    setError(null);
    try {
      const res = await api<{ ok: boolean; jobs: Job[] }>(
        "/v1/jobs?limit=100",
        { method: "GET" },
      );
      const m: Record<string, Job> = {};
      for (const j of res.jobs || []) m[j.id] = j;
      setJobsById(m);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const cancel = async (jobId: string) => {
    try {
      await api(`/v1/jobs/${encodeURIComponent(jobId)}/cancel`, {
        method: "POST",
        json: {},
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  useEffect(() => {
    void refresh();

    const es = new EventSource("/v1/jobs/stream");
    esRef.current = es;

    const onSnapshot = (ev: Event) => {
      try {
        const msg = ev as MessageEvent;
        const parsed = JSON.parse(msg.data) as { jobs?: Job[] };
        const m: Record<string, Job> = {};
        for (const j of parsed.jobs || []) m[j.id] = j;
        receivedRef.current = true;
        setJobsById(m);
      } catch {
        // ignore
      }
    };

    const onMessage = (ev: Event) => {
      try {
        const msg = ev as MessageEvent;
        const parsed = JSON.parse(msg.data) as { type?: string; job?: Job };
        if (!parsed.job) return;
        receivedRef.current = true;
        setJobsById((prev) => ({ ...prev, [parsed.job!.id]: parsed.job! }));
      } catch {
        // ignore
      }
    };

    es.addEventListener("snapshot", onSnapshot);
    es.addEventListener("message", onMessage);
    es.onerror = () => {
      // Let the browser auto-retry; surface an error if we never got data.
      if (!receivedRef.current) setError("Job stream disconnected.");
    };

    return () => {
      try {
        es.removeEventListener("snapshot", onSnapshot);
        es.removeEventListener("message", onMessage);
        es.close();
      } catch {
        // ignore
      }
      esRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <Stack spacing={2}>
      {error ? <Alert severity="error">{error}</Alert> : null}
      <Card>
        <CardContent>
          <Typography variant="h6">Jobs</Typography>
          <Typography variant="body2" color="text.secondary">
            Long-running tasks run in the background. This page updates live via
            SSE.
          </Typography>
        </CardContent>
        <CardActions>
          <Button startIcon={<RefreshIcon />} onClick={refresh}>
            Refresh
          </Button>
        </CardActions>
      </Card>

      {jobs.length === 0 ? (
        <Card>
          <CardContent>
            <Typography>No jobs yet.</Typography>
          </CardContent>
        </Card>
      ) : null}

      {jobs.map((j) => {
        const pct = progressPct(j);
        const outFile = outFileFromResult(j);
        const canCancel = j.status === "queued" || j.status === "running";
        return (
          <Card key={j.id} variant="outlined">
            <CardContent>
              <Stack
                direction="row"
                spacing={1}
                sx={{ alignItems: "center", flexWrap: "wrap" }}
              >
                <Typography variant="subtitle1" sx={{ flexGrow: 1 }}>
                  {j.kind}
                </Typography>
                <Chip
                  size="small"
                  label={j.status}
                  color={
                    j.status === "succeeded"
                      ? "success"
                      : j.status === "failed"
                        ? "error"
                        : j.status === "canceled"
                          ? "warning"
                          : "default"
                  }
                />
              </Stack>

              {j.progress?.message ? (
                <Typography
                  variant="body2"
                  color="text.secondary"
                  sx={{ mt: 0.5 }}
                >
                  {j.progress.message}
                </Typography>
              ) : null}

              {pct != null ? (
                <Box sx={{ mt: 1 }}>
                  <LinearProgress variant="determinate" value={pct} />
                  <Typography variant="caption" color="text.secondary">
                    {Math.round(pct)}% ({j.progress.current}/{j.progress.total})
                  </Typography>
                </Box>
              ) : j.status === "running" ? (
                <Box sx={{ mt: 1 }}>
                  <LinearProgress />
                </Box>
              ) : null}

              <Typography
                variant="caption"
                color="text.secondary"
                sx={{ display: "block", mt: 1 }}
              >
                Created: {fmtTs(j.created_at)} · Started: {fmtTs(j.started_at)}{" "}
                · Finished: {fmtTs(j.finished_at)}
              </Typography>

              {j.error ? (
                <Alert
                  severity={j.status === "canceled" ? "warning" : "error"}
                  sx={{ mt: 1 }}
                >
                  {j.error}
                </Alert>
              ) : null}

              {j.result ? (
                <Box
                  component="pre"
                  sx={{
                    whiteSpace: "pre-wrap",
                    wordBreak: "break-word",
                    fontSize: 12,
                    mt: 1,
                  }}
                >
                  {JSON.stringify(j.result, null, 2)}
                </Box>
              ) : null}
            </CardContent>
            <CardActions>
              {outFile ? (
                <Button
                  component="a"
                  href={`/v1/files/download?path=${encodeURIComponent(outFile)}`}
                  target="_blank"
                  rel="noreferrer"
                >
                  Download output
                </Button>
              ) : null}
              {canCancel ? (
                <Button
                  color="warning"
                  startIcon={<CancelIcon />}
                  onClick={() => cancel(j.id)}
                >
                  Cancel
                </Button>
              ) : null}
            </CardActions>
          </Card>
        );
      })}
    </Stack>
  );
}
