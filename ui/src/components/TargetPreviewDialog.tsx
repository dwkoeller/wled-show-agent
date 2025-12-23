import {
  Alert,
  Button,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  LinearProgress,
  Stack,
  Typography,
} from "@mui/material";
import React, { useEffect, useMemo, useState } from "react";
import { api } from "../api";

type FleetResolveResponse = {
  ok: boolean;
  targets: string[];
  discovery_enabled: boolean;
  stale_after_s: number;
  resolved: Array<{
    name: string;
    base_url: string;
    source: string;
    matched_by: string[];
    agent: {
      agent_id: string | null;
      name: string | null;
      role: string | null;
      controller_kind: string | null;
      version: string | null;
      base_url: string | null;
      tags: string[] | null;
      updated_at: number | null;
      started_at: number | null;
      age_s: number | null;
      online: boolean | null;
    };
  }>;
  unresolved: Array<{ target: string; reason: string }>;
};

export function TargetPreviewDialog(props: {
  open: boolean;
  title?: string;
  targets: string[] | null;
  onClose: () => void;
}) {
  const { open, onClose, targets, title } = props;
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<FleetResolveResponse | null>(null);

  const targetsKey = useMemo(
    () => (targets ?? []).map(String).join(","),
    [targets],
  );

  useEffect(() => {
    if (!open) return;
    setBusy(true);
    setError(null);
    setData(null);
    void api<FleetResolveResponse>("/v1/fleet/resolve", {
      method: "POST",
      json: { targets: targets ?? null },
    })
      .then((r) => setData(r))
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setBusy(false));
  }, [open, targetsKey, targets]);

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>{title ?? "Target preview"}</DialogTitle>
      <DialogContent>
        {busy ? <LinearProgress sx={{ mb: 2 }} /> : null}
        {error ? <Alert severity="error">{error}</Alert> : null}

        {data ? (
          <Stack spacing={2} sx={{ mt: 2 }}>
            <Typography variant="body2" color="text.secondary">
              Discovery:{" "}
              <code>{data.discovery_enabled ? "enabled" : "disabled"}</code>{" "}
              (stale after <code>{Math.round(data.stale_after_s)}</code>s)
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Targets:{" "}
              <code>
                {data.targets?.length ? data.targets.join(", ") : "(default)"}
              </code>
            </Typography>

            <Stack spacing={1}>
              <Typography variant="subtitle1">Resolved</Typography>
              {data.resolved?.length ? (
                data.resolved.map((p) => {
                  const online = p.agent?.online;
                  const age = p.agent?.age_s;
                  return (
                    <Stack
                      key={p.base_url}
                      spacing={0.5}
                      sx={{
                        border: "1px solid rgba(255,255,255,0.12)",
                        borderRadius: 1,
                        p: 1,
                      }}
                    >
                      <Stack
                        direction="row"
                        spacing={1}
                        sx={{ alignItems: "center", flexWrap: "wrap" }}
                      >
                        <Typography variant="body2">
                          <code>{p.name}</code>
                        </Typography>
                        <Chip
                          size="small"
                          label={p.source}
                          variant="outlined"
                        />
                        <Chip
                          size="small"
                          color={
                            online === true
                              ? "success"
                              : online === false
                                ? "error"
                                : "default"
                          }
                          label={
                            online === true
                              ? "online"
                              : online === false
                                ? "offline"
                                : "unknown"
                          }
                          variant="outlined"
                        />
                        <Typography variant="body2" color="text.secondary">
                          <code>{p.base_url}</code>
                        </Typography>
                      </Stack>

                      <Typography variant="body2" color="text.secondary">
                        {p.agent?.role ? (
                          <>
                            role=<code>{p.agent.role}</code>{" "}
                          </>
                        ) : null}
                        {p.agent?.agent_id ? (
                          <>
                            id=<code>{p.agent.agent_id}</code>{" "}
                          </>
                        ) : null}
                        {typeof age === "number" ? (
                          <>
                            age_s=<code>{Math.round(age)}</code>{" "}
                          </>
                        ) : null}
                        {p.agent?.tags?.length ? (
                          <>
                            tags=<code>{p.agent.tags.join(",")}</code>{" "}
                          </>
                        ) : null}
                      </Typography>

                      {p.matched_by?.length ? (
                        <Typography variant="body2" color="text.secondary">
                          matched_by=<code>{p.matched_by.join(", ")}</code>
                        </Typography>
                      ) : null}
                    </Stack>
                  );
                })
              ) : (
                <Typography variant="body2">No peers resolved.</Typography>
              )}
            </Stack>

            {data.unresolved?.length ? (
              <Stack spacing={1}>
                <Typography variant="subtitle1">Unresolved</Typography>
                <Alert severity="warning">
                  <Stack spacing={0.5}>
                    {data.unresolved.map((u) => (
                      <Typography
                        key={`${u.target}:${u.reason}`}
                        variant="body2"
                      >
                        <code>{u.target}</code> – {u.reason}
                      </Typography>
                    ))}
                  </Stack>
                </Alert>
              </Stack>
            ) : null}
          </Stack>
        ) : null}
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
      </DialogActions>
    </Dialog>
  );
}
