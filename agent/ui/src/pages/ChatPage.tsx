import SendIcon from "@mui/icons-material/Send";
import {
  Alert,
  Box,
  Button,
  Card,
  CardActions,
  CardContent,
  Stack,
  TextField,
  Typography,
} from "@mui/material";
import React, { useState } from "react";
import { api } from "../api";
import { useAuth } from "../auth";
import { VoiceInputButton } from "../components/VoiceInputButton";

export function ChatPage() {
  const { config } = useAuth();
  const [text, setText] = useState("");
  const [out, setOut] = useState<unknown>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const send = async () => {
    setBusy(true);
    setError(null);
    try {
      const res = await api("/v1/command", { method: "POST", json: { text } });
      setOut(res);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Stack spacing={2}>
      {error ? <Alert severity="error">{error}</Alert> : null}
      <Card>
        <CardContent>
          <Typography variant="h6">Chat</Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
            Uses <code>/v1/command</code>.{" "}
            {config?.openai_enabled
              ? "OpenAI enabled."
              : "OpenAI not configured; local commands only."}
          </Typography>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <TextField
              label="Command"
              placeholder="e.g. make it candy cane and a bit brighter"
              value={text}
              onChange={(e) => setText(e.target.value)}
              multiline
              minRows={3}
              disabled={busy}
            />
          </Stack>
        </CardContent>
        <CardActions>
          <VoiceInputButton
            onText={(t) => setText((prev) => (prev ? `${prev} ${t}` : t))}
            disabled={busy}
          />
          <Button
            variant="contained"
            startIcon={<SendIcon />}
            onClick={send}
            disabled={busy || !text.trim()}
          >
            Send
          </Button>
        </CardActions>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6">Response</Typography>
          <Box
            component="pre"
            sx={{
              whiteSpace: "pre-wrap",
              wordBreak: "break-word",
              fontSize: 12,
              mt: 1,
            }}
          >
            {out ? JSON.stringify(out, null, 2) : "—"}
          </Box>
        </CardContent>
      </Card>
    </Stack>
  );
}
