import SendIcon from "@mui/icons-material/Send";
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
import React, { useEffect, useState } from "react";
import { api } from "../api";
import { useAuth } from "../auth";
import { VoiceInputButton } from "../components/VoiceInputButton";

export function ChatPage() {
  const { config } = useAuth();
  const [text, setText] = useState("");
  const [out, setOut] = useState<unknown>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [autoRunVoice, setAutoRunVoice] = useState(false);
  const [voiceMode, setVoiceMode] = useState<
    "browser" | "server_transcribe" | "server_intent"
  >("browser");
  const [voiceLanguage, setVoiceLanguage] = useState("");
  const [voicePrompt, setVoicePrompt] = useState("");

  useEffect(() => {
    if (voiceMode === "server_intent" && autoRunVoice) {
      setAutoRunVoice(false);
    }
  }, [voiceMode, autoRunVoice]);

  const sendText = async (nextText: string) => {
    const payload = nextText.trim();
    if (!payload) return;
    setBusy(true);
    setError(null);
    try {
      const res = await api("/v1/command", {
        method: "POST",
        json: { text: payload },
      });
      setOut(res);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const send = async () => {
    await sendText(text);
  };

  const handleVoiceText = async (t: string) => {
    let merged = t;
    setText((prev) => {
      merged = prev.trim() ? `${prev.trim()} ${t}` : t;
      return merged;
    });
    if (autoRunVoice && !busy) {
      await sendText(merged);
    }
  };

  const handleVoiceIntent = (t: string, result: unknown) => {
    let merged = t;
    setText((prev) => {
      merged = prev.trim() ? `${prev.trim()} ${t}` : t;
      return merged;
    });
    setError(null);
    setOut(result);
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
            <Stack direction="row" spacing={2} sx={{ flexWrap: "wrap" }}>
              <FormControl size="small" sx={{ minWidth: 220 }}>
                <InputLabel id="voice-mode-label">Voice mode</InputLabel>
                <Select
                  labelId="voice-mode-label"
                  value={voiceMode}
                  label="Voice mode"
                  onChange={(e) =>
                    setVoiceMode(
                      e.target.value as
                        | "browser"
                        | "server_transcribe"
                        | "server_intent",
                    )
                  }
                  disabled={busy}
                >
                  <MenuItem value="browser">Browser speech recognition</MenuItem>
                  <MenuItem value="server_transcribe">
                    Server transcription
                  </MenuItem>
                  <MenuItem value="server_intent">Server intent (run)</MenuItem>
                </Select>
              </FormControl>
              <FormControlLabel
                control={
                  <Switch
                    checked={autoRunVoice}
                    onChange={(e) => setAutoRunVoice(e.target.checked)}
                    disabled={busy || voiceMode === "server_intent"}
                  />
                }
                label="Auto-run voice commands"
              />
            </Stack>
            {voiceMode !== "browser" ? (
              <Stack direction="row" spacing={2} sx={{ flexWrap: "wrap" }}>
                <TextField
                  label="STT language"
                  placeholder="e.g. en, en-US"
                  value={voiceLanguage}
                  onChange={(e) => setVoiceLanguage(e.target.value)}
                  disabled={busy}
                  size="small"
                />
                <TextField
                  label="STT prompt"
                  placeholder="Optional hints for transcription"
                  value={voicePrompt}
                  onChange={(e) => setVoicePrompt(e.target.value)}
                  disabled={busy}
                  size="small"
                  fullWidth
                />
              </Stack>
            ) : null}
            <Typography variant="body2" color="text.secondary">
              Auto-run is ignored for server intent mode; it runs immediately.
            </Typography>
          </Stack>
        </CardContent>
        <CardActions>
          <VoiceInputButton
            onText={handleVoiceText}
            onIntent={handleVoiceIntent}
            mode={voiceMode}
            language={voiceLanguage}
            prompt={voicePrompt}
            onError={(msg) => setError(msg)}
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
            {out ? JSON.stringify(out, null, 2) : "-"}
          </Box>
        </CardContent>
      </Card>
    </Stack>
  );
}
