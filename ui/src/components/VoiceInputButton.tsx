import MicIcon from "@mui/icons-material/Mic";
import MicOffIcon from "@mui/icons-material/MicOff";
import { IconButton, Tooltip } from "@mui/material";
import React, { useEffect, useMemo, useRef, useState } from "react";
import { csrfHeaders } from "../api";

type SpeechRecognition = {
  continuous: boolean;
  interimResults: boolean;
  lang: string;
  onresult: ((event: any) => void) | null;
  onerror: ((event: any) => void) | null;
  onend: ((event: any) => void) | null;
  start: () => void;
  stop: () => void;
};

type SpeechRecognitionCtor = new () => SpeechRecognition;

function getSpeechRecognitionCtor(): SpeechRecognitionCtor | null {
  const w = window as unknown as {
    SpeechRecognition?: SpeechRecognitionCtor;
    webkitSpeechRecognition?: SpeechRecognitionCtor;
  };
  return w.SpeechRecognition ?? w.webkitSpeechRecognition ?? null;
}

export function VoiceInputButton({
  onText,
  disabled,
  onError,
  mode = "browser",
  language,
  prompt,
  onIntent,
}: {
  onText: (text: string) => void;
  disabled?: boolean;
  onError?: (message: string) => void;
  mode?: "browser" | "server_transcribe" | "server_intent";
  language?: string;
  prompt?: string;
  onIntent?: (text: string, result: unknown) => void;
}) {
  const ctor = useMemo(() => getSpeechRecognitionCtor(), []);
  const canRecord =
    typeof window !== "undefined" &&
    typeof window.MediaRecorder !== "undefined" &&
    typeof navigator !== "undefined" &&
    Boolean(navigator.mediaDevices?.getUserMedia);
  const recRef = useRef<SpeechRecognition | null>(null);
  const recorderRef = useRef<MediaRecorder | null>(null);
  const streamRef = useRef<MediaStream | null>(null);
  const chunksRef = useRef<Blob[]>([]);
  const [listening, setListening] = useState(false);
  const [processing, setProcessing] = useState(false);
  const canBrowser = Boolean(ctor);
  const canServer = canRecord;
  const wantsBrowser = mode === "browser";
  let useBrowser = false;
  let useServer = false;
  if (wantsBrowser) {
    if (canBrowser) useBrowser = true;
    else if (canServer) useServer = true;
  } else {
    if (canServer) useServer = true;
    else if (canBrowser) useBrowser = true;
  }
  const supported = useBrowser || useServer;
  const intentMode = mode === "server_intent";

  useEffect(() => {
    return () => {
      try {
        recRef.current?.stop();
      } catch {
        // ignore
      }
      try {
        recorderRef.current?.stop();
      } catch {
        // ignore
      }
      if (streamRef.current) {
        for (const track of streamRef.current.getTracks()) {
          try {
            track.stop();
          } catch {
            // ignore
          }
        }
      }
    };
  }, []);

  const handleError = (message: string) => {
    if (onError) onError(message);
  };

  const startSpeech = () => {
    if (!ctor) return;
    const rec = new ctor();
    recRef.current = rec;
    rec.continuous = false;
    rec.interimResults = true;
    const lang = (language || "").trim();
    const fallback =
      typeof navigator !== "undefined" ? navigator.language || "en-US" : "en-US";
    rec.lang = lang || fallback;
    rec.onresult = (event: any) => {
      let txt = "";
      for (let i = event.resultIndex; i < event.results.length; i++) {
        txt += event.results[i]?.[0]?.transcript ?? "";
      }
      txt = txt.trim();
      if (txt) onText(txt);
    };
    rec.onerror = () => {
      setListening(false);
      handleError("Speech recognition failed.");
    };
    rec.onend = () => setListening(false);
    setListening(true);
    rec.start();
  };

  const uploadAudio = async (blob: Blob) => {
    setProcessing(true);
    try {
      const form = new FormData();
      form.append("file", blob, "speech.webm");
      const lang = (language || "").trim();
      const pr = (prompt || "").trim();
      if (lang) form.append("language", lang);
      if (pr) form.append("prompt", pr);
      const useIntent = useServer && intentMode && Boolean(onIntent);
      const endpoint = useIntent ? "/v1/voice/command" : "/v1/voice/transcribe";
      const resp = await fetch(endpoint, {
        method: "POST",
        body: form,
        credentials: "include",
        headers: csrfHeaders("POST"),
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
        handleError(String(msg));
        return;
      }
      const text =
        body && typeof body === "object" && "text" in body
          ? String((body as any).text || "")
          : "";
      if (!text.trim()) {
        handleError("Transcription returned empty text.");
        return;
      }
      if (useIntent && onIntent) {
        const command =
          body && typeof body === "object" && "command" in body
            ? (body as any).command
            : null;
        if (command === null) {
          handleError("Voice command returned no result.");
          return;
        }
        onIntent(text.trim(), command);
        return;
      }
      onText(text.trim());
    } catch (e) {
      handleError(e instanceof Error ? e.message : String(e));
    } finally {
      setProcessing(false);
    }
  };

  const startRecording = async () => {
    if (!canRecord) return;
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      streamRef.current = stream;
      const preferred = "audio/webm;codecs=opus";
      const opts = MediaRecorder.isTypeSupported(preferred)
        ? { mimeType: preferred }
        : undefined;
      const rec = new MediaRecorder(stream, opts);
      recorderRef.current = rec;
      chunksRef.current = [];
      rec.ondataavailable = (ev) => {
        if (ev.data && ev.data.size > 0) {
          chunksRef.current.push(ev.data);
        }
      };
      rec.onerror = () => {
        handleError("Audio recording failed.");
        setListening(false);
      };
      rec.onstop = async () => {
        setListening(false);
        const blob = new Blob(chunksRef.current, {
          type: opts?.mimeType || rec.mimeType || "audio/webm",
        });
        if (streamRef.current) {
          for (const track of streamRef.current.getTracks()) {
            try {
              track.stop();
            } catch {
              // ignore
            }
          }
        }
        streamRef.current = null;
        recorderRef.current = null;
        chunksRef.current = [];
        if (blob.size > 0) {
          await uploadAudio(blob);
        } else {
          handleError("No audio captured.");
        }
      };
      setListening(true);
      rec.start();
    } catch (e) {
      handleError(e instanceof Error ? e.message : String(e));
    }
  };

  const stop = () => {
    try {
      recRef.current?.stop();
    } catch {
      // ignore
    }
    try {
      recorderRef.current?.stop();
    } catch {
      // ignore
    }
    setListening(false);
  };

  const serverLabel = intentMode
    ? "Voice input (server intent)"
    : "Voice input (server transcription)";
  const title = supported
    ? processing
      ? "Transcribing..."
      : listening
        ? "Stop listening"
        : useBrowser
          ? "Voice input (browser)"
          : serverLabel
    : "Voice input not supported by this browser";

  return (
    <Tooltip title={title}>
      <span>
        <IconButton
          color={listening ? "error" : "primary"}
          onClick={listening ? stop : useBrowser ? startSpeech : startRecording}
          disabled={disabled || processing || !supported}
        >
          {listening ? <MicOffIcon /> : <MicIcon />}
        </IconButton>
      </span>
    </Tooltip>
  );
}
