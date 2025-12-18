import MicIcon from "@mui/icons-material/Mic";
import MicOffIcon from "@mui/icons-material/MicOff";
import { IconButton, Tooltip } from "@mui/material";
import React, { useEffect, useMemo, useRef, useState } from "react";

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
}: {
  onText: (text: string) => void;
  disabled?: boolean;
}) {
  const ctor = useMemo(() => getSpeechRecognitionCtor(), []);
  const recRef = useRef<SpeechRecognition | null>(null);
  const [listening, setListening] = useState(false);

  useEffect(() => {
    return () => {
      try {
        recRef.current?.stop();
      } catch {
        // ignore
      }
    };
  }, []);

  const start = () => {
    if (!ctor) return;
    const rec = new ctor();
    recRef.current = rec;
    rec.continuous = false;
    rec.interimResults = true;
    rec.lang = "en-US";
    rec.onresult = (event: any) => {
      let txt = "";
      for (let i = event.resultIndex; i < event.results.length; i++) {
        txt += event.results[i]?.[0]?.transcript ?? "";
      }
      txt = txt.trim();
      if (txt) onText(txt);
    };
    rec.onerror = () => setListening(false);
    rec.onend = () => setListening(false);
    setListening(true);
    rec.start();
  };

  const stop = () => {
    try {
      recRef.current?.stop();
    } finally {
      setListening(false);
    }
  };

  const supported = Boolean(ctor);
  const title = supported
    ? listening
      ? "Stop listening"
      : "Voice input"
    : "Voice input not supported by this browser";

  return (
    <Tooltip title={title}>
      <span>
        <IconButton
          color={listening ? "error" : "primary"}
          onClick={listening ? stop : start}
          disabled={disabled || !supported}
        >
          {listening ? <MicOffIcon /> : <MicIcon />}
        </IconButton>
      </span>
    </Tooltip>
  );
}
