import React from "react";

type ServerEvent = {
  id?: number;
  type: string;
  data?: unknown;
  ts?: number;
};

type EventsStatus = {
  connected: boolean;
  lastEventId: number | null;
  lastEventAt: number | null;
  lastErrorAt: number | null;
  errorCount: number;
};

type UseFilteredEventsOptions = {
  types?: string[];
  events?: string[];
  enabled?: boolean;
};

const eventsEnabled = (() => {
  if (typeof window === "undefined") return false;
  if (typeof window.EventSource === "undefined") return false;
  try {
    if ((navigator as any)?.webdriver) return false;
  } catch {
    // ignore
  }
  const flag = (import.meta as any)?.env?.VITE_DISABLE_EVENTS;
  return !(flag === "1" || flag === "true");
})();

function normalize(values?: string[]): string[] {
  if (!values || values.length === 0) return [];
  const out: string[] = [];
  values.forEach((raw) => {
    const trimmed = String(raw || "")
      .split(",")
      .map((part) => part.trim())
      .filter(Boolean);
    out.push(...trimmed);
  });
  return Array.from(new Set(out));
}

export function useFilteredEvents(options: UseFilteredEventsOptions = {}) {
  const enabled = Boolean(options.enabled ?? true) && eventsEnabled;
  const types = React.useMemo(() => normalize(options.types), [options.types]);
  const events = React.useMemo(() => normalize(options.events), [options.events]);
  const key = `${types.join(",")}::${events.join(",")}`;
  const lastEventIdRef = React.useRef<number | null>(null);

  const [event, setEvent] = React.useState<ServerEvent | null>(null);
  const [status, setStatus] = React.useState<EventsStatus>({
    connected: false,
    lastEventId: null,
    lastEventAt: null,
    lastErrorAt: null,
    errorCount: 0,
  });

  React.useEffect(() => {
    if (!enabled) {
      setStatus((prev) => ({ ...prev, connected: false }));
      return undefined;
    }

    lastEventIdRef.current = null;
    setStatus({
      connected: false,
      lastEventId: null,
      lastEventAt: null,
      lastErrorAt: null,
      errorCount: 0,
    });

    const params = new URLSearchParams();
    if (lastEventIdRef.current) {
      params.set("last_event_id", String(lastEventIdRef.current));
    }
    if (types.length) params.set("types", types.join(","));
    if (events.length) params.set("event", events.join(","));
    const url = params.toString() ? `/v1/events?${params}` : "/v1/events";
    const source = new EventSource(url, { withCredentials: true });

    source.onopen = () => {
      setStatus((prev) => ({ ...prev, connected: true }));
    };
    source.onerror = () => {
      setStatus((prev) => ({
        ...prev,
        connected: false,
        errorCount: prev.errorCount + 1,
        lastErrorAt: Date.now(),
      }));
    };
    source.onmessage = (ev) => {
      try {
        const parsed = JSON.parse(ev.data);
        if (!parsed || typeof parsed !== "object") return;
        const rawId = ev.lastEventId || (parsed as any).id;
        const idNum = Number(rawId);
        const eventId = Number.isFinite(idNum) && idNum > 0 ? idNum : undefined;
        const msg: ServerEvent = {
          id: eventId,
          type: String((parsed as any).type || "event"),
          data: (parsed as any).data,
          ts: (parsed as any).ts,
        };
        if (eventId != null) lastEventIdRef.current = eventId;
        setEvent(msg);
        setStatus((prev) => ({
          ...prev,
          lastEventId: eventId ?? prev.lastEventId,
          lastEventAt: Date.now(),
        }));
      } catch {
        // ignore malformed event payloads
      }
    };

    return () => {
      try {
        source.close();
      } catch {
        // ignore
      }
      setStatus((prev) => ({ ...prev, connected: false }));
    };
  }, [enabled, key]);

  return {
    event,
    connected: status.connected,
    enabled,
    lastEventId: status.lastEventId,
    lastEventAt: status.lastEventAt,
    lastErrorAt: status.lastErrorAt,
    errorCount: status.errorCount,
  };
}
