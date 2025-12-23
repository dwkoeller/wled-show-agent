import React from "react";

type ServerEvent = {
  id?: number;
  type: string;
  data?: unknown;
  ts?: number;
};

type EventListener = (event: ServerEvent) => void;
type EventsStatus = {
  connected: boolean;
  lastEventId: number | null;
  lastEventAt: number | null;
  lastErrorAt: number | null;
  errorCount: number;
};
type StatusListener = (status: EventsStatus) => void;

let eventSource: EventSource | null = null;
let lastEvent: ServerEvent | null = null;
let lastEventId: number | null = null;
let lastEventAt: number | null = null;
let lastErrorAt: number | null = null;
let errorCount = 0;
let connected = false;
const listeners = new Set<EventListener>();
const statusListeners = new Set<StatusListener>();

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

function notifyStatus() {
  const status: EventsStatus = {
    connected,
    lastEventId,
    lastEventAt,
    lastErrorAt,
    errorCount,
  };
  statusListeners.forEach((cb) => cb(status));
}

function openEventSource() {
  if (!eventsEnabled || eventSource) return;
  const params = new URLSearchParams();
  if (lastEventId) params.set("last_event_id", String(lastEventId));
  const url = params.toString() ? `/v1/events?${params}` : "/v1/events";
  eventSource = new EventSource(url, { withCredentials: true });
  eventSource.onopen = () => {
    connected = true;
    notifyStatus();
  };
  eventSource.onerror = () => {
    connected = false;
    errorCount += 1;
    lastErrorAt = Date.now();
    notifyStatus();
  };
  eventSource.onmessage = (ev) => {
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
      if (eventId != null) lastEventId = eventId;
      lastEventAt = Date.now();
      lastEvent = msg;
      notifyStatus();
      listeners.forEach((cb) => cb(msg));
    } catch {
      // ignore malformed event payloads
    }
  };
}

function closeEventSource() {
  if (!eventSource) return;
  try {
    eventSource.close();
  } catch {
    // ignore
  }
  eventSource = null;
  connected = false;
  notifyStatus();
}

export function useServerEvents() {
  const [event, setEvent] = React.useState<ServerEvent | null>(lastEvent);
  const [status, setStatus] = React.useState<EventsStatus>({
    connected,
    lastEventId,
    lastEventAt,
    lastErrorAt,
    errorCount,
  });

  React.useEffect(() => {
    if (!eventsEnabled) return undefined;
    const onEvent: EventListener = (msg) => setEvent(msg);
    const onStatus: StatusListener = (next) => setStatus(next);
    listeners.add(onEvent);
    statusListeners.add(onStatus);
    openEventSource();
    return () => {
      listeners.delete(onEvent);
      statusListeners.delete(onStatus);
      if (listeners.size === 0) closeEventSource();
    };
  }, []);

  return {
    event,
    connected: status.connected,
    enabled: eventsEnabled,
    lastEventId: status.lastEventId,
    lastEventAt: status.lastEventAt,
    lastErrorAt: status.lastErrorAt,
    errorCount: status.errorCount,
  };
}
