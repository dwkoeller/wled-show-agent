import React from "react";
import { useServerEvents } from "./useServerEvents";

type UseEventRefreshArgs = {
  types: string[];
  refresh: () => void | Promise<void>;
  minIntervalMs?: number;
  fallbackIntervalMs?: number;
  ignoreEvents?: string[];
};

export function useEventRefresh({
  types,
  refresh,
  minIntervalMs = 1000,
  fallbackIntervalMs,
  ignoreEvents,
}: UseEventRefreshArgs) {
  const { event, enabled } = useServerEvents();
  const lastRunRef = React.useRef(0);
  const refreshRef = React.useRef(refresh);
  const typesRef = React.useRef(new Set(types));
  const ignoreRef = React.useRef(new Set(ignoreEvents ?? []));

  React.useEffect(() => {
    refreshRef.current = refresh;
  }, [refresh]);

  React.useEffect(() => {
    typesRef.current = new Set(types);
  }, [types]);

  React.useEffect(() => {
    ignoreRef.current = new Set(ignoreEvents ?? []);
  }, [ignoreEvents]);

  React.useEffect(() => {
    if (!event) return;
    if (!typesRef.current.has(event.type)) return;
    const eventKind =
      event.data && typeof event.data === "object"
        ? String((event.data as { event?: unknown }).event ?? "")
        : "";
    if (eventKind && ignoreRef.current.has(eventKind)) return;
    const now = Date.now();
    if (now - lastRunRef.current < minIntervalMs) return;
    lastRunRef.current = now;
    void refreshRef.current();
  }, [event, minIntervalMs]);

  React.useEffect(() => {
    if (enabled) return undefined;
    const interval = Math.max(
      3000,
      fallbackIntervalMs ?? minIntervalMs,
    );
    const id = window.setInterval(() => {
      void refreshRef.current();
    }, interval);
    return () => {
      window.clearInterval(id);
    };
  }, [enabled, fallbackIntervalMs, minIntervalMs]);
}
