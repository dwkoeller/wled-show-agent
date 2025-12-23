export type ApiErrorBody = {
  ok?: boolean;
  error?: string;
  detail?: string;
};

type CsrfConfig = {
  enabled: boolean;
  cookieName: string;
  headerName: string;
};

let csrfConfig: CsrfConfig = {
  enabled: false,
  cookieName: "wsa_csrf",
  headerName: "X-CSRF-Token",
};

export function setCsrfConfig(next: Partial<CsrfConfig>) {
  csrfConfig = {
    ...csrfConfig,
    ...next,
  };
}

export function csrfHeaders(method?: string): Record<string, string> {
  const verb = (method || "GET").toUpperCase();
  if (csrfConfig.enabled && !["GET", "HEAD", "OPTIONS"].includes(verb)) {
    const token = readCookie(csrfConfig.cookieName);
    if (token) {
      return { [csrfConfig.headerName]: token };
    }
  }
  return {};
}

function readCookie(name: string): string | null {
  if (typeof document === "undefined") return null;
  const raw = document.cookie || "";
  const parts = raw.split(";").map((c) => c.trim());
  for (const part of parts) {
    if (!part) continue;
    const idx = part.indexOf("=");
    if (idx < 0) continue;
    const key = part.slice(0, idx).trim();
    if (key !== name) continue;
    return decodeURIComponent(part.slice(idx + 1));
  }
  return null;
}

export async function api<T>(
  path: string,
  options?: RequestInit & { json?: unknown },
): Promise<T> {
  const init: RequestInit = {
    ...options,
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
      ...(options?.headers ?? {}),
    },
  };

  const method = (init.method || "GET").toUpperCase();
  if (
    csrfConfig.enabled &&
    !["GET", "HEAD", "OPTIONS"].includes(method)
  ) {
    const token = readCookie(csrfConfig.cookieName);
    if (token && typeof init.headers === "object") {
      (init.headers as Record<string, string>)[csrfConfig.headerName] = token;
    }
  }

  if (options && "json" in options) {
    init.body = JSON.stringify(options.json ?? {});
  }

  const resp = await fetch(path, init);
  const contentType = resp.headers.get("content-type") ?? "";

  let body: unknown = null;
  if (contentType.includes("application/json")) {
    body = await resp.json().catch(() => null);
  } else {
    body = await resp.text().catch(() => "");
  }

  if (!resp.ok) {
    const err = body as ApiErrorBody | null;
    const msg =
      (err && (err.detail || err.error)) ||
      (typeof body === "string" && body.trim()) ||
      `HTTP ${resp.status}`;
    throw new Error(msg);
  }

  return body as T;
}
