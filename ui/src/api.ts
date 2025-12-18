export type ApiErrorBody = {
  ok?: boolean;
  error?: string;
  detail?: string;
};

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
