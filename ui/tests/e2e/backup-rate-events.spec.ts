import { expect, test } from "@playwright/test";

const authConfig = {
  ok: true,
  version: "test",
  ui_enabled: true,
  auth_enabled: true,
  totp_enabled: false,
  openai_enabled: false,
  fpp_enabled: true,
  mqtt_enabled: true,
  peers_configured: 0,
};

async function mockAuth(page) {
  await page.route("**/v1/auth/config", async (route) => {
    await route.fulfill({ json: authConfig });
  });
  await page.route("**/v1/auth/me", async (route) => {
    await route.fulfill({
      json: { ok: true, user: { username: "tester", role: "admin" } },
    });
  });
}

test("backup export triggers a download", async ({ page }) => {
  await mockAuth(page);
  await page.route("**/v1/backup/export**", async (route) => {
    await route.fulfill({
      status: 200,
      headers: { "content-type": "application/zip" },
      body: "PK\x03\x04",
    });
  });
  await page.goto("tools/backup");

  const [download] = await Promise.all([
    page.waitForEvent("download"),
    page.getByRole("button", { name: "Download backup" }).click(),
  ]);

  expect(download.suggestedFilename()).toContain("wsa_backup");
});

test("backup import shows success", async ({ page }) => {
  await mockAuth(page);
  await page.route("**/v1/backup/import**", async (route) => {
    await route.fulfill({ json: { ok: true } });
  });
  await page.goto("tools/backup");

  const fileInput = page.locator('input[type="file"]');
  await fileInput.setInputFiles({
    name: "backup.zip",
    mimeType: "application/zip",
    buffer: Buffer.from("PK\x03\x04"),
  });

  await page.getByRole("button", { name: "Restore backup" }).click();
  await expect(page.getByText("Restore completed successfully.")).toBeVisible();
});

test("backup import shows error details", async ({ page }) => {
  await mockAuth(page);
  await page.route("**/v1/backup/import**", async (route) => {
    await route.fulfill({
      status: 400,
      json: { detail: "Invalid backup zip" },
    });
  });
  await page.goto("tools/backup");

  const fileInput = page.locator('input[type="file"]');
  await fileInput.setInputFiles({
    name: "backup.zip",
    mimeType: "application/zip",
    buffer: Buffer.from("not-a-zip"),
  });

  await page.getByRole("button", { name: "Restore backup" }).click();
  await expect(page.getByText("Invalid backup zip")).toBeVisible();
});

test("rate limit responses surface in tools UI", async ({ page }) => {
  await mockAuth(page);
  await page.route("**/v1/files/list**", async (route) => {
    await route.fulfill({
      status: 429,
      json: { detail: "Too many requests. Slow down." },
    });
  });
  await page.goto("tools/files");
  await expect(page.getByText("Too many requests. Slow down.")).toBeVisible();
});

test("SSE-disabled fallback polls for updates", async ({ page }) => {
  await mockAuth(page);
  let listCalls = 0;
  await page.route("**/v1/files/list**", async (route) => {
    listCalls += 1;
    await route.fulfill({ json: { ok: true, files: [] } });
  });

  await page.goto("tools/files");
  await expect.poll(() => listCalls).toBeGreaterThan(0);
  const initialCalls = listCalls;
  await page.waitForTimeout(3500);
  await expect.poll(() => listCalls).toBeGreaterThan(initialCalls);
});

test("SSE reconnect still triggers refresh", async ({ page }) => {
  await page.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", {
      get: () => false,
      configurable: true,
    });
    class FakeEventSource {
      url: string;
      withCredentials: boolean;
      readyState = 0;
      onopen: ((ev?: Event) => void) | null = null;
      onerror: ((ev?: Event) => void) | null = null;
      onmessage: ((ev?: MessageEvent) => void) | null = null;
      constructor(url: string, opts?: { withCredentials?: boolean }) {
        this.url = url;
        this.withCredentials = Boolean(opts?.withCredentials);
        (window as any).__eventSource = this;
        setTimeout(() => {
          this.readyState = 1;
          this.onopen?.(new Event("open"));
        }, 0);
      }
      close() {
        this.readyState = 2;
      }
      emit(payload: unknown) {
        const data = JSON.stringify(payload);
        this.onmessage?.(new MessageEvent("message", { data }));
      }
      error() {
        this.onerror?.(new Event("error"));
      }
      open() {
        this.readyState = 1;
        this.onopen?.(new Event("open"));
      }
    }
    (window as any).EventSource = FakeEventSource;
    (window as any).__emitServerEvent = (payload: unknown) => {
      (window as any).__eventSource?.emit(payload);
    };
    (window as any).__triggerServerError = () => {
      (window as any).__eventSource?.error();
    };
    (window as any).__triggerServerOpen = () => {
      (window as any).__eventSource?.open();
    };
  });

  await mockAuth(page);
  let listCalls = 0;
  await page.route("**/v1/files/list**", async (route) => {
    listCalls += 1;
    await route.fulfill({ json: { ok: true, files: [] } });
  });

  await page.goto("tools/files");
  await expect.poll(() => listCalls).toBeGreaterThan(0);
  const initialCalls = listCalls;

  await page.evaluate(() =>
    (window as any).__emitServerEvent({
      type: "files",
      data: {},
      ts: Date.now() / 1000,
    }),
  );
  await expect.poll(() => listCalls).toBeGreaterThan(initialCalls);
  const afterFirstEvent = listCalls;

  await page.waitForTimeout(2100);
  await page.evaluate(() => {
    (window as any).__triggerServerError?.();
    (window as any).__triggerServerOpen?.();
    (window as any).__emitServerEvent?.({
      type: "files",
      data: {},
      ts: Date.now() / 1000,
    });
  });
  await expect.poll(() => listCalls).toBeGreaterThan(afterFirstEvent, {
    timeout: 5000,
  });
});
