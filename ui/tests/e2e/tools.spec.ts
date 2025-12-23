import { expect, test } from "@playwright/test";

const authConfig = {
  ok: true,
  version: "test",
  ui_enabled: true,
  auth_enabled: true,
  totp_enabled: false,
  openai_enabled: false,
  fpp_enabled: true,
  ledfx_enabled: true,
  mqtt_enabled: true,
  peers_configured: 0,
};

async function mockToolApi(page) {
  await page.route("**/v1/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;

    if (path === "/v1/auth/config") {
      await route.fulfill({ json: authConfig });
      return;
    }
    if (path === "/v1/auth/me") {
      await route.fulfill({
        json: { ok: true, user: { username: "tester", role: "admin" } },
      });
      return;
    }

    if (path === "/v1/auth/users") {
      await route.fulfill({ json: { ok: true, users: [] } });
      return;
    }
    if (path === "/v1/auth/sessions") {
      await route.fulfill({
        json: { ok: true, sessions: [], count: 0, limit: 50, offset: 0 },
      });
      return;
    }
    if (path === "/v1/auth/login_attempts") {
      await route.fulfill({
        json: { ok: true, attempts: [], count: 0, limit: 50, offset: 0 },
      });
      return;
    }
    if (path === "/v1/auth/api_keys") {
      await route.fulfill({
        json: { ok: true, api_keys: [], count: 0, limit: 50, offset: 0 },
      });
      return;
    }

    if (path === "/v1/files/list") {
      await route.fulfill({ json: { ok: true, files: [] } });
      return;
    }
    if (path === "/v1/looks/packs") {
      await route.fulfill({ json: { ok: true, packs: [], latest: null } });
      return;
    }
    if (path === "/v1/sequences/list") {
      await route.fulfill({ json: { ok: true, files: [] } });
      return;
    }
    if (path === "/v1/ddp/patterns") {
      await route.fulfill({ json: { ok: true, patterns: [] } });
      return;
    }
    if (path === "/v1/wled/presets") {
      await route.fulfill({ json: { ok: true, presets: {} } });
      return;
    }
    if (path === "/v1/wled/effects") {
      await route.fulfill({ json: { ok: true, effects: [] } });
      return;
    }
    if (path === "/v1/wled/palettes") {
      await route.fulfill({ json: { ok: true, palettes: [] } });
      return;
    }
    if (path === "/v1/meta/last_applied") {
      await route.fulfill({ json: { ok: true, last_applied: {} } });
      return;
    }
    if (path === "/v1/orchestration/presets") {
      await route.fulfill({
        json: { ok: true, presets: [], count: 0, limit: 200, offset: 0 },
      });
      return;
    }
    if (path === "/v1/orchestration/status") {
      await route.fulfill({ json: { ok: true } });
      return;
    }
    if (path === "/v1/fleet/orchestration/status") {
      await route.fulfill({ json: { ok: true } });
      return;
    }

    if (path === "/v1/fleet/status") {
      await route.fulfill({
        json: {
          ok: true,
          now: 0,
          stale_after_s: 30,
          summary: { agents: 0, online: 0, configured: 0 },
          agents: [],
        },
      });
      return;
    }
    if (path === "/v1/fleet/health") {
      await route.fulfill({
        json: {
          ok: true,
          cached: false,
          summary: {
            total: 0,
            online: 0,
            wled_ok: 0,
            fpp_ok: 0,
            ledfx_ok: 0,
          },
          entries: [],
        },
      });
      return;
    }
    if (path === "/v1/fleet/history") {
      await route.fulfill({
        json: { ok: true, history: [], count: 0, limit: 100, offset: 0 },
      });
      return;
    }
    if (path === "/v1/orchestration/runs") {
      await route.fulfill({
        json: { ok: true, runs: [], count: 0, limit: 100, offset: 0 },
      });
      return;
    }
    if (path === "/v1/audit/logs") {
      await route.fulfill({
        json: { ok: true, logs: [], count: 0, limit: 50, offset: 0 },
      });
      return;
    }

    if (path === "/v1/scheduler/status") {
      await route.fulfill({
        json: {
          ok: true,
          running: false,
          in_window: false,
          last_action_at: null,
          last_action: null,
          last_error: null,
          next_action_in_s: null,
          config: {},
        },
      });
      return;
    }
    if (path === "/v1/scheduler/events") {
      await route.fulfill({
        json: { ok: true, events: [], count: 0, limit: 20, offset: 0 },
      });
      return;
    }

    if (path === "/v1/meta/packs") {
      await route.fulfill({ json: { ok: true, packs: [] } });
      return;
    }
    if (path === "/v1/meta/sequences") {
      await route.fulfill({ json: { ok: true, sequences: [] } });
      return;
    }
    if (path === "/v1/meta/audio_analyses") {
      await route.fulfill({ json: { ok: true, audio_analyses: [] } });
      return;
    }
    if (path === "/v1/meta/show_configs") {
      await route.fulfill({ json: { ok: true, show_configs: [] } });
      return;
    }
    if (path === "/v1/meta/fseq_exports") {
      await route.fulfill({ json: { ok: true, fseq_exports: [] } });
      return;
    }
    if (path === "/v1/meta/fpp_scripts") {
      await route.fulfill({ json: { ok: true, fpp_scripts: [] } });
      return;
    }
    if (path === "/v1/meta/reconcile/status") {
      await route.fulfill({ json: { ok: true, exists: false, status: null } });
      return;
    }
    if (path === "/v1/meta/reconcile/history") {
      await route.fulfill({
        json: { ok: true, runs: [], count: 0, limit: 10, offset: 0 },
      });
      return;
    }

    if (path === "/v1/mqtt/status") {
      await route.fulfill({
        json: { ok: true, enabled: true, connected: false, base_topic: "wsa/test" },
      });
      return;
    }
    if (path === "/v1/mqtt/config") {
      await route.fulfill({
        json: { ok: true, mqtt_enabled: true, base_topic: "wsa/test" },
      });
      return;
    }

    if (path === "/v1/fpp/status") {
      await route.fulfill({ json: { ok: true } });
      return;
    }
    if (path === "/v1/fpp/discover") {
      await route.fulfill({ json: { ok: true } });
      return;
    }
    if (path === "/v1/fpp/playlists") {
      await route.fulfill({ json: { ok: true, playlists: [] } });
      return;
    }
    if (path === "/v1/ledfx/status") {
      await route.fulfill({ json: { ok: true } });
      return;
    }
    if (path === "/v1/ledfx/fleet") {
      await route.fulfill({
        json: { ok: true, cached: false, summary: { total: 0 }, agents: {} },
      });
      return;
    }
    if (path === "/v1/ledfx/virtuals") {
      await route.fulfill({ json: { ok: true, virtuals: [] } });
      return;
    }
    if (path === "/v1/ledfx/scenes") {
      await route.fulfill({ json: { ok: true, scenes: [] } });
      return;
    }
    if (path === "/v1/ledfx/effects") {
      await route.fulfill({ json: { ok: true, effects: [] } });
      return;
    }

    await route.fulfill({ json: { ok: true } });
  });
}

const pages = [
  { path: "tools/auth", heading: "Auth" },
  { path: "tools/audit", heading: "Audit Log" },
  { path: "tools/files", heading: "Browse" },
  { path: "tools/packs", heading: "Pack ingestion" },
  { path: "tools/sequences", heading: "Sequence Generator" },
  { path: "tools/audio", heading: "Audio Beat/BPM Analyzer" },
  { path: "tools/xlights", heading: "Project Import" },
  { path: "tools/meta", heading: "Metadata" },
  { path: "tools/fseq", heading: "Export .fseq" },
  { path: "tools/orchestration", heading: "Orchestration" },
  { path: "tools/fpp", heading: "Playlists" },
  { path: "tools/ledfx", heading: "LedFx Status" },
  { path: "tools/mqtt", heading: "MQTT Bridge" },
  { path: "tools/scheduler", heading: "Scheduler Status" },
  { path: "tools/backup", heading: "Backup" },
];

for (const pageDef of pages) {
  test(`tools page loads: ${pageDef.path}`, async ({ page }) => {
    await mockToolApi(page);
    await page.goto(pageDef.path);
    await expect(
      page.getByRole("heading", { name: pageDef.heading, exact: true }),
    ).toBeVisible();
  });
}
