import { expect, test } from "@playwright/test";

test("fleet tools load with empty data", async ({ page }) => {
  await page.route("**/v1/auth/config", async (route) => {
    await route.fulfill({
      json: {
        ok: true,
        version: "test",
        ui_enabled: true,
        auth_enabled: true,
        totp_enabled: false,
        openai_enabled: false,
        fpp_enabled: false,
        mqtt_enabled: false,
        peers_configured: 0,
      },
    });
  });
  await page.route("**/v1/auth/me", async (route) => {
    await route.fulfill({ json: { ok: true, user: { username: "tester" } } });
  });
  await page.route("**/v1/fleet/status", async (route) => {
    await route.fulfill({
      json: {
        ok: true,
        now: 0,
        stale_after_s: 30,
        summary: { agents: 0, online: 0, configured: 0 },
        agents: [],
      },
    });
  });
  await page.route("**/v1/fleet/history**", async (route) => {
    await route.fulfill({
      json: { ok: true, history: [], count: 0, limit: 100, offset: 0 },
    });
  });
  await page.route("**/v1/orchestration/runs**", async (route) => {
    await route.fulfill({
      json: { ok: true, runs: [], count: 0, limit: 100, offset: 0 },
    });
  });
  await page.route("**/v1/fleet/health", async (route) => {
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
  });
  await page.route("**/v1/ledfx/fleet", async (route) => {
    await route.fulfill({
      json: { ok: true, cached: false, summary: { total: 0 }, agents: {} },
    });
  });

  await page.goto("tools/fleet");
  await expect(
    page.getByRole("heading", { name: "Fleet", exact: true }),
  ).toBeVisible();
  await expect(page.getByText("No agents.")).toBeVisible();
});
