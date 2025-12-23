import { expect, test } from "@playwright/test";

test("orchestration builder can generate a look payload", async ({ page }) => {
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
  await page.route("**/v1/orchestration/status", async (route) => {
    await route.fulfill({ json: { ok: true } });
  });
  await page.route("**/v1/fleet/orchestration/status", async (route) => {
    await route.fulfill({ json: { ok: true } });
  });
  await page.route("**/v1/sequences/list", async (route) => {
    await route.fulfill({ json: { ok: true, files: [] } });
  });
  await page.route("**/v1/ddp/patterns", async (route) => {
    await route.fulfill({ json: { ok: true, patterns: [] } });
  });
  await page.route("**/v1/wled/presets", async (route) => {
    await route.fulfill({ json: { ok: true, presets: {} } });
  });
  await page.route("**/v1/wled/effects", async (route) => {
    await route.fulfill({ json: { ok: true, effects: ["Solid", "Blink"] } });
  });
  await page.route("**/v1/wled/palettes", async (route) => {
    await route.fulfill({ json: { ok: true, palettes: ["Default", "Rainbow"] } });
  });
  await page.route("**/v1/meta/last_applied", async (route) => {
    await route.fulfill({ json: { ok: true } });
  });
  await page.route("**/v1/orchestration/presets**", async (route) => {
    await route.fulfill({ json: { ok: true, presets: [] } });
  });

  await page.goto("tools/orchestration");
  await expect(
    page.getByRole("heading", { name: "Orchestration" }),
  ).toBeVisible();

  await page.getByLabel("New step kind").click();
  await page.getByRole("option", { name: "look" }).click();
  await page.getByRole("button", { name: "Add step" }).click();

  const lookToggle = page.getByLabel("Use visual look builder").last();
  await lookToggle.click();

  await page.getByLabel("effect").last().fill("Blink");
  await page.getByLabel("palette").last().fill("Rainbow");

  const generated = page.getByLabel("Generated look JSON").last();
  await expect(generated).toHaveValue(/wled_look/);
});
