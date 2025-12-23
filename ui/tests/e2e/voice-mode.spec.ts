import { expect, test } from "@playwright/test";

test("voice mode toggles show server controls and disable auto-run for intent", async ({
  page,
}) => {
  await page.route("**/v1/auth/config", async (route) => {
    await route.fulfill({
      json: {
        ok: true,
        version: "test",
        ui_enabled: true,
        auth_enabled: true,
        totp_enabled: false,
        openai_enabled: true,
        fpp_enabled: false,
        mqtt_enabled: false,
        peers_configured: 0,
      },
    });
  });
  await page.route("**/v1/auth/me", async (route) => {
    await route.fulfill({ json: { ok: true, user: { username: "tester" } } });
  });

  await page.goto("chat");

  await page.getByRole("combobox", { name: "Voice mode" }).click();
  await page.getByRole("option", { name: "Server transcription" }).click();
  await expect(page.getByLabel("STT language")).toBeVisible();
  await expect(page.getByLabel("STT prompt")).toBeVisible();

  await page.getByRole("combobox", { name: "Voice mode" }).click();
  await page.getByRole("option", { name: "Server intent (run)" }).click();
  await expect(page.getByLabel("Auto-run voice commands")).toBeDisabled();
});
