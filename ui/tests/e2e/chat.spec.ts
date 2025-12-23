import { expect, test } from "@playwright/test";

test("can send a chat command (stubbed)", async ({ page }) => {
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
  await page.route("**/v1/command", async (route) => {
    const body = route.request().postDataJSON() as { text?: string };
    await route.fulfill({
      json: { ok: true, response: `echo:${body.text ?? ""}` },
    });
  });

  await page.goto("chat");
  await expect(page.getByRole("heading", { name: "Chat" })).toBeVisible();

  await page.getByRole("textbox", { name: "Command" }).fill("stop all");
  await page.getByRole("button", { name: "Send" }).click();

  await expect(page.getByText(/echo:stop all/)).toBeVisible();

  await page.getByRole("combobox", { name: "Voice mode" }).click();
  await page.getByRole("option", { name: "Server transcription" }).click();
  await expect(page.getByLabel("STT language")).toBeVisible();
  await expect(page.getByLabel("STT prompt")).toBeVisible();

  await page.getByRole("combobox", { name: "Voice mode" }).click();
  await page.getByRole("option", { name: "Server intent (run)" }).click();
  await expect(page.getByLabel("Auto-run voice commands")).toBeDisabled();
});
