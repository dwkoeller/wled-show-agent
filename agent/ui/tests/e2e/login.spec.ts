import { expect, test } from "@playwright/test";

test("shows TOTP field when totp is enabled", async ({ page }) => {
  await page.route("**/v1/auth/config", async (route) => {
    await route.fulfill({
      json: {
        ok: true,
        version: "test",
        ui_enabled: true,
        auth_enabled: true,
        totp_enabled: true,
        openai_enabled: false,
        fpp_enabled: false,
        peers_configured: 0,
      },
    });
  });
  await page.route("**/v1/auth/me", async (route) => {
    await route.fulfill({
      status: 401,
      json: { ok: false, error: "unauthorized" },
    });
  });

  await page.goto("login");
  await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
  await expect(page.getByLabel("TOTP (6 digits)")).toBeVisible();
});

test("hides TOTP field when totp is disabled", async ({ page }) => {
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
        peers_configured: 0,
      },
    });
  });
  await page.route("**/v1/auth/me", async (route) => {
    await route.fulfill({
      status: 401,
      json: { ok: false, error: "unauthorized" },
    });
  });

  await page.goto("login");
  await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
  await expect(page.getByLabel("TOTP (6 digits)")).toHaveCount(0);
});
