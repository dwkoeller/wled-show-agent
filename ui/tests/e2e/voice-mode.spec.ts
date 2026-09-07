import { expect, test } from "@playwright/test";

test("chat offers browser voice input without requiring API-funded transcription", async ({ page }) => {
  await page.route("**/api/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    const json = path.endsWith("/auth/config") ? { auth_enabled: true, totp_enabled: true }
      : path.endsWith("/auth/me") ? { user: { username: "tester", role: "admin" } }
      : path.endsWith("/chat/account") ? { connected: true }
      : path.endsWith("/chat/models") ? { data: [] }
      : { threads: [] };
    await route.fulfill({ json });
  });
  await page.goto("/");
  await expect(page.getByRole("button", { name: /Voice input/ })).toBeVisible();
  await expect(page.getByRole("textbox", { name: "Message", exact: true })).toBeVisible();
});
