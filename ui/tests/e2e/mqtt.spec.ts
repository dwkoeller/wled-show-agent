import { expect, test } from "@playwright/test";

test("mqtt tools show bridge status and topics", async ({ page }) => {
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
        mqtt_enabled: true,
        peers_configured: 0,
      },
    });
  });
  await page.route("**/v1/auth/me", async (route) => {
    await route.fulfill({ json: { ok: true, user: { username: "tester" } } });
  });
  await page.route("**/v1/mqtt/status", async (route) => {
    await route.fulfill({
      json: {
        ok: true,
        enabled: true,
        running: true,
        connected: true,
        base_topic: "wsa/test",
        qos: 1,
        topics: {
          base: "wsa/test",
          commands: ["sequence/start", "stop_all"],
          state: ["status", "availability"],
        },
        broker: { host: "mqtt.local", port: 1883, tls: false },
        counters: { messages_received: 5, actions_ok: 5, actions_failed: 0 },
      },
    });
  });

  await page.goto("tools/mqtt");
  await expect(
    page.getByRole("heading", { name: "MQTT Bridge", exact: true }),
  ).toBeVisible();
  await expect(page.getByText(/^connected$/)).toBeVisible();
  await expect(page.getByText("wsa/test/sequence/start")).toBeVisible();
});
