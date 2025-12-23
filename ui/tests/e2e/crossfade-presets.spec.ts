import { expect, test } from "@playwright/test";

test("crossfade presets load and populate builder fields", async ({ page }) => {
  await page.route("**/v1/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;

    if (path === "/v1/auth/config") {
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
      return;
    }
    if (path === "/v1/auth/me") {
      await route.fulfill({ json: { ok: true, user: { username: "tester" } } });
      return;
    }
    if (path === "/v1/orchestration/presets") {
      if (!url.searchParams.get("scope")?.includes("crossfade")) {
        await route.fulfill({ json: { ok: true, presets: [] } });
        return;
      }
      await route.fulfill({
        json: {
          ok: true,
          presets: [
            {
              id: 7,
              name: "Snow Glow",
              scope: "crossfade",
              payload: {
                scope: "fleet",
                crossfade: {
                  look: {
                    type: "wled_look",
                    name: "Snow Glow",
                    theme: "icy",
                    seg: {
                      fx: "Solid",
                      pal: "Default",
                      col: [
                        [0, 120, 255],
                        [255, 255, 255],
                        [0, 64, 128],
                      ],
                      on: true,
                    },
                  },
                  brightness: 180,
                  transition_ms: 1200,
                  targets: ["roofline"],
                  include_self: true,
                },
              },
            },
          ],
        },
      });
      return;
    }

    await route.fulfill({ json: { ok: true } });
  });

  const presetsResponse = page.waitForResponse("**/v1/orchestration/presets**");
  await page.goto(".");
  await presetsResponse;
  await page.getByRole("button", { name: "Crossfade", exact: true }).click();

  await page.getByLabel("Preset", { exact: true }).click();
  await page.getByRole("option", { name: "Snow Glow" }).click();

  await expect(page.getByLabel("Preset name")).toHaveValue("Snow Glow");
  await expect(page.getByLabel("Name (optional)")).toHaveValue("Snow Glow");
  await expect(page.getByLabel("transition_ms (optional)")).toHaveValue("1200");
  await expect(page.getByLabel("Effect")).toHaveValue("Solid");
});
