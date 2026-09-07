import { expect, test, type Page } from "@playwright/test";

export async function mockChat(page: Page, connected = true) {
  await page.route("**/api/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    const method = route.request().method();
    let json: unknown = { ok: true };
    if (path === "/api/auth/config") json = { auth_enabled: true, totp_enabled: true, csrf_enabled: false, peers_configured: 0 };
    else if (path === "/api/auth/me") json = { ok: true, user: { username: "show-owner", role: "admin" } };
    else if (path === "/api/chat/account") json = { connected, account: connected ? { email: "owner@example.com", planType: "plus" } : null };
    else if (path === "/api/chat/models") json = { data: [{ id: "account-model", model: "account-model", displayName: "My subscription model", isDefault: true, defaultReasoningEffort: "medium", supportedReasoningEfforts: [{ reasoningEffort: "medium", description: "Balanced" }] }] };
    else if (path === "/api/chat/threads") json = method === "POST" ? { id: "thread-test", title: "New conversation" } : { threads: [{ id: "previous", title: "Last night's show" }] };
    else if (path === "/api/chat/threads/previous") json = { turns: [{ items: [{ id: "old", type: "agentMessage", text: "Your previous show is saved." }] }] };
    else if (path.endsWith("/messages")) {
      const prompt = route.request().postDataJSON().text;
      const events = [
        { method: "item/started", params: { item: { id: "tool1", type: "dynamicToolCall", tool: "show_request", status: "inProgress", arguments: { path: "/api/fpp/playlists" } } } },
        { method: "item/completed", params: { item: { id: "tool1", type: "dynamicToolCall", tool: "show_request", status: "completed", success: true } } },
        { method: "item/agentMessage/delta", params: { itemId: "reply", delta: `Falcon Player is ready. You asked: ${prompt}` } },
        { method: "turn/completed", params: { turn: { status: "completed" } } },
      ];
      await route.fulfill({ contentType: "text/event-stream", body: events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join("") });
      return;
    }
    await route.fulfill({ json });
  });
}

test("mobile chat streams replies and exposes actual account models", async ({ page }) => {
  await mockChat(page);
  await page.goto("/");
  await expect(page.getByRole("heading", { name: /bring to light/ })).toBeVisible();
  await page.getByRole("textbox", { name: "Message", exact: true }).fill("List my playlists");
  await page.getByRole("button", { name: "Send message", exact: true }).click();
  await expect(page.getByText("Falcon Player is ready. You asked: List my playlists")).toBeVisible();
  await expect(page.getByText("Using show controls")).toBeVisible();
  await page.getByRole("button", { name: /My subscription model/ }).click();
  await expect(page.getByRole("combobox", { name: "Model", exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Close panel" }).click();
  await page.getByRole("button", { name: "Conversations", exact: true }).click();
  await page.getByRole("button", { name: "Last night's show", exact: true }).click();
  await expect(page.getByText("Your previous show is saved.")).toBeVisible();
});

test("phone layout keeps controls visible without horizontal scrolling", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await mockChat(page);
  await page.goto("/");
  await expect(page.getByRole("textbox", { name: "Message", exact: true })).toBeVisible();
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth);
  expect(overflow).toBe(false);
  const send = await page.getByRole("button", { name: "Send message", exact: true }).boundingBox();
  expect(send!.y + send!.height).toBeLessThan(844);
  await page.screenshot({ path: "test-results/chat-mobile.png", fullPage: true });
});

test("disconnected subscription presents sign-in instead of claiming AI is available", async ({ page }) => {
  await mockChat(page, false);
  await page.goto("/");
  await expect(page.getByRole("button", { name: "Sign in with ChatGPT" })).toBeVisible();
  await expect(page.getByRole("textbox", { name: "Message", exact: true })).toBeDisabled();
});

test("stop show reaches both the show agent and Falcon Player", async ({ page }) => {
  await mockChat(page);
  const stopped: string[] = [];
  page.on("request", (request) => {
    if (request.url().includes("/stop_all") || request.url().includes("/playlist/stop")) stopped.push(new URL(request.url()).pathname);
  });
  await page.goto("/");
  await page.getByRole("button", { name: "Stop show", exact: true }).click();
  await expect(page.getByText(/confirmed the stop request/)).toBeVisible();
  expect(stopped.sort()).toEqual(["/api/fleet/stop_all", "/api/fpp/playlist/stop"]);
});
