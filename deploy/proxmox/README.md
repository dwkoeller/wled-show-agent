# Proxmox deployment

The application runs in Docker inside an unprivileged Debian LXC. Falcon Player
runs in Docker in a second, dedicated LXC. Attach both guests to the deployment
bridge and show VLAN used by your environment. Keep addresses, hostnames, VLAN
IDs, and resource assignments in a private deployment inventory.

## Deployment inventory

| Service | Proxmox ID | Node | Address | Resources |
| --- | --- | --- | --- | --- |
| Show application | `<APP_LXC_ID>` | `<PVE_NODE>` | `<APP_IP>` | `<APP_RESOURCES>` |
| Falcon Player | `<FPP_LXC_ID>` | `<PVE_NODE>` | `<FPP_IP>` | `<FPP_RESOURCES>` |

Reserve DHCP leases by MAC address before relying on permanent bookmarks. All
project LXCs should use `onboot=1`, `nesting=1`, and `keyctl=1`.

Application URLs:

- `http://<APP_IP>/` — mobile-first React chatbot
- `http://<APP_IP>/dashboard` — device dashboard
- `http://<APP_IP>/tools/fpp` — Falcon Player controls
- `http://<APP_IP>/api/docs` — authenticated FastAPI documentation
- `http://<APP_IP>/api/health` — process health
- `http://<APP_IP>/api/readyz` — dependency readiness
- `http://<FPP_IP>/` — Falcon Player administration

The application Compose project is `/opt/wled-show-agent` in the application LXC. Only Nginx
publishes port 80. The API and MySQL use Docker's private service network.
Falcon Player uses a private persistent media directory and host networking
inside its LXC, so DDP, sACN, Art-Net, and MultiSync traffic use its assigned
address.

## ChatGPT connection

The chatbot uses the official [Codex App Server](https://learn.chatgpt.com/docs/app-server)
with ChatGPT device sign-in. It does not repurpose a ChatGPT browser cookie or
require an API key. Available models are discovered from the signed-in account;
subscription eligibility, model access, and limits still apply.

1. Sign in to the show application using its administrator credentials and TOTP.
2. Select **Sign in with ChatGPT**.
3. Open the verification link, enter the displayed code, and authorize the account.
4. Return to the show application. The model list appears automatically.

Deployment credentials and the authenticator setup key must be saved in a private
host secret store (mode 0600), outside this repo.
OAuth credentials and Codex conversation history persist in the `codex_data`
Docker volume. Conversation ownership and titles persist in MySQL. The browser
never receives OAuth tokens. All assistant tool calls use the user's existing
FastAPI authorization. Shell execution is disabled for the assistant.

The chat supports streamed text, Markdown, conversation history, model and
reasoning selection, browser speech input, audio/sequence uploads, tool results,
reply interruption, and a stop control for both the agent and Falcon Player.
Browser microphone and PWA features may require HTTPS on mobile browsers.

## Controller configuration

`FPP_BASE_URL` points to the dedicated FPP LXC. The fleet deployment now targets
the two real WLED controllers: `<WLED_TREE_IP>` as the coordinator/tree and
`<WLED_SECONDARY_IP>` as the second WLED agent. They may be powered off during setup,
so WLED readiness and output checks will remain unavailable until they are on.
Set `DDP_HOST`, geometry, and segment IDs after power-on if the physical layout
differs from the current segment-0 defaults. No show playback was enabled.

## Updates and verification

Run inside the application LXC, after copying updated source into `/opt/wled-show-agent`:

```sh
cd /opt/wled-show-agent
docker compose up -d --build
docker compose ps
docker compose logs --tail=80 api
python3 deploy/proxmox/verify.py
```

The verifier logs in using `.env`, checks React routes, OpenAPI, ChatGPT runtime,
and real Falcon Player status/playlists, then logs out. It never starts playback.

For Falcon Player, use `fpp.compose.yml` from this directory as the deployment
Compose file, and set `FPP_IP` in the adjacent `.env`. Preserve `media` ownership
as UID/GID `500:500`, mode `770`.

## Persistence, backups, and cluster movement

Back up both complete LXCs with Proxmox vzdump. This includes the application
`.env`, MySQL volume, Codex volume, application `/data`, and Falcon Player media.
Protect those backups because they include credentials. Use a MySQL logical
backup or stop the application LXC for a fully consistent database backup.

Both guests may use cluster-managed storage.
They are not configured as active-active services or Proxmox HA resources.
A stopped-container migration to the other node can transfer the root disk;
VLAN 10 must remain available on `vmbr0` on the destination. Do not run duplicate
copies of the same show coordinator or FPP output instance simultaneously.

Runtime references: [FPP Docker deployment](https://github.com/FalconChristmas/fpp/blob/master/Docker/docker-compose.yml),
[Proxmox container options](https://pve.proxmox.com/pve-docs/pct.1.html).
