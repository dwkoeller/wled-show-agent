# WLED Show Agent

Local-first **show director + pattern/sequence generator** for a WLED mega tree.

Runs as a single FastAPI service in Docker (works great on a Proxmox VM/LXC) and can:

- **Generate a lot of “looks”** (WLED JSON state payloads)
- **Optionally import some looks as WLED presets** (careful: WLED presets are limited)
- **Generate + play timed sequences** (deterministic cue lists)
- **Stream realtime procedural animations over DDP** (UDP 4048)
- **Export renderable sequences to `.fseq`** (procedural `ddp` steps only)
- **Analyze audio for BPM + beats** (writes `beats.json`)
- **(Optional) Natural language control** via OpenAI tool-calling or local commands (`/v1/command`)

This repo is designed for **LAN use only**.

---

## How it works

Think of it as two planes:

- **Control plane (HTTP/JSON):** set brightness, apply a “look”, import presets, run sequences.
- **Realtime plane (DDP/UDP):** stream deterministic frames for patterns that aren’t convenient as static WLED states.

The “agentic” part is optional:

- If you set `OPENAI_API_KEY`, `/v1/command` becomes a tool-using director that decides which local actions to take.
- Without OpenAI, `/v1/command` supports a small local command set (and the generator/sequence/DDP endpoints still work).

---

## Requirements

- A running WLED controller reachable from the machine/container running this service
- Docker + Docker Compose
- (Optional) OpenAI API key to enable `/v1/command`

---

## Quick start (Docker)

1. Download the repo zip and unzip it

2. Create your `.env`

```bash
cp .env.example .env
# edit .env and set at minimum: WLED_TREE_URL
```

3. Start the service

```bash
docker compose up -d --build
```

This starts 2 containers:

- `ui` (Nginx) serves `/ui/*` and proxies `/v1/*` + `/docs` to the API
- `api` (FastAPI) runs the agent backend

4. Open the API docs (Swagger UI)

- `http://<host>:8088/docs`

Optional: open the mobile-friendly UI:

- `http://<host>:8088/ui`

Mobile install (PWA):

- On iOS/Android, open `/ui` and use “Add to Home Screen” (installable web app).
- For voice input + secure cookies on phones, HTTPS is strongly recommended (see “HTTPS on LAN” below).

5. Sanity check

```bash
curl -sS http://<host>:8088/v1/health
```

---

## Configuration (.env)

The full set of environment variables is documented in `.env.example`.

### Controller kind

- `CONTROLLER_KIND` – `wled` (default) or `pixel` (ESPixelStick / sACN / Art‑Net)
  - `wled`: run `main:app` and set `WLED_TREE_URL`
  - `pixel`: run `pixel_main:app` and set the `PIXEL_*` vars

### Required (WLED controllers)

- `WLED_TREE_URL` – base URL of your WLED device (example: `http://172.16.200.50`)

### Required (ESPixelStick / pixel controllers)

- `PIXEL_PROTOCOL` – `e131` (sACN) or `artnet`
- `PIXEL_HOST` – device IP (example: `172.16.200.60`)
- `PIXEL_COUNT` – number of pixels on that output
- `PIXEL_UNIVERSE_START` – start universe/port-address (match your ESPixelStick + xLights plan)

### Recommended safety / reliability

- `WLED_MAX_BRI` – hard brightness cap (1–255). The service will never set above this.
- `WLED_COMMAND_COOLDOWN_MS` – minimum delay between write calls to WLED.
- `WLED_HTTP_TIMEOUT_S` – HTTP timeout for WLED requests.

### Segments

If your WLED tree uses multiple segments (common for multi-output builds):

- `WLED_SEGMENT_IDS=0,1,2,3` (recommended)
  - OR `WLED_SEGMENT_COUNT=4`
- `WLED_REPLICATE_TO_ALL_SEGMENTS=true`
  - If a generated look only specifies one segment, it will be replicated across all segments so the whole tree changes.

### Street-facing orientation (for 4-segment quarter trees)

If your tree is 4 segments representing 4 quarters around the circle, you can tell the service how your segments map to the street view. This enables friendly controls like:

- “start at **front**”
- “rotate **clockwise**”

Relevant vars:

- `QUAD_RIGHT_SEGMENT_ID` – which segment is on street-right (facing the house from the street)
- `QUAD_ORDER_FROM_STREET` – does segment order increase **cw** or **ccw** from the street viewpoint?
- `QUAD_DEFAULT_START_POS` – default for quadrant patterns (`front/right/back/left`)

Example (segment 0 is on street-right, IDs increase counterclockwise):

```env
QUAD_RIGHT_SEGMENT_ID=0
QUAD_ORDER_FROM_STREET=ccw
QUAD_DEFAULT_START_POS=front
```

### DDP streaming (realtime patterns)

- `DDP_HOST` – defaults to the host in `WLED_TREE_URL` if blank
- `DDP_PORT` – default `4048`
- `DDP_MAX_PIXELS_PER_PACKET` – keep modest for Wi‑Fi (default `480`)
- `DDP_FPS_DEFAULT` / `DDP_FPS_MAX`

### OpenAI (optional)

Enables `/v1/command`:

- `OPENAI_API_KEY`
- `OPENAI_MODEL` (default `gpt-5-mini`)
  - Recommended: `gpt-5-mini` (best reliability/$ for tool-calling)
  - Cheapest: `gpt-5-nano` (works, but can be less reliable with tool args)
  - Best quality: `gpt-5` / `gpt-5.2` (usually unnecessary for this toolset)

> Keep your API key server-side (in `.env` / container secrets). Do not embed it in a browser app.

When enabled, the director can call tools like:

- `apply_random_look`, `start_ddp_pattern`, `stop_all`
- `fleet_start_sequence`, `fleet_stop_sequence`
- `fpp_start_playlist`, `fpp_stop_playlist`, `fpp_trigger_event` (when `FPP_BASE_URL` is set)

### Web UI + local auth (optional)

- The UI is a **React + MUI + TypeScript** single-page app served at `GET /ui` (mobile friendly, includes text + voice command input).
- To enable local login (service-issued JWT cookie):
  - `AUTH_ENABLED=true`
  - `AUTH_USERNAME` / `AUTH_PASSWORD`
  - `AUTH_JWT_SECRET` (HMAC secret for HS256)
  - Optional 2FA: `AUTH_TOTP_ENABLED=true` + `AUTH_TOTP_SECRET` (base32)

Notes:

- When `AUTH_ENABLED=true`, all endpoints require either a valid JWT (cookie or `Authorization: Bearer <jwt>`) or the configured `X-A2A-Key` (if you also use A2A/fleet).
- `GET /v1/health`, `GET /v1/auth/config`, `POST /v1/auth/login`, `POST /v1/auth/logout`, and `GET /ui/*` remain accessible without a token so you can sign in.
- Generate a JWT secret:

```bash
python -c 'import secrets; print(secrets.token_urlsafe(32))'
```

- Optional: generate a PBKDF2 password hash for `AUTH_PASSWORD`:

```bash
python -c 'import sys; sys.path.insert(0,"agent"); from auth import hash_password_pbkdf2; print(hash_password_pbkdf2("changeme"))'
```

- Optional: generate a TOTP secret for `AUTH_TOTP_SECRET`:

```bash
python -c 'import sys; sys.path.insert(0,"agent"); from auth import totp_generate_secret; print(totp_generate_secret())'
```

### Falcon Player (FPP) (optional)

If you want the agent to control Falcon Player (playlist start/stop, event trigger):

- `FPP_BASE_URL` – base URL of your FPP instance (example: `http://172.16.200.20`)
- `FPP_HTTP_TIMEOUT_S` – HTTP timeout for FPP requests
- `FPP_HEADERS_JSON` – optional extra headers (JSON object) for auth (example: `{"Authorization":"Bearer <token>"}`)

### Database (optional)

- `DATABASE_URL` – SQLAlchemy URL (MySQL recommended). When set, job history + small UI state (scheduler config and `runtime_state`) are also persisted in SQL.
- With the included MySQL container: run `docker compose --profile db up -d --build` and set `DATABASE_URL=mysql://wsa:wsa@db:3306/wsa`.
- Retention (SQL only): `JOB_HISTORY_MAX_ROWS`, `JOB_HISTORY_MAX_DAYS`, `JOB_HISTORY_MAINTENANCE_INTERVAL_S`.
- Optional startup reconcile (SQL only): `DB_RECONCILE_ON_STARTUP=true` to scan `DATA_DIR` and backfill metadata tables.

### AI capability + cost (estimates)

Important: **Only** `POST /v1/command` uses model tokens. Everything else (looks/sequences/DDP/pixel streaming) is local and free.

Also note: “cool sequences” in this repo are generated locally via `POST /v1/sequences/generate` (no AI required). The model is only used to interpret natural language and choose which local tool/endpoints to call.

How many tokens per command?

- `POST /v1/command` usually results in **2 model calls** (tool selection + final confirmation).
- Typical total per command: **~700–1600 input tokens** + **~50–200 output tokens** (depends on prompt length and tool output size).

Show-window estimate (sunset→midnight ≈ **6 hours/night** for **40 days** ⇒ **240 hours**):

If you call `/v1/command` every **10 minutes** while the show runs:

- Calls: `6/hour * 240 hours = 1440`
- Total tokens (rough): **1.008M–2.304M input** + **0.072M–0.288M output**

Approx cost for that 40‑day run (using the token prices you provided):

| Model        | Input $/1M | Output $/1M | Cost for 1440 calls |
| ------------ | ---------: | ----------: | ------------------: |
| `gpt-5-nano` |       0.05 |        0.40 |     **$0.08–$0.23** |
| `gpt-5-mini` |       0.25 |        2.00 |     **$0.40–$1.15** |
| `gpt-5`      |       1.25 |       10.00 |     **$1.98–$5.76** |
| `gpt-5.2`    |       1.75 |       14.00 |     **$2.77–$8.06** |

Practical recommendation:

- Use `gpt-5-mini` on the coordinator (`.env.tree`) for the best experience.
- Increase call frequency if you like (cost stays low), but visually you usually want changes every **5–15 minutes**; more frequent can look chaotic and can cause more WLED state churn.
  - Cost scales ~linearly with cadence: every **5 min ≈ 2×** the table above; every **1 min ≈ 10×**.

---

## Core endpoints

Base URL below assumes you’re running locally: `http://localhost:8088`

### Status / diagnostics

- `GET /v1/health`
- `GET /livez` – liveness probe (always 200 if process is up)
- `GET /readyz` – readiness checks (WLED + DB)
- `GET /v1/wled/info`
- `GET /v1/wled/state`
- `GET /v1/wled/segments`
- `GET /v1/segments/layout`
- `GET /v1/segments/orientation`

### Looks

- `POST /v1/looks/generate` – generate a big look pack into `./data/looks/`
- `GET /v1/looks/packs` – list available packs
- `POST /v1/looks/apply_random` – apply a random look (no AI required)

### Preset import (optional)

- `POST /v1/presets/import_from_pack`

### Sequences

- `POST /v1/sequences/generate`
- `GET /v1/sequences/list`
- `POST /v1/sequences/play`
- `POST /v1/sequences/stop`
- `GET /v1/sequences/status`

### Fleet sequences (multi-controller)

Run a single generated sequence across your whole A2A fleet:

- `POST /v1/fleet/sequences/start`
- `POST /v1/fleet/sequences/stop`
- `GET /v1/fleet/sequences/status`

### DDP patterns (realtime)

- `GET /v1/ddp/patterns`
- `POST /v1/ddp/start`
- `POST /v1/ddp/stop`
- `GET /v1/ddp/status`

### Natural-language control (optional)

- `POST /v1/command`

### A2A (agent-to-agent) + fleet (multi-controller)

Use this when you run **multiple WLED controllers** (mega tree + rooflines) and want a single agent to coordinate them.

- `GET /v1/a2a/card` – agent metadata + supported actions
- `POST /v1/a2a/invoke` – invoke an action on this agent
- `GET /v1/fleet/peers` – list configured peer agents
- `POST /v1/fleet/apply_random_look` – pick a look on this agent and apply the same look spec to peers
- `POST /v1/fleet/invoke` – invoke any A2A action on peers (and optionally self)
- `POST /v1/fleet/stop_all` – stop sequences + DDP across the fleet

### Falcon Player (FPP) integration (optional)

- `GET /v1/fpp/status`
- `GET /v1/fpp/playlists`
- `POST /v1/fpp/playlist/start`
- `POST /v1/fpp/playlist/stop`
- `POST /v1/fpp/event/trigger`
- `POST /v1/fpp/request` – proxy a raw request to FPP (escape hatch)
- `POST /v1/fpp/export/fleet_sequence_start_script` – generate an FPP script that triggers a fleet sequence
- `POST /v1/fpp/export/fleet_stop_all_script` – generate an FPP script that stops the fleet

### xLights helpers (optional)

- `POST /v1/xlights/import_networks` – best-effort import of `xlights_networks.xml` to a show config skeleton
- `POST /v1/xlights/import_project` – import an xLights project folder (networks + model channel ranges)
- `POST /v1/xlights/import_sequence` – extract a timing/beat grid from an xLights `.xsq` (no effect data)
- `POST /v1/show/config/load` – load a show config JSON from `DATA_DIR`

### Jobs + progress (UI uses this)

- `GET /v1/jobs` – list recent jobs
- `GET /v1/jobs/stream` – Server-Sent Events (SSE) stream of job updates
- `POST /v1/jobs/*` – submit long-running tasks (looks generation, xLights import, audio analyze, sequence generate, `.fseq` export)
- Jobs are persisted under `DATA_DIR/jobs/jobs.json` and (optionally) to SQL when `DATABASE_URL` is set.

### File helpers (UI uses this)

- `GET /v1/files/list` – list files under `DATA_DIR`
- `GET /v1/files/download` – download a file under `DATA_DIR`
- `PUT /v1/files/upload?path=...` – upload raw bytes to a file under `DATA_DIR` (UI: Tools → Files)
- `DELETE /v1/files/delete?path=...` – delete a file under `DATA_DIR`

### Pack ingestion (UI uses this)

- `PUT /v1/packs/ingest?dest_dir=...&overwrite=true|false` – upload a `.zip` and unpack it under `DATA_DIR` (UI: Tools → Packs)
- Limits: `PACK_MAX_FILES`, `PACK_MAX_UNPACKED_MB`

### Scheduler (UI uses this)

Basic show-window automation (UI: Tools → Scheduler):

- `GET /v1/scheduler/status`
- `GET /v1/scheduler/config`
- `POST /v1/scheduler/config`
- `POST /v1/scheduler/start`
- `POST /v1/scheduler/stop`
- `POST /v1/scheduler/run_once`

### Metadata (SQL only)

UI-facing metadata backed by SQL (when `DATABASE_URL` is set):

- `GET /v1/meta/packs`
- `GET /v1/meta/sequences`
- `GET /v1/meta/audio_analyses`
- `GET /v1/meta/last_applied`
- `POST /v1/meta/reconcile` – scan `DATA_DIR` and upsert metadata rows

### Metrics

- `GET /v1/metrics` – lightweight JSON metrics (uptime, scheduler, current status)
- `GET /metrics` – Prometheus exposition format
  - When `AUTH_ENABLED=true`: set `METRICS_PUBLIC=true` or configure `METRICS_SCRAPE_TOKEN` + `METRICS_SCRAPE_HEADER`.

---

## Usage examples

### Confirm the service and WLED are reachable

```bash
curl -sS http://localhost:8088/v1/health
curl -sS http://localhost:8088/v1/wled/info | jq
```

### Confirm your segment layout (especially for 4×784 “quarters”)

```bash
curl -sS http://localhost:8088/v1/segments/layout | jq
curl -sS http://localhost:8088/v1/segments/orientation | jq
```

### Generate a big pack of looks

```bash
curl -sS http://localhost:8088/v1/looks/generate \
  -H "Content-Type: application/json" \
  -d '{
    "total_looks": 3000,
    "themes": ["classic","candy_cane","icy","warm_white","rainbow","halloween"],
    "brightness": 180,
    "seed": 1337,
    "write_files": true,
    "include_multi_segment": true
  }' | jq
```

List packs:

```bash
curl -sS http://localhost:8088/v1/looks/packs | jq
```

### Apply a random look (fast “DJ mode”)

```bash
curl -sS http://localhost:8088/v1/looks/apply_random \
  -H "Content-Type: application/json" \
  -d '{"theme":"candy_cane","brightness":120}' | jq
```

### Import a subset as WLED presets (optional)

WLED preset memory is limited; importing huge numbers repeatedly is not recommended.

```bash
curl -sS http://localhost:8088/v1/presets/import_from_pack \
  -H "Content-Type: application/json" \
  -d '{
    "pack_file":"<put pack filename here>",
    "start_id": 120,
    "limit": 50,
    "name_prefix":"AI",
    "include_brightness": true,
    "save_bounds": true
  }' | jq
```

### Generate a timed sequence (cue list)

```bash
curl -sS http://localhost:8088/v1/sequences/generate \
  -H "Content-Type: application/json" \
  -d '{
    "name":"CandyMix",
    "duration_s": 240,
    "step_s": 8,
    "include_ddp": true,
    "seed": 1337
  }' | jq
```

Generate a beat-aligned sequence (uses `beats.json` from `/v1/audio/analyze` or `/v1/xlights/import_sequence`):

```bash
curl -sS http://localhost:8088/v1/sequences/generate \
  -H "Content-Type: application/json" \
  -d '{
    "name":"BeatMix",
    "duration_s": 240,
    "include_ddp": true,
    "beats_file":"audio/beats.json",
    "beats_per_step": 4,
    "seed": 1337
  }' | jq
```

List sequences:

```bash
curl -sS http://localhost:8088/v1/sequences/list | jq
```

Play a sequence:

```bash
curl -sS http://localhost:8088/v1/sequences/play \
  -H "Content-Type: application/json" \
  -d '{"file":"<sequence filename>","loop":false}' | jq
```

Stop:

```bash
curl -sS -X POST http://localhost:8088/v1/sequences/stop | jq
```

Play a generated sequence across the whole fleet (coordinator only):

```bash
curl -sS http://localhost:8088/v1/fleet/sequences/start \
  -H "Content-Type: application/json" \
  -d '{"file":"<sequence filename>","loop":false}' | jq
```

### Start a realtime DDP pattern

List patterns:

```bash
curl -sS http://localhost:8088/v1/ddp/patterns | jq
```

Start a pattern:

```bash
curl -sS http://localhost:8088/v1/ddp/start \
  -H "Content-Type: application/json" \
  -d '{"pattern":"candy_spiral","duration_s":45,"brightness":120,"fps":25}' | jq
```

Quadrant-aware motion using friendly controls:

```bash
curl -sS http://localhost:8088/v1/ddp/start \
  -H "Content-Type: application/json" \
  -d '{
    "pattern":"quad_chase",
    "duration_s":45,
    "brightness":120,
    "fps":25,
    "direction":"cw",
    "start_pos":"front"
  }' | jq
```

Stop streaming:

```bash
curl -sS -X POST http://localhost:8088/v1/ddp/stop | jq
```

### Natural language director (optional)

Enable by setting `OPENAI_API_KEY` in `.env`.

```bash
curl -sS http://localhost:8088/v1/command \
  -H "Content-Type: application/json" \
  -d '{"text":"Do a clockwise quadrant chase starting at the front for 25 seconds, then switch to a warm white classy look."}' | jq
```

---

## Multi-controller setup (tree + rooflines)

Run one **API container per controller** (tree, rooflines, props). The coordinator also runs a separate `ui` reverse-proxy container that serves `/ui/*` and proxies `/v1/*` to the coordinator API.

An example multi-agent compose file is included: `docker-compose.fleet.yml`.

```bash
cp .env.tree.example .env.tree
cp .env.roofline1.example .env.roofline1
cp .env.roofline2.example .env.roofline2
cp .env.star_wled.example .env.star_wled
cp .env.star_esps.example .env.star_esps
# edit each .env.* and set the right WLED_TREE_URL (+ segments if needed)
docker compose -f docker-compose.fleet.yml up -d --build
```

Optional (for FPP script export):

- Copy `data/show/show_config.example.json` into the coordinator’s data dir (example: `./data/tree/show/show_config.json`) and edit `coordinator.base_url` to match the coordinator URL reachable from the FPP host.

Recommended env vars per agent:

- `AGENT_ID` / `AGENT_NAME` / `AGENT_ROLE` – identify the agent (`tree`, `roofline1`, `roofline2`, etc.)
- `A2A_API_KEY` – recommended shared key (set the same on all agents)

On the agent you want to use as the **fleet coordinator** (often the tree), set:

- `A2A_PEERS=roofline1=http://roofline1:8088,roofline2=http://roofline2:8088,star_wled=http://star_wled:8088,star_esps=http://star_esps:8088`

### Suggested addressing (172.16.200.0/24)

Keep controllers on static IPs in your `172.16.200.0/24` LAN/VLAN. Example device plan:

| Prop                 | Controller    | Device IP       | Agent service | Host port | Env file                            |
| -------------------- | ------------- | --------------- | ------------- | --------- | ----------------------------------- |
| Mega tree            | WLED          | `172.16.200.50` | `tree`        | `8088`    | `.env.tree`                         |
| Roofline 1st floor   | WLED          | `172.16.200.51` | `roofline1`   | `8089`    | `.env.roofline1`                    |
| Roofline 2nd floor   | WLED          | `172.16.200.52` | `roofline2`   | `8090`    | `.env.roofline2`                    |
| Star (WLED)          | WLED          | `172.16.200.53` | `star_wled`   | `8091`    | `.env.star_wled`                    |
| Star (ESPixelStick)  | sACN/Art‑Net  | `172.16.200.60` | `star_esps`   | `8092`    | `.env.star_esps`                    |
| Scheduler (optional) | Falcon Player | `172.16.200.20` | —             | —         | (set `FPP_BASE_URL` on coordinator) |

Notes:

- In `docker-compose.fleet.yml`, host port `8088` is served by the `ui` container (reverse proxy) which forwards `/v1/*` to the coordinator API.
- `WLED_TREE_URL` / `PIXEL_HOST` always point at the physical device IPs.
- `A2A_PEERS` can use docker service DNS names (as shown) when everything runs in one compose stack.

Adding more WLED props (stars, mini trees, etc):

- Duplicate a `.env.*.example` file, set `WLED_TREE_URL`, and add another service to `docker-compose.fleet.yml`.
- Add the new service to the coordinator’s `A2A_PEERS`.

ESPixelStick / non-WLED pixel controllers:

- Use the `pixel_main` app (already wired in `docker-compose.fleet.yml` via the `star_esps` service).
- Set `CONTROLLER_KIND=pixel` and the `PIXEL_*` settings in `.env.star_esps` (protocol, host, pixel count, universe start).
- These agents support realtime pattern streaming + `stop_all`, but do not support WLED looks/presets.

### Strategy: looks vs patterns

- Use `POST /v1/fleet/apply_random_look` when you want quick “theme” changes on **WLED-only** devices (tree/rooflines/star_wled).
- Use `POST /v1/fleet/invoke` with `action="start_ddp_pattern"` when you want a **single synced effect across everything**, including ESPixelStick props.

Example: start a solid red “all props” look for 5 minutes:

```bash
curl -sS http://localhost:8088/v1/fleet/invoke \
  -H "Content-Type: application/json" \
  -d '{
    "action":"start_ddp_pattern",
    "params":{
      "pattern":"solid",
      "duration_s":300,
      "brightness":140,
      "params":{"color":[255,0,0]}
    }
  }' | jq
```

### Universe strategy (future-proofing for xLights/FPP)

If you plan to sequence these props in xLights/Falcon Player later, assign universes in a global plan now:

- Use `PIXEL_CHANNELS_PER_UNIVERSE=510` (170 RGB pixels/universe) and unicast (`PIXEL_HOST=<device IP>`).
- For each prop, reserve `ceil(pixel_count*3 / 510)` universes; keep ranges non-overlapping across the show.
- Keep `PIXEL_UNIVERSE_START` (E1.31, 1-based) or `PIXEL_UNIVERSE_START` (Art‑Net Port‑Address, often 0-based) consistent with your xLights model definitions.

### Falcon Player (FPP) + xLights integration

Recommended approach: treat **FPP as the scheduler/timebase (audio + calendar)** and this agent fleet as the **effect engine**.

#### 1) FPP → Agent (triggers)

- Use the coordinator’s `/v1/fpp/export/*` endpoints to generate **FPP shell scripts** that call the coordinator’s `/v1/fleet/*` endpoints.
- Copy the generated scripts from the coordinator’s data dir (example: `./data/tree/fpp/scripts/`) onto the FPP host and attach them to Events or Playlists.
- Use `coordinator_base_url` as an IP/hostname reachable from the FPP host (Docker service names like `tree` will not resolve from FPP).

Trigger a generated sequence across the whole fleet:

1. Generate a sequence on the coordinator:

```bash
curl -sS http://localhost:8088/v1/sequences/generate \
  -H "Content-Type: application/json" \
  -d '{"name":"ShowMix","duration_s":240,"step_s":8,"include_ddp":true,"seed":1337}' | jq
```

2. Start it across the fleet:

```bash
curl -sS http://localhost:8088/v1/fleet/sequences/start \
  -H "Content-Type: application/json" \
  -d '{"file":"<sequence filename from /v1/sequences/list>","loop":false}' | jq
```

3. Export an FPP script to trigger that sequence:

```bash
curl -sS http://localhost:8088/v1/fpp/export/fleet_sequence_start_script \
  -H "Content-Type: application/json" \
  -d '{
    "sequence_file":"<sequence filename>",
    "coordinator_base_url":"http://172.16.200.10:8088",
    "out_filename":"start_showmix.sh",
    "include_a2a_key":true
  }' | jq
```

#### 2) Agent → FPP (control plane)

Set `FPP_BASE_URL` (and optionally `FPP_HEADERS_JSON`) on the coordinator, then use:

- `/v1/fpp/playlist/start` / `/v1/fpp/playlist/stop`
- `/v1/fpp/event/trigger`
- `/v1/command` can also call `fpp_start_playlist`, `fpp_stop_playlist`, and `fpp_trigger_event` when OpenAI is enabled.

#### 3) xLights helpers (best-effort)

- Place `xlights_networks.xml` (and optionally `xlights_rgbeffects.xml`) under the coordinator’s data dir (example: `./data/tree/xlights/`).
- Import networks-only to a show-config skeleton:

```bash
curl -sS http://localhost:8088/v1/xlights/import_networks \
  -H "Content-Type: application/json" \
  -d '{
    "networks_file":"xlights/xlights_networks.xml",
    "out_file":"show/show_config_xlights.json",
    "subnet":"172.16.200.0/24",
    "coordinator_base_url":"http://172.16.200.10:8088",
    "fpp_base_url":"http://172.16.200.20"
  }' | jq
```

Import an entire xLights project folder (networks + model channel ranges):

```bash
curl -sS http://localhost:8088/v1/xlights/import_project \
  -H "Content-Type: application/json" \
  -d '{
    "project_dir":"xlights",
    "out_file":"show/show_config_xlights_project.json",
    "include_controllers":true,
    "include_models":true
  }' | jq
```

Import a timing/beat grid from an xLights `.xsq` (for beat-aligned sequence generation):

```bash
curl -sS http://localhost:8088/v1/xlights/import_sequence \
  -H "Content-Type: application/json" \
  -d '{
    "xsq_file":"xlights/song.xsq",
    "timing_track":"Beat",
    "out_file":"audio/beats_xlights.json"
  }' | jq
```

Limitations right now:

- `.fseq` export is supported for **renderable** sequences only (procedural `ddp` steps). Steps of type `look` (WLED JSON states) are not offline-renderable into frames.
- `.fseq` upload to FPP is supported via `POST /v1/fpp/upload_file` (uploads into `sequences/` by default).
- xLights import is best-effort (networks + model channel ranges); `.xsq` import is limited to timing/beat grids only (no xLights effect data).

Future opportunity (music sync):

- The sequence generator supports beat-aligned step boundaries via `beats_file`; a future improvement is to align **pattern parameters** (speed, direction changes, palette switches) to the beat grid too, then let FPP trigger the sequence start so audio + visuals share a common timebase.

Audio analyzer (beats/BPM):

```bash
curl -sS http://localhost:8088/v1/audio/analyze \
  -H "Content-Type: application/json" \
  -d '{"audio_file":"music/song.wav","out_file":"audio/beats.json"}' | jq
```

OpenAI (optional):

- Put `OPENAI_API_KEY` in the coordinator’s env (`.env.tree`) if you want `/v1/command` to drive the whole fleet.
- If you want natural-language control on roofline agents directly, also set `OPENAI_API_KEY` in those env files.

Then you can apply a consistent look everywhere:

```bash
curl -sS http://localhost:8088/v1/fleet/apply_random_look \
  -H "Content-Type: application/json" \
  -d '{"theme":"candy_cane","brightness":140}' | jq
```

If you set `A2A_API_KEY`, add `-H "X-A2A-Key: <key>"` to calls to `/v1/a2a/*`, `/v1/fleet/*`, `/v1/fpp/*`, `/v1/xlights/*`, and `/v1/show/*`.

Note: `/v1/fleet/apply_random_look` automatically skips peers that don’t support `apply_look_spec` (e.g. ESPixelStick pixel agents).

Stop everything across all controllers:

```bash
curl -sS http://localhost:8088/v1/fleet/stop_all \
  -H "Content-Type: application/json" \
  -d '{}' | jq
```

---

## Data files

`./data` is mounted into the container at `/data`.

- `./data/looks/looks_pack_*.jsonl` – newline-delimited JSON look states
- `./data/sequences/sequence_*.json` – generated cue lists
- `./data/fseq/*.fseq` – exported `.fseq` files (renderable sequences only)
- `./data/audio/beats.json` – audio BPM + beat timestamps
- `./data/jobs/jobs.json` – job history (for the UI)
- `./data/show/scheduler.json` – scheduler config (for the UI)
- `./data/state/runtime_state.json` – last-known runtime state snapshot

If you set `DATABASE_URL`, the agent also persists job history + these small state files into SQL (and will read from SQL when available).

Safe to delete any time.

---

## Safety notes

- Keep this service private on your LAN (it can control your lights).
- Use `WLED_MAX_BRI` to protect your power setup.
- Keep `WLED_COMMAND_COOLDOWN_MS` > 0 to avoid rapid-fire updates.

---

## HTTPS on LAN (recommended for mobile voice + secure cookies)

Browsers commonly require a **secure context** (HTTPS) for microphone permissions, and `AUTH_COOKIE_SECURE=true` requires HTTPS.

1. Set:

- `AUTH_ENABLED=true`
- `AUTH_JWT_SECRET=...`
- `AUTH_COOKIE_SECURE=true`

2. Put a TLS terminator (Caddy/Traefik) in front of the `ui` container.

### Caddy (self-signed, easiest)

Example `Caddyfile` for a Docker network (proxy to the `ui` service):

```caddyfile
wsa.local {
  tls internal
  reverse_proxy ui:80
}
```

Trust Caddy’s internal CA on your phone (or use a real cert if you have one).

### Traefik (outline)

- Route a `websecure` router to the `ui` service on port `80`.
- Use Traefik’s TLS options/certs (self-signed or your own CA).
- Keep forwarding headers (`X-Forwarded-Proto`) so the app can make correct security decisions.

---

## Troubleshooting

**502 errors from `/v1/wled/*`:**

- Confirm `WLED_TREE_URL` is correct and reachable from the container host.
- If using Docker in an LXC, confirm the container can reach your VLAN/subnet.

**DDP patterns don’t show:**

- Confirm the WLED device is the same IP as `DDP_HOST`.
- Confirm nothing else is actively streaming realtime data to the device.

**WLED becomes briefly unresponsive when importing presets:**

- Import fewer at a time (e.g., 20–50). Preset import writes to flash.

---

## Development

UI dev server (mobile-friendly React app):

```bash
cd ui
npm install
npm run dev
# open http://localhost:5173/ui/ (Vite proxies /v1 to http://localhost:8088)
```

If you run the UI dev server without proxying `/v1` (different origin), set API CORS config (see `.env.example`).

UI E2E tests (Playwright):

```bash
cd ui
npm install
npx playwright install
npm run test:e2e
```

Run unit tests (no WLED/FPP required):

```bash
docker build -t wled-show-agent-test ./agent
docker run --rm wled-show-agent-test pytest -q
```

---

## License

MIT License. See `LICENSE`.
