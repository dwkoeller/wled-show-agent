# Changelog

All notable changes to this project will be documented in this file.

## 12-18-2025

### Added

- Optional MySQL persistence (SQLModel) behind `DATABASE_URL` for job history and small UI state (scheduler config + runtime snapshot).
- Docker Compose `db` profile with a MySQL service (`docker-compose.yml`, `docker-compose.fleet.yml`).
- Async DB drivers support (`aiomysql`/`aiosqlite`).
- Backend refactor: app factory + lifespan wiring (`agent/app_factory.py`), routers split into `agent/routes/*`, and main backend module moved to `agent/services/app_state.py`.
- Pack ingestion: `PUT /v1/packs/ingest` (zip upload + unpack) and UI tab (Tools → Packs).
- Prometheus endpoint `GET /metrics` plus request/latency middleware.
- Liveness endpoint `GET /livez`.
- Readiness endpoint `GET /readyz` (checks WLED reachability + DB).
- SQL-backed metadata endpoints for UI: `GET /v1/meta/packs`, `/v1/meta/sequences`, `/v1/meta/audio_analyses`, `/v1/meta/last_applied`.
- Metadata reconciler: `POST /v1/meta/reconcile` and `DB_RECONCILE_ON_STARTUP`.
- Async “core loop”: async job manager + async scheduler execution with SSE job streaming (`GET /v1/jobs/{job_id}/stream`) and cancelation (`POST /v1/jobs/{job_id}/cancel`).
- Job history retention/maintenance via `JOB_HISTORY_MAX_ROWS`, `JOB_HISTORY_MAX_DAYS`, and `JOB_HISTORY_MAINTENANCE_INTERVAL_S`.
- Optional Prometheus scrape auth settings: `METRICS_PUBLIC`, `METRICS_SCRAPE_TOKEN`, `METRICS_SCRAPE_HEADER`.
- Outbound-call hardening: retries/backoff + per-target timeouts, plus Prometheus outbound failure/latency counters for WLED/FPP/peer requests.
- Multipart file uploads: `POST /v1/files/upload` (strict allowlist) and UI upload progress/validation (Tools → Files).
- `X-Request-Id` middleware and structured request logging.
- Docker Compose healthchecks for `api`/`ui` and fleet services.

### Fixed

- Removed tracked `__pycache__` artifacts; `.gitignore` ignores `*.pyc` and `__pycache__/`.

## 12-17-2025

### Added

- React + MUI + TypeScript UI (mobile-friendly) with dashboard + chat + voice input (`ui/`)
- JWT login with optional TOTP 2FA (`AUTH_TOTP_ENABLED`, `AUTH_TOTP_SECRET`)
- `GET /v1/auth/config` for UI bootstrap (`agent/main.py`)
- Beat/BPM extraction endpoint `POST /v1/audio/analyze` (writes `beats.json`)
- File manager endpoints + UI tab:
  - `PUT /v1/files/upload` (raw bytes upload to `DATA_DIR`)
  - `DELETE /v1/files/delete`
- xLights helpers:
  - `POST /v1/xlights/import_project` (networks + model channel ranges)
  - `POST /v1/xlights/import_sequence` (extract timing/beat grid from `.xsq`)
- Beat-aligned sequence generation via `POST /v1/sequences/generate` using `beats_file`
- `.fseq` v1 export for renderable (`ddp`) sequences and FPP file upload/discovery improvements
- Playwright E2E test scaffolding for the UI (`ui/playwright.config.ts`)
- Scheduler endpoints + UI tab (`/v1/scheduler/*`) for show-window automation
- `GET /v1/metrics` JSON metrics endpoint

### Changed

- Updated runtimes/deps: Python 3.14 (API image), Node 24 (UI build); bumped backend (FastAPI/Uvicorn/Pydantic/OpenAI/Pytest) and UI (React/MUI/Router/Vite/TS/Playwright) packages.
- Docker Compose now runs separate containers: `ui` (reverse proxy) and `api` (`docker-compose.yml`, `docker-compose.fleet.yml`)
- API image now includes `ffmpeg` to support non-WAV audio decoding for `/v1/audio/analyze` when `prefer_ffmpeg=true`.
- Sequence generation now fills the requested `duration_s` (shortens the final step instead of truncating)
- `/v1/command` falls back to a small local command parser when OpenAI is not configured
- Persisted job history under `DATA_DIR/jobs/jobs.json` and last runtime snapshot under `DATA_DIR/state/runtime_state.json`

### Fixed

- Fixed a lock re-entrancy bug in `stop()` methods that could deadlock sequence/stream services (and hang tests).
- Fixed IPv4 matching in the xLights networks importer.
