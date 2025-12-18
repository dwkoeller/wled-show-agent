# Changelog

All notable changes to this project will be documented in this file.

## 12-17-2025

### Added

- React + MUI + TypeScript UI (mobile-friendly) with dashboard + chat + voice input (`agent/ui/`)
- JWT login with optional TOTP 2FA (`AUTH_TOTP_ENABLED`, `AUTH_TOTP_SECRET`)
- `GET /v1/auth/config` for UI bootstrap (`agent/main.py`)
- Beat/BPM extraction endpoint `POST /v1/audio/analyze` (writes `beats.json`)
- xLights helpers:
  - `POST /v1/xlights/import_project` (networks + model channel ranges)
  - `POST /v1/xlights/import_sequence` (extract timing/beat grid from `.xsq`)
- Beat-aligned sequence generation via `POST /v1/sequences/generate` using `beats_file`
- `.fseq` v1 export for renderable (`ddp`) sequences and FPP file upload/discovery improvements
- Playwright E2E test scaffolding for the UI (`agent/ui/playwright.config.ts`)

### Changed

- Updated runtimes/deps: Python 3.14 (API image), Node 24 (UI build); bumped backend (FastAPI/Uvicorn/Pydantic/OpenAI/Pytest) and UI (React/MUI/Router/Vite/TS/Playwright) packages.
- Docker Compose now runs separate containers: `ui` (reverse proxy) and `api` (`docker-compose.yml`, `docker-compose.fleet.yml`)
- Sequence generation now fills the requested `duration_s` (shortens the final step instead of truncating)
- `/v1/command` falls back to a small local command parser when OpenAI is not configured

### Fixed

- Fixed a lock re-entrancy bug in `stop()` methods that could deadlock sequence/stream services (and hang tests).
- Fixed IPv4 matching in the xLights networks importer.
