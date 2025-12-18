# Data directory

This folder is mounted into the container at `/data`.

The agent writes:

- `looks/looks_pack_*.jsonl`
- `packs/*` (uploaded/unpacked zip packs + `manifest.json`)
- `sequences/sequence_*.json`
- `fseq/*.fseq` (optional) – exported `.fseq` files (renderable sequences only)
- `audio/beats*.json` (optional) – BPM + beat timestamps
- `fpp/scripts/*.sh` (optional) – exported Falcon Player trigger scripts
- `show/*.json` (optional) – show config files / xLights import skeletons
- `jobs/jobs.json` (optional) – job history (for the UI)
- `show/scheduler.json` (optional) – scheduler config
- `state/runtime_state.json` (optional) – last runtime snapshot

Safe to delete anytime.

If you set `DATABASE_URL`, job history + small UI state (scheduler config, runtime snapshot) are also persisted to SQL.
