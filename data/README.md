# Data directory

This folder is mounted into the container at `/data`.

The agent writes:

- `looks/looks_pack_*.jsonl`
- `sequences/sequence_*.json`
- `fseq/*.fseq` (optional) – exported `.fseq` files (renderable sequences only)
- `audio/beats*.json` (optional) – BPM + beat timestamps
- `fpp/scripts/*.sh` (optional) – exported Falcon Player trigger scripts
- `show/*.json` (optional) – show config files / xLights import skeletons

Safe to delete anytime.
