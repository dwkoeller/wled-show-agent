# Session handoff — 2026-09-06

Deployment resumed and brought online. Private addresses and guest IDs are kept
in the deployment inventory, outside this repository.
- Application: `http://<APP_IP>/`
- Falcon Player: `http://<FPP_IP>/`
- Credentials and TOTP setup: private host secret store.
- WLED controllers: `<WLED_TREE_IP>` (coordinator/tree) and `<WLED_SECONDARY_IP>`
  (second WLED agent). They are currently powered off, so HTTP readiness checks
  will remain unavailable until the controllers are switched on.

Fixed MySQL startup failure: revision 0003 is longer than Alembic's default
VARCHAR(32) version column. env.py now creates/widens the MySQL version column
to VARCHAR(128), retaining published revision IDs. Alembic logging now preserves
existing loggers so startup exceptions remain visible.

Recovered partially recorded 0003 using recover_migration_0003.py, which checked
every expected column and index before advancing the version marker. No tables
or show data were removed. Recovery script is only for the specific 0002 marker
with fully applied 0003 DDL; do not rerun against the completed deployment.

Validation:
- 85 backend tests passed.
- React production build passed.
- 8 mocked desktop/mobile chat browser checks passed.
- Live verifier passed authenticated login/logout, root and nested React routes,
  180 /api OpenAPI routes, ChatGPT runtime, FPP status and playlists.

Remaining user setup:
- Sign in to the web application, then connect ChatGPT using its sign-in control.
  Account endpoint currently reports connected=false; real model replies have
  not been tested with an authenticated subscription.
- Configure DDP destination and geometry after powering the controllers. No
  lighting playback was started.

Frontend Docker health check uses explicit IPv4 loopback because localhost
resolved to IPv6 while Nginx listened on IPv4.

All accumulated project edits remain uncommitted in the local worktree.
