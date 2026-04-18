# Open Work List

## Track 3 — Group B implementation (decisions locked, see session_handoff_2026-04-18.md)

- Item 5: Audit convention sweep (scope: `IDENTITY_FIELDS`)
- Item 4: `audit.py` refactor to `run_audit(params) → dict`
- Item 10: Per-allocation capital sizing
- Item 6: VOL boost publication
- Item 9: Binance margin executor

## Operationally gated

- Retire `blofin_logger.py` cron after multi-tenant executor stable ≥ 7 days (starts counting from `spawn_traders` first cron tick)
- Resolve plaintext BloFin row under `admin@mullincap.com` (Option A delete, after Binance executor confirms live)

## Small polish (any time)

- Frontend "Last refreshed N ago" label on Allocator cards (Track 1 exposes `metrics_updated_at` in API)
- $25K allocation slider UI clamp (display-only, server-side enforcement will land in Track 3 item 10)

## Environment / infrastructure

- BASE_DATA_DIR drift between backend and celery services (see `docs/deferred_work.md`)

## Future (larger scope, not scheduled)

- Generic strategy executor dispatch (today's `trader-blofin.py` hardcodes Overlap logic)
- Manager module product work
- Publish more strategy variants (operational via Simulator UI, not code)
