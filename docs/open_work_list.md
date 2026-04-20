# Open Work List

## Track 3 — Group B implementation (decisions locked, see session_handoff_2026-04-18.md)

- ✅ Item 5: Audit convention sweep (scope: `IDENTITY_FIELDS`) — SHIPPED in `9b14233` (docs-only, zero convention mismatches)
- ✅ Item 4: `audit.py` refactor to `run_audit(params) → dict` — SHIPPED in `1199708`
- ✅ Item 10: Per-allocation capital sizing — SHIPPED in `5a7bdc7`
- ✅ Item 6: VOL boost publication — SHIPPED in `f26d460`
- Item 9: Binance margin executor
  - **HARD PREREQUISITE before any live BloFin allocation deploys under Item 9:** exercise Items 10 + 6 end-to-end with real signals. The harness that validated Item 10 at commit time + the refresh-side verification that validated Item 6 did NOT cover FOUR integration gaps:
    - `connection_id` threading through `run_session_for_allocation` → `_run_fresh_session_for_allocation` → `_account_advisory_lock` (Item 10)
    - `TRADER_LOCK_TEST_SLEEP_S` env-var scaffolding activation inside the live CLI flow (Item 10)
    - Phase 5's integration with the surrounding CLI phases: signal load, conviction check, credential load, monitoring loop handoff (Item 10)
    - Phase 4's `vol_boost` read + `l_high × vol_boost = eff_lev` log format: the caller-side read of `strategy_version.current_metrics["vol_boost"]` threading through `_run_fresh_session_for_allocation`'s new `vol_boost: float` param (Item 6)
  - These gaps only resolve when a real BloFin allocation activates with signals present AND conviction passing. Item 9 is the session where that window opens. Before Item 9's live-money deploy, reproduce the harness-plus-end-to-end check covering all four paths; document results in that session's handoff.

## Operationally gated

- Retire `blofin_logger.py` cron after multi-tenant executor stable ≥ 7 days (starts counting from `spawn_traders` first cron tick)
- Resolve plaintext BloFin row under `admin@mullincap.com` (Option A delete, after Binance executor confirms live)

## Small polish (any time)

- Frontend "Last refreshed N ago" label on Allocator cards (Track 1 exposes `metrics_updated_at` in API)
- $25K allocation slider UI clamp (display-only, server-side enforcement will land in Track 3 item 10)
- Avg lev column on Allocator strategy cards (UI follow-up — surfacing effective leverage per strategy at-a-glance)

## Environment / infrastructure

- BASE_DATA_DIR drift between backend and celery services (see `docs/deferred_work.md`)

## Future (larger scope, not scheduled)

- Generic strategy executor dispatch (today's `trader-blofin.py` hardcodes Overlap logic)
- Manager module product work
- Publish more strategy variants (operational via Simulator UI, not code)
