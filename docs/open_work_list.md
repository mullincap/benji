# Open Work List

## Track 3 — Group B implementation (decisions locked, see session_handoff_2026-04-18.md)

- ✅ Item 5: Audit convention sweep (scope: `IDENTITY_FIELDS`) — SHIPPED in `9b14233` (docs-only, zero convention mismatches)
- ✅ Item 4: `audit.py` refactor to `run_audit(params) → dict` — SHIPPED in `1199708`
- ✅ Item 10: Per-allocation capital sizing — SHIPPED in `5a7bdc7`
- ✅ Item 6: VOL boost publication — SHIPPED in `f26d460`
- Item 9: Binance margin executor — **INVESTIGATION COMPLETE (Session C); implementation + architectural ratification OUTSTANDING**

  **Investigation findings (see [session_handoff_2026-04-20.md](session_handoff_2026-04-20.md) for full report):**
  - `backend/app/services/exchanges/binance.py` is **READ-ONLY by design** — no trade endpoints. `BinanceClient` has only: `get_permissions`, `get_spot_account`, `get_futures_account`, `get_margin_account`. Compare to `BlofinREST` at `trader_blofin.py:606-727` which has the full `set_leverage` + `place_order` + `close_position` surface.
  - Two exchange gates currently block the $20 Binance allocation: `spawn_traders.py:53` SQL filter + `trader_blofin.py:3323` hardcoded `creds.exchange != "blofin"` check. Lifting both without other changes makes a BloFin-client-with-Binance-creds call to `openapi.blofin.com` — unsafe.
  - Scope: **12-20h implementation** across Binance trade-endpoint extension (~3-5h), adapter pattern in trader (~2-4h), symbol + metadata translation (~2-3h), margin mode semantics (~1-2h), stop-loss synthesis + rollback (~2h), gate lifts (~30m), testing (~2-4h).

  **Six architectural decisions to ratify in Session D (ratification-only session; no code):**
  1. Adapter pattern: `ExchangeAdapter` ABC with concretes, OR sibling `trader_binance.py`, OR inline branching
  2. Binance trade-endpoint scope + signing (stdlib HMAC-SHA256 stays, or adopt `python-binance` SDK?)
  3. Stop-loss synthesis semantics (rollback when entry fills but SL placement fails; ordering)
  4. Margin mode lifecycle (per-spawn warming vs per-session setup)
  5. Symbol / metadata translation ownership (config vs adapter vs per-call)
  6. Testnet vs real-money minimum-trade verification strategy

  **Session D = scope + decisions. Session E or later = implementation.** Pattern mirrors Item 10's A/B split.

  **4-path BloFin integration gate — HARD PREREQUISITE, independent of Item 9 decisions:** before any Binance live-money deploy, exercise these four paths against a real BloFin allocation with today's signals (conviction passing):
    - `connection_id` threading through `run_session_for_allocation` → `_run_fresh_session_for_allocation` → `_account_advisory_lock` (Item 10)
    - `TRADER_LOCK_TEST_SLEEP_S` env-var scaffolding activation inside the live CLI flow (Item 10)
    - Phase 5's integration with the surrounding CLI phases: signal load, conviction check, credential load, monitoring loop handoff (Item 10)
    - Phase 4's `vol_boost` read + `l_high × vol_boost = eff_lev` log format: the caller-side read of `strategy_version.current_metrics["vol_boost"]` threading through `_run_fresh_session_for_allocation`'s new `vol_boost: float` param (Item 6)

  This gate can be exercised on any day BloFin conviction passes — does not need to co-occur with Item 9 implementation. Small-session work (~1-2h) using a scratch BloFin allocation under j@mullincap.com's BloFin connection. Document results in that session's handoff.

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
