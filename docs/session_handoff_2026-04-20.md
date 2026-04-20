# Session Handoff — 2026-04-20 (Track 3 Session C close-out)

**Session C = Track 3 Session C.** Predecessor doc: [session_handoff_2026-04-19.md](session_handoff_2026-04-19.md) (Sessions A + B + Session C kickoff state).

Session C shipped two live-path fixes + one housekeeping note. Item 9 (Binance executor) deferred to Session D on live-money safety grounds. Closing on strength.

---

## Tomorrow's first action (Session D)

**1. Verify the 01:30 UTC nightly cron tick cleared clean.**

```bash
ssh mcap 'tail -50 /mnt/quant-data/logs/trader/refresh_metrics.log'
ssh mcap 'docker exec -i timescaledb psql -U quant -d marketdata -c "
  SELECT s.display_name,
         sv.metrics_updated_at,
         sv.metrics_data_through,
         sv.current_metrics->>'\''sharpe'\'' AS sharpe,
         sv.current_metrics->>'\''vol_boost'\'' AS vol_boost
  FROM audit.strategy_versions sv
  JOIN audit.strategies s ON s.strategy_id = sv.strategy_id
  WHERE sv.is_active = TRUE AND s.is_published = TRUE
  ORDER BY sv.strategy_id;"'
```

Expected shape: three alpha strategies with `metrics_updated_at` in the 01:30–02:00 UTC window 2026-04-21, `metrics_data_through` = 2026-04-20, sharpe ≈ 3.5064 (still leverage-invariant), `vol_boost` populated on all three (Item 6's first unattended production run).

**2. Check conviction state on today's (2026-04-20) signal row:**

```bash
ssh mcap 'docker exec -i timescaledb psql -U quant -d marketdata -c "
  SELECT signal_date, filter_name, conviction_roi_x, conviction_kill_y,
         conviction_passed, sit_flat
  FROM user_mgmt.daily_signals
  WHERE signal_date >= CURRENT_DATE - 1
  ORDER BY signal_date DESC, filter_name;"'
```

This is the **hard prerequisite for Item 9**. If `conviction_passed = TRUE` on a Tail-Guardrail row for today, the 4-path integration gate can run against real signals. If FALSE, see "If conviction fails" below.

**3. Decision branch:**
- Cron green + conviction passes → proceed to Item 9 investigation → 4-path integration gate exercise
- Cron green + conviction fails → document, see "If conviction fails" below
- Cron red (any strategy fails refresh) → pause Item 9, debug refresh first

---

## What shipped — Session C (2026-04-20)

### v1.0 housekeeping note
Commit: `4b80b7f` (docs-only, `docs/deferred_work.md`)
Documented why `strategy_id=1 / v1.0` (overlap_tail_disp) is excluded from nightly refresh despite `is_active=TRUE`: parent strategy is `is_published=FALSE`. Intentional by design; note pre-empts future rediscovery.

### Item 6 — VOL boost publication
Status: **COMPLETE, verified across all three active alpha strategies.**
Commit: `f26d460` (4 files: new `backend/app/services/audit/vol_boost.py`; modified `trader_blofin.py`, `refresh_strategy_metrics.py`, `trader_config.py`). Handoff addition in `78869f5`.

**Decisions ratified at investigation:** 6.1 (b) new `compute_strategy_vol_boost(rets)` sibling in a service module (not same-file as legacy); 6.3 JSONB (`current_metrics["vol_boost"]` as first-class key, no migration); 6.new accept NULL-after-promote lag (Phase 4 defaults boost=1.0, matches host fallback); 6.new-2 picked-filter returns (use `filter_mode`'s fees_table, not best_filter's).

**Verification:** three-strategy refresh produced vol_boost 1.4767–1.4768 (saturation — see note below), metrics_updated_at bumped, sharpe + data_through unchanged. Post-deploy smoke: `compute_strategy_vol_boost` imports clean from fresh image on both backend + celery; Phase 4 signature has `vol_boost` param; TraderConfig field count 21 (dead `vol_boost_enabled` removed).

**Saturation observation:** today's three-strategy vol_boost is leverage-invariant (~1.477) because trailing 30d realized_vol is small (many flat/no-entry days) → vc stage saturates at `MAX_BOOST=2.0`; sc stage is mathematically leverage-invariant; dg stage structurally 1.0. `eff_lev` differentiation lives entirely in `l_high` under today's conditions. Not a bug — preserved in handoff so future readers don't flag near-identical boost values.

**Gap:** Phase 4 live-exercise not performed this session (no active BloFin allocations, no signals yet). Folds into Item 9's 4-path integration gate.

### active_filter normalization
Status: **COMPLETE, verified against two broken prod rows + factory-default regression check.**
Commit: `f9491a1` (2 files: `trader_config.py`, `simulator.py`). Handoff addition in `d75b6a7`.

**Bug closed:** Simulator promote persisted UI label form `"A - Tail Guardrail"` into `strategy_version.config.active_filter`; live trader compared against `live_deploys_signal.csv / daily_signals.filter_name` canonical form `"Tail Guardrail"`. Case-insensitive equality didn't bridge the gap. **Two of three published strategies (Low lev, High lev) had the broken label-form persisted today** — any BloFin allocation against them would have silently traded zero symbols. Strategy 2 (Med lev) worked by accident (key absent, factory default happened to match canonical).

**Fix:** new `_canonicalize_filter_name()` helper in `trader_config.py` strips strict `^[A-Z] - ` prefix; applied inside `TraderConfig.from_strategy_version` factory boundary (matches port_tsl normalize-at-boundary pattern). Simulator write site unchanged; comment expanded to document normalize-downstream contract. No migration / hash churn.

**Verification:** 3-case smoke harness (label / canonical / absent) all resolve to `'Tail Guardrail'`; live DB verification against both broken prod rows + regression check on strategy 2; post-deploy smoke clean in both containers.

**Scope:** scoped to live-trading paths only. Nightly refresh + Item 6 vol_boost use `audit.strategies.filter_mode` directly via SQL, never reading `config.active_filter` — unaffected.

---

## Why Item 9 deferred — investigation complete, defer to Session E or later

Session C included an Item 9 investigation pass after the initial defer decision was revisited. The investigation confirmed the defer is correct, and produced the authoritative scope + decision-points list for the next architectural ratification session. Full findings:

### Investigation summary (Session C, ~05:15 UTC)

**Critical finding: `backend/app/services/exchanges/binance.py` is READ-ONLY by design.** From its own docstring:
> "Minimal read-only Binance REST client for account introspection. Scope: permissions probe + spot balance + optional futures balance. **No trade endpoints.** HMAC-SHA256 signing via stdlib to avoid adding a dependency."

`BinanceClient` has: `get_permissions()`, `get_spot_account()`, `get_futures_account()`, `get_margin_account()`. **Zero trade endpoints.** `BlofinREST` at `trader_blofin.py:606-727` has the full trade surface (`set_leverage`, `place_order` with inline SL, `close_position`, `get_fills_history`, etc.).

### Exchange gates inventoried across the codebase

| Site | Behavior |
|---|---|
| `allocator.py:815` | `SUPPORTED_EXCHANGES = {"binance", "blofin"}` — API accepts both for credential onboarding |
| `spawn_traders.py:53` | `SUPPORTED_EXCHANGES = {"blofin"}` — SQL filters Binance out of eligible allocations |
| `spawn_traders.py:107` | Query filter via `ec.exchange = ANY(%s)` |
| `trader_blofin.py:3323` | `if creds.exchange != "blofin": phase="skipped"; return` — second gate |
| `trader_blofin.py:3338` | Hardcoded `api = BlofinREST(...)` — no dispatch |
| `permissions.py:162,177,226,245` | Exchange-specific READ validation branches (exist, read-only) |
| `allocator.py:319/341, 757/759, 881` | Exchange-specific credential onboarding branches (exist) |

**Read-side is already exchange-agnostic. Write-side is BloFin-only at every layer.**

### $20 Binance allocation — current code paths

- **Today (both gates in place):** `spawn_traders` SQL filter excludes it → never reaches trader. Dormant. Safe.
- **If only spawn_traders filter lifted:** subprocess launches → hits `:3323` skip check → `phase="skipped"` → exits cleanly. Trades zero.
- **If both gates lifted, no other changes:** `BlofinREST` gets instantiated with Binance credentials → first `api.get_balance()` call hits `openapi.blofin.com` with Binance keys → non-deterministic failure (401 or silent wrong-account data). **Unsafe.**

### Scope estimate for Item 9 implementation: **12–20h**

1. Extend `BinanceClient` with trade endpoints (~3–5h): `set_leverage`, `set_marginType`, `place_order` (MARKET + STOP_MARKET), `get_positions` via `/fapi/v2/positionRisk`, close-position synthesis via reduce-only, fills history, exchange info for tick/step/min_notional.
2. Abstraction/dispatch in trader (~2–4h): adapter pattern ratification + implementation.
3. Symbol + metadata translation (~2–3h): `BTC-USDT` ↔ `BTCUSDT`, per-exchange contract-size caching.
4. Margin mode semantics (~1–2h): BloFin per-order vs Binance per-symbol one-time setup.
5. Stop-loss/TSL synthesis (~2h): BloFin inline vs Binance separate STOP_MARKET with rollback semantics on entry-fail.
6. Gate lifts + credential dispatch (~30m — trivial, last).
7. Testing (~2–4h): testnet harness OR minimum-size real trades, 4 dry-run scenarios on Binance path.
8. Plus the Items-10+6 BloFin 4-path integration gate exercise (separate, prereq-blocked today).

### Gate recommendation — DEFER (accepted)

`$19.29` balance caps **position-sizing** blast radius; does NOT cap **code-surface** blast radius. Bugs in `place_order`, `close_position`, or `set_marginType` can create orphaned Binance positions, corrupt connection-level margin state for future allocations, or leak funding/liquidation risk past the $19.29 cap once leverage applies.

All three defer criteria met: scope >>2h; multiple unratified architectural decisions; 4-path prereq gate still blocked.

---

## Session D kickoff — architectural ratification (NOT implementation)

Session D is a **scope-and-decisions session**, not a code session. Code ships in Session E or later after decisions are locked. This mirrors the Item 10 Session A/B split where investigation + decision-pre-commits came before implementation.

### Six decisions to ratify in Session D

1. **Adapter pattern** — choose one: (a) `ExchangeAdapter` ABC with `BlofinAdapter`, `BinanceAdapter` concretes; (b) sibling `trader_binance.py` module + shared core helpers; (c) inline branching at each `creds.exchange` site. Recommend (a) but ratify explicitly with interface signature (what methods + return shapes).

2. **Binance trade-endpoint scope + signing** — enumerate the endpoints to add to `BinanceClient`, decide on signing helpers (currently pure stdlib HMAC-SHA256 — stays that way, or switch to `python-binance` SDK?). Decide error-code mapping strategy for Binance-specific trade-time codes (-1013, -2019, -4164, etc.).

3. **Stop-loss synthesis semantics** — BloFin places SL inline in entry `place_order` body (atomic). Binance requires separate STOP_MARKET order. Decide rollback semantics when entry fills but SL placement fails (close entry immediately? mark position risky? retry?). Decide ordering: entry-then-SL, or reserve-SL-then-entry.

4. **Margin mode lifecycle** — per-spawn warming (set_marginType once per symbol during spawn_traders) vs per-session (each `_run_fresh_session_for_allocation` sets margin mode on its symbols). Affects idempotency and blast radius if a symbol's margin mode gets corrupted.

5. **Symbol / metadata translation ownership** — where does `BTC-USDT` ↔ `BTCUSDT` mapping live: config constants, adapter-owned helper, per-call translation? Where does contract metadata (tick/step/min_notional) get cached: adapter instance attribute, Redis, in-memory module dict?

6. **Testnet vs real-money minimum-trade verification** — Binance testnet exists but has quirks (different recv_window behavior, limited symbol availability, stale price feeds). Real-money with minimum-size positions on Binance mainnet is safer for correctness but risks small losses during iteration. Decide strategy before any Binance code deploys.

### Other Session D work that doesn't depend on Item 9

- The 4-path BloFin integration gate — executable on any day BloFin conviction passes. Treat it as independent work: create scratch BloFin allocation under j@mullincap.com's BloFin connection, exercise the 4 paths on that day's 06:00 UTC session, soft-close allocation afterward. Small-session work (~1-2h).
- Small polish from [open_work_list.md](open_work_list.md): "Last refreshed N ago" label; $25K slider clamp; Avg lev column.
- Deferred items from [deferred_work.md](deferred_work.md): CAPITAL_MODE cleanup (zero-risk cosmetic), strategy taxonomy batch rename, BASE_DATA_DIR env drift.

### Expected Session D output

- Written ratification of decisions 1-6 above (e.g., updated `docs/open_work_list.md` with a dedicated Item 9 scope block + decision hashes)
- NO code changes to `trader_blofin.py` or `binance.py`. Maybe a spec doc. Code ships in Session E.
- Optionally, the 4-path BloFin gate exercise if conviction permits (independent of Item 9 decisions).

### If tonight's 2026-04-20 06:00 UTC session has conviction pass on BloFin

The master BloFin trader at 06:00 UTC runs independently of Item 9. If conviction passes on its ACTIVE_FILTER row, the 4-path gate exercise becomes available for Session D. Check `daily_signals.conviction_passed` post-06:00 UTC.

---

## Remaining work summary

**Track 3 Group B** ([open_work_list.md](open_work_list.md)):
- ✅ Item 5 (convention sweep) — shipped `9b14233`
- ✅ Item 4 (audit refactor) — shipped `1199708`
- ✅ Item 10 (per-allocation capital sizing) — shipped `5a7bdc7`
- ✅ Item 6 (VOL boost publication) — shipped `f26d460`
- Item 9 (Binance executor) — **investigation COMPLETE (Session C); 12-20h implementation + 6 architectural decisions unratified. Session D = ratification-only; code ships Session E or later.** 4-path BloFin integration gate remains HARD prerequisite and can be exercised independently whenever BloFin conviction passes.

**Operationally gated:**
- Retire `blofin_logger.py` cron after multi-tenant stable ≥ 7 days
- Resolve plaintext BloFin row under admin@mullincap.com (after Binance executor live)

**Small polish:**
- Allocator "Last refreshed N ago" label
- $25K slider clamp UI
- Avg lev column on Allocator cards

**Deferred work** ([deferred_work.md](deferred_work.md)):
- CAPITAL_MODE/CAPITAL_VALUE dead-code cleanup (zero-risk cosmetic)
- Strategy taxonomy decision + batch rename
- BASE_DATA_DIR env drift
- ~~active_filter string-namespace~~ — RESOLVED Session C
- v1.0 refresh exclusion — intentional-by-design note

---

## Production state at Session C close

- **Active published strategy versions:** 3 alpha variants unchanged from Session B:
  - `alpha_tail_guardrail_low_risk` / "Alpha Tail Guardrail - Med lev" (strategy_id=2, sv `6b6168b0-…`)
  - `alpha_tail_guardrail_low_lev` / "Alpha Tail Guardrail - Low lev" (strategy_id=3, sv `3100d339-…`)
  - `alpha_tail_guardrail_high_lev` / "Alpha Tail Guardrail - High lev" (strategy_id=4, sv `987312fd-…`)
- **Active allocations:** 1, unchanged. `686077fc-82a4-45e4-872f-47f7ad328780` — $20 Binance, j@mullincap.com, strategy_id=2. Dormant (spawn_traders `SUPPORTED_EXCHANGES={"blofin"}`). Item 9 will be the first session where this activates.
- **Connection balances** (~04:00 UTC): BloFin $3,951.03; Binance $19.29.
- **Deploy state:** commit `f9491a1` on `main` (+ docs-only `d75b6a7`), code deployed via `./redeploy.sh` at ~04:55 UTC 2026-04-20 post-active_filter fix. All 5 containers healthy.
- **Nightly metrics state (from pre-fix Session C manual refresh runs):** all three alpha strategies have `vol_boost` populated in `current_metrics` JSONB; metrics_updated_at in 04:11–04:26 UTC window; data_through=2026-04-19; sharpe=3.5064. Tonight's 01:30 UTC cron will be the first unattended vol_boost population on tomorrow's data.

## Cron state (unchanged)

```
0 6 * * *    [host]               master BloFin trader
5 6 * * *    [container:backend]  spawn_traders
30 1 * * *   [container:celery]   refresh_strategy_metrics
*/5 * * * *  [host]               blofin_logger.py (legacy — retire after multi-tenant stable ≥ 7d)
*/5 * * * *  [container]          sync_exchange_snapshots
+ metl 00:15, coingecko 00:30, indexer 01:00, caggs 01:15–18, overlap 01:20
+ manager briefing 00:30, sync-to-storage 02:00, signal 05:58, certbot 03:00
```

## Guardrails (active across all future sessions)

- `/root/benji/trader-blofin.py` on host — NEVER modified.
- Master cron line at `0 6 * * *` — NEVER modified.
- `audit.jobs` + `audit.results` — append-only; nightly runs add rows, never update/delete.
- User allocations + exchange_connections — no DELETE; soft-close via status column.
- Any live-money execution path change — smoke test against real allocation in dry-run before cron integration.
- Redeploy — check `celery inspect active` before `./redeploy.sh` to avoid `--force-recreate` killing in-flight jobs.
- **Item 9 specifically**: never ship Binance executor code without the 4-path integration gate passing cleanly against real signals. This gate is mechanical, not judgment-based.

## Commits added this session

- `4b80b7f` — v1.0 refresh exclusion note (housekeeping, docs-only)
- `f26d460` — **Item 6: strategy-level vol_boost publication + per-allocation read**
- `78869f5` — handoff update: Item 6 shipped + Item 9 gate expanded to 4 paths
- `f9491a1` — **active_filter normalization: TraderConfig factory-boundary canonicalization** (closes latent silent-zero-trade bug on Low lev / High lev)
- `d75b6a7` — handoff update: active_filter resolution + deferred_work cross-reference

Five commits on `main`. All pushed to origin; code commits deployed via redeploy; docs commits in working copy only (mcap behind by one docs commit at `f9491a1` — cosmetic).
