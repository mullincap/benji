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

---

# SESSION D — Binance Margin Executor Shipped

Session D executed on the same date (2026-04-20) after the Session C close-out above. The defer-to-Session-E recommendation from Session C's Item 9 investigation was **overridden by the user** with explicit acknowledgment of scope (12–20h estimated) and risk (6 unratified architectural decisions, state-coupling past the $20 cap, no prior Binance trade-endpoint code). The user opted to ship implementation in a single session.

## Goal → outcome

- **Goal**: ship a Binance executor using **cross margin** (not futures, explicitly chosen), activate live at the 06:05 UTC spawn window, unblock the dormant `$20` Binance allocation (`686077fc-…`).
- **Outcome**: code shipped as commit `592206a` on `main`, deployed to mcap at approximately 07:53 UTC 2026-04-20. Both BloFin + Binance subprocesses will spawn at **2026-04-21 06:05 UTC** (today's 06:05 UTC window fired during the deploy itself with the old filter, as designed).

## Six decisions ratified

| # | Topic | Choice |
|---|---|---|
| 0 | Margin mode | **Cross margin** (isolated explicitly rejected — simpler on a $20 account, 3x BTCUSDT tier) |
| A | Adapter pattern | **`ExchangeAdapter` ABC with concrete adapters** (`BloFinAdapter`, `BinanceMarginAdapter`); single dispatch point at `trader_blofin.py:3312` |
| B | Entry mechanic | **B3**: `MARGIN_BUY` (auto-borrow USDT) primary → on `-3006` (USDT borrow limit), fallback to `create_margin_loan(asset="USDT", amount=shortfall×1.005)` + `NO_SIDE_EFFECT BUY` |
| C | Symbol universe | **C1**: strict pre-filter at session start against `client.get_margin_all_pairs()` (30-min TTL cache); size across survivors; terminal `phase=filtered` if zero remain |
| D | Leverage semantics | **D3**: `set_leverage` becomes a no-op verification — calls `get_max_margin_loan(asset="USDT")`, returns `success=True iff requested ≤ (collateral + max_borrow_usdt) / collateral`. Error code `INSUFFICIENT_BORROW_LIMIT` if not achievable. |
| #2 | Post-borrow failure | **(ii)**: immediate `repay_margin_loan(asset="USDT")` on order failure after loan succeeded. Keeps interest exposure at zero on failed entries. |

## Four mid-session corrections

Applied after first-draft review flagged gaps:

1. **Sync confirmed**: 56 functions in `trader_blofin.py`, zero `async def` / `await` / `asyncio` / `AsyncClient`. Sync ABC + python-binance sync `Client` locked in.
2. **`get_recent_fills` signature**: gained `since_ms: int | None = None` and `inst_ids: list[str] | None = None`. Without explicit `inst_ids`, just-closed positions vanish from reconciliation — caller-responsibility documented in the Binance adapter's docstring.
3. **`FillInfo` shape**: added `order_id: str` and `size: float`. Exit reconciliation without `order_id` is ambiguous when multiple reduce orders are in flight or a partial fill splits into records.
4. **`native_sl_supported` as class attribute** (not `@property`): checkable without instantiation, zero method-call overhead in the hot path at [trader_blofin.py:1199](backend/app/cli/trader_blofin.py#L1199). BloFin=`True`, Binance=`False`. Binance adapter's `place_entry_order` also runtime-asserts (raises `NotImplementedError`) if a non-`None` `sl_trigger_price` arrives — belt + suspenders.

## Code shipped

Commit `592206a`, 7 files, +5041 / −273:

| File | Status | Purpose |
|---|---|---|
| `backend/app/services/exchanges/adapter.py` | NEW | `ExchangeAdapter` ABC + 5 frozen dataclasses (`BalanceInfo`, `InstrumentInfo`, `PositionInfo`, `OrderResult`, `FillInfo`) + `adapter_for(creds)` lazy-import dispatch |
| `backend/app/services/exchanges/blofin_adapter.py` | NEW | Thin 1:1 wrapper over unchanged `BlofinREST`; field-fallback chains copied verbatim from existing call sites with `file:line` attribution |
| `backend/app/services/exchanges/binance_margin_adapter.py` | NEW | `python-binance` cross-margin client: MARGIN_BUY primary, `-3006` USDT-borrow fallback, AUTO_REPAY exits, `quoteOrderQty` dust cleanup, 30-min TTL cache of margin-pair universe |
| `backend/app/cli/trader_blofin.py` | MODIFIED | 526 lines churned. `build_api()` returns `BloFinAdapter`; 4 helpers thinned to adapter delegation; 5 `get_actual_positions` callers adapted to `list[PositionInfo]`; `_place_order_chunked` reads `OrderResult.error_code` (102015 retry stays in trader, not adapter); `reconcile_fill_prices` + `reconcile_exit_prices` consume dataclasses; `close_all_positions` primary + reduce-only fallback; Decision C1 pre-filter inserted; SL gate requires `EXCHANGE_SL_ENABLED AND api.native_sl_supported`; dispatch at `:3312`; `run_session` deprecation warning |
| `backend/app/cli/spawn_traders.py` | MODIFIED | `SUPPORTED_EXCHANGES = {"blofin", "binance"}` |
| `backend/requirements.txt` | MODIFIED | `+python-binance==1.0.36` + 12 pinned transitives (aiohttp, dateparser, pycryptodome, yarl, etc.) |
| `backend/app/cli/trader_blofin_fallback.py` | NEW | Verbatim snapshot of pre-refactor HEAD (3479 lines); NOT imported; rollback safety net retirement-gated per `open_work_list.md` |

**Zero strategy/orchestration logic changed.** Sizing math, chunking, retries, stale-sweep, conviction, `vol_boost`, `port_sl` / `port_tsl`, advisory locks, `runtime_state` writes — all preserved in-place. Only field-fallback chains and exchange-specific wire calls moved into adapters.

## Audit results summary

**Pre-deploy validation** (all green):
- Celery `inspect active`: 1 node online, queue empty — safe to `--force-recreate`.
- Pre-deploy trader subprocess check (host + container): zero processes — no orphan sessions.
- Container rebuild: all 5 containers recreated + healthy.
- `pip install` inside container: `python-binance==1.0.36` + 12 transitives installed at exact expected pins; no conflicts.
- Post-deploy import smoke in backend container: all 3 adapter modules load; class attributes reflect design (`BloFinAdapter.native_sl_supported=True`, `BinanceMarginAdapter.native_sl_supported=False`); `spawn_traders.SUPPORTED_EXCHANGES={'binance', 'blofin'}`.

**SL-path audit** (client-side SL for Binance):
- `_run_monitoring_loop` at `trader_blofin.py:2042` already takes `api: ExchangeAdapter`.
- SL trigger sites at `:2150` (per-symbol), `:2221` (portfolio hard stop), `:2231` (trailing stop).
- Zero direct `api.*` calls inside the monitoring loop; all API access routes through already-audited helpers (`get_mark_prices`, `close_all_positions`, `reconcile_exit_prices`, `get_account_balance_usdt`) — each has `api: ExchangeAdapter` signature.
- All adapter-method calls inside the monitoring path are on the ABC surface (`get_price`, `get_positions`, `close_position`, `place_reduce_order`, `get_recent_fills`, `get_balance`). Zero `BlofinREST`-only method leaks.
- Return-shape handling uses `@dataclass(frozen=True)` attributes everywhere — no residual `resp["data"]` / `resp.get("code")` on adapter returns.

**None-safety audit** (concern: `PositionInfo.average_price` can be `None` from Binance fill-indexing lag):
- SL computation uses `entry_prices` dict (kline-history-anchored `dict[str, float]`), **not** `p.average_price` from the adapter. `p.average_price` only feeds `reconcile_fill_prices`, which writes exit-slippage telemetry and has its own None guards at every division site.
- All three SL arithmetic sites have explicit falsy / positive-number guards before division. Reviewed at `:2147-2149` (per-symbol: `if not ref or not price: continue`) and `:2177-2178` (portfolio: `iid in current and entry_prices.get(iid, 0) > 0`).
- Binance fill-indexing lag is a **diagnostic quality** concern (slippage telemetry blank for affected symbols) — not an SL enforcement concern.
- **Verdict: SL path is None-safe across all three trigger sites.** Activation is go.

## Activation plan — 2026-04-21 06:05 UTC

Tomorrow's first real activation. Expected flow:

1. `spawn_traders.py` (05 6 * * *) queries `user_mgmt.allocations` for active rows on supported exchanges.
2. Binance `$20` allocation (`686077fc-…`, `status=active`) spawns as subprocess `trader_blofin --allocation-id 686077fc-…`.
3. If a BloFin allocation also exists under j@mullincap.com's BloFin connection (user plan: create one via allocator UI), spawns in parallel.
4. Both subprocesses run for up to ~17 hours; parallel-safe via separate credentials, advisory locks, log files, runtime_state rows.
5. Master BloFin cron (`0 6 * * *`, host) continues running on the separate master account — unchanged.

What to watch on the Binance subprocess's log (`/mnt/quant-data/logs/trader/allocation_686077fc-…_2026-04-21.log`):
- Pre-filter drop count: `"Allocation {id}: N/M symbols unsupported on binance, dropped: [...]"`.
- Conviction check and `eff_lev` derivation (Phase 4 `vol_boost` read + `l_high × vol_boost = eff_lev` log format).
- `set_leverage` verification outcome per symbol: success or `INSUFFICIENT_BORROW_LIMIT`.
- `MARGIN_BUY` outcomes: success (common) or `-3006` → manual-borrow fallback path.
- Entry fill reconciliation may show null `fill_entry_price` on Binance due to `get_margin_trades` indexing lag — expected, not a bug.

## Known non-blocking follow-ups

Carried forward to the Session E (or later) open-work-list:

1. **Binance margin PnL display blank**: `exchange_snapshots.positions.mark_price = 0.0` for Binance margin positions → allocator card's PnL% will be blank until `_fetch_live_binance` is enhanced to pull live marks for margin positions. Trading works; display doesn't.
2. **Symbol coverage gap on Binance margin**: Tail Guardrail's filter may return long-tail alts that are BloFin perps but not `isMarginTradingAllowed=true` on Binance spot. Observable post-activation via pre-filter drop-count log lines.
3. **Borrow interest accrual accounting**: ~0.14% per 14h session on cross-margin borrowed USDT. Will manifest as PnL divergence vs. BloFin on identical signals. Needs explicit accounting in return-reporting if allocations scale up.
4. **Fallback retirement**: `backend/app/cli/trader_blofin_fallback.py` stays in-tree ≥ 7 days of successful multi-tenant operation, then cleanup. Retirement gate shared with `blofin_logger.py` + master cron in `open_work_list.md`.
5. **Doc rot**: stale comment at `trader_blofin.py:2317` (`"Then reconcile against BloFin fill history."` — reconcile is now exchange-agnostic). `get_mark_prices` docstring at `:784-801` describes a BloFin-or-Binance-Futures world but PRICE_SOURCE="blofin" now silently means "adapter-native price" which is exchange-dependent. Update after first activation confirms behavior.
6. **Master-cron retirement (Phase 2 operational task)**: gated on 7 consecutive days of multi-tenant stability per `docs/open_work_list.md`. Earliest possible date: 2026-04-28.

## Session D commits

- `592206a` — **feat(trader): Binance cross-margin executor via ExchangeAdapter** — the refactor itself. Deployed via `./redeploy.sh` on mcap at ~07:53 UTC 2026-04-20.
- (This close-out commit) — `docs(session-d): close-out — operational gate, fallback header, session summary`.

## Production state at Session D close

- **Commit on `main`**: `592206a` (deployed) + docs close-out commit (no deploy needed).
- **Containers**: all 5 healthy post-redeploy.
- **Active allocations**: 1 (`$20` Binance, dormant until 2026-04-21 06:05 UTC when the new gate allows spawn). User plan: add BloFin allocation before tomorrow's window.
- **Connection balances** at audit time: BloFin master `$3,951.03`, Binance user `$19.29`.
- **Next critical timestamp**: 2026-04-21 06:05 UTC — first activation window for the new code.
