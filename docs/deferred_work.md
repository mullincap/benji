# Deferred work

Items surfaced during normal work that aren't in scope for the current track but shouldn't be lost. Add to top; keep entries terse with enough detail to act on cold.

---

## Note: strategy_id=1 / v1.0 excluded from nightly refresh — intentional

**Surfaced:** 2026-04-20 during Item 6 investigation (Session C).

`audit.strategy_versions` has four `is_active=TRUE` rows. Nightly `refresh_strategy_metrics` picks up only three — the three published alpha variants. The fourth row is:

```
strategy_version_id = d023dc1e-d48a-46f1-ba7b-6c21a11b01a5
strategy_id         = 1
version_label       = v1.0
is_active           = t
metrics_updated_at  = NULL
```

Parent strategy is `audit.strategies strategy_id=1 name="overlap_tail_disp" display_name="Overlap — Tail + Dispersion" is_published=FALSE` — the legacy pre-alpha Overlap strategy.

Eligibility filter at `refresh_strategy_metrics.py:63–64` is `sv.is_active AND s.is_published`. The strategy-level `is_published=FALSE` gates this version out. `is_active` on the version row just marks it as the current version of the (unpublished) strategy, not "ready-for-refresh." Two-level published-ness (strategy row + version row) is intentional.

**Why it's worth a note:** future investigation querying `WHERE is_active=TRUE` alone will see 4 rows and wonder about the missing metrics. This entry pre-empts the mystery.

**No action required.** Remove this note if/when Overlap is unpublished outright or the strategy is re-published and starts refreshing.

---

## Dead-code cleanup: CAPITAL_MODE / CAPITAL_VALUE in containerized trader

**Surfaced:** 2026-04-19 during Item 10 scope review.

**Problem:**
`backend/app/cli/trader_blofin.py` lines 126–127 still define:
```python
CAPITAL_MODE  = "pct_balance"
CAPITAL_VALUE = 1.0
```
These are consumed inside `enter_positions` (line 1093) as:
```python
usdt_total = (balance * CAPITAL_VALUE if CAPITAL_MODE == "pct_balance"
              else min(CAPITAL_VALUE, balance))
```

After Item 10, the per-allocation path passes pre-sized `usdt_for_allocation` as the `balance` param. `CAPITAL_VALUE=1.0` makes the multiplication a no-op; the `else` branch is never exercised (since `CAPITAL_MODE` is hardcoded `"pct_balance"`). The constants exist now only because the legacy `run_session` master path (line 1994) still passes full balance and relies on the same code.

Since the containerized `run_session` is unused in production (spawn_traders always passes `--allocation-id`, routing to `_run_fresh_session_for_allocation` instead), the legacy `run_session` itself is also dead in this containerized copy.

**Cleanup scope:**
- Remove `CAPITAL_MODE`, `CAPITAL_VALUE` module constants.
- Simplify `enter_positions` to `usdt_total = balance` (param renamed `capital_budget` for semantic clarity).
- Decide whether to also remove the legacy `run_session` function itself from the containerized copy. Host master `/root/benji/trader-blofin.py` is a separate file and untouched.

**Risk:** zero — the dead paths don't execute in production. Cleanup is cosmetic + semantic-clarity.

**Gate:** any time after Item 10 lands. Do not bundle into Item 10 to keep that diff tight on the live-money change.

**Tracking:** carry on open work list, address when next touching trader_blofin.py.

---

## Strategy taxonomy decision + batch rename

**Surfaced:** 2026-04-19 when considering a one-off rename of `alpha_tail_guardrail_low_risk` display_name to `... - Med risk`.

**Problem:**
Current three published strategies mix two different naming axes:
- `alpha_tail_guardrail_low_risk` — "Low risk" (risk-level axis)
- `alpha_tail_guardrail_low_lev` — "Low lev" (leverage axis)
- `alpha_tail_guardrail_high_lev` — "High lev" (leverage axis)

The axes aren't interchangeable ("low risk" could map to low leverage OR to tight stops OR to short duration, depending on what dimension is being expressed). Users and future strategies will drift further without a canonical taxonomy.

**Decision needed:**
Pick one axis — risk-level, leverage, or a compound key — and migrate all three to be consistent.

**Open question: slug vs display_name scope.**
- `display_name` rename is cheap: single UPDATE on `audit.strategies.display_name`, no FK impact. Allocator card label updates immediately, zero code changes.
- `name` slug rename is expensive:
  - UNIQUE constraint; touches string-match call sites (logs across `refresh_strategy_metrics`, `spawn_traders`, manager briefings).
  - Historical handoff docs and commits reference the old slug (e.g. `session_handoff_2026-04-18.md` references `alpha_tail_guardrail_low_risk`); those don't re-resolve automatically.
  - Baselines on disk (`/tmp/benji_baselines/*`) and prior audit job rows reference by strategy_version_id (UUID, unaffected) but semantic grep for the slug across history would miss after rename.

**Gate:** Not until Track 3 closes. No one-off renames between sessions — batch all three at once after taxonomy is fixed, one commit, one deploy, so the mental model stays coherent.

**Tracking:** carry on open work list, address during Track 3 wrap-up or as a dedicated session.

---

## ~~active_filter string-namespace inconsistency~~ — RESOLVED 2026-04-20

**Resolved in commit `f9491a1` (Session C).** Adopted Option B from the durable-fix list below: read-site normalization at the `TraderConfig.from_strategy_version` factory boundary, matching the port_tsl pattern. New `_canonicalize_filter_name()` helper strips a strict `^[A-Z] - ` prefix so live-trading callers always see the canonical identifier, regardless of whether Simulator promote persisted the UI label form ("A - Tail Guardrail") or the canonical form ("Tail Guardrail") or left the key absent. Simulator write site unchanged — continues storing label form intentionally for UI semantics; comment at [simulator.py:254](../backend/app/api/routes/simulator.py#L254) expanded to document the normalize-downstream contract.

Verified against the two broken prod rows (Low lev, High lev) and strategy 2 (key absent, regression check): all three resolve to `'Tail Guardrail'` post-fix. No migration / hash churn required.

Scoped to live-trading paths only. Nightly refresh + Item 6 vol_boost use `audit.strategies.filter_mode` directly via SQL and are unaffected.

Original investigation preserved below for reference.

---

## active_filter string-namespace inconsistency (pre-Item 4 scope check)

**Discovered:** 2026-04-19 during Item 5 IDENTITY_FIELDS convention sweep.

**Mechanism:**
- `audit.strategies.filter_mode` for alpha v1 = `"A - Tail Guardrail"` (human label; visible in `refresh_metrics.log` as `filter_mode='A - Tail Guardrail'`).
- Host master trader `/root/benji/trader-blofin.py` module constant `ACTIVE_FILTER = "Tail Guardrail"` — this is the string written into `user_mgmt.daily_signals.filter` at 06:00 UTC each day.
- `backend/app/services/trading/trader_config.py` default `active_filter = "Tail Guardrail"`.
- Alpha v1 `strategy_version.config` JSONB has **no** `active_filter` / `filter_mode` key → factory `_pick` falls through to default `"Tail Guardrail"` (with a WARN log).
- `backend/app/cli/trader_blofin.py:584` matches `daily_signals` rows by case-insensitive equality on `filter`.

**Why it works today:** the JSONB key absence is accidental alignment — factory default happens to equal the string written into `daily_signals`.

**Latent failure:** Track 1's promote path (`simulator.py`) now persists `active_filter` into `strategy_version.config` on future promotions. If the value written is the label form `"A - Tail Guardrail"` (whatever the strategy row's `filter_mode` is), the factory will resolve that label, the executor will query `daily_signals` by `"A - Tail Guardrail"`, find zero matches against `"Tail Guardrail"` rows, and silently trade zero symbols. Case-insensitive equality does not bridge the gap (`"a - tail guardrail" != "tail guardrail"`).

**Pre-Item 4 scope check:**
During Item 4 investigation (`run_audit(params) → dict` refactor), explicitly confirm whether the refactor touches any code path that persists `active_filter` into `strategy_version.config`.
- If **yes**: resolve this issue before shipping Item 4 (likely: normalize at the promote-path write site, so stored form matches `daily_signals.filter` form, or make `trader_blofin.py` tolerant by normalizing at the factory).
- If **no**: leaves the issue deferred for its own session.

**Durable fix options (for the standalone session):**
- Option A: canonicalize at the write site in `simulator.py` promote — write only the form that matches `daily_signals.filter`.
- Option B: normalize at the read site in `trader_config.from_strategy_version` — map label → canonical form via a dict lookup, similar to the `port_tsl` boundary normalization pattern.
- Option C: pick one namespace (label vs canonical) and migrate both sides to it.

**Blast radius if not fixed:** silent zero-trade days for any newly-promoted allocation using a strategy whose `filter_mode` is a label not identical to what the host trader writes into `daily_signals`. No active blocker today because alpha v1's config has the key absent; exposure begins on the next Simulator-UI promotion.

**Tracking:** keep on open work list, revisit during Item 4 investigation or as its own session.

---

## Environment drift: BASE_DATA_DIR inconsistency between services

**Discovered:** 2026-04-18 during Track 1 Part 10 smoke test.

**Problem:**
- `.env.production` has legacy `BASE_DATA_DIR=/data` (pre-`/mnt/quant-data` migration)
- `docker-compose.yml` celery service explicitly overrides: `BASE_DATA_DIR=/mnt/quant-data`
- `backend` service does NOT override → inherits `/data` from `env_file`
- Pre-stage parquet freshness check against `/data/leaderboards/…` finds nothing → triggers 3-hour full parquet rebuild

**Immediate mitigation (in place):**
- Nightly refresh cron targets the `celery` service (correct env), not `backend`.

**Durable fix (deferred):**
- Option A: update `.env.production` `BASE_DATA_DIR` / `PARQUET_PATH` / `MARKETCAP_DIR` to `/mnt/quant-data`, remove the celery service override in `docker-compose.yml`
- Option B: add explicit `BASE_DATA_DIR=/mnt/quant-data` override to the backend service in `docker-compose.yml`
- Either option aligns all services; Option A is cleaner.

**Blast radius if not fixed:**
- Any future CLI run against the `backend` service that depends on parquet paths will trigger the 3-hour rebuild
- Manual debugging sessions against the backend container will fail silently or slowly
- Only affects deferred maintenance — no active blocker

**Tracking:** carry on open work list, address when next touching env configuration.
