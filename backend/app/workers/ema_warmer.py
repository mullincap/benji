"""
backend/app/workers/ema_warmer.py
==================================
Celery beat task that pre-computes MA values for all currently-open
position symbols at the listed timeframe, on the schedule defined in
pipeline_worker.beat_schedule.

Filename retains "ema_warmer" for historical reasons; the task now
warms every variant in `WARM_VARIANTS` (currently SMA20 + SMA60) per
fire so the panel toggle is always served from a hot cache regardless
of which variant the user has selected.

Why this exists: the Live tab's MA Alignment heatmap reads from a
per-(symbol, tf, variant, bar_close_ts) Redis cache populated on
demand from Binance USDM klines. On a cold cache (first request
after a bar close) the endpoint fans out 1 Binance round-trip per
(position, timeframe), adding ~1s of latency to the first render.
With this warmer running on the bar-close cadence, the cache is
always pre-populated before the first user request hits it.

Schedule (registered in pipeline_worker.celery_app.conf.beat_schedule):
  * 5m  → every 5 min     * 4h  → every 4 hours at :00
  * 15m → every 15 min    * 8h  → every 8 hours at 00/08/16
  * 30m → every 30 min    * 1d  → daily at 00:00 UTC
  * 1h  → every hour at :00
Each fire warms all WARM_VARIANTS for every active-position symbol
at that timeframe.

The task itself is read-only against the DB (one SELECT for active
position symbols + binance_id resolution) and idempotent — re-running
on the same bar close hits existing cache keys and returns instantly.
Per-symbol exceptions are logged and counted; one symbol failing does
not abort the others.
"""

from __future__ import annotations

import logging
from typing import Any

from psycopg2.extras import RealDictCursor

from app.db import get_worker_conn
from app.services.ema_cache import (
    TF_ORDER,
    WARM_VARIANTS,
    compute_and_cache_ma,
)
from app.services.exchanges.binance_market import BinanceMarketClient
from app.workers.pipeline_worker import celery_app

log = logging.getLogger("ema_warmer")


def _active_position_binance_ids(cur) -> list[str]:
    """Distinct binance_id for every base symbol in any latest snapshot's
    open positions across all active connections. Returns binance_ids
    (e.g. 'BTCUSDT'); positions whose base has no binance_id in
    market.symbols are omitted — there's no point warming a cache key
    that the live request will never resolve."""
    cur.execute("""
        WITH latest AS (
            SELECT DISTINCT ON (es.connection_id)
                   es.connection_id, es.positions
              FROM user_mgmt.exchange_snapshots es
              JOIN user_mgmt.exchange_connections ec
                ON ec.connection_id = es.connection_id
             WHERE es.fetch_ok = TRUE
               AND ec.status = 'active'
             ORDER BY es.connection_id, es.snapshot_at DESC
        ),
        bases AS (
            SELECT DISTINCT
                   CASE
                     WHEN UPPER(p->>'symbol') LIKE '%USDT'
                       THEN LEFT(UPPER(p->>'symbol'), LENGTH(p->>'symbol') - 4)
                     ELSE UPPER(p->>'symbol')
                   END AS base
              FROM latest, jsonb_array_elements(latest.positions) AS p
        )
        SELECT s.binance_id
          FROM bases b
          JOIN market.symbols s ON s.base = b.base
         WHERE s.binance_id IS NOT NULL
    """)
    return [r["binance_id"] for r in cur.fetchall()]


@celery_app.task(name="ema_warmer.warm_ema_cache")
def warm_ema_cache(tf: str) -> dict[str, Any]:
    """Pre-warm the MA Redis cache for every active-position symbol at
    the given timeframe, across every variant in WARM_VARIANTS. Returns
    a result dict suitable for celery beat introspection.

    Designed to fire at the timeframe's bar-close boundary; the cache
    key in compute_and_cache_ma includes latest_closed_bar_open_ms so
    re-running mid-window is a no-op (hits cache, returns instantly).

    Task name retained as `ema_warmer.warm_ema_cache` so the existing
    pipeline_worker.beat_schedule entries don't need touching — the
    function body is what changed, not the registered task name.
    """
    if tf not in TF_ORDER:
        log.warning("ema_warmer: unknown timeframe %r — skipping", tf)
        return {"tf": tf, "skipped": True, "reason": "unknown_timeframe"}

    conn = get_worker_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        binance_ids = _active_position_binance_ids(cur)
    finally:
        conn.close()

    if not binance_ids:
        return {
            "tf": tf, "variants": list(WARM_VARIANTS),
            "warmed": 0, "errors": 0, "no_positions": True,
        }

    client = BinanceMarketClient()
    warmed = 0
    errors = 0
    for variant in WARM_VARIANTS:
        for binance_id in binance_ids:
            try:
                result = compute_and_cache_ma(
                    binance_symbol=binance_id, tf=tf, variant=variant,
                    client=client,
                )
                # `reason` is non-null on insufficient-history /
                # not-listed — those are still cached (the live
                # request will surface the reason), so they count
                # as warmed.
                warmed += 1
                if result.get("reason"):
                    log.debug(
                        "ema_warmer: %s %s %s reason=%s",
                        binance_id, tf, variant, result["reason"],
                    )
            except Exception as e:
                errors += 1
                log.warning(
                    "ema_warmer: %s %s %s failed: %s",
                    binance_id, tf, variant, e,
                )

    log.info(
        "ema_warmer: tf=%s variants=%s warmed=%d errors=%d "
        "(of %d active-position symbols × %d variants)",
        tf, ",".join(WARM_VARIANTS), warmed, errors,
        len(binance_ids), len(WARM_VARIANTS),
    )
    return {
        "tf": tf,
        "variants": list(WARM_VARIANTS),
        "symbol_count": len(binance_ids),
        "warmed": warmed,
        "errors": errors,
    }
