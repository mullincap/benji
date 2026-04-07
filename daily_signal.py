#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
daily_signal.py
===============
Self-sustaining daily signal generator for the Overlap Strategy.

Fetches all required data live from Binance Futures API and BloFin API,
then writes the deploys CSV consumed by trader-blofin.py.

Run via cron at 05:58 UTC so the full 00:00-06:00 window is complete:
  58 5 * * *  /home/ubuntu/benji3m/run_daily.sh

The electoral window closes at 06:00 UTC. Running at 05:58 gives the last
5-min bars time to close. Script finishes ~06:00-06:01, well before the
conviction check at 06:35 UTC.

Pipeline:
  1. Fetch BloFin instrument list             -> universe gate
  2. Fetch Binance top-N USDT perps by volume -> eligible universe (~100)
  3. Fetch 5-min klines  00:00-06:00 UTC      -> all 72 bars per symbol
  4. Fetch OI history    00:00-06:00 UTC      -> all 72 bars per symbol
  5. w20c20 frequency leaderboards:
       For each of 72 bars:
         - Rank all symbols by log-return from midnight    (price)
         - Rank all symbols by OI delta from midnight      (OI)
         - Top-20 (FREQ_WIDTH) per bar earn a frequency count
       Price leaderboard: symbols ranked by how many bars they appeared in top-20
       OI leaderboard:    same for OI
       Overlap pool:      intersection of top-20 (FREQ_CUTOFF) from each leaderboard
  6. Tail Guardrail filter (1-day lag):
       - BTC prev-day return < -3%       [crash flag]
       - BTC 5d rvol > 1.4x 60d baseline [vol spike]
       Either fires -> sit flat today
  7. Write deploys CSV (prune rows older than DEPLOYS_RETAIN_DAYS)
"""

import os, sys, csv, math, time, logging, datetime, requests, calendar
from collections import Counter
from pathlib import Path
import numpy as np


# ==========================================================================
# CONFIGURATION
# ==========================================================================

# -- Universe ---------------------------------------------------------------
LEADERBOARD_UNIVERSE  = 100   # top-N symbols by 24h volume to scan
FREQ_WIDTH            = 20    # w20: top-N symbols per bar that earn a count
FREQ_CUTOFF           = 20    # c20: top-N by frequency before intersection

# -- Session timing ---------------------------------------------------------
DEPLOYMENT_START_HOUR = 6     # 06:00 UTC -- electoral window end
BAR_INTERVAL          = "5m"
BARS_PER_WINDOW       = 72    # 6h / 5min

# -- Tail Guardrail (Figure 2.3) -------------------------------------------
TAIL_DROP_PCT         = 0.04  # BTC prev-day return < -4%  -> sit flat
TAIL_VOL_MULT         = 1.4   # BTC 5d rvol > 1.4x 60d baseline -> sit flat
TAIL_VOL_SHORT_WINDOW = 5
TAIL_VOL_LONG_WINDOW  = 60

# -- Dispersion filter (Figure 2.3) ----------------------------------------
# Sits flat when cross-sectional std of top-cap perp returns falls below
# a fraction of its rolling median (low dispersion = momentum edge gone).
# Parameters match audit.py exactly.
DISPERSION_THRESHOLD    = 0.66   # sit flat if disp_ratio < this
DISPERSION_BASELINE_WIN = 33     # rolling median window (days)
DISPERSION_MIN_SYMBOLS  = 20     # minimum symbols with valid returns per day

# Static universe: top-40 perps by market cap (matches DISPERSION_N=40 in audit.py)
# DISPERSION_SYMBOLS_30 + first 10 of _ADDED_60 from audit.py.
# The audit uses a dynamic CoinGecko-ranked universe; the live pipeline uses
# this fixed list to avoid a CoinGecko API dependency.
DISPERSION_SYMBOLS = [
    # Top 30 (DISPERSION_SYMBOLS_30)
    "BTCUSDT","ETHUSDT","BNBUSDT","XRPUSDT","SOLUSDT",
    "TRXUSDT","DOGEUSDT","HYPEUSDT","BCHUSDT","ADAUSDT",
    "LINKUSDT","XLMUSDT","LTCUSDT","AVAXUSDT","HBARUSDT",
    "ZECUSDT","SUIUSDT","1000SHIBUSDT","TONUSDT","TAOUSDT",
    "DOTUSDT","MNTUSDT","UNIUSDT","NEARUSDT","AAVEUSDT",
    "1000PEPEUSDT","ICPUSDT","ETCUSDT","ONDOUSDT","KASUSDT",
    # Ranks 31-40 (first 10 of _ADDED_60 from audit.py)
    "POLUSDT","WLDUSDT","QNTUSDT","ATOMUSDT","RENDERUSDT",
    "ENAUSDT","APTUSDT","FILUSDT","STXUSDT","ARBUSDT",
]

# Cache file for daily returns (avoids re-fetching on every run)
DISPERSION_CACHE_FILE = Path("dispersion_returns_cache.csv")

# -- Binance API ------------------------------------------------------------
BINANCE_BASE    = "https://fapi.binance.com"
REQUEST_TIMEOUT = 15
REQUEST_DELAY   = 0.08    # seconds between requests (rate-limit safety)
MAX_RETRIES     = 3

# -- Output -----------------------------------------------------------------
DEPLOYS_CSV          = Path("live_deploys_signal.csv")
DEPLOYS_RETAIN_DAYS  = 90     # prune rows older than this many days
FILTER_NAME          = "Tail Guardrail"
LOG_FILE             = Path("daily_signal.log")


# ==========================================================================
# LOGGING
# ==========================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("daily_signal")


# ==========================================================================
# HELPERS
# ==========================================================================

def utcnow() -> datetime.datetime:
    return datetime.datetime.utcnow().replace(tzinfo=None)

def to_ms(dt: datetime.datetime) -> int:
    """Convert naive UTC datetime to Unix milliseconds.
    Uses calendar.timegm to treat the datetime as UTC regardless of
    the local system timezone -- dt.timestamp() would use local time.
    """
    return int(calendar.timegm(dt.timetuple()) * 1000)

def binance_get(path: str, params: dict = None):
    url = BINANCE_BASE + path
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt == MAX_RETRIES:
                raise
            log.warning(f"Request failed ({attempt}/{MAX_RETRIES}): {path} -- {e}")
            time.sleep(2 ** attempt)


# ==========================================================================
# STEP 1 — BLOFIN UNIVERSE GATE
# ==========================================================================

def get_blofin_symbols() -> set:
    """
    Fetch all USDT perpetual swap instruments listed on BloFin.
    Uses a direct public REST call -- no SDK required.
    Returns bare symbols {'BTC', 'ETH', ...}.
    Falls back to empty set (no gate) if API unavailable.
    """
    try:
        url  = "https://openapi.blofin.com/api/v1/market/instruments"
        resp = requests.get(url, params={"instType": "SWAP"}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        syms = {
            inst["instId"].replace("-USDT", "").upper()
            for inst in (data.get("data") or [])
            if inst.get("instId", "").endswith("-USDT")
        }
        log.info(f"BloFin universe: {len(syms)} instruments")
        return syms
    except Exception as e:
        log.warning(f"BloFin gate unavailable: {e} -- no filtering applied")
        return set()


# ==========================================================================
# STEP 2 — BINANCE UNIVERSE
# ==========================================================================

def get_binance_universe(blofin_syms: set) -> list:
    """
    Fetch all active USDT perp tickers from Binance, rank by 24h quote volume,
    return top LEADERBOARD_UNIVERSE that are also on BloFin (if gate available).
    """
    log.info("Fetching Binance 24h tickers ...")
    tickers = binance_get("/fapi/v1/ticker/24hr")
    rows = []
    for t in tickers:
        sym = t.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        bare = sym[:-4]
        if blofin_syms and bare not in blofin_syms:
            continue
        try:
            rows.append((bare, float(t.get("quoteVolume", 0))))
        except (TypeError, ValueError):
            continue
    rows.sort(key=lambda x: x[1], reverse=True)
    top = [r[0] for r in rows[:LEADERBOARD_UNIVERSE]]
    log.info(f"Universe (post BloFin gate): {len(top)} symbols")
    return top


# ==========================================================================
# STEP 3 & 4 — FETCH BAR DATA
# ==========================================================================

def window_times(ref_date: datetime.date):
    """Return (start_dt, end_dt) for the 00:00-06:00 UTC electoral window."""
    start = datetime.datetime.combine(ref_date, datetime.time(0, 0, 0))
    end   = datetime.datetime.combine(ref_date, datetime.time(DEPLOYMENT_START_HOUR, 0, 0))
    return start, end

def fetch_klines(symbol: str, start_dt: datetime.datetime,
                 end_dt: datetime.datetime) -> list:
    return binance_get("/fapi/v1/klines", params={
        "symbol":    symbol + "USDT",
        "interval":  BAR_INTERVAL,
        "startTime": to_ms(start_dt),
        "endTime":   to_ms(end_dt),
        "limit":     BARS_PER_WINDOW + 5,
    }) or []

def fetch_oi_history(symbol: str, start_dt: datetime.datetime,
                     end_dt: datetime.datetime) -> list:
    try:
        return binance_get("/futures/data/openInterestHist", params={
            "symbol":    symbol + "USDT",
            "period":    "5m",
            "startTime": to_ms(start_dt),
            "endTime":   to_ms(end_dt),
            "limit":     BARS_PER_WINDOW + 5,
        }) or []
    except Exception:
        return []


# ==========================================================================
# STEP 5 — w20c20 FREQUENCY LEADERBOARDS  (matches backtest exactly)
# ==========================================================================

def build_overlap_pool(symbols: list, start_dt: datetime.datetime,
                       end_dt: datetime.datetime) -> list:
    """
    Implements the w20c20 frequency logic used in the backtest.

    For each of the 72 5-min bars in the 00:00-06:00 electoral window:
      - Rank all symbols by log-return from midnight  (price momentum)
      - Rank all symbols by OI delta from midnight    (OI accumulation)
      - Top FREQ_WIDTH (20) symbols per bar earn +1 to their frequency count

    After all 72 bars:
      - Price leaderboard: top FREQ_CUTOFF (20) symbols by frequency count
      - OI leaderboard:    top FREQ_CUTOFF (20) symbols by frequency count
      - Overlap pool:      intersection of both top-20 lists
    """
    log.info(f"Fetching bar data for {len(symbols)} symbols (w{FREQ_WIDTH}c{FREQ_CUTOFF}) ...")

    # -- Collect all bar data -----------------------------------------------
    klines_map = {}   # sym -> list of kline bars
    oi_map     = {}   # sym -> list of OI records

    for i, sym in enumerate(symbols):
        if i > 0 and i % 20 == 0:
            log.info(f"  ... {i}/{len(symbols)}")
        try:
            k = fetch_klines(sym, start_dt, end_dt)
            if k:
                klines_map[sym] = k
        except Exception as e:
            log.warning(f"  {sym}: klines failed: {e}")
        try:
            o = fetch_oi_history(sym, start_dt, end_dt)
            if o:
                oi_map[sym] = o
        except Exception as e:
            log.warning(f"  {sym}: OI failed: {e}")
        time.sleep(REQUEST_DELAY)

    log.info(f"  Data: {len(klines_map)} price series, {len(oi_map)} OI series")

    # -- Build timestamp-indexed lookups ------------------------------------
    # price_ts[sym][ts_ms] = close_price
    price_ts = {
        sym: {bar[0]: float(bar[4]) for bar in bars}
        for sym, bars in klines_map.items()
    }
    # oi_ts[sym][ts_ms] = sum_oi_usd
    oi_ts = {
        sym: {rec["timestamp"]: float(rec.get("sumOpenInterestValue", 0))
              for rec in recs}
        for sym, recs in oi_map.items()
    }

    # -- Anchor values at midnight (first bar) ------------------------------
    anchor_price = {
        sym: float(bars[0][1])   # first bar open price
        for sym, bars in klines_map.items() if bars
    }
    anchor_oi = {
        sym: float(recs[0].get("sumOpenInterestValue", 0))
        for sym, recs in oi_map.items() if recs
    }

    # -- Collect all bar timestamps (skip midnight anchor itself) -----------
    all_ts = sorted({
        bar[0]
        for bars in klines_map.values()
        for bar in bars[1:]   # skip bar[0] = midnight
    })

    if not all_ts:
        log.warning("No bar timestamps found -- returning empty overlap pool")
        return []

    # -- Per-bar frequency counting -----------------------------------------
    price_freq = Counter()
    oi_freq    = Counter()

    for ts in all_ts:
        # Price: log-return from midnight at this bar
        p_scores = {}
        for sym in price_ts:
            close = price_ts[sym].get(ts)
            ap    = anchor_price.get(sym)
            if close and ap and ap > 0:
                try:
                    p_scores[sym] = math.log(close / ap)
                except ValueError:
                    pass

        for sym in sorted(p_scores, key=p_scores.get, reverse=True)[:FREQ_WIDTH]:
            price_freq[sym] += 1

        # OI: delta from midnight at this bar
        o_scores = {}
        for sym in oi_ts:
            oi_val = oi_ts[sym].get(ts)
            ao     = anchor_oi.get(sym)
            if oi_val is not None and ao is not None:
                o_scores[sym] = oi_val - ao

        for sym in sorted(o_scores, key=o_scores.get, reverse=True)[:FREQ_WIDTH]:
            oi_freq[sym] += 1

    # -- OI fallback: use kline quoteVolume if OI data is sparse -----------
    oi_fallback = False
    if len(oi_freq) < len(price_freq) * 0.5:
        log.warning("OI data sparse (<50% coverage) -- using kline quoteVolume as proxy")
        oi_fallback = True
        oi_freq = Counter()
        vol_ts = {
            sym: {bar[0]: float(bar[7]) for bar in bars if len(bar) > 7}
            for sym, bars in klines_map.items()
        }
        for ts in all_ts:
            v_scores = {sym: vol_ts[sym].get(ts, 0) for sym in vol_ts}
            for sym in sorted(v_scores, key=v_scores.get, reverse=True)[:FREQ_WIDTH]:
                if v_scores[sym] > 0:
                    oi_freq[sym] += 1

    # -- Final intersection -------------------------------------------------
    price_top = {sym for sym, _ in price_freq.most_common(FREQ_CUTOFF)}
    oi_top    = {sym for sym, _ in oi_freq.most_common(FREQ_CUTOFF)}
    overlap   = sorted(price_top & oi_top)

    log.info(
        f"Bars processed: {len(all_ts)}\n"
        f"  Price top-{FREQ_CUTOFF}: {sorted(price_top)}\n"
        f"  OI top-{FREQ_CUTOFF}:    {sorted(oi_top)}\n"
        f"  Overlap ({len(overlap)}): {overlap}"
        + (" [OI=volume fallback]" if oi_fallback else "")
    )
    return overlap


# ==========================================================================
# STEP 6a — DISPERSION FILTER
# ==========================================================================

def fetch_dispersion_returns(today: datetime.date) -> "pd.DataFrame | None":
    """
    Fetch daily log-returns for DISPERSION_SYMBOLS from Binance 1d klines.
    Covers the last DISPERSION_BASELINE_WIN + 5 days to ensure the rolling
    median is fully warmed up.

    Results are cached to DISPERSION_CACHE_FILE. The cache is refreshed if
    it doesn't cover yesterday (the most recent completed trading day).

    Returns DataFrame: index=date, columns=symbols. None on failure.
    """
    import pandas as _pd
    import numpy as _np

    yesterday = today - datetime.timedelta(days=1)

    # Check cache
    if DISPERSION_CACHE_FILE.exists():
        try:
            cached = _pd.read_csv(DISPERSION_CACHE_FILE, index_col=0, parse_dates=True)
            cached.index = _pd.to_datetime(cached.index).tz_localize(None)
            if not cached.empty and cached.index[-1].date() >= yesterday:
                log.info(f"Dispersion cache hit: {len(cached)} rows x {len(cached.columns)} symbols")
                return cached
            log.info(f"Dispersion cache stale (ends {cached.index[-1].date()}) -- refreshing")
        except Exception as e:
            log.warning(f"Dispersion cache read failed: {e} -- re-fetching")

    # Fetch last ~90 days of daily closes
    lookback_days = DISPERSION_BASELINE_WIN + 60
    start_dt = today - datetime.timedelta(days=lookback_days)
    start_ms  = to_ms(datetime.datetime.combine(start_dt, datetime.time(0, 0)))
    end_ms    = to_ms(datetime.datetime.combine(today,    datetime.time(0, 0)))

    log.info(f"Fetching dispersion daily closes for {len(DISPERSION_SYMBOLS)} symbols ...")
    closes = {}
    for sym in DISPERSION_SYMBOLS:
        try:
            resp = binance_get("/fapi/v1/klines", params={
                "symbol": sym, "interval": "1d",
                "startTime": start_ms, "endTime": end_ms, "limit": 200,
            })
            if resp:
                import pandas as _pd2
                dates  = _pd2.to_datetime([b[0] for b in resp], unit="ms").normalize()
                prices = [float(b[4]) for b in resp]
                s = _pd2.Series(prices, index=dates)
                closes[sym] = s.groupby(level=0).last()
        except Exception as e:
            log.debug(f"  {sym}: daily close fetch failed: {e}")
        time.sleep(0.05)

    if not closes:
        log.warning("Dispersion: all fetches failed")
        return None

    import pandas as _pd3
    import numpy as _np2
    price_df = _pd3.DataFrame(closes).sort_index()
    ret_df   = _np2.log(price_df / price_df.shift(1))
    ret_df.index = _pd3.to_datetime(ret_df.index).tz_localize(None)

    # Cache
    try:
        ret_df.to_csv(DISPERSION_CACHE_FILE)
    except Exception as e:
        log.warning(f"Dispersion cache write failed: {e}")

    log.info(f"Dispersion returns: {len(ret_df)} days x {len(ret_df.columns)} symbols")
    return ret_df


def compute_dispersion_filter(today: datetime.date) -> tuple:
    """
    Compute dispersion filter for today using 1-day lag.

    disp_t     = cross-sectional std of log-returns across symbols
    baseline_t = rolling(DISPERSION_BASELINE_WIN).median(disp)
    ratio_t    = disp_t / baseline_t
    sit_flat   = ratio_{yesterday} < DISPERSION_THRESHOLD

    Returns (sit_flat: bool, reason: str)
    """
    import pandas as _pd
    import numpy as _np
    log.info("Computing Dispersion filter ...")

    ret_df = fetch_dispersion_returns(today)
    if ret_df is None or ret_df.empty:
        log.warning("Dispersion filter: no data -- defaulting to PASS")
        return False, "no_dispersion_data"

    try:
        # Drop days with too few valid symbols
        valid_mask = ret_df.notna().sum(axis=1) >= DISPERSION_MIN_SYMBOLS
        ret_clean  = ret_df[valid_mask]

        if len(ret_clean) < DISPERSION_BASELINE_WIN:
            log.warning(f"Dispersion: only {len(ret_clean)} valid days -- defaulting to PASS")
            return False, "insufficient_dispersion_data"

        # Cross-sectional std per day
        dispersion = ret_clean.std(axis=1)

        # Rolling median baseline (no lookahead)
        baseline   = dispersion.rolling(
            DISPERSION_BASELINE_WIN,
            min_periods=max(5, DISPERSION_BASELINE_WIN // 2)
        ).median()
        disp_ratio = dispersion / baseline.replace(0, _np.nan)

        # Yesterday's value (1-day lag)
        yesterday = _pd.Timestamp(today - datetime.timedelta(days=1))
        # Find the most recent ratio at or before yesterday
        past = disp_ratio[disp_ratio.index <= yesterday]
        if past.empty:
            log.warning("Dispersion: no ratio value for yesterday -- defaulting to PASS")
            return False, "no_yesterday_ratio"

        ratio_yesterday = float(past.iloc[-1])
        disp_yesterday  = float(dispersion.reindex(past.index).iloc[-1])

        log.info(
            f"  Dispersion yesterday: std={disp_yesterday:.4f}  "
            f"ratio={ratio_yesterday:.3f}  threshold={DISPERSION_THRESHOLD}"
        )

        if ratio_yesterday < DISPERSION_THRESHOLD:
            reason = (f"disp_ratio {ratio_yesterday:.3f} < threshold {DISPERSION_THRESHOLD} "
                      f"(low dispersion -- momentum edge reduced)")
            log.warning(f"  Dispersion Gate FIRED: {reason}")
            return True, reason

        log.info("  Dispersion filter: PASS")
        return False, "pass"

    except Exception as e:
        log.warning(f"Dispersion filter computation failed: {e} -- defaulting to PASS")
        return False, f"error: {e}"


# ==========================================================================
# STEP 6 — TAIL GUARDRAIL FILTER  (Figure 2.3)
# ==========================================================================

def compute_tail_filter():
    """
    1-day lag: uses yesterday's BTC data to gate today's session.

    Gate 1 (Signal A): BTC prev-day log-return < -TAIL_DROP_PCT (-3%)
    Gate 2 (Signal B): BTC 5d rvol > TAIL_VOL_MULT (1.4x) × 60d baseline

    Returns (sit_flat: bool, reason: str)
    """
    log.info("Computing Tail Guardrail ...")
    try:
        daily = binance_get("/fapi/v1/klines", params={
            "symbol": "BTCUSDT", "interval": "1d", "limit": 65,
        })
        if not daily or len(daily) < TAIL_VOL_LONG_WINDOW + 2:
            log.warning("Insufficient BTC data -- defaulting to PASS")
            return False, "insufficient_data"

        closes   = [float(bar[4]) for bar in daily]
        log_rets = [math.log(closes[i] / closes[i-1]) for i in range(1, len(closes))]

        # prev_day_ret = yesterday's complete daily return (daily[-2] is yesterday)
        prev_day_ret = log_rets[-1]

        # Gate 1: crash flag
        if prev_day_ret < -TAIL_DROP_PCT:
            reason = f"BTC prev-day {prev_day_ret*100:.2f}% < -{TAIL_DROP_PCT*100:.0f}%"
            log.warning(f"  Gate 1 FIRED: {reason}")
            return True, reason

        # Gate 2: vol spike (use returns excluding yesterday as the lag point)
        short_rets = log_rets[-(TAIL_VOL_SHORT_WINDOW + 1):-1]
        long_rets  = log_rets[-(TAIL_VOL_LONG_WINDOW  + 1):-1]

        if len(short_rets) < TAIL_VOL_SHORT_WINDOW or len(long_rets) < TAIL_VOL_LONG_WINDOW:
            log.warning("Insufficient rvol data -- defaulting to PASS")
            return False, "insufficient_rvol_data"

        rvol_short    = float(np.std(short_rets)) * math.sqrt(365)
        rvol_baseline = float(np.std(long_rets))  * math.sqrt(365)

        log.info(
            f"  BTC prev-day: {prev_day_ret*100:.2f}%  "
            f"5d rvol: {rvol_short*100:.2f}%  "
            f"60d baseline: {rvol_baseline*100:.2f}%  "
            f"ratio: {rvol_short/max(rvol_baseline, 1e-8):.3f}x  "
            f"threshold: {TAIL_VOL_MULT}x"
        )

        if rvol_baseline > 0 and rvol_short > TAIL_VOL_MULT * rvol_baseline:
            reason = (f"BTC 5d rvol {rvol_short*100:.2f}% > "
                      f"{TAIL_VOL_MULT}x × 60d {rvol_baseline*100:.2f}%")
            log.warning(f"  Gate 2 FIRED: {reason}")
            return True, reason

        log.info("  Tail Guardrail: PASS -- both gates clear")
        return False, "pass"

    except Exception as e:
        log.warning(f"Tail filter failed: {e} -- defaulting to PASS")
        return False, f"error: {e}"


# ==========================================================================
# STEP 7 — WRITE DEPLOYS CSV  (with pruning)
# ==========================================================================

def write_deploys_csv(date_str: str, filter_name: str, symbols: list,
                      sit_flat: bool, filter_reason: str):
    """
    Write today's row to the deploys CSV.
    - Idempotent: removes any existing row for today before writing
    - Prunes rows older than DEPLOYS_RETAIN_DAYS to keep file lean
    """
    fieldnames = ["date", "filter", "symbols", "sit_flat", "filter_reason"]
    cutoff = (utcnow().date() - datetime.timedelta(days=DEPLOYS_RETAIN_DAYS)).strftime("%Y-%m-%d")

    existing = []
    if DEPLOYS_CSV.exists():
        with open(DEPLOYS_CSV, newline="") as f:
            for row in csv.DictReader(f):
                d = row.get("date", "")
                if d != date_str and d >= cutoff:   # keep: not today, not too old
                    existing.append(row)

    new_row = {
        "date":          date_str,
        "filter":        filter_name,
        "symbols":       " ".join(symbols) if not sit_flat else "",
        "sit_flat":      str(sit_flat),
        "filter_reason": filter_reason,
    }

    with open(DEPLOYS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(existing)
        writer.writerow(new_row)

    log.info(
        f"Deploys CSV written -> {DEPLOYS_CSV}  "
        f"(kept {len(existing)} prior rows, pruned before {cutoff})\n"
        f"  date={date_str}  filter={filter_name}  "
        f"sit_flat={sit_flat}  symbols={new_row['symbols'] or '(none)'}"
    )


# ==========================================================================
# MAIN
# ==========================================================================

def main():
    today     = utcnow().date()
    today_str = today.strftime("%Y-%m-%d")

    log.info("=" * 65)
    log.info(f"  DAILY SIGNAL -- {today_str}")
    log.info("=" * 65)

    # Step 1: BloFin universe gate
    blofin_syms = get_blofin_symbols()

    # Step 2: Binance top-N universe
    universe = get_binance_universe(blofin_syms)
    if not universe:
        log.error("Empty universe -- aborting.")
        sys.exit(1)

    # Step 3 & 4: Electoral window + fetch all bar data
    start_dt, end_dt = window_times(today)

    # Step 5: w20c20 frequency leaderboards -> overlap pool
    overlap_pool = build_overlap_pool(universe, start_dt, end_dt)

    # Step 6: Apply filter(s) based on FILTER_NAME
    if FILTER_NAME == "Tail Guardrail":
        sit_flat, filter_reason = compute_tail_filter()

    elif FILTER_NAME == "Tail + Dispersion":
        tail_flat,  tail_reason = compute_tail_filter()
        disp_flat,  disp_reason = compute_dispersion_filter(today)
        # OR logic: sit flat if either gate fires
        sit_flat      = tail_flat or disp_flat
        filter_reason = (tail_reason if tail_flat else "") +                         (" | " if tail_flat and disp_flat else "") +                         (disp_reason if disp_flat else "")
        if not sit_flat:
            filter_reason = f"tail:{tail_reason} disp:{disp_reason}"

    else:
        log.warning(f"Unknown FILTER_NAME '{FILTER_NAME}' -- defaulting to no filter")
        sit_flat, filter_reason = False, "no_filter"

    # Step 7: Write deploys CSV
    if sit_flat:
        log.info(f"FILTER FIRED ({FILTER_NAME}) -- sit flat. Reason: {filter_reason}")
        write_deploys_csv(today_str, FILTER_NAME, [], True, filter_reason)
    elif not overlap_pool:
        log.info("Overlap pool empty -- no trade today.")
        write_deploys_csv(today_str, FILTER_NAME, [], True, "empty_overlap_pool")
    else:
        log.info(f"TRADE TODAY -- {len(overlap_pool)} symbols: {overlap_pool}")
        write_deploys_csv(today_str, FILTER_NAME, overlap_pool, False, filter_reason)

    log.info(f"Done. ({utcnow().strftime('%H:%M:%S')} UTC)")
    log.info("=" * 65)


if __name__ == "__main__":
    main()
