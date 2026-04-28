#!/usr/bin/env python3
"""peek.py — single-day basket dry-run, Amberdata-direct or via leaderboards.

Usage:
    python pipeline/peek.py 2026-04-28
    python pipeline/peek.py 2026-04-28 --diagnostics --out /tmp/peek.json
    python pipeline/peek.py 2026-04-28 --source db        # legacy path
    python pipeline/peek.py 2026-04-28 --no-cache         # force refetch
    python pipeline/peek.py --clear-cache 2026-04-25      # purge a date

Computes the canonical (non --live-parity) basket: top-FREQ_WIDTH price ∩
top-FREQ_WIDTH OI by pct_change since 00:00 UTC, mirroring audit job
235aa3d6 params. No audit / perf computation — basket only.

Two data paths:

  --source amber  (default)  Fetches per-minute price (OHLCV.close) + OI
    directly from Amberdata, mirroring the API patterns in metl.py
    (fetch_ohlcv_data + fetch_oi_data). Caches results at
    /mnt/quant-data/peek_cache/{date}_{metric}.parquet so iterative re-runs
    only fetch new minutes since the last call. Self-contained — no
    dependency on metl having ingested today's data, no leaderboards
    rebuild, no DB writes. Ideal for same-day "watch the basket evolve"
    workflows: snapshot at 06:00 UTC for past dates; snapshot at "now" for
    today (with the latest-bar fallback when 06:00 is in the future).

  --source db                Uses market.leaderboards (built by the indexer
    from market.futures_1m, populated by metl). Faster for past dates with
    full leaderboard coverage already in DB. Triggers an inline indexer
    build if [00:00, 06:00) coverage is incomplete.

Cache lifecycle: each peek_date has its own cache files. Within a day, the
cache builds up across iterative runs. Use `--clear-cache YYYY-MM-DD` to
purge after the day is complete (or just `rm` the parquet files).
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# Set env vars BEFORE importing overlap_analysis so its module-level globals
# match the canonical audit configuration (job 235aa3d6 baseline).
os.environ.setdefault("INDEX_LOOKBACK", "6")
os.environ.setdefault("SORT_LOOKBACK", "6")
os.environ.setdefault("FREQ_CUTOFF", "20")
os.environ.setdefault("SAMPLE_INTERVAL", "5")
os.environ.setdefault("LEADERBOARD_TOP_N", "333")

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))

from pipeline import overlap_analysis as oa  # noqa: E402
from pipeline.db.connection import get_conn  # noqa: E402

import pandas as pd  # noqa: E402
import requests  # noqa: E402

INDEXER = _HERE / "indexer" / "build_intraday_leaderboard.py"

# Canonical knobs — match audit job 235aa3d6 params exactly.
FREQ_WIDTH = 20
FREQ_CUTOFF = 20
SAMPLE_INTERVAL = 5
MODE = "snapshot"
MIN_MCAP = 0.0
SORT_BY = "price"
OVERLAP_DIMENSIONS = "price_oi"
DEPLOYMENT_START_HOUR = 6

# Amberdata config (mirrors metl.py).
AMBER_BASE = "https://api.amberdata.com/markets/futures"
AMBER_RATE_LIMIT_RPS = 15
AMBER_MAX_WORKERS = 15
AMBER_TIMEOUT = 30
AMBER_RETRY_LIMIT = 5

# Cache root. Lives under the shared mount so containers + host see same files.
PEEK_CACHE_DIR = Path("/mnt/quant-data/peek_cache")

# Module-level rate limiter + session (shared across threads).
_rl_lock = threading.Lock()
_next_allowed = 0.0
_amber_session: requests.Session | None = None
_amber_session_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Amberdata helpers (mirror metl.py)
# ---------------------------------------------------------------------------

def _load_amber_api_key() -> str:
    key = os.environ.get("AMBER_API_KEY")
    if key:
        return key
    secrets_path = Path("/mnt/quant-data/credentials/secrets.env")
    if secrets_path.exists():
        for line in secrets_path.read_text().splitlines():
            line = line.strip()
            if line.startswith("AMBER_API_KEY="):
                return line.split("=", 1)[1].strip()
    raise RuntimeError("AMBER_API_KEY not set — add to env or secrets.env")


def amber_session() -> requests.Session:
    global _amber_session
    if _amber_session is None:
        with _amber_session_lock:
            if _amber_session is None:
                s = requests.Session()
                s.headers.update({"x-api-key": _load_amber_api_key()})
                _amber_session = s
    return _amber_session


def rl_sleep() -> None:
    """Token-bucket-style global rate limit, shared across threads."""
    global _next_allowed
    with _rl_lock:
        now = time.time()
        if now < _next_allowed:
            time.sleep(_next_allowed - now)
        _next_allowed = max(_next_allowed, now) + (1.0 / AMBER_RATE_LIMIT_RPS)


def parse_amber_ts(value) -> datetime.datetime:
    if isinstance(value, (int, float)):
        return datetime.datetime.fromtimestamp(int(value) / 1000,
                                                tz=datetime.timezone.utc)
    return datetime.datetime.strptime(str(value)[:19], "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=datetime.timezone.utc
    )


def fetch_universe_for_date(peek_date: datetime.date) -> list[str]:
    """Return Binance USDT-perp instruments active on peek_date.

    Mirrors metl.get_symbols_for_date — paginates Amberdata's open-interest
    information endpoint, filters by start.date <= peek_date <= end.date.
    """
    url = f"{AMBER_BASE}/open-interest/information"
    params: dict | None = {"exchange": "binance"}
    sess = amber_session()
    symbols: list[str] = []
    next_url: str | None = None
    while True:
        rl_sleep()
        if next_url:
            r = sess.get(next_url, timeout=AMBER_TIMEOUT)
        else:
            r = sess.get(url, params=params, timeout=AMBER_TIMEOUT)
        r.raise_for_status()
        resp = r.json().get("payload", {})
        for row in resp.get("data", []):
            symb = row.get("instrument")
            if not symb:
                continue
            try:
                start = parse_amber_ts(row["startDate"])
                end = parse_amber_ts(row["endDate"])
            except (KeyError, ValueError, TypeError):
                continue
            if start.date() <= peek_date <= end.date():
                symbols.append(symb)
        next_url = resp.get("metadata", {}).get("next")
        if not next_url:
            break
    return symbols


def fetch_amber_metric(metric: str, symbol: str,
                       start_dt: datetime.datetime,
                       end_dt: datetime.datetime) -> dict[int, float]:
    """Fetch one symbol's per-minute series for `metric` from Amberdata.

    Returns dict[ts_min: float] where ts_min = unix-ms-timestamp // 60_000.
    metric ∈ {"price", "open_interest"}. Mirrors metl.fetch_ohlcv_data
    (price=close from /ohlcv) and metl.fetch_oi_data (value from /open-interest).
    """
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_dt.strftime("%Y-%m-%dT%H:%M:%S")
    if metric == "price":
        url = f"{AMBER_BASE}/ohlcv/{symbol}"
        params = {"exchange": "binance", "timeInterval": "minutes",
                  "startDate": start_str, "endDate": end_str}
        ts_keys = ("exchangeTimestamp", "timestamp")
        value_key = "close"
    elif metric == "open_interest":
        url = f"{AMBER_BASE}/open-interest/{symbol}"
        params = {"exchange": "binance",
                  "startDate": start_str, "endDate": end_str}
        ts_keys = ("timestamp", "exchangeTimestamp")
        value_key = "value"
    else:
        raise ValueError(f"unsupported metric {metric!r}")

    sess = amber_session()
    resp = None
    for attempt in range(AMBER_RETRY_LIMIT):
        rl_sleep()
        try:
            r = sess.get(url, params=params, timeout=AMBER_TIMEOUT)
        except requests.RequestException:
            time.sleep(0.5 * (attempt + 1))
            continue
        if r.status_code == 200:
            resp = r
            break
        if r.status_code == 429:
            time.sleep(0.5 * (attempt + 1))
            continue
        return {}
    if resp is None:
        return {}

    try:
        data = resp.json().get("payload", {}).get("data", [])
    except ValueError:
        return {}
    out: dict[int, float] = {}
    for row in data:
        ts_raw = None
        for k in ts_keys:
            if k in row and row[k] is not None:
                ts_raw = row[k]
                break
        if ts_raw is None:
            continue
        try:
            if isinstance(ts_raw, (int, float)):
                ts_ms = int(ts_raw)
            else:
                ts_ms = int(parse_amber_ts(ts_raw).timestamp() * 1000)
        except (ValueError, TypeError):
            continue
        try:
            val = float(row.get(value_key, 0))
        except (ValueError, TypeError):
            continue
        if val <= 0:
            continue
        out[ts_ms // 60000] = val
    return out


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _cache_path(peek_date: datetime.date, metric: str) -> Path:
    return PEEK_CACHE_DIR / f"{peek_date.isoformat()}_{metric}.parquet"


def _universe_cache_path(peek_date: datetime.date) -> Path:
    return PEEK_CACHE_DIR / f"{peek_date.isoformat()}_universe.json"


def load_cache(peek_date: datetime.date, metric: str) -> pd.DataFrame:
    p = _cache_path(peek_date, metric)
    if p.exists():
        try:
            return pd.read_parquet(p)
        except Exception:
            pass
    return pd.DataFrame(columns=["symbol", "ts_min", "value"])


def save_cache(peek_date: datetime.date, metric: str, df: pd.DataFrame) -> None:
    PEEK_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(_cache_path(peek_date, metric), index=False)


def get_universe(peek_date: datetime.date, force: bool = False) -> list[str]:
    p = _universe_cache_path(peek_date)
    if not force and p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            pass
    universe = fetch_universe_for_date(peek_date)
    PEEK_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(universe))
    return universe


def clear_cache_for_date(peek_date: datetime.date) -> list[str]:
    """Delete all cache files for a given date. Returns deleted file names."""
    deleted: list[str] = []
    if not PEEK_CACHE_DIR.exists():
        return deleted
    for f in PEEK_CACHE_DIR.glob(f"{peek_date.isoformat()}_*"):
        f.unlink()
        deleted.append(f.name)
    return deleted


# ---------------------------------------------------------------------------
# Amber data fetch + cache
# ---------------------------------------------------------------------------

def fetch_amber_with_cache(peek_date: datetime.date, metric: str,
                            snap_dt: datetime.datetime,
                            force: bool = False) -> pd.DataFrame:
    """Returns merged cache+delta DataFrame for (peek_date, metric).

    Same-day: reads cache, fetches only the per-symbol delta (s_max+1, snap_dt]
    from Amberdata, merges, saves. Past dates: cache is bypassed and not
    written — Amberdata may revise historical data and we want a fresh
    snapshot each time. (Override with `force=False` from caller is not
    supported for past dates; force only controls whether to *bypass* a
    same-day cache.)
    """
    utc_today = datetime.datetime.now(datetime.timezone.utc).date()
    is_today = peek_date == utc_today
    use_cache = is_today and not force

    cache = load_cache(peek_date, metric) if use_cache \
        else pd.DataFrame(columns=["symbol", "ts_min", "value"])

    if not cache.empty:
        sym_max = cache.groupby("symbol")["ts_min"].max().to_dict()
    else:
        sym_max = {}

    universe = get_universe(peek_date, force=(not use_cache))

    day_start = datetime.datetime.combine(
        peek_date, datetime.time(0, 0, 0),
        tzinfo=datetime.timezone.utc,
    )
    day_start_ts_min = int(day_start.timestamp() // 60)
    snap_ts_min = int(snap_dt.timestamp() // 60)

    # Amberdata's endDate is EXCLUSIVE — fetching to snap_dt would miss the
    # snap_ts_min bar itself. Push end forward 1 minute so the response
    # includes [start, snap_ts_min]. compute_ranks_from_cache filters back
    # to <= snap_ts_min, so any spillover bars at snap_ts_min+1 are ignored.
    fetch_end_dt = snap_dt + datetime.timedelta(minutes=1)

    def fetch_one(sym: str):
        s_max = sym_max.get(sym, day_start_ts_min - 1)
        if s_max >= snap_ts_min:
            return sym, []
        eff_start_min = max(day_start_ts_min, s_max + 1)
        eff_start_dt = datetime.datetime.fromtimestamp(
            eff_start_min * 60, tz=datetime.timezone.utc,
        )
        result = fetch_amber_metric(metric, sym, eff_start_dt, fetch_end_dt)
        rows = [(sym, ts, val) for ts, val in result.items()]
        return sym, rows

    new_rows: list[tuple] = []
    if universe:
        with ThreadPoolExecutor(max_workers=AMBER_MAX_WORKERS) as ex:
            for _, rows in ex.map(fetch_one, universe):
                new_rows.extend(rows)

    if new_rows:
        new_df = pd.DataFrame(new_rows, columns=["symbol", "ts_min", "value"])
        combined = pd.concat([cache, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["symbol", "ts_min"], keep="last")
        if use_cache:
            save_cache(peek_date, metric, combined)
        return combined
    return cache


def compute_ranks_from_cache(df: pd.DataFrame,
                              peek_date: datetime.date,
                              snap_dt: datetime.datetime) -> list[tuple]:
    """Compute pct_change since 00:00 anchor and return top-FREQ_WIDTH ranks.

    Anchor per symbol = earliest cached bar in [day_start, snap_dt].
    Snap per symbol   = latest cached bar in [day_start, snap_dt].

    Returns: [(rank, normalized_base, pct_change_decimal), ...] sorted by rank.
    """
    if df.empty:
        return []
    day_start_ts_min = int(datetime.datetime.combine(
        peek_date, datetime.time(0, 0, 0),
        tzinfo=datetime.timezone.utc,
    ).timestamp() // 60)
    snap_ts_min = int(snap_dt.timestamp() // 60)

    df = df[(df["ts_min"] >= day_start_ts_min) & (df["ts_min"] <= snap_ts_min)]
    if df.empty:
        return []

    pct_changes: list[tuple[str, float]] = []
    for sym, sub in df.groupby("symbol"):
        sub = sub.sort_values("ts_min")
        if sub.empty:
            continue
        anchor = float(sub.iloc[0]["value"])
        snap = float(sub.iloc[-1]["value"])
        if anchor <= 0 or snap <= 0:
            continue
        pct = (snap / anchor) - 1.0
        base = oa.normalize_symbol(sym)
        if base is None:
            continue
        pct_changes.append((base, pct))

    pct_changes.sort(key=lambda x: x[1], reverse=True)
    top = pct_changes[:FREQ_WIDTH]
    return [(i + 1, base, pct) for i, (base, pct) in enumerate(top)]


# ---------------------------------------------------------------------------
# DB-source helpers (legacy --source db path, kept for past-date speed)
# ---------------------------------------------------------------------------

def coverage_minutes(metric: str, peek_date: datetime.date) -> int:
    """Distinct-minute count in market.leaderboards for [00:00, deployment_start)."""
    anchor_hour = (DEPLOYMENT_START_HOUR - oa.INDEX_LOOKBACK) % 24
    start_dt = datetime.datetime.combine(peek_date, datetime.time(0, 0))
    end_dt = datetime.datetime.combine(peek_date,
                                        datetime.time(DEPLOYMENT_START_HOUR, 0))
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(DISTINCT timestamp_utc)
        FROM market.leaderboards
        WHERE metric = %s AND anchor_hour = %s AND variant = 'close'
          AND timestamp_utc >= %s AND timestamp_utc <  %s
    """, (metric, anchor_hour, start_dt, end_dt))
    n = cur.fetchone()[0]
    cur.close()
    conn.close()
    return int(n or 0)


def spawn_indexer(metric: str, peek_date: datetime.date) -> subprocess.Popen:
    date_str = peek_date.isoformat()
    cmd = [
        sys.executable, str(INDEXER),
        "--metric", metric, "--source", "db",
        "--start", date_str, "--end", date_str,
        "--triggered-by", "cli", "--run-tag", f"peek_{date_str}",
    ]
    return subprocess.Popen(cmd, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True)


def latest_bar_in_window_db(peek_date: datetime.date) -> datetime.datetime | None:
    anchor_hour = (DEPLOYMENT_START_HOUR - oa.INDEX_LOOKBACK) % 24
    start_dt = datetime.datetime.combine(peek_date, datetime.time(0, 0))
    end_dt = datetime.datetime.combine(peek_date,
                                        datetime.time(DEPLOYMENT_START_HOUR, 0))
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT MAX(timestamp_utc) FROM (
            SELECT DISTINCT timestamp_utc FROM market.leaderboards
            WHERE metric = 'price' AND anchor_hour = %s AND variant = 'close'
              AND timestamp_utc >= %s AND timestamp_utc < %s
            INTERSECT
            SELECT DISTINCT timestamp_utc FROM market.leaderboards
            WHERE metric = 'open_interest' AND anchor_hour = %s AND variant = 'close'
              AND timestamp_utc >= %s AND timestamp_utc < %s
        ) t
    """, (anchor_hour, start_dt, end_dt, anchor_hour, start_dt, end_dt))
    ts = cur.fetchone()[0]
    cur.close()
    conn.close()
    return ts


def freq_at_exact_timestamp_db(metric: str, ts: datetime.datetime,
                                peek_date: datetime.date) -> dict:
    from collections import Counter
    anchor_hour = (DEPLOYMENT_START_HOUR - oa.INDEX_LOOKBACK) % 24
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT s.binance_id
        FROM market.leaderboards l
        JOIN market.symbols s ON s.symbol_id = l.symbol_id
        WHERE l.metric = %s AND l.anchor_hour = %s AND l.variant = 'close'
          AND l.rank <= %s AND l.timestamp_utc = %s
    """, (metric, anchor_hour, FREQ_WIDTH, ts))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    counter: Counter = Counter()
    for (raw_sym,) in rows:
        base = oa.normalize_symbol(raw_sym) if raw_sym else None
        if base is None:
            continue
        counter[base] += 1
    return {peek_date: counter}


def rank_table_at_timestamp_db(metric: str, ts: datetime.datetime) -> list:
    """Top-FREQ_WIDTH ranks at exact ts from market.leaderboards. Returns
    [(rank, base, pct_change_decimal), ...]."""
    anchor_hour = (DEPLOYMENT_START_HOUR - oa.INDEX_LOOKBACK) % 24
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT l.rank, s.binance_id, l.pct_change
        FROM market.leaderboards l
        JOIN market.symbols s ON s.symbol_id = l.symbol_id
        WHERE l.metric = %s AND l.anchor_hour = %s AND l.variant = 'close'
          AND l.rank <= %s AND l.timestamp_utc = %s
        ORDER BY l.rank
    """, (metric, anchor_hour, FREQ_WIDTH, ts))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    out = []
    for rank, raw_sym, pct in rows:
        base = oa.normalize_symbol(raw_sym) if raw_sym else None
        if base is None:
            continue
        out.append((int(rank), base, float(pct) if pct is not None else 0.0))
    return out


# ---------------------------------------------------------------------------
# Source dispatchers — each returns (basket, snap_dt, fallback_dt, price_ranks,
# oi_ranks, timing_dict). price_ranks / oi_ranks are [(rank, base, pct), ...].
# ---------------------------------------------------------------------------

def run_amber_source(peek_dt: datetime.date, no_cache: bool):
    now = datetime.datetime.now(datetime.timezone.utc)
    utc_today = now.date()
    canonical_snap = datetime.datetime.combine(
        peek_dt, datetime.time(DEPLOYMENT_START_HOUR, 0, 0),
        tzinfo=datetime.timezone.utc,
    )
    if peek_dt < utc_today:
        snap_dt = canonical_snap
    else:
        snap_dt = min(now, canonical_snap)

    fetch_t0 = time.time()
    print(f"[peek] target date: {peek_dt.isoformat()}  source: amber  "
          f"snap target: {snap_dt:%Y-%m-%d %H:%M:%S} UTC")
    print(f"[peek] fetching universe + per-symbol deltas (cache: "
          f"{'BYPASSED' if no_cache else 'enabled'})")
    price_df = fetch_amber_with_cache(peek_dt, "price", snap_dt, force=no_cache)
    oi_df = fetch_amber_with_cache(peek_dt, "open_interest", snap_dt,
                                    force=no_cache)
    fetch_seconds = time.time() - fetch_t0

    rank_t0 = time.time()
    price_ranks = compute_ranks_from_cache(price_df, peek_dt, snap_dt)
    oi_ranks = compute_ranks_from_cache(oi_df, peek_dt, snap_dt)

    price_top = {base for _, base, _ in price_ranks}
    oi_top = {base for _, base, _ in oi_ranks}
    basket = sorted(price_top & oi_top)
    rank_seconds = time.time() - rank_t0

    fallback_dt = snap_dt if snap_dt < canonical_snap else None
    return (basket, snap_dt, fallback_dt, price_ranks, oi_ranks,
            {"fetch_seconds": round(fetch_seconds, 1),
             "rank_seconds": round(rank_seconds, 1)})


def run_db_source(peek_dt: datetime.date, force_build: bool):
    expected = DEPLOYMENT_START_HOUR * 60
    metrics = ["price", "open_interest"]
    print(f"[peek] target date: {peek_dt.isoformat()}  source: db  "
          f"window: [00:00, {DEPLOYMENT_START_HOUR:02d}:00) UTC")
    to_build = []
    for m in metrics:
        n = coverage_minutes(m, peek_dt)
        mark = "✓" if n >= expected else "✗"
        print(f"  coverage: {m:14s} {n:>4}/{expected} minutes {mark}")
        if force_build or n < expected:
            to_build.append(m)

    build_t0 = time.time()
    if to_build:
        print(f"[peek] building {to_build} for {peek_dt} (parallel)")
        procs = {m: spawn_indexer(m, peek_dt) for m in to_build}
        for m, p in procs.items():
            out, _ = p.communicate()
            if p.returncode != 0:
                sys.stderr.write(
                    f"[peek] {m} build FAILED rc={p.returncode}\n"
                    f"{(out or '')[-2000:]}\n")
                sys.exit(2)
            print(f"  build done: {m}")
    build_seconds = time.time() - build_t0

    freq_t0 = time.time()
    price_all = oa._load_frequency_from_db(
        "price", FREQ_WIDTH, SAMPLE_INTERVAL, MODE, min_mcap=MIN_MCAP,
    )
    oi_all = oa._load_frequency_from_db(
        "open_interest", FREQ_WIDTH, SAMPLE_INTERVAL, MODE, min_mcap=MIN_MCAP,
    )
    price_freq = {peek_dt: price_all.get(peek_dt, {})}
    oi_freq = {peek_dt: oi_all.get(peek_dt, {})}

    canonical_snap = datetime.datetime.combine(
        peek_dt, datetime.time(DEPLOYMENT_START_HOUR, 0, 0),
        tzinfo=datetime.timezone.utc,
    )
    fallback_dt = None
    if not price_freq[peek_dt] and not oi_freq[peek_dt]:
        fallback_dt = latest_bar_in_window_db(peek_dt)
        if fallback_dt:
            print(f"  [peek] {DEPLOYMENT_START_HOUR:02d}:00 UTC bar not in "
                  f"market.leaderboards yet — falling back to latest available: "
                  f"{fallback_dt:%Y-%m-%d %H:%M:%S} UTC")
            price_freq = freq_at_exact_timestamp_db("price", fallback_dt, peek_dt)
            oi_freq = freq_at_exact_timestamp_db("open_interest", fallback_dt,
                                                   peek_dt)

    overlap_df = oa.compute_overlap(
        price_freq, oi_freq,
        freq_cutoff=FREQ_CUTOFF, sort_by=SORT_BY,
        overlap_dimensions=OVERLAP_DIMENSIONS,
    )
    freq_seconds = time.time() - freq_t0

    row = overlap_df[overlap_df["date"] == pd.Timestamp(peek_dt)]
    basket = list(row["overlap_symbols"].iloc[0]) if len(row) else []

    snap_dt = fallback_dt or canonical_snap
    diag_ts = fallback_dt or canonical_snap
    price_ranks = rank_table_at_timestamp_db("price", diag_ts)
    oi_ranks = rank_table_at_timestamp_db("open_interest", diag_ts)

    return (basket, snap_dt, fallback_dt, price_ranks, oi_ranks,
            {"build_seconds": round(build_seconds, 1),
             "freq_seconds": round(freq_seconds, 1)})


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def print_diagnostics(price_ranks, oi_ranks, basket: list[str],
                       diag_ts: datetime.datetime) -> None:
    basket_set = set(basket)

    def _print_table(label, rows):
        print(f"\n[diagnostics] {label} top-{FREQ_WIDTH} @ "
              f"{diag_ts:%Y-%m-%d %H:%M:%S} UTC  (★ = in basket)")
        if not rows:
            print(f"  (no rows at this timestamp)")
            return
        for rank, base, pct in rows:
            marker = "★" if base in basket_set else " "
            print(f"  {rank:>3}.  {base:<12}  {pct * 100:+8.3f}%  {marker}")

    _print_table("price", price_ranks)
    _print_table("open_interest", oi_ranks)


def write_json_output(out_path: Path, peek_dt: datetime.date, basket: list[str],
                       snap_dt: datetime.datetime,
                       fallback_dt: datetime.datetime | None,
                       timing: dict, source: str,
                       diagnostics_block: dict | None) -> None:
    payload = {
        "date": peek_dt.isoformat(),
        "symbols": basket,
        "basket_size": len(basket),
        "snapshot_timestamp_utc": snap_dt.isoformat(),
        "is_partial": fallback_dt is not None or (
            snap_dt < datetime.datetime.combine(
                peek_dt, datetime.time(DEPLOYMENT_START_HOUR, 0, 0),
                tzinfo=datetime.timezone.utc)
        ),
        "source": source,
        "timing": timing,
        "run_config": {
            "freq_width": FREQ_WIDTH,
            "freq_cutoff": FREQ_CUTOFF,
            "mode": MODE,
            "deployment_start_hour": DEPLOYMENT_START_HOUR,
            "index_lookback": oa.INDEX_LOOKBACK,
            "sort_lookback": oa._resolve_sort_lookback(),
            "sample_interval": SAMPLE_INTERVAL,
            "min_mcap": MIN_MCAP,
            "overlap_dimensions": OVERLAP_DIMENSIONS,
            "sort_by": SORT_BY,
            "price_ranking_metric": "pct_change",
            "oi_ranking_metric": "pct_change",
        },
    }
    if diagnostics_block is not None:
        payload["diagnostics"] = diagnostics_block
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("date", type=str, nargs="?",
                    help="Date YYYY-MM-DD whose basket to peek")
    ap.add_argument("--source", choices=["amber", "db"], default="amber",
                    help="Data source: 'amber' (default) goes direct to "
                         "Amberdata with a per-date cache. 'db' uses "
                         "market.leaderboards (legacy path; faster for past "
                         "dates with full coverage already in DB).")
    ap.add_argument("--out", type=str, default=None,
                    help="Optional JSON output path for scripted diff against "
                         "live_deploys_signal.csv")
    ap.add_argument("--diagnostics", action="store_true",
                    help="Print top-FREQ_WIDTH rank tables (price + OI) at the "
                         "snapshot timestamp, marking which symbols ended up "
                         "in the basket. Also embedded in --out JSON.")
    ap.add_argument("--no-cache", action="store_true",
                    help="(amber path) Bypass cache and refetch from Amberdata. "
                         "Useful when source data has been revised.")
    ap.add_argument("--force-build", action="store_true",
                    help="(db path) Rebuild leaderboards even if [00:00, 06:00) "
                         "coverage is complete.")
    ap.add_argument("--clear-cache", type=str, default=None, metavar="YYYY-MM-DD",
                    help="Delete all peek_cache files for the given date and exit.")
    args = ap.parse_args()

    if args.clear_cache:
        try:
            d = datetime.date.fromisoformat(args.clear_cache)
        except ValueError:
            ap.error(f"--clear-cache requires YYYY-MM-DD, got {args.clear_cache!r}")
        deleted = clear_cache_for_date(d)
        if deleted:
            print(f"[peek] cleared {len(deleted)} cache file(s) for {d}: "
                  f"{', '.join(deleted)}")
        else:
            print(f"[peek] no cache files to clear for {d}")
        return

    if not args.date:
        ap.error("date positional arg required (or use --clear-cache)")
    try:
        peek_dt = datetime.date.fromisoformat(args.date)
    except ValueError:
        ap.error(f"date must be YYYY-MM-DD, got {args.date!r}")
    utc_today = datetime.datetime.now(datetime.timezone.utc).date()
    if peek_dt > utc_today + datetime.timedelta(days=1):
        ap.error(f"date {peek_dt} is in the future (UTC); no data possible")

    if args.source == "amber":
        basket, snap_dt, fallback_dt, price_ranks, oi_ranks, timing = \
            run_amber_source(peek_dt, no_cache=args.no_cache)
    else:
        basket, snap_dt, fallback_dt, price_ranks, oi_ranks, timing = \
            run_db_source(peek_dt, force_build=args.force_build)

    canonical_snap = datetime.datetime.combine(
        peek_dt, datetime.time(DEPLOYMENT_START_HOUR, 0, 0),
        tzinfo=datetime.timezone.utc,
    )
    is_partial = fallback_dt is not None or snap_dt < canonical_snap
    snap_label = (
        f"snapshot @ {snap_dt:%H:%M:%S} UTC (partial)"
        if is_partial
        else f"snapshot @ {DEPLOYMENT_START_HOUR:02d}:00:00 UTC"
    )
    print()
    print(f"[peek-date {peek_dt.isoformat()}] {len(basket)} symbols  ({snap_label})")
    print(f"  → {' '.join(basket) if basket else '(empty basket)'}")
    timing_str = "  ".join(f"{k}={v}s" for k, v in timing.items())
    print(f"  {timing_str}")

    diagnostics_block = None
    if args.diagnostics:
        diag_ts = snap_dt
        print_diagnostics(price_ranks, oi_ranks, basket, diag_ts)
        basket_set = set(basket)
        diagnostics_block = {
            "snapshot_timestamp_utc": diag_ts.isoformat(),
            "price_top": [
                {"rank": r, "base": b, "pct_change": pct,
                 "in_basket": b in basket_set}
                for r, b, pct in price_ranks
            ],
            "oi_top": [
                {"rank": r, "base": b, "pct_change": pct,
                 "in_basket": b in basket_set}
                for r, b, pct in oi_ranks
            ],
        }

    if args.out:
        out_path = Path(args.out)
        write_json_output(out_path, peek_dt, basket, snap_dt, fallback_dt,
                            timing, args.source, diagnostics_block)
        print(f"  wrote {out_path}")


if __name__ == "__main__":
    main()
