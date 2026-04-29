"""
pipeline/blofin_universe.py

Single source of truth for "was symbol X listed on BloFin at date D?"

Source: BloFin's public `/api/v1/market/instruments?instType=SWAP` endpoint.
Each instrument row carries a `listTime` (Unix ms). We index by base
currency and keep the EARLIEST listTime across all tradeable quote
currencies (USDT and USDC). Once a base is live on BloFin in any
tradeable quote, the strategy can trade it.

Time-correctness caveat: the API returns CURRENTLY LIVE instruments
only. Symbols that were listed and later delisted before today are
absent from the response, so they will be reported as "not on BloFin"
for any historical date. In practice this miss is small — symbols
delisted from BloFin typically also lose Binance data and drop out of
our basket organically. A daily snapshot cron is queued as a follow-up
to capture future delistings.

Public API:
    load_blofin_universe(snapshot_path="") -> dict[base, list_ms]
    is_listed_at(base, date, universe)     -> bool
    save_universe_csv(universe, path)
"""

from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import requests

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

ENDPOINTS = [
    ("https://openapi.blofin.com/api/v1/market/instruments", {"instType": "SWAP"}),
    ("https://openapi.blofin.com/api/v1/market/instruments", {}),
]

TRADEABLE_QUOTES = {"USDT", "USDC"}


def _normalize_base(s: str) -> str:
    s = s.upper().strip()
    for suffix in ("USDT", "USDC"):
        if s.endswith(suffix) and len(s) > len(suffix):
            return s[: -len(suffix)]
    return s


def load_blofin_universe(
    snapshot_path: str = "",
    user_agent: str = DEFAULT_USER_AGENT,
    timeout: int = 10,
    verbose: bool = True,
) -> dict[str, int]:
    """
    Return {base_symbol: earliest_list_ms} across BloFin's USDT+USDC perps.

    If snapshot_path is set and exists, loads from CSV instead of hitting
    the API. CSV format: header `base,list_ms`. Useful for offline /
    deterministic runs and for replaying a captured snapshot.

    Empty dict on total failure — callers should degrade gracefully.
    """
    if snapshot_path and Path(snapshot_path).exists():
        return _load_from_csv(snapshot_path, verbose=verbose)

    headers = {"User-Agent": user_agent, "Accept": "application/json"}
    for url, params in ENDPOINTS:
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if not resp.ok:
                if verbose:
                    print(f"  [blofin_universe] HTTP {resp.status_code} from {resp.url}")
                continue
            data = resp.json()
            instruments = data.get("data") or []
            if not instruments:
                continue
            return _index_instruments(instruments, verbose=verbose)
        except Exception as e:
            if verbose:
                print(f"  [blofin_universe] fetch failed (params={params}): "
                      f"{type(e).__name__}: {e}")

    if verbose:
        print("  [blofin_universe] all attempts failed — returning empty universe")
    return {}


def _index_instruments(instruments: list, verbose: bool = True) -> dict[str, int]:
    universe: dict[str, int] = {}
    skipped_state = 0
    skipped_quote = 0
    for inst in instruments:
        base = (inst.get("baseCurrency") or "").upper().strip()
        quote = (inst.get("quoteCurrency") or "").upper().strip()
        lt = inst.get("listTime")
        state = (inst.get("state") or "").lower()
        if not base or lt is None:
            continue
        if quote not in TRADEABLE_QUOTES:
            skipped_quote += 1
            continue
        if state and state != "live":
            skipped_state += 1
            continue
        try:
            lt_int = int(lt)
        except (TypeError, ValueError):
            continue
        if base not in universe or lt_int < universe[base]:
            universe[base] = lt_int

    if verbose:
        print(f"  [blofin_universe] indexed {len(universe)} bases from "
              f"{len(instruments)} instruments "
              f"(skipped: {skipped_state} non-live state, {skipped_quote} non-USDT/USDC quote)")
    return universe


def _load_from_csv(path: str, verbose: bool = True) -> dict[str, int]:
    universe: dict[str, int] = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            base = (row.get("base") or "").upper().strip()
            try:
                lt = int(row.get("list_ms") or 0)
            except (TypeError, ValueError):
                continue
            if base and lt:
                universe[base] = lt
    if verbose:
        print(f"  [blofin_universe] loaded {len(universe)} bases from {path}")
    return universe


def save_universe_csv(universe: dict[str, int], path: str) -> None:
    """Save a {base: list_ms} dict to CSV for snapshot persistence."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    rows = sorted(universe.items())
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["base", "list_ms", "list_date_utc"])
        for base, lt in rows:
            iso = pd.Timestamp(lt, unit="ms", tz="UTC").strftime("%Y-%m-%d")
            w.writerow([base, lt, iso])


def is_listed_at(
    base: str,
    date: Union[str, "pd.Timestamp"],
    universe: dict[str, int],
) -> bool:
    """
    True iff `base` was listed on BloFin (USDT or USDC quote) on or before
    `date`. `date` may be a string (YYYY-MM-DD), pd.Timestamp, or datetime.
    Returns False if `base` is not in `universe` at all.

    Accepts both bare-base ("BTC") and concat ("BTCUSDT") input forms.
    """
    if not universe:
        return False
    base_norm = _normalize_base(base)
    list_ms = universe.get(base_norm)
    if list_ms is None:
        return False
    ts = pd.Timestamp(date)
    if ts.tz is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return int(ts.timestamp() * 1000) >= list_ms


def listed_subset(
    bases: list[str],
    date: Union[str, "pd.Timestamp"],
    universe: dict[str, int],
) -> list[str]:
    """
    Return the subset of `bases` that were listed on BloFin on or before
    `date`. Preserves input order. Empty universe → empty result (caller
    should treat as "BloFin filter unavailable, skipping").
    """
    if not universe:
        return []
    return [b for b in bases if is_listed_at(b, date, universe)]


if __name__ == "__main__":
    # Smoke test: hit the API, print summary, save snapshot.
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--save", help="Path to save the snapshot CSV")
    p.add_argument("--load", help="Path to load a prior snapshot CSV instead of hitting API")
    p.add_argument("--check-date", help="Check listings as of YYYY-MM-DD")
    p.add_argument("--check-bases", help="Comma-separated bases to check (default: BTC,ETH,SOL)",
                   default="BTC,ETH,SOL,DOGE,ADA")
    args = p.parse_args()

    u = load_blofin_universe(snapshot_path=args.load or "")
    print(f"\nUniverse size: {len(u)} bases")
    if u:
        sample = sorted(u.items(), key=lambda kv: kv[1])[:5]
        print("Earliest 5 listings:")
        for base, lt in sample:
            iso = pd.Timestamp(lt, unit="ms", tz="UTC").strftime("%Y-%m-%d")
            print(f"  {base:8s}  {iso}  ({lt})")

    if args.save:
        save_universe_csv(u, args.save)
        print(f"\nSaved snapshot → {args.save}")

    if args.check_date and u:
        print(f"\nListings check as of {args.check_date}:")
        for b in args.check_bases.split(","):
            b = b.strip()
            print(f"  {b:8s}  {'YES' if is_listed_at(b, args.check_date, u) else 'no'}")
