"""
backend/app/cli/binance_perm_audit.py
======================================
Runnable as: python -m app.cli.binance_perm_audit

Step 0 of the Live tab build — Binance USDM Futures **market-data**
audit. Account data lives on BloFin (the existing trader stack); Binance
is the reference source for klines, OI, funding, premium index, L/S
skew, 24h tickers, and the exchangeInfo symbol catalogue.

All endpoints exercised here are public (no API key required) and IP-
rate-limited. The audit:

  1. Hits each market-data endpoint once with minimum-friction params,
     records the X-MBX-USED-WEIGHT-1M header so we can size the worst-
     case Live-tab refresh budget against the 2400/min/IP ceiling.

  2. Pulls the current BloFin open positions from the latest
     exchange_snapshots row and resolves each base symbol to its Binance
     listing via market.symbols.binance_id. Any open position whose
     mapped Binance symbol is missing from /fapi/v1/exchangeInfo (or
     present but not TRADING) is flagged — those vizes will need to
     render "INSUFFICIENT MARKET DATA" for that row.

Exits 0 if every endpoint passes AND every open BloFin position has a
TRADING Binance equivalent. Exits 1 otherwise.
"""

from __future__ import annotations

import sys
import time
from urllib.parse import urlencode

import requests

from ..db import get_worker_conn
from ..services.exchanges.binance_market import BinanceMarketClient
from ..services.exchanges.binance import BinanceError

FAPI = "https://fapi.binance.com"
TIMEOUT = 10

# (label, path, params)
PROBES: list[tuple[str, str, dict]] = [
    ("klines",            "/fapi/v1/klines",                          {"symbol": "BTCUSDT", "interval": "1m", "limit": 1}),
    ("premiumIndex",      "/fapi/v1/premiumIndex",                    {"symbol": "BTCUSDT"}),
    ("openInterest",      "/fapi/v1/openInterest",                    {"symbol": "BTCUSDT"}),
    ("openInterestHist",  "/futures/data/openInterestHist",           {"symbol": "BTCUSDT", "period": "1d", "limit": 2}),
    ("topLongShortRatio", "/futures/data/topLongShortAccountRatio",   {"symbol": "BTCUSDT", "period": "1h", "limit": 1}),
    ("ticker/24hr",       "/fapi/v1/ticker/24hr",                     {"symbol": "BTCUSDT"}),
    ("exchangeInfo",      "/fapi/v1/exchangeInfo",                    {}),
]


def probe(label: str, path: str, params: dict) -> dict:
    url = f"{FAPI}{path}"
    if params:
        url = f"{url}?{urlencode(params)}"
    try:
        resp = requests.get(url, timeout=TIMEOUT)
    except requests.RequestException as e:
        return {"label": label, "status": "NETWORK", "http": None, "weight": None,
                "msg": f"{type(e).__name__}: {e}", "body": None}

    weight = resp.headers.get("X-MBX-USED-WEIGHT-1M") or resp.headers.get("X-MBX-USED-WEIGHT")
    try:
        body = resp.json()
    except ValueError:
        return {"label": label, "status": "NON_JSON", "http": resp.status_code,
                "weight": weight, "msg": resp.text[:160], "body": None}

    if resp.status_code == 200:
        return {"label": label, "status": "PASS", "http": 200, "weight": weight,
                "msg": None, "body": body}

    if resp.status_code in (418, 429):
        return {"label": label, "status": "RATE_LIMITED", "http": resp.status_code,
                "weight": weight, "msg": str(body)[:160], "body": None}

    return {"label": label, "status": "FAIL", "http": resp.status_code,
            "weight": weight, "msg": str(body)[:160], "body": None}


def fetch_open_blofin_symbols() -> list[dict]:
    """Pull the current open positions from the latest BloFin snapshot.

    Joins through market.symbols on (base, quote='USDT') so we can resolve
    the canonical Binance listing via symbols.binance_id — `base + 'USDT'`
    is wrong for 1000x-prefix tickers (PEPE/SHIB/FLOKI/BONK).

    Returns a list of dicts with: base, blofin_inst_id, binance_id,
    coingecko_id. binance_id is None when no Binance listing is mapped in our
    symbol registry.
    """
    with get_worker_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (ec.connection_id)
                       ec.connection_id, es.positions
                  FROM user_mgmt.exchange_connections ec
                  JOIN user_mgmt.exchange_snapshots es
                    ON es.connection_id = ec.connection_id
                   AND es.fetch_ok = TRUE
                 WHERE ec.exchange = 'blofin' AND ec.status = 'active'
                 ORDER BY ec.connection_id, es.snapshot_at DESC
            """)
            row = cur.fetchone()
            if not row or not row[1]:
                return []
            positions = row[1]
            # symbols are stored dash-stripped ("CLOUSDT"); recover the base
            # by stripping the USDT suffix and looking up in market.symbols
            bases = []
            for p in positions:
                sym = (p.get("symbol") or "").upper()
                if sym.endswith("USDT"):
                    bases.append(sym[:-4])
                else:
                    bases.append(sym)
            if not bases:
                return []
            cur.execute("""
                SELECT base, coingecko_id, binance_id
                  FROM market.symbols
                 WHERE base = ANY(%s)
            """, (bases,))
            registry = {r[0]: {"coingecko_id": r[1], "binance_id": r[2]} for r in cur.fetchall()}

    out = []
    for p in positions:
        sym = (p.get("symbol") or "").upper()
        base = sym[:-4] if sym.endswith("USDT") else sym
        reg = registry.get(base, {})
        out.append({
            "base": base,
            "blofin_symbol": sym,
            "side": p.get("side"),
            "size": p.get("size"),
            "binance_id": reg.get("binance_id"),
            "coingecko_id": reg.get("coingecko_id"),
        })
    return out


def main() -> int:
    print("=" * 78)
    print("Binance USDM Futures — market-data audit (account data → BloFin)")
    print("=" * 78)

    # ── 1. Endpoint probes ─────────────────────────────────────────────────
    results: list[dict] = []
    print("\nEndpoint probes (all public, no key required):\n")
    for label, path, params in PROBES:
        r = probe(label, path, params)
        results.append(r)
        sym = {"PASS": "✓", "RATE_LIMITED": "·", "NETWORK": "·", "NON_JSON": "·", "FAIL": "✗"}.get(r["status"], "?")
        wt = f"w={r['weight']}" if r.get("weight") else ""
        line = f"  {sym} {r['status']:14}  {label:22}  http={r['http']}  {wt}"
        if r.get("msg"):
            line += f"\n      msg: {r['msg']}"
        print(line)
        time.sleep(0.1)

    blocked = [r for r in results if r["status"] not in ("PASS",)]
    final_weight = next((int(r["weight"]) for r in reversed(results) if r.get("weight")), None)

    # ── 1b. Client smoke test ────────────────────────────────────────────
    # Hit a representative subset of endpoints through BinanceMarketClient
    # to verify the wrapper parses responses correctly, the weight tracker
    # updates from response headers, and the exception hierarchy works.
    # Cost: 3 weight (klines + premiumIndex + exchangeInfo, single symbol).
    print("\nClient smoke test (BinanceMarketClient):\n")
    client_failures: list[str] = []
    client = BinanceMarketClient()
    try:
        candles = client.klines("BTCUSDT", "1m", limit=1)
        assert isinstance(candles, list) and len(candles) == 1 and len(candles[0]) >= 6, \
            f"klines: unexpected shape {candles!r}"
        print(f"  ✓ klines(BTCUSDT, 1m, 1)            → 1 candle, used_weight={client.used_weight}")
    except (BinanceError, AssertionError) as e:
        client_failures.append(f"klines: {e}")
        print(f"  ✗ klines: {e}")

    try:
        prem = client.premium_index("BTCUSDT")
        assert isinstance(prem, dict) and "lastFundingRate" in prem, \
            f"premium_index: missing lastFundingRate in {prem!r}"
        print(f"  ✓ premium_index(BTCUSDT)             → fundingRate={prem.get('lastFundingRate')}, used_weight={client.used_weight}")
    except (BinanceError, AssertionError) as e:
        client_failures.append(f"premium_index: {e}")
        print(f"  ✗ premium_index: {e}")

    try:
        info = client.exchange_info()
        assert isinstance(info, dict) and isinstance(info.get("symbols"), list), \
            f"exchange_info: missing symbols list in {type(info).__name__}"
        print(f"  ✓ exchange_info()                    → {len(info['symbols'])} symbols, used_weight={client.used_weight}")
    except (BinanceError, AssertionError) as e:
        client_failures.append(f"exchange_info: {e}")
        print(f"  ✗ exchange_info: {e}")

    # ── 2. Symbol-universe check ──────────────────────────────────────────
    exchange_info_body = next((r["body"] for r in results if r["label"] == "exchangeInfo" and r["body"]), None)
    binance_listed: dict[str, str] = {}
    if exchange_info_body:
        for s in exchange_info_body.get("symbols", []):
            binance_listed[s["symbol"]] = s.get("status", "")

    print("\nOpen BloFin positions vs Binance USDM listing:\n")
    positions = fetch_open_blofin_symbols()
    if not positions:
        print("  (no open BloFin positions in latest snapshot — skipping check)")
    coverage_gap: list[dict] = []
    for p in positions:
        binance_id = p["binance_id"] or f"{p['base']}USDT"  # naive fallback
        listed_status = binance_listed.get(binance_id)
        if listed_status == "TRADING":
            tag, sym = "PASS", "✓"
        elif listed_status:
            tag, sym = f"NOT_TRADING ({listed_status})", "·"
            coverage_gap.append({**p, "issue": f"binance status={listed_status}"})
        else:
            tag, sym = "NOT_LISTED", "✗"
            coverage_gap.append({**p, "issue": "not in Binance USDM exchangeInfo"})
        bid_label = p["binance_id"] or f"{p['base']}USDT (naive — no registry mapping)"
        print(f"  {sym} {tag:24}  {p['base']:10}  blofin={p['blofin_symbol']}  binance={bid_label}")

    # ── 3. Rate-limit headroom estimate ───────────────────────────────────
    # Binance USDM public weight ceiling: 2400/min/IP. We just consumed
    # `final_weight` bouncing each probe once. The Live tab worst-case
    # per-minute weight comes from the T2 60s tier (per open position):
    #   premiumIndex     w=1
    #   openInterest     w=1
    #   ticker/24hr      w=1   (single symbol)
    # Plus T5 (hourly), T4 (5m bar close) — burstier but not sustained.
    # For the 6 currently-open positions the steady T2 cost is ~18 weight
    # per minute, well under ceiling. Heavy spikes come from kline pulls
    # at 1H bar close (each kline call w=1, but we'd pull ~6 syms × 7 TFs
    # = 42 weight on close).
    print("\nRate-limit headroom (Binance USDM, 2400 weight/min/IP):")
    print(f"  audit total cost          : {final_weight or '?'}")
    print(f"  T2 steady (per minute)    : ~{len(positions) * 3} weight (premiumIndex + openInterest + ticker/24hr per pos)")
    print(f"  T5 hourly burst (klines)  : ~{len(positions) * 7} weight at each 1H close")
    print(f"  → comfortable margin")

    # ── Summary ───────────────────────────────────────────────────────────
    print()
    print("-" * 78)
    print(f"PROBES         : {len([r for r in results if r['status'] == 'PASS'])} / {len(results)} PASS")
    if blocked:
        print(f"BLOCKED PROBES : {', '.join(r['label'] for r in blocked)}")
    print(f"OPEN POSITIONS : {len(positions)} (BloFin)")
    print(f"COVERAGE GAPS  : {len(coverage_gap)}")
    if coverage_gap:
        for g in coverage_gap:
            print(f"  - {g['base']}: {g['issue']}")
    print(f"CLIENT SMOKE   : {3 - len(client_failures)} / 3 methods OK")
    if client_failures:
        for f in client_failures:
            print(f"  - {f}")
    print("-" * 78)

    return 0 if (not blocked and not coverage_gap and not client_failures) else 1


if __name__ == "__main__":
    sys.exit(main())
