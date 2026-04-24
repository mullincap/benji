"""
backend/app/cli/refresh_symbol_registry.py
============================================
Runnable as: python -m app.cli.refresh_symbol_registry

Nightly refresh of market.symbols.binance_id and market.symbols.blofin_id
from each exchange's public /instruments endpoint.

Rationale: the existing table was populated once at migration time and
drifts as exchanges list/delist symbols. The trader successfully entered
GENIUS on BloFin on 2026-04-24, but market.symbols.blofin_id for GENIUS
was NULL — the registry had gone stale. This breaks downstream checks
like "is this basket symbol tradable on BloFin?" in audit.py and any
UI that filters by exchange availability.

Behavior:
  - Pulls BloFin: GET https://openapi.blofin.com/api/v1/market/instruments
    (inst_type=SWAP) → `{base}-USDT` → inst_id
  - Pulls Binance: GET https://fapi.binance.com/fapi/v1/exchangeInfo
    → symbols[] → `{base}USDT` for PERPETUAL contracts → symbol
  - UPSERTs into market.symbols keyed on `base`:
      * Existing row → UPDATE binance_id / blofin_id
      * Missing row  → INSERT new (base, binance_id, blofin_id)
  - Does NOT delete rows — past symbols stay in the table with their
    ids (downstream FKs from market.leaderboards reference them).

Exits 0 on success. Failures (network, auth errors on public endpoints —
shouldn't happen but) are logged and the script exits 1 without
committing.
"""

from __future__ import annotations

import logging
import sys

import requests

from ..db import get_worker_conn


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("refresh_symbol_registry")


BLOFIN_INSTRUMENTS_URL = "https://openapi.blofin.com/api/v1/market/instruments"
BINANCE_EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
HTTP_TIMEOUT_S = 15


def _fetch_blofin_bases_to_inst_ids() -> dict[str, str]:
    """Return {base: inst_id} for all live BloFin USDT perp listings."""
    resp = requests.get(
        BLOFIN_INSTRUMENTS_URL,
        params={"instType": "SWAP"},
        timeout=HTTP_TIMEOUT_S,
    )
    resp.raise_for_status()
    data = resp.json().get("data") or []
    bases: dict[str, str] = {}
    for row in data:
        inst_id = row.get("instId") or ""
        state = (row.get("state") or "").lower()
        if state and state != "live":
            continue
        if not inst_id.endswith("-USDT"):
            continue
        base = inst_id[: -len("-USDT")]
        if base:
            bases[base.upper()] = inst_id
    return bases


def _fetch_binance_bases_to_symbols() -> dict[str, str]:
    """Return {base: symbol} for all live Binance USDT perpetual contracts."""
    resp = requests.get(BINANCE_EXCHANGE_INFO_URL, timeout=HTTP_TIMEOUT_S)
    resp.raise_for_status()
    symbols = resp.json().get("symbols") or []
    bases: dict[str, str] = {}
    for row in symbols:
        sym = row.get("symbol") or ""
        ctype = row.get("contractType")
        status = row.get("status")
        quote = row.get("quoteAsset")
        base = row.get("baseAsset") or ""
        if ctype != "PERPETUAL" or status != "TRADING" or quote != "USDT":
            continue
        if not base:
            continue
        bases[base.upper()] = sym
    return bases


def refresh() -> int:
    try:
        blofin = _fetch_blofin_bases_to_inst_ids()
        log.info(f"blofin: {len(blofin)} SWAP USDT perp listings")
    except Exception as e:
        log.error(f"blofin fetch failed: {e}")
        return 1

    try:
        binance = _fetch_binance_bases_to_symbols()
        log.info(f"binance: {len(binance)} PERPETUAL USDT contracts")
    except Exception as e:
        log.error(f"binance fetch failed: {e}")
        return 1

    all_bases = set(blofin.keys()) | set(binance.keys())
    log.info(f"union: {len(all_bases)} distinct bases")

    conn = get_worker_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT base, binance_id, blofin_id FROM market.symbols")
            existing = {r[0]: (r[1], r[2]) for r in cur.fetchall()}

        inserts = 0
        updates = 0
        with conn.cursor() as cur:
            for base in sorted(all_bases):
                new_binance = binance.get(base)
                new_blofin = blofin.get(base)
                prior = existing.get(base)
                if prior is None:
                    cur.execute(
                        """
                        INSERT INTO market.symbols (base, binance_id, blofin_id, active)
                        VALUES (%s, %s, %s, TRUE)
                        ON CONFLICT (base) DO NOTHING
                        """,
                        (base, new_binance, new_blofin),
                    )
                    inserts += 1
                else:
                    cur_binance, cur_blofin = prior
                    if cur_binance != new_binance or cur_blofin != new_blofin:
                        cur.execute(
                            """
                            UPDATE market.symbols
                               SET binance_id = %s,
                                   blofin_id  = %s
                             WHERE base = %s
                            """,
                            (new_binance, new_blofin, base),
                        )
                        updates += 1
        conn.commit()
        log.info(f"symbol registry refresh: {inserts} inserts, {updates} updates")
        return 0
    except Exception as e:
        conn.rollback()
        log.error(f"db write failed: {e}")
        return 1
    finally:
        conn.close()


def main() -> int:
    return refresh()


if __name__ == "__main__":
    sys.exit(main())
