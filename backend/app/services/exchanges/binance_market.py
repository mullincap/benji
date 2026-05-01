"""
backend/app/services/exchanges/binance_market.py
=================================================
Public market-data client for Binance USDM Futures — the **reference
source** for klines, OI, funding, premium index, L/S skew, and the
exchangeInfo symbol catalogue powering the Manager Live tab.

Per the venue split locked in CLAUDE.md:
  * Account data → BloFin (existing trader code paths)
  * Market data → Binance USDM, **public endpoints only**

No auth, no API key. Binance's public futures endpoints are IP-rate-
limited at 2400 weight/min/IP. This client tracks the
X-MBX-USED-WEIGHT-1M response header and refuses to send when an
outgoing call would push the rolling-window total past the ceiling
(minus headroom). The expected steady-state load from the Live tab is
~18 weight/min for 6 open positions — well under the limit — but the
pre-flight guard exists so a runaway worker can't accidentally trip a
418 Ban that takes hours to clear.

Sync (`requests.Session`) for parity with the existing exchange client
modules (Celery worker context, FastAPI sync route handlers + thread
pool). Reuses the BinanceError / BinanceNetworkError /
BinanceRateLimitError exception hierarchy from binance.py.
"""

from __future__ import annotations

import logging
import time
from typing import Any
from urllib.parse import urlencode

import requests

from .binance import (
    BinanceError,
    BinanceNetworkError,
    BinanceRateLimitError,
)

log = logging.getLogger(__name__)

FAPI_BASE = "https://fapi.binance.com"
DEFAULT_TIMEOUT_S = 10.0

# Per-IP weight ceiling and headroom. Binance docs put the ceiling at
# 2400/minute for the futures REST API; we back off when a call would
# push the running weight past (CEILING − HEADROOM) to leave slack for
# concurrent callers (the same IP serves the FastAPI process + every
# Celery worker + the eventual sidecar).
WEIGHT_CEILING = 2400
WEIGHT_HEADROOM = 200

# Conservative pre-flight estimate per endpoint. The actual cost is
# whatever the response header says; this table is only used to decide
# whether to *send* the call when we're already near the ceiling.
# Underestimating here is safe (we'll pick up the true cost from the
# header on the way back); overestimating just means earlier back-off.
WEIGHT_ESTIMATE: dict[str, int] = {
    "/fapi/v1/klines":                          5,   # 1 if limit≤100, 2 if ≤500, 5 if ≤1000, 10 if >1000
    "/fapi/v1/premiumIndex":                    1,
    "/fapi/v1/openInterest":                    1,
    "/futures/data/openInterestHist":           0,   # /futures/data/* uses a separate raw-request bucket
    "/futures/data/topLongShortAccountRatio":   0,
    "/fapi/v1/ticker/24hr":                     1,   # 40 with no symbol param — caller's responsibility
    "/fapi/v1/exchangeInfo":                    1,
}


class BinanceMarketClient:
    """Sync, weight-aware client for Binance USDM Futures public market data.

    Instance state (current used_weight, last-update timestamp) is updated
    from response headers on every call. Process-local — each FastAPI
    worker / Celery worker / sidecar holds its own counter, but they all
    share the same IP, so the value tends to converge as long as the
    workers are calling Binance at comparable rates.

    Typical usage:
        c = BinanceMarketClient()
        info = c.exchange_info()
        candles = c.klines("BTCUSDT", "1m", limit=288)
        print(c.used_weight)  # → 4 or so, after two calls
    """

    def __init__(self, base_url: str = FAPI_BASE, timeout: float = DEFAULT_TIMEOUT_S):
        self._base = base_url.rstrip("/")
        self._timeout = timeout
        self._session = requests.Session()
        self._used_weight: int = 0
        self._used_weight_at: float = 0.0

    # ── Properties ────────────────────────────────────────────────────────

    @property
    def used_weight(self) -> int:
        """Most recent X-MBX-USED-WEIGHT-1M reading. Reset by Binance every
        rolling minute; we don't decay locally — the next response refreshes
        the value. Reads as 0 until the first successful call."""
        return self._used_weight

    @property
    def used_weight_age_s(self) -> float:
        """Wall-clock seconds since the used_weight reading. Useful to
        decide whether the cached value is still meaningful (Binance's
        rolling window is 60s)."""
        if self._used_weight_at == 0.0:
            return float("inf")
        return time.time() - self._used_weight_at

    # ── Endpoint methods ──────────────────────────────────────────────────

    def klines(
        self, symbol: str, interval: str,
        *, limit: int = 500, start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[list]:
        """GET /fapi/v1/klines — OHLCV candles.

        Returns a list of arrays, each:
        [open_time, open, high, low, close, volume, close_time,
         quote_volume, trade_count, taker_buy_volume, taker_buy_quote, ignore]
        """
        params: dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        return self._get("/fapi/v1/klines", params)

    def premium_index(self, symbol: str | None = None) -> dict | list[dict]:
        """GET /fapi/v1/premiumIndex — mark price, index price, last funding rate.

        With a symbol → single dict. Without → list of dicts for every
        symbol; v1 callers should always pass a symbol to keep weight at 1.
        """
        params = {"symbol": symbol} if symbol else {}
        return self._get("/fapi/v1/premiumIndex", params)

    def open_interest(self, symbol: str) -> dict:
        """GET /fapi/v1/openInterest — current OI (count of open contracts)."""
        return self._get("/fapi/v1/openInterest", {"symbol": symbol})

    def open_interest_hist(
        self, symbol: str, *, period: str = "1d", limit: int = 30,
        start_time_ms: int | None = None, end_time_ms: int | None = None,
    ) -> list[dict]:
        """GET /futures/data/openInterestHist — historical OI series.

        period ∈ {"5m","15m","30m","1h","2h","4h","6h","12h","1d"}.
        For the OI Δ 24h cell in the drill-down: period="1d", limit=2.
        """
        params: dict[str, Any] = {"symbol": symbol, "period": period, "limit": limit}
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        return self._get("/futures/data/openInterestHist", params)

    def top_long_short_account_ratio(
        self, symbol: str, *, period: str = "1h", limit: int = 30,
    ) -> list[dict]:
        """GET /futures/data/topLongShortAccountRatio — top-trader L/S skew."""
        return self._get(
            "/futures/data/topLongShortAccountRatio",
            {"symbol": symbol, "period": period, "limit": limit},
        )

    def ticker_24hr(self, symbol: str | None = None) -> dict | list[dict]:
        """GET /fapi/v1/ticker/24hr — 24h price change, volume, quote volume.

        Single-symbol = weight 1. Without symbol = weight 40 (returns every
        listed symbol). Live tab passes a symbol per call.
        """
        params = {"symbol": symbol} if symbol else {}
        return self._get("/fapi/v1/ticker/24hr", params)

    def exchange_info(self) -> dict:
        """GET /fapi/v1/exchangeInfo — symbol catalogue, lot size filters,
        contract specs. Cached aggressively by callers (refreshes once a day)."""
        return self._get("/fapi/v1/exchangeInfo")

    # ── Internals ─────────────────────────────────────────────────────────

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """Pre-flight weight check, then GET. Updates used_weight from response.

        Raises:
          BinanceRateLimitError — pre-flight ceiling exceeded, or 418/429.
          BinanceNetworkError   — timeout, DNS, connection refused.
          BinanceError          — non-JSON or 4xx/5xx with a JSON error body.
        """
        # Pre-flight: refuse to send when we're already over the threshold.
        # The header reading is from a Binance rolling 60s window, but if
        # it's older than 60s, treat as stale and reset (next call will
        # refresh it from the response).
        #
        # Known corner case: spawn → heavy call (used_weight=2200) → 65s
        # idle → heavy call. Both calls hit the same Binance minute bucket
        # (the rolling window slides continuously), but the second skips
        # the guard because used_weight_age_s > 60. The wrapper accepts
        # this — Binance will return the true running total in the next
        # response, and a second pre-flight refusal would just push the
        # caller into back-off without need. Low-probability scenario for
        # the Live tab's steady ~18/min load.
        if self.used_weight_age_s < 60:
            est = WEIGHT_ESTIMATE.get(path, 1)
            if self._used_weight + est > WEIGHT_CEILING - WEIGHT_HEADROOM:
                raise BinanceRateLimitError(
                    f"Pre-flight refusal: used_weight={self._used_weight} + "
                    f"est={est} > {WEIGHT_CEILING - WEIGHT_HEADROOM} (ceiling - headroom)"
                )

        url = f"{self._base}{path}"
        if params:
            # Drop None values so callers can pass them without manual
            # filtering (e.g. start_time_ms=None means "omit the param").
            params = {k: v for k, v in params.items() if v is not None}
            if params:
                url = f"{url}?{urlencode(params)}"

        try:
            resp = self._session.get(url, timeout=self._timeout)
        except requests.Timeout as e:
            raise BinanceNetworkError(f"Binance request timed out: {e}") from e
        except requests.ConnectionError as e:
            raise BinanceNetworkError(f"Binance connection error: {e}") from e
        except requests.RequestException as e:
            raise BinanceNetworkError(f"Binance request failed: {e}") from e

        # Update weight tracker from response header. Some endpoints
        # (the /futures/data/* family) don't set this header; we leave the
        # previous reading in place so we don't lose state.
        wt = resp.headers.get("X-MBX-USED-WEIGHT-1M") or resp.headers.get("X-MBX-USED-WEIGHT")
        if wt:
            try:
                new_weight = int(wt)
                # Estimate-vs-actual divergence sentinel: if the per-call
                # delta diverges from our static estimate by >2× in either
                # direction, log a warning. Catches cases where Binance
                # silently changes endpoint weights without updating the
                # docs (we'd rather notice from a log line than from a
                # surprise 418 ban).
                est = WEIGHT_ESTIMATE.get(path, 1)
                actual_delta = new_weight - self._used_weight
                if est > 0 and actual_delta > 0:
                    ratio = actual_delta / est
                    if ratio > 2 or ratio < 0.5:
                        log.warning(
                            "binance_market: weight estimate diverged for %s: "
                            "actual=%d, est=%d, ratio=%.2fx",
                            path, actual_delta, est, ratio,
                        )
                self._used_weight = new_weight
                self._used_weight_at = time.time()
            except ValueError:
                pass

        if resp.status_code in (418, 429):
            raise BinanceRateLimitError(
                f"Binance rate-limited (HTTP {resp.status_code}): {resp.text[:200]}"
            )

        try:
            body = resp.json()
        except ValueError as e:
            raise BinanceError(
                f"Binance returned non-JSON ({resp.status_code}): {resp.text[:200]}"
            ) from e

        if resp.status_code >= 400 or (isinstance(body, dict) and body.get("code") and body.get("code") != 200):
            code = body.get("code") if isinstance(body, dict) else None
            msg = body.get("msg") if isinstance(body, dict) else resp.text[:200]
            raise BinanceError(
                f"Binance error: {msg} (code={code}, http={resp.status_code})"
            )

        return body
