"""
backend/app/services/exchanges/blofin_adapter.py
=================================================
BloFin concrete adapter for the ExchangeAdapter ABC.

Wraps the existing BlofinREST client at backend/app/cli/trader_blofin.py
without modifying it. Normalizes response shapes to the dataclasses defined
in adapter.py. Field-fallback chains are copied verbatim from the live
trader call sites -- this class is a 1:1 translation layer, not a rewrite.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from app.services.exchanges.adapter import (
    BalanceInfo,
    CapitalEventInfo,
    ExchangeAdapter,
    FillInfo,
    InstrumentInfo,
    OrderResult,
    PositionInfo,
)

if TYPE_CHECKING:
    from app.cli.trader_blofin import BlofinREST

log = logging.getLogger("blofin_adapter")


# BloFin-internal constants; mirror the module constants in trader_blofin.py
# for today's strategy (isolated margin, net position side).
_MARGIN_MODE  = "isolated"
_POSITION_SIDE = "net"


class BloFinAdapter(ExchangeAdapter):
    exchange_name       = "blofin"
    native_sl_supported = True

    def __init__(self, api_key: str, api_secret: str, passphrase: str | None):
        if not passphrase:
            raise ValueError("BloFin requires a passphrase")
        # Lazy import avoids a circular dependency with the cli module and
        # keeps import-time cost off the critical path for non-BloFin subprocesses.
        from app.cli.trader_blofin import BlofinREST
        self._rest: BlofinREST = BlofinREST(
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            demo=False,
        )

    # -- Account -------------------------------------------------------

    def get_balance(self) -> BalanceInfo:
        """Extract USDT availableEquity + totalEquity from BloFin balance.

        Field-fallback chain copied from
        trader_blofin.py:get_account_balance_usdt (lines 738-778).
        """
        resp = self._rest.get_balance()
        data = resp.get("data") or {}

        available = 0.0
        total     = 0.0

        # Structure 1: {data: {totalEquity, details:[{availableEquity, currency}]}}
        if isinstance(data, dict):
            total_raw = data.get("totalEquity") or data.get("totalEq") or 0
            try:
                total = float(total_raw) if total_raw else 0.0
            except (TypeError, ValueError):
                total = 0.0

            for item in (data.get("details") or []):
                ccy = (item.get("currency") or item.get("ccy") or "").upper()
                if ccy == "USDT":
                    val = (item.get("availableEquity")
                           or item.get("available")
                           or item.get("availBal") or 0)
                    try:
                        available = float(val)
                    except (TypeError, ValueError):
                        available = 0.0
                    break

        # Structure 2: {data: [{currency, available}]}  (SDK wrapper format)
        elif isinstance(data, list):
            for item in data:
                ccy = (item.get("currency") or item.get("ccy") or "").upper()
                if ccy == "USDT":
                    val = (item.get("availableEquity")
                           or item.get("available")
                           or item.get("availBal") or 0)
                    try:
                        available = float(val)
                    except (TypeError, ValueError):
                        available = 0.0
                    break

        # Preserve legacy fallback: if available not found but total is, use total.
        if available <= 0 and total > 0:
            log.warning(
                f"availableEquity not found -- using totalEquity ${total:,.2f}"
            )
            available = total

        return BalanceInfo(
            available_usdt=available,
            total_usdt=total if total > 0 else available,
        )

    # -- Market metadata -----------------------------------------------

    def supports_symbol(self, inst_id: str) -> bool:
        """BloFin lists a wide perp universe; return True and let the trader's
        per-symbol error handling surface any inactive symbols. Matches
        existing behavior (no pre-filter today)."""
        return True

    def get_instrument_info(self, inst_id: str) -> InstrumentInfo:
        """Copied from trader_blofin.py:get_instrument_info (lines 848-869)."""
        try:
            resp = self._rest.get_instruments(inst_id=inst_id, inst_type="SWAP")
            data = resp.get("data") or []
            if data:
                row = data[0]
                return InstrumentInfo(
                    inst_id=inst_id,
                    contract_value=_safe_float(row.get("contractValue"), 1.0),
                    min_size=_safe_float(row.get("minSize"), 1.0),
                    lot_size=_safe_float(row.get("lotSize"), 1.0),
                    max_market_size=_safe_float(row.get("maxMarketSize"), None),
                    state=row.get("state", "live"),
                )
        except Exception as e:
            log.warning(f"Instrument info failed for {inst_id}: {e}")
        return InstrumentInfo(
            inst_id=inst_id,
            contract_value=1.0,
            min_size=1.0,
            lot_size=1.0,
            max_market_size=None,
            state="live",
        )

    def get_price(self, inst_id: str) -> float | None:
        """Single-symbol price via BloFin tickers.

        Field-fallback chain copied from trader_blofin.py:_get_prices_blofin
        (lines 828-846). Caller iterates for multi-symbol.
        """
        try:
            resp = self._rest.get_tickers(inst_id=inst_id)
            data = resp.get("data") or []
            if data:
                price = float(data[0].get("last") or data[0].get("markPrice") or 0)
                return price if price > 0 else None
        except Exception as e:
            log.warning(f"  {inst_id}: BloFin price fetch error: {e}")
        return None

    # -- Position state ------------------------------------------------

    def get_positions(
        self, inst_ids: list[str] | None = None,
    ) -> list[PositionInfo]:
        """Copied from trader_blofin.py:get_actual_positions (lines 934-956)
        and reconcile_entry_prices (lines 1478-1527). Unified: always returns
        both contracts and average_price when available.
        """
        results: list[PositionInfo] = []
        inst_id_set = set(inst_ids) if inst_ids else None
        try:
            resp = self._rest.get_positions()
        except Exception as e:
            log.warning(f"Could not fetch BloFin positions: {e}")
            return results

        for pos in (resp.get("data") or []):
            iid = pos.get("instId", "")
            if not iid:
                continue
            if inst_id_set is not None and iid not in inst_id_set:
                continue

            # Size: field-fallback from get_actual_positions
            size_raw = pos.get("positions", 0) or pos.get("pos", 0) or 0
            try:
                contracts = float(size_raw)
            except (TypeError, ValueError):
                contracts = 0.0
            if contracts <= 0:
                continue

            # Average fill price: field-fallback from reconcile_entry_prices
            avg_raw = (pos.get("averagePrice")
                       or pos.get("avgPx")
                       or pos.get("averagePx"))
            try:
                avg = float(avg_raw) if avg_raw not in (None, "", "0") else None
            except (TypeError, ValueError):
                avg = None

            results.append(PositionInfo(
                inst_id=iid, contracts=contracts, average_price=avg,
            ))

        return results

    # -- Trading -------------------------------------------------------

    def set_leverage(self, inst_id: str, leverage: int) -> OrderResult:
        """POST /account/set-leverage. Matches trader_blofin.py:1218."""
        try:
            resp = self._rest.set_leverage(
                inst_id=inst_id, leverage=int(leverage), margin_mode=_MARGIN_MODE,
            )
            code = str(resp.get("code", ""))
            msg  = resp.get("msg", "") or ""
            return OrderResult(
                success=(code in ("0", "")),
                order_id="",
                error_code=code,
                error_msg=msg,
            )
        except Exception as e:
            return OrderResult(
                success=False, order_id="",
                error_code="exception", error_msg=str(e),
            )

    def place_entry_order(
        self, inst_id: str, size: float,
        sl_trigger_price: float | None = None,
    ) -> OrderResult:
        """MARKET BUY with optional inline sl_trigger_price. Matches the
        call at trader_blofin.py:_place_order_chunked (line 1434)."""
        sl_str = f"{sl_trigger_price:.8g}" if sl_trigger_price is not None else None
        return self._submit_order(
            inst_id=inst_id, side="buy", size=size,
            reduce_only=False, sl_trigger_price=sl_str,
        )

    def place_reduce_order(self, inst_id: str, size: float) -> OrderResult:
        """MARKET SELL reduce_only=true. Matches fallback at
        trader_blofin.py:1664."""
        return self._submit_order(
            inst_id=inst_id, side="sell", size=size,
            reduce_only=True, sl_trigger_price=None,
        )

    def close_position(self, inst_id: str) -> OrderResult:
        """Native /trade/close-position. Matches trader_blofin.py:1650."""
        try:
            resp = self._rest.close_position(
                inst_id=inst_id,
                margin_mode=_MARGIN_MODE,
                position_side=_POSITION_SIDE,
            )
            code = str(resp.get("code", ""))
            data = resp.get("data") or [{}]
            order_id = (data[0].get("orderId", "")
                        if isinstance(data, list) and data else "")
            return OrderResult(
                success=(code == "0"),
                order_id=str(order_id),
                error_code=code,
                error_msg=resp.get("msg", "") or "",
            )
        except Exception as e:
            return OrderResult(
                success=False, order_id="",
                error_code="exception", error_msg=str(e),
            )

    # -- Reconciliation ------------------------------------------------

    def get_recent_fills(
        self,
        since_ms: int | None = None,
        inst_ids: list[str] | None = None,
    ) -> list[FillInfo]:
        """BloFin: /trade/fills-history with /trade/fills fallback (already
        built into BlofinREST.get_fills_history). Adapter post-filters by
        since_ms and inst_ids client-side; the underlying endpoint does not
        support begin/instId query params in the existing BlofinREST wrapper.

        Field-fallback chain copied from trader_blofin.py:reconcile_exit_prices
        (lines 1551-1575).
        """
        results: list[FillInfo] = []
        inst_id_set = set(inst_ids) if inst_ids else None
        try:
            resp = self._rest.get_fills_history(inst_type="SWAP")
        except Exception as e:
            log.warning(f"BloFin fills history fetch failed: {e}")
            return results

        for row in (resp.get("data") or []):
            iid = row.get("instId", "")
            if not iid:
                continue
            if inst_id_set is not None and iid not in inst_id_set:
                continue

            side = (row.get("side") or "").lower()
            if side not in ("buy", "sell"):
                continue

            price_raw = (row.get("fillPrice")
                         or row.get("price")
                         or row.get("fillPx"))
            try:
                price = float(price_raw) if price_raw not in (None, "") else None
            except (TypeError, ValueError):
                price = None
            if price is None:
                continue

            size_raw = (row.get("fillSize")
                        or row.get("size")
                        or row.get("fillSz")
                        or row.get("sz") or 0)
            try:
                size = float(size_raw)
            except (TypeError, ValueError):
                size = 0.0

            ts_raw = row.get("ts") or row.get("fillTime") or row.get("cTime") or 0
            try:
                ts_ms = int(ts_raw)
            except (TypeError, ValueError):
                ts_ms = 0

            if since_ms is not None and ts_ms < since_ms:
                continue

            order_id = str(row.get("orderId") or row.get("ordId") or "")

            results.append(FillInfo(
                inst_id=iid, order_id=order_id, side=side,
                price=price, size=size, ts_ms=ts_ms,
            ))

        return results

    # -- Capital flow --------------------------------------------------

    def get_capital_events(
        self,
        since_ms: int | None = None,
    ) -> list[CapitalEventInfo]:
        """Pull deposit + withdrawal history from BloFin asset endpoints.

        ALL currencies are tracked, not just stablecoins:
          - Stablecoins (USDT/USDC/DAI/etc.) are valued 1:1 USD
          - Volatile assets (SOL/BTC/ETH/etc.) are valued at the 1-minute
            close price of {asset}-USDT at the deposit timestamp, fetched
            via BloFin's /market/candles. The operator's intent at deposit
            time is the historical USD value, regardless of any later
            intra-exchange swapping.

        Failed price fetches surface the row with amount_usd=0 + a note
        flagging it for manual review — the row is still visible so the
        operator knows it exists and can correct it.

        BloFin pagination: API caps result count per page at 100. Default
        lookback when since_ms is None: 90 days (BloFin's documented
        history retention).
        """
        if since_ms is None:
            # 90 days back, in epoch-ms
            import time as _t
            since_ms = int(_t.time() * 1000) - (90 * 86_400_000)

        # NOTE on BloFin pagination: the after/before params on
        # /asset/{deposit,withdrawal}-history are CURSOR-based (record IDs
        # in newest-first order), not date filters. Passing after=ts_ms
        # silently returns empty/wrong-window results. We omit them and
        # rely on limit=100 to fetch the most-recent N rows; client-side
        # filter on ts >= since_ms keeps the dataset bounded. For >100
        # rows of history we'd need to walk via id-cursor, which BloFin
        # documents but isn't needed at current volumes.
        results: list[CapitalEventInfo] = []
        for kind, fetch in (("deposit",    self._rest.get_deposit_history),
                             ("withdrawal", self._rest.get_withdrawal_history)):
            try:
                resp = fetch(limit=100)
            except Exception as e:
                log.warning(f"BloFin {kind} history fetch failed: {e}")
                continue

            rows = resp.get("data") or []
            for row in rows:
                # Client-side date filter to honor the since_ms contract.
                try:
                    row_ts = int(
                        row.get("ts") or row.get("createTime")
                        or row.get("cTime") or row.get("updatedTime") or 0
                    )
                except (TypeError, ValueError):
                    row_ts = 0
                if row_ts and row_ts < since_ms:
                    continue
                results.extend(_parse_blofin_capital_row(row, kind, self._rest))

        return results

    # -- Internals -----------------------------------------------------

    def _submit_order(
        self, inst_id: str, side: str, size: float,
        reduce_only: bool, sl_trigger_price: str | None,
    ) -> OrderResult:
        """Single place_order call with normalized error extraction.

        Preserves the 102015 (exceeds maxMarketSize) nested-code semantic
        that trader_blofin.py:_place_order_chunked (line 1450) reads from
        data[0].code -- callers of the adapter will see error_code="102015"
        for that case regardless of whether BloFin surfaces it at the
        top-level or nested in data[0].
        """
        try:
            resp = self._rest.place_order(
                inst_id=inst_id,
                margin_mode=_MARGIN_MODE,
                position_side=_POSITION_SIDE,
                side=side,
                order_type="market",
                size=str(size),
                reduce_only=reduce_only,
                sl_trigger_price=sl_trigger_price,
            )
        except Exception as e:
            return OrderResult(
                success=False, order_id="",
                error_code="exception", error_msg=str(e),
            )

        top_code = str(resp.get("code", ""))
        data     = resp.get("data") or [{}]
        inner    = data[0] if (isinstance(data, list) and data) else {}
        inner_code = str(inner.get("code", ""))
        inner_msg  = inner.get("msg", "") or ""

        # Surface 102015 regardless of where BloFin placed it.
        if top_code == "102015" or inner_code == "102015":
            return OrderResult(
                success=False, order_id="",
                error_code="102015",
                error_msg=inner_msg or resp.get("msg", "") or "",
            )

        if top_code not in ("0", ""):
            return OrderResult(
                success=False, order_id="",
                error_code=top_code,
                error_msg=(resp.get("msg", "") or "")
                          + (f" | {inner_msg}" if inner_msg else ""),
            )

        order_id = str(inner.get("orderId", "unknown")) if inner else "unknown"
        return OrderResult(
            success=True, order_id=order_id,
            error_code="0", error_msg="",
        )


# -- Helpers -------------------------------------------------------------

# Stablecoins that can be valued 1:1 USD without an external price source.
# Mirrors the STABLECOINS set in daily_signal_v2.py / overlap_analysis.py
# (canonical normalize_symbol semantics).
_STABLECOINS_USD_PARITY = {
    "USDT", "USDC", "BUSD", "TUSD", "USDP", "FDUSD",
    "USDS", "USDE", "FRAX", "DAI", "PYUSD", "USD1",
}


def _safe_float(val, default):
    """Copied from trader_blofin.py -- defensive float parse with default."""
    try:
        if val in (None, ""):
            return default
        return float(val)
    except (TypeError, ValueError):
        return default


def _fetch_blofin_historical_usd_price(
    rest, asset: str, ts_ms: int,
) -> tuple[float | None, str | None]:
    """Return (price_usd, source_note) for `asset` at `ts_ms` (epoch-ms),
    using BloFin's /market/candles 1-min close of {asset}-USDT.

    Returns (None, reason) on any failure (no instrument, no candle in
    window, parse error, network) so the caller can log + continue.
    Stablecoins short-circuit at 1:1 parity with no API call.
    """
    asset = asset.upper().strip()
    if asset in _STABLECOINS_USD_PARITY:
        return 1.0, None
    inst = f"{asset}-USDT"
    # BloFin /market/candles semantics (verified empirically 2026-04-23):
    #   - `after=X` → returns bars starting at the bar boundary at-or-just-
    #     before X, walking newer. First row in response is closest to X.
    #   - `before=Y` → walks OLDER from Y. Combining `after`+`before` does
    #     not narrow the window — it returns wrong/empty results.
    #   - We pass `after=ts_ms - 60_000` (one full bar back so the bar
    #     CONTAINING ts_ms is returned). The first row's close is our price.
    # Fall back to 1H, then 1D bars if 1m comes back empty (BloFin retains
    # only ~7 days of 1m candles; older deposits need coarser granularity).
    for bar in ("1m", "1H", "1D"):
        try:
            resp = rest.request("GET", "/api/v1/market/candles", params={
                "instId": inst,
                "bar":    bar,
                "after":  str(ts_ms - 60_000),
                "limit":  "5",
            })
        except Exception as e:
            return None, f"price fetch error ({bar}): {e}"
        code = str(resp.get("code", ""))
        if code not in ("0", ""):
            continue  # try next bar size
        rows = resp.get("data") or []
        if rows:
            break
    else:
        return None, f"no candle near {ts_ms} for {inst}"

    # BloFin candle format: [ts, open, high, low, close, vol, ...].
    # First row is closest to our target ts.
    best_row = None
    best_dist = None
    for r in rows:
        try:
            r_ts = int(r[0])
            dist = abs(r_ts - ts_ms)
            if best_dist is None or dist < best_dist:
                best_dist = dist
                best_row = r
        except (TypeError, ValueError, IndexError):
            continue
    if best_row is None:
        return None, f"unparseable candles for {inst}"
    try:
        price = float(best_row[4])  # close
    except (TypeError, ValueError, IndexError):
        return None, f"unparseable close for {inst}"

    import datetime as _dt
    when = _dt.datetime.fromtimestamp(ts_ms / 1000, tz=_dt.timezone.utc)
    return price, f"valued at ${price:.4f}/{asset} @ {when.strftime('%Y-%m-%d %H:%M UTC')}"


def _parse_blofin_capital_row(
    row: dict, kind: str, rest,
) -> list[CapitalEventInfo]:
    """Convert one BloFin deposit/withdrawal API row into 0-or-1
    CapitalEventInfo. Tracks ALL currencies — non-stablecoins are valued
    at the historical 1-min USDT close price at the deposit timestamp via
    the rest client (passed in to keep the parser pure-ish).

    Skips:
      - Rows without amount, timestamp, or exchange_event_id
      - (Failed status filtering deliberately omitted to stay tolerant of
        BloFin code churn; spurious rows can be soft-deleted in the UI.)
    Failed historical-price fetches still produce a row, with amount_usd=0
    and a "price fetch failed" note so the operator can correct manually.
    """
    asset = (row.get("currency") or row.get("ccy") or "").upper().strip()
    if not asset:
        return []

    amount_raw = row.get("amount") or row.get("amt") or row.get("size") or 0
    amount = _safe_float(amount_raw, 0.0)
    if amount <= 0:
        return []

    # BloFin returns ts as epoch-ms string. Field-fallback for both naming
    # conventions seen across `/asset/deposit-history` and `/withdrawal-history`.
    ts_raw = (row.get("ts")
              or row.get("createTime")
              or row.get("cTime")
              or row.get("updatedTime")
              or 0)
    try:
        ts_ms = int(ts_raw)
    except (TypeError, ValueError):
        return []
    if ts_ms <= 0:
        return []

    # Exchange's stable ID for dedup. Different field names per endpoint.
    event_id = str(
        row.get("transferId")
        or row.get("withdrawId")
        or row.get("depositId")
        or row.get("txId")
        or ""
    )
    if not event_id:
        return []

    # Resolve USD value via stablecoin parity OR historical kline.
    price_per_unit, price_note = _fetch_blofin_historical_usd_price(
        rest, asset, ts_ms,
    )
    if price_per_unit is not None:
        amount_usd = amount * price_per_unit
    else:
        amount_usd = 0.0  # surface the row but flag for manual review

    # Free-form provenance — BloFin returns chain + tx hash on most rows,
    # plus the price source for non-stablecoin rows.
    notes_parts = []
    if asset != "USDT":
        notes_parts.append(f"{amount} {asset}")
    if row.get("chain"):
        notes_parts.append(f"chain={row['chain']}")
    if row.get("txId") and row.get("txId") != event_id:
        notes_parts.append(f"tx={row['txId']}")
    if price_note:
        notes_parts.append(price_note)
    elif price_per_unit is None:
        notes_parts.append("price fetch failed — set amount manually")
    notes = "; ".join(notes_parts) if notes_parts else None

    return [CapitalEventInfo(
        event_id=event_id,
        kind=kind,
        amount_usd=amount_usd,
        ts_ms=ts_ms,
        asset=asset,
        notes=notes,
    )]
