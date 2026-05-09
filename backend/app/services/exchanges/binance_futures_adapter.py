"""
backend/app/services/exchanges/binance_futures_adapter.py
==========================================================
Binance USDⓈ-M Futures concrete adapter for the ExchangeAdapter ABC.

Trade surface: long-only entries via MARKET BUY on the perpetual; exits via
MARKET SELL reduceOnly. Native stop-loss supported via STOP_MARKET reduceOnly
+ closePosition=true at MARK_PRICE (matches BloFin's atomic-SL semantics).
Default isolated margin per-symbol; per-symbol leverage set on entry.

Inst IDs are BloFin form ("BTC-USDT") at every adapter boundary; translation
to Binance form ("BTCUSDT") is internal. USDT-quoted only.

Account assumptions (enforced at __init__):
  • One-way position mode. Hedge mode (dualSidePosition=true) is refused —
    the trader assumes net positions everywhere; supporting hedge would
    require per-row positionSide propagation through the trade surface.

Exchange-info caching: /fapi/v1/exchangeInfo is several hundred KB and rarely
changes. Cached per-adapter-instance with a 30-min TTL, mirroring the margin
adapter's pair-cache strategy.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from app.services.exchanges.adapter import (
    BalanceInfo,
    CapitalEventInfo,
    ExchangeAdapter,
    FillInfo,
    InstrumentInfo,
    OrderResult,
    PositionInfo,
)
from app.services.exchanges.binance import (
    BinanceAuthError,
    BinanceClient,
    BinanceError,
    BinanceNetworkError,
    BinancePermissionError,
)

log = logging.getLogger("binance_futures_adapter")


_EXCHANGE_INFO_TTL_S = 1800       # 30 min — exchangeInfo rarely changes
_DEFAULT_FILLS_LOOKBACK_MS = 24 * 60 * 60 * 1000     # 24h
_DEFAULT_INCOME_LOOKBACK_MS = 365 * 24 * 60 * 60 * 1000  # 365 days

# Futures uses ISOLATED per-symbol by default to match BloFin's posture
# (one allocation = one symbol's risk budget, not pooled across positions).
_DEFAULT_MARGIN_TYPE = "ISOLATED"

# Stop-loss orders trigger off mark price to match BloFin's algoOrderType="sl"
# semantics (BloFin uses index price internally; mark price is the closest
# Binance equivalent and avoids triggering on transient last-trade spikes).
_STOP_WORKING_TYPE = "MARK_PRICE"


class BinanceFuturesAdapter(ExchangeAdapter):
    exchange_name       = "binance_futures"
    native_sl_supported = True

    def __init__(self, api_key: str, api_secret: str):
        if not api_key or not api_secret:
            raise BinanceAuthError("api_key and api_secret are required")
        self._client = BinanceClient(
            api_key=api_key, api_secret=api_secret, testnet=False,
        )
        self._exchange_info_cache: dict[str, dict] | None = None
        self._exchange_info_fetched_at: float = 0.0

        # Refuse hedge mode at construction. The trader assumes one-way
        # positions; a hedge-mode account would mis-route orders (every order
        # needs a positionSide field which the ABC doesn't carry). Better to
        # fail loud at startup than silently mis-trade.
        self._enforce_one_way_mode()

    # -- Init guards ---------------------------------------------------

    def _enforce_one_way_mode(self) -> None:
        """Probe /fapi/v1/positionSide/dual; raise if hedge mode is active.

        Account-level setting (not per-key, not per-symbol). Switching
        requires zero open positions, so we cannot toggle on the user's
        behalf — surface a clear error and let them fix it via the Binance
        UI before retrying.
        """
        try:
            resp = self._client.get_futures_position_side_dual()
        except (BinanceAuthError, BinancePermissionError) as e:
            raise BinanceAuthError(
                f"Binance Futures key cannot read position-side mode: {e}. "
                "Verify 'Enable Futures' is checked on the API key."
            ) from e
        except (BinanceNetworkError, BinanceError) as e:
            raise BinanceError(
                f"Could not verify Binance Futures position-side mode: {e}"
            ) from e

        is_hedge = bool(resp.get("dualSidePosition", False))
        if is_hedge:
            raise BinanceError(
                "Binance Futures account is in Hedge mode (dualSidePosition=true). "
                "This trader requires One-way mode. Close all open futures "
                "positions, then switch via Binance UI: "
                "Futures → Preferences → Position Mode → One-way Mode."
            )

    # -- Symbol translation -------------------------------------------

    @staticmethod
    def _to_binance(inst_id: str) -> str:
        """BloFin form BTC-USDT → Binance form BTCUSDT. USDT-only."""
        if not inst_id.endswith("-USDT"):
            raise ValueError(
                f"BinanceFuturesAdapter requires USDT-quoted symbols; got "
                f"{inst_id!r}. Non-USDT quotes are not implemented."
            )
        return inst_id.replace("-", "")

    @staticmethod
    def _to_blofin(symbol: str) -> str:
        """Binance form BTCUSDT → BloFin form BTC-USDT. USDT-only."""
        if not symbol.endswith("USDT"):
            raise ValueError(f"Expected USDT-quoted symbol; got {symbol!r}")
        return f"{symbol[:-len('USDT')]}-USDT"

    # -- Exchange-info cache ------------------------------------------

    def _exchange_info_map(self) -> dict[str, dict]:
        """Return {symbol → row} from /fapi/v1/exchangeInfo. 30-min TTL."""
        now = time.time()
        if (
            self._exchange_info_cache is not None
            and now - self._exchange_info_fetched_at < _EXCHANGE_INFO_TTL_S
        ):
            return self._exchange_info_cache
        try:
            payload = self._client.get_futures_exchange_info()
        except (BinanceNetworkError, BinanceError) as e:
            log.warning(f"Futures exchangeInfo fetch failed: {e}")
            return self._exchange_info_cache or {}
        rows = payload.get("symbols") or []
        self._exchange_info_cache = {r.get("symbol"): r for r in rows if r.get("symbol")}
        self._exchange_info_fetched_at = now
        return self._exchange_info_cache

    # -- Account ------------------------------------------------------

    def get_balance(self) -> BalanceInfo:
        """Extract USDT availableBalance + balance from /fapi/v2/balance.

        Futures wallet has its own USDT row distinct from the spot wallet.
        availableBalance is what's free for opening new positions; balance
        is total wallet equity (margin + free).
        """
        try:
            rows = self._client.get_futures_balance()
        except (BinanceNetworkError, BinanceError) as e:
            log.warning(f"Binance get_futures_balance failed: {e}")
            return BalanceInfo(available_usdt=0.0, total_usdt=0.0)

        available = 0.0
        total = 0.0
        for r in rows or []:
            if r.get("asset") == "USDT":
                try:
                    available = float(r.get("availableBalance") or 0)
                except (TypeError, ValueError):
                    available = 0.0
                try:
                    total = float(r.get("balance") or 0)
                except (TypeError, ValueError):
                    total = 0.0
                # Include unrealized PnL in total when available — matches
                # BloFin's totalEquity which is wallet + uPnL.
                try:
                    total += float(r.get("crossUnPnl") or 0)
                except (TypeError, ValueError):
                    pass
                break

        if total <= 0 and available > 0:
            total = available

        return BalanceInfo(available_usdt=available, total_usdt=total)

    # -- Market metadata ----------------------------------------------

    def supports_symbol(self, inst_id: str) -> bool:
        """True iff symbol is a live USDT-quoted PERPETUAL on /fapi/v1/exchangeInfo."""
        try:
            symbol = self._to_binance(inst_id)
        except ValueError:
            return False
        row = self._exchange_info_map().get(symbol)
        if not row:
            return False
        return (
            row.get("status") == "TRADING"
            and row.get("contractType") == "PERPETUAL"
            and row.get("quoteAsset") == "USDT"
        )

    def get_instrument_info(self, inst_id: str) -> InstrumentInfo:
        """Translate /fapi/v1/exchangeInfo filters into InstrumentInfo.

        For USDM perps, contractSize is implicit (1 base unit per contract for
        most pairs; explicit `contractSize` field for pairs like 1000SHIBUSDT
        where 1 contract = 1000 SHIB). We surface contractSize=1.0 to match
        the trader's BloFin assumption (size in base units), and rely on
        Binance's symbol form (e.g. 1000SHIBUSDT) to encode the multiplier.
        """
        try:
            symbol = self._to_binance(inst_id)
        except ValueError as e:
            log.warning(f"{inst_id}: {e}")
            return _inactive_instrument(inst_id)

        row = self._exchange_info_map().get(symbol)
        if not row:
            return _inactive_instrument(inst_id)

        min_size = 0.0
        lot_size = 1.0
        max_market_size: float | None = None
        for f in row.get("filters", []) or []:
            ft = f.get("filterType")
            if ft == "LOT_SIZE":
                try:
                    min_size = float(f.get("minQty", 0) or 0)
                    lot_size = float(f.get("stepSize", 1) or 1)
                except (TypeError, ValueError):
                    pass
            elif ft == "MARKET_LOT_SIZE":
                # USDM also has MARKET_LOT_SIZE which can be tighter than LOT_SIZE.
                # Use its maxQty as the chunking ceiling — analogous to BloFin's
                # maxMarketSize, drives the trader's chunked-order path.
                try:
                    mq = float(f.get("maxQty", 0) or 0)
                    if mq > 0:
                        max_market_size = mq
                except (TypeError, ValueError):
                    pass

        state_raw = row.get("status", "TRADING")
        state = "live" if state_raw == "TRADING" else state_raw.lower()

        return InstrumentInfo(
            inst_id=inst_id,
            contract_value=1.0,
            min_size=min_size,
            lot_size=lot_size,
            max_market_size=max_market_size,
            state=state,
        )

    def get_price(self, inst_id: str) -> float | None:
        """Last trade price via /fapi/v1/ticker/price."""
        try:
            symbol = self._to_binance(inst_id)
        except ValueError:
            return None
        try:
            resp = self._client.get_futures_ticker_price(symbol)
        except (BinanceNetworkError, BinanceError) as e:
            log.warning(f"{inst_id}: futures ticker fetch failed: {e}")
            return None
        try:
            price = float(resp.get("price") or 0)
        except (TypeError, ValueError):
            return None
        return price if price > 0 else None

    # -- Position state -----------------------------------------------

    def get_positions(
        self, inst_ids: list[str] | None = None,
    ) -> list[PositionInfo]:
        """Read /fapi/v2/positionRisk, return long positions only.

        Long-only adapter: positionAmt > 0. In one-way mode (enforced at
        __init__) each symbol has at most one row; in hedge mode there'd be
        two rows per symbol (LONG + SHORT) keyed by positionSide — the
        constructor refused that already.
        """
        inst_id_set = set(inst_ids) if inst_ids else None
        try:
            rows = self._client.get_futures_position_risk()
        except (BinanceNetworkError, BinanceError) as e:
            log.warning(f"Binance get_futures_position_risk failed: {e}")
            return []
        if rows is None:
            return []

        results: list[PositionInfo] = []
        for r in rows:
            symbol = r.get("symbol", "")
            if not symbol or not symbol.endswith("USDT"):
                continue
            try:
                amt = float(r.get("positionAmt", 0) or 0)
            except (TypeError, ValueError):
                continue
            if amt <= 0:
                continue   # closed or short — skip

            inst_id = self._to_blofin(symbol)
            if inst_id_set is not None and inst_id not in inst_id_set:
                continue

            try:
                avg = float(r.get("entryPrice") or 0)
                avg = avg if avg > 0 else None
            except (TypeError, ValueError):
                avg = None

            margin_mode = (r.get("marginType") or "").lower() or None
            # One-way mode → positionSide = "BOTH" per Binance docs; normalize
            # to "net" for the ABC (matches BloFin's _POSITION_SIDE constant).
            ps = (r.get("positionSide") or "").lower()
            position_side = "net" if ps in ("", "both") else ps

            results.append(PositionInfo(
                inst_id=inst_id, contracts=amt, average_price=avg,
                margin_mode=margin_mode, position_side=position_side,
            ))
        return results

    # -- Trading ------------------------------------------------------

    def set_leverage(self, inst_id: str, leverage: int) -> OrderResult:
        """Set per-symbol leverage. Forces ISOLATED margin mode first.

        Binance separates leverage (POST /fapi/v1/leverage) from margin type
        (POST /fapi/v1/marginType). Margin-type set is idempotent — error
        -4046 ("No need to change margin type") means it was already set;
        we swallow it. Leverage set is also idempotent at the same value.
        """
        try:
            symbol = self._to_binance(inst_id)
        except ValueError as e:
            return OrderResult(success=False, order_id="", error_code="exception", error_msg=str(e))

        # Step 1: ensure ISOLATED margin. Idempotent — swallow -4046.
        try:
            self._client.set_futures_margin_type(symbol, _DEFAULT_MARGIN_TYPE)
        except BinanceError as e:
            msg = str(e)
            if "-4046" not in msg and "No need to change" not in msg:
                return OrderResult(
                    success=False, order_id="",
                    error_code="MARGIN_TYPE_FAILED", error_msg=msg,
                )

        # Step 2: set leverage.
        try:
            self._client.set_futures_leverage(symbol, int(leverage))
        except BinanceError as e:
            return OrderResult(
                success=False, order_id="",
                error_code="LEVERAGE_FAILED", error_msg=str(e),
            )
        except Exception as e:
            return OrderResult(
                success=False, order_id="",
                error_code="exception", error_msg=str(e),
            )

        return OrderResult(success=True, order_id="", error_code="0", error_msg="")

    def place_entry_order(
        self, inst_id: str, size: float,
        sl_trigger_price: float | None = None,
    ) -> OrderResult:
        """MARKET BUY entry, optionally followed by a STOP_MARKET reduceOnly
        protective stop at sl_trigger_price.

        Binance does not support attaching the stop atomically with the entry
        (no inline stopPrice on a non-stop order). We submit the entry first;
        on success, we submit the stop. If the stop submission fails, the
        entry is left in place — the trader's port_sl loop is still a backup,
        and the alternative (auto-closing the entry) is worse since it
        creates phantom round-trips on transient API blips. Stop failure is
        logged and surfaced via OrderResult.error_msg as a soft warning.
        """
        try:
            symbol = self._to_binance(inst_id)
        except ValueError as e:
            return OrderResult(success=False, order_id="", error_code="exception", error_msg=str(e))

        # Step 1: market entry.
        entry_result = self._submit_order(
            symbol=symbol, side="BUY", order_type="MARKET",
            quantity=size, reduce_only=False,
        )
        if not entry_result.success:
            return entry_result
        if sl_trigger_price is None:
            return entry_result

        # Step 2: protective stop. STOP_MARKET reduceOnly closePosition=true,
        # triggered off MARK_PRICE to avoid liquidation-cascade false-trips.
        try:
            self._client.place_futures_order(
                symbol=symbol, side="SELL", order_type="STOP_MARKET",
                close_position=True,
                stop_price=sl_trigger_price,
                working_type=_STOP_WORKING_TYPE,
            )
        except (BinanceNetworkError, BinanceError) as e:
            log.warning(
                f"{inst_id}: SL stop placement failed (entry succeeded): {e}. "
                "Falling back to client-side port_sl monitoring."
            )
            # Entry stays; flag in error_msg but keep success=True.
            return OrderResult(
                success=True, order_id=entry_result.order_id,
                error_code="0",
                error_msg=f"entry ok; sl placement failed: {e}",
            )

        return entry_result

    def place_reduce_order(self, inst_id: str, size: float) -> OrderResult:
        """MARKET SELL reduceOnly to trim or close a long position by `size`."""
        try:
            symbol = self._to_binance(inst_id)
        except ValueError as e:
            return OrderResult(success=False, order_id="", error_code="exception", error_msg=str(e))
        return self._submit_order(
            symbol=symbol, side="SELL", order_type="MARKET",
            quantity=size, reduce_only=True,
        )

    def close_position(
        self, inst_id: str,
        margin_mode:   str | None = None,
        position_side: str | None = None,
    ) -> OrderResult:
        """Fully close the position. Cancels open orders first to drop any
        lingering protective stops, then issues MARKET SELL closePosition=true.

        margin_mode / position_side are ignored — Binance Futures one-way mode
        doesn't have per-bucket position rows like BloFin. The kwargs exist
        on the ABC for BloFin's manual-fill reconcile path; not relevant here.
        """
        try:
            symbol = self._to_binance(inst_id)
        except ValueError as e:
            return OrderResult(success=False, order_id="", error_code="exception", error_msg=str(e))

        # Cancel any dangling stops/orders before closing. Failure here is
        # non-fatal — closePosition=true on the close call clears the
        # position regardless of pending orders.
        try:
            self._client.cancel_all_futures_orders(symbol)
        except (BinanceNetworkError, BinanceError) as e:
            log.warning(f"{inst_id}: cancel_all_orders failed (non-fatal): {e}")

        return self._submit_order(
            symbol=symbol, side="SELL", order_type="MARKET",
            quantity=None, reduce_only=False, close_position=True,
        )

    # -- Reconciliation -----------------------------------------------

    def get_recent_fills(
        self,
        since_ms: int | None = None,
        inst_ids: list[str] | None = None,
    ) -> list[FillInfo]:
        """Per-symbol /fapi/v1/userTrades query, unioned across inst_ids.

        Binance has no all-symbols userTrades endpoint. When inst_ids is None
        we fall back to current-open-positions to bound fan-out — same shape
        as BinanceMarginAdapter. since_ms defaults to last 24h.
        """
        if since_ms is None:
            since_ms = int(time.time() * 1000) - _DEFAULT_FILLS_LOOKBACK_MS

        if inst_ids is None:
            # Bound fan-out to active symbols. An empty open-position set
            # results in an empty fills list — same as the margin adapter.
            inst_ids = [p.inst_id for p in self.get_positions()]

        results: list[FillInfo] = []
        for inst_id in inst_ids:
            try:
                symbol = self._to_binance(inst_id)
            except ValueError:
                continue
            try:
                rows = self._client.get_futures_user_trades(
                    symbol, since_ms=since_ms,
                )
            except (BinanceNetworkError, BinanceError) as e:
                log.warning(f"{inst_id}: userTrades fetch failed: {e}")
                continue

            for r in rows or []:
                side = (r.get("side") or "").lower()
                if side not in ("buy", "sell"):
                    continue
                try:
                    price = float(r.get("price") or 0)
                except (TypeError, ValueError):
                    continue
                if price <= 0:
                    continue
                try:
                    qty = float(r.get("qty") or 0)
                except (TypeError, ValueError):
                    qty = 0.0
                try:
                    ts_ms = int(r.get("time") or 0)
                except (TypeError, ValueError):
                    ts_ms = 0
                order_id = str(r.get("orderId") or "")
                results.append(FillInfo(
                    inst_id=inst_id, order_id=order_id, side=side,
                    price=price, size=qty, ts_ms=ts_ms,
                ))
        return results

    # -- Capital flow -------------------------------------------------

    def get_capital_events(
        self,
        since_ms: int | None = None,
    ) -> list[CapitalEventInfo]:
        """Pull TRANSFER rows from /fapi/v1/income.

        Binance income endpoint reports wallet flows in USDT-equivalent
        (positive = into futures wallet, negative = out). For the auto-poller
        we map: positive → deposit, negative → withdrawal. Lookback default
        365 days, capped by Binance's own retention.
        """
        if since_ms is None:
            since_ms = int(time.time() * 1000) - _DEFAULT_INCOME_LOOKBACK_MS

        try:
            rows = self._client.get_futures_income(
                since_ms=since_ms, income_type="TRANSFER",
            )
        except (BinanceNetworkError, BinanceError) as e:
            log.warning(f"Binance futures income fetch failed: {e}")
            return []

        results: list[CapitalEventInfo] = []
        for r in rows or []:
            try:
                amount = float(r.get("income") or 0)
            except (TypeError, ValueError):
                continue
            if amount == 0:
                continue
            try:
                ts_ms = int(r.get("time") or 0)
            except (TypeError, ValueError):
                continue
            if ts_ms <= 0:
                continue

            # Binance gives us a tranId (transaction id) — use that as the
            # dedup key. Falls back to the income row's hash for the rare
            # case where tranId is absent.
            event_id = str(r.get("tranId") or "")
            if not event_id:
                continue

            kind = "deposit" if amount > 0 else "withdrawal"
            results.append(CapitalEventInfo(
                event_id=event_id,
                kind=kind,
                amount_usd=abs(amount),
                ts_ms=ts_ms,
                asset=str(r.get("asset") or "USDT").upper(),
                notes=r.get("info") or None,
            ))
        return results

    # -- Internals ----------------------------------------------------

    def _submit_order(
        self, *, symbol: str, side: str, order_type: str,
        quantity: float | None, reduce_only: bool = False,
        close_position: bool = False,
    ) -> OrderResult:
        """Single place_futures_order call with normalized error extraction.

        Binance order errors that warrant special-casing:
          -2019  Margin is insufficient
          -2010  New order rejected (filter / risk)
          -1111  Precision is over the maximum defined for this asset
          -4131  PERCENT_PRICE filter (price too far from mark)
        These currently surface verbatim via error_code; callers that want
        to retry or chunk gate on the textual code.
        """
        try:
            resp = self._client.place_futures_order(
                symbol=symbol, side=side, order_type=order_type,
                quantity=quantity,
                reduce_only=reduce_only, close_position=close_position,
            )
        except BinanceError as e:
            return OrderResult(
                success=False, order_id="",
                error_code=_extract_binance_error_code(str(e)),
                error_msg=str(e),
            )
        except Exception as e:
            return OrderResult(
                success=False, order_id="",
                error_code="exception", error_msg=str(e),
            )

        order_id = str(resp.get("orderId") or "")
        return OrderResult(
            success=True, order_id=order_id, error_code="0", error_msg="",
        )


# -- Helpers -------------------------------------------------------------

def _inactive_instrument(inst_id: str) -> InstrumentInfo:
    return InstrumentInfo(
        inst_id=inst_id, contract_value=1.0,
        min_size=0.0, lot_size=1.0,
        max_market_size=None, state="unknown",
    )


def _extract_binance_error_code(msg: str) -> str:
    """Pull the leading numeric error code from a Binance error message.

    BinanceError's str() looks like "Binance: <msg> (code=-2019, ...)" — we
    surface the negative code so callers can branch on specific failure
    modes without parsing the whole message.
    """
    import re
    m = re.search(r"code=(-?\d+)", msg)
    return m.group(1) if m else "exception"
