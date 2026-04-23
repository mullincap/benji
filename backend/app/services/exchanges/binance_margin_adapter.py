"""
backend/app/services/exchanges/binance_margin_adapter.py
=========================================================
Binance cross-margin concrete adapter for the ExchangeAdapter ABC.

Trade surface: long-only entries via cross-margin borrow-and-buy; exits via
AUTO_REPAY. No native SL (monitored client-side by the trader's port_sl /
port_tsl loop).

Uses the python-binance sync Client. Inst IDs are BloFin form at every
adapter boundary ("BTC-USDT"); translation to Binance form ("BTCUSDT") is
internal. Only USDT-quoted symbols are supported.

Design refs:
  - Session D ratifications (A=ABC, B3=MARGIN_BUY+USDT-borrow fallback,
    C1=strict pre-filter, D3=per-symbol max-borrow lookup).
  - Gotcha #2(ii): immediate repay on post-borrow order failure.
  - User's binance_margin_trading.py reference script (entry + exit +
    dust-cleanup patterns).
"""

from __future__ import annotations

import logging
import time

from binance.client import Client
from binance.exceptions import BinanceAPIException

from app.services.exchanges.adapter import (
    BalanceInfo,
    CapitalEventInfo,
    ExchangeAdapter,
    FillInfo,
    InstrumentInfo,
    OrderResult,
    PositionInfo,
)

log = logging.getLogger("binance_margin_adapter")


# TTL cache for margin-pair universe; fetched from /margin/allPairs which is
# stable (new listings rare). Shared per-adapter-instance.
_MARGIN_PAIRS_TTL_S = 1800   # 30 min


class BinanceMarginAdapter(ExchangeAdapter):
    exchange_name       = "binance"
    native_sl_supported = False

    def __init__(self, api_key: str, api_secret: str):
        # Defaults: recvWindow=5000 ms tolerates normal container clock skew;
        # python-binance handles -1021 timestamp errors via timestamp_offset.
        self._client = Client(api_key=api_key, api_secret=api_secret)
        self._margin_pairs_cache: set[str] | None = None
        self._margin_pairs_fetched_at: float = 0.0

    # -- Symbol translation ------------------------------------------

    @staticmethod
    def _to_binance(inst_id: str) -> str:
        """BloFin form BTC-USDT -> Binance form BTCUSDT.

        Guards against non-USDT quotes — strategy universe is USDT-only today,
        and adding e.g. ETH-BTC would route to the wrong symbol silently
        without this assertion.
        """
        if not inst_id.endswith("-USDT"):
            raise ValueError(
                f"BinanceMarginAdapter requires USDT-quoted symbols; got "
                f"{inst_id!r}. Non-USDT quotes (BTC, USDC, FDUSD, BUSD) are "
                "not implemented."
            )
        return inst_id.replace("-", "")

    @staticmethod
    def _to_blofin(symbol: str) -> str:
        """Binance form BTCUSDT -> BloFin form BTC-USDT. USDT-only."""
        if not symbol.endswith("USDT"):
            raise ValueError(f"Expected USDT-quoted Binance symbol; got {symbol!r}")
        base = symbol[: -len("USDT")]
        return f"{base}-USDT"

    @staticmethod
    def _base_asset(inst_id: str) -> str:
        """Return the base-asset symbol (e.g., 'BTC' from 'BTC-USDT')."""
        if not inst_id.endswith("-USDT"):
            raise ValueError(f"Expected USDT-quoted inst_id; got {inst_id!r}")
        return inst_id[: -len("-USDT")]

    # -- Account -----------------------------------------------------

    def get_balance(self) -> BalanceInfo:
        """Extract USDT available + BTC-denominated total from cross-margin.

        marginLevel / totalNetAssetOfBtc are BTC-denominated. We convert
        total to USDT via BTCUSDT spot price at session start; callers can
        re-read as needed. available_usdt is the USDT row's `free` amount.
        """
        try:
            acct = self._client.get_margin_account()
        except BinanceAPIException as e:
            log.warning(f"Binance get_margin_account failed: {e}")
            return BalanceInfo(available_usdt=0.0, total_usdt=0.0)

        available_usdt = 0.0
        for item in acct.get("userAssets", []):
            if item.get("asset") == "USDT":
                try:
                    available_usdt = float(item.get("free", 0) or 0)
                except (TypeError, ValueError):
                    available_usdt = 0.0
                break

        # Total in USDT via BTC-denominated net asset × BTCUSDT price.
        total_usdt = 0.0
        try:
            net_btc = float(acct.get("totalNetAssetOfBtc", 0) or 0)
            if net_btc > 0:
                btc_price = self._spot_price("BTCUSDT")
                if btc_price is not None:
                    total_usdt = net_btc * btc_price
        except (TypeError, ValueError):
            total_usdt = 0.0

        if total_usdt <= 0:
            total_usdt = available_usdt

        return BalanceInfo(
            available_usdt=available_usdt,
            total_usdt=total_usdt,
        )

    # -- Market metadata ---------------------------------------------

    def supports_symbol(self, inst_id: str) -> bool:
        """True iff the symbol's Binance form is in the cross-margin pair
        universe. Uses a per-instance TTL cache (30 min)."""
        try:
            symbol = self._to_binance(inst_id)
        except ValueError:
            return False
        return symbol in self._margin_pairs()

    def get_instrument_info(self, inst_id: str) -> InstrumentInfo:
        """Translate Binance spot exchangeInfo LOT_SIZE / NOTIONAL filters
        into the ABC's InstrumentInfo shape.

        contract_value is 1.0 for spot-margin (base units = contracts).
        max_market_size is None — Binance doesn't chunk like BloFin does.
        """
        try:
            symbol = self._to_binance(inst_id)
        except ValueError as e:
            log.warning(f"{inst_id}: {e}")
            return _inactive_instrument(inst_id)

        try:
            info = self._client.get_symbol_info(symbol)
        except BinanceAPIException as e:
            log.warning(f"{inst_id}: get_symbol_info failed: {e}")
            return _inactive_instrument(inst_id)

        if not info:
            return _inactive_instrument(inst_id)

        min_size = 0.0
        lot_size = 1.0
        for f in info.get("filters", []):
            if f.get("filterType") == "LOT_SIZE":
                try:
                    min_size = float(f.get("minQty", 0) or 0)
                    lot_size = float(f.get("stepSize", 1) or 1)
                except (TypeError, ValueError):
                    pass
                break

        state = "live" if info.get("status") == "TRADING" else info.get("status", "unknown").lower()

        return InstrumentInfo(
            inst_id=inst_id,
            contract_value=1.0,
            min_size=min_size,
            lot_size=lot_size,
            max_market_size=None,
            state=state,
        )

    def get_price(self, inst_id: str) -> float | None:
        """Spot last price for monitoring — same series the margin book uses."""
        try:
            symbol = self._to_binance(inst_id)
        except ValueError:
            return None
        return self._spot_price(symbol)

    # -- Position state ----------------------------------------------

    def get_positions(
        self, inst_ids: list[str] | None = None,
    ) -> list[PositionInfo]:
        """Synthesize positions from cross-margin userAssets where free > 0
        on the base asset (long position = net-long base + net-short USDT).

        For average_price, we query /margin/myTrades per base-asset symbol
        and take the volume-weighted average of recent BUY fills. This is
        an approximation — callers needing precise fill prices should use
        get_recent_fills with an explicit inst_ids filter.
        """
        inst_id_set = set(inst_ids) if inst_ids else None

        try:
            acct = self._client.get_margin_account()
        except BinanceAPIException as e:
            log.warning(f"Binance get_margin_account failed: {e}")
            return []

        results: list[PositionInfo] = []
        for item in acct.get("userAssets", []):
            asset = item.get("asset")
            if not asset or asset == "USDT":
                continue
            try:
                net = float(item.get("netAsset", 0) or 0)
            except (TypeError, ValueError):
                continue
            if net <= 0:
                # Zero or short (net-negative via borrow); long-only adapter.
                continue

            inst_id = f"{asset}-USDT"
            if inst_id_set is not None and inst_id not in inst_id_set:
                continue

            avg = self._average_buy_price(self._to_binance(inst_id))
            results.append(PositionInfo(
                inst_id=inst_id, contracts=net, average_price=avg,
            ))

        return results

    # -- Trading -----------------------------------------------------

    def set_leverage(self, inst_id: str, leverage: int) -> OrderResult:
        """No-op verification on cross margin.

        Cross-margin has no per-symbol leverage setter. We verify that the
        requested leverage is achievable given the current USDT borrow
        ceiling for this allocation. Per Decision D3:

          effective_max_leverage = (collateral + max_borrowable_usdt) / collateral
          success iff requested_leverage <= effective_max_leverage

        collateral here is inferred as the current available_usdt. Caller
        is responsible for sizing collateral correctly (per-symbol slice
        of the allocation).
        """
        try:
            max_loan_resp = self._client.get_max_margin_loan(asset="USDT")
            max_borrow = float(max_loan_resp.get("amount", 0) or 0)
        except BinanceAPIException as e:
            return OrderResult(
                success=False, order_id="",
                error_code=str(getattr(e, "code", "")) or "exception",
                error_msg=str(e),
            )
        except (TypeError, ValueError):
            max_borrow = 0.0

        balance = self.get_balance()
        collateral = balance.available_usdt
        if collateral <= 0:
            return OrderResult(
                success=False, order_id="",
                error_code="INSUFFICIENT_COLLATERAL",
                error_msg="available_usdt is zero; cannot verify leverage",
            )

        effective_max_lev = (collateral + max_borrow) / collateral
        if leverage > effective_max_lev + 1e-9:
            return OrderResult(
                success=False, order_id="",
                error_code="INSUFFICIENT_BORROW_LIMIT",
                error_msg=(
                    f"requested leverage {leverage}x exceeds effective max "
                    f"{effective_max_lev:.2f}x "
                    f"(collateral=${collateral:,.2f}, "
                    f"max_borrow_usdt=${max_borrow:,.2f})"
                ),
            )

        return OrderResult(
            success=True, order_id="",
            error_code="0", error_msg="",
        )

    def place_entry_order(
        self, inst_id: str, size: float,
        sl_trigger_price: float | None = None,
    ) -> OrderResult:
        """MARGIN_BUY primary; on -3006 USDT borrow limit, fallback to manual
        USDT borrow + NO_SIDE_EFFECT BUY. On post-borrow order failure,
        immediately repay the borrowed USDT (gotcha #2(ii))."""
        if sl_trigger_price is not None:
            raise NotImplementedError(
                "Binance margin has no native atomic SL at entry. "
                "Check adapter.native_sl_supported before populating; "
                "client-side SL monitoring is in the trader's monitoring loop."
            )

        symbol = self._to_binance(inst_id)

        # Primary: MARGIN_BUY (auto-borrow USDT).
        try:
            order = self._client.create_margin_order(
                symbol=symbol,
                side="BUY",
                type="MARKET",
                quantity=size,
                sideEffectType="MARGIN_BUY",
            )
            return OrderResult(
                success=True,
                order_id=str(order.get("orderId", "")),
                error_code="0", error_msg="",
            )
        except BinanceAPIException as primary_exc:
            code = str(getattr(primary_exc, "code", "") or "")
            if code != "-3006":
                return OrderResult(
                    success=False, order_id="",
                    error_code=code or "exception",
                    error_msg=str(primary_exc),
                )
            log.warning(
                f"{inst_id}: MARGIN_BUY hit -3006 (USDT borrow limit). "
                "Attempting manual USDT borrow fallback."
            )

        # Fallback: manual USDT borrow + NO_SIDE_EFFECT BUY.
        price = self._spot_price(symbol)
        if price is None or price <= 0:
            return OrderResult(
                success=False, order_id="",
                error_code="NO_PRICE",
                error_msg=f"Cannot price {symbol} for manual-borrow fallback",
            )

        balance = self.get_balance()
        notional = size * price
        # Small buffer to absorb slippage + fees; keep loan sized to the gap.
        shortfall = max(0.0, notional - balance.available_usdt) * 1.005
        if shortfall <= 0:
            # Shouldn't happen — MARGIN_BUY wouldn't have needed borrow. Retry
            # as NO_SIDE_EFFECT directly.
            try:
                order = self._client.create_margin_order(
                    symbol=symbol, side="BUY", type="MARKET",
                    quantity=size, sideEffectType="NO_SIDE_EFFECT",
                )
                return OrderResult(
                    success=True,
                    order_id=str(order.get("orderId", "")),
                    error_code="0", error_msg="",
                )
            except BinanceAPIException as e:
                return OrderResult(
                    success=False, order_id="",
                    error_code=str(getattr(e, "code", "")) or "exception",
                    error_msg=str(e),
                )

        borrow_amount_str = f"{shortfall:.2f}"
        try:
            self._client.create_margin_loan(
                asset="USDT", amount=borrow_amount_str,
            )
        except BinanceAPIException as borrow_exc:
            borrow_code = str(getattr(borrow_exc, "code", "") or "")
            return OrderResult(
                success=False, order_id="",
                error_code=borrow_code or "-3006",
                error_msg=(
                    f"manual USDT borrow of {borrow_amount_str} also failed: "
                    f"{borrow_exc}"
                ),
            )

        # Order step. If it fails after a successful borrow, immediately repay
        # the borrowed USDT so we don't accrue interest on an unused loan.
        try:
            order = self._client.create_margin_order(
                symbol=symbol, side="BUY", type="MARKET",
                quantity=size, sideEffectType="NO_SIDE_EFFECT",
            )
            return OrderResult(
                success=True,
                order_id=str(order.get("orderId", "")),
                error_code="0", error_msg="",
            )
        except BinanceAPIException as order_exc:
            # Gotcha #2(ii): repay before returning.
            try:
                self._client.repay_margin_loan(
                    asset="USDT", amount=borrow_amount_str,
                )
                log.warning(
                    f"{inst_id}: order failed after manual borrow; repaid "
                    f"${borrow_amount_str} USDT to zero interest exposure."
                )
            except BinanceAPIException as repay_exc:
                log.error(
                    f"{inst_id}: FAILED TO REPAY ${borrow_amount_str} USDT "
                    f"after order failure: {repay_exc}. Manual intervention "
                    "required — borrowed USDT is accruing interest."
                )
            return OrderResult(
                success=False, order_id="",
                error_code=str(getattr(order_exc, "code", "")) or "exception",
                error_msg=f"NO_SIDE_EFFECT BUY after borrow failed: {order_exc}",
            )

    def place_reduce_order(self, inst_id: str, size: float) -> OrderResult:
        """MARKET SELL with AUTO_REPAY. On NOTIONAL filter rejection,
        retry with quoteOrderQty to bypass the quantity-based filter
        (dust-exit pattern from the reference script)."""
        symbol = self._to_binance(inst_id)
        try:
            order = self._client.create_margin_order(
                symbol=symbol, side="SELL", type="MARKET",
                quantity=size, sideEffectType="AUTO_REPAY",
            )
            return OrderResult(
                success=True,
                order_id=str(order.get("orderId", "")),
                error_code="0", error_msg="",
            )
        except BinanceAPIException as e:
            code = str(getattr(e, "code", "") or "")
            # -1013 / NOTIONAL / filter failure → try quoteOrderQty path.
            if code in ("-1013", "-4164") or "NOTIONAL" in str(e).upper():
                return self._sell_by_quote(symbol, size)
            return OrderResult(
                success=False, order_id="",
                error_code=code or "exception",
                error_msg=str(e),
            )

    def close_position(self, inst_id: str) -> OrderResult:
        """Read userAssets.free for base asset, MARKET SELL with AUTO_REPAY.
        On NOTIONAL rejection, retry with quoteOrderQty (dust cleanup)."""
        base = self._base_asset(inst_id)
        try:
            acct = self._client.get_margin_account()
        except BinanceAPIException as e:
            return OrderResult(
                success=False, order_id="",
                error_code=str(getattr(e, "code", "")) or "exception",
                error_msg=str(e),
            )

        free = 0.0
        for item in acct.get("userAssets", []):
            if item.get("asset") == base:
                try:
                    free = float(item.get("free", 0) or 0)
                except (TypeError, ValueError):
                    free = 0.0
                break

        if free <= 0:
            return OrderResult(
                success=False, order_id="",
                error_code="NO_POSITION",
                error_msg=f"No free {base} balance to close",
            )

        return self.place_reduce_order(inst_id=inst_id, size=free)

    # -- Reconciliation ----------------------------------------------

    def get_recent_fills(
        self,
        since_ms: int | None = None,
        inst_ids: list[str] | None = None,
    ) -> list[FillInfo]:
        """Recent fills via /margin/myTrades iterated over inst_ids.

        Default time window: last 24 hours if since_ms is None. Binance
        myTrades is per-symbol; if inst_ids is None, we union across symbols
        from current get_positions(), BUT this misses just-closed positions.

        NOTE FOR CALLERS: For exit-price reconciliation of a position that
        was just closed, you MUST pass inst_ids=[symbol] explicitly — a
        closed position is no longer in get_positions(), so inst_ids=None
        would skip it entirely.
        """
        if since_ms is None:
            since_ms = int(time.time() * 1000) - 24 * 3600 * 1000

        if inst_ids is None:
            # Fallback: union of currently-open positions. Warn about the
            # just-closed blind spot.
            open_positions = self.get_positions()
            inst_ids = [p.inst_id for p in open_positions]
            log.warning(
                "Binance get_recent_fills called with inst_ids=None — using "
                "open positions only; just-closed symbols will be missed. "
                "Pass inst_ids=[...] explicitly for exit reconciliation."
            )

        results: list[FillInfo] = []
        for iid in inst_ids:
            try:
                symbol = self._to_binance(iid)
            except ValueError:
                continue
            try:
                trades = self._client.get_margin_trades(
                    symbol=symbol, startTime=since_ms,
                )
            except BinanceAPIException as e:
                log.warning(f"{iid}: get_margin_trades failed: {e}")
                continue

            for t in trades:
                try:
                    price = float(t.get("price", 0) or 0)
                    qty   = float(t.get("qty", 0) or 0)
                    ts_ms = int(t.get("time", 0) or 0)
                except (TypeError, ValueError):
                    continue
                if price <= 0 or qty <= 0:
                    continue
                # Binance: isBuyer=True → we were the buyer (side=BUY).
                side = "buy" if t.get("isBuyer") else "sell"
                order_id = str(t.get("orderId", "") or "")
                results.append(FillInfo(
                    inst_id=iid, order_id=order_id, side=side,
                    price=price, size=qty, ts_ms=ts_ms,
                ))

        return results

    # -- Capital flow ------------------------------------------------

    def get_capital_events(
        self,
        since_ms: int | None = None,
    ) -> list[CapitalEventInfo]:
        """Pull deposit + withdrawal history from Binance SAPI.

        Endpoints used:
          /sapi/v1/capital/deposit/hisrec   — deposit history
          /sapi/v1/capital/withdraw/history — withdrawal history
          /sapi/v1/margin/transfer          — margin in/out (NOT polled here;
              these are intra-account moves between spot ↔ margin and aren't
              true capital events for PnL purposes — they don't change the
              total USD on the exchange, only its sub-account location.
              Worth revisiting if the strategy switches accounts.)

        Stablecoin amounts (USDT/USDC/etc.) are taken at face value.
        Non-stablecoin assets are skipped — capital events for those are
        rare and better surfaced via manual operator entry than mis-valued.

        Default lookback when since_ms is None: 365 days (Binance retention).
        """
        if since_ms is None:
            since_ms = int(time.time() * 1000) - (365 * 86_400_000)

        results: list[CapitalEventInfo] = []
        try:
            deposits = self._client.get_deposit_history(startTime=since_ms)
        except BinanceAPIException as e:
            log.warning(f"Binance deposit history fetch failed: {e}")
            deposits = []
        try:
            withdraws = self._client.get_withdraw_history(startTime=since_ms)
        except BinanceAPIException as e:
            log.warning(f"Binance withdraw history fetch failed: {e}")
            withdraws = []

        for row in deposits or []:
            results.extend(_parse_binance_capital_row(row, "deposit"))
        for row in withdraws or []:
            results.extend(_parse_binance_capital_row(row, "withdrawal"))

        return results

    # -- Internals ---------------------------------------------------

    def _margin_pairs(self) -> set[str]:
        """TTL-cached set of symbols supporting cross-margin trading."""
        now = time.time()
        if (self._margin_pairs_cache is None
                or now - self._margin_pairs_fetched_at > _MARGIN_PAIRS_TTL_S):
            try:
                pairs = self._client.get_margin_all_pairs()
                self._margin_pairs_cache = {
                    p.get("symbol") for p in pairs if p.get("symbol")
                }
                self._margin_pairs_fetched_at = now
            except BinanceAPIException as e:
                log.warning(f"get_margin_all_pairs failed: {e}")
                if self._margin_pairs_cache is None:
                    self._margin_pairs_cache = set()
        return self._margin_pairs_cache

    def _spot_price(self, symbol: str) -> float | None:
        """Last traded price via /api/v3/ticker/price."""
        try:
            resp = self._client.get_symbol_ticker(symbol=symbol)
            price = float(resp.get("price", 0) or 0)
            return price if price > 0 else None
        except BinanceAPIException as e:
            log.warning(f"{symbol}: get_symbol_ticker failed: {e}")
            return None
        except (TypeError, ValueError):
            return None

    def _average_buy_price(self, symbol: str) -> float | None:
        """Volume-weighted average of recent BUY fills for this symbol."""
        try:
            trades = self._client.get_margin_trades(symbol=symbol)
        except BinanceAPIException:
            return None

        total_qty  = 0.0
        total_cost = 0.0
        for t in trades:
            if not t.get("isBuyer"):
                continue
            try:
                qty   = float(t.get("qty", 0) or 0)
                price = float(t.get("price", 0) or 0)
            except (TypeError, ValueError):
                continue
            if qty <= 0 or price <= 0:
                continue
            total_qty  += qty
            total_cost += qty * price
        return (total_cost / total_qty) if total_qty > 0 else None

    def _sell_by_quote(self, symbol: str, size: float) -> OrderResult:
        """Fallback SELL using quoteOrderQty (USDT amount) instead of qty.
        Used when LOT_SIZE / NOTIONAL filters reject a qty-based sell."""
        price = self._spot_price(symbol)
        if price is None:
            return OrderResult(
                success=False, order_id="",
                error_code="NO_PRICE",
                error_msg=f"Cannot price {symbol} for quoteOrderQty sell",
            )
        notional = round(size * price, 2)
        if notional < 1.0:
            return OrderResult(
                success=False, order_id="",
                error_code="DUST",
                error_msg=(
                    f"{symbol}: notional ${notional:.4f} below $1 floor — "
                    "use Binance Convert Dust manually"
                ),
            )
        try:
            order = self._client.create_margin_order(
                symbol=symbol, side="SELL", type="MARKET",
                quoteOrderQty=notional, sideEffectType="AUTO_REPAY",
            )
            return OrderResult(
                success=True,
                order_id=str(order.get("orderId", "")),
                error_code="0", error_msg="",
            )
        except BinanceAPIException as e:
            return OrderResult(
                success=False, order_id="",
                error_code=str(getattr(e, "code", "")) or "exception",
                error_msg=str(e),
            )


# -- Module helpers ------------------------------------------------------

# Parallel to BloFin's _STABLECOINS_USD_PARITY — kept inline rather than
# imported to avoid a cross-adapter coupling for what is conceptually a
# per-adapter concern (each exchange may add stablecoins independently).
_STABLECOINS_USD_PARITY = {
    "USDT", "USDC", "BUSD", "TUSD", "USDP", "FDUSD",
    "USDS", "USDE", "FRAX", "DAI", "PYUSD", "USD1",
}


def _parse_binance_capital_row(row: dict, kind: str) -> list[CapitalEventInfo]:
    """Convert one Binance deposit/withdrawal row into 0-or-1
    CapitalEventInfo. Skips:
      - Non-stablecoin assets (no FX source available here)
      - Failed / cancelled transfers (status != 1 for deposits,
        status != 6 for withdrawals — the python-binance SDK exposes
        status codes per Binance docs)
      - Rows missing amount, timestamp, or the exchange-internal id
    """
    asset = (row.get("coin") or row.get("asset") or "").upper().strip()
    if asset not in _STABLECOINS_USD_PARITY:
        return []

    # Status filter: Binance deposit status=1 means SUCCESS; any other value
    # (0=pending, 6=credited but cannot withdraw, etc.) is excluded.
    # Withdrawal status=6 means COMPLETED. Filter conservatively — pending
    # entries get picked up on a later poll once they finalize.
    status = row.get("status")
    if kind == "deposit" and status not in (1, "1"):
        return []
    if kind == "withdrawal" and status not in (6, "6"):
        return []

    amount = 0.0
    try:
        amount = float(row.get("amount", 0) or 0)
    except (TypeError, ValueError):
        return []
    if amount <= 0:
        return []

    # Binance returns insertTime (deposits) or applyTime (withdrawals)
    # in epoch-ms.
    ts_raw = row.get("insertTime") or row.get("applyTime") or 0
    try:
        ts_ms = int(ts_raw)
    except (TypeError, ValueError):
        return []
    if ts_ms <= 0:
        return []

    # Stable dedup ID. Deposits have `id` (string); withdrawals also have
    # `id`. Both can be NULL on edge cases (manually-credited rebates etc.)
    # — fall through to txId then a synthesized hash.
    event_id = str(row.get("id") or row.get("txId") or "")
    if not event_id:
        return []

    notes_parts = []
    if row.get("network"):
        notes_parts.append(f"network={row['network']}")
    if row.get("txId") and row.get("txId") != event_id:
        notes_parts.append(f"tx={row['txId']}")
    notes = "; ".join(notes_parts) if notes_parts else None

    return [CapitalEventInfo(
        event_id=event_id,
        kind=kind,
        amount_usd=amount,
        ts_ms=ts_ms,
        asset=asset,
        notes=notes,
    )]


def _inactive_instrument(inst_id: str) -> InstrumentInfo:
    """Placeholder for symbols we can't fetch info for; state=suspended so
    callers' state checks can gate trading."""
    return InstrumentInfo(
        inst_id=inst_id,
        contract_value=1.0,
        min_size=1.0,
        lot_size=1.0,
        max_market_size=None,
        state="suspended",
    )
