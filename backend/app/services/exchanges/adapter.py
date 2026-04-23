"""
backend/app/services/exchanges/adapter.py
==========================================
Exchange-agnostic trade surface for the per-allocation trader.

The trader at backend/app/cli/trader_blofin.py is the single orchestration
engine for all supported exchanges. This module provides the ABC that lets
it dispatch exchange-specific RPC through a common interface.

Concrete adapters:
  - BloFinAdapter        -- wraps the existing BlofinREST client (futures/perps)
  - BinanceMarginAdapter -- cross-margin trading via python-binance

Design principles:
  1. Inst IDs in BloFin form (e.g., "BTC-USDT") everywhere the trader code
     touches. Binance adapter translates to Binance form ("BTCUSDT") internally.
  2. Return shapes are normalized dataclasses, not raw exchange JSON.
     Field-name chaos (e.g., "positions" vs "pos", "fillPrice" vs "fillPx")
     is handled inside the adapter, never leaks to the trader.
  3. Exchange-specific concepts (margin_mode, position_side, sideEffectType,
     maxBorrowable, reduce_only) live inside the adapter, not in caller code.
  4. Multi-order orchestration (chunking, round-based retries, dry-run) stays
     in the caller. Single-call fallbacks (e.g., Binance -3006 MARGIN_BUY ->
     direct-borrow retry) live inside the adapter.
  5. Sync throughout. The trader and its spawner are fully synchronous; no
     event loop exists in the subprocess.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from app.services.trading.credential_loader import ExchangeCredentials


# -- Normalized return shapes --------------------------------------------

@dataclass(frozen=True)
class BalanceInfo:
    available_usdt: float
    total_usdt:     float


@dataclass(frozen=True)
class InstrumentInfo:
    inst_id:         str            # BloFin form ("BTC-USDT")
    contract_value:  float
    min_size:        float
    lot_size:        float
    max_market_size: float | None   # None = no chunking needed (Binance margin)
    state:           str            # "live" / "suspended" / etc.


@dataclass(frozen=True)
class PositionInfo:
    inst_id:       str
    contracts:     float            # base-unit size; positive = long
    average_price: float | None


@dataclass(frozen=True)
class OrderResult:
    success:    bool
    order_id:   str
    error_code: str                 # "0" (BloFin ok) / "102015" / "-3006" / ""
    error_msg:  str


@dataclass(frozen=True)
class FillInfo:
    inst_id:  str
    order_id: str                   # exchange-assigned order ID for reconciliation
    side:     str                   # "buy" / "sell" (lowercase)
    price:    float
    size:     float                 # base-unit size of this fill
    ts_ms:    int


@dataclass(frozen=True)
class CapitalEventInfo:
    """One deposit OR withdrawal as reported by an exchange income API.

    The poller writes these into user_mgmt.allocation_capital_events with
    source='auto' and exchange_event_id=event_id; later polls dedup against
    (connection_id, exchange_event_id) so a row is inserted at most once.
    """
    event_id:    str                # exchange's own ID (BloFin: transferId; Binance: id)
    kind:        str                # "deposit" | "withdrawal"
    amount_usd:  float              # positive value; sign comes from `kind`
    ts_ms:       int                # epoch-ms when the funds settled
    asset:       str                # "USDT" / "USDC" / etc. (informational)
    notes:       str | None = None  # free-form (tx hash, network, exchange-internal note)


# -- Adapter ABC ---------------------------------------------------------

class ExchangeAdapter(ABC):
    exchange_name:       str        # "blofin" / "binance"
    native_sl_supported: bool       # BloFin: True. Binance margin: False.

    # -- Account -------------------------------------------------------
    @abstractmethod
    def get_balance(self) -> BalanceInfo: ...

    # -- Market metadata -----------------------------------------------
    @abstractmethod
    def supports_symbol(self, inst_id: str) -> bool:
        """True if the symbol can be traded on this exchange/account.

        BloFin: always True when get_instruments returns live data.
        Binance margin: inst_id must be in get_margin_all_pairs().
        """

    @abstractmethod
    def get_instrument_info(self, inst_id: str) -> InstrumentInfo: ...

    @abstractmethod
    def get_price(self, inst_id: str) -> float | None: ...

    # -- Position state ------------------------------------------------
    @abstractmethod
    def get_positions(
        self, inst_ids: list[str] | None = None,
    ) -> list[PositionInfo]:
        """Return currently-open positions, optionally filtered by inst_ids.

        BloFin: read /account/positions.
        Binance margin: synthesized from userAssets where netAsset != 0;
                        average_price from /margin/myTrades per asset.
        """

    # -- Trading -------------------------------------------------------
    @abstractmethod
    def set_leverage(self, inst_id: str, leverage: int) -> OrderResult:
        """Configure leverage for the symbol.

        BloFin: /account/set-leverage POST.
        Binance margin: no-op verification -- compute max effective leverage
                        from get_max_margin_loan; OrderResult.success=True iff
                        requested leverage is achievable, else error_code=
                        'INSUFFICIENT_BORROW_LIMIT'.
        """

    @abstractmethod
    def place_entry_order(
        self, inst_id: str, size: float,
        sl_trigger_price: float | None = None,
    ) -> OrderResult:
        """Open / add to a long position of `size` base units.

        sl_trigger_price: exchange-native stop-loss attached atomically at entry.
            Callers MUST check self.native_sl_supported before populating this.
            Adapters where native_sl_supported=False MUST raise
            NotImplementedError if sl_trigger_price is not None. Client-side
            SL monitoring via the trader's existing port_sl / port_tsl loop
            is the replacement path.

        BloFin: MARKET BUY with optional inline sl_trigger_price.
        Binance margin: MARGIN_BUY primary; on -3006 (USDT borrow limit),
                        fallback to create_margin_loan(asset=base) +
                        NO_SIDE_EFFECT; on order-failure-after-loan, immediate
                        repay_margin_loan before returning success=False.
                        Raises NotImplementedError if sl_trigger_price is not
                        None (native_sl_supported=False).
        """

    @abstractmethod
    def place_reduce_order(self, inst_id: str, size: float) -> OrderResult:
        """Reduce an existing long position by `size` base units.

        BloFin: MARKET SELL reduce_only=true.
        Binance margin: MARKET SELL with AUTO_REPAY.
        """

    @abstractmethod
    def close_position(self, inst_id: str) -> OrderResult:
        """Fully close the position in `inst_id`, repaying any borrowed asset.

        BloFin: /trade/close-position (native atomic endpoint).
        Binance margin: read userAssets.free for base asset, MARKET SELL with
                        AUTO_REPAY; on NOTIONAL-filter rejection, retry with
                        quoteOrderQty (dust-cleanup pattern).
        """

    # -- Reconciliation ------------------------------------------------
    # -- Capital flow --------------------------------------------------
    def get_capital_events(
        self,
        since_ms: int | None = None,
    ) -> list[CapitalEventInfo]:
        """Recent deposits + withdrawals on this exchange account, in
        USD-denominated form (stablecoins assumed 1:1; volatile assets are
        valued at exchange-provided USD value when available, else skipped).

        since_ms: epoch-ms lower bound. None -> adapter-default lookback
                  (BloFin: ~90 days, Binance: ~365 days, both API-capped).

        Default implementation returns []; concrete adapters MAY override
        when the underlying exchange exposes the necessary endpoints.
        Falling back to [] is the safe choice for adapters not yet wired —
        the auto-poller treats an empty list as "nothing new" rather than
        an error, so deployment is gradual-rollout-friendly.

        Adapters that DO implement should return ALL events visible in the
        time window — the poller dedups against (connection_id,
        exchange_event_id) at insert time.
        """
        return []

    # -- Reconciliation ------------------------------------------------
    @abstractmethod
    def get_recent_fills(
        self,
        since_ms: int | None = None,
        inst_ids: list[str] | None = None,
    ) -> list[FillInfo]:
        """Recent fills for order/exit reconciliation.

        since_ms: epoch-ms lower bound. None -> adapter picks a reasonable
                  default (BloFin: last 3 days per /trade/fills-history;
                  Binance: last 24 hours per /margin/myTrades per-symbol).
        inst_ids: filter to these symbols. None -> all symbols with activity
                  in the time window. Binance uses per-symbol queries so
                  providing inst_ids bounds request fan-out meaningfully.

        BloFin: /trade/fills-history with /trade/fills fallback; post-filter
                by inst_ids if provided.
        Binance margin: /margin/myTrades iterated over inst_ids; if inst_ids
                is None, union across symbols in current open-positions list.
        """


# -- Dispatch ------------------------------------------------------------

def adapter_for(creds: ExchangeCredentials) -> ExchangeAdapter:
    """Build the right adapter for a decrypted ExchangeCredentials.

    Lazy-imports the concrete adapter so a BloFin-only subprocess doesn't pay
    the python-binance import cost (and vice versa).

    Raises ValueError on unsupported exchange; callers gate on this.
    """
    exchange = (creds.exchange or "").lower()
    if exchange == "blofin":
        from app.services.exchanges.blofin_adapter import BloFinAdapter
        return BloFinAdapter(
            api_key=creds.api_key,
            api_secret=creds.api_secret,
            passphrase=creds.passphrase,
        )
    if exchange == "binance":
        from app.services.exchanges.binance_margin_adapter import BinanceMarginAdapter
        return BinanceMarginAdapter(
            api_key=creds.api_key,
            api_secret=creds.api_secret,
        )
    raise ValueError(f"Unsupported exchange: {exchange!r}")
