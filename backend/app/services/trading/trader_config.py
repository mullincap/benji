"""
backend/app/services/trading/trader_config.py
==============================================
Frozen parameter bundle for a single trader run.

master_defaults() intentionally reproduces the module-level constants in
trader-blofin.py line-for-line. If this dataclass's values ever diverge from
the live script, one of them has drifted from the audit-matched baseline.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TraderConfig:
    """Strategy + execution parameters for a single trader run.

    For the master account: populated from module-level constants (legacy
    defaults, see master_defaults).
    For user allocations: populated from strategy_version.config JSONB +
    allocation-specific overrides (capital, symbol filter).
    """

    # ── Strategy-level (per-strategy-version in future) ────────────────────
    l_high: float = 2.3
    kill_y: float = 0.003
    port_sl_pct: float = -0.06
    port_tsl_pct: float = -0.075
    early_fill_y: float = 0.09
    fill_max_bar: int = 143
    active_filter: str = "Tail Guardrail"

    # ── Session timing ─────────────────────────────────────────────────────
    session_start_hour: int = 6
    bar_minutes: int = 5
    conviction_bar: int = 6
    conviction_exec_buffer_min: int = 20

    # ── Exchange mechanics ─────────────────────────────────────────────────
    margin_mode: str = "isolated"
    position_side: str = "net"
    exchange_sl_enabled: bool = True
    exchange_sl_pct: float = -0.085
    symbol_suffix: str = "-USDT"

    # ── Capital sizing ─────────────────────────────────────────────────────
    capital_mode: str = "pct_balance"   # 'pct_balance' | 'fixed_usd'
    capital_value: float = 1.0          # fraction if pct_balance, dollars if fixed_usd

    # ── Fee constants (audit-matching) ─────────────────────────────────────
    taker_fee_pct: float = 0.0008
    funding_rate_daily_pct: float = 0.0002

    # ── Price source ───────────────────────────────────────────────────────
    price_source: str = "blofin"        # 'blofin' | 'binance'

    # ── VOL boost (deferred) ───────────────────────────────────────────────
    # User allocations always run at l_high (no boost). Master account
    # keeps its blofin_returns_log.csv-driven boost via the legacy script.
    vol_boost_enabled: bool = False

    # ─────────────────────────────────────────────────────────────────────────
    # Factories
    # ─────────────────────────────────────────────────────────────────────────

    @classmethod
    def master_defaults(cls) -> "TraderConfig":
        """Config matching trader-blofin.py module-level constants verbatim.

        Used both at runtime (the containerized copy boots with these) and as a
        regression check — if this diverges from the live script someone forgot
        to sync one side.
        """
        return cls()  # defaults above match current live constants

    @classmethod
    def from_strategy_version(
        cls,
        strategy_version_config: dict | None,
        *,
        capital_usd: float,
    ) -> "TraderConfig":
        """Derive a config from strategy_version.config JSONB + allocation capital.

        Strategy-level params come from the JSONB payload that was frozen into
        the audited strategy version. Allocation-level capital_usd overrides
        capital_mode to 'fixed_usd'.
        """
        cfg = strategy_version_config or {}
        return cls(
            l_high=float(cfg.get("L_HIGH", 2.3)),
            kill_y=float(cfg.get("KILL_Y", 0.003)),
            port_sl_pct=float(cfg.get("PORT_SL_PCT", -0.06)),
            port_tsl_pct=float(cfg.get("PORT_TSL_PCT", -0.075)),
            early_fill_y=float(cfg.get("EARLY_FILL_Y", 0.09)),
            active_filter=cfg.get(
                "active_filter", cfg.get("ACTIVE_FILTER", "Tail Guardrail")
            ),
            capital_mode="fixed_usd",
            capital_value=capital_usd,
            vol_boost_enabled=False,
            # All other fields intentionally use defaults — they're exchange
            # mechanics and session timing, not strategy parameters.
        )


# ─────────────────────────────────────────────────────────────────────────────
# Self-check: master_defaults() vs live trader-blofin.py constants
# ─────────────────────────────────────────────────────────────────────────────
# Values below are copied verbatim from trader-blofin.py lines 55-159. Asserts
# run on `python -m app.services.trading.trader_config`.

_LIVE_BASELINE = {
    "l_high": 2.3,
    "kill_y": 0.003,
    "port_sl_pct": -0.06,
    "port_tsl_pct": -0.075,
    "early_fill_y": 0.09,
    "fill_max_bar": 143,
    "active_filter": "Tail Guardrail",
    "session_start_hour": 6,
    "bar_minutes": 5,
    "conviction_bar": 6,
    "conviction_exec_buffer_min": 20,
    "margin_mode": "isolated",
    "position_side": "net",
    "exchange_sl_enabled": True,
    "exchange_sl_pct": -0.085,
    "symbol_suffix": "-USDT",
    "capital_mode": "pct_balance",
    "capital_value": 1.0,
    "taker_fee_pct": 0.0008,
    "funding_rate_daily_pct": 0.0002,
    "price_source": "blofin",
}


def _assert_master_matches_live() -> None:
    cfg = TraderConfig.master_defaults()
    mismatches = []
    for k, expected in _LIVE_BASELINE.items():
        actual = getattr(cfg, k)
        if actual != expected:
            mismatches.append(f"  {k}: master={actual!r} live={expected!r}")
    if mismatches:
        raise AssertionError(
            "TraderConfig.master_defaults() has drifted from live constants:\n"
            + "\n".join(mismatches)
        )


if __name__ == "__main__":
    _assert_master_matches_live()
    print("TraderConfig.master_defaults() matches live baseline (21 fields checked).")
    cfg = TraderConfig.master_defaults()
    for field_name in _LIVE_BASELINE:
        print(f"  {field_name:32s} = {getattr(cfg, field_name)!r}")
