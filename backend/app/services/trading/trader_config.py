"""
backend/app/services/trading/trader_config.py
==============================================
Frozen parameter bundle for a single trader run.

master_defaults() intentionally reproduces the module-level constants in
trader-blofin.py line-for-line. If this dataclass's values ever diverge from
the live script, one of them has drifted from the audit-matched baseline.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)


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

        Field lookup is case-insensitive. Each field has a list of alias names
        ordered by preference — first match wins, and if nothing matches, the
        master default is used and a WARN is logged (greppable signal for
        future strategy configs with unknown field names).
        """
        cfg = strategy_version_config or {}
        # Lowercase once at entry — all alias lookups are lowercase.
        cfg_lower = {str(k).lower(): v for k, v in cfg.items() if k is not None}

        defaults = cls()  # master baseline

        def _pick(field: str, aliases: list[str], default):
            for alias in aliases:
                if alias in cfg_lower:
                    return cfg_lower[alias]
            log.warning(
                "TraderConfig.from_strategy_version: field %r not found "
                "(tried aliases %s). Using master default %r.",
                field, aliases, default,
            )
            return default

        # Aliases grounded in observed prod configs — see investigation
        # output in the PR description for the raw config dumps.
        raw_tsl = float(_pick(
            "port_tsl_pct", ["port_tsl_pct", "port_tsl"], defaults.port_tsl_pct,
        ))
        # Audit framework stores port_tsl as a positive magnitude (see
        # pipeline/audit.py CUBE_VALUES_PORT_TSL and the `-abs(port_tsl)`
        # negation at comparison time, audit.py:8387). Live trader expects
        # signed negative. Normalize unconditionally at this boundary so
        # both sides keep their own convention.
        port_tsl_pct = -abs(raw_tsl)
        if raw_tsl > 0:
            log.info(
                "TraderConfig: port_tsl_pct normalized %s -> %s (audit convention "
                "is positive magnitude; trader expects signed negative)",
                raw_tsl, port_tsl_pct,
            )

        return cls(
            l_high=float(_pick("l_high", ["l_high"], defaults.l_high)),
            kill_y=float(_pick("kill_y", ["kill_y", "early_kill_y"], defaults.kill_y)),
            port_sl_pct=float(_pick(
                "port_sl_pct", ["port_sl_pct", "port_sl"], defaults.port_sl_pct,
            )),
            port_tsl_pct=port_tsl_pct,
            early_fill_y=float(_pick(
                "early_fill_y", ["early_fill_y"], defaults.early_fill_y,
            )),
            active_filter=str(_pick(
                "active_filter",
                ["active_filter", "filter_mode"],
                defaults.active_filter,
            )),
            capital_mode="fixed_usd",
            capital_value=capital_usd,
            # All other fields intentionally use master defaults — they're
            # exchange mechanics and session timing, not strategy parameters.
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
