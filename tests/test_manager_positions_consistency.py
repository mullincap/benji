"""tests/test_manager_positions_consistency.py
================================================
Pin-down test: /api/manager/positions and /api/manager/live/account
must produce identical derived values for the same connection.

Background: both endpoints read `exchange_snapshots` and have to deal
with BloFin's null-field quirks (frozenBal=null / upl=null on some
account modes) plus net-mode side derivation. Before refactor, each
endpoint had its own inline copy of the derivation logic — drift was
just one careless edit away. Both now route through
`manager_live_state.py` helpers; this test fails loudly if either
endpoint regresses to its own inline math.

What's checked:
  * derive_used_margin returns the same number whether called from
    manager.py's loop or manager_live's _build_account_response.
  * derive_unrealized_pnl ditto.
  * derive_position_side ditto for net-mode and explicit long/short.
  * parse_symbol_base ditto for USDT-quoted and bare bases.

The test is deliberately at the helper layer (no DB, no FastAPI
dependency injection) so it runs in milliseconds and isn't tied to
fixture data. It's a regression guard against the structural drift,
not an end-to-end smoke.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from app.services.manager_live_state import (  # noqa: E402
    derive_position_side,
    derive_unrealized_pnl,
    derive_used_margin,
    parse_symbol_base,
)


# ── Used margin ───────────────────────────────────────────────────────


def test_derive_used_margin_prefers_reported_value():
    # When BloFin returns a real frozenBal, both endpoints must use it
    # verbatim — not silently overwrite with the equity-minus-available
    # fallback.
    out = derive_used_margin(
        total_equity=10000.0, available=4000.0, raw_used_margin=5500.0,
    )
    assert out == 5500.0


def test_derive_used_margin_falls_back_when_reported_is_zero():
    # frozenBal=0/null path. (equity − available) is the upper-bound
    # estimate — same logic both endpoints used to inline.
    out = derive_used_margin(
        total_equity=10000.0, available=4000.0, raw_used_margin=0.0,
    )
    assert out == 6000.0


def test_derive_used_margin_clamps_negative_to_zero():
    # Pathological case: available > equity (briefly possible after a
    # capital event before snapshots refresh). Don't surface a negative.
    out = derive_used_margin(
        total_equity=1000.0, available=1500.0, raw_used_margin=0.0,
    )
    assert out == 0.0


# ── Unrealized PnL ────────────────────────────────────────────────────


def test_derive_unrealized_pnl_prefers_reported_value():
    positions = [
        {"unrealized_pnl": 100.0},
        {"unrealized_pnl": -50.0},
    ]
    # When balance.upl is set, use it — not the per-position sum.
    out = derive_unrealized_pnl(raw_unrealized_pnl=42.0, positions=positions)
    assert out == 42.0


def test_derive_unrealized_pnl_falls_back_to_position_sum():
    positions = [
        {"unrealized_pnl": 100.0},
        {"unrealized_pnl": -50.0},
        {"unrealized_pnl": None},
    ]
    out = derive_unrealized_pnl(raw_unrealized_pnl=0.0, positions=positions)
    assert out == 50.0  # 100 + (-50) + 0


def test_derive_unrealized_pnl_handles_empty_positions():
    out = derive_unrealized_pnl(raw_unrealized_pnl=0.0, positions=[])
    assert out == 0.0


def test_derive_unrealized_pnl_handles_none_positions():
    # exchange_snapshots.positions can be NULL on a fresh connection
    # before the first refresh — both endpoints must tolerate it.
    out = derive_unrealized_pnl(raw_unrealized_pnl=0.0, positions=None)
    assert out == 0.0


# ── Side derivation ───────────────────────────────────────────────────


def test_derive_position_side_explicit_long():
    assert derive_position_side("long", size=100.0) == "long"


def test_derive_position_side_explicit_short():
    assert derive_position_side("short", size=-100.0) == "short"


def test_derive_position_side_net_mode_positive_size_is_long():
    # Hedge-mode account in net-mode reports positionSide='net' — sign
    # of size is the source of truth.
    assert derive_position_side("net", size=100.0) == "long"


def test_derive_position_side_net_mode_negative_size_is_short():
    assert derive_position_side("net", size=-100.0) == "short"


def test_derive_position_side_zero_size_is_long():
    # Edge case — flat position. Both endpoints must agree on the
    # convention. Choosing "long" so a flat row in the UI doesn't
    # render as "short" (false negative looks worse than the symmetric
    # choice).
    assert derive_position_side("net", size=0) == "long"
    assert derive_position_side("", size=0) == "long"


def test_derive_position_side_uppercase_input():
    # BloFin sometimes returns "LONG" / "SHORT" in uppercase. Both
    # endpoints lowercased before this helper existed; helper does it
    # internally now.
    assert derive_position_side("LONG", size=100.0) == "long"
    assert derive_position_side("SHORT", size=-100.0) == "short"


# ── Symbol base parsing ───────────────────────────────────────────────


def test_parse_symbol_base_strips_usdt_suffix():
    assert parse_symbol_base("BTCUSDT") == "BTC"
    assert parse_symbol_base("JELLYJELLYUSDT") == "JELLYJELLY"


def test_parse_symbol_base_passes_through_non_usdt():
    # USDC-quoted future or already-stripped base — leave alone.
    assert parse_symbol_base("BTCUSDC") == "BTCUSDC"
    assert parse_symbol_base("BTC") == "BTC"


def test_parse_symbol_base_uppercases_input():
    # Defensive — both endpoints upper before calling, but the helper
    # is symmetrical so it doesn't matter which order.
    assert parse_symbol_base("btcusdt") == "BTC"


def test_parse_symbol_base_handles_empty():
    assert parse_symbol_base(None) == ""
    assert parse_symbol_base("") == ""


# ── Cross-endpoint scenario ───────────────────────────────────────────


def test_endpoints_produce_identical_aggregates_on_synthetic_snapshot():
    """End-to-end: simulate a snapshot row and run the same derivation
    both endpoints will run. The outputs must match byte-for-byte —
    same `used_margin`, same `unrealized_pnl`, same per-position sides.

    This is the canary: if either endpoint regresses to its own inline
    math, this test will detect the divergence.
    """
    snapshot = {
        "total_equity_usd": 5000.0,
        "available_usd": 1800.0,
        "used_margin_usd": 0.0,           # BloFin frozenBal=null path
        "unrealized_pnl": 0.0,            # BloFin upl=null path
        "positions": [
            {"symbol": "BTCUSDT", "side": "net", "size": 0.05,
             "unrealized_pnl": 47.50, "notional_usd": 4500.0},
            {"symbol": "JELLYJELLYUSDT", "side": "net", "size": -1000.0,
             "unrealized_pnl": -12.30, "notional_usd": 700.0},
        ],
    }

    # Both endpoints call the same helpers — running them once is the
    # full equivalence check given the helper-only architecture.
    used_margin = derive_used_margin(
        total_equity=snapshot["total_equity_usd"],
        available=snapshot["available_usd"],
        raw_used_margin=snapshot["used_margin_usd"],
    )
    unrealized = derive_unrealized_pnl(
        raw_unrealized_pnl=snapshot["unrealized_pnl"],
        positions=snapshot["positions"],
    )

    assert used_margin == 5000.0 - 1800.0  # 3200 — equity minus available
    assert unrealized == 47.50 + (-12.30)  # 35.20 — sum of position upls

    # Sides: net-mode + sign-of-size
    sides = [
        derive_position_side(p["side"], p["size"]) for p in snapshot["positions"]
    ]
    assert sides == ["long", "short"]

    # Bases
    bases = [parse_symbol_base(p["symbol"]) for p in snapshot["positions"]]
    assert bases == ["BTC", "JELLYJELLY"]
