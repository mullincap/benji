"""Canonicalize and hash strategy config dicts.

Identity-bearing keys define what a strategy IS for executor purposes.
Metadata keys (starting_capital, sweep toggles, reporting flags, notes,
timestamps, data-source pointers) are excluded.

Lowercases keys once at entry, resolves four aliases, filters to identity
fields, normalizes numeric types, serializes as sorted-keys JSON, SHA256.
"""
import hashlib
import json
from decimal import Decimal

# Identity-bearing fields — everything else stripped before hashing.
# Update this set carefully; any change invalidates existing hashes and
# requires a backfill. Fields confirmed against both prod strategy
# version configs (alpha_tail_guardrail_low_risk v1 lowercase,
# overlap_tail_disp v1.0 UPPERCASE).
IDENTITY_FIELDS = frozenset([
    # Core execution params
    "l_high",
    "kill_y",           # alias: early_kill_y
    "port_sl_pct",      # alias: port_sl
    "port_tsl_pct",     # alias: port_tsl
    "early_fill_y",
    "active_filter",    # alias: filter_mode
    # Signal / filter params
    "tail_drop_pct",
    "tail_vol_mult",
    "dispersion_threshold",
    "dispersion_baseline_win",
    "freq_cutoff",
    "freq_width",
    # VOL leverage parameters (identity, not runtime state)
    "vol_lev_target_vol",
    "vol_lev_window",
    "vol_lev_sharpe_ref",
    "vol_lev_max_boost",
    "vol_lev_dd_threshold",
    "vol_lev_dd_scale",
])

# Pre-canonicalization aliases. Applied after lowercasing.
# Canonical form wins if both are present in the same config.
ALIASES = {
    "early_kill_y": "kill_y",
    "port_sl": "port_sl_pct",
    "port_tsl": "port_tsl_pct",
    "filter_mode": "active_filter",
}


def canonicalize_config(config: dict | None) -> dict:
    """Lowercase keys → resolve aliases → filter to identity → normalize numerics.

    Returns the canonical subset of config used for hashing.
    """
    if not config:
        return {}
    lc = {k.lower(): v for k, v in config.items()}
    resolved: dict = {}
    for k, v in lc.items():
        canonical = ALIASES.get(k, k)
        if canonical not in resolved:
            resolved[canonical] = v
    identity = {k: v for k, v in resolved.items() if k in IDENTITY_FIELDS}
    normalized: dict = {}
    for k, v in identity.items():
        if isinstance(v, bool):
            normalized[k] = v
        elif isinstance(v, (int, float, Decimal)):
            normalized[k] = float(v)
        elif isinstance(v, str) and _is_numeric(v):
            normalized[k] = float(v)
        else:
            normalized[k] = v
    return normalized


def hash_config(config: dict | None) -> str:
    """SHA256 of canonicalized config as hex string."""
    canonical = canonicalize_config(config)
    serialized = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _is_numeric(s: str) -> bool:
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False
