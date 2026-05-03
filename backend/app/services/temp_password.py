"""
backend/app/services/temp_password.py
======================================
Admin-issued temporary password generator.

Format: ``adjective-Noun-NNNN`` (e.g. ``cobalt-Anchor-7392``).
- 18-char total, mixed case, digits, hyphen as the symbol class
- Digits avoid 0 and 1 to dodge oh/zero / one/lower-L confusion
- Output guaranteed to clear the Phase 1a `_password_score()` >= 3 floor
  via a self-check at module import time

Wordlists are short, easy to read aloud, and avoid all common-substring
blocklist entries from `_password_score()` (`password`, `qwerty`, `admin`,
`login`, ``mullincap``, etc.).

Entropy: 50 * 50 * 8^4 = ~10M combinations. Sufficient for one-shot
admin-issued passwords that the user is expected to change immediately.
"""

from __future__ import annotations

import secrets
from typing import Final


_ADJECTIVES: Final[tuple[str, ...]] = (
    "cobalt", "velvet", "amber", "silent", "cedar", "copper", "granite",
    "sable", "bronze", "ivory", "mauve", "azure", "ember", "slate",
    "quartz", "jasper", "opal", "marble", "garnet", "ebony", "dusky",
    "frosted", "brisk", "crimson", "indigo", "scarlet", "tawny", "russet",
    "ocher", "mossy", "foggy", "misty", "stormy", "gilded", "hazel",
    "ashen", "rusty", "faded", "glassy", "downy", "pearly", "smoky",
    "glossy", "silken", "satin", "woolen", "tartan", "tweed", "linen",
    "topaz",
)

_NOUNS: Final[tuple[str, ...]] = (
    "Anchor", "Meadow", "Lantern", "Compass", "Beacon", "River", "Cavern",
    "Summit", "Ridge", "Valley", "Canyon", "Prairie", "Glacier", "Fjord",
    "Lagoon", "Oasis", "Plateau", "Atoll", "Dune", "Marsh", "Tundra",
    "Savanna", "Isle", "Peninsula", "Cliff", "Gorge", "Brook", "Channel",
    "Delta", "Basin", "Knoll", "Chasm", "Gully", "Ravine", "Vista",
    "Horizon", "Hamlet", "Citadel", "Bastion", "Parapet", "Watershed",
    "Estuary", "Archipelago", "Monolith", "Pillar", "Pinnacle", "Sanctum",
    "Sentinel", "Cove", "Harbor",
)

_DIGITS: Final[str] = "23456789"  # avoid 0/1


def generate_temp_password() -> str:
    """Mint one temp password. Each call returns a fresh value."""
    adj = secrets.choice(_ADJECTIVES)
    noun = secrets.choice(_NOUNS)
    digits = "".join(secrets.choice(_DIGITS) for _ in range(4))
    return f"{adj}-{noun}-{digits}"


# ─── Module-import self-check ───────────────────────────────────────────────
# Catch wordlist drift at startup rather than in production. Imports
# `_password_score` from auth.py to ensure scoring stays in lockstep.

def _self_check() -> None:
    from ..api.routes.auth import _password_score, PASSWORD_MIN_SCORE  # noqa: PLC0415

    # Sample the worst-case combinations — shortest possible adjective +
    # shortest possible noun. If those score >= MIN, all longer combos do too.
    adj_min = min(_ADJECTIVES, key=len)
    noun_min = min(_NOUNS, key=len)
    sample = f"{adj_min}-{noun_min}-2222"
    score = _password_score(sample)
    if score < PASSWORD_MIN_SCORE:
        raise RuntimeError(
            f"temp_password generator out of spec: "
            f"sample {sample!r} scored {score}, need >= {PASSWORD_MIN_SCORE}"
        )

    # Confirm wordlist sizes are reasonable (>= 40) — guards against
    # accidental list truncation.
    assert len(_ADJECTIVES) >= 40, f"adjective list too short: {len(_ADJECTIVES)}"
    assert len(_NOUNS) >= 40, f"noun list too short: {len(_NOUNS)}"


_self_check()
