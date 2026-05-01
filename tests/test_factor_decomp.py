"""tests/test_factor_decomp.py
================================
Regression-test guards for `backend/app/services/factor_decomp_cache.py`.

The original step-11 synthetic tests used uncorrelated factors and missed
the multicollinearity issue that surfaced when the worker met real-world
BTC/alt-index data (~0.85+ correlated on 30d daily). This file fixes that
gap by simulating the realistic correlated-factors scenario and asserting:

  * The `_orthogonalize` helper drives sample correlation between its
    output and the predictor to zero by construction.
  * After the orthogonalization fix, β_BTC recovers with the correct
    sign and reasonable magnitude on a position with known synthetic
    sensitivity, even when the raw factor is 0.85+ correlated with BTC.
  * Without orthogonalization (the pre-fix path), the per-position β's
    are unstable enough to flip sign on a meaningful share of samples —
    exactly the "NEIRO β_BTC < 0" failure mode that motivated the fix.
  * Variance attribution at the portfolio level remains rotation-
    invariant within rounding, so the diversification narrative
    survives the factor change.
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from app.services.factor_decomp_cache import (  # noqa: E402
    _orthogonalize,
    _ridge_2d,
    _variance,
)


def _gen_correlated_factors(
    n: int, *, btc_sd: float, alpha: float, idio_sd: float, seed: int,
) -> tuple[list[float], list[float]]:
    """Build (btc_returns, raw_alt_returns) with controlled correlation.
    raw_alt = alpha × btc + idio. Sample correlation typically ≈
    alpha × btc_sd / sqrt(alpha² × btc_sd² + idio_sd²) — tune via
    btc_sd / idio_sd to land near 0.85+."""
    rng = random.Random(seed)
    btc = [rng.gauss(0, btc_sd) for _ in range(n)]
    raw_alt = [alpha * btc[i] + rng.gauss(0, idio_sd) for i in range(n)]
    return btc, raw_alt


def _correlation(xs: list[float], ys: list[float]) -> float:
    n = len(xs)
    mx = sum(xs) / n
    my = sum(ys) / n
    cov = sum((xs[i] - mx) * (ys[i] - my) for i in range(n)) / n
    sx = (sum((v - mx) ** 2 for v in xs) / n) ** 0.5
    sy = (sum((v - my) ** 2 for v in ys) / n) ** 0.5
    if sx == 0 or sy == 0:
        return 0.0
    return cov / (sx * sy)


# ── Tests ─────────────────────────────────────────────────────────────


def test_orthogonalize_removes_correlation_with_predictor():
    """`_orthogonalize(target, predictor)` must produce a residual series
    that is uncorrelated with `predictor` by construction. This is the
    load-bearing property the multicollinearity fix relies on."""
    btc, raw_alt = _gen_correlated_factors(
        n=29, btc_sd=0.04, alpha=0.9, idio_sd=0.012, seed=11,
    )
    # Sanity: raw factors should be highly correlated.
    raw_corr = _correlation(btc, raw_alt)
    assert raw_corr > 0.85, (
        f"Test setup expected raw correlation ≥ 0.85; got {raw_corr:.3f}"
    )

    alt_resid = _orthogonalize(raw_alt, btc)
    resid_corr = _correlation(btc, alt_resid)
    assert abs(resid_corr) < 1e-10, (
        f"Orthogonalized residual must be uncorrelated with predictor; "
        f"got {resid_corr:.3e}"
    )

    # Residual must have mean ≈ 0 (OLS-with-intercept property).
    n = len(alt_resid)
    mean = sum(alt_resid) / n
    assert abs(mean) < 1e-10, f"Residual mean should be ≈0; got {mean:.3e}"


def test_beta_btc_sign_stable_on_collinear_factors():
    """The smoking-gun test: in a low-SNR regime that mirrors the live
    book (small alt position with weak factor sensitivity, high
    idiosyncratic noise), the **raw** multicollinear regression flips
    β_BTC negative on a meaningful share of seeds — exactly the
    "NEIRO β_BTC = -3.04x notional on a long meme coin" failure mode
    that motivated the orthogonalization fix. The **orthogonalized**
    regression keeps the sign correct nearly always.

    Asserts:
      * Orth path: β_BTC sign correct in ≥95% of trials.
      * Raw path: visibly worse than orth (orth - raw ≥ 5 trials),
        proving orthogonalization is doing real work — if both paths
        perform identically the test setup is too easy or the
        orthogonalization step has been silently bypassed.
    """
    n_trials = 200
    n_obs = 29
    # Low-SNR setup matching live conditions: position notional ~$2k,
    # daily PnL stdev a few $, true β small (~$200/+100% BTC), noise
    # large relative to signal so collinearity-driven sign flips
    # actually manifest in the raw regression.
    true_beta_btc = 200.0

    raw_sign_correct = 0
    orth_sign_correct = 0

    for seed in range(n_trials):
        btc, raw_alt = _gen_correlated_factors(
            n=n_obs, btc_sd=0.04, alpha=0.9, idio_sd=0.012, seed=seed,
        )
        rng = random.Random(seed * 31 + 7)
        noise = [rng.gauss(0, 25.0) for _ in range(n_obs)]
        y = [100 + true_beta_btc * btc[i] + noise[i] for i in range(n_obs)]

        # Pre-fix path: regress on raw (multicollinear) factors.
        _a, b_btc_raw, _b_alt_raw, _r = _ridge_2d(y, btc, raw_alt)
        if b_btc_raw > 0:
            raw_sign_correct += 1

        # Post-fix path: orthogonalize first.
        alt_resid = _orthogonalize(raw_alt, btc)
        _a, b_btc_orth, _b_alt_orth, _r = _ridge_2d(y, btc, alt_resid)
        if b_btc_orth > 0:
            orth_sign_correct += 1

    assert orth_sign_correct >= int(n_trials * 0.95), (
        f"Orthogonalized regression should recover correct β_BTC sign in "
        f">=95% of trials; got {orth_sign_correct}/{n_trials}"
    )
    # Raw path should be visibly worse — confirms orthogonalization is
    # doing real work, not silently bypassed.
    assert orth_sign_correct - raw_sign_correct >= 5, (
        f"Orthogonalization expected to materially improve sign stability; "
        f"raw correct {raw_sign_correct}/{n_trials} vs "
        f"orth correct {orth_sign_correct}/{n_trials}"
    )


def test_beta_btc_magnitude_recovers_post_orthogonalization():
    """Average β_BTC across many seeds should land near the true value
    after orthogonalization, even with collinear raw factors. Tolerance
    is wide enough to absorb the n=29 sampling noise floor."""
    n_trials = 100
    n_obs = 29
    true_beta_btc = 1000.0

    sum_b_btc = 0.0
    for seed in range(n_trials):
        btc, raw_alt = _gen_correlated_factors(
            n=n_obs, btc_sd=0.04, alpha=0.9, idio_sd=0.012, seed=seed,
        )
        rng = random.Random(seed * 31 + 7)
        noise = [rng.gauss(0, 5.0) for _ in range(n_obs)]
        y = [100 + true_beta_btc * btc[i] + noise[i] for i in range(n_obs)]
        alt_resid = _orthogonalize(raw_alt, btc)
        _a, b_btc, _b_alt, _r = _ridge_2d(y, btc, alt_resid)
        sum_b_btc += b_btc

    mean_b_btc = sum_b_btc / n_trials
    # Within 5% of the true value averaged over 100 trials.
    assert abs(mean_b_btc - true_beta_btc) < 0.05 * true_beta_btc, (
        f"Mean recovered β_BTC = {mean_b_btc:.1f}; expected ~{true_beta_btc}"
    )


def test_variance_attribution_concentrates_on_true_factor():
    """A position with pure-BTC sensitivity (no alt-residual β) and modest
    noise should land BTC% > 50% under the orthogonalized regression.
    This is the verbal claim the §9b dictionary makes — codifies it."""
    btc, raw_alt = _gen_correlated_factors(
        n=29, btc_sd=0.04, alpha=0.9, idio_sd=0.012, seed=42,
    )
    rng = random.Random(99)
    noise = [rng.gauss(0, 3.0) for _ in range(29)]
    y = [100 + 1000.0 * btc[i] + noise[i] for i in range(29)]
    alt_resid = _orthogonalize(raw_alt, btc)
    _a, b_btc, b_alt, residuals = _ridge_2d(y, btc, alt_resid)

    v_btc = b_btc ** 2 * _variance(btc)
    v_alt = b_alt ** 2 * _variance(alt_resid)
    v_idio = _variance(residuals)
    total = v_btc + v_alt + v_idio

    pct_btc = v_btc / total * 100
    pct_alt = v_alt / total * 100
    pct_idio = v_idio / total * 100

    assert abs(pct_btc + pct_alt + pct_idio - 100.0) < 1e-6, (
        f"Variance attribution must sum to 100; got {pct_btc + pct_alt + pct_idio:.6f}"
    )
    assert pct_btc > 50, (
        f"Pure-BTC position should attribute >50% to BTC; got {pct_btc:.1f}%"
    )
    # Alt residual sensitivity is zero by construction → small share.
    assert pct_alt < 10, (
        f"No-alt-sensitivity position should attribute <10% to ALT; "
        f"got {pct_alt:.1f}%"
    )


def test_pure_alt_residual_position_attributes_to_alt_not_btc():
    """Mirror image of the BTC-only test: a position with sensitivity
    only to alt-residual moves (zero β to BTC) should land ALT% >> BTC%
    after orthogonalization. Exercises the symmetric case so a future
    change can't swap which factor absorbs which class of variance."""
    btc, raw_alt = _gen_correlated_factors(
        n=29, btc_sd=0.04, alpha=0.9, idio_sd=0.012, seed=17,
    )
    alt_resid = _orthogonalize(raw_alt, btc)

    # Position PnL driven only by alt-residual moves.
    rng = random.Random(23)
    noise = [rng.gauss(0, 2.0) for _ in range(29)]
    y = [50 + 800.0 * alt_resid[i] + noise[i] for i in range(29)]

    _a, b_btc, b_alt, residuals = _ridge_2d(y, btc, alt_resid)
    v_btc = b_btc ** 2 * _variance(btc)
    v_alt = b_alt ** 2 * _variance(alt_resid)
    v_idio = _variance(residuals)
    total = v_btc + v_alt + v_idio

    pct_btc = v_btc / total * 100
    pct_alt = v_alt / total * 100

    assert pct_alt > pct_btc, (
        f"Pure-alt-residual position should attribute more to ALT than BTC; "
        f"got BTC%={pct_btc:.1f}, ALT%={pct_alt:.1f}"
    )
    assert b_btc < 0.5 * 800.0, (
        f"β_BTC on a pure-alt position should be small; got {b_btc:.1f}"
    )
