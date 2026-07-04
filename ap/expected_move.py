# =============================================================================
# ap/expected_move.py — PR-G (foundation, bundled PR-E scope)
#
# Pure, deterministic expected-move math. NO I/O, NO env reads, NO imports
# from any AP module. Every function is TOTAL: it never raises on bad input;
# it returns (None, reason) instead. This module is the single computation
# authority for expected-move quantities consumed by:
#   - the volatility-scaled exit ladder (ap/exit_ladder.py, PR-G)
#   - the expected-move feasibility gate (PR-F, future)
#   - the FeatureSet assembler (PR-K, future)
#
# Money-safety posture: this module never makes decisions. It produces
# numbers with quality flags; callers decide, and callers MUST treat a
# (None, reason) result as "fall back to legacy behavior".
# =============================================================================
from __future__ import annotations

import math
from typing import Optional, Tuple

# Trading days per year — standard options convention.
TRADING_DAYS_PER_YEAR = 252.0

# Clamp bounds for the derived option daily-range estimate (fraction of
# premium). The delta approximation degrades for deep-OTM lottery tickets
# (range explodes) and deep-ITM stock-substitutes (range collapses); the
# clamp keeps the ladder inside sane, auditable territory.
# Spec (PR-G): clamp 10% .. 200%.
OPTION_RANGE_CLAMP_LO = 0.10
OPTION_RANGE_CLAMP_HI = 2.00

# Sanity bounds for annualized IV inputs. Tradier mid_iv is a decimal
# (0.35 = 35%). Anything outside (0, 5.0] is treated as corrupt data.
_IV_MIN_EXCLUSIVE = 0.0
_IV_MAX_INCLUSIVE = 5.0


def _as_pos_float(value, *, allow_zero: bool = False) -> Optional[float]:
    """Total conversion to a positive finite float. None on any failure."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    if allow_zero:
        return f if f >= 0.0 else None
    return f if f > 0.0 else None


def expected_move_1d_pct(atm_iv: object) -> Tuple[Optional[float], str]:
    """One-trading-day expected move of the underlying, as a FRACTION of spot.

    Formula: sigma_1d = IV_annualized / sqrt(252)

    Returns (value, quality) where quality is:
      "ok"                 — value usable
      "iv_missing"         — input absent / non-numeric
      "iv_out_of_bounds"   — IV <= 0 or > 500% annualized (corrupt feed)
    """
    iv = _as_pos_float(atm_iv)
    if iv is None:
        return None, "iv_missing"
    if not (_IV_MIN_EXCLUSIVE < iv <= _IV_MAX_INCLUSIVE):
        return None, "iv_out_of_bounds"
    return iv / math.sqrt(TRADING_DAYS_PER_YEAR), "ok"


def expected_move_pct_over(atm_iv: object, trading_days: object) -> Tuple[Optional[float], str]:
    """Expected move of the underlying over N trading days (fraction of spot).

    sigma_N = IV_annualized * sqrt(N / 252). Used by PR-F feasibility math.
    """
    em1, quality = expected_move_1d_pct(atm_iv)
    if em1 is None:
        return None, quality
    days = _as_pos_float(trading_days)
    if days is None:
        return None, "days_invalid"
    return em1 * math.sqrt(days), "ok"


def expected_option_daily_range_pct(
    delta: object,
    premium_per_share: object,
    underlying_price: object,
    atm_iv: object,
) -> Tuple[Optional[float], str]:
    """Approximate one-day expected P&L swing of the OPTION, as a fraction
    of its premium.

        range ≈ |delta| × (S × sigma_1d) / premium_per_share

    This is a first-order (delta-only) approximation. It deliberately
    ignores gamma/vega — documented limitation; the clamp bounds the
    approximation error. Result clamped to [0.10, 2.00] per PR-G spec.

    Returns (value, quality):
      "ok"            — usable, unclamped
      "ok_clamped_lo" / "ok_clamped_hi" — usable, clamp applied (stamped
                        so paper-soak review can see how often we clamp)
      "delta_missing" | "premium_invalid" | "underlying_invalid"
      + IV failure reasons propagated from expected_move_1d_pct
    """
    em1, quality = expected_move_1d_pct(atm_iv)
    if em1 is None:
        return None, quality

    d = _as_pos_float(abs(delta) if isinstance(delta, (int, float)) else delta)
    if d is None or not (0.0 < d <= 1.0):
        return None, "delta_missing"

    prem = _as_pos_float(premium_per_share)
    if prem is None:
        return None, "premium_invalid"

    spot = _as_pos_float(underlying_price)
    if spot is None:
        return None, "underlying_invalid"

    raw = d * (spot * em1) / prem
    if raw < OPTION_RANGE_CLAMP_LO:
        return OPTION_RANGE_CLAMP_LO, "ok_clamped_lo"
    if raw > OPTION_RANGE_CLAMP_HI:
        return OPTION_RANGE_CLAMP_HI, "ok_clamped_hi"
    return raw, "ok"


def feasibility_ratio(
    entry: object,
    target: object,
    em_pct_over_horizon: object,
    underlying_price: object = None,
) -> Tuple[Optional[float], str]:
    """|target − entry| / (S × em_pct). Ratio > 1 means the target sits
    outside the priced expected move for the horizon. Consumed by PR-F
    (observe-only feasibility gate); included here so the math has one home.

    If underlying_price is None, entry is used as the spot proxy.
    """
    e = _as_pos_float(entry)
    if e is None:
        return None, "entry_invalid"
    t = _as_pos_float(target)
    if t is None:
        return None, "target_invalid"
    em = _as_pos_float(em_pct_over_horizon)
    if em is None:
        return None, "em_invalid"
    spot = _as_pos_float(underlying_price) if underlying_price is not None else e
    if spot is None:
        return None, "underlying_invalid"
    move_needed = abs(t - e)
    expected_move_abs = spot * em
    if expected_move_abs <= 0.0:
        return None, "em_zero"
    return move_needed / expected_move_abs, "ok"
