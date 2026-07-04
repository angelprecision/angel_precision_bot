from __future__ import annotations

import math
from typing import Optional

_SQRT_252 = math.sqrt(252.0)
_MIN_EXPECTED_OPTION_DAILY_RANGE_PCT = 0.10
_MAX_EXPECTED_OPTION_DAILY_RANGE_PCT = 2.00


def normalize_iv(iv) -> Optional[float]:
    """Normalize IV to a decimal fraction (0.40 = 40%)."""
    try:
        value = float(iv)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    if value > 1.0:
        value = value / 100.0
    return value if value > 0 else None


def compute_expected_move_1d_pct_underlying(entry_atm_iv) -> Optional[float]:
    """Approximate 1-day underlying move as IV / sqrt(252)."""
    iv = normalize_iv(entry_atm_iv)
    if iv is None:
        return None
    return iv / _SQRT_252


def clamp_expected_option_daily_range_pct(value) -> Optional[float]:
    try:
        pct = float(value)
    except (TypeError, ValueError):
        return None
    if pct <= 0:
        return None
    return min(_MAX_EXPECTED_OPTION_DAILY_RANGE_PCT, max(_MIN_EXPECTED_OPTION_DAILY_RANGE_PCT, pct))


def compute_expected_option_daily_range_pct(
    abs_delta,
    underlying_price,
    expected_move_1d_pct_underlying,
    option_premium_per_share,
) -> Optional[float]:
    try:
        delta = abs(float(abs_delta))
        underlying = float(underlying_price)
        move_pct = float(expected_move_1d_pct_underlying)
        premium = float(option_premium_per_share)
    except (TypeError, ValueError):
        return None
    if delta <= 0 or underlying <= 0 or move_pct <= 0 or premium <= 0:
        return None
    expected_underlying_move = underlying * move_pct
    raw_pct = (delta * expected_underlying_move) / premium
    return clamp_expected_option_daily_range_pct(raw_pct)


def build_vol_exit_snapshot(
    *,
    entry_atm_iv,
    abs_delta,
    underlying_price,
    option_premium_per_share,
) -> dict:
    expected_move = compute_expected_move_1d_pct_underlying(entry_atm_iv)
    expected_range = compute_expected_option_daily_range_pct(
        abs_delta,
        underlying_price,
        expected_move,
        option_premium_per_share,
    )
    unavailable_reason = None
    if entry_atm_iv in (None, ""):
        unavailable_reason = "missing_iv"
    elif expected_move is None:
        unavailable_reason = "invalid_iv"
    elif expected_range is None:
        unavailable_reason = "missing_expected_range_inputs"
    return {
        "entry_atm_iv": normalize_iv(entry_atm_iv),
        "expected_move_1d_pct_underlying": expected_move,
        "expected_option_daily_range_pct": expected_range,
        "unavailable_reason": unavailable_reason,
    }
