# =============================================================================
# ap/expected_move.py — CANONICAL shared expected-move math
#
# This is the SINGLE canonical implementation. It is the reconciliation of the
# three previously-divergent copies that shipped on separate branches:
#   • PR-E (#279) / PR-F (#290): ExpectedMoveResult / AtmIvResult surface
#   • PR-G (#291): tuple (value, quality) surface for the vol-scaled exit ladder
#
# DESIGN CONTRACT:
#   1. PURE MATH ONLY. No I/O, no env reads, no imports from any AP module.
#      Every function is TOTAL — it never raises on bad input; it returns a
#      structured "unavailable" result (dataclass or tuple) instead.
#   2. NO TRADING BEHAVIOR. This module makes no decisions and enforces
#      nothing. Callers decide; callers treat an unavailable result as
#      "fall back to safe/legacy behavior".
#   3. BACKWARD-COMPATIBLE SUPERSET. Both public surfaces are preserved so
#      existing consumers on both branches import and run unchanged:
#        PR-F consumes: atm_iv_from_chain, expected_move_1d, expected_move_to,
#                       feasibility_ratio  (all ExpectedMoveResult/AtmIvResult)
#        PR-G consumes: expected_move_1d_pct, expected_move_pct_over,
#                       expected_option_daily_range_pct  (tuple form)
#      The ONE name that collided across surfaces was `feasibility_ratio`
#      (PR-F returns ExpectedMoveResult; PR-G returned a tuple). PR-F's is the
#      only one with a production consumer, so it KEEPS the canonical name.
#      PR-G's tuple variant — used only by its own unit test — is preserved as
#      `feasibility_ratio_pct`. No production consumer changes.
#
# The two surfaces intentionally keep their own private float/IV helpers
# (`_positive_float` vs `_as_pos_float`) because their semantics differ:
#   • PR-E `_normalize_iv` treats a value >3 as a percent and divides by 100
#     (so 31 -> 0.31), because atm_iv_from_chain reads raw chain rows.
#   • PR-G `expected_move_1d_pct` expects an already-decimal annualized IV and
#     validates it against (0, 5.0]; it does NOT percent-normalize.
# Merging them would silently change tested behavior on one side. They are
# preserved verbatim.
# =============================================================================
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Tuple

TRADING_DAYS_PER_YEAR = 252.0
DEFAULT_TRADING_HOURS_PER_DAY = 6.5

# PR-G clamp/validation constants (option daily-range estimate).
OPTION_RANGE_CLAMP_LO = 0.10
OPTION_RANGE_CLAMP_HI = 2.00
_IV_MIN_EXCLUSIVE = 0.0
_IV_MAX_INCLUSIVE = 5.0


# ─────────────────────────────────────────────────────────────────────────────
# SURFACE 1 — PR-E / PR-F: ExpectedMoveResult / AtmIvResult
# (preserved verbatim from PR-E #279; feasibility_ratio consumed by PR-F #290)
# ─────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class ExpectedMoveResult:
    """Result of an expected-move / feasibility computation.

    value: the numeric result, or None when unavailable.
    reason: "ok" on success, otherwise a structured unavailable reason.
    """

    value: float | None
    reason: str


@dataclass(frozen=True)
class AtmIvResult:
    """Result of ATM-IV derivation from a chain.

    quality:
      - ok: nearest call and put IV were both usable
      - single_leg: only one nearest side had usable IV
      - stale: nearest legs exist but look stale/unquoted
      - unavailable: IV could not be derived
    """

    value: float | None
    quality: str
    reason: str


def _positive_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric) or numeric <= 0:
        return None
    return numeric


def _normalize_iv(value: Any) -> float | None:
    iv = _positive_float(value)
    if iv is None:
        return None
    return iv / 100.0 if iv > 3 else iv


def _contract_type(row: dict[str, Any]) -> str:
    raw = str(
        row.get("option_type")
        or row.get("type")
        or row.get("put_call")
        or row.get("side")
        or ""
    ).strip().lower()
    if raw.startswith("c"):
        return "call"
    if raw.startswith("p"):
        return "put"
    symbol = str(row.get("symbol") or row.get("option_symbol") or "").upper()
    if "C" in symbol[-10:]:
        return "call"
    if "P" in symbol[-10:]:
        return "put"
    return ""


def _row_iv(row: dict[str, Any]) -> float | None:
    greeks = row.get("greeks")
    greeks = greeks if isinstance(greeks, dict) else {}
    return _normalize_iv(
        row.get("iv")
        or row.get("implied_volatility")
        or row.get("smv_vol")
        or greeks.get("mid_iv")
        or greeks.get("smv_vol")
    )


def _has_live_quote(row: dict[str, Any]) -> bool:
    bid = _positive_float(row.get("bid"))
    ask = _positive_float(row.get("ask"))
    volume = _positive_float(row.get("volume") or row.get("vol"))
    open_interest = _positive_float(row.get("open_interest") or row.get("oi"))
    return bool((bid is not None and ask is not None) or volume is not None or open_interest is not None)


def expected_move_1d(underlying_price: Any, atm_iv: Any) -> ExpectedMoveResult:
    """Return one-trading-day expected move using `S * sigma * sqrt(1 / 252)`.

    `S` is underlying price. `sigma` is annualized ATM implied volatility as a
    decimal; percent IV inputs such as `31` are normalized to `0.31`.
    """

    price = _positive_float(underlying_price)
    if price is None:
        return ExpectedMoveResult(None, "invalid_underlying_price")
    iv = _normalize_iv(atm_iv)
    if iv is None:
        return ExpectedMoveResult(None, "invalid_atm_iv")
    return ExpectedMoveResult(price * iv * math.sqrt(1.0 / TRADING_DAYS_PER_YEAR), "ok")


def expected_move_to(
    underlying_price: Any,
    atm_iv: Any,
    hours_ahead: Any,
    trading_hours_per_day: Any = DEFAULT_TRADING_HOURS_PER_DAY,
) -> ExpectedMoveResult:
    """Return horizon expected move using trading-hours annualization.

    Formula: `S * sigma * sqrt(trading_days / 252)`, where
    `trading_days = hours_ahead / trading_hours_per_day`.
    """

    price = _positive_float(underlying_price)
    if price is None:
        return ExpectedMoveResult(None, "invalid_underlying_price")
    iv = _normalize_iv(atm_iv)
    if iv is None:
        return ExpectedMoveResult(None, "invalid_atm_iv")
    hours = _positive_float(hours_ahead)
    if hours is None:
        return ExpectedMoveResult(None, "invalid_hours_ahead")
    hours_per_day = _positive_float(trading_hours_per_day)
    if hours_per_day is None:
        return ExpectedMoveResult(None, "invalid_trading_hours_per_day")
    trading_days = hours / hours_per_day
    return ExpectedMoveResult(price * iv * math.sqrt(trading_days / TRADING_DAYS_PER_YEAR), "ok")


def atm_iv_from_chain(chain: Iterable[Any] | None, underlying_price: Any) -> AtmIvResult:
    """Derive ATM IV from the nearest-strike call/put average.

    Picks the closest strike separately for calls and puts. Only LIVE legs
    (with a real quote) contribute IV; a leg that carries an IV number but has
    no live quote is treated as stale and EXCLUDED from the average. When no
    live-IV leg exists but a stale IV is present, returns quality="stale" so
    callers never price off a stale quote. `quality="ok"` requires live IV on
    BOTH sides. Malformed rows are ignored.

    (Canonical = PR-F #290 implementation — the stale-exclusion refinement over
    PR-E #279; PR-F is the version with the production consumer.)
    """

    price = _positive_float(underlying_price)
    if price is None:
        return AtmIvResult(None, "unavailable", "invalid_underlying_price")
    if not chain:
        return AtmIvResult(None, "unavailable", "missing_chain")

    by_side: dict[str, list[dict[str, Any]]] = {"call": [], "put": []}
    malformed_seen = False
    for row in chain:
        if not isinstance(row, dict):
            malformed_seen = True
            continue
        side = _contract_type(row)
        strike = _positive_float(row.get("strike"))
        if side not in by_side or strike is None:
            malformed_seen = True
            continue
        by_side[side].append(row)

    legs: list[dict[str, Any]] = []
    missing_sides: list[str] = []
    for side in ("call", "put"):
        rows = by_side[side]
        if not rows:
            missing_sides.append(side)
            continue
        legs.append(min(rows, key=lambda r: abs(float(r.get("strike")) - price)))

    if not legs:
        reason = "malformed_chain" if malformed_seen else "missing_chain_legs"
        return AtmIvResult(None, "unavailable", reason)

    live_legs = [leg for leg in legs if _has_live_quote(leg)]
    live_iv_legs = [(leg, _row_iv(leg)) for leg in live_legs]
    live_iv_legs = [(leg, iv) for leg, iv in live_iv_legs if iv is not None]
    stale_iv_present = any(_row_iv(leg) is not None for leg in legs if leg not in live_legs)

    if not live_iv_legs:
        if stale_iv_present:
            return AtmIvResult(None, "stale", "stale_chain")
        quality = "stale" if legs and not live_legs else "unavailable"
        reason = "stale_chain" if quality == "stale" else "missing_iv"
        return AtmIvResult(None, quality, reason)

    if not live_legs:
        return AtmIvResult(None, "stale", "stale_chain")

    ivs = [iv for _, iv in live_iv_legs]
    live_iv_sides = {_contract_type(leg) for leg, _ in live_iv_legs}
    quality = "ok" if live_iv_sides == {"call", "put"} and len(ivs) == 2 and not missing_sides else "single_leg"
    reason = "ok" if quality == "ok" else "missing_" + "_".join(missing_sides or ["leg"])
    return AtmIvResult(sum(ivs) / len(ivs), quality, reason)


def feasibility_ratio(entry: Any, target: Any, expected_move: Any) -> ExpectedMoveResult:
    """Return `abs(target - entry) / expected_move` as an ExpectedMoveResult.

    The ratio describes how many expected moves are required to travel from
    entry to target. Missing entry/target or non-positive expected move returns
    a structured unavailable reason.

    NOTE: this is the PR-F/PR-E signature (absolute expected-move input,
    ExpectedMoveResult output) and is the CANONICAL feasibility_ratio because
    it is the one with a production consumer (master_control feasibility obs).
    The PR-G percent-based variant lives at `feasibility_ratio_pct` below.
    """

    entry_price = _positive_float(entry)
    if entry_price is None:
        return ExpectedMoveResult(None, "invalid_entry")
    target_price = _positive_float(target)
    if target_price is None:
        return ExpectedMoveResult(None, "invalid_target")
    move = _positive_float(expected_move)
    if move is None:
        return ExpectedMoveResult(None, "invalid_expected_move")
    return ExpectedMoveResult(abs(target_price - entry_price) / move, "ok")


# ─────────────────────────────────────────────────────────────────────────────
# SURFACE 2 — PR-G: tuple (value, quality) — vol-scaled exit ladder math
# (preserved verbatim from PR-G #291; feasibility_ratio renamed to
#  feasibility_ratio_pct to resolve the name clash with PR-F's above)
# ─────────────────────────────────────────────────────────────────────────────
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

    sigma_N = IV_annualized * sqrt(N / 252).
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


def feasibility_ratio_pct(
    entry: object,
    target: object,
    em_pct_over_horizon: object,
    underlying_price: object = None,
) -> Tuple[Optional[float], str]:
    """|target − entry| / (S × em_pct). Ratio > 1 means the target sits
    outside the priced expected move for the horizon.

    PR-G percent-based variant (tuple output). Named `_pct` to disambiguate
    from the canonical PR-F `feasibility_ratio` (absolute-move input,
    ExpectedMoveResult output). If underlying_price is None, entry is used as
    the spot proxy.
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
