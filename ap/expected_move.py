from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Iterable

TRADING_DAYS_PER_YEAR = 252.0
DEFAULT_TRADING_HOURS_PER_DAY = 6.5


@dataclass(frozen=True)
class ExpectedMoveResult:
    """Structured result for expected-move calculations.

    `value` is the dollar expected move when available. `reason` is `"ok"` for
    usable results and a stable unavailable reason otherwise.
    """

    value: float | None
    reason: str


@dataclass(frozen=True)
class AtmIvResult:
    """Structured ATM IV extraction result.

    quality values:
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

    The function picks the closest strike separately for calls and puts, averages
    usable IV from both legs when available, and falls back to one leg with
    `quality="single_leg"`. Malformed chain rows are ignored.
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
    ivs = [iv for iv in (_row_iv(leg) for leg in legs) if iv is not None]
    if not ivs:
        quality = "stale" if legs and not live_legs else "unavailable"
        reason = "stale_chain" if quality == "stale" else "missing_iv"
        return AtmIvResult(None, quality, reason)

    if not live_legs:
        return AtmIvResult(None, "stale", "stale_chain")

    quality = "ok" if len(ivs) == 2 and not missing_sides else "single_leg"
    reason = "ok" if quality == "ok" else "missing_" + "_".join(missing_sides or ["leg"])
    return AtmIvResult(sum(ivs) / len(ivs), quality, reason)


def feasibility_ratio(entry: Any, target: Any, expected_move: Any) -> ExpectedMoveResult:
    """Return `abs(target - entry) / expected_move`.

    The ratio describes how many expected moves are required to travel from
    entry to target. Missing entry/target or non-positive expected move returns
    a structured unavailable reason.
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
