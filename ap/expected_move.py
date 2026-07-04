from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Iterable

TRADING_DAYS_PER_YEAR = 252.0
DEFAULT_TRADING_HOURS_PER_DAY = 6.5


@dataclass(frozen=True)
class ExpectedMoveResult:
    value: float | None
    reason: str


@dataclass(frozen=True)
class AtmIvResult:
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
