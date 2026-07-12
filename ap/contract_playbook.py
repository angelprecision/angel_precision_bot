from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional
from zoneinfo import ZoneInfo


ET = ZoneInfo("America/New_York")


def _env_flag(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}


def playbook_contract_selection_enabled() -> bool:
    return (
        _env_flag("PLAYBOOK_CONTRACT_SELECTION_ENABLED", "0")
        or _env_flag("PLAYBOOK_STRIKE_SELECTION_ENABLED", "0")
    )


def _configured_liquid_index_etfs() -> set[str]:
    raw = os.getenv("PLAYBOOK_LIQUID_INDEX_ETFS", "SPY,QQQ,IWM,DIA")
    return {part.strip().upper() for part in raw.split(",") if part.strip()}


def classify_instrument_class(ticker: str) -> str:
    symbol = str(ticker or "").strip().upper()
    if symbol in _configured_liquid_index_etfs():
        return "LIQUID_INDEX_ETF"
    if symbol in {"SPY", "QQQ", "IWM", "DIA", "GLD", "TLT", "XLF", "XLE", "XLK", "XLV", "XLC"}:
        return "OTHER_ETF"
    return "ORDINARY_EQUITY"


def _safe_float(value) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_date(value: str) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except Exception:
        return None


def _dte_map(available_expirations: list[str], today: date) -> list[tuple[int, str]]:
    pairs: list[tuple[int, str]] = []
    for expiration in available_expirations:
        exp_date = _safe_date(expiration)
        if exp_date is None:
            continue
        pairs.append(((exp_date - today).days, expiration))
    pairs.sort()
    return pairs


def _within_window(available_expirations: list[str], today: date, min_dte: int, max_dte: int) -> list[tuple[int, str]]:
    return [(dte, exp) for dte, exp in _dte_map(available_expirations, today) if min_dte <= dte <= max_dte]


def _equity_preferred_max_dte() -> int:
    try:
        return int(str(os.getenv("PLAYBOOK_EQUITY_PREFERRED_MAX_DTE", "5")).strip())
    except (TypeError, ValueError):
        return 5


def _equity_fallback_max_dte() -> int:
    try:
        return int(str(os.getenv("PLAYBOOK_EQUITY_FALLBACK_MAX_DTE", "14")).strip())
    except (TypeError, ValueError):
        return 14


def _index_short_max_dte() -> int:
    try:
        return int(str(os.getenv("PLAYBOOK_INDEX_MAX_SHORT_DTE", "2")).strip())
    except (TypeError, ValueError):
        return 2


def _cutoff_hour_minute() -> tuple[int, int]:
    raw = str(os.getenv("PLAYBOOK_0DTE_ENTRY_CUTOFF_ET", "13:30")).strip()
    try:
        hour, minute = raw.split(":", 1)
        return int(hour), int(minute)
    except Exception:
        return 13, 30


def _before_index_cutoff(now_et: datetime | None) -> bool:
    if now_et is None:
        return True
    if now_et.tzinfo is not None:
        now_et = now_et.astimezone(ET)
    hour, minute = _cutoff_hour_minute()
    return (now_et.hour, now_et.minute) < (hour, minute)


@dataclass(frozen=True)
class ContractPlaybookSpec:
    instrument_class: str
    timeframe: str
    side: str
    preferred_expirations: list[str] = field(default_factory=list)
    permitted_expirations: list[str] = field(default_factory=list)
    preferred_dte_order: list[int] = field(default_factory=list)
    strike_policy: str = "ATM_OR_ONE_STEP_OTM"
    preferred_strikes: list[float] = field(default_factory=list)
    strike_band_low: float | None = None
    strike_band_high: float | None = None
    target_underlying: float | None = None
    trigger_price: float | None = None
    underlying_price: float = 0.0
    fallback_policy: str = "NEXT_APPROVED_EXPIRATION"
    policy_reason: str = ""
    diagnostics: dict = field(default_factory=dict)


def resolve_contract_playbook(
    *,
    ticker: str,
    side: str,
    timeframe: str,
    pattern: str | None,
    underlying_price: float,
    trigger_price: float | None,
    target_underlying: float | None,
    wick_targets: list,
    available_expirations: list[str],
    today: date,
    min_dte: int,
    max_dte: int,
    metadata: dict,
    now_et: datetime | None = None,
) -> ContractPlaybookSpec:
    instrument_class = classify_instrument_class(ticker)
    side_norm = str(side or "").upper()
    windowed = _within_window(available_expirations, today, min_dte, max_dte)
    diagnostics: dict = {
        "ticker": str(ticker or "").upper(),
        "pattern": pattern,
        "available_expirations": list(available_expirations),
        "windowed_expirations": [exp for _, exp in windowed],
        "min_dte": int(min_dte),
        "max_dte": int(max_dte),
        "flag_enabled": playbook_contract_selection_enabled(),
        "index_0dte_allowed": _env_flag("PLAYBOOK_INDEX_ALLOW_0DTE", "1"),
    }
    preferred: list[str] = []
    preferred_dtes: list[int] = []
    policy_reason = "no_windowed_expirations"

    if instrument_class == "LIQUID_INDEX_ETF":
        allow_0dte = _env_flag("PLAYBOOK_INDEX_ALLOW_0DTE", "1")
        before_cutoff = _before_index_cutoff(now_et)
        short_max = max(0, _index_short_max_dte())
        diagnostics["cutoff_allows_0dte"] = before_cutoff
        diagnostics["cutoff_time_et"] = "%02d:%02d" % _cutoff_hour_minute()
        permitted_windowed: list[tuple[int, str]] = []
        for dte, exp in windowed:
            if dte == 0 and (not allow_0dte or not before_cutoff):
                continue
            permitted_windowed.append((dte, exp))
            if dte <= short_max:
                preferred.append(exp)
                preferred_dtes.append(dte)
        for dte, exp in permitted_windowed:
            if exp not in preferred:
                preferred.append(exp)
                preferred_dtes.append(dte)
        policy_reason = "liquid_index_short_duration_preference"
    else:
        prefer_wed = _env_flag("PLAYBOOK_PREFER_WEDNESDAY", "1")
        prefer_fri = _env_flag("PLAYBOOK_PREFER_FRIDAY", "1")
        preferred_cap = min(max_dte, _equity_preferred_max_dte())
        fallback_cap = min(max_dte, _equity_fallback_max_dte())
        same_week: list[tuple[int, str]] = []
        fallback: list[tuple[int, str]] = []
        for dte, exp in windowed:
            exp_date = _safe_date(exp)
            if exp_date is None:
                continue
            bucket = same_week if dte <= preferred_cap else fallback
            bucket.append((dte, exp))
        weekday_priority = {2: 0 if prefer_wed else 9, 4: 1 if prefer_fri else 9}
        same_week.sort(key=lambda item: (weekday_priority.get(_safe_date(item[1]).weekday(), 5), item[0], item[1]))
        fallback = [item for item in fallback if item[0] <= fallback_cap]
        preferred = [exp for _, exp in same_week] + [exp for _, exp in fallback]
        preferred_dtes = [dte for dte, _ in same_week] + [dte for dte, _ in fallback]
        policy_reason = "ordinary_equity_same_week_preference"

    target = _safe_float(target_underlying)
    trigger = _safe_float(trigger_price)
    reference = _safe_float(underlying_price) or trigger or target or 0.0
    if reference > 0 and target is not None and target > 0:
        band_low = min(reference, target)
        band_high = max(reference, target)
    else:
        band_low = reference or None
        band_high = reference or None

    diagnostics["resolved_preferred_expirations"] = list(preferred)
    diagnostics["resolved_preferred_dtes"] = list(preferred_dtes)
    diagnostics["wick_target_count"] = len(wick_targets or [])

    return ContractPlaybookSpec(
        instrument_class=instrument_class,
        timeframe=str(timeframe or ""),
        side=side_norm,
        preferred_expirations=list(preferred),
        permitted_expirations=[exp for _, exp in windowed],
        preferred_dte_order=list(preferred_dtes),
        strike_policy="ATM_OR_ONE_STEP_OTM_TOWARD_TARGET",
        preferred_strikes=[],
        strike_band_low=band_low,
        strike_band_high=band_high,
        target_underlying=target,
        trigger_price=trigger,
        underlying_price=float(reference or 0.0),
        fallback_policy="NEXT_APPROVED_EXPIRATION",
        policy_reason=policy_reason,
        diagnostics=diagnostics,
    )


def resolve_playbook_expiration_order(
    spec: ContractPlaybookSpec,
    available_expirations: list[str],
    *,
    today: date,
    min_dte: int,
    max_dte: int,
) -> list[str]:
    windowed = {exp for _, exp in _within_window(available_expirations, today, min_dte, max_dte)}
    ordered: list[str] = []
    for exp in spec.preferred_expirations:
        if exp in windowed and exp not in ordered:
            ordered.append(exp)
    for exp in available_expirations:
        if exp in windowed and exp not in ordered:
            ordered.append(exp)
    return ordered


def build_playbook_candidate_context(
    spec: ContractPlaybookSpec,
    candidates: list[dict],
) -> dict:
    strikes = sorted(
        {
            float(opt.get("strike"))
            for opt in candidates
            if _safe_float(opt.get("strike")) is not None
        }
    )
    if not strikes:
        return {
            "atm_strike": None,
            "preferred_strikes": [],
            "strike_band_low": spec.strike_band_low,
            "strike_band_high": spec.strike_band_high,
            "per_symbol": {},
        }
    underlying = float(spec.underlying_price or 0.0)
    target = _safe_float(spec.target_underlying)
    atm = min(strikes, key=lambda strike: abs(strike - underlying)) if underlying > 0 else strikes[0]
    if spec.side == "PUT":
        toward_target = sorted((strike for strike in strikes if strike < atm), reverse=True)
    else:
        toward_target = sorted(strike for strike in strikes if strike > atm)
    next_step = toward_target[0] if toward_target else atm
    preferred = [atm]
    if next_step != atm:
        preferred.append(next_step)
    band_low = spec.strike_band_low if spec.strike_band_low is not None else min(preferred)
    band_high = spec.strike_band_high if spec.strike_band_high is not None else max(preferred)
    if target is not None:
        band_low = min(float(band_low), target)
        band_high = max(float(band_high), target)
    per_symbol: dict[str, dict] = {}
    for opt in candidates:
        symbol = str(opt.get("symbol") or "")
        strike = _safe_float(opt.get("strike"))
        if strike is None:
            per_symbol[symbol] = {
                "strike_match": False,
                "bounded_bonus": 0.0,
                "distance_to_preferred": None,
            }
            continue
        strike_match = any(abs(strike - pref) < 1e-9 for pref in preferred)
        in_band = float(band_low) <= strike <= float(band_high)
        beyond_target = (
            target is not None
            and ((spec.side == "CALL" and strike > target) or (spec.side == "PUT" and strike < target))
        )
        distance = min(abs(strike - pref) for pref in preferred) if preferred else None
        bonus = 0.0
        if strike_match:
            bonus += 8.0
        elif in_band and not beyond_target:
            bonus += 4.0
        if beyond_target:
            bonus -= 3.0
        per_symbol[symbol] = {
            "strike_match": strike_match,
            "bounded_bonus": bonus,
            "distance_to_preferred": distance,
        }
    return {
        "atm_strike": atm,
        "preferred_strikes": preferred,
        "strike_band_low": band_low,
        "strike_band_high": band_high,
        "per_symbol": per_symbol,
    }
