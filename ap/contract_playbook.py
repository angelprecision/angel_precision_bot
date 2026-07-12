from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional
from zoneinfo import ZoneInfo


ET = ZoneInfo("America/New_York")

TIMEFRAME_INTRADAY_SHORT = "INTRADAY_SHORT"
TIMEFRAME_INTRADAY_HOURLY = "INTRADAY_HOURLY"
TIMEFRAME_DAILY = "DAILY"
TIMEFRAME_OVERNIGHT = "OVERNIGHT"
TIMEFRAME_WEEKLY = "WEEKLY"
TIMEFRAME_UNKNOWN = "UNKNOWN"

STRIKE_POLICY_TIER_ATM = 0
STRIKE_POLICY_TIER_ONE_STEP_OTM = 1
STRIKE_POLICY_TIER_NONMATCH = 2

STRIKE_POLICY_LABEL_ATM = "ATM"
STRIKE_POLICY_LABEL_ONE_STEP_OTM = "ONE_STEP_OTM"
STRIKE_POLICY_LABEL_NONMATCH = "NONMATCH"


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


def normalize_playbook_timeframe(timeframe: str | None) -> str:
    raw = str(timeframe or "").strip().lower()
    if raw in {"1", "1m", "3", "3m", "5", "5m", "15", "15m", "intraday", "intraday_short"}:
        return TIMEFRAME_INTRADAY_SHORT
    if raw in {"30", "30m", "60", "60m", "1h", "hourly", "intraday_hourly"}:
        return TIMEFRAME_INTRADAY_HOURLY
    if raw in {"d", "1d", "day", "daily"}:
        return TIMEFRAME_DAILY
    if raw in {"overnight", "swing", "next_day"}:
        return TIMEFRAME_OVERNIGHT
    if raw in {"w", "1w", "week", "weekly"}:
        return TIMEFRAME_WEEKLY
    return TIMEFRAME_UNKNOWN


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


def _equity_weekly_max_dte() -> int:
    try:
        return int(str(os.getenv("PLAYBOOK_EQUITY_WEEKLY_MAX_DTE", "21")).strip())
    except (TypeError, ValueError):
        return 21


def _index_short_max_dte() -> int:
    try:
        return int(str(os.getenv("PLAYBOOK_INDEX_MAX_SHORT_DTE", "2")).strip())
    except (TypeError, ValueError):
        return 2


def _index_weekly_min_dte() -> int:
    try:
        return int(str(os.getenv("PLAYBOOK_INDEX_WEEKLY_MIN_DTE", "5")).strip())
    except (TypeError, ValueError):
        return 5


def _index_weekly_max_dte() -> int:
    try:
        return int(str(os.getenv("PLAYBOOK_INDEX_WEEKLY_MAX_DTE", "21")).strip())
    except (TypeError, ValueError):
        return 21


def _index_daily_allow_0dte() -> bool:
    return _env_flag("PLAYBOOK_INDEX_DAILY_ALLOW_0DTE", "0")


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


def _same_calendar_week(today: date, expiration: date) -> bool:
    today_iso = today.isocalendar()
    exp_iso = expiration.isocalendar()
    return today_iso.year == exp_iso.year and today_iso.week == exp_iso.week


def _order_by_dte_sequence(
    permitted_windowed: list[tuple[int, str]],
    preferred_dtes: list[int],
) -> tuple[list[str], list[int]]:
    preferred: list[str] = []
    preferred_order: list[int] = []
    for preferred_dte in preferred_dtes:
        for dte, exp in permitted_windowed:
            if dte == preferred_dte and exp not in preferred:
                preferred.append(exp)
                preferred_order.append(dte)
    for dte, exp in permitted_windowed:
        if exp not in preferred:
            preferred.append(exp)
            preferred_order.append(dte)
    return preferred, preferred_order


def _order_equity_daily(
    permitted_windowed: list[tuple[int, str]],
    *,
    today: date,
    preferred_cap: int,
) -> tuple[list[str], list[int]]:
    preferred_rows: list[tuple[int, int, str]] = []
    fallback_rows: list[tuple[int, int, str]] = []
    for dte, exp in permitted_windowed:
        exp_date = _safe_date(exp)
        if exp_date is None:
            continue
        weekday_priority = 4
        if _same_calendar_week(today, exp_date) and dte <= preferred_cap:
            if exp_date.weekday() == 2:
                weekday_priority = 0
            elif exp_date.weekday() == 4:
                weekday_priority = 1
            else:
                weekday_priority = 2
            preferred_rows.append((weekday_priority, dte, exp))
        else:
            fallback_rows.append((weekday_priority, dte, exp))
    preferred_rows.sort(key=lambda item: (item[0], item[1], item[2]))
    fallback_rows.sort(key=lambda item: (item[1], item[2]))
    ordered = [exp for _, _, exp in preferred_rows] + [exp for _, _, exp in fallback_rows]
    ordered_dtes = [dte for _, dte, _ in preferred_rows] + [dte for _, dte, _ in fallback_rows]
    return ordered, ordered_dtes


def _order_equity_weekly(
    permitted_windowed: list[tuple[int, str]],
) -> tuple[list[str], list[int]]:
    friday_rows: list[tuple[int, str]] = []
    other_rows: list[tuple[int, str]] = []
    for dte, exp in permitted_windowed:
        exp_date = _safe_date(exp)
        if exp_date is None:
            continue
        if exp_date.weekday() == 4:
            friday_rows.append((dte, exp))
        else:
            other_rows.append((dte, exp))
    friday_rows.sort()
    other_rows.sort()
    ordered = [exp for _, exp in friday_rows] + [exp for _, exp in other_rows]
    ordered_dtes = [dte for dte, _ in friday_rows] + [dte for dte, _ in other_rows]
    return ordered, ordered_dtes


def _resolve_liquid_index_policy(
    windowed: list[tuple[int, str]],
    *,
    timeframe_class: str,
    before_cutoff: bool,
    allow_0dte: bool,
    max_dte: int,
) -> tuple[list[tuple[int, str]], list[str], list[int], str]:
    permitted_windowed: list[tuple[int, str]] = []
    short_max = max(2, _index_short_max_dte())

    if timeframe_class == TIMEFRAME_INTRADAY_SHORT:
        for dte, exp in windowed:
            if dte == 0 and (not allow_0dte or not before_cutoff):
                continue
            permitted_windowed.append((dte, exp))
        preferred_dtes = ([0] if allow_0dte and before_cutoff else []) + [1, 2]
        preferred, preferred_order = _order_by_dte_sequence(permitted_windowed, preferred_dtes)
        return permitted_windowed, preferred, preferred_order, "liquid_index_intraday_short"

    if timeframe_class == TIMEFRAME_INTRADAY_HOURLY:
        for dte, exp in windowed:
            if dte == 0 and (not allow_0dte or not before_cutoff):
                continue
            permitted_windowed.append((dte, exp))
        preferred_dtes = [1, 2] + ([0] if allow_0dte and before_cutoff else [])
        preferred, preferred_order = _order_by_dte_sequence(permitted_windowed, preferred_dtes)
        return permitted_windowed, preferred, preferred_order, "liquid_index_intraday_hourly"

    if timeframe_class in {TIMEFRAME_DAILY, TIMEFRAME_OVERNIGHT}:
        allow_daily_0dte = allow_0dte and before_cutoff and _index_daily_allow_0dte()
        for dte, exp in windowed:
            if dte == 0 and not allow_daily_0dte:
                continue
            permitted_windowed.append((dte, exp))
        preferred_dtes = [1, 2]
        preferred, preferred_order = _order_by_dte_sequence(permitted_windowed, preferred_dtes)
        return permitted_windowed, preferred, preferred_order, "liquid_index_daily_overnight"

    if timeframe_class == TIMEFRAME_WEEKLY:
        weekly_min = max(0, _index_weekly_min_dte())
        weekly_max = min(max_dte, max(weekly_min, _index_weekly_max_dte()))
        permitted_windowed = [(dte, exp) for dte, exp in windowed if weekly_min <= dte <= weekly_max]
        preferred, preferred_order = _order_equity_weekly(permitted_windowed)
        return permitted_windowed, preferred, preferred_order, "liquid_index_weekly"

    return [], [], [], "UNKNOWN_PLAYBOOK_TIMEFRAME"


def _resolve_equity_policy(
    windowed: list[tuple[int, str]],
    *,
    timeframe_class: str,
    today: date,
    max_dte: int,
) -> tuple[list[tuple[int, str]], list[str], list[int], str]:
    preferred_cap = min(max_dte, _equity_preferred_max_dte())
    fallback_cap = min(max_dte, _equity_fallback_max_dte())

    if timeframe_class in {TIMEFRAME_DAILY, TIMEFRAME_OVERNIGHT}:
        permitted_windowed = [(dte, exp) for dte, exp in windowed if dte <= fallback_cap]
        preferred, preferred_order = _order_equity_daily(
            permitted_windowed,
            today=today,
            preferred_cap=preferred_cap,
        )
        return permitted_windowed, preferred, preferred_order, "ordinary_equity_daily_overnight"

    if timeframe_class == TIMEFRAME_WEEKLY:
        weekly_cap = min(max_dte, max(_equity_weekly_max_dte(), fallback_cap + 7))
        permitted_windowed = [(dte, exp) for dte, exp in windowed if dte <= weekly_cap]
        preferred, preferred_order = _order_equity_weekly(permitted_windowed)
        return permitted_windowed, preferred, preferred_order, "ordinary_equity_weekly"

    return [], [], [], "UNKNOWN_PLAYBOOK_TIMEFRAME"


def _resolve_expiration_override(
    override: str | None,
    *,
    available_expirations: list[str],
    permitted_windowed: list[tuple[int, str]],
    today: date,
    min_dte: int,
    max_dte: int,
) -> tuple[str | None, str | None, str | None]:
    if not override:
        return None, None, None
    override_date = _safe_date(override)
    if override_date is None:
        return None, "INVALID_EXPIRATION_OVERRIDE", f"Expiration override {override!r} is not a valid ISO date"
    if override not in available_expirations:
        return None, "EXPIRATION_OVERRIDE_UNAVAILABLE", f"Expiration override {override} is not in the provider expiration list"
    dte = (override_date - today).days
    if dte < min_dte or dte > max_dte:
        return None, "EXPIRATION_OVERRIDE_OUT_OF_RANGE", (
            f"Expiration override {override} has dte={dte} outside hard window [{min_dte},{max_dte}]"
        )
    if override not in {exp for _, exp in permitted_windowed}:
        return None, "EXPIRATION_OVERRIDE_POLICY_BLOCKED", (
            f"Expiration override {override} is blocked by the active playbook safety policy"
        )
    return override, None, None


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
    expiration_override: str | None = None,
) -> ContractPlaybookSpec:
    instrument_class = classify_instrument_class(ticker)
    timeframe_class = normalize_playbook_timeframe(timeframe)
    side_norm = str(side or "").upper()
    windowed = _within_window(available_expirations, today, min_dte, max_dte)
    if now_et is not None and now_et.tzinfo is not None:
        now_et = now_et.astimezone(ET)
    diagnostics: dict = {
        "ticker": str(ticker or "").upper(),
        "pattern": pattern,
        "available_expirations": list(available_expirations),
        "windowed_expirations": [exp for _, exp in windowed],
        "min_dte": int(min_dte),
        "max_dte": int(max_dte),
        "flag_enabled": playbook_contract_selection_enabled(),
        "instrument_class": instrument_class,
        "timeframe_input": str(timeframe or ""),
        "timeframe_class": timeframe_class,
        "transaction_date_et": today.isoformat(),
        "transaction_now_et": now_et.isoformat() if now_et is not None else None,
        "expiration_override": expiration_override,
        "index_0dte_allowed": _env_flag("PLAYBOOK_INDEX_ALLOW_0DTE", "1"),
    }
    preferred: list[str] = []
    preferred_dtes: list[int] = []
    permitted_windowed = list(windowed)
    policy_reason = "PLAYBOOK_NO_POLICY_MATCH"
    before_cutoff = _before_index_cutoff(now_et)
    diagnostics["cutoff_allows_0dte"] = before_cutoff
    diagnostics["cutoff_time_et"] = "%02d:%02d" % _cutoff_hour_minute()

    if timeframe_class == TIMEFRAME_UNKNOWN:
        diagnostics["error_reason"] = "UNKNOWN_PLAYBOOK_TIMEFRAME"
        diagnostics["error_explanation"] = f"Unsupported playbook timeframe {timeframe!r}"
        permitted_windowed = []
    elif instrument_class == "LIQUID_INDEX_ETF":
        permitted_windowed, preferred, preferred_dtes, policy_reason = _resolve_liquid_index_policy(
            windowed,
            timeframe_class=timeframe_class,
            before_cutoff=before_cutoff,
            allow_0dte=_env_flag("PLAYBOOK_INDEX_ALLOW_0DTE", "1"),
            max_dte=max_dte,
        )
    else:
        permitted_windowed, preferred, preferred_dtes, policy_reason = _resolve_equity_policy(
            windowed,
            timeframe_class=timeframe_class,
            today=today,
            max_dte=max_dte,
        )

    override_value, override_error, override_explanation = _resolve_expiration_override(
        expiration_override,
        available_expirations=available_expirations,
        permitted_windowed=permitted_windowed,
        today=today,
        min_dte=min_dte,
        max_dte=max_dte,
    )
    if override_error is not None:
        diagnostics["error_reason"] = override_error
        diagnostics["error_explanation"] = override_explanation
        permitted_windowed = []
        preferred = []
        preferred_dtes = []
        policy_reason = override_error
    elif override_value is not None:
        preferred = [override_value] + [exp for exp in preferred if exp != override_value]
        preferred_dtes = [
            (date.fromisoformat(exp) - today).days
            for exp in preferred
            if _safe_date(exp) is not None
        ]
        diagnostics["override_applied"] = True
        diagnostics["override_selected"] = override_value
        policy_reason = "EXPIRATION_OVERRIDE_APPLIED"
    else:
        diagnostics["override_applied"] = False

    target = _safe_float(target_underlying)
    trigger = _safe_float(trigger_price)
    reference = _safe_float(underlying_price) or trigger or target or 0.0
    if reference > 0:
        band_low = reference
        band_high = reference
        if target is not None and target > 0:
            band_low = min(reference, target)
            band_high = max(reference, target)
    else:
        band_low = None
        band_high = None

    diagnostics["resolved_preferred_expirations"] = list(preferred)
    diagnostics["resolved_permitted_expirations"] = [exp for _, exp in permitted_windowed]
    diagnostics["resolved_preferred_dtes"] = list(preferred_dtes)
    diagnostics["wick_target_count"] = len(wick_targets or [])

    return ContractPlaybookSpec(
        instrument_class=instrument_class,
        timeframe=timeframe_class,
        side=side_norm,
        preferred_expirations=list(preferred),
        permitted_expirations=[exp for _, exp in permitted_windowed],
        preferred_dte_order=list(preferred_dtes),
        strike_policy="LEXICOGRAPHIC_ATM_OR_ONE_STEP_OTM_TOWARD_TARGET",
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
    del today, min_dte, max_dte
    permitted = set(spec.permitted_expirations)
    ordered: list[str] = []
    for exp in spec.preferred_expirations:
        if exp in permitted and exp not in ordered:
            ordered.append(exp)
    for exp in available_expirations:
        if exp in permitted and exp not in ordered:
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
            "one_step_otm_strike": None,
            "preferred_strikes": [],
            "strike_band_low": spec.strike_band_low,
            "strike_band_high": spec.strike_band_high,
            "per_symbol": {},
        }
    underlying = float(spec.underlying_price or 0.0)
    target = _safe_float(spec.target_underlying)
    if underlying > 0:
        atm = min(strikes, key=lambda strike: (abs(strike - underlying), strike))
    else:
        atm = strikes[0]
    if spec.side == "PUT":
        otm_candidates = sorted((strike for strike in strikes if strike < atm), reverse=True)
    else:
        otm_candidates = sorted(strike for strike in strikes if strike > atm)
    next_step = otm_candidates[0] if otm_candidates else None
    preferred = [atm]
    if next_step is not None:
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
                "strike_policy_tier": STRIKE_POLICY_TIER_NONMATCH,
                "strike_policy_label": STRIKE_POLICY_LABEL_NONMATCH,
                "strike_policy_match": False,
                "distance_to_preferred": None,
            }
            continue
        distance = min(abs(strike - pref) for pref in preferred) if preferred else None
        if abs(strike - atm) < 1e-9:
            tier = STRIKE_POLICY_TIER_ATM
            label = STRIKE_POLICY_LABEL_ATM
        elif next_step is not None and abs(strike - next_step) < 1e-9:
            tier = STRIKE_POLICY_TIER_ONE_STEP_OTM
            label = STRIKE_POLICY_LABEL_ONE_STEP_OTM
        else:
            tier = STRIKE_POLICY_TIER_NONMATCH
            label = STRIKE_POLICY_LABEL_NONMATCH
        per_symbol[symbol] = {
            "strike_policy_tier": tier,
            "strike_policy_label": label,
            "strike_policy_match": tier != STRIKE_POLICY_TIER_NONMATCH,
            "distance_to_preferred": 0.0 if tier != STRIKE_POLICY_TIER_NONMATCH else distance,
        }
    return {
        "atm_strike": atm,
        "one_step_otm_strike": next_step,
        "preferred_strikes": preferred,
        "strike_band_low": band_low,
        "strike_band_high": band_high,
        "per_symbol": per_symbol,
    }
