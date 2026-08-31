from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from datetime import date, datetime, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from ap.parse_utils import first_float, first_present, safe_float, safe_int, safe_str

log = logging.getLogger("ap.trade_dossier")

SCHEMA_VERSION = os.getenv("TRADE_DOSSIER_SCHEMA_VERSION", "v1")
INTELLIGENCE_REVIEW_VERSION = "trade_dossier_intelligence_review_v1"
ENABLE_TRADE_DOSSIER = os.getenv("ENABLE_TRADE_DOSSIER", "true").lower() == "true"
TRADE_DOSSIER_REPORT_ONLY = os.getenv("TRADE_DOSSIER_REPORT_ONLY", "true").lower() == "true"
MAX_DOSSIER_JSON_BYTES = int(os.getenv("TRADE_DOSSIER_MAX_JSON_BYTES", "120000"))
ET = ZoneInfo("America/New_York")

STATUSES = {"BUILDING", "READY", "INCOMPLETE", "INVALID", "DECIDED", "ENTERED", "CLOSED", "UNAVAILABLE"}
REQUIRED_COLUMNS = {
    "dossier_id", "signal_id", "canonical_signal_id", "client_id", "execution_mode",
    "trade_date", "ticker", "direction", "strategy", "timeframe", "dossier_status",
    "case_quality_score", "case_grade", "decision", "decision_reason",
    "primary_strength", "primary_risk", "dossier", "created_at", "updated_at",
    "schema_version", "git_commit", "config_hash",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _today() -> str:
    return datetime.now(ET).date().isoformat()


def _git_commit() -> str:
    try:
        from ap.observability import get_git_commit
        return get_git_commit()
    except Exception:
        return os.getenv("GIT_COMMIT", "unknown")


def _config_hash() -> str:
    return os.getenv("AP_CONFIG_HASH", "unknown")


def _canonical_signal_id(signal: Mapping[str, Any], signal_id: str) -> str:
    explicit = safe_str(signal.get("canonical_signal_id"), None)
    if explicit:
        return explicit
    try:
        from ap_canonical_signal import build_canonical_signal_id
        return build_canonical_signal_id(signal_id, signal) or signal_id
    except Exception:
        return signal_id


def _compact_signal(signal: Mapping[str, Any]) -> dict[str, Any]:
    keep = (
        "signal_id", "canonical_signal_id", "ticker", "symbol", "side", "direction",
        "strategy", "pattern", "pattern_id", "timeframe", "score", "tier",
        "entry", "entry_price", "trigger", "trigger_price", "underlying_entry",
        "stop", "stop_price", "target", "target_price", "pt1", "expiration",
        "dte", "delta", "iv", "bid", "ask", "mid", "open_interest", "volume",
    )
    out = {}
    for key in keep:
        value = signal.get(key)
        if value not in (None, ""):
            out[key] = value
    return out


def _direction_is_put(direction: str | None) -> bool:
    return str(direction or "").upper() in {"PUT", "BEARISH", "SHORT"}


def _levels(signal: Mapping[str, Any], direction: str | None) -> dict[str, Any]:
    entry = first_float(signal, ("entry", "entry_price", "trigger", "trigger_price", "underlying_entry"))
    stop = first_float(signal, ("stop", "stop_price", "trigger.stop"))
    target = first_float(signal, ("target", "target_price", "pt1", "trigger.pt1"))
    missing = []
    if entry is None:
        missing.append("entry")
    if stop is None:
        missing.append("stop")
    if target is None:
        missing.append("target")

    risk = reward = rr = None
    quality = "OK"
    if missing:
        quality = "UNAVAILABLE"
    else:
        if _direction_is_put(direction):
            risk = stop - entry
            reward = entry - target
        else:
            risk = entry - stop
            reward = target - entry
        if risk is None or risk <= 0:
            quality = "INVALID"
        elif reward is None or reward <= 0:
            quality = "INVALID"
        else:
            rr = reward / risk
            if rr < 1.5:
                quality = "WEAK"
    return {
        "entry": entry,
        "trigger": first_float(signal, ("trigger_price", "trigger.entry", "entry")),
        "stop": stop,
        "target": target,
        "risk_per_share": risk,
        "reward_per_share": reward,
        "planned_rr": rr,
        "level_quality": quality,
        "missing_fields": missing,
    }


def _daily_structure(signal: Mapping[str, Any], ctx: Mapping[str, Any], levels: dict, direction: str | None) -> dict[str, Any]:
    merged = {**dict(ctx or {}), **dict(signal or {})}
    current = first_float(merged, ("current_price", "underlying_price", "price", "last"))
    target = levels.get("target")
    stop = levels.get("stop")
    reasons = []
    room = distance_target = distance_stop = None
    if current and target:
        room = abs(target - current)
        distance_target = room / current
    if current and stop:
        distance_stop = abs(current - stop) / current
    target_too_close = distance_target is not None and distance_target < 0.003
    if target_too_close:
        reasons.append("target_too_close")
    verdict = "STRUCTURE_UNAVAILABLE"
    if levels.get("level_quality") == "INVALID":
        verdict = "STRUCTURE_INVALID"
        reasons.append("invalid_levels")
    elif levels.get("level_quality") in {"OK", "WEAK"}:
        verdict = "STRUCTURE_WEAK" if target_too_close or levels.get("level_quality") == "WEAK" else "STRUCTURE_GOOD"
    return {
        "previous_day_high": first_float(merged, ("previous_day_high", "pdh")),
        "previous_day_low": first_float(merged, ("previous_day_low", "pdl")),
        "previous_day_close": first_float(merged, ("previous_day_close", "pdc")),
        "current_price": current,
        "distance_to_target_pct": distance_target,
        "distance_to_stop_pct": distance_stop,
        "room_to_target": room,
        "already_extended": bool(first_present(merged, ("already_extended",), False)),
        "target_too_close": target_too_close,
        "structure_verdict": verdict,
        "structure_reasons": reasons,
    }


def _trend_value(raw) -> str:
    s = str(raw or "").upper()
    if s in {"UP", "UPTREND", "BULL", "BULLISH"}:
        return "UP"
    if s in {"DOWN", "DOWNTREND", "BEAR", "BEARISH"}:
        return "DOWN"
    if s in {"MIXED", "CHOP"}:
        return "MIXED"
    return "UNAVAILABLE"


def _alignment(direction: str | None, trend: str) -> str:
    if trend == "UNAVAILABLE":
        return "UNAVAILABLE"
    bullish = not _direction_is_put(direction)
    if (bullish and trend == "UP") or ((not bullish) and trend == "DOWN"):
        return "ALIGNED"
    if trend == "MIXED":
        return "MIXED"
    return "AGAINST"


def _trend_profile(signal: Mapping[str, Any], ctx: Mapping[str, Any], direction: str | None) -> dict[str, Any]:
    merged = {**dict(ctx or {}), **dict(signal or {})}
    daily = _trend_value(first_present(merged, ("daily_trend", "trend", "trend_direction")))
    weekly = _trend_value(first_present(merged, ("weekly_trend",)))
    monthly = _trend_value(first_present(merged, ("monthly_trend",)))
    return {
        "daily_trend": daily,
        "weekly_trend": weekly,
        "monthly_trend": monthly,
        "strat_continuity": first_present(merged, ("strat_continuity",), None),
        "ema_alignment": first_present(merged, ("ema_alignment",), None),
        "trend_alignment": _alignment(direction, daily),
        "trend_reasons": [] if daily != "UNAVAILABLE" else ["daily_trend_unavailable"],
    }


def _market_context(signal: Mapping[str, Any], ctx: Mapping[str, Any], direction: str | None) -> dict[str, Any]:
    merged = {**dict(ctx or {}), **dict(signal or {})}
    spy = _trend_value(first_present(merged, ("spy_trend", "market.spy_trend")))
    qqq = _trend_value(first_present(merged, ("qqq_trend", "market.qqq_trend")))
    aligned = [_alignment(direction, t) for t in (spy, qqq) if t != "UNAVAILABLE"]
    if not aligned:
        idx_align = "UNAVAILABLE"
    elif all(a == "ALIGNED" for a in aligned):
        idx_align = "ALIGNED"
    elif any(a == "AGAINST" for a in aligned):
        idx_align = "AGAINST"
    else:
        idx_align = "MIXED"
    return {
        "spy_trend": spy,
        "qqq_trend": qqq,
        "vix_level": first_float(merged, ("vix_level", "vix")),
        "regime": safe_str(first_present(merged, ("regime", "market_regime"), "UNAVAILABLE"), "UNAVAILABLE"),
        "index_alignment": idx_align,
        "market_reasons": [] if idx_align != "UNAVAILABLE" else ["index_context_unavailable"],
    }


def _sector_profile(signal: Mapping[str, Any], ctx: Mapping[str, Any], direction: str | None) -> dict[str, Any]:
    merged = {**dict(ctx or {}), **dict(signal or {})}
    trend = _trend_value(first_present(merged, ("sector_trend",)))
    return {
        "sector": first_present(merged, ("sector",), None),
        "sector_etf": first_present(merged, ("sector_etf",), None),
        "sector_trend": trend,
        "relative_strength": first_present(merged, ("relative_strength",), None),
        "sector_alignment": _alignment(direction, trend),
        "sector_reasons": [] if trend != "UNAVAILABLE" else ["sector_trend_unavailable"],
    }


def _options_profile(signal: Mapping[str, Any], ctx: Mapping[str, Any], levels: dict) -> dict[str, Any]:
    merged = {**dict(ctx or {}), **dict(signal or {})}
    option_symbol = first_present(merged, ("option_symbol", "contract", "selected_contract"))
    bid = first_float(merged, ("bid", "option_bid"))
    ask = first_float(merged, ("ask", "option_ask"))
    mid = first_float(merged, ("mid", "option_mid"))
    if mid is None and bid and ask:
        mid = (bid + ask) / 2.0
    spread_pct = ((ask - bid) / mid) if bid and ask and mid and mid > 0 else None
    reasons = []
    readiness = "UNAVAILABLE"
    if not option_symbol:
        reasons.append("contract_not_selected")
    else:
        oi = safe_int(first_present(merged, ("open_interest", "oi")), None)
        vol = safe_int(first_present(merged, ("volume", "vol")), None)
        if spread_pct is not None and spread_pct > 0.20:
            readiness = "CONTRACT_RISKY"
            reasons.append("wide_spread")
        elif oi is not None and oi < 50:
            readiness = "CONTRACT_RISKY"
            reasons.append("low_open_interest")
        else:
            readiness = "CONTRACT_READY"
    return {
        "option_symbol": option_symbol,
        "expiration": first_present(merged, ("expiration", "expiration_date"), None),
        "dte": safe_int(first_present(merged, ("dte",)), None),
        "delta": first_float(merged, ("delta",)),
        "iv": first_float(merged, ("iv", "atm_iv")),
        "iv_rank": first_float(merged, ("iv_rank",)),
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread_pct": spread_pct,
        "open_interest": safe_int(first_present(merged, ("open_interest", "oi")), None),
        "volume": safe_int(first_present(merged, ("volume", "vol")), None),
        "expected_move_1d": first_float(merged, ("expected_move_1d",)),
        "expected_move_to_target_ratio": None,
        "contract_readiness": readiness,
        "options_reasons": reasons,
    }


def _historical_profile(signal: Mapping[str, Any], ctx: Mapping[str, Any]) -> dict[str, Any]:
    merged = {**dict(ctx or {}), **dict(signal or {})}
    n = safe_int(first_present(merged, ("same_ticker_pattern_n", "sample_size", "historical_sample_size")), None)
    if n is None:
        confidence = "UNAVAILABLE"
    elif n <= 0:
        confidence = "NO_HISTORY"
    elif n < 20:
        confidence = "LOW_N"
    elif n < 75:
        confidence = "MEDIUM_N"
    else:
        confidence = "HIGH_N"
    return {
        "same_ticker_pattern_n": n,
        "same_pattern_all_tickers_n": safe_int(first_present(merged, ("same_pattern_all_tickers_n",)), None),
        "raw_win_rate": first_float(merged, ("raw_win_rate", "win_rate")) if n else None,
        "avg_r": first_float(merged, ("avg_r",)),
        "median_r": first_float(merged, ("median_r",)),
        "mfe_median": first_float(merged, ("mfe_median",)),
        "mae_median": first_float(merged, ("mae_median",)),
        "sample_confidence": confidence,
        "history_reasons": [] if n else ["history_sample_unavailable"],
    }


def _failure_strength(levels: dict, daily: dict, trend: dict, market: dict, sector: dict, options: dict, hist: dict) -> tuple[dict, dict, int]:
    flags = {
        "too_extended": bool(daily.get("already_extended")),
        "target_too_close": bool(daily.get("target_too_close")),
        "stop_too_wide": False,
        "rr_too_low": bool(levels.get("planned_rr") is not None and levels.get("planned_rr") < 1.5),
        "against_index_trend": market.get("index_alignment") == "AGAINST",
        "against_sector_trend": sector.get("sector_alignment") == "AGAINST",
        "low_volume_confirmation": False,
        "gap_risk": False,
        "earnings_event_risk": False,
        "spread_risk": "wide_spread" in options.get("options_reasons", []),
        "contract_liquidity_risk": "low_open_interest" in options.get("options_reasons", []),
        "capital_efficiency_risk": False,
    }
    reasons = [k for k, v in flags.items() if v]
    unavailable = sum(1 for v in (
        daily.get("structure_verdict"), trend.get("trend_alignment"), market.get("index_alignment"),
        sector.get("sector_alignment"), options.get("contract_readiness"), hist.get("sample_confidence")
    ) if str(v).endswith("UNAVAILABLE") or v == "UNAVAILABLE")
    failure_score = min(100, len(reasons) * 12 + unavailable * 5)
    strength_flags = {
        "clean_daily_level": daily.get("structure_verdict") == "STRUCTURE_GOOD",
        "strong_rr": bool(levels.get("planned_rr") is not None and levels.get("planned_rr") >= 2.0),
        "trend_aligned": trend.get("trend_alignment") == "ALIGNED",
        "sector_aligned": sector.get("sector_alignment") == "ALIGNED",
        "market_aligned": market.get("index_alignment") == "ALIGNED",
        "liquid_contract": options.get("contract_readiness") == "CONTRACT_READY",
        "historical_edge_present": hist.get("sample_confidence") in {"MEDIUM_N", "HIGH_N"} and hist.get("raw_win_rate") is not None,
    }
    strengths = [k for k, v in strength_flags.items() if v]
    return (
        {"failure_score": failure_score, "primary_failure_mode": reasons[0] if reasons else None, "failure_flags": flags, "failure_reasons": reasons},
        {"primary_strength": strengths[0] if strengths else None, "strength_flags": strength_flags, "strength_reasons": strengths},
        unavailable,
    )


def _case_score(levels: dict, daily: dict, trend: dict, market: dict, sector: dict, options: dict, hist: dict, failure: dict, unavailable: int) -> dict[str, Any]:
    incomplete = levels.get("level_quality") in {"UNAVAILABLE", "INVALID"} or unavailable >= 4
    components = {
        "structure_quality": 25 if daily.get("structure_verdict") == "STRUCTURE_GOOD" else 10 if daily.get("structure_verdict") == "STRUCTURE_WEAK" else None,
        "trend_alignment": 15 if trend.get("trend_alignment") == "ALIGNED" else 5 if trend.get("trend_alignment") == "MIXED" else 0 if trend.get("trend_alignment") == "AGAINST" else None,
        "sector_alignment": 10 if sector.get("sector_alignment") == "ALIGNED" else 4 if sector.get("sector_alignment") == "MIXED" else 0 if sector.get("sector_alignment") == "AGAINST" else None,
        "risk_reward_quality": 15 if (levels.get("planned_rr") or 0) >= 2 else 7 if (levels.get("planned_rr") or 0) >= 1.5 else 0 if levels.get("planned_rr") is not None else None,
        "options_readiness": 15 if options.get("contract_readiness") == "CONTRACT_READY" else 5 if options.get("contract_readiness") == "CONTRACT_RISKY" else None,
        "historical_confidence": 10 if hist.get("sample_confidence") in {"HIGH_N", "MEDIUM_N"} else 3 if hist.get("sample_confidence") == "LOW_N" else None,
        "failure_penalty": -min(20, int((failure.get("failure_score") or 0) / 5)),
        "data_completeness": max(0, 10 - unavailable * 2),
    }
    if incomplete:
        score = None
        grade = "INCOMPLETE"
    else:
        score = sum(v for v in components.values() if isinstance(v, (int, float)))
        grade = "A" if score >= 80 else "B" if score >= 65 else "C" if score >= 50 else "D"
    return {"case_quality_score": score, "case_grade": grade, "components": components, "score_reasons": []}


def _summary(identity: dict, levels: dict, daily: dict, failure: dict, strength: dict, case: dict) -> dict[str, Any]:
    ticker = identity.get("ticker") or "UNKNOWN"
    direction = identity.get("direction") or "UNKNOWN"
    thesis = f"{ticker} {direction} {identity.get('timeframe') or ''}".strip()
    why_win = list(strength.get("strength_reasons") or [])
    why_fail = list(failure.get("failure_reasons") or [])
    if case.get("case_grade") == "INCOMPLETE":
        verdict = "DATA_INCOMPLETE"
    elif daily.get("structure_verdict") == "STRUCTURE_INVALID":
        verdict = "INVALID_CASE"
    elif case.get("case_grade") in {"A", "B"}:
        verdict = "READY_FOR_OPEN"
    elif case.get("case_grade") == "C":
        verdict = "NEEDS_RECHECK"
    else:
        verdict = "WEAK_CASE"
    return {
        "one_line_thesis": thesis,
        "why_it_can_win": why_win,
        "why_it_can_fail": why_fail,
        "premarket_verdict": verdict,
        "operator_summary": f"{thesis}: grade={case.get('case_grade')} verdict={verdict}",
    }


def _status(case: dict, levels: dict) -> str:
    if levels.get("level_quality") == "INVALID":
        return "INVALID"
    if case.get("case_grade") == "INCOMPLETE":
        return "INCOMPLETE"
    return "READY"


def _enforce_size_cap(dossier_json: dict) -> dict:
    encoded = json.dumps(dossier_json, default=str, separators=(",", ":")).encode()
    if len(encoded) <= MAX_DOSSIER_JSON_BYTES:
        return dossier_json
    dossier_json = dict(dossier_json)
    dossier_json["raw_signal"] = {"unavailable_reason": "trade_dossier_size_cap"}
    dossier_json["size_cap_applied"] = True
    encoded = json.dumps(dossier_json, default=str, separators=(",", ":")).encode()
    if len(encoded) <= MAX_DOSSIER_JSON_BYTES:
        return dossier_json
    identity = dossier_json.get("identity", {})
    return {
        "identity": identity,
        "dossier_status": "UNAVAILABLE",
        "dossier_error": "trade_dossier_size_cap",
        "size_cap_applied": True,
    }


def _minimal(signal: Mapping[str, Any], client_id: str, execution_mode: str, error: str) -> dict:
    signal_id = safe_str(signal.get("signal_id"), str(uuid.uuid4())) if isinstance(signal, Mapping) else str(uuid.uuid4())
    canonical = _canonical_signal_id(signal if isinstance(signal, Mapping) else {}, signal_id)
    ticker = safe_str(first_present(signal, ("ticker", "symbol"), "UNKNOWN"), "UNKNOWN") if isinstance(signal, Mapping) else "UNKNOWN"
    trade_date = _today()
    identity = {
        "signal_id": signal_id, "canonical_signal_id": canonical, "client_id": client_id,
        "execution_mode": execution_mode, "ticker": ticker, "direction": None,
        "strategy": None, "timeframe": None, "trade_date": trade_date,
        "created_at": _now_iso(), "schema_version": SCHEMA_VERSION,
        "git_commit": _git_commit(), "config_hash": _config_hash(),
    }
    return {
        "dossier_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{canonical}:{client_id}:{execution_mode}:{SCHEMA_VERSION}")),
        "signal_id": signal_id,
        "canonical_signal_id": canonical,
        "client_id": client_id,
        "execution_mode": execution_mode,
        "trade_date": trade_date,
        "ticker": ticker,
        "direction": None,
        "strategy": None,
        "timeframe": None,
        "dossier_status": "UNAVAILABLE",
        "case_quality_score": None,
        "case_grade": "INCOMPLETE",
        "decision": None,
        "decision_reason": None,
        "primary_strength": None,
        "primary_risk": "dossier_build_failed",
        "git_commit": identity["git_commit"],
        "config_hash": identity["config_hash"],
        "schema_version": SCHEMA_VERSION,
        "dossier": {"identity": identity, "dossier_error": str(error), "dossier_status": "UNAVAILABLE"},
    }


def _intelligence_score_for_dossier(
    signal: Mapping[str, Any],
    ctx: Mapping[str, Any],
    identity: Mapping[str, Any],
) -> dict[str, Any]:
    """Attach a matching persisted score, or build a fail-closed fallback."""

    from ap.intelligence_score import build_intelligence_score, score_matches_identity

    canonical = str(identity.get("canonical_signal_id") or "")
    client_id = str(identity.get("client_id") or "")
    execution_mode = str(identity.get("execution_mode") or "")
    candidates = [
        ctx.get("intelligence_score"),
        (ctx.get("intelligence_evaluation") or {}).get("intelligence_score")
        if isinstance(ctx.get("intelligence_evaluation"), Mapping) else None,
        (ctx.get("intelligence_context") or {}).get("intelligence_score")
        if isinstance(ctx.get("intelligence_context"), Mapping) else None,
    ]
    for candidate in candidates:
        if score_matches_identity(
            candidate,
            canonical_signal_id=canonical,
            client_id=client_id,
            execution_mode=execution_mode,
            ticker=str(identity.get("ticker") or ""),
            side=identity.get("direction"),
        ):
            return dict(candidate)

    scoring_signal = dict(signal or {})
    scoring_signal["canonical_signal_id"] = canonical
    try:
        from ap.market_context_builder import build_market_context_for_signal
        from ap.position_score_profile import build_position_score_profile

        market_context = build_market_context_for_signal(
            scoring_signal, data_sources=dict(ctx or {})
        )
        profile = build_position_score_profile(scoring_signal, market_context)
    except Exception as exc:
        log.warning("trade_dossier_intelligence_profile_failed: %s", exc)
        profile = {}
    return build_intelligence_score(
        profile,
        signal=scoring_signal,
        client_id=client_id,
        execution_mode=execution_mode,
        scored_at=identity.get("created_at"),
        data_as_of=ctx.get("data_as_of") or signal.get("data_as_of") or identity.get("created_at"),
        source="trade_dossier_fallback",
        git_commit=str(identity.get("git_commit") or ""),
    )


def build_intelligence_review(score: Mapping[str, Any] | None) -> dict[str, Any]:
    """Create an operator-readable review without changing trade behavior."""

    item = score if isinstance(score, Mapping) else {}
    valid = bool(item.get("score_valid"))
    rankable = bool(item.get("eligible_for_ranking"))
    invalid = sorted({str(v) for v in (item.get("invalid_reasons") or []) if v})
    blocks = sorted({str(v) for v in (item.get("block_recommendations") or []) if v})
    unavailable = sorted({str(v) for v in (item.get("unavailable_components") or []) if v})
    warnings = sorted({str(v) for v in (item.get("warnings") or []) if v})

    if not item:
        verdict, primary_risk = "SCORE_MISSING", "canonical_intelligence_score_missing"
    elif not valid:
        verdict = "SCORE_INVALID"
        primary_risk = (invalid or unavailable or ["canonical_intelligence_score_invalid"])[0]
    elif not rankable:
        verdict = "VALID_NOT_RANKABLE"
        primary_risk = (blocks or ["ranking_eligibility_false"])[0]
    else:
        verdict = "RANKABLE"
        primary_risk = (warnings or [None])[0]

    return {
        "review_version": INTELLIGENCE_REVIEW_VERSION,
        "verdict": verdict,
        "score_valid": valid,
        "eligible_for_ranking": rankable,
        "policy_score": item.get("policy_score") if valid else None,
        "policy_version": item.get("policy_version"),
        "score_version": item.get("score_version"),
        "score_config_hash": item.get("score_config_hash"),
        "profile_version": item.get("profile_version"),
        "evidence_coverage": item.get("evidence_coverage"),
        "scored_at": item.get("scored_at"),
        "data_as_of": item.get("data_as_of"),
        "source": item.get("source"),
        "primary_risk": primary_risk,
        "invalid_reasons": invalid,
        "block_recommendations": blocks,
        "unavailable_components": unavailable,
        "warnings": warnings,
        "observe_only": True,
        "affected_eligibility": False,
    }


def _latest_pretrigger_intelligence_score(
    conn: Any,
    *,
    canonical_signal_id: str,
    client_id: str,
    execution_mode: str,
    ticker: str | None = None,
    side: str | None = None,
) -> dict[str, Any] | None:
    """Best-effort read of the already-durable score; never blocks dossier writes."""

    savepoint = "ap_trade_dossier_score_lookup"
    try:
        conn.execute(f"SAVEPOINT {savepoint}")
        conn.execute(
            """
            SELECT payload
            FROM ap_intelligence_snapshots
            WHERE client_id=%s
              AND lower(execution_mode)=lower(%s)
              AND canonical_signal_id=%s
              AND phase='PRETRIGGER'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (client_id, execution_mode, canonical_signal_id),
        )
        row = conn.fetchone()
        payload = row.get("payload") if isinstance(row, Mapping) else row[0] if row else None
        if isinstance(payload, str):
            payload = json.loads(payload)
        score = payload.get("intelligence_score") if isinstance(payload, Mapping) else None
        from ap.intelligence_score import score_matches_identity
        if score_matches_identity(
            score,
            canonical_signal_id=canonical_signal_id,
            client_id=client_id,
            execution_mode=execution_mode,
            ticker=ticker,
            side=side,
        ):
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
            return dict(score)
        conn.execute(f"RELEASE SAVEPOINT {savepoint}")
    except Exception as exc:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        except Exception:
            pass
        log.debug("trade_dossier_intelligence_lookup_unavailable: %s", exc)
    return None


def build_trade_dossier(signal: dict, *, client_id: str, execution_mode: str, decision_context: dict | None = None) -> dict:
    try:
        sig = signal if isinstance(signal, dict) else {}
        ctx = decision_context if isinstance(decision_context, dict) else {}
        signal_id = safe_str(sig.get("signal_id"), str(uuid.uuid4()))
        canonical = _canonical_signal_id(sig, signal_id)
        ticker = safe_str(first_present(sig, ("ticker", "symbol"), "UNKNOWN"), "UNKNOWN").upper()
        direction = safe_str(first_present(sig, ("direction", "side"), None), None)
        strategy = safe_str(first_present(sig, ("strategy", "pattern", "pattern_id"), None), None)
        timeframe = safe_str(first_present(sig, ("timeframe", "time_horizon"), None), None)
        trade_date = safe_str(first_present(sig, ("trade_date", "date"), _today()), _today())[:10]
        identity = {
            "signal_id": signal_id, "canonical_signal_id": canonical, "client_id": client_id,
            "execution_mode": execution_mode, "ticker": ticker, "direction": direction,
            "strategy": strategy, "timeframe": timeframe, "trade_date": trade_date,
            "created_at": _now_iso(), "schema_version": SCHEMA_VERSION,
            "git_commit": safe_str(ctx.get("git_commit"), _git_commit()),
            "config_hash": safe_str(ctx.get("config_hash"), _config_hash()),
        }
        levels = _levels(sig, direction)
        daily = _daily_structure(sig, ctx, levels, direction)
        trend = _trend_profile(sig, ctx, direction)
        market = _market_context(sig, ctx, direction)
        sector = _sector_profile(sig, ctx, direction)
        options = _options_profile(sig, ctx, levels)
        hist = _historical_profile(sig, ctx)
        failure, strength, unavailable = _failure_strength(levels, daily, trend, market, sector, options, hist)
        case = _case_score(levels, daily, trend, market, sector, options, hist, failure, unavailable)
        review = _summary(identity, levels, daily, failure, strength, case)
        intelligence_score = _intelligence_score_for_dossier(sig, ctx, identity)
        intelligence_review = build_intelligence_review(intelligence_score)
        decision_snapshot = {
            "master_control_decision": ctx.get("master_control_decision"),
            "decision_reason": ctx.get("decision_reason"),
            "block_reason": ctx.get("block_reason"),
            "watch_reason": ctx.get("watch_reason"),
            "approved": ctx.get("approved"),
            "created_before_execution": True,
        }
        dossier_json = _enforce_size_cap({
            "identity": identity,
            "levels": levels,
            "daily_structure": daily,
            "trend_profile": trend,
            "market_context": market,
            "sector_profile": sector,
            "options_profile": options,
            "historical_profile": hist,
            "failure_profile": failure,
            "strength_profile": strength,
            "case_score": case,
            "intelligence_score": intelligence_score,
            "intelligence_review": intelligence_review,
            "review_summary": review,
            "decision_snapshot": decision_snapshot,
            "raw_signal": _compact_signal(sig),
            "report_only": TRADE_DOSSIER_REPORT_ONLY,
        })
        status = dossier_json.get("dossier_status") or _status(case, levels)
        return {
            "dossier_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{canonical}:{client_id}:{execution_mode}:{SCHEMA_VERSION}")),
            "signal_id": signal_id,
            "canonical_signal_id": canonical,
            "client_id": client_id,
            "execution_mode": execution_mode,
            "trade_date": trade_date,
            "ticker": ticker,
            "direction": direction,
            "strategy": strategy,
            "timeframe": timeframe,
            "dossier_status": status if status in STATUSES else "BUILDING",
            "case_quality_score": case.get("case_quality_score"),
            "case_grade": case.get("case_grade"),
            "decision": ctx.get("master_control_decision"),
            "decision_reason": ctx.get("decision_reason") or ctx.get("block_reason"),
            "primary_strength": strength.get("primary_strength"),
            "primary_risk": failure.get("primary_failure_mode"),
            "dossier": dossier_json,
            "git_commit": identity["git_commit"],
            "config_hash": identity["config_hash"],
            "schema_version": SCHEMA_VERSION,
        }
    except Exception as exc:
        log.warning("trade_dossier_build_failed: %s", exc)
        return _minimal(signal if isinstance(signal, dict) else {}, client_id, execution_mode, str(exc))


def _columns_exist(conn) -> bool:
    try:
        conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'trade_dossiers'
            """
        )
        rows = conn.fetchall()
        found = {r.get("column_name") if hasattr(r, "get") else r[0] for r in rows}
        return REQUIRED_COLUMNS.issubset(found)
    except Exception as exc:
        log.warning("trade_dossier_column_check_failed: %s", exc)
        return False


def persist_trade_dossier(conn, dossier: dict) -> bool:
    try:
        if not isinstance(dossier, dict) or not conn:
            return False
        if not _columns_exist(conn):
            return False
        conn.execute(
            """
            INSERT INTO trade_dossiers (
              dossier_id,
              signal_id,
              canonical_signal_id,
              client_id,
              execution_mode,
              trade_date,
              ticker,
              direction,
              strategy,
              timeframe,
              dossier_status,
              case_quality_score,
              case_grade,
              decision,
              decision_reason,
              primary_strength,
              primary_risk,
              dossier,
              git_commit,
              config_hash,
              schema_version
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
            ON CONFLICT (canonical_signal_id, client_id, execution_mode, schema_version)
            DO UPDATE SET
              dossier_status = EXCLUDED.dossier_status,
              case_quality_score = EXCLUDED.case_quality_score,
              case_grade = EXCLUDED.case_grade,
              decision = EXCLUDED.decision,
              decision_reason = EXCLUDED.decision_reason,
              primary_strength = EXCLUDED.primary_strength,
              primary_risk = EXCLUDED.primary_risk,
              dossier = EXCLUDED.dossier,
              updated_at = now()
            """,
            (
                dossier.get("dossier_id"), dossier.get("signal_id"), dossier.get("canonical_signal_id"),
                dossier.get("client_id"), dossier.get("execution_mode"), dossier.get("trade_date"),
                dossier.get("ticker"), dossier.get("direction"), dossier.get("strategy"), dossier.get("timeframe"),
                dossier.get("dossier_status"), dossier.get("case_quality_score"), dossier.get("case_grade"),
                dossier.get("decision"), dossier.get("decision_reason"), dossier.get("primary_strength"),
                dossier.get("primary_risk"), json.dumps(dossier.get("dossier") or {}, default=str),
                dossier.get("git_commit"), dossier.get("config_hash"), dossier.get("schema_version") or SCHEMA_VERSION,
            ),
        )
        return True
    except Exception as exc:
        log.warning(
            "trade_dossier_write_failed ticker=%s client_id=%s execution_mode=%s canonical_signal_id=%s err=%s",
            dossier.get("ticker") if isinstance(dossier, dict) else "",
            dossier.get("client_id") if isinstance(dossier, dict) else "",
            dossier.get("execution_mode") if isinstance(dossier, dict) else "",
            dossier.get("canonical_signal_id") if isinstance(dossier, dict) else "",
            exc,
        )
        return False


def build_and_persist_trade_dossier(conn, signal: dict, *, client_id: str, execution_mode: str, decision_context: dict | None = None) -> bool:
    if not ENABLE_TRADE_DOSSIER:
        return False
    try:
        ctx = dict(decision_context or {})
        signal_id = safe_str((signal or {}).get("signal_id"), "")
        canonical = _canonical_signal_id(signal or {}, signal_id)
        persisted_score = _latest_pretrigger_intelligence_score(
            conn,
            canonical_signal_id=canonical,
            client_id=client_id,
            execution_mode=execution_mode,
            ticker=str((signal or {}).get("ticker") or (signal or {}).get("symbol") or ""),
            side=(signal or {}).get("side") or (signal or {}).get("direction"),
        )
        if persisted_score is not None:
            ctx["intelligence_score"] = persisted_score
        dossier = build_trade_dossier(
            signal,
            client_id=client_id,
            execution_mode=execution_mode,
            decision_context=ctx,
        )
        return persist_trade_dossier(conn, dossier)
    except Exception as exc:
        log.warning("trade_dossier_write_failed client_id=%s execution_mode=%s err=%s", client_id, execution_mode, exc)
    return False


def reconcile_trade_dossier_intelligence_score(
    conn: Any,
    *,
    canonical_signal_id: str,
    client_id: str,
    execution_mode: str,
    intelligence_score: Mapping[str, Any],
    snapshot_id: str = "",
) -> dict[str, Any]:
    """Replace an early fallback with the durable PRETRIGGER score.

    The update is identity-bound, report-only, and does not touch admission,
    order, position, proof, or broker state.
    """

    try:
        conn.execute(
            """
            SELECT dossier_id, ticker, direction, dossier
            FROM trade_dossiers
            WHERE canonical_signal_id=%s
              AND client_id=%s
              AND lower(execution_mode)=lower(%s)
              AND schema_version=%s
            LIMIT 1
            """,
            (canonical_signal_id, client_id, execution_mode, SCHEMA_VERSION),
        )
        row = conn.fetchone()
        if not row:
            return {"ok": True, "updated": False, "reason": "dossier_not_found"}
        get = row.get if isinstance(row, Mapping) else None
        ticker = str(get("ticker") if get else row[1] or "")
        side = get("direction") if get else row[2]
        from ap.intelligence_score import score_matches_identity
        if not score_matches_identity(
            intelligence_score,
            canonical_signal_id=canonical_signal_id,
            client_id=client_id,
            execution_mode=execution_mode,
            ticker=ticker,
            side=side,
        ):
            return {"ok": False, "updated": False, "reason": "score_identity_or_integrity_mismatch"}
        dossier_id = str(get("dossier_id") if get else row[0])
        payload = get("dossier") if get else row[3]
        if isinstance(payload, str):
            payload = json.loads(payload)
        dossier_json = dict(payload) if isinstance(payload, Mapping) else {}
        dossier_json["intelligence_score"] = dict(intelligence_score)
        dossier_json["intelligence_review"] = build_intelligence_review(intelligence_score)
        dossier_json["intelligence_reconciliation"] = {
            "snapshot_id": str(snapshot_id or ""),
            "reconciled_at": _now_iso(),
            "source": "durable_pretrigger_snapshot",
            "observe_only": True,
            "affected_eligibility": False,
        }
        dossier_json = _enforce_size_cap(dossier_json)
        conn.execute(
            """
            UPDATE trade_dossiers
            SET dossier=%s::jsonb, updated_at=now()
            WHERE dossier_id=%s
              AND canonical_signal_id=%s
              AND client_id=%s
              AND lower(execution_mode)=lower(%s)
            """,
            (
                json.dumps(dossier_json, default=str),
                dossier_id,
                canonical_signal_id,
                client_id,
                execution_mode,
            ),
        )
        return {"ok": True, "updated": getattr(conn, "rowcount", 1) == 1, "dossier_id": dossier_id}
    except Exception as exc:
        log.warning(
            "trade_dossier_intelligence_reconciliation_failed canonical_signal_id=%s client_id=%s execution_mode=%s err=%s",
            canonical_signal_id,
            client_id,
            execution_mode,
            exc,
        )
        return {"ok": False, "updated": False, "reason": str(exc)[:500]}


def reconcile_pretrigger_snapshot_best_effort(
    snapshot_kwargs: Mapping[str, Any],
    *,
    snapshot_id: str = "",
) -> dict[str, Any]:
    """Wire a completed PRETRIGGER snapshot back into an existing dossier."""

    if str(snapshot_kwargs.get("phase") or "").upper() != "PRETRIGGER":
        return {"ok": True, "updated": False, "reason": "not_pretrigger"}
    if os.getenv("INTELLIGENCE_CONTEXT_STORE_BACKEND", "").lower() == "memory":
        return {"ok": True, "updated": False, "reason": "memory_backend"}
    payload = snapshot_kwargs.get("payload")
    score = payload.get("intelligence_score") if isinstance(payload, Mapping) else None
    if not isinstance(score, Mapping):
        return {"ok": False, "updated": False, "reason": "intelligence_score_missing"}
    try:
        from ap.db import conn
        with conn() as cursor:
            return reconcile_trade_dossier_intelligence_score(
                cursor,
                canonical_signal_id=str(snapshot_kwargs.get("canonical_signal_id") or ""),
                client_id=str(snapshot_kwargs.get("client_id") or ""),
                execution_mode=str(snapshot_kwargs.get("execution_mode") or ""),
                intelligence_score=score,
                snapshot_id=snapshot_id,
            )
    except Exception as exc:
        log.warning("trade_dossier_pretrigger_reconciliation_unavailable: %s", exc)
        return {"ok": False, "updated": False, "reason": str(exc)[:500]}


def reconcile_trade_dossier_backlog_best_effort(
    *,
    client_id: str,
    execution_mode: str,
    limit: int = 50,
) -> dict[str, Any]:
    """Repair existing dossiers from their latest durable PRETRIGGER snapshot."""

    if os.getenv("INTELLIGENCE_CONTEXT_STORE_BACKEND", "").lower() == "memory":
        return {"ok": True, "candidates": 0, "updated": 0, "reason": "memory_backend"}
    bounded_limit = max(1, min(int(limit or 50), 200))
    try:
        from ap.db import conn
        from ap.intelligence_score import INTELLIGENCE_SCORE_VERSION
        with conn() as cursor:
            cursor.execute(
                """
                SELECT
                  td.canonical_signal_id,
                  td.client_id,
                  td.execution_mode,
                  snap.id AS snapshot_id,
                  snap.payload
                FROM trade_dossiers td
                JOIN LATERAL (
                  SELECT id, payload
                  FROM ap_intelligence_snapshots s
                  WHERE s.canonical_signal_id=td.canonical_signal_id
                    AND s.client_id=td.client_id
                    AND lower(s.execution_mode)=lower(td.execution_mode)
                    AND s.phase='PRETRIGGER'
                  ORDER BY s.created_at DESC
                  LIMIT 1
                ) snap ON TRUE
                WHERE td.client_id=%s
                  AND lower(td.execution_mode)=lower(%s)
                  AND (
                    COALESCE(td.dossier #>> '{intelligence_score,score_version}', '') <> %s
                    OR COALESCE(td.dossier #>> '{intelligence_score,score_integrity_hash}', '')
                       IS DISTINCT FROM
                       COALESCE(snap.payload #>> '{intelligence_score,score_integrity_hash}', '')
                  )
                ORDER BY td.created_at ASC
                LIMIT %s
                """,
                (client_id, execution_mode, INTELLIGENCE_SCORE_VERSION, bounded_limit),
            )
            rows = list(cursor.fetchall() or [])
            updated = 0
            failed = 0
            for row in rows:
                get = row.get if isinstance(row, Mapping) else None
                payload = get("payload") if get else row[4]
                if isinstance(payload, str):
                    payload = json.loads(payload)
                score = payload.get("intelligence_score") if isinstance(payload, Mapping) else None
                if not isinstance(score, Mapping):
                    failed += 1
                    continue
                result = reconcile_trade_dossier_intelligence_score(
                    cursor,
                    canonical_signal_id=str(get("canonical_signal_id") if get else row[0]),
                    client_id=str(get("client_id") if get else row[1]),
                    execution_mode=str(get("execution_mode") if get else row[2]),
                    intelligence_score=score,
                    snapshot_id=str(get("snapshot_id") if get else row[3]),
                )
                updated += int(bool(result.get("updated")))
                failed += int(not result.get("ok"))
            return {"ok": failed == 0, "candidates": len(rows), "updated": updated, "failed": failed}
    except Exception as exc:
        log.warning("trade_dossier_intelligence_backlog_reconciliation_unavailable: %s", exc)
        return {"ok": False, "candidates": 0, "updated": 0, "reason": str(exc)[:500]}
