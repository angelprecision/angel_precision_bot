"""Canonical, pre-entry intelligence score for research ranking.

The position score profile remains the detailed diagnostic surface.  This
module turns that profile into one strict 0-100 score with an explicit scale,
evidence coverage, identity, timestamp, and policy lineage.  Invalid or
under-covered profiles are retained for population accounting but are never
eligible for ranking.

This module is observe-only.  It must not change admission, sizing, broker, or
position behavior.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from ap.position_score_profile import PROFILE_VERSION, RANK_EXCLUDED_COMPONENTS
from ap.score_profile_config import DEFAULT_SCORE_PROFILE_CONFIG
from ap.score_profile_side import normalize_signal_side


INTELLIGENCE_SCORE_VERSION = "intelligence_policy_score_v2_observe_only"
RAW_PROFILE_SCORE_MAX = 120.0
PROFILE_BONUS_MAX = 20.0
PROFILE_PENALTY_MAX = 35.0
MIN_EVIDENCE_COVERAGE = 0.70
MAX_DATA_AGE_SECONDS = 300.0
MAX_FUTURE_SKEW_SECONDS = 5.0
PROFILE_ARITHMETIC_TOLERANCE = 0.051
EXPECTED_COMPONENT_MAXIMA = {
    "scanner_quality": 15.0,
    "trigger_geometry": 10.0,
    "remaining_opportunity": 10.0,
    "higher_timeframe_confluence": 18.0,
    "fair_value_gap": 15.0,
    "price_stacking": 10.0,
    "volume_confirmation": 10.0,
    "vwap_context": 10.0,
    "sector_context": 5.0,
    "contract_execution_quality": 10.0,
    "historical_feedback": 5.0,
}
RANK_COMPONENTS = tuple(
    name for name in EXPECTED_COMPONENT_MAXIMA
    if name not in RANK_EXCLUDED_COMPONENTS
)
REQUIRED_COMPONENTS = (
    "scanner_quality",
    "trigger_geometry",
    "remaining_opportunity",
)
_UNAVAILABLE_STATUSES = {"error", "missing", "missing_data", "unavailable"}


def _stable_hash(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _finite(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _timestamp(value: Any) -> str | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        raw = value.strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc).isoformat()


def score_config() -> dict[str, Any]:
    """Return the complete versioned configuration that defines the score."""

    return {
        "score_version": INTELLIGENCE_SCORE_VERSION,
        "raw_profile_score_max": RAW_PROFILE_SCORE_MAX,
        "min_evidence_coverage": MIN_EVIDENCE_COVERAGE,
        "max_data_age_seconds": MAX_DATA_AGE_SECONDS,
        "max_future_skew_seconds": MAX_FUTURE_SKEW_SECONDS,
        "profile_arithmetic_tolerance": PROFILE_ARITHMETIC_TOLERANCE,
        "expected_component_maxima": dict(EXPECTED_COMPONENT_MAXIMA),
        "rank_components": list(RANK_COMPONENTS),
        "rank_excluded_components": list(RANK_EXCLUDED_COMPONENTS),
        "profile_bonus_max": PROFILE_BONUS_MAX,
        "profile_penalty_max": PROFILE_PENALTY_MAX,
        "required_components": list(REQUIRED_COMPONENTS),
        "ranking_requires_no_block_recommendations": True,
        "normalization": "raw_profile_score/raw_profile_score_max*100",
        "position_profile_version": PROFILE_VERSION,
        "position_profile_config": asdict(DEFAULT_SCORE_PROFILE_CONFIG),
    }


def score_config_hash() -> str:
    return _stable_hash(score_config())[:16]


def score_policy_version() -> str:
    return f"{INTELLIGENCE_SCORE_VERSION}:{score_config_hash()}"


def _component_summary(
    profile: Mapping[str, Any],
) -> tuple[dict[str, dict[str, Any]], float, float, list[str], list[str]]:
    components = profile.get("components")
    if not isinstance(components, Mapping):
        total_weight = sum(EXPECTED_COMPONENT_MAXIMA[name] for name in RANK_COMPONENTS)
        return {}, 0.0, total_weight, list(RANK_COMPONENTS), ["components_missing"]

    summary: dict[str, dict[str, Any]] = {}
    available_weight = 0.0
    total_weight = sum(EXPECTED_COMPONENT_MAXIMA[name] for name in RANK_COMPONENTS)
    unavailable: list[str] = []
    invalid: list[str] = []
    actual_names = {str(name) for name in components}
    expected_names = set(EXPECTED_COMPONENT_MAXIMA)
    for name in sorted(expected_names - actual_names):
        invalid.append(f"component_missing:{name}")
    for name in sorted(actual_names - expected_names):
        invalid.append(f"component_unexpected:{name}")

    for name, expected_max in EXPECTED_COMPONENT_MAXIMA.items():
        raw_component = components.get(name)
        component = raw_component if isinstance(raw_component, Mapping) else {}
        score = _finite(component.get("score"))
        max_score = _finite(component.get("max_score"))
        status = str(component.get("status") or "missing").strip().lower()
        max_matches = max_score is not None and abs(max_score - expected_max) <= 1e-6
        score_in_range = score is not None and 0.0 <= score <= expected_max
        name_matches = str(component.get("name") or name) == name
        if raw_component is not None and not isinstance(raw_component, Mapping):
            invalid.append(f"component_invalid:{name}")
        if raw_component is not None and not max_matches:
            invalid.append(f"component_max_mismatch:{name}")
        if raw_component is not None and not score_in_range:
            invalid.append(f"component_score_out_of_range:{name}")
        if raw_component is not None and not name_matches:
            invalid.append(f"component_name_mismatch:{name}")
        rank_included = name in RANK_COMPONENTS
        available = bool(
            rank_included
            and max_matches
            and score_in_range
            and status not in _UNAVAILABLE_STATUSES
        )
        if available:
            available_weight += expected_max
        elif rank_included:
            unavailable.append(name)
        summary[name] = {
            "score": round(score, 2) if score is not None else None,
            "max_score": round(max_score, 2) if max_score is not None else None,
            "expected_max_score": round(expected_max, 2),
            "status": status,
            "available": available,
            "rank_included": rank_included,
        }
    return (
        summary,
        available_weight,
        total_weight,
        sorted(set(unavailable)),
        sorted(set(invalid)),
    )


def _score_integrity_payload(score: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): value
        for key, value in score.items()
        if str(key) != "score_integrity_hash"
    }


def score_envelope_integrity_valid(score: Mapping[str, Any] | None) -> bool:
    if not isinstance(score, Mapping):
        return False
    expected = str(score.get("score_integrity_hash") or "")
    return bool(expected and expected == _stable_hash(_score_integrity_payload(score)))


def build_intelligence_score(
    profile: Mapping[str, Any] | None,
    *,
    signal: Mapping[str, Any] | None,
    client_id: str,
    execution_mode: str,
    scored_at: Any,
    data_as_of: Any = None,
    source: str,
    git_commit: str = "",
) -> dict[str, Any]:
    """Build one fail-closed score envelope from pre-entry evidence."""

    prof = profile if isinstance(profile, Mapping) else {}
    sig = signal if isinstance(signal, Mapping) else {}
    canonical_signal_id = str(
        sig.get("canonical_signal_id") or sig.get("signal_id") or ""
    ).strip()
    signal_id = str(sig.get("signal_id") or "").strip()
    ticker = str(sig.get("ticker") or sig.get("symbol") or "").strip().upper()
    side = normalize_signal_side(sig.get("side") or sig.get("direction"))
    normalized_scored_at = _timestamp(scored_at)
    normalized_data_as_of = _timestamp(data_as_of) if data_as_of is not None else normalized_scored_at
    raw_score = _finite(prof.get("total_score"))
    base_score = _finite(prof.get("base_score"))
    bonus_score = _finite(prof.get("bonus_score"))
    penalty_score = _finite(prof.get("penalty_score"))
    component_summary, available_weight, total_weight, unavailable, component_invalid = _component_summary(prof)
    coverage = available_weight / total_weight if total_weight > 0 else 0.0

    invalid_reasons: list[str] = list(component_invalid)
    if not canonical_signal_id:
        invalid_reasons.append("canonical_signal_id_missing")
    if not str(client_id or "").strip():
        invalid_reasons.append("client_id_missing")
    normalized_mode = str(execution_mode or "").strip().upper()
    if normalized_mode not in {"PAPER", "LIVE"}:
        invalid_reasons.append("execution_mode_invalid")
    if not ticker:
        invalid_reasons.append("ticker_missing")
    if side == "UNKNOWN":
        invalid_reasons.append("side_unknown")
    if normalized_scored_at is None:
        invalid_reasons.append("scored_at_invalid")
    if normalized_data_as_of is None:
        invalid_reasons.append("data_as_of_invalid")
    if normalized_scored_at is not None and normalized_data_as_of is not None:
        scored_dt = datetime.fromisoformat(normalized_scored_at)
        data_dt = datetime.fromisoformat(normalized_data_as_of)
        if data_dt > scored_dt + timedelta(seconds=MAX_FUTURE_SKEW_SECONDS):
            invalid_reasons.append("data_as_of_after_scored_at")
        age_seconds = (scored_dt - data_dt).total_seconds()
        if age_seconds > MAX_DATA_AGE_SECONDS:
            invalid_reasons.append("data_as_of_stale")
    else:
        age_seconds = None
    if str(prof.get("profile_version") or "") != PROFILE_VERSION:
        invalid_reasons.append("profile_version_mismatch")
    if prof.get("observe_only") is not True:
        invalid_reasons.append("profile_observe_only_required")
    rank_excluded = set((prof.get("diagnostics") or {}).get("rank_excluded_components") or []) if isinstance(prof.get("diagnostics"), Mapping) else set()
    if rank_excluded != set(RANK_EXCLUDED_COMPONENTS):
        invalid_reasons.append("rank_excluded_components_mismatch")
    if raw_score is None or not 0 <= raw_score <= RAW_PROFILE_SCORE_MAX:
        invalid_reasons.append("raw_profile_score_out_of_range")
    expected_base = sum(
        float((component_summary.get(name) or {}).get("score") or 0.0)
        for name in RANK_COMPONENTS
    )
    if base_score is None or abs(base_score - expected_base) > PROFILE_ARITHMETIC_TOLERANCE:
        invalid_reasons.append("base_score_mismatch")
    if bonus_score is None or not 0.0 <= bonus_score <= PROFILE_BONUS_MAX:
        invalid_reasons.append("bonus_score_out_of_range")
    if penalty_score is None or not 0.0 <= penalty_score <= PROFILE_PENALTY_MAX:
        invalid_reasons.append("penalty_score_out_of_range")
    if base_score is not None and bonus_score is not None and penalty_score is not None:
        expected_total = max(0.0, min(RAW_PROFILE_SCORE_MAX, base_score + bonus_score - penalty_score))
        if raw_score is None or abs(raw_score - expected_total) > PROFILE_ARITHMETIC_TOLERANCE:
            invalid_reasons.append("total_score_mismatch")
    for name in REQUIRED_COMPONENTS:
        if not (component_summary.get(name) or {}).get("available"):
            invalid_reasons.append(f"required_component_unavailable:{name}")
    if coverage < MIN_EVIDENCE_COVERAGE:
        invalid_reasons.append("evidence_coverage_below_minimum")

    score_valid = not invalid_reasons
    policy_score = (
        round(max(0.0, min(100.0, raw_score / RAW_PROFILE_SCORE_MAX * 100.0)), 2)
        if score_valid and raw_score is not None
        else None
    )
    blocks = sorted(set(str(item) for item in (prof.get("block_recommendations") or []) if item))
    warnings = sorted(set(str(item) for item in (prof.get("warnings") or []) if item))
    eligible_for_ranking = bool(score_valid and not blocks)
    cfg_hash = score_config_hash()
    evidence = {
        "identity": {
            "canonical_signal_id": canonical_signal_id,
            "client_id": str(client_id or ""),
            "execution_mode": normalized_mode,
            "ticker": ticker,
            "side": side,
        },
        "score_config_hash": cfg_hash,
        "profile_version": str(prof.get("profile_version") or ""),
        "raw_profile_score": raw_score,
        "base_score": base_score,
        "bonus_score": bonus_score,
        "penalty_score": penalty_score,
        "components": component_summary,
        "coverage_ratio": round(coverage, 6),
        "block_recommendations": blocks,
        "warnings": warnings,
        "data_as_of": normalized_data_as_of,
        "data_age_seconds": round(age_seconds, 3) if age_seconds is not None else None,
        "source": str(source or ""),
    }

    envelope = {
        "score_version": INTELLIGENCE_SCORE_VERSION,
        "policy_version": score_policy_version(),
        "score_config_hash": cfg_hash,
        "policy_score": policy_score,
        "raw_profile_score": round(raw_score, 2) if raw_score is not None else None,
        "raw_profile_score_max": RAW_PROFILE_SCORE_MAX,
        "base_score": round(base_score, 2) if base_score is not None else None,
        "bonus_score": round(bonus_score, 2) if bonus_score is not None else None,
        "penalty_score": round(penalty_score, 2) if penalty_score is not None else None,
        "score_valid": score_valid,
        "eligible_for_ranking": eligible_for_ranking,
        "evidence_coverage": round(coverage, 6),
        "available_component_weight": round(available_weight, 2),
        "total_component_weight": round(total_weight, 2),
        "required_components": list(REQUIRED_COMPONENTS),
        "rank_components": list(RANK_COMPONENTS),
        "rank_excluded_components": list(RANK_EXCLUDED_COMPONENTS),
        "unavailable_components": unavailable,
        "invalid_reasons": sorted(set(invalid_reasons)),
        "block_recommendations": blocks,
        "warnings": warnings,
        "components": component_summary,
        "identity": {
            "opportunity_id": canonical_signal_id,
            "canonical_signal_id": canonical_signal_id,
            "signal_id": signal_id,
            "client_id": str(client_id or ""),
            "execution_mode": normalized_mode,
            "ticker": ticker,
            "side": side,
        },
        "scored_at": normalized_scored_at,
        "data_as_of": normalized_data_as_of,
        "data_age_seconds": round(age_seconds, 3) if age_seconds is not None else None,
        "source": str(source or ""),
        "profile_version": str(prof.get("profile_version") or ""),
        "input_hash": _stable_hash(evidence),
        "git_commit": str(git_commit or ""),
        "observe_only": True,
        "affected_eligibility": False,
    }
    envelope["score_integrity_hash"] = _stable_hash(envelope)
    return envelope


def score_matches_identity(
    score: Mapping[str, Any] | None,
    *,
    canonical_signal_id: str,
    client_id: str,
    execution_mode: str,
    ticker: str | None = None,
    side: str | None = None,
) -> bool:
    """Prevent a persisted score from being attached across account identities."""

    if not isinstance(score, Mapping):
        return False
    identity = score.get("identity")
    if not isinstance(identity, Mapping):
        return False
    expected_ticker = str(ticker or "").strip().upper()
    expected_side = normalize_signal_side(side) if side is not None else ""
    return (
        score_envelope_integrity_valid(score)
        and str(score.get("score_version") or "") == INTELLIGENCE_SCORE_VERSION
        and str(score.get("score_config_hash") or "") == score_config_hash()
        and str(score.get("policy_version") or "") == score_policy_version()
        and str(identity.get("canonical_signal_id") or "") == str(canonical_signal_id or "")
        and str(identity.get("client_id") or "") == str(client_id or "")
        and str(identity.get("execution_mode") or "").upper() == str(execution_mode or "").upper()
        and (not expected_ticker or str(identity.get("ticker") or "").upper() == expected_ticker)
        and (not expected_side or str(identity.get("side") or "").upper() == expected_side)
    )
