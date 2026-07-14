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
from datetime import datetime, timezone
from typing import Any, Mapping

from ap.position_score_profile import PROFILE_VERSION
from ap.score_profile_config import DEFAULT_SCORE_PROFILE_CONFIG
from ap.score_profile_side import normalize_signal_side


INTELLIGENCE_SCORE_VERSION = "intelligence_policy_score_v1_observe_only"
RAW_PROFILE_SCORE_MAX = 120.0
MIN_EVIDENCE_COVERAGE = 0.70
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


def _component_summary(profile: Mapping[str, Any]) -> tuple[dict[str, dict[str, Any]], float, float, list[str]]:
    components = profile.get("components")
    if not isinstance(components, Mapping):
        return {}, 0.0, 0.0, list(REQUIRED_COMPONENTS)

    summary: dict[str, dict[str, Any]] = {}
    available_weight = 0.0
    total_weight = 0.0
    unavailable: list[str] = []
    for raw_name, raw_component in components.items():
        name = str(raw_name)
        component = raw_component if isinstance(raw_component, Mapping) else {}
        score = _finite(component.get("score"))
        max_score = _finite(component.get("max_score"))
        status = str(component.get("status") or "unknown").strip().lower()
        max_score = max(0.0, max_score or 0.0)
        total_weight += max_score
        available = bool(max_score > 0 and score is not None and status not in _UNAVAILABLE_STATUSES)
        if available:
            available_weight += max_score
        else:
            unavailable.append(name)
        summary[name] = {
            "score": round(score, 2) if score is not None else None,
            "max_score": round(max_score, 2),
            "status": status,
            "available": available,
        }
    return summary, available_weight, total_weight, sorted(set(unavailable))


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
    normalized_data_as_of = _timestamp(data_as_of) or normalized_scored_at
    raw_score = _finite(prof.get("total_score"))
    component_summary, available_weight, total_weight, unavailable = _component_summary(prof)
    coverage = available_weight / total_weight if total_weight > 0 else 0.0

    invalid_reasons: list[str] = []
    if not canonical_signal_id:
        invalid_reasons.append("canonical_signal_id_missing")
    if not str(client_id or "").strip():
        invalid_reasons.append("client_id_missing")
    if not str(execution_mode or "").strip():
        invalid_reasons.append("execution_mode_missing")
    if not ticker:
        invalid_reasons.append("ticker_missing")
    if side == "UNKNOWN":
        invalid_reasons.append("side_unknown")
    if normalized_scored_at is None:
        invalid_reasons.append("scored_at_invalid")
    if raw_score is None or not 0 <= raw_score <= RAW_PROFILE_SCORE_MAX:
        invalid_reasons.append("raw_profile_score_out_of_range")
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
            "execution_mode": str(execution_mode or "").upper(),
            "ticker": ticker,
            "side": side,
        },
        "score_config_hash": cfg_hash,
        "profile_version": str(prof.get("profile_version") or ""),
        "raw_profile_score": raw_score,
        "components": component_summary,
        "coverage_ratio": round(coverage, 6),
        "block_recommendations": blocks,
        "warnings": warnings,
        "data_as_of": normalized_data_as_of,
        "source": str(source or ""),
    }

    return {
        "score_version": INTELLIGENCE_SCORE_VERSION,
        "policy_version": score_policy_version(),
        "score_config_hash": cfg_hash,
        "policy_score": policy_score,
        "raw_profile_score": round(raw_score, 2) if raw_score is not None else None,
        "raw_profile_score_max": RAW_PROFILE_SCORE_MAX,
        "score_valid": score_valid,
        "eligible_for_ranking": eligible_for_ranking,
        "evidence_coverage": round(coverage, 6),
        "available_component_weight": round(available_weight, 2),
        "total_component_weight": round(total_weight, 2),
        "required_components": list(REQUIRED_COMPONENTS),
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
            "execution_mode": str(execution_mode or "").upper(),
            "ticker": ticker,
            "side": side,
        },
        "scored_at": normalized_scored_at,
        "data_as_of": normalized_data_as_of,
        "source": str(source or ""),
        "profile_version": str(prof.get("profile_version") or ""),
        "input_hash": _stable_hash(evidence),
        "git_commit": str(git_commit or ""),
        "observe_only": True,
        "affected_eligibility": False,
    }


def score_matches_identity(
    score: Mapping[str, Any] | None,
    *,
    canonical_signal_id: str,
    client_id: str,
    execution_mode: str,
) -> bool:
    """Prevent a persisted score from being attached across account identities."""

    if not isinstance(score, Mapping):
        return False
    identity = score.get("identity")
    if not isinstance(identity, Mapping):
        return False
    return (
        str(score.get("score_version") or "") == INTELLIGENCE_SCORE_VERSION
        and str(score.get("score_config_hash") or "") == score_config_hash()
        and str(score.get("policy_version") or "") == score_policy_version()
        and str(identity.get("canonical_signal_id") or "") == str(canonical_signal_id or "")
        and str(identity.get("client_id") or "") == str(client_id or "")
        and str(identity.get("execution_mode") or "").upper() == str(execution_mode or "").upper()
    )
