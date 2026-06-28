from __future__ import annotations

from typing import Any

ERROR_PROFILE_VERSION = "position_score_profile_v1_observe_only"


def attach_observe_only_position_profile(plan: Any, signal: dict[str, Any], market_context: dict[str, Any] | None = None) -> dict[str, Any]:
    """Attach observe-only score profile metadata without changing decisions."""
    try:
        from ap.position_score_profile import build_position_score_profile

        profile = build_position_score_profile(signal, market_context)
    except Exception as exc:  # pragma: no cover - defensive runtime boundary
        profile = {
            "profile_version": ERROR_PROFILE_VERSION,
            "observe_only": True,
            "status": "error",
            "error": "profile_build_failed",
            "error_detail": str(exc),
            "components": {},
            "missing_data": ["position_score_profile_exception"],
            "warnings": ["position_score_profile_exception"],
            "block_recommendations": [],
            "diagnostics": {
                "observe_only": True,
                "live_behavior_changed": False,
                "metadata_only": True,
            },
        }

    metadata = getattr(plan, "metadata", None)
    if metadata is None:
        metadata = {}
        try:
            plan.metadata = metadata
        except Exception:
            return profile

    metadata["position_score_profile"] = profile
    score_audit = metadata.get("score_audit")
    if isinstance(score_audit, dict):
        score_audit["position_score_profile"] = profile
    return profile
