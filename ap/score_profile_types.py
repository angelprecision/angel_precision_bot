from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ScoreComponent:
    name: str
    score: float
    max_score: float
    status: str = "ok"
    reason: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "score": round(float(self.score), 2),
            "max_score": round(float(self.max_score), 2),
            "status": self.status,
            "reason": self.reason,
            "details": dict(self.details or {}),
        }


@dataclass(frozen=True)
class PositionScoreProfile:
    profile_version: str
    total_score: float
    base_score: float
    bonus_score: float
    penalty_score: float
    grade: str
    client_eligible_recommendation: bool
    components: dict[str, ScoreComponent]
    missing_data: list[str] = field(default_factory=list)
    block_recommendations: list[str] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_version": self.profile_version,
            "total_score": round(float(self.total_score), 2),
            "base_score": round(float(self.base_score), 2),
            "bonus_score": round(float(self.bonus_score), 2),
            "penalty_score": round(float(self.penalty_score), 2),
            "grade": self.grade,
            "client_eligible_recommendation": bool(self.client_eligible_recommendation),
            "components": {key: value.to_dict() for key, value in (self.components or {}).items()},
            "missing_data": list(self.missing_data or []),
            "block_recommendations": list(self.block_recommendations or []),
            "diagnostics": dict(self.diagnostics or {}),
        }


def grade_from_score(score: float) -> str:
    score = float(score or 0)
    if score >= 90:
        return "A+"
    if score >= 80:
        return "A"
    if score >= 70:
        return "B"
    if score >= 60:
        return "C"
    return "REJECT"


def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(float(low), min(float(high), float(value or 0)))
