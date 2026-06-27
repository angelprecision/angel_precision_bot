from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Optional

FALLBACK_DEFAULT_SOURCE = "FALLBACK_DEFAULT"
MISSING_SOURCE_REASON = "historical_edge_untrusted:missing_source"
FALLBACK_DEFAULT_REASON = "historical_edge_untrusted:FALLBACK_DEFAULT"
INSUFFICIENT_SAMPLE_REASON = "historical_edge_untrusted:insufficient_sample_size"
BLOCK_REASON_FALLBACK_DEFAULT = "blocked_historical_edge:FALLBACK_DEFAULT"

_SOURCE_KEYS = ("backtest_match_source", "historical_match_source", "historical_edge_source", "backtest_source", "match_source")
_WIN_RATE_KEYS = ("win_rate", "historical_win_rate", "backtest_win_rate")
_AVG_OPT_RET_KEYS = ("avg_opt_ret", "historical_avg_opt_ret", "avg_option_return", "avg_return")
_RR_RATIO_KEYS = ("rr_ratio", "historical_rr_ratio", "risk_reward_ratio", "risk_reward")
_SAMPLE_SIZE_KEYS = ("sample_size", "backtest_sample_size", "historical_sample_size", "matched_sample_size", "matched_trades", "trades", "n", "n_occurrences", "occurrences")
_SCORE_POINT_KEYS = ("historical_edge", "hist_edge", "historical_score", "historical_edge_score", "backtest_score", "backtest_points", "history_score", "edge_score")
_CONTAINER_KEYS = ("metadata", "decision_meta", "score_meta", "score_audit", "scanner_payload", "raw_signal", "payload")
_SCORE_CONTAINER_KEYS = ("score_breakdown", "score_components", "score_audit", "metadata")


@dataclass(frozen=True)
class HistoricalEdgeDecision:
    historical_edge_valid: bool
    backtest_match_source: Optional[str]
    trusted_win_rate: Optional[float]
    trusted_avg_opt_ret: Optional[float]
    trusted_rr_ratio: Optional[float]
    trusted_sample_size: int
    reason: Optional[str] = None
    raw_values: dict[str, Any] = field(default_factory=dict)
    score_adjustment_points: float = 0.0
    raw_score: float = 0.0
    effective_score: float = 0.0
    has_historical_stats: bool = False
    fail_closed: bool = True
    observe_only_override: bool = False

    @property
    def score_depends_on_untrusted_history(self) -> bool:
        return (not self.historical_edge_valid) and self.score_adjustment_points > 0

    @property
    def should_block_client_eligibility(self) -> bool:
        return self.fail_closed and not self.observe_only_override and not self.historical_edge_valid and self.has_historical_stats

    @property
    def is_fallback_default(self) -> bool:
        return _norm_source(self.backtest_match_source) == FALLBACK_DEFAULT_SOURCE

    def to_metadata(self) -> dict[str, Any]:
        return {
            "historical_edge_valid": self.historical_edge_valid,
            "historical_edge_fail_closed": self.fail_closed,
            "historical_edge_observe_only_override": self.observe_only_override,
            "historical_edge_has_stats": self.has_historical_stats,
            "backtest_match_source": self.backtest_match_source,
            "trusted_win_rate": self.trusted_win_rate,
            "trusted_avg_opt_ret": self.trusted_avg_opt_ret,
            "trusted_rr_ratio": self.trusted_rr_ratio,
            "trusted_sample_size": self.trusted_sample_size,
            "historical_edge_reason": self.reason,
            "historical_edge_diagnostics": [self.reason] if self.reason else [],
            "historical_edge_raw_values": dict(self.raw_values),
            "historical_edge_score_adjustment_points": self.score_adjustment_points,
            "historical_edge_raw_score": self.raw_score,
            "historical_edge_effective_score": self.effective_score,
            "score_depends_on_untrusted_history": self.score_depends_on_untrusted_history,
            "historical_edge_blocks_client_eligibility": self.should_block_client_eligibility,
        }


def evaluate_historical_edge(payload: dict[str, Any] | None, *, raw_score: float = 0.0, min_sample_size: Optional[int] = None, trust_missing_source: Optional[bool] = None, fail_closed: Optional[bool] = None, observe_only_override: Optional[bool] = None) -> HistoricalEdgeDecision:
    signal = payload if isinstance(payload, dict) else {}
    min_n = _env_int("HISTORICAL_EDGE_MIN_SAMPLE_SIZE", 5) if min_sample_size is None else int(min_sample_size)
    trust_missing = _env_bool("HISTORICAL_EDGE_TRUST_MISSING_SOURCE", False) if trust_missing_source is None else bool(trust_missing_source)
    fail_closed_value = _env_bool("HISTORICAL_EDGE_FAIL_CLOSED", True) if fail_closed is None else bool(fail_closed)
    observe_only_value = _env_bool("HISTORICAL_EDGE_OBSERVE_ONLY_OVERRIDE", False) if observe_only_override is None else bool(observe_only_override)

    source = _first_value(signal, _SOURCE_KEYS)
    raw_win_rate = _first_value(signal, _WIN_RATE_KEYS)
    raw_avg_opt_ret = _first_value(signal, _AVG_OPT_RET_KEYS)
    raw_rr_ratio = _first_value(signal, _RR_RATIO_KEYS)
    raw_sample = _first_value(signal, _SAMPLE_SIZE_KEYS)
    has_stats = any(v is not None for v in (raw_win_rate, raw_avg_opt_ret, raw_rr_ratio, raw_sample))

    win_rate = _safe_float(raw_win_rate)
    avg_opt_ret = _safe_float(raw_avg_opt_ret)
    rr_ratio = _safe_float(raw_rr_ratio)
    sample_size = _safe_int(raw_sample) or 0
    score_adjustment = _resolve_historical_score_points(signal)
    score = _safe_float(raw_score) or 0.0
    effective_score = max(0.0, score - score_adjustment)
    raw_values = {"backtest_match_source": source, "win_rate": raw_win_rate, "avg_opt_ret": raw_avg_opt_ret, "rr_ratio": raw_rr_ratio, "sample_size": raw_sample}

    source_norm = _norm_source(source)
    if not source_norm and not trust_missing:
        return _untrusted(source, MISSING_SOURCE_REASON, raw_values, score_adjustment, score, effective_score, has_stats, fail_closed_value, observe_only_value)
    if source_norm == FALLBACK_DEFAULT_SOURCE:
        return _untrusted(source, FALLBACK_DEFAULT_REASON, raw_values, score_adjustment, score, effective_score, has_stats, fail_closed_value, observe_only_value)
    if source_norm and sample_size < min_n:
        return _untrusted(source, INSUFFICIENT_SAMPLE_REASON, raw_values, score_adjustment, score, effective_score, has_stats, fail_closed_value, observe_only_value)

    return HistoricalEdgeDecision(True, str(source) if source is not None else None, win_rate, avg_opt_ret, rr_ratio, sample_size, raw_values=raw_values, raw_score=score, effective_score=score, has_historical_stats=has_stats, fail_closed=fail_closed_value, observe_only_override=observe_only_value)


def block_reason_for_untrusted_historical_edge(decision: HistoricalEdgeDecision) -> Optional[str]:
    if not decision.should_block_client_eligibility and not decision.score_depends_on_untrusted_history:
        return None
    if decision.is_fallback_default:
        return BLOCK_REASON_FALLBACK_DEFAULT
    if decision.reason == MISSING_SOURCE_REASON:
        return "blocked_historical_edge:missing_source"
    if decision.reason == INSUFFICIENT_SAMPLE_REASON:
        return "blocked_historical_edge:insufficient_sample_size"
    if decision.reason:
        return f"blocked_historical_edge:{decision.reason.split(':', 1)[-1]}"
    return "blocked_historical_edge:untrusted"


def _untrusted(source: Any, reason: str, raw_values: dict[str, Any], score_adjustment: float, raw_score: float, effective_score: float, has_stats: bool, fail_closed: bool, observe_only: bool) -> HistoricalEdgeDecision:
    return HistoricalEdgeDecision(False, str(source) if source is not None else None, None, None, None, 0, reason=reason, raw_values=raw_values, score_adjustment_points=score_adjustment, raw_score=raw_score, effective_score=effective_score, has_historical_stats=has_stats, fail_closed=fail_closed, observe_only_override=observe_only)


def _iter_containers(payload: dict[str, Any]):
    yield payload
    for key in _CONTAINER_KEYS:
        value = payload.get(key)
        if isinstance(value, dict):
            yield value


def _first_value(payload: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for container in _iter_containers(payload):
        for key in keys:
            if key in container and container.get(key) is not None:
                return container.get(key)
    return None


def _resolve_historical_score_points(payload: dict[str, Any]) -> float:
    best = 0.0
    def visit(container: dict[str, Any], depth: int = 0) -> None:
        nonlocal best
        if depth > 3:
            return
        for key in _SCORE_POINT_KEYS:
            val = _safe_float(container.get(key)) if key in container else None
            if val is not None and val > best:
                best = val
        for key in _SCORE_CONTAINER_KEYS:
            value = container.get(key)
            if isinstance(value, dict):
                visit(value, depth + 1)
    for container in _iter_containers(payload):
        visit(container)
    return max(0.0, best)


def _norm_source(value: Any) -> str:
    return str(value or "").strip().upper()


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _env_bool(key: str, default: bool) -> bool:
    return os.getenv(key, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except (TypeError, ValueError):
        return default
