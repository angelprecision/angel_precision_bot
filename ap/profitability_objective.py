"""Leak-resistant evaluation for a frozen daily opportunity-ranking policy.

This module is deliberately research-only.  It evaluates a score that was
already stamped before an opportunity's outcome existed; it does not train a
model, mutate production thresholds, or participate in broker execution.

Percent returns are represented as decimal fractions (``0.18`` means +18%).
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import date, datetime
from statistics import mean, stdev
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo


class ProfitabilityDataError(ValueError):
    """Raised when evaluation input cannot prove a leak-free time boundary."""


_LEAKAGE_TOKENS = {
    "actual_return",
    "close_price",
    "exit_price",
    "exit_reason",
    "future_price",
    "hypothetical_r",
    "mae",
    "mae_pct",
    "mfe",
    "mfe_pct",
    "net_pnl",
    "outcome",
    "outcome_return_pct",
    "realized_pnl",
    "realized_r",
    "resolution",
    "stop_hit",
    "target_hit",
    "win_flag",
}

_MARKET_TZ = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class ProfitabilityTargets:
    """The user's desired profile plus evidence needed before promotion.

    ``reference_avg_win_ceiling_pct`` describes the desired 18-25% band but
    is not a failure ceiling: an honestly measured average win above 25% is
    not penalized.
    """

    target_win_rate: float = 0.80
    max_avg_loss_pct: float = 0.12
    min_avg_win_pct: float = 0.18
    reference_avg_win_ceiling_pct: float = 0.25
    min_daily_selections: int = 5
    max_daily_selections: int = 7
    min_holdout_sessions: int = 20
    min_resolved_trades: int = 100
    min_outcome_coverage: float = 0.95
    min_score_coverage: float = 0.95
    min_resolved_unselected: int = 100
    confidence_z: float = 1.959963984540054

    def __post_init__(self) -> None:
        if not 0 < self.target_win_rate < 1:
            raise ProfitabilityDataError("target_win_rate must be between 0 and 1")
        if not 0 < self.max_avg_loss_pct <= 1:
            raise ProfitabilityDataError("max_avg_loss_pct must be in (0, 1]")
        if not 0 < self.min_avg_win_pct <= self.reference_avg_win_ceiling_pct:
            raise ProfitabilityDataError("average-win target band is invalid")
        if not 1 <= self.min_daily_selections <= self.max_daily_selections:
            raise ProfitabilityDataError("daily selection bounds are invalid")
        if self.min_holdout_sessions < 1 or self.min_resolved_trades < 1:
            raise ProfitabilityDataError("minimum evidence sizes must be positive")
        if not 0 < self.min_outcome_coverage <= 1:
            raise ProfitabilityDataError("min_outcome_coverage must be in (0, 1]")
        if not 0 < self.min_score_coverage <= 1:
            raise ProfitabilityDataError("min_score_coverage must be in (0, 1]")
        if self.min_resolved_unselected < 1:
            raise ProfitabilityDataError("min_resolved_unselected must be positive")
        if self.confidence_z <= 0:
            raise ProfitabilityDataError("confidence_z must be positive")


@dataclass(frozen=True)
class Opportunity:
    opportunity_id: str
    client_id: str
    session_date: date
    observed_at: datetime
    policy_version: str
    policy_score: float
    eligible: bool
    features: Mapping[str, Any]
    outcome_return_pct: float | None = None
    outcome_known_at: datetime | None = None

    @property
    def resolved(self) -> bool:
        return self.outcome_return_pct is not None


@dataclass(frozen=True)
class DailySelection:
    client_id: str
    session_date: date
    source_count: int
    eligible_count: int
    source: tuple[Opportunity, ...]
    ranked_eligible: tuple[Opportunity, ...]
    selected: tuple[Opportunity, ...]

    @property
    def selected_count(self) -> int:
        return len(self.selected)


@dataclass(frozen=True)
class ProfitabilityReport:
    policy_version: str
    policy_frozen_at: str
    sessions: int
    source_opportunities: int
    eligible_opportunities: int
    selected_trades: int
    resolved_trades: int
    unresolved_trades: int
    outcome_coverage: float | None
    valid_score_opportunities: int
    score_validity_coverage: float | None
    resolved_eligible_opportunities: int
    unresolved_eligible_opportunities: int
    eligible_outcome_coverage: float | None
    resolved_unselected_opportunities: int
    unselected_win_rate: float | None
    unselected_win_rate_ci_high: float | None
    unselected_expectancy_pct: float | None
    unselected_expectancy_ci_high: float | None
    selected_win_rate_lift: float | None
    selected_win_rate_lift_ci_low: float | None
    selected_expectancy_lift_pct: float | None
    selected_expectancy_lift_ci_low: float | None
    score_return_correlation: float | None
    wins: int
    losses: int
    breakeven: int
    win_rate: float | None
    win_rate_ci_low: float | None
    win_rate_ci_high: float | None
    avg_win_pct: float | None
    avg_win_ci_low: float | None
    avg_loss_pct: float | None
    avg_loss_ci_high: float | None
    expectancy_pct: float | None
    expectancy_ci_low: float | None
    avg_selected_per_session: float | None
    avg_source_opportunities_per_session: float | None
    underfilled_sessions: int
    target_checks: Mapping[str, bool]
    verdict: str
    verdict_reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _aware_datetime(value: Any, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        raw = value.strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ProfitabilityDataError(f"{field_name} must be an ISO-8601 timestamp") from exc
    else:
        raise ProfitabilityDataError(f"{field_name} must be an ISO-8601 timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ProfitabilityDataError(f"{field_name} must include a timezone offset")
    return parsed


def _session_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value.strip())
        except ValueError as exc:
            raise ProfitabilityDataError("session_date must be YYYY-MM-DD") from exc
    raise ProfitabilityDataError("session_date must be YYYY-MM-DD")


def _finite_number(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise ProfitabilityDataError(f"{field_name} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ProfitabilityDataError(f"{field_name} must be numeric") from exc
    if not math.isfinite(parsed):
        raise ProfitabilityDataError(f"{field_name} must be finite")
    return parsed


def _leakage_paths(value: Any, prefix: str = "features") -> list[str]:
    found: list[str] = []
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key).strip().lower()
            path = f"{prefix}.{raw_key}"
            if key in _LEAKAGE_TOKENS or any(token in key for token in ("realized_pnl", "future_return", "outcome_return")):
                found.append(path)
            found.extend(_leakage_paths(child, path))
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            found.extend(_leakage_paths(child, f"{prefix}[{index}]"))
    return found


def opportunity_mapping_from_intelligence_score(
    score: Mapping[str, Any],
    *,
    session_date: Any = None,
    outcome: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Convert one canonical pre-entry score into the strict evaluator row."""

    if not isinstance(score, Mapping):
        raise ProfitabilityDataError("intelligence_score must be an object")
    identity = score.get("identity")
    if not isinstance(identity, Mapping):
        raise ProfitabilityDataError("intelligence_score.identity must be an object")
    canonical_id = str(
        identity.get("canonical_signal_id") or identity.get("opportunity_id") or ""
    ).strip()
    client_id = str(identity.get("client_id") or "").strip()
    execution_mode = str(identity.get("execution_mode") or "").strip().upper()
    if not canonical_id:
        raise ProfitabilityDataError("intelligence_score canonical identity is required")
    if not client_id:
        raise ProfitabilityDataError("intelligence_score client identity is required")
    if not execution_mode:
        raise ProfitabilityDataError("intelligence_score execution mode is required")
    observed_at = _aware_datetime(score.get("scored_at"), "intelligence_score.scored_at")
    market_session = (
        _session_date(session_date)
        if session_date is not None
        else observed_at.astimezone(_MARKET_TZ).date()
    )
    policy_score = score.get("policy_score")
    valid = bool(score.get("score_valid"))
    normalized_score = _finite_number(
        policy_score if valid and policy_score is not None else 0.0,
        "intelligence_score.policy_score",
    )
    features = {
        "score_version": score.get("score_version"),
        "score_config_hash": score.get("score_config_hash"),
        "raw_profile_score": score.get("raw_profile_score"),
        "raw_profile_score_max": score.get("raw_profile_score_max"),
        "score_valid": valid,
        "evidence_coverage": score.get("evidence_coverage"),
        "unavailable_components": list(score.get("unavailable_components") or []),
        "invalid_reasons": list(score.get("invalid_reasons") or []),
        "block_recommendations": list(score.get("block_recommendations") or []),
        "warnings": list(score.get("warnings") or []),
        "components": dict(score.get("components") or {}),
        "ticker": identity.get("ticker"),
        "side": identity.get("side"),
        "execution_mode": execution_mode,
        "data_as_of": score.get("data_as_of"),
        "score_input_hash": score.get("input_hash"),
        "score_source": score.get("source"),
    }
    return {
        "opportunity_id": "|".join((canonical_id, client_id, execution_mode)),
        "client_id": client_id,
        "session_date": market_session.isoformat(),
        "observed_at": observed_at.isoformat(),
        "policy_version": str(score.get("policy_version") or ""),
        "policy_score": normalized_score,
        "eligible": bool(valid and score.get("eligible_for_ranking")),
        "features": features,
        "outcome": dict(outcome) if isinstance(outcome, Mapping) else None,
    }


def opportunity_mapping_from_intelligence_snapshot(
    snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    """Convert a durable intelligence snapshot export without hand-copying fields."""

    if not isinstance(snapshot, Mapping):
        raise ProfitabilityDataError("intelligence snapshot must be an object")
    payload = snapshot.get("payload") if isinstance(snapshot.get("payload"), Mapping) else snapshot
    score = payload.get("intelligence_score") if isinstance(payload, Mapping) else None
    return opportunity_mapping_from_intelligence_score(
        score,
        session_date=snapshot.get("session_date") or payload.get("session_date"),
        outcome=snapshot.get("outcome"),
    )


def opportunity_mapping_from_trade_dossier(dossier: Mapping[str, Any]) -> dict[str, Any]:
    """Convert a trade-dossier export using its attached canonical score."""

    if not isinstance(dossier, Mapping):
        raise ProfitabilityDataError("trade dossier must be an object")
    payload = dossier.get("dossier")
    if not isinstance(payload, Mapping):
        raise ProfitabilityDataError("trade dossier payload is required")
    return opportunity_mapping_from_intelligence_score(
        payload.get("intelligence_score"),
        session_date=dossier.get("trade_date"),
        outcome=dossier.get("outcome"),
    )


def opportunity_from_mapping(row: Mapping[str, Any]) -> Opportunity:
    """Parse one strict opportunity row with features and outcomes separated."""

    if not isinstance(row, Mapping):
        raise ProfitabilityDataError("opportunity row must be an object")
    opportunity_id = str(row.get("opportunity_id") or "").strip()
    client_id = str(row.get("client_id") or "").strip()
    policy_version = str(row.get("policy_version") or "").strip()
    if not opportunity_id:
        raise ProfitabilityDataError("opportunity_id is required")
    if not client_id:
        raise ProfitabilityDataError("client_id is required")
    if not policy_version:
        raise ProfitabilityDataError("policy_version is required")

    observed_at = _aware_datetime(row.get("observed_at"), "observed_at")
    session = _session_date(row.get("session_date"))
    policy_score = _finite_number(row.get("policy_score"), "policy_score")
    if not 0 <= policy_score <= 100:
        raise ProfitabilityDataError("policy_score must be between 0 and 100")
    eligible = row.get("eligible")
    if not isinstance(eligible, bool):
        raise ProfitabilityDataError("eligible must be a boolean")

    features = row.get("features")
    if not isinstance(features, Mapping):
        raise ProfitabilityDataError("features must be an object")
    leaks = _leakage_paths(features)
    if leaks:
        raise ProfitabilityDataError("post-entry fields are forbidden in features: " + ", ".join(sorted(leaks)))

    outcome = row.get("outcome")
    outcome_return_pct: float | None = None
    outcome_known_at: datetime | None = None
    if outcome is not None:
        if not isinstance(outcome, Mapping):
            raise ProfitabilityDataError("outcome must be an object or null")
        outcome_return_pct = _finite_number(outcome.get("return_pct"), "outcome.return_pct")
        if outcome_return_pct < -1:
            raise ProfitabilityDataError("outcome.return_pct cannot be below -1.0")
        outcome_known_at = _aware_datetime(outcome.get("known_at"), "outcome.known_at")
        if outcome_known_at <= observed_at:
            raise ProfitabilityDataError("outcome.known_at must be after observed_at")

    return Opportunity(
        opportunity_id=opportunity_id,
        client_id=client_id,
        session_date=session,
        observed_at=observed_at,
        policy_version=policy_version,
        policy_score=policy_score,
        eligible=eligible,
        features=dict(features),
        outcome_return_pct=outcome_return_pct,
        outcome_known_at=outcome_known_at,
    )


def load_opportunities(rows: Iterable[Mapping[str, Any]]) -> list[Opportunity]:
    opportunities = [opportunity_from_mapping(row) for row in rows]
    seen: set[str] = set()
    duplicates: set[str] = set()
    for item in opportunities:
        if item.opportunity_id in seen:
            duplicates.add(item.opportunity_id)
        seen.add(item.opportunity_id)
    if duplicates:
        raise ProfitabilityDataError("duplicate opportunity_id values: " + ", ".join(sorted(duplicates)))
    return opportunities


def select_frozen_policy_candidates(
    opportunities: Sequence[Opportunity],
    *,
    policy_version: str,
    policy_frozen_at: datetime | str,
    targets: ProfitabilityTargets | None = None,
    min_policy_score: float = 0.0,
) -> tuple[DailySelection, ...]:
    """Select the deterministic top candidates without reading outcomes.

    Only records observed strictly after the policy freeze are eligible for
    holdout evaluation.  Outcome values never participate in sorting.
    """

    cfg = targets or ProfitabilityTargets()
    frozen_at = _aware_datetime(policy_frozen_at, "policy_frozen_at")
    threshold = _finite_number(min_policy_score, "min_policy_score")
    if not 0 <= threshold <= 100:
        raise ProfitabilityDataError("min_policy_score must be between 0 and 100")
    version = str(policy_version or "").strip()
    if not version:
        raise ProfitabilityDataError("policy_version is required")

    grouped: dict[tuple[str, date], list[Opportunity]] = {}
    for item in opportunities:
        if item.policy_version != version or item.observed_at <= frozen_at:
            continue
        grouped.setdefault((item.client_id, item.session_date), []).append(item)

    selections: list[DailySelection] = []
    for (client_id, session), group in sorted(grouped.items(), key=lambda pair: pair[0]):
        eligible = [
            item
            for item in group
            if item.eligible and item.policy_score >= threshold
        ]
        ranked = sorted(
            eligible,
            key=lambda item: (-item.policy_score, item.observed_at, item.opportunity_id),
        )
        selected = tuple(ranked[: cfg.max_daily_selections])
        selections.append(
            DailySelection(
                client_id=client_id,
                session_date=session,
                source_count=len(group),
                eligible_count=len(ranked),
                source=tuple(sorted(group, key=lambda item: (item.observed_at, item.opportunity_id))),
                ranked_eligible=tuple(ranked),
                selected=selected,
            )
        )
    return tuple(selections)


def _wilson_interval(wins: int, total: int, z: float) -> tuple[float | None, float | None]:
    if total <= 0:
        return None, None
    proportion = wins / total
    z2 = z * z
    denominator = 1 + z2 / total
    center = (proportion + z2 / (2 * total)) / denominator
    margin = z * math.sqrt((proportion * (1 - proportion) + z2 / (4 * total)) / total) / denominator
    return max(0.0, center - margin), min(1.0, center + margin)


def _mean_interval(values: Sequence[float], z: float) -> tuple[float | None, float | None, float | None]:
    if not values:
        return None, None, None
    center = mean(values)
    if len(values) == 1:
        return center, None, None
    margin = z * stdev(values) / math.sqrt(len(values))
    return center, center - margin, center + margin


def _correlation(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    left_mean = mean(left)
    right_mean = mean(right)
    left_delta = [value - left_mean for value in left]
    right_delta = [value - right_mean for value in right]
    denominator = math.sqrt(
        sum(value * value for value in left_delta)
        * sum(value * value for value in right_delta)
    )
    if denominator == 0:
        return None
    return sum(a * b for a, b in zip(left_delta, right_delta)) / denominator


def evaluate_profitability(
    selections: Sequence[DailySelection],
    *,
    policy_version: str,
    policy_frozen_at: datetime | str,
    targets: ProfitabilityTargets | None = None,
) -> ProfitabilityReport:
    """Evaluate a frozen policy and return a conservative promotion verdict."""

    cfg = targets or ProfitabilityTargets()
    frozen_at = _aware_datetime(policy_frozen_at, "policy_frozen_at")
    selected = [item for batch in selections for item in batch.selected]
    resolved = [item for item in selected if item.resolved]
    returns = [float(item.outcome_return_pct) for item in resolved if item.outcome_return_pct is not None]
    wins = [value for value in returns if value > 0]
    losses = [-value for value in returns if value < 0]
    breakeven = sum(1 for value in returns if value == 0)

    win_rate = len(wins) / len(returns) if returns else None
    win_low, win_high = _wilson_interval(len(wins), len(returns), cfg.confidence_z)
    avg_win, avg_win_low, _avg_win_high = _mean_interval(wins, cfg.confidence_z)
    avg_loss, _avg_loss_low, avg_loss_high = _mean_interval(losses, cfg.confidence_z)
    expectancy, expectancy_low, _expectancy_high = _mean_interval(returns, cfg.confidence_z)

    sessions = len(selections)
    selected_counts = [batch.selected_count for batch in selections]
    source_counts = [batch.source_count for batch in selections]
    underfilled = sum(1 for count in selected_counts if count < cfg.min_daily_selections)
    source_count = sum(source_counts)
    eligible_count = sum(batch.eligible_count for batch in selections)
    source_opportunities = [item for batch in selections for item in batch.source]
    eligible_opportunities = [item for batch in selections for item in batch.ranked_eligible]
    selected_ids = {item.opportunity_id for item in selected}
    resolved_eligible = [item for item in eligible_opportunities if item.resolved]
    resolved_unselected = [
        item for item in resolved_eligible if item.opportunity_id not in selected_ids
    ]
    unselected_returns = [
        float(item.outcome_return_pct)
        for item in resolved_unselected
        if item.outcome_return_pct is not None
    ]
    unselected_wins = sum(1 for value in unselected_returns if value > 0)
    unselected_win_rate = (
        unselected_wins / len(unselected_returns) if unselected_returns else None
    )
    _unselected_win_low, unselected_win_high = _wilson_interval(
        unselected_wins, len(unselected_returns), cfg.confidence_z
    )
    unselected_expectancy, _unselected_expectancy_low, unselected_expectancy_high = (
        _mean_interval(unselected_returns, cfg.confidence_z)
    )
    selected_win_rate_lift = (
        win_rate - unselected_win_rate
        if win_rate is not None and unselected_win_rate is not None
        else None
    )
    selected_expectancy_lift = (
        expectancy - unselected_expectancy
        if expectancy is not None and unselected_expectancy is not None
        else None
    )
    selected_win_rate_lift_low = (
        win_low - unselected_win_high
        if win_low is not None and unselected_win_high is not None
        else None
    )
    selected_expectancy_lift_low = (
        expectancy_low - unselected_expectancy_high
        if expectancy_low is not None and unselected_expectancy_high is not None
        else None
    )
    score_return_correlation = _correlation(
        [item.policy_score for item in resolved_eligible],
        [float(item.outcome_return_pct) for item in resolved_eligible if item.outcome_return_pct is not None],
    )
    valid_score_count = sum(
        1 for item in source_opportunities
        if item.features.get("score_valid", True) is not False
    )
    score_validity_coverage = valid_score_count / source_count if source_count else None
    eligible_outcome_coverage = (
        len(resolved_eligible) / eligible_count if eligible_count else None
    )
    enough_data = sessions >= cfg.min_holdout_sessions and len(resolved) >= cfg.min_resolved_trades
    outcome_coverage = len(resolved) / len(selected) if selected else None

    checks = {
        "sample_size": enough_data,
        "outcome_coverage": (
            outcome_coverage is not None
            and outcome_coverage >= cfg.min_outcome_coverage
        ),
        "score_validity_coverage": (
            score_validity_coverage is not None
            and score_validity_coverage >= cfg.min_score_coverage
        ),
        "eligible_outcome_coverage": (
            eligible_outcome_coverage is not None
            and eligible_outcome_coverage >= cfg.min_outcome_coverage
        ),
        "unselected_sample_size": (
            len(resolved_unselected) >= cfg.min_resolved_unselected
        ),
        "positive_win_rate_lift": (
            selected_win_rate_lift is not None and selected_win_rate_lift > 0
        ),
        "positive_expectancy_lift": (
            selected_expectancy_lift is not None and selected_expectancy_lift > 0
        ),
        "confidence_positive_win_rate_lift": (
            selected_win_rate_lift_low is not None and selected_win_rate_lift_low > 0
        ),
        "confidence_positive_expectancy_lift": (
            selected_expectancy_lift_low is not None and selected_expectancy_lift_low > 0
        ),
        "positive_score_return_correlation": (
            score_return_correlation is not None and score_return_correlation > 0
        ),
        "daily_selection_cap": all(count <= cfg.max_daily_selections for count in selected_counts),
        "observed_win_rate": win_rate is not None and win_rate >= cfg.target_win_rate,
        "observed_avg_win": avg_win is not None and avg_win >= cfg.min_avg_win_pct,
        "observed_avg_loss": avg_loss is not None and avg_loss <= cfg.max_avg_loss_pct,
        "observed_positive_expectancy": expectancy is not None and expectancy > 0,
        "confidence_win_rate": win_low is not None and win_low >= cfg.target_win_rate,
        "confidence_avg_win": avg_win_low is not None and avg_win_low >= cfg.min_avg_win_pct,
        "confidence_avg_loss": avg_loss_high is not None and avg_loss_high <= cfg.max_avg_loss_pct,
        "confidence_positive_expectancy": expectancy_low is not None and expectancy_low > 0,
    }

    point_checks = (
        checks["observed_win_rate"],
        checks["observed_avg_win"],
        checks["observed_avg_loss"],
        checks["observed_positive_expectancy"],
        checks["daily_selection_cap"],
    )
    confidence_checks = (
        checks["confidence_win_rate"],
        checks["confidence_avg_win"],
        checks["confidence_avg_loss"],
        checks["confidence_positive_expectancy"],
    )

    reasons: list[str] = []
    if not enough_data:
        verdict = "NOT_ENOUGH_DATA"
        if sessions < cfg.min_holdout_sessions:
            reasons.append(f"holdout_sessions_{sessions}_below_{cfg.min_holdout_sessions}")
        if len(resolved) < cfg.min_resolved_trades:
            reasons.append(f"resolved_trades_{len(resolved)}_below_{cfg.min_resolved_trades}")
    elif not (
        checks["outcome_coverage"]
        and checks["score_validity_coverage"]
        and checks["eligible_outcome_coverage"]
    ):
        verdict = "HOLD_DATA_QUALITY"
        if not checks["outcome_coverage"]:
            reasons.append(
                f"outcome_coverage_{(outcome_coverage or 0):.4f}_below_"
                f"{cfg.min_outcome_coverage:.4f}"
            )
        if not checks["score_validity_coverage"]:
            reasons.append(
                f"score_validity_coverage_{(score_validity_coverage or 0):.4f}_below_"
                f"{cfg.min_score_coverage:.4f}"
            )
        if not checks["eligible_outcome_coverage"]:
            reasons.append(
                f"eligible_outcome_coverage_{(eligible_outcome_coverage or 0):.4f}_below_"
                f"{cfg.min_outcome_coverage:.4f}"
            )
    elif not all(point_checks):
        verdict = "HOLD_TARGET_MISSED"
        reasons.extend(name for name, passed in checks.items() if name.startswith("observed_") and not passed)
    elif not all(confidence_checks):
        verdict = "HOLD_UNPROVEN"
        reasons.extend(name for name, passed in checks.items() if name.startswith("confidence_") and not passed)
    elif not all(
        checks[name]
        for name in (
            "unselected_sample_size",
            "positive_win_rate_lift",
            "positive_expectancy_lift",
            "confidence_positive_win_rate_lift",
            "confidence_positive_expectancy_lift",
            "positive_score_return_correlation",
        )
    ):
        verdict = "HOLD_UNPROVEN"
        reasons.extend(
            name
            for name in (
                "unselected_sample_size",
                "positive_win_rate_lift",
                "positive_expectancy_lift",
                "confidence_positive_win_rate_lift",
                "confidence_positive_expectancy_lift",
                "positive_score_return_correlation",
            )
            if not checks[name]
        )
    else:
        verdict = "PAPER_PROMOTION_CANDIDATE"
        reasons.append("all_target_confidence_and_ranking_checks_met")

    if underfilled:
        reasons.append(f"underfilled_sessions_{underfilled}")

    return ProfitabilityReport(
        policy_version=str(policy_version),
        policy_frozen_at=frozen_at.isoformat(),
        sessions=sessions,
        source_opportunities=source_count,
        eligible_opportunities=eligible_count,
        selected_trades=len(selected),
        resolved_trades=len(resolved),
        unresolved_trades=len(selected) - len(resolved),
        outcome_coverage=outcome_coverage,
        valid_score_opportunities=valid_score_count,
        score_validity_coverage=score_validity_coverage,
        resolved_eligible_opportunities=len(resolved_eligible),
        unresolved_eligible_opportunities=eligible_count - len(resolved_eligible),
        eligible_outcome_coverage=eligible_outcome_coverage,
        resolved_unselected_opportunities=len(resolved_unselected),
        unselected_win_rate=unselected_win_rate,
        unselected_win_rate_ci_high=unselected_win_high,
        unselected_expectancy_pct=unselected_expectancy,
        unselected_expectancy_ci_high=unselected_expectancy_high,
        selected_win_rate_lift=selected_win_rate_lift,
        selected_win_rate_lift_ci_low=selected_win_rate_lift_low,
        selected_expectancy_lift_pct=selected_expectancy_lift,
        selected_expectancy_lift_ci_low=selected_expectancy_lift_low,
        score_return_correlation=score_return_correlation,
        wins=len(wins),
        losses=len(losses),
        breakeven=breakeven,
        win_rate=win_rate,
        win_rate_ci_low=win_low,
        win_rate_ci_high=win_high,
        avg_win_pct=avg_win,
        avg_win_ci_low=avg_win_low,
        avg_loss_pct=avg_loss,
        avg_loss_ci_high=avg_loss_high,
        expectancy_pct=expectancy,
        expectancy_ci_low=expectancy_low,
        avg_selected_per_session=mean(selected_counts) if selected_counts else None,
        avg_source_opportunities_per_session=mean(source_counts) if source_counts else None,
        underfilled_sessions=underfilled,
        target_checks=checks,
        verdict=verdict,
        verdict_reasons=tuple(reasons),
    )


def evaluate_frozen_policy(
    rows: Iterable[Mapping[str, Any]],
    *,
    policy_version: str,
    policy_frozen_at: datetime | str,
    targets: ProfitabilityTargets | None = None,
    min_policy_score: float = 0.0,
) -> ProfitabilityReport:
    opportunities = load_opportunities(rows)
    selections = select_frozen_policy_candidates(
        opportunities,
        policy_version=policy_version,
        policy_frozen_at=policy_frozen_at,
        targets=targets,
        min_policy_score=min_policy_score,
    )
    return evaluate_profitability(
        selections,
        policy_version=policy_version,
        policy_frozen_at=policy_frozen_at,
        targets=targets,
    )
