from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from ap.profitability_objective import (
    ProfitabilityDataError,
    ProfitabilityTargets,
    evaluate_frozen_policy,
    load_opportunities,
    opportunity_from_mapping,
    select_frozen_policy_candidates,
)


UTC = timezone.utc
FROZEN_AT = datetime(2026, 1, 1, tzinfo=UTC)


def _row(
    index: int,
    *,
    day: int = 1,
    score: float = 80,
    return_pct: float | None = 0.20,
    policy_version: str = "policy-v1",
    eligible: bool = True,
    client_id: str = "paper-account",
    features: dict | None = None,
) -> dict:
    observed = FROZEN_AT + timedelta(days=day, minutes=index)
    outcome = None
    if return_pct is not None:
        outcome = {
            "return_pct": return_pct,
            "known_at": (observed + timedelta(hours=6)).isoformat(),
        }
    return {
        "opportunity_id": f"opp-{day:03d}-{index:03d}",
        "client_id": client_id,
        "session_date": observed.date().isoformat(),
        "observed_at": observed.isoformat(),
        "policy_version": policy_version,
        "policy_score": score,
        "eligible": eligible,
        "features": features or {"scanner_score": score, "ticker": f"T{index}"},
        "outcome": outcome,
    }


def _policy_rows(*, sessions: int, per_day: int, wins: int, win_return: float, loss_return: float) -> list[dict]:
    rows: list[dict] = []
    total = sessions * per_day
    for flat_index in range(total):
        day = flat_index // per_day + 1
        index = flat_index % per_day
        realized = win_return if flat_index < wins else loss_return
        rows.append(_row(index, day=day, score=100 - index, return_pct=realized))
    return rows


def _ranked_policy_rows(*, sessions: int = 40) -> list[dict]:
    """Ten daily candidates: top seven hit targets; bottom three are controls."""

    rows: list[dict] = []
    selected_index = 0
    for day in range(1, sessions + 1):
        for index in range(10):
            if index < 7:
                realized = 0.22 if selected_index < 252 else -0.08
                selected_index += 1
            else:
                realized = -0.08
            rows.append(
                _row(
                    index,
                    day=day,
                    score=100 - index,
                    return_pct=realized,
                )
            )
    return rows


def test_rejects_nested_post_entry_feature_leakage():
    row = _row(1, features={"market": {"mfe_pct": 0.42}})

    with pytest.raises(ProfitabilityDataError, match=r"features\.market\.mfe_pct"):
        opportunity_from_mapping(row)


@pytest.mark.parametrize("field", ["realized_pnl", "future_return_1d", "outcome_return_pct", "win_flag"])
def test_rejects_common_leakage_field_names(field):
    row = _row(1, features={field: 1})

    with pytest.raises(ProfitabilityDataError, match="post-entry fields are forbidden"):
        opportunity_from_mapping(row)


def test_requires_outcome_to_be_known_after_decision():
    row = _row(1)
    row["outcome"]["known_at"] = row["observed_at"]

    with pytest.raises(ProfitabilityDataError, match="must be after observed_at"):
        opportunity_from_mapping(row)


def test_requires_timezone_aware_decision_timestamp():
    row = _row(1)
    row["observed_at"] = "2026-01-02T14:30:00"

    with pytest.raises(ProfitabilityDataError, match="timezone offset"):
        opportunity_from_mapping(row)


def test_rejects_duplicate_opportunity_identity():
    rows = [_row(1), _row(1)]

    with pytest.raises(ProfitabilityDataError, match="duplicate opportunity_id"):
        load_opportunities(rows)


def test_daily_selection_uses_only_frozen_policy_score_and_caps_at_seven():
    rows = [
        _row(index, score=float(index), return_pct=(-0.90 if index >= 3 else 2.0))
        for index in range(10)
    ]
    opportunities = load_opportunities(rows)

    batches = select_frozen_policy_candidates(
        opportunities,
        policy_version="policy-v1",
        policy_frozen_at=FROZEN_AT,
    )

    assert len(batches) == 1
    assert batches[0].source_count == 10
    assert batches[0].eligible_count == 10
    assert batches[0].selected_count == 7
    assert [item.policy_score for item in batches[0].selected] == [9, 8, 7, 6, 5, 4, 3]


def test_selection_is_deterministic_when_scores_tie():
    rows = [_row(2, score=80), _row(1, score=80), _row(3, score=80)]

    batches = select_frozen_policy_candidates(
        load_opportunities(reversed(rows)),
        policy_version="policy-v1",
        policy_frozen_at=FROZEN_AT,
    )

    assert [item.opportunity_id for item in batches[0].selected] == [
        "opp-001-001",
        "opp-001-002",
        "opp-001-003",
    ]


def test_selection_excludes_pre_freeze_wrong_version_ineligible_and_below_threshold():
    after = _row(1, day=2, score=90)
    pre_freeze = _row(2, day=2, score=99)
    pre_freeze["observed_at"] = (FROZEN_AT - timedelta(minutes=1)).isoformat()
    pre_freeze["session_date"] = (FROZEN_AT - timedelta(minutes=1)).date().isoformat()
    wrong_version = _row(3, day=2, score=98, policy_version="policy-v0")
    ineligible = _row(4, day=2, score=97, eligible=False)
    below_threshold = _row(5, day=2, score=74)

    batches = select_frozen_policy_candidates(
        load_opportunities([after, pre_freeze, wrong_version, ineligible, below_threshold]),
        policy_version="policy-v1",
        policy_frozen_at=FROZEN_AT,
        min_policy_score=75,
    )

    assert [item.opportunity_id for item in batches[0].selected] == [after["opportunity_id"]]


def test_perfect_thin_sample_is_not_enough_data():
    report = evaluate_frozen_policy(
        [_row(index, return_pct=0.25) for index in range(7)],
        policy_version="policy-v1",
        policy_frozen_at=FROZEN_AT,
    )

    assert report.win_rate == 1.0
    assert report.verdict == "NOT_ENOUGH_DATA"
    assert report.target_checks["sample_size"] is False


def test_observed_eighty_percent_is_held_when_confidence_does_not_prove_it():
    rows = _policy_rows(
        sessions=20,
        per_day=5,
        wins=80,
        win_return=0.20,
        loss_return=-0.10,
    )

    report = evaluate_frozen_policy(
        rows,
        policy_version="policy-v1",
        policy_frozen_at=FROZEN_AT,
    )

    assert report.resolved_trades == 100
    assert report.win_rate == pytest.approx(0.80)
    assert report.avg_win_pct == pytest.approx(0.20)
    assert report.avg_loss_pct == pytest.approx(0.10)
    assert report.expectancy_pct == pytest.approx(0.14)
    assert report.target_checks["observed_win_rate"] is True
    assert report.target_checks["confidence_win_rate"] is False
    assert report.verdict == "HOLD_UNPROVEN"


def test_large_strong_holdout_can_only_become_paper_promotion_candidate():
    rows = _ranked_policy_rows()

    report = evaluate_frozen_policy(
        rows,
        policy_version="policy-v1",
        policy_frozen_at=FROZEN_AT,
    )

    assert report.win_rate == pytest.approx(0.90)
    assert report.win_rate_ci_low > 0.80
    assert report.avg_win_ci_low == pytest.approx(0.22)
    assert report.avg_loss_ci_high == pytest.approx(0.08)
    assert report.expectancy_ci_low > 0
    assert report.resolved_unselected_opportunities == 120
    assert report.unselected_win_rate == 0
    assert report.selected_win_rate_lift == pytest.approx(0.90)
    assert report.selected_win_rate_lift_ci_low > 0
    assert report.selected_expectancy_lift_pct > 0
    assert report.selected_expectancy_lift_ci_low > 0
    assert report.score_return_correlation > 0
    assert report.verdict == "PAPER_PROMOTION_CANDIDATE"
    assert report.verdict_reasons == ("all_target_confidence_and_ranking_checks_met",)


def test_target_results_without_unselected_controls_cannot_validate_ranking():
    rows = _policy_rows(
        sessions=40,
        per_day=7,
        wins=252,
        win_return=0.22,
        loss_return=-0.08,
    )

    report = evaluate_frozen_policy(
        rows,
        policy_version="policy-v1",
        policy_frozen_at=FROZEN_AT,
    )

    assert report.verdict == "HOLD_UNPROVEN"
    assert report.target_checks["unselected_sample_size"] is False
    assert "unselected_sample_size" in report.verdict_reasons


def test_missing_unselected_outcomes_cannot_hide_ranking_quality():
    rows = _ranked_policy_rows()
    for row in rows:
        if row["policy_score"] < 94:
            row["outcome"] = None

    report = evaluate_frozen_policy(
        rows,
        policy_version="policy-v1",
        policy_frozen_at=FROZEN_AT,
    )

    assert report.eligible_outcome_coverage == pytest.approx(0.70)
    assert report.verdict == "HOLD_DATA_QUALITY"
    assert report.target_checks["eligible_outcome_coverage"] is False


def test_invalid_scores_cannot_be_hidden_outside_selected_set():
    rows = _ranked_policy_rows()
    for row in rows[-21:]:
        row["eligible"] = False
        row["features"]["score_valid"] = False

    report = evaluate_frozen_policy(
        rows,
        policy_version="policy-v1",
        policy_frozen_at=FROZEN_AT,
    )

    assert report.score_validity_coverage == pytest.approx(379 / 400)
    assert report.verdict == "HOLD_DATA_QUALITY"
    assert report.target_checks["score_validity_coverage"] is False


def test_sufficient_sample_that_misses_loss_target_is_held():
    rows = _policy_rows(
        sessions=40,
        per_day=5,
        wins=180,
        win_return=0.22,
        loss_return=-0.20,
    )

    report = evaluate_frozen_policy(
        rows,
        policy_version="policy-v1",
        policy_frozen_at=FROZEN_AT,
    )

    assert report.target_checks["observed_avg_loss"] is False
    assert report.verdict == "HOLD_TARGET_MISSED"
    assert "observed_avg_loss" in report.verdict_reasons


def test_unresolved_outcomes_cannot_be_hidden_behind_one_hundred_resolved_wins():
    rows = _policy_rows(
        sessions=20,
        per_day=7,
        wins=140,
        win_return=0.22,
        loss_return=-0.08,
    )
    for row in rows[100:]:
        row["outcome"] = None

    report = evaluate_frozen_policy(
        rows,
        policy_version="policy-v1",
        policy_frozen_at=FROZEN_AT,
    )

    assert report.resolved_trades == 100
    assert report.outcome_coverage == pytest.approx(100 / 140)
    assert report.verdict == "HOLD_DATA_QUALITY"
    assert report.target_checks["outcome_coverage"] is False


def test_underfilled_days_are_reported_without_forcing_low_score_candidates():
    targets = ProfitabilityTargets(min_resolved_trades=1, min_holdout_sessions=1)
    rows = [_row(index, score=90 - index) for index in range(3)]

    report = evaluate_frozen_policy(
        rows,
        policy_version="policy-v1",
        policy_frozen_at=FROZEN_AT,
        targets=targets,
    )

    assert report.selected_trades == 3
    assert report.underfilled_sessions == 1
    assert "underfilled_sessions_1" in report.verdict_reasons


def test_report_is_json_serializable_shape():
    report = evaluate_frozen_policy(
        [_row(index) for index in range(5)],
        policy_version="policy-v1",
        policy_frozen_at=FROZEN_AT,
    )

    payload = report.to_dict()
    assert payload["policy_version"] == "policy-v1"
    assert payload["source_opportunities"] == 5
    assert payload["selected_trades"] == 5
    assert isinstance(payload["target_checks"], dict)
