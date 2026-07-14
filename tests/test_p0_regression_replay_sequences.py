from __future__ import annotations

from pathlib import Path

from ap.regression_replay import (
    StageEvent,
    compare_traces,
    load_fixture_bundle,
    run_replay,
)

FIXTURE_PATH = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "regression_replay"
    / "june_july_reference_trades.json"
)


def test_repeated_stage_events_are_compared_as_an_ordered_sequence():
    fixture = next(
        item
        for item in load_fixture_bundle(FIXTURE_PATH)
        if item.fixture_id == "pypl_call_2026_06_22"
    )

    def baseline_exit(view, prior):
        return (
            StageEvent(
                stage="exit_decision",
                decision="SUBMIT",
                reason_code="TP_SCALE_OUT",
            ),
            StageEvent(
                stage="exit_decision",
                decision="SUBMIT",
                reason_code="PROFIT_LOCK",
            ),
        )

    def candidate_exit(view, prior):
        return (
            StageEvent(
                stage="exit_decision",
                decision="SUBMIT",
                reason_code="TP_SCALE_OUT",
            ),
            StageEvent(
                stage="exit_decision",
                decision="HOLD",
                reason_code="DATA_DEGRADED_HOLD",
            ),
        )

    baseline = run_replay(
        fixture,
        version="baseline",
        stage_runners={"exit_decision": baseline_exit},
    )
    candidate = run_replay(
        fixture,
        version="candidate",
        stage_runners={"exit_decision": candidate_exit},
    )
    comparison = compare_traces(baseline, candidate)

    assert comparison.first_divergence_stage == "exit_decision"
    assert comparison.baseline_event.reason_code == "PROFIT_LOCK"
    assert comparison.candidate_event.reason_code == "DATA_DEGRADED_HOLD"
    assert len(baseline.events_for_stage("exit_decision")) == 2
