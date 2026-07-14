from __future__ import annotations

import json
from pathlib import Path

import pytest

from ap.regression_replay import (
    STAGE_ORDER,
    UNKNOWN,
    FixtureValidationError,
    StageEvent,
    build_fixture_summary,
    compare_traces,
    historical_trace,
    load_fixture_bundle,
    run_replay,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_PATH = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "regression_replay"
    / "june_july_reference_trades.json"
)
MODULE_SOURCE = (REPO_ROOT / "ap" / "regression_replay.py").read_text(encoding="utf-8")


def _fixtures():
    return load_fixture_bundle(FIXTURE_PATH)


def test_reference_bundle_loads_all_required_windows():
    fixtures = _fixtures()
    ids = {fixture.fixture_id for fixture in fixtures}
    assert len(fixtures) == 10
    assert {
        "mnst_call_2026_06_10",
        "rivn_put_win_2026_06_10",
        "bac_call_2026_06_15",
        "c_call_2026_06_18",
        "pypl_call_2026_06_22",
        "nke_put_2026_06_23",
        "rivn_put_loss_2026_06_23",
        "googl_call_2026_07_07",
        "meta_call_2026_07_07",
    }.issubset(ids)


def test_numeric_pnl_is_authority_not_historical_win_flag():
    fixture = next(
        item for item in _fixtures() if item.fixture_id == "wfc_call_data_quality_2026_06_15"
    )
    assert fixture.outcome["proof_win"] is True
    assert fixture.authoritative_pnl_pct == pytest.approx(-1.33)
    assert fixture.derived_win is False


def test_fixture_bundle_has_no_raw_client_email_or_secret_fields():
    raw = FIXTURE_PATH.read_text(encoding="utf-8")
    assert "@" not in raw
    lowered = raw.lower()
    for forbidden in ("api_key", "password", "authorization", "broker_order_id"):
        assert forbidden not in lowered


def test_missing_production_evidence_is_explicit_unknown():
    for fixture in _fixtures():
        for field_name in (
            "execution_mode",
            "signal_id",
            "local_order_id",
            "selected_contract",
            "score",
            "pattern",
        ):
            assert field_name in fixture.evidence
            assert fixture.evidence[field_name] not in (None, "")
        assert fixture.evidence_gaps


def test_deployment_commits_are_evidence_backed_and_current_main_is_exact():
    fixtures = _fixtures()
    assert {fixture.current_main for fixture in fixtures} == {
        "9776cf7fd83e4fb42ecf944f662e283a345735c3"
    }
    commit_sets = {commit for fixture in fixtures for commit in fixture.deployment_git_commits}
    assert {"d12b4fb", "9d716d2", "e76fc42", "6cff00a", "184dcd4"}.issubset(
        commit_sets
    )


def test_historical_trace_preserves_meta_data_degraded_hold_before_late_exit():
    fixture = next(item for item in _fixtures() if item.fixture_id == "meta_call_2026_07_07")
    trace = historical_trace(fixture)
    event = next(event for event in trace.events if event.reason_code == "DATA_DEGRADED_HOLD")
    assert event.stage == "exit_decision"
    assert event.decision == "HOLD"
    assert event.details["repeated_events"] == 15
    assert fixture.authoritative_pnl_pct == pytest.approx(-33.67)


def test_run_replay_marks_unwired_stages_not_run_instead_of_passing():
    fixture = next(item for item in _fixtures() if item.fixture_id == "bac_call_2026_06_15")

    def master_control_runner(view, prior):
        assert view["ticker"] == "BAC"
        assert isinstance(prior, tuple)
        return StageEvent(stage="master_control", decision="APPROVE", reason_code=UNKNOWN)

    trace = run_replay(
        fixture,
        version="candidate",
        stage_runners={"master_control": master_control_runner},
    )
    assert len(trace.events) == len(STAGE_ORDER)
    assert trace.by_stage()["master_control"].decision == "APPROVE"
    assert trace.by_stage()["contract_selector"].decision == "NOT_RUN"
    assert trace.by_stage()["contract_selector"].reason_code == "NO_STAGE_RUNNER"


def test_fixture_view_is_deeply_read_only_for_stage_runners():
    fixture = next(item for item in _fixtures() if item.fixture_id == "bac_call_2026_06_15")

    def mutating_runner(view, prior):
        with pytest.raises(TypeError):
            view["evidence"]["score"] = 100
        return {"stage": "scanner", "decision": "OBSERVED", "reason_code": "RECORDED"}

    trace = run_replay(fixture, version="candidate", stage_runners={"scanner": mutating_runner})
    assert trace.by_stage()["scanner"].decision == "OBSERVED"
    assert fixture.evidence["score"] == UNKNOWN


def test_compare_traces_reports_first_canonical_divergence():
    fixture = next(item for item in _fixtures() if item.fixture_id == "bac_call_2026_06_15")

    def same_runner(stage, decision, reason, contract=UNKNOWN):
        return lambda view, prior: StageEvent(
            stage=stage,
            decision=decision,
            reason_code=reason,
            contract=contract,
        )

    baseline = run_replay(
        fixture,
        version="baseline",
        stage_runners={
            "scanner": same_runner("scanner", "ALLOW", "PATTERN_FOUND"),
            "master_control": same_runner("master_control", "APPROVE", UNKNOWN),
            "contract_selector": same_runner(
                "contract_selector", "ALLOW", "SELECTED", "BAC260626C00056000"
            ),
        },
    )
    candidate = run_replay(
        fixture,
        version="candidate",
        stage_runners={
            "scanner": same_runner("scanner", "ALLOW", "PATTERN_FOUND"),
            "master_control": same_runner("master_control", "REJECT", "REGIME_MISMATCH"),
            "contract_selector": same_runner(
                "contract_selector", "ALLOW", "SELECTED", "BAC260626C00056000"
            ),
        },
    )
    comparison = compare_traces(baseline, candidate)
    assert comparison.changed is True
    assert comparison.first_divergence_stage == "master_control"
    assert comparison.baseline_event.decision == "APPROVE"
    assert comparison.candidate_event.reason_code == "REGIME_MISMATCH"


def test_comparison_ignores_timestamps_but_not_decision_evidence():
    fixture = next(item for item in _fixtures() if item.fixture_id == "bac_call_2026_06_15")
    baseline = historical_trace(fixture)
    shifted = type(baseline)(
        fixture_id=baseline.fixture_id,
        version="same-behavior-new-time",
        events=tuple(
            StageEvent(
                stage=event.stage,
                decision=event.decision,
                reason_code=event.reason_code,
                contract=event.contract,
                timestamp="2099-01-01T00:00:00Z",
                details=event.details,
            )
            for event in baseline.events
        ),
        authoritative_pnl_pct=baseline.authoritative_pnl_pct,
        derived_win=baseline.derived_win,
    )
    assert compare_traces(baseline, shifted).changed is False


def test_invalid_fixture_cannot_hide_missing_critical_evidence(tmp_path):
    raw = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    del raw["fixtures"][0]["evidence"]["execution_mode"]
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(FixtureValidationError, match="execution_mode"):
        load_fixture_bundle(path)


def test_invalid_fixture_rejects_raw_email(tmp_path):
    raw = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    raw["fixtures"][0]["client_alias"] = "person@example.com"
    path = tmp_path / "invalid-email.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(FixtureValidationError, match="raw email"):
        load_fixture_bundle(path)


def test_harness_has_no_runtime_side_effect_imports_or_sql_mutations():
    forbidden_imports = (
        "import requests",
        "import psycopg2",
        "from ap.db",
        "from ap.execution",
        "from ap.order_state_machine",
        "from ap.broker",
    )
    for forbidden in forbidden_imports:
        assert forbidden not in MODULE_SOURCE
    for sql_verb in ("INSERT INTO", "UPDATE orders", "DELETE FROM", "requests.post"):
        assert sql_verb not in MODULE_SOURCE


def test_summary_exposes_data_quality_not_fake_certainty():
    summary = build_fixture_summary(_fixtures())
    assert summary["fixtures"] == 10
    assert summary["wins"] == 4
    assert summary["losses"] == 6
    assert summary["fixtures_with_unknown_execution_mode"] == 10
    assert summary["fixtures_with_missing_identity"] == 10
