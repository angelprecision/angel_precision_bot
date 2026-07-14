from __future__ import annotations

from datetime import datetime, timezone
import json
from unittest.mock import patch

import pytest

from ap.intelligence_evaluation import evaluate_intelligence
from ap.intelligence_context_materializer import build_intelligence_context_payload
from ap.intelligence_score import (
    INTELLIGENCE_SCORE_VERSION,
    build_intelligence_score,
    score_policy_version,
)
from ap.profitability_objective import (
    load_opportunities,
    opportunity_mapping_from_intelligence_snapshot,
    opportunity_mapping_from_trade_dossier,
    select_frozen_policy_candidates,
)
from ap.trade_dossier import build_and_persist_trade_dossier, build_trade_dossier
from ap.trigger_geometry import score_trigger_geometry
from scripts.profitability_objective_report import _read_jsonl


UTC = timezone.utc
SCORED_AT = "2026-07-14T14:30:00+00:00"


def _signal(**overrides):
    signal = {
        "signal_id": "sig-score-1",
        "canonical_signal_id": "canon-score-1",
        "ticker": "SPY",
        "side": "CALL",
        "score": 90,
        "entry_price": 500.0,
        "stop": 495.0,
        "target": 512.0,
    }
    signal.update(overrides)
    return signal


def _valid_profile(raw_score=96.0):
    maxima = {
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
    return {
        "profile_version": "position_score_profile_v1_observe_only",
        "total_score": raw_score,
        "components": {
            name: {
                "name": name,
                "score": max_score * 0.8,
                "max_score": max_score,
                "status": "ok",
            }
            for name, max_score in maxima.items()
        },
        "block_recommendations": [],
        "warnings": [],
        "observe_only": True,
    }


def _score(raw_score=96.0):
    return build_intelligence_score(
        _valid_profile(raw_score),
        signal=_signal(),
        client_id="account-a",
        execution_mode="PAPER",
        scored_at=SCORED_AT,
        data_as_of="2026-07-14T14:29:59+00:00",
        source="test_snapshot",
        git_commit="abc123",
    )


def test_canonical_score_has_explicit_0_to_100_scale_and_lineage():
    score = _score(96.0)

    assert score["score_valid"] is True
    assert score["policy_score"] == pytest.approx(80.0)
    assert score["raw_profile_score"] == pytest.approx(96.0)
    assert score["raw_profile_score_max"] == pytest.approx(120.0)
    assert score["evidence_coverage"] == pytest.approx(1.0)
    assert score["score_version"] == INTELLIGENCE_SCORE_VERSION
    assert score["policy_version"] == score_policy_version()
    assert len(score["score_config_hash"]) == 16
    assert len(score["input_hash"]) == 64
    assert score["observe_only"] is True
    assert score["affected_eligibility"] is False


def test_missing_required_evidence_is_retained_but_never_rankable():
    profile = _valid_profile()
    profile["components"]["trigger_geometry"]["status"] = "missing_data"
    score = build_intelligence_score(
        profile,
        signal=_signal(),
        client_id="account-a",
        execution_mode="PAPER",
        scored_at=SCORED_AT,
        source="test",
    )

    assert score["score_valid"] is False
    assert score["policy_score"] is None
    assert score["eligible_for_ranking"] is False
    assert "required_component_unavailable:trigger_geometry" in score["invalid_reasons"]


def test_evaluation_keeps_raw_compatibility_and_attaches_canonical_score():
    with patch(
        "ap.intelligence_evaluation._run_position_score_profile",
        return_value=_valid_profile(96.0),
    ):
        result = evaluate_intelligence(
            _signal(), client_id="account-a", execution_mode="PAPER"
        )

    assert result["overall_score"] == pytest.approx(96.0)
    assert result["policy_score"] == pytest.approx(80.0)
    assert result["intelligence_score"]["policy_score"] == pytest.approx(80.0)


def test_pretrigger_snapshot_score_flows_unchanged_into_daily_ranking():
    score = _score(102.0)
    row = opportunity_mapping_from_intelligence_snapshot(
        {"phase": "PRETRIGGER", "payload": {"intelligence_score": score}}
    )
    opportunities = load_opportunities([row])
    selections = select_frozen_policy_candidates(
        opportunities,
        policy_version=score["policy_version"],
        policy_frozen_at=datetime(2026, 7, 13, tzinfo=UTC),
    )

    assert row["policy_score"] == score["policy_score"]
    assert row["features"]["score_input_hash"] == score["input_hash"]
    assert selections[0].source_count == 1
    assert selections[0].selected[0].policy_score == score["policy_score"]


def test_context_materializer_stamps_score_inside_durable_snapshot_payload():
    point_in_time = {
        "collected_at": "2026-07-14T14:29:59+00:00",
        "underlying_observation": {"price": 500.0, "age_seconds": 1.0},
        "data_sources": {},
        "errors": [],
        "provenance": {"underlying": "test"},
    }
    with patch(
        "ap.intelligence_context_materializer.collect_point_in_time_context",
        return_value=point_in_time,
    ), patch(
        "ap.position_score_profile.build_position_score_profile",
        return_value=_valid_profile(96.0),
    ):
        payload = build_intelligence_context_payload(
            _signal(),
            phase="PRETRIGGER",
            client_id="account-a",
            execution_mode="PAPER",
            canonical_signal_id="canon-score-1",
        )

    assert payload["intelligence_score"]["policy_score"] == pytest.approx(80.0)
    assert payload["intelligence_score"]["source"] == "intelligence_context_pretrigger"
    assert payload["component_statuses"]["intelligence_score"] == "AVAILABLE"
    assert payload["position_score_profile"]["total_score"] == pytest.approx(96.0)


def test_invalid_snapshot_still_counts_in_source_population_but_is_not_selected():
    score = _score()
    score["score_valid"] = False
    score["eligible_for_ranking"] = False
    score["policy_score"] = None
    score["invalid_reasons"] = ["evidence_coverage_below_minimum"]
    row = opportunity_mapping_from_intelligence_snapshot(
        {"payload": {"intelligence_score": score}}
    )
    selections = select_frozen_policy_candidates(
        load_opportunities([row]),
        policy_version=score["policy_version"],
        policy_frozen_at=datetime(2026, 7, 13, tzinfo=UTC),
    )

    assert selections[0].source_count == 1
    assert selections[0].eligible_count == 0
    assert selections[0].selected == ()


def test_dossier_reuses_matching_durable_score_and_exports_same_policy_value():
    score = _score(90.0)
    dossier = build_trade_dossier(
        _signal(),
        client_id="account-a",
        execution_mode="PAPER",
        decision_context={"intelligence_score": score},
    )
    row = opportunity_mapping_from_trade_dossier(dossier)

    assert dossier["dossier"]["intelligence_score"] == score
    assert row["policy_score"] == score["policy_score"]
    assert row["policy_version"] == score["policy_version"]


def test_dossier_refuses_cross_account_score_attachment():
    wrong_score = _score()
    wrong_score["identity"] = {**wrong_score["identity"], "client_id": "other-account"}
    dossier = build_trade_dossier(
        _signal(),
        client_id="account-a",
        execution_mode="PAPER",
        decision_context={"intelligence_score": wrong_score},
    )

    attached = dossier["dossier"]["intelligence_score"]
    assert attached != wrong_score
    assert attached["identity"]["client_id"] == "account-a"
    assert attached["source"] == "trade_dossier_fallback"


def test_dossier_writer_reads_matching_durable_pretrigger_score():
    score = _score(90.0)

    class _Conn:
        def __init__(self):
            self.calls = []

        def execute(self, sql, params=None):
            self.calls.append((sql, params))

        def fetchone(self):
            return {"payload": {"intelligence_score": score}}

    captured = {}
    with patch(
        "ap.trade_dossier.persist_trade_dossier",
        side_effect=lambda conn, dossier: captured.setdefault("dossier", dossier) or True,
    ):
        build_and_persist_trade_dossier(
            _Conn(),
            _signal(),
            client_id="account-a",
            execution_mode="PAPER",
        )

    assert captured["dossier"]["dossier"]["intelligence_score"] == score


def test_failed_snapshot_lookup_rolls_back_savepoint_before_dossier_write():
    class _Conn:
        def __init__(self):
            self.aborted = False
            self.calls = []

        def execute(self, sql, params=None):
            self.calls.append((sql, params))
            normalized = " ".join(str(sql).split()).upper()
            if normalized.startswith("ROLLBACK TO SAVEPOINT"):
                self.aborted = False
                return
            if self.aborted:
                raise RuntimeError("transaction is aborted")
            if "FROM AP_INTELLIGENCE_SNAPSHOTS" in normalized:
                self.aborted = True
                raise RuntimeError("snapshot table unavailable")

        def fetchall(self):
            from ap.trade_dossier import REQUIRED_COLUMNS

            return [{"column_name": name} for name in REQUIRED_COLUMNS]

    conn = _Conn()
    build_and_persist_trade_dossier(
        conn,
        _signal(),
        client_id="account-a",
        execution_mode="PAPER",
    )

    sql_calls = [" ".join(str(sql).split()).upper() for sql, _ in conn.calls]
    assert any(call.startswith("ROLLBACK TO SAVEPOINT") for call in sql_calls)
    assert any("INSERT INTO TRADE_DOSSIERS" in call for call in sql_calls)


def test_profile_consumes_upstream_result_without_recomputing_module():
    from ap.position_score_profile import build_position_score_profile

    upstream_vwap = {
        "score": 7.0,
        "max_score": 10.0,
        "status": "ok",
        "missing_data": [],
        "block_recommendations": [],
        "warnings": [],
        "diagnostics": {"vwap": 499.0},
    }
    context = {
        "_intelligence_upstream": {
            "vwap_context": {"provided": True, "raw": upstream_vwap}
        }
    }
    with patch(
        "ap.position_score_profile.score_vwap_context",
        side_effect=AssertionError("vwap scorer ran twice"),
    ):
        profile = build_position_score_profile(_signal(), context)

    assert profile["components"]["vwap_context"]["score"] == pytest.approx(7.0)


def test_geometry_accepts_production_scalar_stop_and_target_aliases():
    result = score_trigger_geometry(_signal(), "CALL")

    assert result["status"] == "ok"
    assert result["stop"] == pytest.approx(495.0)
    assert result["target"] == pytest.approx(512.0)


def test_report_reader_accepts_snapshot_export_without_manual_field_copy(tmp_path):
    source = tmp_path / "snapshots.jsonl"
    source.write_text(
        json.dumps({"phase": "PRETRIGGER", "payload": {"intelligence_score": _score()}})
        + "\n",
        encoding="utf-8",
    )

    rows = _read_jsonl(source, "auto")

    assert rows[0]["policy_score"] == pytest.approx(80.0)
    assert rows[0]["policy_version"] == score_policy_version()
