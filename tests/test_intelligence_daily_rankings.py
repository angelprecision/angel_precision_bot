from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from ap.intelligence_daily_rankings import (
    EVALUATION_LIMIT,
    FEED_LIMIT,
    fetch_daily_performance,
    freeze_daily_rankings,
    rank_pretrigger_candidates,
)
from ap.intelligence_score import EXPECTED_COMPONENT_MAXIMA, build_intelligence_score, score_policy_version
from ap.position_score_profile import PROFILE_VERSION


POLICY_FROZEN_AT = "2026-07-19T00:00:00+00:00"


def _score(index: int):
    components = {
        name: {
            "name": name,
            "score": float(index) if name == "scanner_quality" else 0.0,
            "max_score": maximum,
            "status": "diagnostic_only" if name == "historical_feedback" else "ok",
        }
        for name, maximum in EXPECTED_COMPONENT_MAXIMA.items()
    }
    profile = {
        "profile_version": PROFILE_VERSION,
        "observe_only": True,
        "total_score": float(index),
        "base_score": float(index),
        "bonus_score": 0.0,
        "penalty_score": 0.0,
        "components": components,
        "block_recommendations": [],
        "warnings": [],
        "diagnostics": {"rank_excluded_components": ["historical_feedback"]},
    }
    return build_intelligence_score(
        profile,
        signal={
            "signal_id": f"signal-{index}",
            "canonical_signal_id": f"canonical-{index}",
            "ticker": f"T{index}",
            "side": "CALL",
        },
        client_id="client@example.com",
        execution_mode="PAPER",
        scored_at=f"2026-07-19T14:{index:02d}:00+00:00",
        data_as_of=f"2026-07-19T14:{index:02d}:00+00:00",
        source="test",
    )


def _row(index: int):
    score = _score(index)
    return {
        "client_id": "client@example.com",
        "execution_mode": "PAPER",
        "canonical_signal_id": f"canonical-{index}",
        "snapshot_id": f"00000000-0000-0000-0000-{index:012d}",
        "payload": {
            "client_id": "client@example.com",
            "execution_mode": "PAPER",
            "canonical_signal_id": f"canonical-{index}",
            "signal_id": f"signal-{index}",
            "ticker": f"T{index}",
            "side": "CALL",
            "trade_geometry": {"entry": 100 + index, "stop": 99, "target": 105},
            "intelligence_score": score,
        },
    }


def test_daily_ranking_is_deterministic_top_10_with_separate_top_7_cohort():
    result = rank_pretrigger_candidates(
        [_row(index) for index in range(1, 13)],
        policy_version=score_policy_version(),
        policy_frozen_at=POLICY_FROZEN_AT,
    )

    assert result["source_count"] == 12
    assert result["eligible_count"] == 12
    assert result["selected_count"] == FEED_LIMIT
    assert result["evaluation_count"] == EVALUATION_LIMIT
    assert [item["canonical_signal_id"] for item in result["selected"]] == [
        f"canonical-{index}" for index in range(12, 2, -1)
    ]
    assert sum(item["selected_for_trade_evaluation"] for item in result["selected"]) == 7
    assert result["selected"][0]["context"]["intelligence_review"]["verdict"] == "RANKABLE"


def test_post_entry_fields_cannot_change_frozen_ranking():
    rows = [_row(index) for index in range(1, 5)]
    baseline = rank_pretrigger_candidates(
        rows,
        policy_version=score_policy_version(),
        policy_frozen_at=POLICY_FROZEN_AT,
    )
    for index, row in enumerate(rows):
        row["payload"]["future_return"] = 1000 - index
        row["payload"]["outcome"] = {"win": index % 2 == 0}
    with_outcomes = rank_pretrigger_candidates(
        rows,
        policy_version=score_policy_version(),
        policy_frozen_at=POLICY_FROZEN_AT,
    )

    assert [item["canonical_signal_id"] for item in baseline["selected"]] == [
        item["canonical_signal_id"] for item in with_outcomes["selected"]
    ]


def test_tampered_score_is_counted_in_population_but_excluded():
    row = _row(5)
    row["payload"]["intelligence_score"]["policy_score"] = 100.0
    result = rank_pretrigger_candidates(
        [row],
        policy_version=score_policy_version(),
        policy_frozen_at=POLICY_FROZEN_AT,
    )

    assert result["source_count"] == 1
    assert result["eligible_count"] == 0
    assert result["selected"] == []


def test_latest_revision_is_authoritative_even_when_older_revision_was_valid():
    valid = _row(5)
    valid["context_revision"] = 1
    latest = _row(5)
    latest["context_revision"] = 2
    latest["payload"]["intelligence_score"]["policy_score"] = 100.0

    result = rank_pretrigger_candidates(
        [valid, latest],
        policy_version=score_policy_version(),
        policy_frozen_at=POLICY_FROZEN_AT,
    )

    assert result["source_count"] == 1
    assert result["eligible_count"] == 0
    assert result["selected"] == []


def test_freeze_persists_one_run_and_at_most_ten_append_only_rows():
    rows = [_row(index) for index in range(1, 13)]

    class _Cursor:
        rowcount = 1

        def __init__(self):
            self.kind = ""
            self.calls = []

        def execute(self, sql, params=None):
            self.calls.append((str(sql), params))
            normalized = " ".join(str(sql).split()).upper()
            if normalized.startswith("SELECT ID, POLICY_VERSION"):
                self.kind = "existing_run"
            elif "FROM AP_INTELLIGENCE_SNAPSHOTS" in normalized:
                self.kind = "source"
            elif normalized.startswith("INSERT INTO AP_DAILY_RANKING_RUNS"):
                self.kind = "insert_run"
            else:
                self.kind = "other"

        def fetchone(self):
            if self.kind == "insert_run":
                return {"id": "inserted"}
            return None

        def fetchall(self):
            return rows if self.kind == "source" else []

    cursor = _Cursor()
    result = freeze_daily_rankings(
        cursor,
        client_id="client@example.com",
        execution_mode="PAPER",
        session_date="2026-07-19",
        policy_frozen_at=POLICY_FROZEN_AT,
        selection_frozen_at="2026-07-19T16:00:00+00:00",
    )

    ranking_inserts = [sql for sql, _ in cursor.calls if "INSERT INTO ap_daily_opportunity_rankings" in sql]
    assert result["created"] is True
    assert result["selected_count"] == 10
    assert len(ranking_inserts) == 10


def test_freeze_waits_for_configured_source_population():
    rows = [_row(index) for index in range(1, 5)]

    class _Cursor:
        def __init__(self):
            self.kind = ""
            self.calls = []

        def execute(self, sql, params=None):
            self.calls.append((str(sql), params))
            normalized = " ".join(str(sql).split()).upper()
            if normalized.startswith("SELECT ID, POLICY_VERSION"):
                self.kind = "existing_run"
            elif "FROM AP_INTELLIGENCE_SNAPSHOTS" in normalized:
                self.kind = "source"
            else:
                self.kind = "other"

        def fetchone(self):
            return None

        def fetchall(self):
            return rows if self.kind == "source" else []

    cursor = _Cursor()
    result = freeze_daily_rankings(
        cursor,
        client_id="client@example.com",
        execution_mode="PAPER",
        session_date="2026-07-19",
        policy_frozen_at=POLICY_FROZEN_AT,
        selection_frozen_at="2026-07-19T15:00:00+00:00",
        minimum_source_count=10,
    )

    assert result["frozen"] is False
    assert result["reason"] == "source_population_below_freeze_minimum"
    assert not any("INSERT INTO ap_daily_ranking_runs" in sql for sql, _ in cursor.calls)


def test_performance_uses_only_persisted_official_return_fraction():
    class _Cursor:
        def execute(self, _sql, _params=None):
            pass

        def fetchall(self):
            return [
                {"return_fraction": 0.20},
                {"return_fraction": 0.18},
                {"return_fraction": -0.10},
            ]

    result = fetch_daily_performance(
        _Cursor(), client_id="client@example.com", session_date="2026-07-19"
    )

    assert result["measurement_class"] == "LIVE_OFFICIAL_ONLY"
    assert result["counterfactuals_included"] is False
    assert result["win_rate"] == pytest.approx(2 / 3)
    assert result["average_win"] == pytest.approx(0.19)
    assert result["average_loss"] == pytest.approx(0.10)


def test_migration_enforces_limits_rls_and_append_only_storage():
    sql = (Path(__file__).resolve().parents[1] / "migrations" / "20260719_intelligence_daily_rankings.sql").read_text()

    assert "selected_count BETWEEN 0 AND 10" in sql
    assert "evaluation_count BETWEEN 0 AND 7" in sql
    assert "ENABLE ROW LEVEL SECURITY" in sql
    assert "BEFORE UPDATE OR DELETE" in sql
    assert "REVOKE ALL" in sql
