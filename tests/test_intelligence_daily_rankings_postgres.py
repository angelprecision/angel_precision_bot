from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg2
import psycopg2.extras
import pytest

from ap.intelligence_daily_rankings import freeze_daily_rankings
from ap.intelligence_score import EXPECTED_COMPONENT_MAXIMA, build_intelligence_score
from ap.position_score_profile import PROFILE_VERSION


DATABASE_URL = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL", "")
pytestmark = pytest.mark.skipif(not DATABASE_URL, reason="disposable PostgreSQL URL not configured")
REPO_ROOT = Path(__file__).resolve().parents[1]


class _Cursor:
    def __init__(self, cursor):
        self.cursor = cursor

    @property
    def rowcount(self):
        return self.cursor.rowcount

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params)
        return self

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()


def _valid_score():
    components = {
        name: {
            "name": name,
            "score": 8.0 if name == "scanner_quality" else 0.0,
            "max_score": maximum,
            "status": "diagnostic_only" if name == "historical_feedback" else "ok",
        }
        for name, maximum in EXPECTED_COMPONENT_MAXIMA.items()
    }
    return build_intelligence_score(
        {
            "profile_version": PROFILE_VERSION,
            "observe_only": True,
            "total_score": 8.0,
            "base_score": 8.0,
            "bonus_score": 0.0,
            "penalty_score": 0.0,
            "components": components,
            "block_recommendations": [],
            "warnings": [],
            "diagnostics": {"rank_excluded_components": ["historical_feedback"]},
        },
        signal={
            "signal_id": "signal-pg",
            "canonical_signal_id": "canonical-pg",
            "ticker": "SPY",
            "side": "CALL",
        },
        client_id="client@example.com",
        execution_mode="PAPER",
        scored_at="2026-07-19T14:30:00+00:00",
        data_as_of="2026-07-19T14:29:59+00:00",
        source="postgres_test",
    )


def test_postgres_freeze_is_persisted_once_with_snapshot_lineage():
    connection = psycopg2.connect(DATABASE_URL)
    connection.autocommit = True
    with connection.cursor() as raw:
        raw.execute((REPO_ROOT / "migrations" / "20260712_intelligence_context_snapshots.sql").read_text())
        raw.execute((REPO_ROOT / "migrations" / "20260719_intelligence_daily_rankings.sql").read_text())
        raw.execute(
            "TRUNCATE ap_daily_opportunity_outcomes, ap_daily_opportunity_rankings, "
            "ap_daily_ranking_runs, ap_intelligence_jobs, ap_intelligence_snapshots CASCADE"
        )
    connection.autocommit = False
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    wrapped = _Cursor(cursor)
    try:
        score = _valid_score()
        payload = {
            "client_id": "client@example.com",
            "execution_mode": "PAPER",
            "canonical_signal_id": "canonical-pg",
            "signal_id": "signal-pg",
            "ticker": "SPY",
            "side": "CALL",
            "intelligence_score": score,
        }
        wrapped.execute(
            """
            INSERT INTO ap_intelligence_snapshots (
              client_id, execution_mode, canonical_signal_id, signal_id,
              phase, context_revision, profile_version, input_hash,
              data_as_of, status, payload, computed_at, created_at
            ) VALUES (%s,%s,%s,%s,'PRETRIGGER',1,%s,%s,%s,'COMPLETE',%s::jsonb,%s,%s)
            """,
            (
                "client@example.com", "PAPER", "canonical-pg", "signal-pg",
                PROFILE_VERSION, score["input_hash"], score["data_as_of"],
                json.dumps(payload), score["scored_at"], score["scored_at"],
            ),
        )
        first = freeze_daily_rankings(
            wrapped,
            client_id="client@example.com",
            execution_mode="PAPER",
            session_date="2026-07-19",
            policy_frozen_at="2026-07-19T00:00:00+00:00",
            selection_frozen_at="2026-07-19T15:00:00+00:00",
        )
        second = freeze_daily_rankings(
            wrapped,
            client_id="client@example.com",
            execution_mode="PAPER",
            session_date="2026-07-19",
            policy_frozen_at="2026-07-19T00:00:00+00:00",
            selection_frozen_at="2026-07-19T15:01:00+00:00",
        )
        wrapped.execute("SELECT count(*)::int AS count FROM ap_daily_opportunity_rankings")
        count = wrapped.fetchone()["count"]

        assert first["created"] is True
        assert first["selected_count"] == 1
        assert second["created"] is False
        assert count == 1
    finally:
        connection.rollback()
        cursor.close()
        connection.close()
