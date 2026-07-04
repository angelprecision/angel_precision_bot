from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_counterfactual_tracker",
)

from ap.counterfactual_tracker import (  # noqa: E402
    RESOLUTION_NEITHER_EOD,
    RESOLUTION_STOP_FIRST,
    RESOLUTION_TARGET_FIRST,
    RESOLUTION_UNAVAILABLE,
    RESOLUTION_UNKNOWN,
    build_weekly_counterfactual_rollup,
    resolve_pending_counterfactuals,
    track_counterfactual_signal,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MC_SRC = (REPO_ROOT / "ap_master_control.py").read_text()
QUEUE_SRC = (REPO_ROOT / "ap" / "queue.py").read_text()
APP_SRC = (REPO_ROOT / "app.py").read_text()


class FakeCounterfactualDB:
    def __init__(self):
        self.rows: list[dict] = []
        self._next_id = 1
        self._result = []

    def __call__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        params = params or ()
        if "INSERT INTO public.blocked_signal_counterfactuals" in sql:
            (
                signal_id,
                canonical_signal_id,
                client_id,
                execution_mode,
                ticker,
                direction,
                block_stage,
                block_reason,
                reason_code,
                blocked_at,
                entry_ref,
                target_ref,
                stop_ref,
                resolution,
                meta_json,
            ) = params
            key = (signal_id, client_id, execution_mode)
            if not any((r["signal_id"], r["client_id"], r["execution_mode"]) == key for r in self.rows):
                self.rows.append(
                    {
                        "id": self._next_id,
                        "signal_id": signal_id,
                        "canonical_signal_id": canonical_signal_id,
                        "client_id": client_id,
                        "execution_mode": execution_mode,
                        "ticker": ticker,
                        "direction": direction,
                        "block_stage": block_stage,
                        "block_reason": block_reason,
                        "reason_code": reason_code,
                        "blocked_at": blocked_at,
                        "entry_ref": entry_ref,
                        "target_ref": target_ref,
                        "stop_ref": stop_ref,
                        "resolution": resolution,
                        "hypothetical_r": None,
                        "resolved_at": None,
                        "meta": json.loads(meta_json),
                    }
                )
                self._next_id += 1
            self._result = []
            return self

        if "SELECT *" in sql and "FROM public.blocked_signal_counterfactuals" in sql:
            rows = list(self.rows)
            if "client_id = %s" in sql:
                rows = [r for r in rows if r["client_id"] == params[0]]
                if "execution_mode = %s" in sql:
                    rows = [r for r in rows if r["execution_mode"] == params[1]]
            elif "execution_mode = %s" in sql:
                rows = [r for r in rows if r["execution_mode"] == params[0]]
            rows = [r for r in rows if not r.get("resolution")]
            self._result = [dict(r) for r in rows]
            return self

        if "SET resolution=%s" in sql:
            resolution, hypothetical_r, resolved_at, meta_json, row_id = params
            row = next(r for r in self.rows if r["id"] == row_id)
            row["resolution"] = resolution
            row["hypothetical_r"] = hypothetical_r
            row["resolved_at"] = resolved_at
            row["meta"] = json.loads(meta_json)
            self._result = []
            return self

        if "SET meta=%s::jsonb" in sql:
            meta_json, row_id = params
            row = next(r for r in self.rows if r["id"] == row_id)
            row["meta"] = json.loads(meta_json)
            self._result = []
            return self

        if "GROUP BY COALESCE(NULLIF(reason_code" in sql:
            unknown_resolution, start_dt, end_dt = params
            buckets = {}
            for row in self.rows:
                blocked_at = row["blocked_at"]
                if not (start_dt <= blocked_at < end_dt):
                    continue
                reason_code = row["reason_code"] or "unknown"
                bucket = buckets.setdefault(
                    reason_code,
                    {"reason_code": reason_code, "count": 0, "saved_r": 0.0, "cost_r": 0.0, "unknown_count": 0},
                )
                bucket["count"] += 1
                r_value = row.get("hypothetical_r")
                if r_value is not None and float(r_value) < 0:
                    bucket["saved_r"] += abs(float(r_value))
                if r_value is not None and float(r_value) > 0:
                    bucket["cost_r"] += float(r_value)
                if row.get("resolution") == unknown_resolution:
                    bucket["unknown_count"] += 1
            self._result = sorted(buckets.values(), key=lambda item: (-item["count"], item["reason_code"]))
            return self

        raise AssertionError(f"Unhandled SQL in test fake: {sql}")

    def fetchall(self):
        return list(self._result)


def _signal(**overrides):
    payload = {
        "signal_id": "sig-1",
        "canonical_signal_id": "canon-1",
        "ticker": "AAPL",
        "side": "CALL",
        "entry_price": 100.0,
        "target_price": 110.0,
        "stop_price": 95.0,
        "timeframe": "1d",
    }
    payload.update(overrides)
    return payload


def _bar(ts: str, *, open: float, high: float, low: float, close: float) -> dict:
    return {
        "time": ts,
        "open": open,
        "high": high,
        "low": low,
        "close": close,
    }


def test_master_control_has_counterfactual_block_hook():
    assert "track_counterfactual_signal(" in MC_SRC
    assert 'signal["signal_id"] = signal_id' in MC_SRC
    assert 'self._counterfactual_ctx.signal["signal_id"] = signal_id' in MC_SRC
    assert MC_SRC.index('signal["signal_id"] = signal_id') < MC_SRC.index("self._counterfactual_ctx.signal = dict(signal or {})")


def test_queue_after_hours_watch_has_counterfactual_hook():
    assert 'source="watch"' in QUEUE_SRC
    assert 'reason_code="market_closed_deferred"' in QUEUE_SRC
    assert 'payload_copy = dict(payload or {})' in QUEUE_SRC
    assert 'payload_copy["signal_id"] = signal_id' in QUEUE_SRC
    assert "signal=payload_copy" in QUEUE_SRC


def test_weekly_rollup_response_includes_counterfactual_summary():
    assert 'public["counterfactual_summary"]' in APP_SRC


def test_block_inserts_row():
    fake_db = FakeCounterfactualDB()
    inserted = track_counterfactual_signal(
        signal=_signal(),
        client_id="client@example.com",
        execution_mode="LIVE",
        block_stage="blocked_risk",
        block_reason="capital_limit",
        reason_code="CAPITAL_UTIL_BLOCK",
        conn_factory=fake_db,
    )
    assert inserted is True
    assert len(fake_db.rows) == 1
    row = fake_db.rows[0]
    assert row["signal_id"] == "sig-1"
    assert row["execution_mode"] == "LIVE"
    assert row["entry_ref"] == 100.0
    assert row["target_ref"] == 110.0
    assert row["stop_ref"] == 95.0
    assert row["resolution"] is None


def test_duplicate_block_is_idempotent():
    fake_db = FakeCounterfactualDB()
    kwargs = dict(
        signal=_signal(),
        client_id="client@example.com",
        execution_mode="LIVE",
        block_stage="blocked_risk",
        block_reason="capital_limit",
        reason_code="CAPITAL_UTIL_BLOCK",
        conn_factory=fake_db,
    )
    track_counterfactual_signal(**kwargs)
    track_counterfactual_signal(**kwargs)
    assert len(fake_db.rows) == 1


def test_missing_levels_marked_unavailable():
    fake_db = FakeCounterfactualDB()
    track_counterfactual_signal(
        signal=_signal(target_price=None),
        client_id="client@example.com",
        execution_mode="paper",
        block_stage="blocked_score",
        block_reason="score_low",
        reason_code="REJECTED_LOW_SCORE",
        conn_factory=fake_db,
    )
    row = fake_db.rows[0]
    assert row["resolution"] == RESOLUTION_UNAVAILABLE
    assert row["meta"]["unavailable_reason"] == "missing_or_invalid_levels"
    assert "target_ref" in row["meta"]["missing_levels"]


def test_execution_mode_preserved_exactly():
    fake_db = FakeCounterfactualDB()
    track_counterfactual_signal(
        signal=_signal(),
        client_id="client@example.com",
        execution_mode="LIVE",
        block_stage="blocked_risk",
        block_reason="capital_limit",
        reason_code="CAPITAL_UTIL_BLOCK",
        conn_factory=fake_db,
    )
    assert fake_db.rows[0]["execution_mode"] == "LIVE"


def test_target_first_fixture(monkeypatch):
    fake_db = FakeCounterfactualDB()
    track_counterfactual_signal(
        signal=_signal(),
        client_id="client@example.com",
        execution_mode="LIVE",
        block_stage="blocked_risk",
        block_reason="capital_limit",
        reason_code="CAPITAL_UTIL_BLOCK",
        blocked_at=datetime(2026, 7, 1, 15, 0, tzinfo=timezone.utc),
        conn_factory=fake_db,
    )
    monkeypatch.setattr(
        "ap.counterfactual_tracker.fetch_underlying_bars_for_session",
        lambda **_: [
            _bar("2026-07-01T15:01:00+00:00", open=100.0, high=104.0, low=99.5, close=103.0),
            _bar("2026-07-01T15:02:00+00:00", open=103.0, high=110.5, low=102.0, close=110.0),
        ],
    )
    summary = resolve_pending_counterfactuals(
        broker=object(),
        client_id="client@example.com",
        execution_mode="LIVE",
        conn_factory=fake_db,
    )
    row = fake_db.rows[0]
    assert summary["resolved"] == 1
    assert row["resolution"] == RESOLUTION_TARGET_FIRST
    assert float(row["hypothetical_r"]) == 2.0


def test_stop_first_fixture(monkeypatch):
    fake_db = FakeCounterfactualDB()
    track_counterfactual_signal(
        signal=_signal(),
        client_id="client@example.com",
        execution_mode="PAPER",
        block_stage="blocked_risk",
        block_reason="capital_limit",
        reason_code="CAPITAL_UTIL_BLOCK",
        blocked_at=datetime(2026, 7, 1, 15, 0, tzinfo=timezone.utc),
        conn_factory=fake_db,
    )
    monkeypatch.setattr(
        "ap.counterfactual_tracker.fetch_underlying_bars_for_session",
        lambda **_: [
            _bar("2026-07-01T15:01:00+00:00", open=100.0, high=101.0, low=97.0, close=98.0),
            _bar("2026-07-01T15:02:00+00:00", open=98.0, high=99.0, low=94.5, close=95.0),
        ],
    )
    resolve_pending_counterfactuals(
        broker=object(),
        client_id="client@example.com",
        execution_mode="PAPER",
        conn_factory=fake_db,
    )
    row = fake_db.rows[0]
    assert row["resolution"] == RESOLUTION_STOP_FIRST
    assert float(row["hypothetical_r"]) == -1.0


def test_gap_through_fixture(monkeypatch):
    fake_db = FakeCounterfactualDB()
    track_counterfactual_signal(
        signal=_signal(),
        client_id="client@example.com",
        execution_mode="LIVE",
        block_stage="blocked_risk",
        block_reason="capital_limit",
        reason_code="CAPITAL_UTIL_BLOCK",
        blocked_at=datetime(2026, 7, 1, 15, 0, tzinfo=timezone.utc),
        conn_factory=fake_db,
    )
    monkeypatch.setattr(
        "ap.counterfactual_tracker.fetch_underlying_bars_for_session",
        lambda **_: [
            _bar("2026-07-01T15:01:00+00:00", open=94.0, high=95.5, low=93.0, close=94.5),
        ],
    )
    resolve_pending_counterfactuals(
        broker=object(),
        client_id="client@example.com",
        execution_mode="LIVE",
        conn_factory=fake_db,
    )
    row = fake_db.rows[0]
    assert row["resolution"] == RESOLUTION_STOP_FIRST
    assert float(row["hypothetical_r"]) == -1.0


def test_neither_eod_uses_last_close(monkeypatch):
    fake_db = FakeCounterfactualDB()
    track_counterfactual_signal(
        signal=_signal(),
        client_id="client@example.com",
        execution_mode="PAPER",
        block_stage="blocked_score",
        block_reason="score_low",
        reason_code="REJECTED_LOW_SCORE",
        blocked_at=datetime(2026, 7, 1, 19, 58, tzinfo=timezone.utc),
        conn_factory=fake_db,
    )
    monkeypatch.setattr(
        "ap.counterfactual_tracker.fetch_underlying_bars_for_session",
        lambda **_: [
            _bar("2026-07-01T19:59:00+00:00", open=100.0, high=104.0, low=99.0, close=103.5),
        ],
    )
    resolve_pending_counterfactuals(
        broker=object(),
        client_id="client@example.com",
        execution_mode="PAPER",
        conn_factory=fake_db,
    )
    row = fake_db.rows[0]
    assert row["resolution"] == RESOLUTION_NEITHER_EOD
    assert float(row["hypothetical_r"]) == 0.7


def test_resolver_retry_cap(monkeypatch):
    fake_db = FakeCounterfactualDB()
    track_counterfactual_signal(
        signal=_signal(),
        client_id="client@example.com",
        execution_mode="LIVE",
        block_stage="blocked_system",
        block_reason="snapshot_unavailable",
        reason_code="SNAPSHOT_UNAVAILABLE_LIVE_BLOCKED",
        blocked_at=datetime(2026, 7, 1, 15, 0, tzinfo=timezone.utc),
        conn_factory=fake_db,
    )
    monkeypatch.setattr(
        "ap.counterfactual_tracker.fetch_underlying_bars_for_session",
        lambda **_: [],
    )
    for _ in range(3):
        resolve_pending_counterfactuals(
            broker=object(),
            client_id="client@example.com",
            execution_mode="LIVE",
            conn_factory=fake_db,
        )
    row = fake_db.rows[0]
    assert row["resolution"] == RESOLUTION_UNKNOWN
    assert row["meta"]["resolution_attempts"] == 3


def test_target_hit_before_blocked_at_is_ignored(monkeypatch):
    fake_db = FakeCounterfactualDB()
    track_counterfactual_signal(
        signal=_signal(),
        client_id="client@example.com",
        execution_mode="LIVE",
        block_stage="blocked_risk",
        block_reason="capital_limit",
        reason_code="CAPITAL_UTIL_BLOCK",
        blocked_at=datetime(2026, 7, 1, 15, 1, 30, tzinfo=timezone.utc),
        conn_factory=fake_db,
    )
    monkeypatch.setattr(
        "ap.counterfactual_tracker.fetch_underlying_bars_for_session",
        lambda **_: [
            _bar("2026-07-01T15:01:00+00:00", open=100.0, high=110.5, low=99.0, close=110.0),
            _bar("2026-07-01T15:02:00+00:00", open=99.0, high=100.0, low=94.5, close=95.0),
        ],
    )
    resolve_pending_counterfactuals(
        broker=object(),
        client_id="client@example.com",
        execution_mode="LIVE",
        conn_factory=fake_db,
    )
    row = fake_db.rows[0]
    assert row["resolution"] == RESOLUTION_STOP_FIRST
    assert float(row["hypothetical_r"]) == -1.0


def test_target_hit_after_blocked_at_counts(monkeypatch):
    fake_db = FakeCounterfactualDB()
    track_counterfactual_signal(
        signal=_signal(),
        client_id="client@example.com",
        execution_mode="LIVE",
        block_stage="blocked_risk",
        block_reason="capital_limit",
        reason_code="CAPITAL_UTIL_BLOCK",
        blocked_at=datetime(2026, 7, 1, 15, 0, 30, tzinfo=timezone.utc),
        conn_factory=fake_db,
    )
    monkeypatch.setattr(
        "ap.counterfactual_tracker.fetch_underlying_bars_for_session",
        lambda **_: [
            _bar("2026-07-01T15:00:00+00:00", open=100.0, high=104.0, low=99.5, close=103.0),
            _bar("2026-07-01T15:01:00+00:00", open=103.0, high=110.5, low=102.0, close=110.0),
        ],
    )
    resolve_pending_counterfactuals(
        broker=object(),
        client_id="client@example.com",
        execution_mode="LIVE",
        conn_factory=fake_db,
    )
    row = fake_db.rows[0]
    assert row["resolution"] == RESOLUTION_TARGET_FIRST
    assert float(row["hypothetical_r"]) == 2.0


def test_after_hours_row_resolves_against_next_session(monkeypatch):
    fake_db = FakeCounterfactualDB()
    captured = {}
    track_counterfactual_signal(
        signal=_signal(),
        client_id="client@example.com",
        execution_mode="PAPER",
        block_stage="contract_selection",
        block_reason="market_closed_deferred",
        reason_code="market_closed_deferred",
        blocked_at=datetime(2026, 7, 1, 21, 0, tzinfo=timezone.utc),
        conn_factory=fake_db,
    )

    def _fetch(**kwargs):
        captured["session_date"] = kwargs["session_date"]
        return [
            _bar("2026-07-02T13:30:00+00:00", open=100.0, high=104.0, low=99.5, close=103.0),
            _bar("2026-07-02T13:31:00+00:00", open=103.0, high=110.5, low=102.0, close=110.0),
        ]

    monkeypatch.setattr("ap.counterfactual_tracker.fetch_underlying_bars_for_session", _fetch)
    resolve_pending_counterfactuals(
        broker=object(),
        client_id="client@example.com",
        execution_mode="PAPER",
        conn_factory=fake_db,
    )
    row = fake_db.rows[0]
    assert str(captured["session_date"]) == "2026-07-02"
    assert row["resolution"] == RESOLUTION_TARGET_FIRST


def test_weekly_rollup_aggregates_saved_and_cost_r():
    fake_db = FakeCounterfactualDB()
    now = datetime.now(timezone.utc)
    fake_db.rows.extend(
        [
            {
                "id": 1,
                "signal_id": "a",
                "canonical_signal_id": None,
                "client_id": "client@example.com",
                "execution_mode": "live",
                "ticker": "AAPL",
                "direction": "CALL",
                "block_stage": "blocked_risk",
                "block_reason": "capital_limit",
                "reason_code": "CAPITAL_UTIL_BLOCK",
                "blocked_at": now,
                "entry_ref": 100.0,
                "target_ref": 110.0,
                "stop_ref": 95.0,
                "resolution": RESOLUTION_STOP_FIRST,
                "hypothetical_r": -1.5,
                "resolved_at": now,
                "meta": {},
            },
            {
                "id": 2,
                "signal_id": "b",
                "canonical_signal_id": None,
                "client_id": "client@example.com",
                "execution_mode": "live",
                "ticker": "AAPL",
                "direction": "CALL",
                "block_stage": "blocked_risk",
                "block_reason": "capital_limit",
                "reason_code": "CAPITAL_UTIL_BLOCK",
                "blocked_at": now + timedelta(minutes=1),
                "entry_ref": 100.0,
                "target_ref": 110.0,
                "stop_ref": 95.0,
                "resolution": RESOLUTION_TARGET_FIRST,
                "hypothetical_r": 2.0,
                "resolved_at": now,
                "meta": {},
            },
            {
                "id": 3,
                "signal_id": "c",
                "canonical_signal_id": None,
                "client_id": "client@example.com",
                "execution_mode": "live",
                "ticker": "AAPL",
                "direction": "CALL",
                "block_stage": "blocked_risk",
                "block_reason": "capital_limit",
                "reason_code": "CAPITAL_UTIL_BLOCK",
                "blocked_at": now + timedelta(minutes=2),
                "entry_ref": 100.0,
                "target_ref": 110.0,
                "stop_ref": 95.0,
                "resolution": RESOLUTION_UNKNOWN,
                "hypothetical_r": None,
                "resolved_at": now,
                "meta": {},
            },
        ]
    )
    summary = build_weekly_counterfactual_rollup(
        date_value=now.date(),
        conn_factory=fake_db,
    )
    assert summary == [
        {
            "reason_code": "CAPITAL_UTIL_BLOCK",
            "count": 3,
            "saved_R": 1.5,
            "cost_R": 2.0,
            "unknown_count": 1,
        }
    ]
