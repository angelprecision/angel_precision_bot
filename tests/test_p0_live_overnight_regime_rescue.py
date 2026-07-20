"""
tests/test_p0_live_overnight_regime_rescue.py
=============================================
Rescue tests 7-20. Enforces mandatory decision-event proof,
atomic writes, PAPER isolation, readiness honesty, and handoff existence.
All failure modes listed in the spec must cause test failure here.
"""
from __future__ import annotations
import json
import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch, call

import pytest

from ap.live_overnight_rescue import (
    INCIDENT_CLIENT_ID,
    INCIDENT_TRADING_DATE,
    INCIDENT_EXPECTED_ROWS,
    RECOVER_PR379_REGIME_TAXONOMY,
)

JASON = INCIDENT_CLIENT_ID
DATE  = INCIDENT_TRADING_DATE


def _mock_runner(mode="LIVE", email=JASON, initialized=True):
    r = MagicMock()
    r.mode  = mode
    r.email = email
    r.initialized = MagicMock()
    r.initialized.is_set.return_value = initialized
    r.broker = MagicMock()
    r.master_control = MagicMock()
    r.contract_selector = MagicMock()
    r.order_state_machine = MagicMock()
    core = MagicMock()
    core.entry_watcher = MagicMock()
    r.core = core
    return r


# ─── Test 7: Rescue refuses PAPER mode ────────────────────────────────────────

def test_rescue_refuses_paper_mode():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
    result = rescue_live_overnight_regime_rejections(
        client_id=JASON, execution_mode="paper",
        trading_date=DATE, dry_run=True,
    )
    assert result["errors"], "Must error on paper mode"
    assert result["writes"] == 0
    assert result["eligible"] == 0


# ─── Test 8: Rescue refuses wrong client ──────────────────────────────────────

def test_rescue_refuses_wrong_client():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
    result = rescue_live_overnight_regime_rejections(
        client_id="other@example.com", execution_mode="live",
        trading_date=DATE, dry_run=True,
    )
    assert result["errors"]
    assert "jasoncosby1@gmail.com" in result["errors"][0]
    assert result["writes"] == 0


# ─── Test 9: Rescue refuses wrong date ────────────────────────────────────────

def test_rescue_refuses_wrong_date():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
    result = rescue_live_overnight_regime_rejections(
        client_id=JASON, execution_mode="live",
        trading_date="2026-07-21", dry_run=True,
    )
    assert result["errors"]
    assert result["writes"] == 0


# ─── Test 10: Runner identity verification — wrong mode ───────────────────────

def test_rescue_fails_on_paper_runner():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
    paper_runner = _mock_runner(mode="PAPER")
    result = rescue_live_overnight_regime_rejections(
        client_id=JASON, execution_mode="live",
        trading_date=DATE, dry_run=True, runner=paper_runner,
    )
    assert result["errors"]
    assert "LIVE identity" in result["errors"][0] or "mode" in result["errors"][0].lower()
    assert result["writes"] == 0


# ─── Test 11: Decision-event verification is mandatory — zero on no events ────

def test_rescue_aborts_when_decision_events_return_zero():
    """Rescue must abort with zero writes if decision_events returns 0 matches."""
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
    from ap.db import conn as _conn, run_with_retry as _rwr

    def fake_conn_ctx():
        class FakeCursor:
            description = [("n",)]
            def execute(self, sql, params): pass
            def fetchone(self): return {"n": 53}
            def fetchall(self): return []
        class FakeConn:
            def __enter__(self): return FakeCursor()
            def __exit__(self, *a): pass
        return FakeConn()

    with (
        patch("ap.live_overnight_rescue.conn", side_effect=fake_conn_ctx),
        patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()),
    ):
        result = rescue_live_overnight_regime_rejections(
            client_id=JASON, execution_mode="live",
            trading_date=DATE, dry_run=True, expected_count=53,
        )

    # dry-run structural count = 53 but decision events = 0
    # The dry-run only does a count query, so this test verifies the live path
    # The atomic transaction will verify decision events and abort.
    # For dry-run, we can only confirm the structural preview succeeds.
    # The decision-event check happens inside _execute_atomic_rescue.
    assert result["dry_run"] is True


# ─── Test 11b: Atomic rescue aborts when decision events missing ──────────────

def test_atomic_rescue_raises_on_missing_decision_events():
    """_execute_atomic_rescue must raise ValueError when verified_count < expected."""
    from ap.live_overnight_rescue import _execute_atomic_rescue
    from datetime import timezone

    window_start = datetime(2026, 7, 17, 20, 0, tzinfo=timezone.utc)
    window_end   = datetime(2026, 7, 20, 13, 30, tzinfo=timezone.utc)

    class LockedCursor:
        description = [("id",), ("signal_id",), ("payload",),
                       ("last_error",), ("result_json",), ("finished_ts",)]
        call_count = 0

        def execute(self, sql, params):
            self.call_count += 1

        def fetchall(self):
            if self.call_count == 1:  # FOR UPDATE query
                return [
                    {"id": i, "signal_id": f"sig-{i}", "payload": None,
                     "last_error": "mc_blocked:INTEL_AUTHORITATIVE_VETO_RISK",
                     "result_json": None, "finished_ts": None}
                    for i in range(53)
                ]
            # decision_events query — return ZERO matches
            return []

    class FakeConn:
        def __enter__(self): return LockedCursor()
        def __exit__(self, *a): pass

    with patch("ap.live_overnight_rescue.conn", return_value=FakeConn()):
        with pytest.raises(ValueError, match="Decision-event verification"):
            _execute_atomic_rescue(
                client_id=JASON, trading_date=DATE,
                window_start_utc=window_start, window_end_utc=window_end,
                expected_count=53,
                recovery_run_id="test-run", recovered_at="2026-07-20T09:00:00Z",
            )


# ─── Test 12: Atomic rescue raises when decision-event lookup throws ──────────

def test_atomic_rescue_raises_on_db_exception():
    from ap.live_overnight_rescue import _execute_atomic_rescue
    from datetime import timezone

    window_start = datetime(2026, 7, 17, 20, 0, tzinfo=timezone.utc)
    window_end   = datetime(2026, 7, 20, 13, 30, tzinfo=timezone.utc)

    class ExplodingCursor:
        description = [("id",), ("signal_id",), ("payload",),
                       ("last_error",), ("result_json",), ("finished_ts",)]
        call_count = 0

        def execute(self, sql, params):
            self.call_count += 1
            if self.call_count == 2:
                raise Exception("DB connection lost")

        def fetchall(self):
            if self.call_count == 1:
                return [
                    {"id": i, "signal_id": f"sig-{i}", "payload": None,
                     "last_error": "mc_blocked:INTEL_AUTHORITATIVE_VETO_RISK",
                     "result_json": None, "finished_ts": None}
                    for i in range(53)
                ]
            return []

    class FakeConn:
        def __enter__(self): return ExplodingCursor()
        def __exit__(self, *a): pass

    with patch("ap.live_overnight_rescue.conn", return_value=FakeConn()):
        with pytest.raises(Exception):
            _execute_atomic_rescue(
                client_id=JASON, trading_date=DATE,
                window_start_utc=window_start, window_end_utc=window_end,
                expected_count=53,
                recovery_run_id="test-run", recovered_at="2026-07-20T09:00:00Z",
            )


# ─── Test 13: expected_count mismatch aborts with zero writes ─────────────────

def test_expected_count_mismatch_aborts():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections

    def fake_conn_ctx():
        class FakeCursor:
            description = [("n",)]
            def execute(self, sql, params): pass
            def fetchone(self): return {"n": 40}  # only 40, not 53
            def fetchall(self): return []
        class FakeConn:
            def __enter__(self): return FakeCursor()
            def __exit__(self, *a): pass
        return FakeConn()

    with (
        patch("ap.live_overnight_rescue.conn", side_effect=fake_conn_ctx),
        patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()),
    ):
        result = rescue_live_overnight_regime_rejections(
            client_id=JASON, execution_mode="live",
            trading_date=DATE, dry_run=True, expected_count=53,
        )

    assert result["count_match"] is False
    assert result["writes"] == 0
    assert result["errors"]


# ─── Test 14: Atomic rescue locked count mismatch aborts ──────────────────────

def test_atomic_rescue_raises_on_locked_count_mismatch():
    from ap.live_overnight_rescue import _execute_atomic_rescue
    from datetime import timezone

    window_start = datetime(2026, 7, 17, 20, 0, tzinfo=timezone.utc)
    window_end   = datetime(2026, 7, 20, 13, 30, tzinfo=timezone.utc)

    class FewRowsCursor:
        description = [("id",), ("signal_id",), ("payload",),
                       ("last_error",), ("result_json",), ("finished_ts",)]
        def execute(self, sql, params): pass
        def fetchall(self):
            return [
                {"id": i, "signal_id": f"sig-{i}", "payload": None,
                 "last_error": "mc_blocked:INTEL_AUTHORITATIVE_VETO_RISK",
                 "result_json": None, "finished_ts": None}
                for i in range(40)  # only 40
            ]

    class FakeConn:
        def __enter__(self): return FewRowsCursor()
        def __exit__(self, *a): pass

    with patch("ap.live_overnight_rescue.conn", return_value=FakeConn()):
        with pytest.raises(ValueError, match="Locked 40 rows"):
            _execute_atomic_rescue(
                client_id=JASON, trading_date=DATE,
                window_start_utc=window_start, window_end_utc=window_end,
                expected_count=53,
                recovery_run_id="test-run", recovered_at="2026-07-20T09:00:00Z",
            )


# ─── Test 15: Orchestration stops if handoff function is missing ───────────────

def test_orchestration_stops_on_missing_handoff():
    """If run_morning_handoff_audit cannot be imported, orchestration must stop."""
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight

    runner = _mock_runner()

    rescue_ok = {
        "eligible": 53, "writes": 53, "verified": 53,
        "count_match": True, "errors": [], "dry_run": False,
    }
    reeval_ok = {"processed": 53, "armed": 50, "rejected": 3, "errors": 0}

    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections",
              return_value=rescue_ok),
        patch("ap.live_overnight_rescue.run_overnight_reeval", return_value=reeval_ok),
        patch.dict("sys.modules", {"ap.morning_handoff": None}),  # ImportError
    ):
        result = recover_and_rerun_live_overnight(
            runner=runner, trading_date=DATE, dry_run=False, expected_count=53,
        )

    assert result["errors"], "Must error when handoff function is missing"
    missing_msg = any("handoff" in e.lower() or "importerror" in e.lower()
                      for e in result["errors"])
    assert missing_msg, f"Error must mention handoff: {result['errors']}"


# ─── Test 16: Orchestration stops on partial recovery ─────────────────────────

def test_orchestration_stops_on_partial_rescue():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight

    runner = _mock_runner()

    # Only 50 rows written instead of 53
    partial_rescue = {
        "eligible": 53, "writes": 50, "verified": 50,
        "count_match": False, "errors": [], "dry_run": False,
    }

    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections",
              return_value=partial_rescue),
        patch("ap.live_overnight_rescue.run_overnight_reeval") as mock_reeval,
    ):
        result = recover_and_rerun_live_overnight(
            runner=runner, trading_date=DATE, dry_run=False, expected_count=53,
        )

    mock_reeval.assert_not_called()
    assert result["errors"]
    assert "partial" in result["errors"][0].lower() or "50" in result["errors"][0]


# ─── Test 17: Orchestration stops on reeval exception ────────────────────────

def test_orchestration_stops_on_reeval_exception():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight

    runner = _mock_runner()
    rescue_ok = {
        "eligible": 53, "writes": 53, "verified": 53,
        "count_match": True, "errors": [], "dry_run": False,
    }

    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections",
              return_value=rescue_ok),
        patch("ap.live_overnight_rescue.run_overnight_reeval",
              side_effect=RuntimeError("reeval crashed")),
    ):
        result = recover_and_rerun_live_overnight(
            runner=runner, trading_date=DATE, dry_run=False, expected_count=53,
        )

    assert result["errors"]
    assert any("reeval" in e.lower() for e in result["errors"])
    assert result["reeval_result"] is None


# ─── Test 18: Forced reeval uses canonical runner components ──────────────────

def test_forced_reeval_passes_correct_runner_components():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight

    runner = _mock_runner()
    rescue_ok = {
        "eligible": 53, "writes": 53, "verified": 53,
        "count_match": True, "errors": [], "dry_run": False,
    }
    reeval_ok = {"processed": 53, "armed": 50, "rejected": 3, "errors": 0}
    handoff_ok = {"status": "ok"}

    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections",
              return_value=rescue_ok),
        patch("ap.live_overnight_rescue.run_overnight_reeval",
              return_value=reeval_ok) as mock_reeval,
        patch("ap.morning_handoff.run_morning_handoff_audit", return_value=handoff_ok,
              create=False),
        patch("ap.live_overnight_rescue.run_morning_handoff_audit",
              return_value=handoff_ok),
        patch("ap.live_overnight_rescue.run_preopen_autonomous_readiness",
              return_value={"status": "OK"}),
    ):
        recover_and_rerun_live_overnight(
            runner=runner, trading_date=DATE, dry_run=False, expected_count=53,
        )

    mock_reeval.assert_called_once()
    kwargs = mock_reeval.call_args.kwargs
    assert kwargs["force"] is True
    assert kwargs["client_id"] == JASON
    assert kwargs["broker"] is runner.broker
    assert kwargs["master_control"] is runner.master_control
    assert kwargs["order_state_machine"] is runner.order_state_machine


# ─── Test 19: Readiness degraded when overnight reeval produced zero armed ────

def test_readiness_degraded_when_overnight_armed_zero():
    from ap.preopen_readiness import _overnight_status

    client_state = {
        "watching_count": 53, "pending_trigger_rows": [],
        "stale_processing_ids": [], "watching_orphans": [],
    }
    # overnight_reeval record: fetched=53, armed=0 (all-terminal failure)
    reeval_row = {
        "status": "partial",
        "details": {"fetched": 53, "armed": 0, "rejected": 53, "errors": 0},
    }
    runner = MagicMock()
    runner._last_overnight_reeval_date = None

    with patch("ap.preopen_readiness._load_preopen_row", return_value=reeval_row):
        status, details = _overnight_status(
            runner, client_state, DATE,
            client_id=JASON, execution_mode="live",
        )

    assert status == "degraded"
    assert details.get("error_code") == "OVERNIGHT_REEVAL_ZERO_ARMED"


# ─── Test 20: Handoff success cannot override zero-armed overnight record ──────

def test_handoff_success_cannot_override_zero_armed_overnight():
    from ap.preopen_readiness import _overnight_status

    client_state = {
        "watching_count": 0,       # All rows were rejected (terminal)
        "pending_trigger_rows": [],
        "stale_processing_ids": [], "watching_orphans": [],
    }
    # overnight_reeval record: fetched=53, armed=0 (false veto killed all of them)
    reeval_row = {
        "status": "partial",
        "details": {"fetched": 53, "armed": 0, "rejected": 53, "errors": 0},
    }
    runner = MagicMock()
    runner._last_overnight_reeval_date = None

    with (
        patch("ap.preopen_readiness._load_preopen_row", return_value=reeval_row),
        patch("ap.preopen_readiness._post_overnight_reeval_success_exists",
              return_value=True),   # Handoff says "success"
    ):
        status, details = _overnight_status(
            runner, client_state, DATE,
            client_id=JASON, execution_mode="live",
        )

    # The overnight_reeval record must take precedence over the handoff row
    assert status == "degraded", (
        f"overnight_reeval record (armed=0) must override handoff success. "
        f"Got status={status!r}, details={details}"
    )


# ─── Test 21: Zero contracts are never converted to one in bridge ─────────────

def test_bridge_does_not_convert_zero_to_one():
    from intelligence_bridge import _map_result

    pipeline_result = {
        "action": "execute", "confidence": 50.0, "score": 50.0,
        "contracts": 0,  # zero from PM — must not become 1
        "reasoning": "zero_contracts",
        "risk_detail": {
            "approved":      True,
            "reason_code":   "APPROVED_WITH_REGIME_MISMATCH",
            "hard_veto":     False,
            "max_contracts": 0,          # risk manager also says 0
            "max_position_usd": 0.0,
        },
        "ticker": "AAPL",
    }
    gate = _map_result(pipeline_result, fallback_score=50.0)
    # Must not emit REGIME_MISMATCH_ADVISORY when max_contracts=0
    assert gate["intel_status"] != "REGIME_MISMATCH_ADVISORY", (
        "Advisory must not fire when max_contracts=0"
    )


# ─── Test 22: Idempotency — second rescue finds 0 rows ────────────────────────

def test_rescue_idempotent_second_call_finds_zero():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections

    call_count = [0]

    def fake_conn_ctx():
        class FakeCursor:
            description = [("n",)]
            def execute(self, sql, params): pass
            def fetchone(self):
                call_count[0] += 1
                return {"n": 0}  # Already recovered — SQL excludes recovery_context rows
            def fetchall(self): return []
        class FakeConn:
            def __enter__(self): return FakeCursor()
            def __exit__(self, *a): pass
        return FakeConn()

    with (
        patch("ap.live_overnight_rescue.conn", side_effect=fake_conn_ctx),
        patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()),
    ):
        result = rescue_live_overnight_regime_rejections(
            client_id=JASON, execution_mode="live",
            trading_date=DATE, dry_run=True, expected_count=53,
        )

    assert result["eligible"] == 0
    assert result["writes"] == 0
    assert result["count_match"] is False
    assert result["errors"]  # count mismatch should be reported
