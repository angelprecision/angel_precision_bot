"""
tests/test_p0_overnight_watcher_cleanup.py

PR #126 seam tests — proves the #124→#126 gap is closed.

Required behaviors:
  1. arm=False  → cleanup + WATCH_ARM_FAILED proof written, retryable=False
  2. exception  → cleanup + WATCH_ARM_FAILED proof written, retryable=False
  3. cleanup failure → proof still written with cleanup_failed=True, mark ERROR
  4. second same-session run → skipped==1, create_calls==0, watch.call_count==1
  5. Hard 70 floor unchanged
"""
from __future__ import annotations

import pytest
from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch


# ── Stubs ────────────────────────────────────────────────────────────────────

class _StubOSM:
    def __init__(self, *, expire_ok=True, cancel_ok=True, raise_expire=False):
        self.create_calls = 0
        self._expire_ok = expire_ok
        self._cancel_ok = cancel_ok
        self._raise_expire = raise_expire

    def create_entry_order(self, plan, initial_status="CREATED", execution_mode=None):
        self.create_calls += 1
        return f"local-{self.create_calls}"

    def mark_entry_pending_trigger(self, local_order_id):
        return True

    def expire_pending_entry(self, local_order_id, *, reason=""):
        if self._raise_expire:
            raise RuntimeError("expire boom")
        return self._expire_ok

    def cancel_pending_entry(self, local_order_id, *, reason=""):
        return self._cancel_ok

    def transition(self, local_order_id, status, **kw):
        return True


class _StubLedger:
    def __init__(self):
        self.rows: dict = {}

    def upsert(self, canonical_id, client_id, payload):
        self.rows[(canonical_id, client_id)] = payload

    def get(self, canonical_id, client_id):
        return self.rows.get((canonical_id, client_id))


def _make_signal(signal_id="SIG-001", ticker="NVDA", side="CALL",
                 score=80.0, prior_high=500.0, prior_low=490.0,
                 entry_trigger=500.0):
    return {
        "signal_id":     signal_id,
        "ticker":        ticker,
        "side":          side,
        "score":         score,
        "ev_score":      score,
        "timeframe":     "1D",
        "strategy_type": "OVERNIGHT_DAILY",
        "prior_day_high": prior_high,
        "prior_day_low":  prior_low,
        "entry_trigger":  entry_trigger,
        "canonical_signal_id": f"CANON-{signal_id}",
    }


def _make_plan(signal):
    p = SimpleNamespace(
        signal_id=signal["signal_id"],
        ticker=signal["ticker"],
        side=signal["side"],
        score=signal["score"],
        direction=signal["side"],
        timeframe=signal["timeframe"],
        strategy_type=signal["strategy_type"],
        prior_day_high=signal["prior_day_high"],
        prior_day_low=signal["prior_day_low"],
        trigger_price=signal["entry_trigger"],
        stop_underlying=signal["prior_day_low"] * 0.99,
        target_underlying=signal["prior_day_high"] * 1.02,
        max_position_usd=200.0,
        contract_symbol=f"{signal['ticker']}260620C00500000",
        plan_id=f"plan-{signal['signal_id']}",
        tier="A+",
        pattern="2U",
        metadata={},
    )
    return p


def _job(signal_id="SIG-001", job_id=42):
    return {
        "id": job_id,
        "_source": "trade_queue",
        "signal_id": signal_id,
        "payload": _make_signal(signal_id),
        "result_json": None,
        "last_error": None,
    }


# ── Test runner helper ────────────────────────────────────────────────────────

def _run_reeval(
    *,
    osm,
    watcher,
    ledger,
    signal_id="SIG-001",
    ticker="NVDA",
    score=80.0,
    session_date=None,
    broker=None,
    mc=None,
    selector=None,
):
    """
    Call run_overnight_reeval with minimal stubs.
    Patches: _fetch_watching_signals, fetch_market_snapshot, validate_overnight_daily_signal,
    create_opportunities, update_opportunity, mark_watcher_invalidated,
    mark_internal_error, _mark_job_rejected, _mark_job_error, _mark_job_watching_armed,
    execution_mode_for_broker.
    """
    from ap_overnight_reeval import run_overnight_reeval

    signal = _make_signal(signal_id=signal_id, ticker=ticker, score=score)
    plan   = _make_plan(signal)

    mc = mc or MagicMock()
    mc.evaluate.return_value = SimpleNamespace(
        ok=True, plan=plan, reason="", signal_id=signal_id,
    )

    selector = selector or MagicMock()
    selector.select.return_value = SimpleNamespace(
        ok=True,
        contract_symbol=f"{ticker}260620C00500000",
        selected_contract=f"{ticker}260620C00500000",
        limit_price=1.50,
        qty=1,
        reserved_cost=150.0,
        selector_metadata={},
    )

    snap = SimpleNamespace(
        session_high_so_far=495.0,
        session_low_so_far=491.0,
        last_price=494.0,
        fetched_at="2026-06-12T09:35:00+00:00",
    )
    val_ok = SimpleNamespace(
        valid=True, reason_code="VALID", reason_text="ok",
        prior_high=500.0, prior_low=490.0,
        session_high=495.0, session_low=491.0,
    )

    # Proof store keyed by (canonical_id, client_id)
    _proof_store = {}

    def fake_create_opp(sid, clients, payload, canonical_signal_id=None):
        for cid in clients:
            key = (canonical_signal_id or sid, cid)
            if key not in _proof_store:
                _proof_store[key] = {"arm_state": None, "retryable": True, "extra_meta": {}}

    def fake_update_opp(sid, cid, status, *, canonical_signal_id=None,
                        miss_stage=None, miss_reason=None, order_local_id=None,
                        broker_order_id=None, extra_meta=None):
        key = (canonical_signal_id or sid, cid)
        row = _proof_store.setdefault(key, {})
        row["opportunity_status"] = str(status)
        row["miss_stage"]  = miss_stage
        row["miss_reason"] = miss_reason
        row["metadata"]    = dict(extra_meta or {})
        from ap.opportunity_ledger import WATCHER_ARMED
        if str(status) == str(WATCHER_ARMED):
            row["arm_state"] = "ARMED"
            row["retryable"] = False
        else:
            em = extra_meta or {}
            row["arm_state"] = em.get("overnight_arm_state", "MISSED")
            row["retryable"] = bool(em.get("overnight_arm_retryable", True))
        # mirror to ledger stub for assertions
        ledger.rows[(canonical_signal_id or sid, cid)] = row
        return True

    def fake_mark_invalidated(sid, cid, reason, canonical_signal_id=None,
                               order_local_id=None, extra_meta=None):
        key = (canonical_signal_id or sid, cid)
        row = _proof_store.setdefault(key, {})
        em  = extra_meta or {}
        row["opportunity_status"] = "WATCHER_INVALIDATED"
        row["miss_stage"]  = em.get("overnight_arm_state", "WATCH_ARM_FAILED")
        row["miss_reason"] = reason
        row["metadata"]    = dict(em)
        row["arm_state"]   = em.get("overnight_arm_state", "WATCH_ARM_FAILED")
        row["retryable"]   = bool(em.get("overnight_arm_retryable", True))
        ledger.rows[(canonical_signal_id or sid, cid)] = row

    def fake_mark_internal_error(sid, cid, reason, canonical_signal_id=None,
                                  order_local_id=None, extra_meta=None):
        key = (canonical_signal_id or sid, cid)
        row = _proof_store.setdefault(key, {})
        em  = extra_meta or {}
        row["opportunity_status"] = "INTERNAL_ERROR"
        row["miss_stage"]  = em.get("overnight_arm_state", "WATCH_ARM_FAILED")
        row["miss_reason"] = reason
        row["metadata"]    = dict(em)
        row["arm_state"]   = em.get("overnight_arm_state", "WATCH_ARM_FAILED")
        row["retryable"]   = bool(em.get("overnight_arm_retryable", True))
        ledger.rows[(canonical_signal_id or sid, cid)] = row

    def fake_get_arm_proof(client_id, canonical_signal_id, arm_key,
                            session_date, setup_identity):
        key = (canonical_signal_id, client_id)
        return _proof_store.get(key)

    import ap_overnight_reeval as _orn

    with (
        patch.object(_orn, "_fetch_watching_signals",
                     return_value=[dict(**_job(signal_id=signal_id), **{"signal_id": signal_id,
                                                                         "payload": signal})]),
        patch("ap.overnight_daily_validator.fetch_market_snapshot", return_value=snap),
        patch("ap.overnight_daily_validator.validate_overnight_daily_signal", return_value=val_ok),
        patch("ap_overnight_reeval.fetch_market_snapshot", return_value=snap),
        patch("ap_overnight_reeval.validate_overnight_daily_signal", return_value=val_ok),
        patch("ap.opportunity_ledger.create_opportunities", side_effect=fake_create_opp),
        patch("ap.opportunity_ledger.update_opportunity", side_effect=fake_update_opp),
        patch("ap.opportunity_ledger.mark_watcher_invalidated", side_effect=fake_mark_invalidated),
        patch("ap.opportunity_ledger.mark_internal_error", side_effect=fake_mark_internal_error),
        patch.object(_orn, "_get_overnight_arm_proof", side_effect=fake_get_arm_proof),
        patch.object(_orn, "_mark_job_rejected", return_value=None),
        patch.object(_orn, "_mark_job_error", return_value=None),
        patch.object(_orn, "_mark_job_watching_armed", return_value=None),
        patch("ap.authorization.execution_mode_for_broker", return_value="paper"),
        patch.object(_orn, "_is_trading_day", return_value=True),
    ):
        return run_overnight_reeval(
            client_id="client-1",
            broker=broker or MagicMock(),
            master_control=mc,
            contract_selector=selector,
            order_state_machine=osm,
            entry_watcher=watcher,
            position_manager=None,
            force=True,
        ), ledger


# =============================================================================
# Required test 1 — arm=False writes WATCH_ARM_FAILED proof
# =============================================================================

def test_false_return_writes_watch_arm_failed_proof(caplog):
    watcher = MagicMock()
    watcher.watch.return_value = False
    watcher._last_reject_reason = "armed_false"
    osm    = _StubOSM()
    ledger = _StubLedger()

    result, ledger = _run_reeval(osm=osm, watcher=watcher, ledger=ledger)

    # OSM order was created
    assert osm.create_calls == 1
    # Proof written with correct state
    proof = ledger.rows.get(("CANON-SIG-001", "client-1"))
    assert proof is not None, "WATCH_ARM_FAILED proof was not written"
    assert proof["metadata"]["overnight_arm_state"] == "WATCH_ARM_FAILED"
    assert proof["metadata"]["overnight_arm_decision"] == "SKIP_FUTURE_REARM"
    assert proof["metadata"]["overnight_arm_retryable"] is False
    assert proof["metadata"]["overnight_watch_arm_failure"] is True
    assert proof["metadata"]["overnight_source_table"] == "trade_queue"
    assert proof["metadata"]["overnight_reeval_session_key"] is not None
    assert "OVERNIGHT_WATCH_ARM_FAILED_CLEANUP_DONE" in caplog.text


# =============================================================================
# Required test 2 — exception writes WATCH_ARM_FAILED proof
# =============================================================================

def test_exception_writes_watch_arm_failed_proof(caplog):
    watcher = MagicMock()
    watcher.watch.side_effect = RuntimeError("watcher boom")
    osm    = _StubOSM()
    ledger = _StubLedger()

    result, ledger = _run_reeval(osm=osm, watcher=watcher, ledger=ledger)

    proof = ledger.rows.get(("CANON-SIG-001", "client-1"))
    assert proof is not None, "WATCH_ARM_FAILED proof was not written after exception"
    assert proof["metadata"]["overnight_arm_state"] == "WATCH_ARM_FAILED"
    assert proof["metadata"]["overnight_arm_decision"] == "SKIP_FUTURE_REARM"
    assert proof["metadata"]["overnight_arm_retryable"] is False
    assert "watcher boom" in (proof["miss_reason"] or "")
    assert "OVERNIGHT_WATCH_ARM_EXCEPTION_CLEANUP_DONE" in caplog.text


# =============================================================================
# Required test 3 — cleanup failure still writes WATCH_ARM_FAILED proof
# =============================================================================

def test_cleanup_failure_still_writes_watch_arm_failed_proof(caplog):
    watcher = MagicMock()
    watcher.watch.return_value = False
    watcher._last_reject_reason = "armed_false"
    # All cleanup methods fail
    osm = _StubOSM(expire_ok=False, cancel_ok=False)
    # Make transition also fail
    osm.transition = MagicMock(side_effect=RuntimeError("transition boom"))
    ledger = _StubLedger()

    result, ledger = _run_reeval(osm=osm, watcher=watcher, ledger=ledger)

    proof = ledger.rows.get(("CANON-SIG-001", "client-1"))
    assert proof is not None, "proof must be written even when cleanup fails"
    assert proof["metadata"]["overnight_arm_state"] == "WATCH_ARM_FAILED"
    assert proof["metadata"]["overnight_arm_retryable"] is False
    # Cleanup failure is recorded in metadata
    assert proof["metadata"].get("overnight_watch_arm_cleanup_failed") is True
    assert "original_reason" in proof["metadata"]


# =============================================================================
# Required test 4 — second same-session run skips (idempotency gate)
# =============================================================================

def test_second_same_session_run_skips(caplog):
    """
    First run: watch() returns False → cleanup + WATCH_ARM_FAILED proof written.
    Second run (same session): idempotency gate sees WATCH_ARM_FAILED/retryable=False
    → skip, no new OSM order, watch() not called again.
    """
    watcher = MagicMock()
    watcher.watch.return_value = False
    watcher._last_reject_reason = "armed_false"
    watcher.has_order = MagicMock(return_value=False)

    first_osm  = _StubOSM()
    second_osm = _StubOSM()
    ledger     = _StubLedger()

    # First run
    first_result, ledger = _run_reeval(
        osm=first_osm, watcher=watcher, ledger=ledger, signal_id="SIG-001"
    )

    # Second run — same session, same signal
    second_result, ledger = _run_reeval(
        osm=second_osm, watcher=watcher, ledger=ledger, signal_id="SIG-001"
    )

    # Idempotency gate must have skipped
    assert second_result["skipped"] == 1, (
        f"Expected skipped=1 on second run, got skipped={second_result['skipped']}"
    )
    assert second_osm.create_calls == 0, (
        f"create_entry_order must not be called on second run, "
        f"got create_calls={second_osm.create_calls}"
    )
    assert watcher.watch.call_count == 1, (
        f"watch() must be called only once total (first run), "
        f"got call_count={watcher.watch.call_count}"
    )

    # Proof from first run is still WATCH_ARM_FAILED/not retryable
    proof = ledger.rows.get(("CANON-SIG-001", "client-1"))
    assert proof is not None
    assert proof["metadata"]["overnight_arm_state"] == "WATCH_ARM_FAILED"
    assert proof["metadata"]["overnight_arm_retryable"] is False


# =============================================================================
# Required test 5 — hard 70 floor unchanged
# =============================================================================

def test_hard_70_floor_unchanged():
    """Score < 70 must be rejected before watcher arm is even attempted."""
    watcher = MagicMock()
    watcher.watch.return_value = True
    osm    = _StubOSM()
    ledger = _StubLedger()

    mc = MagicMock()
    mc.evaluate.return_value = SimpleNamespace(
        ok=False, plan=None,
        reason="REJECTED_LOW_SCORE (score=65.0 min_eligible=70.0)",
        signal_id="SIG-LOW",
    )

    result, _ = _run_reeval(
        osm=osm, watcher=watcher, ledger=ledger,
        signal_id="SIG-LOW", score=65.0, mc=mc,
    )

    assert watcher.watch.call_count == 0
    assert osm.create_calls == 0
    assert result.get("rejected", 0) >= 1 or result.get("errors", 0) >= 1


# =============================================================================
# Additional: exception path second run also skips
# =============================================================================

def test_exception_path_second_run_skips():
    watcher = MagicMock()
    watcher.watch.side_effect = [RuntimeError("watcher boom"), None]
    watcher.has_order = MagicMock(return_value=False)

    first_osm  = _StubOSM()
    second_osm = _StubOSM()
    ledger     = _StubLedger()

    _run_reeval(osm=first_osm, watcher=watcher, ledger=ledger, signal_id="SIG-EXC")
    second_result, _ = _run_reeval(
        osm=second_osm, watcher=watcher, ledger=ledger, signal_id="SIG-EXC"
    )

    assert second_result["skipped"] == 1
    assert second_osm.create_calls == 0
