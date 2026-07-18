"""Behavioral proof for process-local setup ownership in Master Control.

Every lifecycle assertion drives the real ``APMasterControl.evaluate`` method.
Only database, telemetry, and other external dependencies are mocked.
"""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

import pytest

import ap_master_control as master_control_module
from ap_master_control import APMasterControl


JOSE = "jose.vasquez4011@gmail.com"
TRADEFLUENCE = "tradefluencehq@gmail.com"


def _signal(signal_id: str | None = "sig-1", **overrides):
    payload = {
        "signal_id": signal_id,
        "canonical_signal_id": signal_id,
        "client_id": JOSE,
        "ticker": "AAPL",
        "symbol": "AAPL",
        "side": "CALL",
        "direction": "CALL",
        "timeframe": "1d",
        "pattern": "2-3",
        "pattern_id": "2-3",
        "score": 95.0,
        "ev_score": 95.0,
        "entry_trigger": 200.0,
        "target_price": 205.0,
        "stop_price": 197.5,
        "underlying_at_signal": 201.0,
        "trigger": {"entry": 200.0, "stop": 197.5, "pt1": 205.0},
    }
    payload.update(overrides)
    return payload


def _snapshot():
    return {
        "_snapshot_ok": True,
        "_snapshot_ts": "2026-07-13T12:00:00Z",
        "_snapshot_age_sec": 0.0,
        "open_count": 0,
        "open_positions": [],
        "closing_positions": [],
        "calls_open": 0,
        "puts_open": 0,
        "filled_unreconciled_calls": 0,
        "filled_unreconciled_puts": 0,
        "pending_entries": 0,
        "pending_entry_capital": 0.0,
        "capital_deployed": 0.0,
        "realized_pnl_today": 0.0,
        "trades_today": 0,
        "trades_today_source": "broker_confirmed_entry_orders",
        "trade_count_query_status": "ok",
        "total_trades": 25,
        "daily_trades": 0,
        "intraday_trades": 0,
        "watcher_count": 0,
        "entry_attempt_lock_count": 0,
        "symbol_trades": {},
        "ticker_open_counts": {},
        "ticker_pending_counts": {},
    }


def _make_mc(*, mode: str = "paper") -> APMasterControl:
    master_control_module.emit_decision_event = None
    master_control_module.track_counterfactual_signal = None
    with patch.object(APMasterControl, "_seed_dedup_from_db", return_value=None):
        mc = APMasterControl(
            mode=mode,
            client_id=JOSE,
            score_floor=60.0,
            context_floor=0.0,
            account_equity=100_000.0,
            max_capital_pct=0.40,
            max_position_pct=0.40,
            max_total_capital_pct=0.90,
            max_sector_pct=1.0,
            max_ticker_pct=1.0,
            max_positions=20,
            max_calls=20,
            max_puts=20,
            max_trades_today=100,
            require_snapshot_freshness_live=False,
            pending_capital_fail_closed_live=False,
        )
    mc._get_snapshot = MagicMock(side_effect=lambda *args, **kwargs: _snapshot())
    mc._has_durable_duplicate_signal = MagicMock(return_value=(False, "", ""))
    mc._pending_capital_from_snapshot_or_db = MagicMock(return_value=0.0)
    mc._sector_capital_deployed = MagicMock(return_value=0.0)
    mc._ticker_capital_deployed = MagicMock(return_value=0.0)
    mc._run_intelligence = MagicMock(return_value={
        "approved": True,
        "score": 0.0,
        "contracts": 1,
        "reasoning": "observe-only unavailable",
        "_available": False,
    })
    mc._run_final_quality_gates = MagicMock(return_value=None)
    mc._persist_dedup = MagicMock(return_value=None)
    mc._emit_trade_dossier = MagicMock()
    mc._log_capital_utilization = MagicMock()
    mc._alert_degraded = MagicMock()
    mc._store_update = MagicMock()
    mc.feedback = None
    mc.sizer = None
    mc.pm = None
    return mc


def _setup_key(client_id: str, mode: str = "PAPER") -> str:
    return f"{client_id}:{mode}:AAPL:CALL:1d"


def _assert_approved(decision):
    assert decision.ok is True, decision.reason
    assert decision.stage == "approved"


def _assert_duplicate_setup(decision):
    assert decision.ok is False
    assert decision.stage == "blocked_system"
    assert decision.reason == "duplicate_setup (AAPL CALL 1d)"


def test_first_real_evaluate_records_setup_owner():
    mc = _make_mc()
    decision = mc.evaluate(_signal("sig-1"), client_id=JOSE)

    _assert_approved(decision)
    key = _setup_key(JOSE)
    assert key in mc._seen_signals
    assert mc._seen_setup_owners[key] == "canonical:sig-1"


def test_same_lifecycle_real_evaluate_does_not_self_reject():
    mc = _make_mc()
    _assert_approved(mc.evaluate(_signal("sig-1"), client_id=JOSE))
    snapshot_calls = mc._get_snapshot.call_count
    pending_calls = mc._pending_capital_from_snapshot_or_db.call_count
    quality_calls = mc._run_final_quality_gates.call_count

    decision = mc.evaluate(_signal("sig-1"), client_id=JOSE)

    _assert_approved(decision)
    assert mc._has_durable_duplicate_signal.call_count == 2
    assert mc._get_snapshot.call_count > snapshot_calls
    assert mc._pending_capital_from_snapshot_or_db.call_count > pending_calls
    assert mc._run_final_quality_gates.call_count > quality_calls


def test_different_lifecycle_real_evaluate_still_blocks():
    mc = _make_mc()
    _assert_approved(mc.evaluate(_signal("sig-1"), client_id=JOSE))
    snapshot_calls = mc._get_snapshot.call_count

    decision = mc.evaluate(_signal("sig-2"), client_id=JOSE)

    _assert_duplicate_setup(decision)
    assert mc._get_snapshot.call_count == snapshot_calls


def test_ownerless_legacy_cache_real_evaluate_blocks():
    mc = _make_mc()
    key = _setup_key(JOSE)
    mc._seen_signals[key] = time.time()

    decision = mc.evaluate(_signal("sig-1"), client_id=JOSE)

    _assert_duplicate_setup(decision)
    assert key not in mc._seen_setup_owners


@pytest.mark.parametrize("identity", [None, ""])
def test_blank_signal_identity_real_evaluate_cannot_bypass(identity):
    mc = _make_mc()
    key = _setup_key(JOSE)
    mc._seen_signals[key] = time.time()
    mc._seen_setup_owners[key] = "signal:sig-1"
    signal = _signal(identity)

    decision = mc.evaluate(signal, client_id=JOSE)

    assert decision.ok is False
    if decision.stage == "metadata_validation":
        assert "missing_signal_id" in decision.reason
    else:
        _assert_duplicate_setup(decision)
        assert signal["signal_id"]
        assert signal["signal_id"] != "sig-1"


def test_durable_duplicate_still_blocks_after_same_lifecycle_cache_bypass():
    mc = _make_mc()
    _assert_approved(mc.evaluate(_signal("sig-1"), client_id=JOSE))
    snapshot_calls = mc._get_snapshot.call_count
    mc._has_durable_duplicate_signal.return_value = (
        True,
        "orders",
        "id=42 status=SUBMITTED",
    )

    decision = mc.evaluate(_signal("sig-1"), client_id=JOSE)

    assert decision.ok is False
    assert decision.reason == "duplicate_signal_id (durable:orders)"
    assert mc._get_snapshot.call_count == snapshot_calls


def test_jose_and_tradefluence_cache_isolation():
    mc = _make_mc()

    jose = mc.evaluate(_signal("sig-jose"), client_id=JOSE)
    tradefluence = mc.evaluate(
        _signal("sig-tradefluence", client_id=TRADEFLUENCE),
        client_id=TRADEFLUENCE,
    )

    _assert_approved(jose)
    _assert_approved(tradefluence)
    assert mc._seen_setup_owners[_setup_key(JOSE)] == "canonical:sig-jose"
    assert mc._seen_setup_owners[_setup_key(TRADEFLUENCE)] == "canonical:sig-tradefluence"


def test_paper_live_mode_isolation_with_same_client_identity():
    mc = _make_mc(mode="paper")
    runtime_mode = {"value": "PAPER"}
    mc._mode_fn = lambda: runtime_mode["value"]

    paper = mc.evaluate(_signal("sig-paper"), client_id=JOSE)
    runtime_mode["value"] = "LIVE"
    live = mc.evaluate(_signal("sig-live"), client_id=JOSE)

    _assert_approved(paper)
    _assert_approved(live)
    assert mc._seen_setup_owners[_setup_key(JOSE, "PAPER")] == "canonical:sig-paper"
    assert mc._seen_setup_owners[_setup_key(JOSE, "LIVE")] == "canonical:sig-live"


def test_expired_cache_prunes_owner_in_real_evaluate_path():
    mc = _make_mc()
    key = _setup_key(JOSE)
    mc._seen_signals[key] = time.time() - 1801
    mc._seen_setup_owners[key] = "sig-expired"

    decision = mc.evaluate(_signal("sig-new"), client_id=JOSE)

    _assert_approved(decision)
    assert mc._seen_setup_owners[key] == "canonical:sig-new"
    assert mc._seen_signals[key] > time.time() - 10


def test_cache_rebuild_prunes_stale_owner():
    mc = _make_mc()
    now = time.time()
    for index in range(501):
        key = f"noise:{index}"
        mc._seen_signals[key] = now if index else now - 1801
    mc._seen_setup_owners["noise:0"] = "stale-owner"

    decision = mc.evaluate(_signal("sig-rebuild"), client_id=JOSE)

    _assert_approved(decision)
    assert "noise:0" not in mc._seen_signals
    assert "noise:0" not in mc._seen_setup_owners


def test_reset_session_clears_owner_state(monkeypatch):
    mc = _make_mc()
    _assert_approved(mc.evaluate(_signal("sig-1"), client_id=JOSE))
    monkeypatch.setattr(mc, "clear_force_close", MagicMock())

    mc.reset_session(client_id=JOSE)

    assert mc._seen_signals == {}
    assert mc._seen_setup_owners == {}


def test_concurrent_different_signals_cannot_both_bypass():
    mc = _make_mc()
    original_intelligence = mc._run_intelligence
    started = threading.Event()

    def slow_intelligence(signal):
        started.set()
        time.sleep(0.05)
        return original_intelligence(signal)

    mc._run_intelligence = MagicMock(side_effect=slow_intelligence)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(mc.evaluate, _signal("sig-a"), client_id=JOSE)
        assert started.wait(timeout=2)
        second = pool.submit(mc.evaluate, _signal("sig-b"), client_id=JOSE)
        decisions = [first.result(timeout=5), second.result(timeout=5)]

    assert sum(decision.ok for decision in decisions) == 1
    blocked = next(decision for decision in decisions if not decision.ok)
    _assert_duplicate_setup(blocked)


def test_july10_repeated_queue_lifecycle_no_longer_returns_duplicate_setup():
    mc = _make_mc()
    first_row = _signal(
        "july10-jose-aapl-call-1d:attempt-1",
        _queue_id="7201",
        canonical_signal_id="july10-jose-aapl-call-1d",
        execution_mode="paper",
        metadata={},
    )
    reclaimed_row = {
        **first_row,
        "signal_id": "july10-jose-aapl-call-1d:recovery-2",
    }

    first = mc.evaluate(dict(first_row), client_id=JOSE)
    reclaimed = mc.evaluate(dict(reclaimed_row), client_id=JOSE)

    _assert_approved(first)
    _assert_approved(reclaimed)
    assert "duplicate_setup" not in reclaimed.reason
    assert mc._seen_setup_owners[_setup_key(JOSE)] == "queue:7201"


def test_same_queue_lifecycle_changed_raw_suffix_does_not_self_reject():
    mc = _make_mc()
    first = _signal(
        "sig-stable:attempt-1",
        canonical_signal_id="sig-stable",
        _queue_id=7201,
    )
    reclaimed = _signal(
        "sig-stable:recovery-2",
        canonical_signal_id="sig-stable",
        _queue_id="7201",
    )

    _assert_approved(mc.evaluate(first, client_id=JOSE))
    decision = mc.evaluate(reclaimed, client_id=JOSE)

    _assert_approved(decision)
    assert mc._seen_setup_owners[_setup_key(JOSE)] == "queue:7201"


def test_different_queue_lifecycle_same_setup_still_blocks():
    mc = _make_mc()
    _assert_approved(mc.evaluate(
        _signal("sig-1", canonical_signal_id="same-market-id", _queue_id=7201),
        client_id=JOSE,
    ))

    decision = mc.evaluate(
        _signal("sig-2", canonical_signal_id="same-market-id", _queue_id=7202),
        client_id=JOSE,
    )

    _assert_duplicate_setup(decision)


def test_same_canonical_identity_without_queue_id_bypasses_only_without_durable_duplicate():
    mc = _make_mc()
    _assert_approved(mc.evaluate(
        _signal("canonical-setup:attempt-1", canonical_signal_id="canonical-setup"),
        client_id=JOSE,
    ))

    decision = mc.evaluate(
        _signal("canonical-setup:recovery-2", canonical_signal_id="canonical-setup"),
        client_id=JOSE,
    )

    _assert_approved(decision)
    assert mc._has_durable_duplicate_signal.call_count == 2
    assert mc._seen_setup_owners[_setup_key(JOSE)] == "canonical:canonical-setup"


def test_raw_signal_identity_is_final_owner_fallback():
    mc = _make_mc()
    signal = _signal("raw-only", canonical_signal_id="")

    _assert_approved(mc.evaluate(signal, client_id=JOSE))

    assert mc._seen_setup_owners[_setup_key(JOSE)] == "signal:raw-only"


def test_invalid_queue_identity_falls_back_to_canonical_identity():
    mc = _make_mc()
    first = _signal(
        "stable:attempt-1",
        canonical_signal_id="stable",
        _queue_id="not-an-integer",
    )
    reclaimed = _signal(
        "stable:recovery-2",
        canonical_signal_id="stable",
        _queue_id="not-an-integer",
    )

    _assert_approved(mc.evaluate(first, client_id=JOSE))
    _assert_approved(mc.evaluate(reclaimed, client_id=JOSE))

    assert mc._seen_setup_owners[_setup_key(JOSE)] == "canonical:stable"


def test_durable_separate_active_order_blocks_same_canonical_identity():
    mc = _make_mc()
    _assert_approved(mc.evaluate(
        _signal("canonical-setup:attempt-1", canonical_signal_id="canonical-setup"),
        client_id=JOSE,
    ))
    mc._has_durable_duplicate_signal.return_value = (
        True,
        "orders",
        "id=42 status=SUBMITTED",
    )

    decision = mc.evaluate(
        _signal("canonical-setup:recovery-2", canonical_signal_id="canonical-setup"),
        client_id=JOSE,
    )

    assert decision.ok is False
    assert decision.reason == "duplicate_signal_id (durable:orders)"


def test_existing_duplicate_reason_and_diagnostics_preserved(caplog):
    mc = _make_mc()
    _assert_approved(mc.evaluate(_signal("sig-1"), client_id=JOSE))

    with caplog.at_level(logging.INFO, logger="ap.master_control"):
        decision = mc.evaluate(_signal("sig-2"), client_id=JOSE)

    _assert_duplicate_setup(decision)
    assert "SETUP_DEDUP_DIFFERENT_LIFECYCLE_BLOCK" in caplog.text
    assert "cached_owner=canonical:sig-1" in caplog.text


def test_canonical_module_imports_directly_from_ap_master_control_py():
    import ap_master_control as module

    assert module.__file__.endswith("/ap_master_control.py")
    canonical_evaluate = getattr(
        APMasterControl,
        "_entry_metadata_guard_original_evaluate",
        APMasterControl.evaluate,
    )
    assert canonical_evaluate.__module__ == "ap_master_control"


def test_explicit_cache_removal_deletes_owner_metadata():
    mc = _make_mc()
    _assert_approved(mc.evaluate(_signal("sig-1"), client_id=JOSE))
    key = _setup_key(JOSE)

    mc._remove_setup_cache_entry(key)

    assert key not in mc._seen_signals
    assert key not in mc._seen_setup_owners
