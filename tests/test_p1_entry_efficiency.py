from __future__ import annotations

import inspect
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_entry_efficiency",
)
os.environ.setdefault("ENCRYPTION_KEY", "ap-entry-efficiency-pr-436-2026")

from ap_entry_efficiency import (
    ENTRY_EFFICIENCY_OBSERVE_ONLY,
    ENTRY_EFFICIENCY_PAPER_AUTHORITATIVE,
    READY_NOW,
    REARM_FOR_REBREACH,
    TERMINAL_INVALID,
    WAIT_CONFIRMATION,
    evaluate_entry_efficiency,
    resolve_entry_efficiency_mode,
)
from ap_entry_watcher import (
    APEntryWatcher,
    WatchedSignal,
    WatchState,
    _parse_entry_efficiency_at,
)
from ap.order_state_machine import APOrderStateMachine
from ap_execution_core import APExecutionCore, _parse_datetime_for_efficiency


FIRST_BREACH = datetime(2026, 8, 12, 13, 51, 51, tzinfo=timezone.utc)


def _decision(**overrides):
    values = {
        "ticker": "AAPL",
        "side": "PUT",
        "pattern": "2-3",
        "timeframe": "1d",
        "metadata": {},
        "execution_mode": "paper",
        "trigger_price": 302.80,
        "stop_price": 304.00,
        "target_price": 300.0,
        "bid": 302.79,
        "ask": 302.81,
        "quote_age_ms": 1_000,
        "first_breach_at": FIRST_BREACH,
        "now": datetime(2026, 8, 12, 13, 52, 30, tzinfo=timezone.utc),
        "mode": "paper_authoritative",
    }
    values.update(overrides)
    return evaluate_entry_efficiency(**values)


def test_targeted_paper_rollout_requires_explicit_promotion_and_fails_closed(monkeypatch):
    monkeypatch.delenv("AP_ENTRY_EFFICIENCY_MODE", raising=False)
    monkeypatch.delenv("ENTRY_EFFICIENCY_MODE", raising=False)

    assert resolve_entry_efficiency_mode() == ENTRY_EFFICIENCY_OBSERVE_ONLY
    assert resolve_entry_efficiency_mode("not-a-mode") == ENTRY_EFFICIENCY_OBSERVE_ONLY
    assert resolve_entry_efficiency_mode("paper") == ENTRY_EFFICIENCY_OBSERVE_ONLY
    assert resolve_entry_efficiency_mode("paper_authoritative") == ENTRY_EFFICIENCY_PAPER_AUTHORITATIVE
    result = _decision(mode=None)
    assert result.authoritative is False
    assert result.decision == WAIT_CONFIRMATION
    assert result.reason_code == "OBSERVE_ONLY"
    assert result.should_hold is False


def test_live_authority_is_reserved_and_cannot_be_enabled_by_environment(monkeypatch):
    monkeypatch.setenv("AP_ENTRY_EFFICIENCY_LIVE_APPROVED", "1")
    monkeypatch.setenv("ENTRY_EFFICIENCY_LIVE_APPROVED", "true")
    assert resolve_entry_efficiency_mode("live_authoritative") == "observe_only"
    result = _decision(execution_mode="live", mode="live_authoritative")
    assert result.authoritative is False
    assert result.should_hold is False


def test_aapl_opening_daily_raw_pattern_normalizes_and_holds():
    result = _decision()

    assert result.authoritative is True
    assert result.decision == WAIT_CONFIRMATION
    assert result.should_hold is True
    assert result.pattern_raw == "2-3"
    assert result.canonical_pattern == "2-3-2"
    assert result.to_meta()["entry_efficiency_pattern_raw"] == "2-3"
    assert result.to_meta()["entry_efficiency_canonical_pattern"] == "2-3-2"
    assert result.breach_was_opening is True
    assert result.opening_window is True
    assert result.reason_code == "ENTRY_EFFICIENCY_OPENING_BREACH"


def test_clock_alone_after_opening_window_does_not_release_old_breach():
    result = _decision(now=datetime(2026, 8, 12, 14, 0, tzinfo=timezone.utc))

    assert result.opening_window is False
    assert result.breach_was_opening is True
    assert result.decision == WAIT_CONFIRMATION
    assert result.reason_code == "ENTRY_EFFICIENCY_OPENING_BREACH"


def test_pullback_requires_rebreach_and_fresh_rebreach_can_release():
    pulled_back = _decision(bid=303.10, ask=303.20)
    assert pulled_back.decision == REARM_FOR_REBREACH
    assert pulled_back.reason_code == "ENTRY_EFFICIENCY_REARM_REQUIRED"

    rebreached = _decision(
        bid=302.79,
        ask=302.81,
        rearm_pending=True,
    )
    assert rebreached.decision == READY_NOW
    assert rebreached.reason_code == "ENTRY_EFFICIENCY_GENUINE_REBREACH"


def test_unproven_rearm_state_cannot_release_on_trigger_relation_alone():
    result = _decision(
        prior_state=REARM_FOR_REBREACH,
        rearm_pending=False,
        now=datetime(2026, 8, 12, 14, 0, tzinfo=timezone.utc),
    )
    assert result.decision == WAIT_CONFIRMATION
    assert result.reason_code == "ENTRY_EFFICIENCY_REARM_STATE_UNPROVEN"


def test_profile_ready_is_ignored_without_direct_rebreach_evidence():
    result = _decision(
        now=datetime(2026, 8, 12, 14, 0, tzinfo=timezone.utc),
        metadata={
            "breach_profile": {
                "decision_class": "VALID_SETUP_READY_NOW",
                "continuation_confirmed": True,
            }
        },
    )
    assert result.decision == WAIT_CONFIRMATION
    assert result.reason_code == "ENTRY_EFFICIENCY_OPENING_BREACH"
    assert result.should_hold is True
    assert result.evidence["intelligence_metadata_authority"] == "IGNORED"
    assert result.evidence["untrusted_intelligence_keys"] == ("breach_profile",)


def test_profile_ready_without_fresh_continuation_does_not_release_old_breach():
    result = _decision(
        now=datetime(2026, 8, 12, 14, 0, tzinfo=timezone.utc),
        metadata={"breach_profile": {"decision_class": "VALID_SETUP_READY_NOW"}},
    )
    assert result.decision == WAIT_CONFIRMATION
    assert result.reason_code == "ENTRY_EFFICIENCY_OPENING_BREACH"


def test_canonical_breach_profile_classes_never_become_timing_authority():
    baseline = _decision()
    for decision_class in (
        "VALID_SETUP_BAD_IMMEDIATE_ENTRY",
        "VALID_SETUP_WAIT_FOR_REBREACH",
        "SETUP_INVALID",
    ):
        result = _decision(
            metadata={"breach_profile": {"decision_class": decision_class}},
        )
        assert result.decision == baseline.decision
        assert result.reason_code == baseline.reason_code
        assert result.evidence["timing_authority_basis"] == "DIRECT_MARKET_EVIDENCE"


def test_naive_first_breach_is_rejected_not_reinterpreted_as_utc():
    result = _decision(
        first_breach_at="2026-08-12T13:51:51",
        metadata={"entry_efficiency_first_breach_at": FIRST_BREACH.isoformat()},
    )
    assert result.first_breach_at is None
    assert result.breach_was_opening is False
    assert result.decision == WAIT_CONFIRMATION
    assert result.reason_code == "ENTRY_EFFICIENCY_FIRST_BREACH_UNAVAILABLE"
    assert result.should_hold is True


@pytest.mark.parametrize(
    "parser",
    [_parse_entry_efficiency_at, _parse_datetime_for_efficiency],
)
def test_entry_efficiency_schedule_parsers_reject_naive_timestamps(parser):
    assert parser("2026-08-12T13:51:51") is None
    assert parser("2026-08-12T13:51:51+00:00") == FIRST_BREACH


def test_durable_deadline_is_not_extended_when_breach_evidence_is_missing():
    result = _decision(
        first_breach_at=None,
        prior_deadline_at="2026-08-12T13:52:00+00:00",
        now=datetime(2026, 8, 12, 13, 53, tzinfo=timezone.utc),
    )
    assert result.decision == TERMINAL_INVALID
    assert result.reason_code == "ENTRY_EFFICIENCY_WAIT_DEADLINE_EXPIRED"


def test_authoritative_rollout_is_scoped_to_daily_232():
    result = _decision(pattern="1-2")
    assert result.canonical_pattern == "1-2"
    assert result.decision == READY_NOW
    assert result.reason_code == "PATTERN_NOT_IN_SCOPE"
    assert result.authoritative is False
    assert result.should_hold is False


@pytest.mark.parametrize(
    "overrides, reason",
    [
        ({"bid": 0, "ask": 0, "quote_age_ms": None}, "ENTRY_EFFICIENCY_QUOTE_UNAVAILABLE"),
        ({"quote_age_ms": 60_001}, "ENTRY_EFFICIENCY_QUOTE_UNAVAILABLE"),
        ({"ask": 306.1}, "ENTRY_EFFICIENCY_STOP_INVALIDATED"),
        ({"bid": 299.9, "ask": 300.1}, "ENTRY_EFFICIENCY_TARGET_COMPLETE"),
    ],
)
def test_authoritative_truth_never_releases_stale_or_invalid_setup(overrides, reason):
    result = _decision(**overrides)
    assert result.decision in {WAIT_CONFIRMATION, TERMINAL_INVALID}
    assert result.reason_code == reason
    if result.decision == TERMINAL_INVALID:
        assert result.should_hold is False


def _watch_signal(contract_symbol="", **metadata):
    signal = {
        "signal_id": "sig-efficiency",
        "canonical_signal_id": "canonical-sig-efficiency",
        "ticker": "AAPL",
        "side": "PUT",
        "entry_price": 302.80,
        "stop_price": 304.00,
        "target_price": 300.0,
        "timeframe": "1d",
        "pattern": "2-3",
        "local_order_id": "order-efficiency",
        "client_id": "jason@example.com",
        "execution_mode": "paper",
        "metadata": metadata,
    }
    if contract_symbol:
        signal["contract_symbol"] = contract_symbol
    return signal


def test_watcher_wait_suppresses_repeated_trigger_until_efficiency_due(monkeypatch):
    monkeypatch.setenv("AP_ENTRY_EFFICIENCY_MODE", ENTRY_EFFICIENCY_PAPER_AUTHORITATIVE)
    future = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
    watched = WatchedSignal(
        _watch_signal(
            entry_efficiency_state="WAIT_CONFIRMATION",
            entry_efficiency_generation=3,
            entry_efficiency_next_eval_at=future,
        ),
        overnight=False,
    )

    assert watched.check(302.79, 302.81, quote_age_ms=1000) == WatchState.PENDING
    assert watched.check(302.79, 302.81, quote_age_ms=1000) == WatchState.PENDING
    assert watched.breach_count == 0
    assert watched.entry_efficiency_state == WAIT_CONFIRMATION

    watched.entry_efficiency_next_eval_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    assert watched.check(302.79, 302.81, quote_age_ms=1000) == WatchState.PENDING
    assert watched.check(302.79, 302.81, quote_age_ms=1000) == WatchState.TRIGGERED


def test_live_stale_efficiency_wait_is_ignored_when_paper_authority_is_enabled(monkeypatch):
    monkeypatch.setenv("AP_ENTRY_EFFICIENCY_MODE", ENTRY_EFFICIENCY_PAPER_AUTHORITATIVE)
    signal = _watch_signal(
        entry_efficiency_state=WAIT_CONFIRMATION,
        entry_efficiency_generation=1,
        entry_efficiency_next_eval_at=(
            datetime.now(timezone.utc) + timedelta(minutes=5)
        ).isoformat(),
    )
    signal["execution_mode"] = "live"
    watched = WatchedSignal(signal, overnight=False)

    assert watched.check(302.79, 302.81, quote_age_ms=1_000) == WatchState.PENDING
    assert watched.check(302.79, 302.81, quote_age_ms=1_000) == WatchState.TRIGGERED
    assert watched.breach_count == 2
    assert watched.entry_efficiency_state == WAIT_CONFIRMATION


def test_paper_stale_efficiency_wait_is_ignored_when_rollout_is_observe_only(monkeypatch):
    monkeypatch.setenv("AP_ENTRY_EFFICIENCY_MODE", ENTRY_EFFICIENCY_OBSERVE_ONLY)
    watched = WatchedSignal(
        _watch_signal(
            entry_efficiency_state=WAIT_CONFIRMATION,
            entry_efficiency_generation=1,
            entry_efficiency_next_eval_at=(
                datetime.now(timezone.utc) + timedelta(minutes=5)
            ).isoformat(),
        ),
        overnight=False,
    )

    assert watched.check(302.79, 302.81, quote_age_ms=1_000) == WatchState.PENDING
    assert watched.check(302.79, 302.81, quote_age_ms=1_000) == WatchState.TRIGGERED
    assert watched.breach_count == 2
    assert watched.entry_efficiency_state == WAIT_CONFIRMATION


def test_paper_stale_efficiency_wait_remains_active_when_authority_is_enabled(monkeypatch):
    monkeypatch.setenv("AP_ENTRY_EFFICIENCY_MODE", ENTRY_EFFICIENCY_PAPER_AUTHORITATIVE)
    watched = WatchedSignal(
        _watch_signal(
            entry_efficiency_state=WAIT_CONFIRMATION,
            entry_efficiency_generation=1,
            entry_efficiency_next_eval_at=(
                datetime.now(timezone.utc) + timedelta(minutes=5)
            ).isoformat(),
        ),
        overnight=False,
    )

    assert watched.check(302.79, 302.81, quote_age_ms=1_000) == WatchState.PENDING
    assert watched.check(302.79, 302.81, quote_age_ms=1_000) == WatchState.PENDING
    assert watched.breach_count == 0
    assert watched.entry_efficiency_state == WAIT_CONFIRMATION


def test_watcher_pullback_stages_distinct_rearm_cas_request(monkeypatch):
    monkeypatch.setenv("AP_ENTRY_EFFICIENCY_MODE", ENTRY_EFFICIENCY_PAPER_AUTHORITATIVE)
    watched = WatchedSignal(
        _watch_signal(
            entry_efficiency_state="WAIT_CONFIRMATION",
            entry_efficiency_generation=4,
            entry_efficiency_next_eval_at=(
                datetime.now(timezone.utc) - timedelta(seconds=1)
            ).isoformat(),
        ),
        overnight=False,
    )

    assert watched.check(303.10, 303.20, quote_age_ms=1000) == WatchState.PENDING
    request = watched._entry_efficiency_persist_request
    assert request["expected_state"] == WAIT_CONFIRMATION
    assert request["expected_generation"] == 4
    assert request["next_state"] == REARM_FOR_REBREACH
    assert getattr(watched, "deferred_retry_not_before", None) is None


def test_deferred_callback_verifies_efficiency_state_without_retry_lifecycle():
    osm = MagicMock()
    osm.get_order.return_value = {
        "status": "PENDING_TRIGGER",
        "broker_order_id": "",
        "submitted_ts": None,
        "meta": {
            "entry_efficiency_state": "WAIT_CONFIRMATION",
            "entry_efficiency_next_eval_at": "2026-08-12T14:00:00+00:00",
        },
    }
    watcher = APEntryWatcher(None, order_state_machine=osm, mode="PAPER")
    watched = WatchedSignal(
        _watch_signal(contract_symbol="DEFERRED:AAPL"),
        overnight=False,
    )
    result = watcher._resolve_trigger_callback_disposition(
        watched,
        {
            "disposition": "ENTRY_EFFICIENCY_WAIT",
            "reason_code": "ENTRY_EFFICIENCY_OPENING_BREACH",
            "next_retry_at": "2026-08-12T14:00:00+00:00",
        },
    )
    assert result == ("ENTRY_EFFICIENCY_WAIT", "2026-08-12T14:00:00+00:00")


def test_poll_loop_keeps_efficiency_wait_out_of_deferred_retry_clock():
    osm = MagicMock()
    osm.update_order_meta.return_value = True
    watcher = APEntryWatcher(None, order_state_machine=osm, mode="PAPER")
    watched = WatchedSignal(
        _watch_signal(
            contract_symbol="AAPL260821P00304000",
            entry_efficiency_state="WAIT_CONFIRMATION",
            entry_efficiency_generation=2,
            entry_efficiency_next_eval_at=(
                datetime.now(timezone.utc) - timedelta(seconds=1)
            ).isoformat(),
        ),
        overnight=False,
    )
    watched._watcher_ref = watcher
    watcher._pending.append(watched)
    watcher._dedup_set.add(watched.signal_id)
    watcher._fetch_quotes = lambda tickers: {
        "AAPL": {"bid": 302.79, "ask": 302.81, "quote_age_ms": 1000}
    }
    callback = MagicMock(
        return_value={
            "disposition": "ENTRY_EFFICIENCY_WAIT",
            "reason_code": "ENTRY_EFFICIENCY_CLOCK_NOT_SUFFICIENT",
            "next_retry_at": (
                datetime.now(timezone.utc) + timedelta(seconds=30)
            ).isoformat(),
        }
    )
    watcher.on_trigger = callback

    watcher._poll_active_signals(open_protect_active=False)
    watcher._poll_active_signals(open_protect_active=False)

    assert callback.call_count == 1
    assert watched.state == WatchState.PENDING
    assert watched.deferred_retry_not_before is None
    assert watched.entry_efficiency_next_eval_at is not None


def test_efficiency_gate_is_before_hydration_selector_and_submit():
    source = inspect.getsource(APExecutionCore._on_entry_trigger)
    gate = source.index("# ── PR #436: bounded entry-efficiency recheck")
    assert source.index("_hydration_bridge_applied", gate) > gate
    assert source.index("self.contract_selector.select", gate) > gate
    assert source.index("submit_res = self.order_state_machine.submit_existing_entry", gate) > gate


def test_aapl_wait_returns_before_runtime_submit_path(monkeypatch):
    monkeypatch.setenv("AP_ENTRY_EFFICIENCY_MODE", "paper_authoritative")
    deadline = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    efficiency_metadata = {
        "entry_efficiency_state": "WAIT_CONFIRMATION",
        "entry_efficiency_generation": 1,
        "entry_efficiency_next_eval_at": (
            datetime.now(timezone.utc) - timedelta(seconds=1)
        ).isoformat(),
        "entry_efficiency_deadline_at": deadline,
    }
    watched = WatchedSignal(_watch_signal(**efficiency_metadata), overnight=False)
    # AAPL incident shape: 09:51:51 ET, inside the first 30-minute window.
    watched.trigger_crossed_at = FIRST_BREACH
    watched.last_quote_bid = 302.79
    watched.last_quote_ask = 302.81
    watched.last_quote_age_ms = 1_000

    plan = SimpleNamespace(
        metadata={
            **efficiency_metadata,
            "canonical_signal_id": "canonical-sig-efficiency",
        },
        execution_mode="paper",
        client_id="jason@example.com",
        pattern="2-3",
        timeframe="1d",
        contract_symbol="AAPL260821P00304000",
    )
    osm = MagicMock()
    osm.cas_entry_efficiency_state.return_value = True
    osm.submit_existing_entry.return_value = {"ok": True}
    core = SimpleNamespace(
        mode="PAPER",
        paper=True,
        execution_mode="paper",
        email="jason@example.com",
        client_id="jason@example.com",
        order_state_machine=osm,
        store=MagicMock(),
        contract_selector=MagicMock(),
        _breach_risk_check=lambda _watched: True,
        _recover_plan_for_revalidation=lambda _watched: plan,
        _emit_breach_diag=lambda *args, **kwargs: None,
    )

    from ap import intelligence_evaluation

    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr(intelligence_evaluation, "_ensure_intelligence_dispatched", lambda *a, **k: None)
        result = APExecutionCore._on_entry_trigger(core, watched)

    assert result["disposition"] == "ENTRY_EFFICIENCY_WAIT"
    assert result["reason_code"] == "ENTRY_EFFICIENCY_OPENING_BREACH"
    osm.submit_existing_entry.assert_not_called()
    core.contract_selector.select.assert_not_called()
    osm.cas_entry_efficiency_state.assert_called_once()


def test_aapl_replay_wait_rearm_rebreach_ready_has_no_first_entry_post(monkeypatch):
    monkeypatch.setenv("AP_ENTRY_EFFICIENCY_MODE", ENTRY_EFFICIENCY_PAPER_AUTHORITATIVE)
    deadline = "2026-08-12T14:21:51+00:00"
    watched = WatchedSignal(
        _watch_signal(
            entry_efficiency_state=WAIT_CONFIRMATION,
            entry_efficiency_generation=1,
            entry_efficiency_next_eval_at=(
                datetime.now(timezone.utc) - timedelta(seconds=1)
            ).isoformat(),
            entry_efficiency_deadline_at=deadline,
        ),
        overnight=False,
    )
    watched.trigger_crossed_at = FIRST_BREACH
    osm = MagicMock()
    osm.cas_entry_efficiency_state.return_value = True
    watcher = APEntryWatcher(None, order_state_machine=osm, mode="PAPER")

    # Direct market pullback stages the durable WAIT -> REARM CAS; no callback
    # or broker submit path is entered for the original poor breach.
    assert watched.check(303.10, 303.20, quote_age_ms=1_000) == WatchState.PENDING
    request = watched._entry_efficiency_persist_request
    assert request["expected_state"] == WAIT_CONFIRMATION
    assert request["next_state"] == REARM_FOR_REBREACH
    assert watcher._persist_entry_efficiency_transition(watched, request) is True
    assert watched.entry_efficiency_state == REARM_FOR_REBREACH
    assert watched.entry_efficiency_rearm_pending is True
    cas_call = osm.cas_entry_efficiency_state.call_args
    assert cas_call.args[0] == "order-efficiency"
    cas_kwargs = cas_call.kwargs
    assert cas_kwargs["signal_id"] == "sig-efficiency"
    assert cas_kwargs["canonical_signal_id"] == "canonical-sig-efficiency"
    assert cas_kwargs["client_id"] == "jason@example.com"
    assert cas_kwargs["execution_mode"] == "paper"
    assert cas_kwargs["expected_state"] == WAIT_CONFIRMATION
    assert cas_kwargs["expected_generation"] == 1
    assert cas_kwargs["next_state"] == REARM_FOR_REBREACH
    assert cas_kwargs["next_generation"] == 2
    assert osm.submit_existing_entry.call_count == 0

    # A fresh direct-market re-breach earns one trigger observation, while the
    # policy evaluator still owns the authoritative READY decision.
    assert watched.check(302.79, 302.81, quote_age_ms=1_000) == WatchState.PENDING
    assert watched.check(302.79, 302.81, quote_age_ms=1_000) == WatchState.TRIGGERED
    assert watched.entry_efficiency_rebreach_at is not None
    ready = _decision(
        prior_state=REARM_FOR_REBREACH,
        rearm_pending=True,
        prior_deadline_at=deadline,
        now=datetime(2026, 8, 12, 14, 1, tzinfo=timezone.utc),
    )
    assert ready.decision == READY_NOW
    assert ready.reason_code == "ENTRY_EFFICIENCY_GENUINE_REBREACH"
    assert ready.evidence["timing_authority_basis"] == "DIRECT_MARKET_EVIDENCE"
    assert osm.submit_existing_entry.call_count == 0


def test_osm_efficiency_cas_has_exact_entry_identity_and_no_broker_evidence():
    source = inspect.getsource(APOrderStateMachine.cas_entry_efficiency_state)
    for required in (
        "local_order_id=%s",
        "client_id=%s",
        "kind='ENTRY'",
        "signal_id",
        "execution_mode",
        "status='PENDING_TRIGGER'",
        "broker_order_id",
        "submit_intent_at",
        "entry_efficiency_generation",
    ):
        assert required in source
    assert "canonical_signal_id" in source
    assert "broker.submit" not in source
