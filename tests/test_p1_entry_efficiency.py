from __future__ import annotations

import json
import inspect
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_entry_efficiency",
)
os.environ.setdefault("ENCRYPTION_KEY", "ap-entry-efficiency-pr-436-2026")

import ap_execution_core as execution_core_module
from ap_entry_efficiency import (
    ENTRY_EFFICIENCY_CONFIG_CONFLICT,
    ENTRY_EFFICIENCY_CONFIG_INVALID,
    ENTRY_EFFICIENCY_CONFIG_UNSET,
    ENTRY_EFFICIENCY_CONFIG_VALID,
    ENTRY_EFFICIENCY_OBSERVE_ONLY,
    ENTRY_EFFICIENCY_PAPER_AUTHORITATIVE,
    READY_NOW,
    REARM_FOR_REBREACH,
    TERMINAL_INVALID,
    WAIT_CONFIRMATION,
    evaluate_entry_efficiency,
    parse_entry_efficiency_generation,
    resolve_entry_efficiency_mode,
    resolve_entry_efficiency_mode_with_reason,
)
from ap_entry_watcher import (
    APEntryWatcher,
    WatchedSignal,
    WatchState,
    _parse_entry_efficiency_at,
)
from ap.order_state_machine import APOrderStateMachine
from ap_execution_core import (
    APExecutionCore,
    _parse_datetime_for_efficiency,
    _resolve_entry_efficiency_execution_mode,
    _resolve_submit_execution_mode,
)


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


def test_rollout_aliases_are_consensus_checked_and_report_fail_closed_reason(monkeypatch):
    monkeypatch.delenv("AP_ENTRY_EFFICIENCY_MODE", raising=False)
    monkeypatch.delenv("ENTRY_EFFICIENCY_MODE", raising=False)
    assert resolve_entry_efficiency_mode_with_reason() == (
        ENTRY_EFFICIENCY_OBSERVE_ONLY,
        ENTRY_EFFICIENCY_CONFIG_UNSET,
    )

    monkeypatch.setenv("AP_ENTRY_EFFICIENCY_MODE", "paper_authoritative")
    assert resolve_entry_efficiency_mode_with_reason() == (
        ENTRY_EFFICIENCY_PAPER_AUTHORITATIVE,
        ENTRY_EFFICIENCY_CONFIG_VALID,
    )

    monkeypatch.setenv("ENTRY_EFFICIENCY_MODE", "paper-authoritative")
    assert resolve_entry_efficiency_mode_with_reason() == (
        ENTRY_EFFICIENCY_PAPER_AUTHORITATIVE,
        ENTRY_EFFICIENCY_CONFIG_VALID,
    )

    monkeypatch.setenv("ENTRY_EFFICIENCY_MODE", "observe_only")
    assert resolve_entry_efficiency_mode_with_reason() == (
        ENTRY_EFFICIENCY_OBSERVE_ONLY,
        ENTRY_EFFICIENCY_CONFIG_CONFLICT,
    )

    monkeypatch.setenv("ENTRY_EFFICIENCY_MODE", "not-a-rollout-mode")
    assert resolve_entry_efficiency_mode_with_reason() == (
        ENTRY_EFFICIENCY_OBSERVE_ONLY,
        ENTRY_EFFICIENCY_CONFIG_INVALID,
    )

    monkeypatch.setenv("ENTRY_EFFICIENCY_MODE", " ")
    assert resolve_entry_efficiency_mode_with_reason() == (
        ENTRY_EFFICIENCY_OBSERVE_ONLY,
        ENTRY_EFFICIENCY_CONFIG_INVALID,
    )

    monkeypatch.delenv("AP_ENTRY_EFFICIENCY_MODE", raising=False)
    monkeypatch.setenv("ENTRY_EFFICIENCY_MODE", "observe_only")
    assert resolve_entry_efficiency_mode_with_reason() == (
        ENTRY_EFFICIENCY_OBSERVE_ONLY,
        ENTRY_EFFICIENCY_CONFIG_VALID,
    )

    monkeypatch.setenv("AP_ENTRY_EFFICIENCY_MODE", "paper_authoritative")
    result = _decision(mode=None)
    assert result.authoritative is False
    assert result.evidence["rollout_config_reason"] == ENTRY_EFFICIENCY_CONFIG_CONFLICT
    monkeypatch.setenv("AP_ENTRY_EFFICIENCY_MODE", "observe_only")
    assert resolve_entry_efficiency_mode_with_reason() == (
        ENTRY_EFFICIENCY_OBSERVE_ONLY,
        ENTRY_EFFICIENCY_CONFIG_VALID,
    )


def test_entry_efficiency_generation_parser_only_infers_the_initial_absence():
    assert parse_entry_efficiency_generation(state="") == 0
    assert parse_entry_efficiency_generation(state=WAIT_CONFIRMATION) is None
    assert parse_entry_efficiency_generation(0, state="") == 0
    assert parse_entry_efficiency_generation("0", state=WAIT_CONFIRMATION) == 0
    assert parse_entry_efficiency_generation(7, state=REARM_FOR_REBREACH) == 7

    for raw in (None, "", " ", " 1", "1 ", "01", "-1", "junk", 1.0, True):
        assert parse_entry_efficiency_generation(raw, state=WAIT_CONFIRMATION) is None


@pytest.mark.parametrize(
    "plan, signal, runtime_mode, paper_flag",
    [
        (
            SimpleNamespace(execution_mode="paper", mode=None),
            {"execution_mode": "paper"},
            "paper",
            True,
        ),
        (
            SimpleNamespace(execution_mode="live", mode=None),
            {"execution_mode": "live"},
            "live",
            False,
        ),
    ],
)
def test_submit_execution_mode_requires_one_consensus_identity(
    plan, signal, runtime_mode, paper_flag
):
    assert _resolve_submit_execution_mode(
        plan, signal, runtime_mode, paper_flag
    ) == signal["execution_mode"]


@pytest.mark.parametrize(
    "plan, signal, runtime_mode, paper_flag",
    [
        (
            SimpleNamespace(execution_mode="paper", mode=None),
            {"execution_mode": "live"},
            "live",
            False,
        ),
        (
            SimpleNamespace(execution_mode="paper", mode="live"),
            {"execution_mode": "paper"},
            "paper",
            True,
        ),
        (
            SimpleNamespace(execution_mode="paper", mode=None),
            {"execution_mode": "paper", "mode": "live"},
            "paper",
            True,
        ),
        (
            SimpleNamespace(execution_mode="paper", mode=None),
            {"execution_mode": "paper"},
            "live",
            True,
        ),
        (
            SimpleNamespace(execution_mode="paper", mode=None),
            {"execution_mode": "paper"},
            "paper",
            "true",
        ),
        (
            SimpleNamespace(execution_mode="unknown", mode=None),
            {"execution_mode": "paper"},
            "paper",
            True,
        ),
        (
            SimpleNamespace(execution_mode=" ", mode=None),
            {"execution_mode": "paper"},
            "paper",
            True,
        ),
    ],
)
def test_submit_execution_mode_conflict_or_malformed_identity_fails_closed(
    plan, signal, runtime_mode, paper_flag
):
    assert _resolve_submit_execution_mode(
        plan, signal, runtime_mode, paper_flag
    ) is None


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


def test_paper_stale_efficiency_wait_is_ignored_for_unrelated_strategy_scope(monkeypatch):
    monkeypatch.setenv("AP_ENTRY_EFFICIENCY_MODE", ENTRY_EFFICIENCY_PAPER_AUTHORITATIVE)
    signal = _watch_signal(
        entry_efficiency_state=WAIT_CONFIRMATION,
        entry_efficiency_generation=1,
        entry_efficiency_next_eval_at=(
            datetime.now(timezone.utc) + timedelta(minutes=5)
        ).isoformat(),
    )
    signal["pattern"] = "1-2"
    watched = WatchedSignal(signal, overnight=False)

    assert watched.check(302.79, 302.81, quote_age_ms=1_000) == WatchState.PENDING
    assert watched.check(302.79, 302.81, quote_age_ms=1_000) == WatchState.TRIGGERED
    assert watched.breach_count == 2
    assert watched.entry_efficiency_state == WAIT_CONFIRMATION


@pytest.mark.parametrize("generation", ["", " ", "-1", "junk"])
def test_watcher_invalid_generation_never_honors_persisted_efficiency_wait(
    monkeypatch, generation
):
    monkeypatch.setenv("AP_ENTRY_EFFICIENCY_MODE", ENTRY_EFFICIENCY_PAPER_AUTHORITATIVE)
    watched = WatchedSignal(
        _watch_signal(
            entry_efficiency_state=WAIT_CONFIRMATION,
            entry_efficiency_generation=generation,
            entry_efficiency_next_eval_at=(
                datetime.now(timezone.utc) + timedelta(minutes=5)
            ).isoformat(),
        ),
        overnight=False,
    )

    assert watched.entry_efficiency_generation is None
    assert watched.entry_efficiency_generation_valid is False
    assert watched.check(302.79, 302.81, quote_age_ms=1_000) == WatchState.PENDING
    assert watched.check(302.79, 302.81, quote_age_ms=1_000) == WatchState.TRIGGERED
    assert watched.breach_count == 2


@pytest.mark.parametrize(
    "plan_modes, signal_mode, runtime_mode, paper_flag",
    [
        (("paper",), "live", "live", False),
        (("live",), "paper", "paper", True),
        (("paper", "live"), "paper", "paper", True),
    ],
)
def test_entry_efficiency_execution_identity_conflicts_fail_closed(
    plan_modes, signal_mode, runtime_mode, paper_flag
):
    plan = SimpleNamespace(
        execution_mode=plan_modes[0],
        mode=plan_modes[1] if len(plan_modes) > 1 else None,
    )
    signal = {"execution_mode": signal_mode}

    assert _resolve_entry_efficiency_execution_mode(
        plan, signal, runtime_mode, paper_flag
    ) is None


def test_entry_efficiency_execution_identity_requires_exact_paper_agreement():
    plan = SimpleNamespace(execution_mode="paper", mode=None)
    signal = {"execution_mode": "paper"}

    assert _resolve_entry_efficiency_execution_mode(
        plan, signal, "paper", True
    ) == "paper"


@pytest.mark.parametrize(
    "plan_mode, signal_mode, runtime_mode, paper_flag",
    [
        ("paper", "live", "live", False),
        ("live", "paper", "paper", True),
    ],
)
def test_execution_core_conflicting_identity_stays_on_existing_path(
    monkeypatch, plan_mode, signal_mode, runtime_mode, paper_flag
):
    monkeypatch.setenv("AP_ENTRY_EFFICIENCY_MODE", ENTRY_EFFICIENCY_PAPER_AUTHORITATIVE)
    efficiency_metadata = {
        "entry_efficiency_state": WAIT_CONFIRMATION,
        "entry_efficiency_generation": 1,
        "entry_efficiency_next_eval_at": (
            datetime.now(timezone.utc) - timedelta(seconds=1)
        ).isoformat(),
        "entry_efficiency_deadline_at": (
            datetime.now(timezone.utc) + timedelta(hours=1)
        ).isoformat(),
    }
    signal = _watch_signal(**efficiency_metadata)
    signal["execution_mode"] = signal_mode
    watched = WatchedSignal(signal, overnight=False)
    watched.trigger_crossed_at = FIRST_BREACH
    watched.last_quote_bid = 302.79
    watched.last_quote_ask = 302.81
    watched.last_quote_age_ms = 1_000

    plan = SimpleNamespace(
        metadata={
            **efficiency_metadata,
            "canonical_signal_id": "canonical-sig-efficiency",
        },
        execution_mode=plan_mode,
        client_id="jason@example.com",
        pattern="2-3",
        timeframe="1d",
        contract_symbol="AAPL260821P00304000",
    )
    osm = MagicMock()

    class ExistingPathReached(RuntimeError):
        pass

    refresh = MagicMock(side_effect=ExistingPathReached)
    observed_results = []
    real_evaluate_entry_efficiency = execution_core_module.evaluate_entry_efficiency

    def _observe_efficiency_result(**kwargs):
        result = real_evaluate_entry_efficiency(**kwargs)
        observed_results.append(result)
        return result

    core = SimpleNamespace(
        mode=runtime_mode.upper(),
        paper=paper_flag,
        execution_mode=runtime_mode,
        email="jason@example.com",
        client_id="jason@example.com",
        order_state_machine=osm,
        store=MagicMock(),
        contract_selector=MagicMock(),
        _breach_risk_check=lambda _watched: True,
        _recover_plan_for_revalidation=lambda _watched: plan,
        _refresh_hydrated_prebreach_plan=refresh,
        _emit_breach_diag=lambda *args, **kwargs: None,
    )

    from ap import intelligence_evaluation

    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr(
            intelligence_evaluation,
            "_ensure_intelligence_dispatched",
            lambda *a, **k: None,
        )
        patcher.setattr(
            execution_core_module,
            "evaluate_entry_efficiency",
            _observe_efficiency_result,
        )
        with pytest.raises(ExistingPathReached):
            APExecutionCore._on_entry_trigger(core, watched)

    assert observed_results
    assert observed_results[0].authoritative is False
    assert observed_results[0].decision not in {WAIT_CONFIRMATION, TERMINAL_INVALID}
    assert _resolve_entry_efficiency_execution_mode(
        plan, signal, runtime_mode, paper_flag
    ) is None
    refresh.assert_called_once()
    osm.cas_entry_efficiency_state.assert_not_called()
    osm.submit_existing_entry.assert_not_called()


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


@pytest.fixture(scope="module")
def postgres_entry_efficiency_db():
    """Exercise the real OSM CAS against PostgreSQL when the service exists."""
    psycopg2 = pytest.importorskip("psycopg2")
    dsn = os.getenv("DATABASE_URL", "")
    if not dsn:
        pytest.skip("DATABASE_URL is not configured")
    try:
        database = psycopg2.connect(dsn, connect_timeout=3)
    except Exception as exc:
        if os.getenv("CI"):
            raise
        pytest.skip(f"PostgreSQL service unavailable: {exc}")

    database.autocommit = True
    schema = f"pr436_cas_{uuid.uuid4().hex[:12]}"
    with database.cursor() as cursor:
        cursor.execute(f'CREATE SCHEMA "{schema}"')
        cursor.execute(
            f'''
            CREATE TABLE "{schema}".orders (
                local_order_id TEXT PRIMARY KEY,
                client_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                signal_id TEXT,
                canonical_signal_id TEXT,
                execution_mode TEXT,
                status TEXT,
                broker_order_id TEXT,
                submitted_ts TIMESTAMPTZ,
                meta JSONB,
                updated_ts TIMESTAMPTZ
            )
            '''
        )

    @contextmanager
    def _test_conn():
        # Match the production OSM contract: each CAS claimant gets its own
        # transaction/connection, allowing PostgreSQL row locking to arbitrate
        # simultaneous claimants instead of serializing the test in Python.
        connection = psycopg2.connect(dsn, connect_timeout=3)
        try:
            connection.autocommit = False
            with connection.cursor() as cursor:
                cursor.execute(f'SET search_path TO "{schema}"')
                yield cursor
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    patcher = pytest.MonkeyPatch()
    # cas_entry_efficiency_state resolves ``conn`` from the function's
    # defining globals.  Patch that exact namespace so the real CAS uses the
    # temporary schema connection rather than the process-wide production
    # connection imported during module initialization.
    patcher.setitem(
        APOrderStateMachine.cas_entry_efficiency_state.__globals__,
        "conn",
        _test_conn,
    )

    def replace_row(
        *,
        meta=None,
        local_order_id="order-cas",
        client_id="client-a",
        signal_id="signal-a",
        canonical_signal_id="canonical-a",
        execution_mode="paper",
        status="PENDING_TRIGGER",
        broker_order_id="",
        submitted_ts=None,
    ):
        row_meta = {} if meta is None else dict(meta)
        with database.cursor() as cursor:
            cursor.execute(f'DELETE FROM "{schema}".orders')
            cursor.execute(
                f'''
                INSERT INTO "{schema}".orders (
                    local_order_id, client_id, kind, signal_id,
                    canonical_signal_id, execution_mode, status,
                    broker_order_id, submitted_ts, meta
                ) VALUES (%s, %s, 'ENTRY', %s, %s, %s, %s, %s, %s, %s::jsonb)
                ''',
                (
                    local_order_id,
                    client_id,
                    signal_id,
                    canonical_signal_id,
                    execution_mode,
                    status,
                    broker_order_id,
                    submitted_ts,
                    json.dumps(row_meta),
                ),
            )

    def read_meta():
        with database.cursor() as cursor:
            cursor.execute(
                f'SELECT meta FROM "{schema}".orders WHERE local_order_id=%s',
                ("order-cas",),
            )
            row = cursor.fetchone()
        return dict(row[0] or {}) if row else {}

    try:
        yield SimpleNamespace(
            replace_row=replace_row,
            read_meta=read_meta,
            osm=APOrderStateMachine("client-a"),
        )
    finally:
        patcher.undo()
        with database.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA "{schema}" CASCADE')
        database.close()


def _postgres_efficiency_cas(osm, **overrides):
    values = {
        "local_order_id": "order-cas",
        "signal_id": "signal-a",
        "canonical_signal_id": "canonical-a",
        "client_id": "client-a",
        "execution_mode": "paper",
        "expected_state": "",
        "expected_generation": 0,
        "next_state": WAIT_CONFIRMATION,
        "next_generation": 1,
        "meta_patch": {"cas_test": True},
    }
    values.update(overrides)
    return osm.cas_entry_efficiency_state(**values)


def test_postgres_cas_accepts_valid_generations_and_rejects_stale_replay(
    postgres_entry_efficiency_db,
):
    db = postgres_entry_efficiency_db
    db.replace_row(meta={})

    assert _postgres_efficiency_cas(db.osm) is True
    assert db.read_meta()["entry_efficiency_generation"] == 1
    assert db.read_meta()["entry_efficiency_state"] == WAIT_CONFIRMATION

    assert _postgres_efficiency_cas(
        db.osm,
        expected_state=WAIT_CONFIRMATION,
        expected_generation=1,
        next_state=REARM_FOR_REBREACH,
        next_generation=2,
    ) is True
    assert db.read_meta()["entry_efficiency_generation"] == 2


def test_postgres_cas_allows_only_one_simultaneous_claimant(
    postgres_entry_efficiency_db,
):
    db = postgres_entry_efficiency_db
    db.replace_row(meta={})

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(lambda _claim: _postgres_efficiency_cas(db.osm), range(2))
        )

    assert sorted(results) == [False, True]
    assert db.read_meta()["entry_efficiency_generation"] == 1
    assert db.read_meta()["entry_efficiency_state"] == WAIT_CONFIRMATION

    # A stale claimant and a replay of the original claimant both miss the
    # exact durable generation; neither can overwrite the newer state.
    assert _postgres_efficiency_cas(
        db.osm,
        expected_state=WAIT_CONFIRMATION,
        expected_generation=1,
        next_state=REARM_FOR_REBREACH,
        next_generation=2,
    ) is False
    assert _postgres_efficiency_cas(
        db.osm,
        expected_state="",
        expected_generation=0,
        next_state=WAIT_CONFIRMATION,
        next_generation=1,
    ) is False
    assert db.read_meta()["entry_efficiency_generation"] == 2


@pytest.mark.parametrize(
    "meta, expected_generation",
    [
        ({"entry_efficiency_state": WAIT_CONFIRMATION}, 1),
        ({"entry_efficiency_state": WAIT_CONFIRMATION, "entry_efficiency_generation": ""}, 1),
        ({"entry_efficiency_state": WAIT_CONFIRMATION, "entry_efficiency_generation": " "}, 1),
        ({"entry_efficiency_state": WAIT_CONFIRMATION, "entry_efficiency_generation": " 1"}, 1),
        ({"entry_efficiency_state": WAIT_CONFIRMATION, "entry_efficiency_generation": "1 "}, 1),
        ({"entry_efficiency_state": WAIT_CONFIRMATION, "entry_efficiency_generation": "01"}, 1),
        ({"entry_efficiency_state": WAIT_CONFIRMATION, "entry_efficiency_generation": "-1"}, 1),
        ({"entry_efficiency_state": WAIT_CONFIRMATION, "entry_efficiency_generation": -1}, 1),
        ({"entry_efficiency_state": WAIT_CONFIRMATION, "entry_efficiency_generation": "junk"}, 1),
        ({"entry_efficiency_state": "", "entry_efficiency_generation": "junk"}, 0),
        ({"entry_efficiency_state": WAIT_CONFIRMATION, "entry_efficiency_generation": 2}, 1),
    ],
)
def test_postgres_cas_rejects_malformed_missing_and_stale_generation(
    postgres_entry_efficiency_db, meta, expected_generation
):
    db = postgres_entry_efficiency_db
    db.replace_row(meta=meta)

    assert _postgres_efficiency_cas(
        db.osm,
        expected_state=str(meta.get("entry_efficiency_state") or ""),
        expected_generation=expected_generation,
        next_state=REARM_FOR_REBREACH,
        next_generation=expected_generation + 1,
    ) is False
    assert db.read_meta() == meta


@pytest.mark.parametrize(
    "row_updates, call_updates",
    [
        ({"client_id": "client-b"}, {}),
        ({}, {"client_id": "client-b"}),
        ({"signal_id": "signal-b"}, {}),
        ({}, {"signal_id": "signal-b"}),
        ({"canonical_signal_id": "canonical-b"}, {}),
        ({}, {"canonical_signal_id": "canonical-b"}),
        ({"execution_mode": "live"}, {}),
        ({}, {"execution_mode": "live"}),
        ({"broker_order_id": "broker-1"}, {}),
        ({"submitted_ts": datetime.now(timezone.utc)}, {}),
        (
            {"meta": {"submit_intent_at": "2026-08-12T13:00:00+00:00"}},
            {},
        ),
        ({"status": "SUBMITTED"}, {}),
    ],
)
def test_postgres_cas_refuses_identity_broker_and_status_conflicts(
    postgres_entry_efficiency_db, row_updates, call_updates
):
    db = postgres_entry_efficiency_db
    row_meta = {
        "entry_efficiency_state": WAIT_CONFIRMATION,
        "entry_efficiency_generation": 1,
    }
    row_kwargs = dict(row_updates)
    row_meta.update(row_kwargs.pop("meta", {}))
    db.replace_row(meta=row_meta, **row_kwargs)

    assert _postgres_efficiency_cas(
        db.osm,
        expected_state=WAIT_CONFIRMATION,
        expected_generation=1,
        next_state=REARM_FOR_REBREACH,
        next_generation=2,
        **call_updates,
    ) is False
    assert db.read_meta() == row_meta
