from __future__ import annotations

import os
import threading
import types
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import ap_execution_core as ec_mod
from tests.test_dte_ladder import _load_selector


def _make_plan(*, reason_code: str | None = None, breach_attempt_count: int = 0, execution_mode: str = "live"):
    return SimpleNamespace(
        ticker="AVGO",
        side="CALL",
        contract_symbol="DEFERRED:AVGO",
        limit_price=0.01,
        contracts=1,
        max_position_usd=250.0,
        trigger_price=210.0,
        signal_id="sig-219-1",
        client_id="jasoncosby1@gmail.com",
        execution_mode=execution_mode,
        metadata={
            "contract_deferred": True,
            "queue_id": 219,
            "breach_attempt_count": breach_attempt_count,
        },
        _reason_code=reason_code,
    )


def _make_watched():
    return SimpleNamespace(
        ticker="AVGO",
        trigger_price=210.25,
        signal={
            "signal_id": "sig-219-1",
            "client_id": "jasoncosby1@gmail.com",
            "local_order_id": "local-219",
            "queue_id": 219,
            "contract_deferred": True,
            "score": 81,
        },
    )


class _Selector:
    def __init__(self, reason_code: str, stage: str = "chain_fetch", explanation: str = "selector miss"):
        self.reason_code = reason_code
        self.stage = stage
        self.explanation = explanation

    def select(self, _plan):
        return None

    def get_last_failure(self):
        return {
            "stage": self.stage,
            "reason_code": self.reason_code,
            "explanation": self.explanation,
        }


def _make_core(selector: _Selector):
    core = ec_mod.APExecutionCore.__new__(ec_mod.APExecutionCore)
    core.paper = False
    core.mode = "LIVE"
    core.email = "jasoncosby1@gmail.com"
    core.client_id = "jasoncosby1@gmail.com"
    core.contract_selector = selector
    core.order_state_machine = MagicMock()
    core.order_state_machine.expire_pending_entry.return_value = True
    core.order_state_machine.transition.return_value = True
    core.order_state_machine.update_order_meta.return_value = True
    core.order_state_machine.submit_existing_entry = MagicMock()
    core.store = MagicMock()
    core.entry_watcher = MagicMock()
    core.exit_eng = MagicMock()
    core.tracker = MagicMock()
    core.position_manager = MagicMock()
    core.broker = SimpleNamespace(cfg=SimpleNamespace(base_url="https://api.tradier.com"))
    return core


class _ThreadRecorder:
    def __init__(self):
        self.starts = 0
        self.targets: list = []

    def factory(self, *, target=None, daemon=None, name=None):
        recorder = self

        class _T:
            def start(self_nonlocal):
                recorder.starts += 1
                recorder.targets.append(target)

        return _T()


def test_retryable_chain_failure_rearms_without_submit_or_terminalize(monkeypatch):
    class _EarlyDatetime:
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 6, 30, 9, 35, 0, tzinfo=tz)

    monkeypatch.setattr(ec_mod, "datetime", _EarlyDatetime)
    write_calls = []
    monkeypatch.setattr(
        ec_mod.APExecutionCore,
        "_breach_risk_check",
        lambda self, watched: True,
    )
    plan = _make_plan(reason_code="CHAIN_PROVIDER_ERROR", execution_mode="live")
    monkeypatch.setattr(
        ec_mod.APExecutionCore,
        "_recover_plan_for_revalidation",
        lambda self, watched: plan,
    )
    monkeypatch.setattr(
        "ap.queue.write_deferred_breach_last_error",
        lambda queue_id, **kwargs: write_calls.append((queue_id, kwargs)),
    )
    recorder = _ThreadRecorder()
    monkeypatch.setattr(threading, "Thread", recorder.factory)

    core = _make_core(_Selector("CHAIN_PROVIDER_ERROR", explanation="tradier warmup miss"))
    watched = _make_watched()
    with monkeypatch.context() as m:
        m.setenv("MAX_BREACH_SELECTOR_RETRIES", "3")
        m.setenv("BREACH_SELECTOR_RETRY_DELAY_SECONDS", "1")
        m.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "945")
        core._on_entry_trigger(watched)

    core.order_state_machine.submit_existing_entry.assert_not_called()
    core.order_state_machine.expire_pending_entry.assert_not_called()
    core.order_state_machine.transition.assert_not_called()
    assert recorder.starts == 1
    assert write_calls[0][0] == 219
    assert write_calls[0][1]["client_id"] == "jasoncosby1@gmail.com"
    assert write_calls[0][1]["reason_code"] == "CONTRACT_SELECTION_RETRY:CHAIN_PROVIDER_ERROR"
    update_meta = core.order_state_machine.update_order_meta.call_args[0][1]
    assert update_meta["contract_selection_status"] == "CONTRACT_SELECTION_RETRY"
    assert update_meta["last_breach_selector_audit"]["execution_mode"] == "live"


def test_no_expiration_in_dte_window_rearms_without_terminalizing(monkeypatch):
    class _EarlyDatetime:
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 6, 30, 9, 35, 0, tzinfo=tz)

    monkeypatch.setattr(ec_mod, "datetime", _EarlyDatetime)
    write_calls = []
    monkeypatch.setattr(ec_mod.APExecutionCore, "_breach_risk_check", lambda self, watched: True)
    monkeypatch.setattr(
        ec_mod.APExecutionCore,
        "_recover_plan_for_revalidation",
        lambda self, watched: _make_plan(reason_code="NO_EXPIRATION_IN_DTE_WINDOW", execution_mode="live"),
    )
    monkeypatch.setattr(
        "ap.queue.write_deferred_breach_last_error",
        lambda queue_id, **kwargs: write_calls.append((queue_id, kwargs)),
    )
    recorder = _ThreadRecorder()
    monkeypatch.setattr(threading, "Thread", recorder.factory)

    core = _make_core(_Selector("NO_EXPIRATION_IN_DTE_WINDOW", explanation="dte ladder warming up"))
    core._on_entry_trigger(_make_watched())

    core.order_state_machine.submit_existing_entry.assert_not_called()
    core.order_state_machine.expire_pending_entry.assert_not_called()
    core.order_state_machine.transition.assert_not_called()
    assert recorder.starts == 1
    assert write_calls[0][1]["reason_code"] == "CONTRACT_SELECTION_RETRY:NO_EXPIRATION_IN_DTE_WINDOW"


def test_max_retry_count_terminalizes_exactly_once(monkeypatch):
    monkeypatch.setattr(ec_mod.APExecutionCore, "_breach_risk_check", lambda self, watched: True)
    monkeypatch.setattr(
        ec_mod.APExecutionCore,
        "_recover_plan_for_revalidation",
        lambda self, watched: _make_plan(reason_code="CHAIN_PROVIDER_ERROR", breach_attempt_count=3),
    )
    monkeypatch.setattr("ap.queue.write_deferred_breach_last_error", lambda *args, **kwargs: None)
    recorder = _ThreadRecorder()
    monkeypatch.setattr(threading, "Thread", recorder.factory)

    core = _make_core(_Selector("CHAIN_PROVIDER_ERROR"))
    core._on_entry_trigger(_make_watched())

    assert recorder.starts == 0
    core.order_state_machine.expire_pending_entry.assert_called_once()
    core.order_state_machine.transition.assert_not_called()
    core.order_state_machine.submit_existing_entry.assert_not_called()


def test_cutoff_after_945_terminalizes(monkeypatch):
    class _LateDatetime:
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 6, 30, 9, 46, 0, tzinfo=tz)

    monkeypatch.setattr(ec_mod, "datetime", _LateDatetime)
    monkeypatch.setattr(ec_mod.APExecutionCore, "_breach_risk_check", lambda self, watched: True)
    monkeypatch.setattr(
        ec_mod.APExecutionCore,
        "_recover_plan_for_revalidation",
        lambda self, watched: _make_plan(reason_code="CHAIN_PROVIDER_ERROR"),
    )
    monkeypatch.setattr("ap.queue.write_deferred_breach_last_error", lambda *args, **kwargs: None)
    recorder = _ThreadRecorder()
    monkeypatch.setattr(threading, "Thread", recorder.factory)

    core = _make_core(_Selector("CHAIN_PROVIDER_ERROR"))
    with monkeypatch.context() as m:
        m.setenv("MAX_BREACH_SELECTOR_RETRIES", "3")
        m.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "945")
        core._on_entry_trigger(_make_watched())

    assert recorder.starts == 0
    core.order_state_machine.expire_pending_entry.assert_called_once()


def test_quality_reject_terminalizes_immediately(monkeypatch):
    monkeypatch.setattr(ec_mod.APExecutionCore, "_breach_risk_check", lambda self, watched: True)
    monkeypatch.setattr(
        ec_mod.APExecutionCore,
        "_recover_plan_for_revalidation",
        lambda self, watched: _make_plan(reason_code="OI_TOO_LOW"),
    )
    monkeypatch.setattr("ap.queue.write_deferred_breach_last_error", lambda *args, **kwargs: None)
    recorder = _ThreadRecorder()
    monkeypatch.setattr(threading, "Thread", recorder.factory)

    core = _make_core(_Selector("OI_TOO_LOW", stage="quality_summary", explanation="illiquid chain"))
    core._on_entry_trigger(_make_watched())

    assert recorder.starts == 0
    core.order_state_machine.expire_pending_entry.assert_called_once()
    update_meta = core.order_state_machine.update_order_meta.call_args[0][1]
    assert update_meta["deferred_breach_failure"] is True


def test_dte_ladder_all_retryable_failures_remain_retryable(monkeypatch):
    mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
    APContractSelectionEngine = mod.APContractSelectionEngine
    sel = object.__new__(APContractSelectionEngine)
    sel.dte_ladder_enabled = True
    sel.dte_bucket_a_max = 2
    sel.dte_bucket_b_max = 7
    sel.dte_ladder_probe_per_bucket = 3
    sel._last_failure = None
    sel._last_dte_ladder_audit = None
    sel._fetch_expirations_list = MagicMock(return_value=["2026-07-01", "2026-07-08"])

    def _fake_select(plan, *, expiration_override=None):
        sel._last_failure = {
            "stage": "chain_fetch",
            "reason_code": "CHAIN_PROVIDER_EMPTY_OPTIONS" if expiration_override == "2026-07-01" else "CHAIN_PARSE_EMPTY",
            "explanation": "provider warming up",
        }
        return None

    sel.select = _fake_select
    result = sel._select_with_dte_ladder(SimpleNamespace(ticker="AVGO", timeframe="1d", metadata={"deferred_breach_selection": True}))
    assert result is None
    assert sel._last_failure["reason_code"] in {"CHAIN_PROVIDER_EMPTY_OPTIONS", "CHAIN_PARSE_EMPTY"}


def test_dte_ladder_preserves_quality_reason_when_chain_rows_exist(monkeypatch):
    mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
    APContractSelectionEngine = mod.APContractSelectionEngine
    sel = object.__new__(APContractSelectionEngine)
    sel.dte_ladder_enabled = True
    sel.dte_bucket_a_max = 2
    sel.dte_bucket_b_max = 7
    sel.dte_ladder_probe_per_bucket = 2
    sel._last_failure = None
    sel._last_dte_ladder_audit = None
    sel._fetch_expirations_list = MagicMock(return_value=["2026-07-01"])

    def _fake_select(plan, *, expiration_override=None):
        sel._last_failure = {
            "stage": "quality_summary",
            "reason_code": "OI_TOO_LOW",
            "explanation": "real rows but illiquid",
        }
        return None

    sel.select = _fake_select
    result = sel._select_with_dte_ladder(SimpleNamespace(ticker="AVGO", timeframe="1d", metadata={"deferred_breach_selection": True}))
    assert result is None
    assert sel._last_failure["reason_code"] == "OI_TOO_LOW"


def test_chain_auth_error_remains_terminal(monkeypatch):
    mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
    APContractSelectionEngine = mod.APContractSelectionEngine
    sel = object.__new__(APContractSelectionEngine)
    sel.dte_ladder_enabled = True
    sel.dte_bucket_a_max = 2
    sel.dte_bucket_b_max = 7
    sel.dte_ladder_probe_per_bucket = 2
    sel._last_failure = None
    sel._last_dte_ladder_audit = None
    sel._fetch_expirations_list = MagicMock(return_value=["2026-07-01", "2026-07-08"])

    def _fake_select(plan, *, expiration_override=None):
        sel._last_failure = {
            "stage": "chain_fetch",
            "reason_code": "CHAIN_AUTH_ERROR",
            "explanation": "401",
        }
        return None

    sel.select = _fake_select
    result = sel._select_with_dte_ladder(SimpleNamespace(ticker="AVGO", timeframe="1d", metadata={"deferred_breach_selection": True}))
    assert result is None
    assert sel._last_failure["reason_code"] == "CHAIN_AUTH_ERROR"
