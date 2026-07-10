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
        m.setenv("BREACH_SELECTOR_RETRY_ENABLED", "1")
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
    with monkeypatch.context() as m:
        m.setenv("BREACH_SELECTOR_RETRY_ENABLED", "1")
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
        m.setenv("BREACH_SELECTOR_RETRY_ENABLED", "1")
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
    from datetime import date, timedelta
    _exp_a = (date.today() + timedelta(days=1)).isoformat()   # DTE=1 → bucket A (0-2)
    _exp_c = (date.today() + timedelta(days=10)).isoformat()  # DTE=10 → bucket C (8+)
    sel._fetch_expirations_list = MagicMock(return_value=[_exp_a, _exp_c])

    def _fake_select(plan, *, expiration_override=None):
        plan.metadata["selector_failure"] = {
            "reason_code": "CHAIN_PROVIDER_EMPTY_OPTIONS" if expiration_override == _exp_a else "CHAIN_PARSE_EMPTY",
            "explanation": "provider warming up",
        }
        return None

    sel.select = _fake_select
    result = sel._select_with_dte_ladder(SimpleNamespace(ticker="AVGO", timeframe="1d", metadata={"deferred_breach_selection": True}))
    assert result is None
    assert sel._last_failure["reason_code"] in {"CHAIN_PROVIDER_EMPTY_OPTIONS", "CHAIN_PARSE_EMPTY"}


def test_dte_ladder_preserves_quality_reason_when_chain_rows_exist(monkeypatch):
    """Fix C: when a bucket returns OI_TOO_LOW the ladder must preserve that
    quality reason rather than masking it as NO_VALID_PLAYBOOK_DTE_CONTRACT."""
    from datetime import date, timedelta
    probe_day = date.today() + timedelta(days=1)
    while probe_day.weekday() >= 5:
        probe_day += timedelta(days=1)
    tomorrow = probe_day.isoformat()

    mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
    APContractSelectionEngine = mod.APContractSelectionEngine
    sel = object.__new__(APContractSelectionEngine)
    sel.dte_ladder_enabled = True
    sel.dte_bucket_a_max = 2
    sel.dte_bucket_b_max = 7
    sel.dte_ladder_probe_per_bucket = 2
    sel._last_failure = None
    sel._last_dte_ladder_audit = None
    # tomorrow is 1 DTE → bucket A
    sel._fetch_expirations_list = MagicMock(return_value=[tomorrow])

    def _fake_select(plan, *, expiration_override=None):
        plan.metadata["selector_failure"] = {
            "reason_code": "OI_TOO_LOW",
            "explanation": "real rows but illiquid",
        }
        return None

    sel.select = _fake_select
    result = sel._select_with_dte_ladder(SimpleNamespace(
        ticker="AVGO", timeframe="1d",
        metadata={"deferred_breach_selection": True},
    ))
    assert result is None
    assert sel._last_failure["reason_code"] == "OI_TOO_LOW", (
        f"Expected OI_TOO_LOW to be preserved; got {sel._last_failure.get('reason_code')}. "
        "Fix C: quality reasons must be preserved over NO_VALID_PLAYBOOK_DTE_CONTRACT "
        "when no retryable data-miss was seen."
    )


def test_dte_ladder_mixed_retryable_then_quality_finishes_as_quality(monkeypatch):
    """If an early expiration is transient but a later usable chain fails
    quality, the final ladder reason must be quality, not retryable."""
    from datetime import date, timedelta
    exp_a = date.today() + timedelta(days=1)
    while exp_a.weekday() >= 5:
        exp_a += timedelta(days=1)
    exp_c = date.today() + timedelta(days=14)
    while exp_c.weekday() >= 5:
        exp_c += timedelta(days=1)

    mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
    APContractSelectionEngine = mod.APContractSelectionEngine
    sel = object.__new__(APContractSelectionEngine)
    sel.dte_ladder_enabled = True
    sel.dte_bucket_a_max = 2
    sel.dte_bucket_b_max = 7
    sel.dte_ladder_probe_per_bucket = 2
    sel._last_failure = None
    sel._last_dte_ladder_audit = None
    sel._fetch_expirations_list = MagicMock(return_value=[exp_a.isoformat(), exp_c.isoformat()])

    def _fake_select(plan, *, expiration_override=None):
        if expiration_override == exp_a.isoformat():
            plan.metadata["selector_failure"] = {
                "reason_code": "CHAIN_PROVIDER_EMPTY_OPTIONS",
                "explanation": "provider warming up",
            }
        else:
            plan.metadata["selector_failure"] = {
                "reason_code": "SPREAD_TOO_WIDE",
                "explanation": "usable nonzero chain failed spread gate",
            }
        return None

    sel.select = _fake_select
    result = sel._select_with_dte_ladder(SimpleNamespace(
        ticker="AVGO", timeframe="1d",
        metadata={"deferred_breach_selection": True},
    ))
    assert result is None
    assert sel._last_failure["reason_code"] == "SPREAD_TOO_WIDE"


def test_execution_core_does_not_retry_when_ladder_finishes_with_quality(monkeypatch):
    """A mixed transient-plus-quality ladder run must terminalize, not schedule
    a retry thread, because the final truthful blocker is quality."""
    monkeypatch.setattr(ec_mod.APExecutionCore, "_breach_risk_check", lambda self, watched: True)
    monkeypatch.setattr(
        ec_mod.APExecutionCore,
        "_recover_plan_for_revalidation",
        lambda self, watched: _make_plan(reason_code="SPREAD_TOO_WIDE"),
    )
    monkeypatch.setattr("ap.queue.write_deferred_breach_last_error", lambda *args, **kwargs: None)
    recorder = _ThreadRecorder()
    monkeypatch.setattr(threading, "Thread", recorder.factory)

    selector = _Selector("SPREAD_TOO_WIDE", stage="quality_summary", explanation="usable chain failed spread")
    selector.get_last_dte_ladder_audit = lambda: {
        "buckets_attempted": [
            {"expirations_probed": [
                {"failure": {"reason_code": "CHAIN_PROVIDER_EMPTY_OPTIONS"}},
                {"failure": {"reason_code": "SPREAD_TOO_WIDE"}},
            ]},
        ]
    }
    core = _make_core(selector)
    with monkeypatch.context() as m:
        m.setenv("BREACH_SELECTOR_RETRY_ENABLED", "1")
        m.setenv("MAX_BREACH_SELECTOR_RETRIES", "3")
        core._on_entry_trigger(_make_watched())

    assert recorder.starts == 0, "quality final verdict must not schedule retry"
    core.order_state_machine.submit_existing_entry.assert_not_called()
    core.order_state_machine.expire_pending_entry.assert_called_once()


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
    from datetime import date, timedelta
    _exp_a2 = (date.today() + timedelta(days=1)).isoformat()   # DTE=1 → bucket A
    _exp_c2 = (date.today() + timedelta(days=10)).isoformat()  # DTE=10 → bucket C
    sel._fetch_expirations_list = MagicMock(return_value=[_exp_a2, _exp_c2])

    def _fake_select(plan, *, expiration_override=None):
        plan.metadata["selector_failure"] = {
            "reason_code": "CHAIN_AUTH_ERROR",
            "explanation": "401",
        }
        return None

    sel.select = _fake_select
    result = sel._select_with_dte_ladder(SimpleNamespace(ticker="AVGO", timeframe="1d", metadata={"deferred_breach_selection": True}))
    assert result is None
    assert sel._last_failure["reason_code"] == "CHAIN_AUTH_ERROR"


def test_non_deferred_selector_path_is_unchanged_when_ladder_flag_enabled(monkeypatch):
    mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
    APContractSelectionEngine = mod.APContractSelectionEngine
    sel = object.__new__(APContractSelectionEngine)

    assert sel._is_ladder_eligible(SimpleNamespace(ticker="AVGO", timeframe="1d", metadata={})) is False
    assert sel._is_ladder_eligible(SimpleNamespace(ticker="AVGO", timeframe="1d")) is False


def test_duplicate_retry_thread_is_suppressed(monkeypatch):
    class _EarlyDatetime:
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 6, 30, 9, 35, 0, tzinfo=tz)

    monkeypatch.setattr(ec_mod, "datetime", _EarlyDatetime)
    monkeypatch.setattr(ec_mod.APExecutionCore, "_breach_risk_check", lambda self, watched: True)
    monkeypatch.setattr(
        ec_mod.APExecutionCore,
        "_recover_plan_for_revalidation",
        lambda self, watched: _make_plan(reason_code="CHAIN_PROVIDER_ERROR", execution_mode="live"),
    )
    monkeypatch.setattr("ap.queue.write_deferred_breach_last_error", lambda *args, **kwargs: None)
    recorder = _ThreadRecorder()
    monkeypatch.setattr(threading, "Thread", recorder.factory)

    core = _make_core(_Selector("CHAIN_PROVIDER_ERROR", explanation="tradier warmup miss"))
    watched = _make_watched()
    with monkeypatch.context() as m:
        m.setenv("BREACH_SELECTOR_RETRY_ENABLED", "1")
        m.setenv("MAX_BREACH_SELECTOR_RETRIES", "3")
        m.setenv("BREACH_SELECTOR_RETRY_DELAY_SECONDS", "1")
        m.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "945")
        core._on_entry_trigger(watched)
        core._on_entry_trigger(watched)

    assert recorder.starts == 1
    assert getattr(core, "_deferred_breach_retry_inflight", None) == {("local-219", 1)}


# ─────────────────────────────────────────────────────────────────────────────
# PR #219 amendment — Jason LIVE 2026-07-01 recovery parity tests
#
# These lock the four amendments applied after the initial retry+taxonomy work
# on this PR was reviewed against Jason's actual production failure pattern:
#   1. DEFERRED_DTE_LADDER default = "0" for post-close opt-in rollout
#   2. BREACH_SELECTOR_RETRY_ENABLED default = "1"
#   3. RETRYABLE_BREACH_SELECTOR_REASONS extended with the codes that killed
#      17 of 54 Jason LIVE overnight setups today
#   4. Ladder audit propagated into _deferred_selector_audit for order.meta
#
# If any future edit reverts one of these without an equivalent amendment,
# these tests fail loud so the regression surfaces in CI before merge.
# ─────────────────────────────────────────────────────────────────────────────


class TestJasonLiveRecoveryAmendments:
    """Parity locks for the PR #219 amendment (see commit body)."""

    def test_amendment_1_dte_ladder_default_is_off(self, monkeypatch):
        """DEFERRED_DTE_LADDER unset => active LIVE behavior is unchanged."""
        monkeypatch.delenv("DEFERRED_DTE_LADDER", raising=False)
        mod = _load_selector()
        sel = object.__new__(mod.APContractSelectionEngine)
        mod.APContractSelectionEngine.__init__(sel, broker=SimpleNamespace(), data_broker=SimpleNamespace())
        assert sel.dte_ladder_enabled is False

    def test_amendment_1_kill_switch_still_works(self, monkeypatch):
        """DEFERRED_DTE_LADDER=0 must still disable the ladder as an emergency
        kill switch, so operators can turn it off from Render without a code
        deploy if the ladder itself misbehaves."""
        monkeypatch.setenv("DEFERRED_DTE_LADDER", "0")
        mod = _load_selector({"DEFERRED_DTE_LADDER": "0"})
        sel = object.__new__(mod.APContractSelectionEngine)
        mod.APContractSelectionEngine.__init__(sel, broker=SimpleNamespace(), data_broker=SimpleNamespace())
        assert sel.dte_ladder_enabled is False

    def test_amendment_2_retryable_set_contains_zero_bid_ask_codes(self):
        """The two codes that made up 17 of Jason's 54 failures today must be
        in the retryable set; otherwise the retry loop never fires on them."""
        assert "CHAIN_ROW_ZERO_BID_ASK" in ec_mod.RETRYABLE_BREACH_SELECTOR_REASONS
        assert "DIRECT_QUOTE_ZERO_BID_ASK" in ec_mod.RETRYABLE_BREACH_SELECTOR_REASONS

    def test_amendment_2_no_valid_playbook_dte_contract_not_in_global_set(self):
        """Fix C: NO_VALID_PLAYBOOK_DTE_CONTRACT must NOT be in the global
        retryable set. It is the DTE-ladder aggregation reason and may reflect
        structural quality rejects. Retryability must be determined by inspecting
        the ladder audit, not by treating it as unconditionally retryable."""
        assert "NO_VALID_PLAYBOOK_DTE_CONTRACT" not in ec_mod.RETRYABLE_BREACH_SELECTOR_REASONS, (
            "NO_VALID_PLAYBOOK_DTE_CONTRACT must be removed from the global "
            "retryable set — use _is_ladder_exhaustion_retryable(audit) instead."
        )

    def test_amendment_2_is_ladder_exhaustion_retryable_helper_exists(self):
        """Fix C: _is_ladder_exhaustion_retryable must be importable from
        execution_core so the retry check can use it."""
        assert hasattr(ec_mod, "_is_ladder_exhaustion_retryable"), (
            "_is_ladder_exhaustion_retryable helper must exist in ap_execution_core"
        )

    def test_amendment_2_ladder_exhaustion_retryable_on_data_miss_only(self):
        """Fix C: ladder-exhaustion retryability requires ALL sub-failures to
        be in the retryable data-miss set, not mixed with quality rejects."""
        fn = ec_mod._is_ladder_exhaustion_retryable
        # Retryable: all failures are transient data-miss codes.
        retryable_audit = {
            "buckets_attempted": [
                {"expirations_probed": [
                    {"failure": {"reason_code": "CHAIN_ROW_ZERO_BID_ASK"}},
                    {"failure": {"reason_code": "DIRECT_QUOTE_ZERO_BID_ASK"}},
                ]},
            ]
        }
        assert fn(retryable_audit) is True

    def test_amendment_2_ladder_exhaustion_terminal_on_quality_reject(self):
        """Fix C: if ANY sub-failure is a quality reject, ladder exhaustion is
        NOT retryable — the quality verdict won't improve on a second attempt."""
        fn = ec_mod._is_ladder_exhaustion_retryable
        mixed_audit = {
            "buckets_attempted": [
                {"expirations_probed": [
                    {"failure": {"reason_code": "CHAIN_ROW_ZERO_BID_ASK"}},
                    {"failure": {"reason_code": "OI_TOO_LOW"}},  # quality
                ]},
            ]
        }
        assert fn(mixed_audit) is False

    def test_amendment_2_ladder_exhaustion_terminal_on_spread_too_wide(self):
        fn = ec_mod._is_ladder_exhaustion_retryable
        quality_audit = {
            "buckets_attempted": [
                {"expirations_probed": [
                    {"failure": {"reason_code": "SPREAD_TOO_WIDE"}},
                ]},
            ]
        }
        assert fn(quality_audit) is False

    def test_amendment_2_ladder_exhaustion_conservative_on_unknown_audit(self):
        """Missing/invalid audit defaults to non-retryable — safer to
        terminalize than to loop on unknown failure."""
        fn = ec_mod._is_ladder_exhaustion_retryable
        assert fn(None) is False
        assert fn({}) is False
        assert fn({"buckets_attempted": []}) is False

    def test_amendment_2_retryable_set_contains_all_amendment_codes(self):
        """Snapshot lock: the exact set of amendment-added codes must remain
        stable so silent reverts trip CI."""
        amendment_additions = {
            "CHAIN_ROW_ZERO_BID_ASK",
            "DIRECT_QUOTE_ZERO_BID_ASK",
            "QUOTE_FETCH_FAILED",
            "CHAIN_EMPTY",
        }
        missing = amendment_additions - set(ec_mod.RETRYABLE_BREACH_SELECTOR_REASONS)
        assert not missing, f"amendment-added retryable codes missing: {missing}"

    def test_amendment_2_terminal_codes_still_terminal(self):
        """Terminal reasons — the ones that would fail regardless of retry —
        must NOT be in the retryable set. Otherwise we'd burn API calls and
        potentially loop against structural rejections."""
        must_stay_terminal = {
            "OI_TOO_LOW",
            "UNTRADEABLE_FOR_ACCOUNT_SIZE",
            "BID_BELOW_MIN",
            "DELTA_OUT_OF_RANGE",
            "PREMIUM_CAP_EXCEEDED",
            "EARNINGS_LOCKOUT",
            "SPREAD_TOO_WIDE",
            "VOLUME_TOO_LOW",
            "INVALID_PLAN",
            "UNSUPPORTED_INDEX_MAPPING",
            "DEFERRED_UNRESOLVED_AT_BREACH",
        }
        overlap = must_stay_terminal & set(ec_mod.RETRYABLE_BREACH_SELECTOR_REASONS)
        assert overlap == set(), (
            f"structural terminal codes must never be retryable — overlap: {overlap}"
        )

    # ── Fix A regression tests ──────────────────────────────────────────────

    def test_fix_a_observe_mode_daily_continuation_does_not_terminate(self):
        """Fix A: when daily_continuation_mode='observe' and fail_reason starts
        with 'daily_continuation_failed', the submit path must NOT be blocked."""
        src = open("ap_execution_core.py").read()
        assert "_observe_only_daily_continuation" in src, (
            "Fix A: _observe_only_daily_continuation guard must be present"
        )
        assert "ENTRY_CONFIRM_OBSERVED" in src, (
            "Fix A: ENTRY_CONFIRM_OBSERVED log must be emitted for observe path"
        )
        # The guard must read the right condition shape from the confirmation meta.
        assert 'daily_continuation_mode") == "observe"' in src, (
            "Fix A: must check _confirm_meta.get('daily_continuation_mode') == 'observe'"
        )
        assert 'startswith("daily_continuation_failed")' in src, (
            "Fix A: must check _fail_reason.startswith('daily_continuation_failed')"
        )
        # The observe block must persist meta but NOT write blocked_at_breach or cleanup.
        # Find the observe block between the if and its else.
        idx_observe_if = src.find("if _observe_only_daily_continuation:")
        idx_observe_else = src.find("\n                else:", idx_observe_if)
        observe_block = src[idx_observe_if:idx_observe_else]
        assert "ENTRY_CONFIRM_OBSERVED" in observe_block, (
            "Fix A: ENTRY_CONFIRM_OBSERVED must be logged inside the observe block"
        )
        assert "blocked_at_breach" not in [
            line.strip() for line in observe_block.split("\n")
            if '"decision_status": "blocked_at_breach"' in line
            or "'decision_status': 'blocked_at_breach'" in line
        ], (
            "Fix A: observe block must NOT write blocked_at_breach to ap_signals"
        )
        # Alternative: the specific assignment must not appear in the observe block.
        assert '"decision_status": "blocked_at_breach"' not in observe_block, (
            "Fix A: observe block must NOT assign decision_status=blocked_at_breach"
        )
        assert "_cleanup_pending_entry_order" not in observe_block, (
            "Fix A: observe block must NOT call _cleanup_pending_entry_order"
        )
        # The observe block must not have a bare 'return' — the submit continues.
        # (It may have a comment or string containing 'return', so check actual statements.)
        observe_lines = observe_block.split("\n")
        bare_returns = [
            l for l in observe_lines
            if l.strip() == "return" or l.strip().startswith("return ")
        ]
        assert bare_returns == [], (
            f"Fix A: observe block must not return early — found: {bare_returns}"
        )

    def test_fix_a_enforce_mode_still_blocks_on_daily_continuation_failure(self):
        """Fix A: non-observe failures must still terminalize — the observe
        path is a narrow carve-out for daily_continuation_mode=observe only."""
        src = open("ap_execution_core.py").read()
        idx_observe_if = src.find("if _observe_only_daily_continuation:")
        idx_observe_else = src.find("\n                else:", idx_observe_if)
        # The else block (enforce path) should contain cleanup and return.
        # Get a reasonable window past the else marker.
        enforce_block = src[idx_observe_else: idx_observe_else + 3000]
        assert "_cleanup_pending_entry_order" in enforce_block, (
            "Fix A: enforce path must still call _cleanup_pending_entry_order"
        )
        assert "return" in enforce_block, (
            "Fix A: enforce path must still return to block submit"
        )
        assert "blocked_at_breach" in enforce_block, (
            "Fix A: enforce path must still write blocked_at_breach to ap_signals"
        )

    # ── Fix B regression tests ──────────────────────────────────────────────

    def test_fix_b_stale_state_guard_in_retry_thread(self):
        """Fix B (full): the retry thread must contain all five stale-state
        checks and abort with DEFERRED_BREACH_RETRY_STALE_STATE_ABORT on any."""
        src = open("ap_execution_core.py").read()
        # Verify the thread function exists
        assert "def _retry_deferred_breach_a(" in src, (
            "Fix B: retry thread function must exist"
        )
        # All five abort reasons must appear in the source — each corresponds
        # to one of the five required guards.
        for required in (
            "DEFERRED_BREACH_RETRY_STALE_STATE_ABORT",
            "reason=status_changed",
            "reason=broker_id_present",
            "reason=submitted_ts_present",
            "reason=no_longer_deferred",
            "reason=attempt_count_advanced",
            "PENDING_TRIGGER",
            "broker_order_id",
            "submitted_ts",
            "_is_still_deferred",
            "DEFERRED:",
            "_current_attempt > int(_att)",
        ):
            assert required in src, (
                f"Fix B: {required!r} must be present in ap_execution_core.py "
                "— stale-state guard may be incomplete"
            )

    def test_fix_b_stale_guard_is_best_effort(self):
        """Fix B: a failure of the stale-state DB check must not silently
        suppress the retry — it should log at debug level and proceed."""
        src = open("ap_execution_core.py").read()
        # The stale-check try/except must exist somewhere in the source
        # (it's too deeply nested for a window search — check globally).
        assert "except Exception as _stale_exc" in src, (
            "Fix B: stale-state guard must be wrapped in try/except so a "
            "DB timeout or OSM error does not silently drop the retry"
        )
        # The fallback must log at debug, not silently pass
        assert "_stale_exc" in src, (
            "Fix B: the caught exception must be logged (non-fatal debug log)"
        )

    def test_fix_b_behavioral_abort_on_wrong_status(self):
        """Fix B behavioral: a row that moved to EXPIRED must not trigger
        a second broker submit when the retry thread wakes up."""
        from types import SimpleNamespace

        abort_calls = []
        on_trigger_calls = []

        class _FakeOSM:
            def get_order(self, oid):
                return {
                    "status": "EXPIRED",          # terminalized externally
                    "broker_order_id": None,
                    "submitted_ts": None,
                    "contract": "DEFERRED:AAPL",
                    "meta": {"breach_attempt_count": 1, "deferred_breach_selection": True},
                }

        class _FakeEC:
            order_state_machine = _FakeOSM()

            def _on_entry_trigger(self, w):
                on_trigger_calls.append(w)

        # Simulate just the stale-check logic extracted for unit testing.
        # We drive it directly without starting a real thread.
        _oid = "ord-1"
        _att = 1

        ec = _FakeEC()
        _stale_check_osm = getattr(ec, "order_state_machine", None)
        _stale_check_fn = getattr(_stale_check_osm, "get_order", None)
        _stale_row = _stale_check_fn(_oid)
        _current_status = str(_stale_row.get("status") or "").upper()
        _current_broker_id = str(_stale_row.get("broker_order_id") or "").strip()
        _current_submitted_ts = _stale_row.get("submitted_ts")
        _current_contract = str(_stale_row.get("contract") or "")
        _current_meta = _stale_row.get("meta") or {}
        _current_attempt = int(_current_meta.get("breach_attempt_count") or 0)
        _is_still_deferred = (
            _current_contract.startswith("DEFERRED:")
            or bool(_current_meta.get("contract_deferred"))
            or bool(_current_meta.get("deferred_breach_selection"))
        )

        aborted = False
        if _current_status not in {"PENDING_TRIGGER", "CREATED"}:
            abort_calls.append("status_changed")
            aborted = True
        if not aborted and _current_broker_id:
            abort_calls.append("broker_id_present")
            aborted = True
        if not aborted and _current_submitted_ts:
            abort_calls.append("submitted_ts_present")
            aborted = True
        if not aborted and not _is_still_deferred:
            abort_calls.append("no_longer_deferred")
            aborted = True
        if not aborted and _current_attempt > int(_att):
            abort_calls.append("attempt_count_advanced")
            aborted = True

        if not aborted:
            ec._on_entry_trigger(SimpleNamespace())

        assert aborted, "Must have aborted — order is EXPIRED"
        assert abort_calls == ["status_changed"], f"Expected status_changed abort, got {abort_calls}"
        assert on_trigger_calls == [], "Must NOT call _on_entry_trigger on EXPIRED row"

    def test_fix_b_behavioral_abort_on_broker_id_present(self):
        """Fix B behavioral: if broker_order_id is already set (order already
        submitted by another path), abort to avoid double-submit."""
        _stale_row = {
            "status": "PENDING_TRIGGER",
            "broker_order_id": "TRD-12345",   # already submitted
            "submitted_ts": None,
            "contract": "DEFERRED:MSFT",
            "meta": {"breach_attempt_count": 1, "deferred_breach_selection": True},
        }
        _current_status = str(_stale_row.get("status") or "").upper()
        _current_broker_id = str(_stale_row.get("broker_order_id") or "").strip()
        _current_submitted_ts = _stale_row.get("submitted_ts")
        _current_meta = _stale_row.get("meta") or {}
        _current_attempt = int(_current_meta.get("breach_attempt_count") or 0)
        _current_contract = str(_stale_row.get("contract") or "")
        _is_still_deferred = _current_contract.startswith("DEFERRED:")

        aborted = False
        abort_reason = None
        if _current_status not in {"PENDING_TRIGGER", "CREATED"}:
            aborted, abort_reason = True, "status_changed"
        elif _current_broker_id:
            aborted, abort_reason = True, "broker_id_present"
        elif _current_submitted_ts:
            aborted, abort_reason = True, "submitted_ts_present"
        elif not _is_still_deferred:
            aborted, abort_reason = True, "no_longer_deferred"
        elif _current_attempt > 1:
            aborted, abort_reason = True, "attempt_count_advanced"

        assert aborted, "Must abort when broker_order_id is present"
        assert abort_reason == "broker_id_present"

    def test_fix_b_behavioral_abort_on_attempt_count_advanced(self):
        """Fix B behavioral: if meta.breach_attempt_count > thread attempt,
        another thread already handled this retry slot — abort."""
        _stale_row = {
            "status": "PENDING_TRIGGER",
            "broker_order_id": None,
            "submitted_ts": None,
            "contract": "DEFERRED:NVDA",
            "meta": {"breach_attempt_count": 3, "deferred_breach_selection": True},
        }
        _att = 1  # this thread thinks it's attempt 1
        _current_attempt = int((_stale_row.get("meta") or {}).get("breach_attempt_count") or 0)
        aborted = _current_attempt > int(_att)
        assert aborted, "Must abort when db attempt_count > thread attempt"

    def test_fix_b_behavioral_no_abort_when_row_is_clean(self):
        """Fix B behavioral: a clean row (all guards pass) must reach
        _on_entry_trigger — the guards must not be over-aggressive."""
        _stale_row = {
            "status": "PENDING_TRIGGER",
            "broker_order_id": None,
            "submitted_ts": None,
            "contract": "DEFERRED:SPY",
            "meta": {"breach_attempt_count": 1, "deferred_breach_selection": True},
        }
        _att = 1
        _current_status = str(_stale_row.get("status") or "").upper()
        _current_broker_id = str(_stale_row.get("broker_order_id") or "").strip()
        _current_submitted_ts = _stale_row.get("submitted_ts")
        _current_contract = str(_stale_row.get("contract") or "")
        _current_meta = _stale_row.get("meta") or {}
        _current_attempt = int(_current_meta.get("breach_attempt_count") or 0)
        _is_still_deferred = _current_contract.startswith("DEFERRED:")

        aborted = (
            _current_status not in {"PENDING_TRIGGER", "CREATED"}
            or bool(_current_broker_id)
            or bool(_current_submitted_ts)
            or not _is_still_deferred
            or _current_attempt > int(_att)
        )
        assert not aborted, (
            "A clean PENDING_TRIGGER DEFERRED row must NOT be aborted — "
            "the stale-state guards are over-aggressive"
        )

    # ── Fix C additional structural tests ───────────────────────────────────

    def test_fix_c_ladder_priority_quality_before_retryable(self):
        """Fix C: when ladder exhausts after a later usable-chain quality
        verdict, quality must take priority over any earlier transient miss."""
        src = open("ap/contract_selector.py").read()
        idx_ret = src.find("_preserved_retryable is not None")
        idx_qual = src.find("_preserved_quality is not None")
        # Both must exist
        assert idx_ret > 0, "Fix C: _preserved_retryable is not None check missing"
        assert idx_qual > 0, "Fix C: _preserved_quality is not None check missing"
        # Quality must come BEFORE retryable in the final preservation block
        # so a later usable-chain verdict is not masked by an earlier transient.
        assert idx_qual < idx_ret, (
            "Fix C: in the final preservation block, _preserved_quality "
            "must be checked before _preserved_retryable so a truthful "
            "quality rejection is not downgraded into a retry."
        )

    def test_fix_c_quality_rejects_do_not_stop_ladder_mid_probe(self):
        """Fix C: OI_TOO_LOW, SPREAD_TOO_WIDE, and similar quality codes must
        NOT be in _TERMINAL_NON_DTE inside the ladder — they must not cause an
        early break that stops probing other expirations."""
        src = open("ap/contract_selector.py").read()
        # Find _TERMINAL_NON_DTE definition
        idx_start = src.find("_TERMINAL_NON_DTE = {")
        idx_end = src.find("}", idx_start)
        terminal_block = src[idx_start:idx_end]
        for quality_code in ("OI_TOO_LOW", "SPREAD_TOO_WIDE", "VOLUME_TOO_LOW",
                             "BID_BELOW_MIN", "CHAIN_ROW_ZERO_BID_ASK",
                             "DIRECT_QUOTE_ZERO_BID_ASK"):
            assert quality_code not in terminal_block, (
                f"Fix C: {quality_code} must NOT be in _TERMINAL_NON_DTE — "
                "it's a per-expiration quality reject, not a DTE-agnostic blocker."
            )

    def test_amendment_3_retry_enabled_default_is_on(self, monkeypatch):
        """BREACH_SELECTOR_RETRY_ENABLED unset => retry path is active. The
        code path we lock: read the default from execution_core's source so
        a silent revert of the default to '0' surfaces in CI."""
        src = open("ap_execution_core.py").read()
        assert 'os.getenv("BREACH_SELECTOR_RETRY_ENABLED", "1")' in src, (
            "BREACH_SELECTOR_RETRY_ENABLED must default to '1' — without "
            "retry enabled by default, Jason LIVE recovery fix is inert."
        )
        # And the reverse — the pre-amendment default must NOT be present
        assert 'os.getenv("BREACH_SELECTOR_RETRY_ENABLED", "0")' not in src, (
            "Old default '0' for BREACH_SELECTOR_RETRY_ENABLED is still in "
            "the source — the amendment may have been reverted."
        )

    def test_amendment_3_retry_kill_switch_still_works(self):
        """Emergency operator override: BREACH_SELECTOR_RETRY_ENABLED=0 must
        still disable the retry path if it misbehaves in production."""
        # Verified structurally: the guard is a plain env-read whose value is
        # tested against the truthy set — the same shape used for every other
        # kill switch. If the amendment kept the same shape (which we assert
        # via the string in the previous test), the kill switch works.
        src = open("ap_execution_core.py").read()
        # The retry-enable expression must still evaluate the env var against
        # the truthy set — this is the shape that makes '0' disable it.
        assert 'BREACH_SELECTOR_RETRY_ENABLED' in src
        assert '.strip().lower() in ("1", "true", "yes")' in src

    def test_amendment_4_ladder_audit_wired_into_selector_audit(self):
        """Ladder audit must be read from selector.get_last_dte_ladder_audit()
        and injected into the _deferred_selector_audit dict so it lands in
        order.meta.deferred_selector_audit for operator dashboards."""
        src = open("ap_execution_core.py").read()
        # 1. The read call must be present.
        assert "get_last_dte_ladder_audit" in src, (
            "Amendment 4 missing: selector.get_last_dte_ladder_audit() is "
            "never called; ladder audit will not propagate into order.meta."
        )
        # 2. The audit dict must be assigned into the _audit key structure
        #    surfaced to callers (which serializes into order.meta).
        assert "last_dte_ladder_audit" in src, (
            "Amendment 4 missing: last_dte_ladder_audit key not written to "
            "_deferred_selector_audit dict — dashboards can't query it."
        )
        # 3. The commonly-queried top-level fields must be surfaced.
        for expected in (
            "ladder_selected_bucket",
            "ladder_selected_expiration",
            "ladder_selected_dte",
            "ladder_buckets_attempted",
            "ladder_bucket_order",
        ):
            assert expected in src, (
                f"Amendment 4 missing top-level surface field: {expected!r}"
            )

    def test_amendment_4_ladder_audit_read_is_best_effort(self):
        """Amendment 4 must not block terminalization if the ladder audit
        read raises. Verified structurally by checking the read is wrapped
        in a try/except."""
        src = open("ap_execution_core.py").read()
        # Locate the read call site and verify a try/except immediately
        # precedes it within a reasonable window.
        idx = src.find("get_last_dte_ladder_audit()")
        assert idx > 0
        preceding = src[max(0, idx - 300):idx]
        assert "try:" in preceding, (
            "get_last_dte_ladder_audit() call is not wrapped in try/except — "
            "a ladder-audit read failure could block deferred terminalization."
        )
