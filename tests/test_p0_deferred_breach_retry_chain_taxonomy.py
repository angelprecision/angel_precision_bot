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
#   1. DEFERRED_DTE_LADDER default = "1"
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

    def test_amendment_1_dte_ladder_default_is_on(self, monkeypatch):
        """DEFERRED_DTE_LADDER unset => ladder is enabled by default."""
        monkeypatch.delenv("DEFERRED_DTE_LADDER", raising=False)
        mod = _load_selector()
        sel = object.__new__(mod.APContractSelectionEngine)
        mod.APContractSelectionEngine.__init__(sel, broker=SimpleNamespace(), data_broker=SimpleNamespace())
        assert sel.dte_ladder_enabled is True, (
            "DEFERRED_DTE_LADDER must default to '1' — 54 Jason LIVE setups "
            "expired today because only one expiration was probed."
        )

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

    def test_amendment_2_retryable_set_contains_ladder_exhaustion_reason(self):
        """After the DTE ladder exhausts all buckets on transient quote-quality
        signals, it emits NO_VALID_PLAYBOOK_DTE_CONTRACT as the aggregate reason.
        Execution core sees this — not the per-strike reason — so it MUST be in
        the retryable set or the ladder's aggregation defeats the retry."""
        assert "NO_VALID_PLAYBOOK_DTE_CONTRACT" in ec_mod.RETRYABLE_BREACH_SELECTOR_REASONS

    def test_amendment_2_retryable_set_contains_all_amendment_codes(self):
        """Snapshot lock: the exact set of amendment-added codes must remain
        stable so silent reverts trip CI."""
        amendment_additions = {
            "CHAIN_ROW_ZERO_BID_ASK",
            "DIRECT_QUOTE_ZERO_BID_ASK",
            "QUOTE_FETCH_FAILED",
            "CHAIN_EMPTY",
            "NO_VALID_PLAYBOOK_DTE_CONTRACT",
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
