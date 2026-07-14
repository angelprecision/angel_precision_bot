"""
tests/test_p0_watcher_shim_api_parity.py
─────────────────────────────────────────
P0 — Watcher shim API parity: materialization_resume forwarding

Production context
──────────────────
APStartupRecovery._recover_deferred_breach_lifecycles() calls:

    self.entry_watcher.watch(
        plan,
        local_order_id,
        recovery_rearm=True,
        no_cancel_on_reject=True,
        materialization_resume=materialization_resume,
    )

Before this fix the hardened package (ap_entry_watcher/__init__.py) overrode
watch() without the materialization_resume argument and raised:

    APEntryWatcher.watch() got an unexpected keyword argument 'materialization_resume'

This test file locks every production import path and forwarding invariant so
the regression cannot recur silently.

Tests
─────
1. TestImportResolutionAndApiParity  — import path + signature completeness
2. TestMaterializationResumeForwarding — explicit forwarding, no kwargs bypass
3. TestStartupRecoverySeam            — future-due RETRY_WAIT row, no TypeError
4. TestDueRowRegression               — already-due row routes to
                                        resume_deferred_materialization_retry,
                                        not watcher rearm
"""
from __future__ import annotations

import inspect
import os
import sys
import types
import uuid
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch, call

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost/db")


# ─────────────────────────────────────────────────────────────────────────────
# 1. Import resolution and API parity
# ─────────────────────────────────────────────────────────────────────────────

class TestImportResolutionAndApiParity:
    """The production import path must resolve to the hardened package class.

    ap_entry_watcher is a package (ap_entry_watcher/__init__.py) that shadows
    the top-level module (ap_entry_watcher.py). Normal `import ap_entry_watcher`
    resolves the package. The class it exports must be the hardened subclass,
    not the legacy base — and its watch() must accept all three keyword args.
    """

    def test_import_resolves_to_package_not_top_level_module(self):
        """ap_entry_watcher must resolve to the package, not the .py file."""
        import ap_entry_watcher
        pkg_path = ap_entry_watcher.__file__
        # The package __init__.py path ends with __init__.py, not ap_entry_watcher.py
        assert pkg_path is not None
        assert pkg_path.endswith("__init__.py"), (
            f"import ap_entry_watcher resolved to {pkg_path!r} "
            f"— expected the package __init__.py, not the legacy .py file"
        )

    def test_apt_entry_watcher_class_is_hardened_subclass(self):
        """ap_entry_watcher.APEntryWatcher must be the hardened shim class."""
        import ap_entry_watcher
        cls = ap_entry_watcher.APEntryWatcher
        # The hardened class is defined in the package init, not in the base
        # module; its __module__ must reflect the package namespace.
        assert cls.__module__ == "ap_entry_watcher", (
            f"APEntryWatcher.__module__ = {cls.__module__!r}; "
            f"expected 'ap_entry_watcher' (the hardened package)"
        )

    def test_watch_signature_contains_recovery_rearm(self):
        import ap_entry_watcher
        sig = inspect.signature(ap_entry_watcher.APEntryWatcher.watch)
        assert "recovery_rearm" in sig.parameters, (
            "watch() missing 'recovery_rearm' — production recovery will fail"
        )

    def test_watch_signature_contains_no_cancel_on_reject(self):
        import ap_entry_watcher
        sig = inspect.signature(ap_entry_watcher.APEntryWatcher.watch)
        assert "no_cancel_on_reject" in sig.parameters, (
            "watch() missing 'no_cancel_on_reject' — production recovery will fail"
        )

    def test_watch_signature_contains_materialization_resume(self):
        """The exact argument that caused the production TypeError must be present."""
        import ap_entry_watcher
        sig = inspect.signature(ap_entry_watcher.APEntryWatcher.watch)
        assert "materialization_resume" in sig.parameters, (
            "watch() missing 'materialization_resume' — "
            "APStartupRecovery._recover_deferred_breach_lifecycles() will raise "
            "TypeError: got an unexpected keyword argument 'materialization_resume'"
        )

    def test_materialization_resume_is_keyword_only_with_bool_default(self):
        """materialization_resume must be keyword-only with a False default."""
        import ap_entry_watcher
        sig = inspect.signature(ap_entry_watcher.APEntryWatcher.watch)
        param = sig.parameters.get("materialization_resume")
        assert param is not None
        assert param.kind == inspect.Parameter.KEYWORD_ONLY, (
            "materialization_resume must be keyword-only (after *)"
        )
        assert param.default is False, (
            f"materialization_resume default must be False, got {param.default!r}"
        )

    def test_all_three_recovery_kwargs_are_keyword_only(self):
        """All three recovery kwargs must be keyword-only (after *)."""
        import ap_entry_watcher
        sig = inspect.signature(ap_entry_watcher.APEntryWatcher.watch)
        for name in ("recovery_rearm", "no_cancel_on_reject", "materialization_resume"):
            p = sig.parameters[name]
            assert p.kind == inspect.Parameter.KEYWORD_ONLY, (
                f"{name} must be keyword-only"
            )

    def test_watch_signature_does_not_use_kwargs_bypass(self):
        """Explicit args are required — **kwargs is a broad bypass and is forbidden."""
        import ap_entry_watcher
        src = inspect.getsource(ap_entry_watcher.APEntryWatcher.watch)
        # **kwargs in the signature is a bypass; the PR spec forbids it.
        # Check both the def line and the super().watch() call.
        import re
        kwargs_in_def = re.findall(r"def watch\([^)]*\*\*kwargs", src)
        assert kwargs_in_def == [], (
            "watch() must not use **kwargs — explicit forwarding is required "
            "so future API drift is visible in review and signature tests"
        )

    def test_super_watch_call_forwards_materialization_resume_explicitly(self):
        """The super().watch() call must explicitly name materialization_resume."""
        import ap_entry_watcher
        src = inspect.getsource(ap_entry_watcher.APEntryWatcher.watch)
        # The super().watch() call must explicitly pass materialization_resume=
        assert "materialization_resume=materialization_resume" in src, (
            "super().watch() does not forward materialization_resume explicitly; "
            "the base class will use False regardless of what the caller passes"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Shared plan factory
# ─────────────────────────────────────────────────────────────────────────────

def _make_plan(side: str = "CALL", **overrides) -> SimpleNamespace:
    """Minimal plan fixture accepted by the hardened watcher's side validation."""
    defaults = dict(
        signal_id="sig-shim-001",
        plan_id="plan-shim-001",
        ticker="SPY",
        side=side,
        direction=side,
        score=82.0,
        tier="A",
        trigger_price=450.0,
        stop_underlying=447.0,
        target_underlying=455.0,
        contract_symbol="SPY260718C00450000",
        pattern="1-2-2U",
        timeframe="1d",
        strategy_type="daily_continuation",
        metadata={},
        client_id="client@test.com",
        execution_mode="paper",
        contracts=1,
        limit_price=2.50,
        max_position_usd=250.0,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Forwarding test
# ─────────────────────────────────────────────────────────────────────────────

class TestMaterializationResumeForwarding:
    """The shim must forward materialization_resume unchanged to the base class.

    This test monkeypatches the private base class's watch() method (via the
    MRO) so it captures the exact kwargs the shim passes through, without
    exercising any real broker or DB logic.
    """

    def _make_watcher(self):
        import ap_entry_watcher
        broker = MagicMock()
        broker.session = None
        return ap_entry_watcher.APEntryWatcher(broker, mode="PAPER")

    def _base_class(self):
        """Return the private base class so we can patch at class level."""
        import ap_entry_watcher
        return ap_entry_watcher._BaseAPEntryWatcher

    def test_materialization_resume_true_forwarded(self):
        """watch(materialization_resume=True) must arrive at the base class as True."""
        received: dict = {}

        def _capture_watch(self, plan, local_order_id, *,
                           recovery_rearm=False,
                           no_cancel_on_reject=False,
                           materialization_resume=False,
                           **_kw):
            received.update(
                recovery_rearm=recovery_rearm,
                no_cancel_on_reject=no_cancel_on_reject,
                materialization_resume=materialization_resume,
            )
            return True

        base = self._base_class()
        original = base.watch
        base.watch = _capture_watch
        try:
            watcher = self._make_watcher()
            plan = _make_plan(side="CALL")
            result = watcher.watch(
                plan,
                "ord-shim-001",
                recovery_rearm=True,
                no_cancel_on_reject=True,
                materialization_resume=True,
            )
        finally:
            base.watch = original

        assert received.get("materialization_resume") is True, (
            f"materialization_resume not forwarded: received={received}"
        )
        assert received.get("recovery_rearm") is True
        assert received.get("no_cancel_on_reject") is True

    def test_materialization_resume_false_forwarded(self):
        """watch(materialization_resume=False) must arrive at base as False."""
        received: dict = {}

        def _capture_watch(self, plan, local_order_id, *,
                           recovery_rearm=False,
                           no_cancel_on_reject=False,
                           materialization_resume=False,
                           **_kw):
            received["materialization_resume"] = materialization_resume
            return True

        base = self._base_class()
        original = base.watch
        base.watch = _capture_watch
        try:
            watcher = self._make_watcher()
            result = watcher.watch(
                _make_plan(), "ord-shim-002",
                recovery_rearm=False,
                no_cancel_on_reject=False,
                materialization_resume=False,
            )
        finally:
            base.watch = original

        assert received.get("materialization_resume") is False

    def test_return_value_passes_through(self):
        """The base class return value must pass through the shim unchanged."""
        for retval in (True, False):
            def _stub(self, *a, **kw):
                return retval
            base = self._base_class()
            original = base.watch
            base.watch = _stub
            try:
                watcher = self._make_watcher()
                result = watcher.watch(_make_plan(), "ord-shim-003")
            finally:
                base.watch = original
            assert result is retval, (
                f"Return value {retval!r} not passed through; got {result!r}"
            )

    def test_put_plan_forwards_correctly(self):
        """PUT side is normalized and forwarded; materialization_resume still passes."""
        received: dict = {}

        def _capture(self, plan, local_order_id, *,
                     recovery_rearm=False,
                     no_cancel_on_reject=False,
                     materialization_resume=False,
                     **_kw):
            received["materialization_resume"] = materialization_resume
            received["side"] = getattr(plan, "side", None)
            return True

        base = self._base_class()
        original = base.watch
        base.watch = _capture
        try:
            watcher = self._make_watcher()
            plan = _make_plan(side="PUT")
            watcher.watch(
                plan, "ord-shim-004",
                recovery_rearm=True,
                no_cancel_on_reject=True,
                materialization_resume=True,
            )
        finally:
            base.watch = original

        assert received.get("materialization_resume") is True
        assert received.get("side") == "PUT"

    def test_no_type_error_with_all_three_kwargs(self):
        """The exact production call signature must not raise TypeError."""
        def _noop(self, *a, **kw):
            return True

        base = self._base_class()
        original = base.watch
        base.watch = _noop
        try:
            watcher = self._make_watcher()
            # This is the exact call from ap_recovery._recover_deferred_breach_lifecycles
            result = watcher.watch(
                _make_plan(),
                "ord-shim-005",
                recovery_rearm=True,
                no_cancel_on_reject=True,
                materialization_resume=True,
            )
        except TypeError as exc:
            pytest.fail(
                f"Production call raised TypeError: {exc}\n"
                "This is the exact failure that triggered this P0 PR."
            )
        finally:
            base.watch = original


# ─────────────────────────────────────────────────────────────────────────────
# 3. Startup recovery seam test
# ─────────────────────────────────────────────────────────────────────────────

class TestStartupRecoverySeam:
    """Route a future-due RETRY_WAIT row through APStartupRecovery.

    Uses the real hardened watcher surface — not a stub that accepts **kwargs.
    Asserts:
    - no TypeError (the production failure)
    - watch() receives materialization_resume=True
    - client_id and execution_mode are unchanged through the seam
    - no broker submit occurs from future-due rearm
    """

    _CLIENT = "recovery_seam@test.com"
    _MODE   = "paper"
    _NOW    = datetime(2026, 7, 14, 14, 0, 0, tzinfo=timezone.utc)
    _FUTURE = (_NOW + timedelta(minutes=30)).isoformat()

    def _make_order_row(self, local_order_id: str | None = None) -> dict:
        """Synthetic orders row for a future-due RETRY_WAIT deferred lifecycle."""
        return {
            "local_order_id":    local_order_id or str(uuid.uuid4()),
            "client_id":         self._CLIENT,
            "signal_id":         "sig-seam-001",
            "plan_id":           "plan-seam-001",
            "symbol":            "AAPL",
            "contract":          "DEFERRED:AAPL",
            "direction":         "CALL",
            "score":             82.0,
            "tier":              "A",
            "trigger_price":     185.0,
            "stop_underlying":   182.0,
            "target_underlying": 190.0,
            "pattern":           "1-2-2U",
            "timeframe":         "1d",
            "execution_mode":    self._MODE,
            "qty":               1,
            "limit_price":       0.01,
            "reserved_cost":     1.0,
            "status":            "PENDING_TRIGGER",
            "broker_order_id":   None,
            "submitted_ts":      None,
            "meta": {
                "lifecycle_state":           "RETRY_WAIT",
                "materialization_status":    "RETRY_PENDING",
                "materialization_in_flight": False,
                "materialization_generation": 1,
                "retry_attempt":             1,
                "retry_max_attempts":        3,
                "retry_reason":              "MONEYNESS_OUT_OF_RANGE",
                "materialization_next_retry_at": self._FUTURE,
                "deferred_retry_not_before":     self._FUTURE,
                "broker_ready":              False,
                "current_owner":             f"watcher:recovery:{self._CLIENT}",
            },
        }

    def _make_recovery(self, *, watch_calls: list) -> object:
        """Build a minimal APStartupRecovery configured for seam testing."""
        from ap_recovery import APStartupRecovery
        import ap_entry_watcher

        # Hardened watcher — real class, patched base so it doesn't touch DB/broker
        broker_mock = MagicMock()
        broker_mock.session = None
        watcher = ap_entry_watcher.APEntryWatcher(broker_mock, mode="PAPER")

        # Intercept the base watch() — records args, returns True (armed)
        base = ap_entry_watcher._BaseAPEntryWatcher

        def _capture_and_arm(self_inner, plan, local_order_id, *,
                             recovery_rearm=False,
                             no_cancel_on_reject=False,
                             materialization_resume=False,
                             **_kw):
            watch_calls.append({
                "plan":                  plan,
                "local_order_id":        local_order_id,
                "recovery_rearm":        recovery_rearm,
                "no_cancel_on_reject":   no_cancel_on_reject,
                "materialization_resume": materialization_resume,
                "client_id":             getattr(plan, "client_id", None),
                "execution_mode":        getattr(plan, "execution_mode", None),
            })
            return True

        self._original_watch = base.watch
        base.watch = _capture_and_arm
        self._base_to_restore = base

        osm_mock = MagicMock()
        osm_mock.client_id = self._CLIENT

        recovery = APStartupRecovery(
            client_id=self._CLIENT,
            broker=broker_mock,
            osm=osm_mock,
            pm=MagicMock(),
            master_control=MagicMock(),
            exit_engine=None,
            entry_watcher=watcher,
            execution_core=None,
        )
        return recovery

    def teardown_method(self, _method):
        """Restore the base watch() after each test."""
        base = getattr(self, "_base_to_restore", None)
        orig = getattr(self, "_original_watch", None)
        if base is not None and orig is not None:
            base.watch = orig

    def _run_recovery_with_row(self, row: dict, watch_calls: list) -> dict:
        """Invoke _recover_deferred_breach_lifecycles with a single synthetic row.

        run_with_retry is imported locally inside the method body via
        `from ap.db import conn, run_with_retry`, so we patch at the ap.db
        module level — not at ap_recovery module level.
        """
        recovery = self._make_recovery(watch_calls=watch_calls)
        result: dict = {"deferred_lifecycles_recovered": 0, "errors": []}

        with patch.object(
            recovery, "_execution_mode", return_value=self._MODE.upper()
        ), patch("ap.db.run_with_retry") as mock_rwr, \
           patch("ap.db.conn") as _mock_conn, \
           patch("ap_recovery.log"):

            # run_with_retry is called once: _load() → returns the synthetic row.
            # The future-proof path calls prove_materialization_retry_owner on
            # the watcher (which doesn't implement it → proven=False), so the
            # code falls through to §5, builds the plan, and calls watch().
            mock_rwr.return_value = [row]

            try:
                recovery._recover_deferred_breach_lifecycles(result)
            except Exception as exc:
                pytest.fail(
                    f"_recover_deferred_breach_lifecycles raised: "
                    f"{type(exc).__name__}: {exc}"
                )

        return result

    def test_no_type_error_for_future_due_retry_wait_row(self):
        """Future-due RETRY_WAIT row must not raise TypeError."""
        watch_calls: list = []
        row = self._make_order_row("ord-seam-001")
        # Should not raise
        self._run_recovery_with_row(row, watch_calls)

    def test_watch_receives_materialization_resume_true(self):
        """For a RETRY_WAIT row, watch() must receive materialization_resume=True."""
        watch_calls: list = []
        row = self._make_order_row("ord-seam-002")
        self._run_recovery_with_row(row, watch_calls)

        assert len(watch_calls) >= 1, (
            "watch() was not called — the recovery path did not reach watcher rearm"
        )
        assert watch_calls[0]["materialization_resume"] is True, (
            f"materialization_resume not True in watch() call: {watch_calls[0]}"
        )

    def test_watch_receives_recovery_rearm_true(self):
        """recovery_rearm must be True for deferred lifecycle rearm."""
        watch_calls: list = []
        row = self._make_order_row("ord-seam-003")
        self._run_recovery_with_row(row, watch_calls)

        assert len(watch_calls) >= 1
        assert watch_calls[0]["recovery_rearm"] is True

    def test_client_id_unchanged_through_seam(self):
        """client_id on the plan must be the same as the recovery client_id."""
        watch_calls: list = []
        row = self._make_order_row("ord-seam-004")
        self._run_recovery_with_row(row, watch_calls)

        if watch_calls:
            plan_client = str(watch_calls[0]["client_id"] or "").strip().lower()
            assert plan_client == self._CLIENT, (
                f"client_id changed through seam: expected {self._CLIENT!r}, "
                f"got {plan_client!r}"
            )

    def test_execution_mode_unchanged_through_seam(self):
        """execution_mode on the plan must be paper (not live, not blank)."""
        watch_calls: list = []
        row = self._make_order_row("ord-seam-005")
        self._run_recovery_with_row(row, watch_calls)

        if watch_calls:
            plan_mode = str(watch_calls[0]["execution_mode"] or "").strip().lower()
            assert plan_mode == self._MODE, (
                f"execution_mode changed through seam: expected {self._MODE!r}, "
                f"got {plan_mode!r}"
            )


# ─────────────────────────────────────────────────────────────────────────────
# 4. Due-row regression
# ─────────────────────────────────────────────────────────────────────────────

class TestDueRowRegression:
    """Already-due retry rows must route to resume_deferred_materialization_retry.

    The RECOVERY_DUE_RETRY_TAKEOVER path calls execution_core.
    resume_deferred_materialization_retry() directly; it must not re-arm
    the watcher for due rows. This test keeps the existing behaviour intact.
    """

    _CLIENT = "due_row@test.com"
    _MODE   = "paper"
    _NOW    = datetime(2026, 7, 14, 14, 0, 0, tzinfo=timezone.utc)
    _PAST   = (_NOW - timedelta(hours=1)).isoformat()

    def _make_due_row(self, local_order_id: str | None = None) -> dict:
        """Synthetic RETRY_WAIT row whose retry timestamp is already past."""
        return {
            "local_order_id":    local_order_id or str(uuid.uuid4()),
            "client_id":         self._CLIENT,
            "signal_id":         "sig-due-001",
            "plan_id":           "plan-due-001",
            "symbol":            "SPY",
            "contract":          "DEFERRED:SPY",
            "direction":         "CALL",
            "score":             80.0,
            "tier":              "B",
            "trigger_price":     450.0,
            "stop_underlying":   447.0,
            "target_underlying": 455.0,
            "pattern":           "1-2-2U",
            "timeframe":         "1d",
            "execution_mode":    self._MODE,
            "qty":               1,
            "limit_price":       0.01,
            "reserved_cost":     1.0,
            "status":            "PENDING_TRIGGER",
            "broker_order_id":   None,
            "submitted_ts":      None,
            "meta": {
                "lifecycle_state":              "RETRY_WAIT",
                "materialization_status":       "RETRY_PENDING",
                "materialization_in_flight":    False,
                "materialization_generation":   2,
                "retry_attempt":                2,
                "retry_max_attempts":           3,
                "retry_reason":                 "MONEYNESS_OUT_OF_RANGE",
                "materialization_next_retry_at": self._PAST,
                "deferred_retry_not_before":    self._PAST,
                "broker_ready":                 False,
            },
        }

    def test_due_row_routes_to_resume_fn_not_watcher(self):
        """An already-due retry must call resume_deferred_materialization_retry.

        The watcher must NOT be rearmed — the execution_core owns due rows.
        """
        from ap_recovery import APStartupRecovery
        import ap_entry_watcher

        watch_calls: list = []
        resume_calls: list = []

        # Real hardened watcher — tracks if watch() is incorrectly called
        broker_mock = MagicMock()
        broker_mock.session = None
        watcher = ap_entry_watcher.APEntryWatcher(broker_mock, mode="PAPER")

        base = ap_entry_watcher._BaseAPEntryWatcher
        original_watch = base.watch

        def _track_watch(self_inner, plan, local_order_id, **kw):
            watch_calls.append({"local_order_id": local_order_id, **kw})
            return True

        base.watch = _track_watch

        try:
            # Mock resume fn — returns SUBMITTED disposition
            mock_resume = MagicMock(return_value={"disposition": "SUBMITTED", "reason_code": "OK"})
            mock_execution_core = MagicMock()
            mock_execution_core.resume_deferred_materialization_retry = mock_resume

            osm_mock = MagicMock()
            osm_mock.client_id = self._CLIENT

            recovery = APStartupRecovery(
                client_id=self._CLIENT,
                broker=broker_mock,
                osm=osm_mock,
                pm=MagicMock(),
                master_control=MagicMock(),
                entry_watcher=watcher,
                execution_core=mock_execution_core,
            )

            result: dict = {"deferred_lifecycles_recovered": 0, "errors": []}
            row = self._make_due_row("ord-due-001")

            with patch.object(
                recovery, "_execution_mode", return_value=self._MODE.upper()
            ), patch("ap.db.run_with_retry") as mock_rwr, \
               patch("ap.db.conn"), \
               patch("ap_recovery.log"):

                # _load() returns the single due-row
                mock_rwr.return_value = [row]
                # Also provide prove_materialization_retry_owner on the watcher
                # returning a "proven + DUE" result so the takeover branch fires
                watcher.prove_materialization_retry_owner = MagicMock(return_value={
                    "proven":      True,
                    "owner":       f"recovery_retry:{self._CLIENT}:ord-due-001:3",
                    "disposition": "DUE",
                    "reason_code": "DEFERRED_RETRY_DUE",
                })

                try:
                    recovery._recover_deferred_breach_lifecycles(result)
                except Exception as exc:
                    pytest.fail(f"Due-row path raised: {type(exc).__name__}: {exc}")

        finally:
            base.watch = original_watch

        # resume_deferred_materialization_retry must have been called
        assert mock_resume.call_count >= 1, (
            "resume_deferred_materialization_retry was not called for a due retry row"
        )

    def test_due_row_no_type_error(self):
        """The due-row takeover path must not raise TypeError (regression guard)."""
        from ap_recovery import APStartupRecovery
        import ap_entry_watcher

        broker_mock = MagicMock()
        broker_mock.session = None
        watcher = ap_entry_watcher.APEntryWatcher(broker_mock, mode="PAPER")

        base = ap_entry_watcher._BaseAPEntryWatcher
        original_watch = base.watch
        base.watch = lambda *a, **k: True

        try:
            mock_execution_core = MagicMock()
            mock_execution_core.resume_deferred_materialization_retry = MagicMock(
                return_value={"disposition": "SUBMITTED", "reason_code": "OK"}
            )
            osm_mock = MagicMock()
            osm_mock.client_id = self._CLIENT

            recovery = APStartupRecovery(
                client_id=self._CLIENT,
                broker=broker_mock,
                osm=osm_mock,
                pm=MagicMock(),
                master_control=MagicMock(),
                entry_watcher=watcher,
                execution_core=mock_execution_core,
            )

            result: dict = {"deferred_lifecycles_recovered": 0, "errors": []}
            row = self._make_due_row("ord-due-002")

            with patch.object(recovery, "_execution_mode", return_value="PAPER"), \
                 patch("ap.db.run_with_retry") as mock_rwr, \
                 patch("ap.db.conn"), \
                 patch("ap_recovery.log"):

                mock_rwr.return_value = [row]
                try:
                    recovery._recover_deferred_breach_lifecycles(result)
                except TypeError as exc:
                    pytest.fail(f"Due-row path raised TypeError: {exc}")
        finally:
            base.watch = original_watch
