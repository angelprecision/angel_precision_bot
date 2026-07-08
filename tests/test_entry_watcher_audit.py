"""
PR C — Pre-LIVE Entry Watcher Audit Bug Sweep
==============================================

Tests for the 10 verified findings in the institutional audit of
ap_entry_watcher.py (audit dated 2026-05-25). Tests-first throughout;
each fix lands only after its test is verified RED.

Coverage map:

  BUG-EW-5 — mode wiring (CRITICAL)
    - APEntryWatcher.__init__ accepts mode kwarg
    - default is "PAPER"
    - APExecutionCore wires self.entry_watcher.mode = self.mode
      after canonical mode derivation
    - LIVE mode + quote outage on overnight revalidation INVALIDATES
      (was silently arming under PAPER fail-open due to ghost mode attr)
    - PAPER mode preserves fail-open (don't change paper semantics)

  BUG-EW-4 — OSM cleanup when watch() blocked
    - watch() returns False when add_signal blocks
    - When add_signal returns False, watch() calls
      order_state_machine.cancel_pending_entry(local_order_id,...)

  BUG-EW-3 — zero-quote guard
    - bid=0 ask=0 during intraday drift check does NOT expire a PUT
    - bid=0 ask=0 also does not expire a CALL (symmetric)

  BUG-EW-1 — lazy import cleanup
    - _is_pre_market_now uses module-level ET, not lazy imports
      (no `from datetime import datetime` inside the function body)

  BUG-EW-2 — explicit None check on entry_trigger
    - entry=0.0 raises ValueError with a clean message (not a swallow)

  _last_reject_reason inside lock
    - All writes to self._last_reject_reason happen under self._lock

  MAX_INTRADAY_DRIFT_PCT env-tunable
    - Module exposes MAX_INTRADAY_DRIFT_PCT and reads from
      os.getenv("MAX_INTRADAY_DRIFT_PCT", "0.015")

  MAX_OPTION_PREMIUM_DRIFT_PCT module-level
    - Module exposes MAX_OPTION_PREMIUM_DRIFT_PCT as a module-level
      constant
    - watch() does NOT do `float(os.getenv(...))` inline anymore

  Redundant health-registry import
    - _poll_loop body does not contain a second
      `from ap_health_registry import` statement

  PRECEDENCE — same-tick trigger + stop breach
    - When ask >= trigger AND bid <= stop_level on the same check(),
      the resulting state is INVALIDATED (NOT TRIGGERED).
      This is the safer-by-design behavior the user asked to preserve.

  INV — structural test: watcher mode is wired after execution-core init
    - After APExecutionCore construction, execution_core.entry_watcher.mode
      equals execution_core.mode

Run:
    pytest tests/test_entry_watcher_audit.py -v
"""
from __future__ import annotations

import importlib
import inspect
import os
import re
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_entry_watcher_audit",
)
os.environ.setdefault("ENCRYPTION_KEY", "ap-entry-watcher-pr-c-2026")

if "supabase" not in sys.modules:
    _supa_stub = types.ModuleType("supabase")
    _supa_stub.create_client = lambda *a, **kw: None
    _supa_stub.Client = type("Client", (), {})
    sys.modules["supabase"] = _supa_stub


import ap_entry_watcher  # noqa: E402
import ap_execution_core  # noqa: E402
from ap_entry_watcher import APEntryWatcher, WatchedSignal, WatchState  # noqa: E402
from ap_execution_core import APExecutionCore  # noqa: E402


# ──────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────

def _make_signal(**overrides) -> dict:
    sig = {
        "signal_id": "sig-pr-c-1",
        "ticker": "SPY",
        "side": "CALL",
        "score": 80,
        "grade": "A",
        "entry_price": 500.00,
        "stop_price": 498.00,
        "target_price": 505.00,
        "timeframe": "5m",
        "local_order_id": "lo-test-1",
    }
    sig.update(overrides)
    return sig


def _make_mc(mode="paper", max_positions=7):
    mc = MagicMock()
    mc.mode = mode
    mc.max_positions = max_positions
    mc._kill_switch_fn = lambda: False
    return mc


def _make_osm():
    osm = MagicMock()
    # By default the OSM has cancel_pending_entry and accepts validation probes
    osm.cancel_pending_entry.return_value = True
    osm.has_order.return_value = True
    return osm


# ══════════════════════════════════════════════════════════════════
# BUG-EW-5 — mode wiring
# ══════════════════════════════════════════════════════════════════

class TestBugEw5ModeWiring:
    def test_apentrywatcher_init_accepts_mode_kwarg(self):
        sig = inspect.signature(APEntryWatcher.__init__)
        assert "mode" in sig.parameters, (
            "APEntryWatcher.__init__ must accept a `mode` kwarg so "
            "execution_core can pass canonical mode at construct time"
        )

    def test_apentrywatcher_default_mode_is_paper(self):
        broker = MagicMock()
        w = APEntryWatcher(broker, order_state_machine=_make_osm())
        assert getattr(w, "mode", None) == "PAPER", (
            "default mode must be PAPER (back-compat)"
        )

    def test_apentrywatcher_accepts_live_mode(self):
        broker = MagicMock()
        w = APEntryWatcher(broker, order_state_machine=_make_osm(), mode="LIVE")
        assert w.mode == "LIVE"

    def test_apentrywatcher_normalizes_mode_to_upper(self):
        broker = MagicMock()
        w = APEntryWatcher(broker, order_state_machine=_make_osm(), mode="live")
        assert w.mode == "LIVE"

    def test_live_overnight_quote_outage_invalidates(self):
        """LIVE + zero-bid+zero-ask on overnight revalidation -> INVALIDATED."""
        broker = MagicMock()
        # Quote returns zero bid + zero ask (simulated outage)
        broker.session.get.return_value.json.return_value = {
            "quotes": {"quote": {"bid": 0, "ask": 0, "last": 0}}
        }
        broker.session.get.return_value.status_code = 200
        w = APEntryWatcher(broker, order_state_machine=_make_osm(), mode="LIVE")
        # Add an overnight signal
        sig = _make_signal(signal_id="sig-live-overnight", timeframe="1d")
        watched = WatchedSignal(sig, overnight=True)
        watched._watcher_ref = w
        w._pending.append(watched)
        w._dedup_set.add(sig["signal_id"])

        # Force the generic (non-daily) revalidation path by stubbing
        # _safe_is_daily_signal to False, and _get_quote to return zeros.
        with patch.object(w, "_get_quote", return_value={"bid": 0, "ask": 0, "last": 0}):
            with patch("ap_entry_watcher._safe_is_daily_signal", return_value=False):
                w._revalidate_overnight_at_open()

        assert watched.state == WatchState.INVALIDATED, (
            f"LIVE + quote outage must INVALIDATE; got {watched.state}. "
            f"The BUG-EW-5 fix: getattr(self, 'mode', 'PAPER') used to "
            f"silently return PAPER and arm fail-open in LIVE."
        )

    def test_paper_overnight_quote_outage_preserves_fail_open(self):
        """PAPER + zero-bid+zero-ask -> overnight=False (arm fail-open).

        Per direction: do NOT change paper semantics. This documents
        the preserved behavior.
        """
        broker = MagicMock()
        w = APEntryWatcher(broker, order_state_machine=_make_osm(), mode="PAPER")
        sig = _make_signal(signal_id="sig-paper-overnight", timeframe="1d")
        watched = WatchedSignal(sig, overnight=True)
        watched._watcher_ref = w
        w._pending.append(watched)
        w._dedup_set.add(sig["signal_id"])

        with patch.object(w, "_get_quote", return_value={"bid": 0, "ask": 0, "last": 0}):
            with patch("ap_entry_watcher._safe_is_daily_signal", return_value=False):
                w._revalidate_overnight_at_open()

        # Per direction: PAPER fail-open keeps the signal armed
        # (overnight=False = generic intraday watcher takes over).
        assert watched.state == WatchState.PENDING
        assert watched.overnight is False


# ══════════════════════════════════════════════════════════════════
# BUG-EW-4 — OSM cleanup when watch() blocks
# ══════════════════════════════════════════════════════════════════

class TestBugEw4OsmCleanup:
    def test_watch_calls_cancel_pending_entry_when_add_signal_blocks(self):
        """When add_signal returns False (e.g. dedup_block), watch()
        MUST call OSM cancel_pending_entry on the local_order_id so the
        order doesn't sit as a ghost CREATED row.
        """
        broker = MagicMock()
        osm = _make_osm()
        w = APEntryWatcher(broker, order_state_machine=osm, mode="PAPER")

        # Pre-populate the watcher with a signal so the SECOND
        # watch() call triggers a dedup block.
        sig1 = _make_signal(signal_id="sig-dup", local_order_id="lo-1")
        w._pending.append(WatchedSignal(sig1, overnight=False))
        w._dedup_set.add("sig-dup")

        # Build a plan duplicate with a DIFFERENT local_order_id so we
        # can see watch() call cancel on that specific order.
        plan = MagicMock(
            signal_id="sig-dup",  # duplicate -> dedup block
            ticker="SPY",
            side="CALL",
            score=80,
            tier="A",
            trigger_price=500.00,
            stop_underlying=498.00,
            target_underlying=505.00,
            entry_option_price=0,
            contract_symbol="SPY260530C00500000",
            plan_id="plan-dup",
            pattern="test",
            prior_day_high=None,
            prior_day_low=None,
            timeframe="5m",
            strategy_type="",
        )

        ok = w.watch(plan, local_order_id="lo-2-blocked")
        assert ok is False, "watch() must propagate the False from add_signal"
        osm.cancel_pending_entry.assert_called_once()
        call = osm.cancel_pending_entry.call_args
        assert call.args[0] == "lo-2-blocked" or call.kwargs.get("local_order_id") == "lo-2-blocked" or "lo-2-blocked" in str(call), (
            f"OSM cancel must target the blocked order's local_order_id; got {call}"
        )

    def test_watch_does_NOT_cancel_osm_on_success(self):
        """When add_signal returns True, watch() must NOT touch OSM."""
        broker = MagicMock()
        osm = _make_osm()
        w = APEntryWatcher(broker, order_state_machine=osm, mode="PAPER")

        plan = MagicMock(
            signal_id="sig-ok",
            ticker="SPY",
            side="CALL",
            score=80,
            tier="A",
            trigger_price=500.00,
            stop_underlying=498.00,
            target_underlying=505.00,
            entry_option_price=0,
            contract_symbol="SPY260530C00500000",
            plan_id="plan-ok",
            pattern="test",
            prior_day_high=None,
            prior_day_low=None,
            timeframe="5m",
            strategy_type="",
        )
        # quote remains below trigger so the already-through-trigger guard does not fire
        with patch.object(w, "_get_quote", return_value={"bid": 499.90, "ask": 499.98, "last": 499.94}):
            ok = w.watch(plan, local_order_id="lo-ok")

        assert ok is True
        osm.cancel_pending_entry.assert_not_called()


# ══════════════════════════════════════════════════════════════════
# BUG-EW-3 — zero-quote guard
# ══════════════════════════════════════════════════════════════════

class TestBugEw3ZeroQuoteGuard:
    def _aged_signal(self, side="PUT"):
        """Build a WatchedSignal old enough to enter the drift-check branch."""
        sig = _make_signal(side=side, signal_id=f"sig-{side}-aged")
        ws = WatchedSignal(sig, overnight=False)
        # Backdate so minutes_watching >= MAX_INTRADAY_WATCH_MIN
        from datetime import timedelta
        ws.created_at = datetime.now(timezone.utc) - timedelta(
            minutes=ap_entry_watcher.MAX_INTRADAY_WATCH_MIN + 1
        )
        return ws

    def test_put_zero_quote_does_not_expire(self):
        """Zero bid + zero ask must NOT silently expire a PUT signal
        (was: drift = -1.0 < -0.015 silently triggered EXPIRED)."""
        ws = self._aged_signal(side="PUT")
        # entry_trigger = 500.00 (from _make_signal)
        # bid=0, ask=0 -> drift would be -1.0 under old code -> stale=True for PUT
        result = ws.check(bid=0.0, ask=0.0)
        assert ws.state != WatchState.EXPIRED, (
            "PUT signal must NOT silently expire on quote outage; "
            f"got state={ws.state}. This is the BUG-EW-3 silent-loss case."
        )

    def test_call_zero_quote_also_does_not_expire(self):
        """Symmetric check for CALL — defensive."""
        ws = self._aged_signal(side="CALL")
        ws.check(bid=0.0, ask=0.0)
        assert ws.state != WatchState.EXPIRED

    def test_normal_drift_still_expires_put(self):
        """Regression: real drift on a PUT still expires (don't over-fix).

        PUT stale rule: drift < -MAX_INTRADAY_DRIFT_PCT means mid is
        far BELOW the trigger — for a PUT this is the WRONG direction
        (we wanted price to fall to trigger but it overshot). Use a
        PUT with stop=515 (above entry, correct for PUT) and a mid
        far below trigger to land in the stale branch without hitting
        the stop-break short-circuit.
        """
        # Custom signal so stop is far enough above that ask=490.5 < 515*1.001
        sig = _make_signal(side="PUT", entry_price=500.00, stop_price=515.00,
                            signal_id="sig-PUT-drift-only")
        ws = WatchedSignal(sig, overnight=False)
        from datetime import timedelta
        ws.created_at = datetime.now(timezone.utc) - timedelta(
            minutes=ap_entry_watcher.MAX_INTRADAY_WATCH_MIN + 1
        )
        # mid = 489.75 → drift = (489.75 - 500)/500 = -2.05% < -1.5% → STALE.
        # ask=490.5 << stop_level*1.001=515.515, so no stop-break short-circuit.
        # bid=489 << entry_trigger=500, so PUT breach branch enters first,
        # but stale check runs BEFORE breach check and returns EXPIRED.
        result = ws.check(bid=489.00, ask=490.50)
        assert ws.state == WatchState.EXPIRED, (
            "Real wrong-direction drift on a PUT (mid 2% below trigger) "
            f"must still expire after MAX_INTRADAY_WATCH_MIN; got state={ws.state}"
        )


# ══════════════════════════════════════════════════════════════════
# BUG-EW-1 — lazy import cleanup
# ══════════════════════════════════════════════════════════════════

class TestBugEw1LazyImportCleanup:
    def test_is_pre_market_now_source_does_not_have_lazy_imports(self):
        src = inspect.getsource(ap_entry_watcher._is_pre_market_now)
        # No lazy `from datetime import` or `from zoneinfo import` inside.
        assert "from datetime import" not in src, (
            "_is_pre_market_now must use module-level datetime, not lazy import"
        )
        assert "from zoneinfo import" not in src, (
            "_is_pre_market_now must use module-level ZoneInfo (ET), not lazy import"
        )

    def test_is_pre_market_now_references_module_et(self):
        """Body must reference the module-level ET constant."""
        src = inspect.getsource(ap_entry_watcher._is_pre_market_now)
        assert "ET" in src, (
            "_is_pre_market_now must reference the module-level ET constant"
        )

    def test_is_pre_market_now_returns_bool(self):
        result = ap_entry_watcher._is_pre_market_now()
        assert isinstance(result, bool)


# ══════════════════════════════════════════════════════════════════
# BUG-EW-2 — explicit None check on entry_trigger
# ══════════════════════════════════════════════════════════════════

class TestBugEw2EntryTriggerParse:
    def test_zero_entry_raises_value_error_clearly(self):
        """An explicit zero entry must raise ValueError with a clean message."""
        sig = _make_signal(entry_price=0.0)
        with pytest.raises(ValueError) as exc:
            WatchedSignal(sig, overnight=False)
        assert "entry" in str(exc.value).lower() or "trigger" in str(exc.value).lower()

    def test_missing_entry_raises(self):
        sig = _make_signal(entry_price=None)
        # Also clear trigger.entry to make sure
        sig.pop("trigger", None)
        with pytest.raises(ValueError):
            WatchedSignal(sig, overnight=False)


# ══════════════════════════════════════════════════════════════════
# _last_reject_reason inside lock
# ══════════════════════════════════════════════════════════════════

class TestLastRejectReasonLock:
    def test_all_reject_reason_writes_are_inside_lock(self):
        """Every self._last_reject_reason assignment in add_signal()
        must occur inside the `with self._lock:` block.
        """
        src = inspect.getsource(APEntryWatcher.add_signal)
        # Find the `with self._lock:` block boundaries
        lock_match = re.search(r"with\s+self\._lock\s*:", src)
        assert lock_match, "add_signal must have a `with self._lock:` block"
        lock_start = lock_match.end()
        # Locate every self._last_reject_reason = ... line
        for m in re.finditer(r"self\._last_reject_reason\s*=", src):
            assert m.start() >= lock_start, (
                f"self._last_reject_reason assignment at offset {m.start()} "
                f"occurs BEFORE `with self._lock:` (at {lock_start}). "
                f"Move it inside the lock block to avoid concurrent-call race."
            )


# ══════════════════════════════════════════════════════════════════
# Module-level constants
# ══════════════════════════════════════════════════════════════════

class TestModuleConstants:
    def test_max_intraday_drift_pct_is_env_tunable(self):
        """MAX_INTRADAY_DRIFT_PCT must be read from os.getenv at module load."""
        src = Path(REPO_ROOT, "ap_entry_watcher.py").read_text()
        pattern = re.compile(
            r'MAX_INTRADAY_DRIFT_PCT\s*=\s*float\(\s*os\.getenv\(\s*["\']MAX_INTRADAY_DRIFT_PCT["\']'
        )
        assert pattern.search(src), (
            "MAX_INTRADAY_DRIFT_PCT must be env-tunable via os.getenv; "
            "currently hardcoded as 0.015"
        )

    def test_max_intraday_drift_pct_default_value(self):
        """Default must remain 0.015 (1.5%) for back-compat."""
        # Clear env to confirm default
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MAX_INTRADAY_DRIFT_PCT", None)
            importlib.reload(ap_entry_watcher)
            try:
                assert ap_entry_watcher.MAX_INTRADAY_DRIFT_PCT == 0.015
            finally:
                importlib.reload(ap_entry_watcher)

    def test_max_option_premium_drift_pct_is_module_level(self):
        """MAX_OPTION_PREMIUM_DRIFT_PCT must be a module-level constant."""
        assert hasattr(ap_entry_watcher, "MAX_OPTION_PREMIUM_DRIFT_PCT"), (
            "MAX_OPTION_PREMIUM_DRIFT_PCT must be elevated to module-level. "
            "Currently the inline `os.getenv(...)` in watch() is read on "
            "every signal arm \u2014 unnecessary overhead and hides the dep."
        )
        assert isinstance(ap_entry_watcher.MAX_OPTION_PREMIUM_DRIFT_PCT, float)

    def test_max_option_premium_drift_pct_not_read_inline_in_watch(self):
        """watch() body must not contain inline os.getenv('MAX_OPTION_PREMIUM_DRIFT_PCT', ...)."""
        src = inspect.getsource(APEntryWatcher.watch)
        pattern = re.compile(
            r'os\.getenv\(\s*["\']MAX_OPTION_PREMIUM_DRIFT_PCT["\']'
        )
        assert pattern.search(src) is None, (
            "watch() must reference the module-level constant, not "
            "call os.getenv on every signal arm"
        )


# ══════════════════════════════════════════════════════════════════
# Redundant health-registry import
# ══════════════════════════════════════════════════════════════════

class TestRedundantHealthImport:
    def test_poll_loop_does_not_re_import_health_registry(self):
        """_poll_loop already captures _WH at thread start; the inner
        `from ap_health_registry import HEALTH as _WH` was redundant."""
        src = inspect.getsource(APEntryWatcher._poll_loop)
        # Count actual import STATEMENTS (not occurrences in comments/strings).
        # Match only lines whose first non-whitespace token is `from
        # ap_health_registry import`. _poll_loop has exactly ONE such
        # statement: the outer registration at thread start (line ~1010).
        # The PR-C fix removes a SECOND inner re-import that previously sat
        # inside the `while self._running` loop and added a sys.modules
        # lookup per heartbeat.
        import_stmts = re.findall(
            r"^\s*from ap_health_registry import", src, re.MULTILINE
        )
        assert len(import_stmts) == 1, (
            f"_poll_loop has {len(import_stmts)} `from ap_health_registry "
            f"import` statement(s); should be exactly 1 (the outer "
            f"registration at thread start). Any additional import inside "
            f"the while-loop is redundant — _WH is captured at thread start."
        )


# ══════════════════════════════════════════════════════════════════
# PRECEDENCE — same-tick trigger + stop breach
# ══════════════════════════════════════════════════════════════════

class TestPrecedenceTriggerVsStop:
    def test_same_tick_call_trigger_and_stop_resolves_to_invalidated(self):
        """When ask >= trigger AND bid <= stop on the SAME check() call,
        the resulting state must be INVALIDATED (not TRIGGERED).

        This is the safer-by-design behavior the user asked to preserve:
        if the underlying is whip-sawing both directions in a single
        poll, do NOT fire the entry.
        """
        sig = _make_signal(side="CALL", entry_price=500.00, stop_price=498.00)
        ws = WatchedSignal(sig, overnight=False)
        # Need MOMENTUM_POLLS_REQUIRED prior breach so this tick would
        # trigger if not for the stop.
        ws.breach_count = ws.MOMENTUM_POLLS_REQUIRED - 1  # one more = TRIGGERED

        # Same-tick: ask=500.10 (above trigger), bid=497.50 (below stop)
        state = ws.check(bid=497.50, ask=500.10)

        assert state == WatchState.INVALIDATED, (
            "Same-tick trigger + stop breach must resolve to INVALIDATED "
            f"(safer). Got {state}. The whip-saw protection is intentional."
        )

    def test_same_tick_put_trigger_and_stop_resolves_to_invalidated(self):
        """Symmetric check for PUT.

        PUT stop-break threshold is ask >= stop_level * (1 + WRONG_DIR_BUFFER_PCT)
        = stop * 1.001. With stop=502 the threshold is 502.502, so ask must
        be >= 502.502 to register the stop break. Use ask=503.00 to clear it.
        """
        sig = _make_signal(side="PUT", entry_price=500.00, stop_price=502.00)
        ws = WatchedSignal(sig, overnight=False)
        ws.breach_count = ws.MOMENTUM_POLLS_REQUIRED - 1

        # PUT: bid<=trigger triggers, ask>=stop*1.001 invalidates.
        # Same-tick: bid=499.50 (below trigger=500) → would TRIGGER,
        #             ask=503.00 (above 502*1.001=502.502) → INVALIDATES.
        # Last-write wins → INVALIDATED.
        state = ws.check(bid=499.50, ask=503.00)

        assert state == WatchState.INVALIDATED, (
            "PUT same-tick trigger + stop breach must resolve to INVALIDATED "
            f"(safer). Got {state}. PUT precedence must match CALL."
        )


# ══════════════════════════════════════════════════════════════════
# INV — Structural: watcher mode is wired after execution-core init
# ══════════════════════════════════════════════════════════════════

class TestInvariantWatcherModeWiredFromExecutionCore:
    def test_execution_core_passes_mode_to_watcher(self):
        """After APExecutionCore construction, entry_watcher.mode must
        equal execution_core.mode (the canonical mode from master_control).
        """
        broker = MagicMock()
        osm = _make_osm()
        mc = _make_mc(mode="live")
        core = APExecutionCore(
            broker=broker,
            supabase_client=None,
            email="alice@x.com",
            position_manager=MagicMock(),
            order_state_machine=osm,
            data_broker=broker,
            master_control=mc,
        )
        assert core.entry_watcher.mode == core.mode, (
            f"entry_watcher.mode ({core.entry_watcher.mode!r}) must equal "
            f"execution_core.mode ({core.mode!r})"
        )
        # Sanity: with mc.mode='live', the canonical is LIVE
        assert core.mode == "LIVE"
        assert core.entry_watcher.mode == "LIVE"

    def test_paper_execution_core_results_in_paper_watcher(self):
        broker = MagicMock()
        osm = _make_osm()
        mc = _make_mc(mode="paper")
        core = APExecutionCore(
            broker=broker,
            supabase_client=None,
            email="alice@x.com",
            order_state_machine=osm,
            data_broker=broker,
            master_control=mc,
        )
        assert core.entry_watcher.mode == "PAPER"
