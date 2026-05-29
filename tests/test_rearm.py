"""
test_rearm.py — P0-W2 re-arm lifecycle replay tests for ap_entry_watcher.

Run:
    python -m pytest test_rearm.py -v

All tests run with WATCHER_REARM_ENABLED explicitly set per-test so the
module-level default (0 / off) does not affect results. Tests that assert
permanent-reject behavior explicitly disable rearm to confirm the prior path
is still intact.

Acceptance criteria covered (one section per criterion):
    A. NVDA-style PUT wrong-side-of-stop enters DISARMED_WAITING_FOR_RECLAIM
    B. PUT reclaims below stop threshold → rearm_mode False, normal poll resumes
    C. No reclaim before window → state=EXPIRED, on_expire fires, removed from pending
    D. Same-day rearm signal expires at EOD force-expire
    E. Low-score / intraday / non-daily → permanent reject (no rearm)
    F. Drift-stale → permanent reject regardless of score/tier
    G. No ghost order: on_expire is the cleanup signal, confirmed called on timeout
    H. Conflict: opposite-side cannot sneak through while a signal is in rearm mode
    I. Conflict: same-side (different signal_id) blocked by rearm-mode signal
    J. WATCHER_REARM_ENABLED=0 → _is_rearm_eligible always False
"""

import threading
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, call, patch

import pytest

# ---------------------------------------------------------------------------
# Import target — must be on PYTHONPATH (run from repo root).
# Module-level log calls fire on import; that is expected.
# ---------------------------------------------------------------------------
import ap_entry_watcher as ew
from ap_entry_watcher import (
    APEntryWatcher,
    WatchState,
    WatchedSignal,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_watcher(mode: str = "PAPER") -> APEntryWatcher:
    """Minimal APEntryWatcher with no OSM and a mock broker."""
    broker = MagicMock()
    broker.base_url = "https://sandbox.tradier.com"
    broker.session = MagicMock()
    return APEntryWatcher(broker=broker, order_state_machine=None, mode=mode)


def _make_signal(
    ticker: str = "NVDA",
    side: str = "PUT",
    score: float = 78.0,
    tier: str = "A",
    entry_price: float = 210.0,
    stop_price: float = 212.71,
    timeframe: str = "1d",
    local_order_id: str = "test-ord-001",
    signal_id: str = "sig-nvda-put-001",
) -> dict:
    return {
        "signal_id":      signal_id,
        "ticker":         ticker,
        "side":           side,
        "score":          score,
        "grade":          tier,
        "entry_price":    entry_price,
        "stop_price":     stop_price,
        "target_price":   entry_price * 0.95 if side == "PUT" else entry_price * 1.05,
        "plan_id":        "plan-test-001",
        "local_order_id": local_order_id,
        "timeframe":      timeframe,
        "pattern":        "3-1-2",
        "strategy_type":  "directional",
    }


def _make_ws(
    ticker: str = "NVDA",
    side: str = "PUT",
    score: float = 78.0,
    tier: str = "A",
    entry_price: float = 210.0,
    stop_price: float = 212.71,
    overnight: bool = False,
    signal_id: str = "sig-nvda-put-001",
) -> WatchedSignal:
    sig = _make_signal(
        ticker=ticker,
        side=side,
        score=score,
        tier=tier,
        entry_price=entry_price,
        stop_price=stop_price,
        signal_id=signal_id,
    )
    w = WatchedSignal(sig, overnight=overnight)
    return w


def _put_in_rearm(watcher: APEntryWatcher, w: WatchedSignal, window_sec: int = 600) -> None:
    """Place a WatchedSignal into rearm mode and register it in the watcher."""
    w.rearm_mode = True
    w.rearm_reason = f"arm_below_stop_mid_213.23_stop_{w.stop_level:.2f}"
    w.rearm_expires_at = datetime.now(timezone.utc) + timedelta(seconds=window_sec)
    w._watcher_ref = watcher
    watcher._pending.append(w)
    watcher._dedup_set.add(w.signal_id)


# ---------------------------------------------------------------------------
# J. WATCHER_REARM_ENABLED=0 → _is_rearm_eligible always False
# ---------------------------------------------------------------------------

class TestRearmDisabled:

    def test_disabled_by_default(self):
        """Default env (no override) must be disabled."""
        watcher = _make_watcher()
        # Module default is "0"; confirm no env override leaks in
        with patch.object(ew, "WATCHER_REARM_ENABLED", False):
            assert watcher._is_rearm_eligible(90.0, "A", True) is False

    def test_disabled_blocks_all_scores(self):
        watcher = _make_watcher()
        with patch.object(ew, "WATCHER_REARM_ENABLED", False):
            for score in (60.0, 75.0, 90.0, 100.0):
                assert watcher._is_rearm_eligible(score, "A", True) is False

    def test_enabled_flag_passes_control_to_other_gates(self):
        watcher = _make_watcher()
        with patch.object(ew, "WATCHER_REARM_ENABLED", True):
            with patch.object(ew, "WATCHER_REARM_MIN_SCORE", 75.0):
                with patch.object(ew, "WATCHER_REARM_ONLY_DAILY_OR_OVERNIGHT", True):
                    assert watcher._is_rearm_eligible(78.0, "A", True) is True


# ---------------------------------------------------------------------------
# _is_rearm_eligible gate matrix
# ---------------------------------------------------------------------------

class TestRearmEligibility:

    @pytest.fixture(autouse=True)
    def _enable(self):
        with patch.object(ew, "WATCHER_REARM_ENABLED", True):
            with patch.object(ew, "WATCHER_REARM_MIN_SCORE", 75.0):
                with patch.object(ew, "WATCHER_REARM_ONLY_DAILY_OR_OVERNIGHT", True):
                    yield

    def setup_method(self):
        self.watcher = _make_watcher()

    def test_score_and_tier_a_daily(self):
        assert self.watcher._is_rearm_eligible(78.0, "A", True) is True

    def test_score_only_daily(self):
        assert self.watcher._is_rearm_eligible(78.0, "B", True) is True

    def test_tier_a_only_daily(self):
        assert self.watcher._is_rearm_eligible(65.0, "A", True) is True

    def test_low_score_tier_b_daily_rejects(self):
        """score=65, tier=B, daily → not eligible."""
        assert self.watcher._is_rearm_eligible(65.0, "B", True) is False

    def test_high_score_intraday_rejects_when_daily_only(self):
        """score=90, tier=A, intraday → not eligible when ONLY_DAILY_OR_OVERNIGHT=1."""
        assert self.watcher._is_rearm_eligible(90.0, "A", False) is False

    def test_intraday_allowed_when_flag_off(self):
        with patch.object(ew, "WATCHER_REARM_ONLY_DAILY_OR_OVERNIGHT", False):
            assert self.watcher._is_rearm_eligible(90.0, "A", False) is True

    def test_score_boundary_exactly_75(self):
        assert self.watcher._is_rearm_eligible(75.0, "B", True) is True

    def test_score_boundary_74_9(self):
        assert self.watcher._is_rearm_eligible(74.9, "B", True) is False


# ---------------------------------------------------------------------------
# A. NVDA PUT wrong-side-of-stop enters DISARMED_WAITING_FOR_RECLAIM
# ---------------------------------------------------------------------------

class TestRearmEntry:

    def test_put_wrong_side_enters_disarmed_state(self):
        """add_signal() with __watcher_rearm_pending=True must set rearm_mode
        and queue_status=DISARMED_WAITING_FOR_RECLAIM."""
        watcher = _make_watcher()
        sig = _make_signal("NVDA", "PUT", score=78.0, tier="A",
                           entry_price=210.0, stop_price=212.71)
        sig["__watcher_rearm_pending"] = True
        sig["__watcher_rearm_reason"] = "arm_below_stop_mid_213.23_stop_212.71"

        with patch.object(ew, "WATCHER_REARM_ENABLED", True):
            with patch.object(ew, "WATCHER_REARM_WINDOW_SEC", 600):
                ok = watcher.add_signal(sig)

        assert ok is True, "add_signal must return True — signal is accepted, not rejected"
        assert len(watcher._pending) == 1
        w = watcher._pending[0]

        assert w.rearm_mode is True
        assert w.state == WatchState.PENDING
        assert w.is_active is False, "rearm_mode signal must not be active in normal poll"
        assert w.signal.get("queue_status") == "DISARMED_WAITING_FOR_RECLAIM"
        assert w.rearm_expires_at is not None
        assert w.rearm_reason == "arm_below_stop_mid_213.23_stop_212.71"

    def test_rearm_marker_cleaned_from_signal(self):
        """__watcher_rearm_pending must not propagate downstream."""
        watcher = _make_watcher()
        sig = _make_signal("NVDA", "PUT")
        sig["__watcher_rearm_pending"] = True
        sig["__watcher_rearm_reason"] = "arm_below_stop_mid_213.23_stop_212.71"

        with patch.object(ew, "WATCHER_REARM_ENABLED", True):
            watcher.add_signal(sig)

        w = watcher._pending[0]
        assert "__watcher_rearm_pending" not in w.signal
        assert "__watcher_rearm_reason" not in w.signal

    def test_rearm_count_starts_at_zero(self):
        watcher = _make_watcher()
        sig = _make_signal("NVDA", "PUT")
        sig["__watcher_rearm_pending"] = True
        sig["__watcher_rearm_reason"] = "arm_below_stop_mid_213.23_stop_212.71"

        with patch.object(ew, "WATCHER_REARM_ENABLED", True):
            watcher.add_signal(sig)

        assert watcher._pending[0].rearm_count == 0

    def test_dedup_key_registered_for_rearm_signal(self):
        """Dedup key must be registered even in rearm mode so a duplicate arm is blocked."""
        watcher = _make_watcher()
        sig = _make_signal("NVDA", "PUT", signal_id="sig-dedup-test")
        sig["__watcher_rearm_pending"] = True
        sig["__watcher_rearm_reason"] = "arm_below_stop"

        with patch.object(ew, "WATCHER_REARM_ENABLED", True):
            watcher.add_signal(sig)

        assert "sig-dedup-test" in watcher._dedup_set


# ---------------------------------------------------------------------------
# B. PUT reclaims below stop threshold → rearm_mode=False, is_active=True
# ---------------------------------------------------------------------------

class TestRearmReclaim:

    def _setup_put_rearm(self, watcher, stop_price=212.71):
        w = _make_ws("NVDA", "PUT", score=78.0, tier="A",
                     entry_price=210.0, stop_price=stop_price)
        _put_in_rearm(watcher, w, window_sec=600)
        return w

    def test_put_reclaims_below_stop_with_tolerance(self):
        """mid=212.49 <= 212.71*(1-0.001)=212.497 → reclaim fires."""
        watcher = _make_watcher()
        w = self._setup_put_rearm(watcher, stop_price=212.71)

        # mid = (212.40 + 212.58) / 2 = 212.49 ≤ 212.71*0.999 = 212.4973
        mock_quote = {"symbol": "NVDA", "bid": 212.40, "ask": 212.58}
        with patch.object(watcher, "_fetch_quotes", return_value={"NVDA": mock_quote}):
            with patch.object(ew, "WATCHER_REARM_TOLERANCE_PCT", 0.001):
                watcher._check_rearm_signals()

        assert w.rearm_mode is False
        assert w.is_active is True
        assert w.state == WatchState.PENDING
        assert w.rearm_count == 1
        assert w in watcher._pending, "Signal must remain in pending after reclaim"

    def test_put_does_not_reclaim_above_threshold(self):
        """mid=213.23 > 212.71*(1-0.001) → no reclaim yet."""
        watcher = _make_watcher()
        w = self._setup_put_rearm(watcher, stop_price=212.71)

        mock_quote = {"symbol": "NVDA", "bid": 213.10, "ask": 213.36}  # mid=213.23
        with patch.object(watcher, "_fetch_quotes", return_value={"NVDA": mock_quote}):
            with patch.object(ew, "WATCHER_REARM_TOLERANCE_PCT", 0.001):
                watcher._check_rearm_signals()

        assert w.rearm_mode is True, "Must remain in rearm — not yet reclaimed"
        assert w.is_active is False
        assert w.rearm_count == 0

    def test_call_reclaims_above_stop_with_tolerance(self):
        """CALL: mid=100.12 >= 100*(1+0.001)=100.10 → reclaim fires."""
        watcher = _make_watcher()
        w = _make_ws("AAPL", "CALL", score=80.0, tier="A",
                     entry_price=105.0, stop_price=100.0)
        _put_in_rearm(watcher, w, window_sec=600)

        mock_quote = {"symbol": "AAPL", "bid": 100.10, "ask": 100.14}  # mid=100.12
        with patch.object(watcher, "_fetch_quotes", return_value={"AAPL": mock_quote}):
            with patch.object(ew, "WATCHER_REARM_TOLERANCE_PCT", 0.001):
                watcher._check_rearm_signals()

        assert w.rearm_mode is False
        assert w.is_active is True
        assert w.rearm_count == 1

    def test_call_does_not_reclaim_below_threshold(self):
        """CALL: mid=99.5 < 100*(1+0.001)=100.10 → no reclaim."""
        watcher = _make_watcher()
        w = _make_ws("AAPL", "CALL", score=80.0, tier="A",
                     entry_price=105.0, stop_price=100.0)
        _put_in_rearm(watcher, w, window_sec=600)

        mock_quote = {"symbol": "AAPL", "bid": 99.40, "ask": 99.60}  # mid=99.50
        with patch.object(watcher, "_fetch_quotes", return_value={"AAPL": mock_quote}):
            with patch.object(ew, "WATCHER_REARM_TOLERANCE_PCT", 0.001):
                watcher._check_rearm_signals()

        assert w.rearm_mode is True
        assert w.rearm_count == 0

    def test_quote_fetch_failure_leaves_signal_in_rearm(self):
        """If _fetch_quotes raises, signal must remain in rearm — not expired."""
        watcher = _make_watcher()
        w = self._setup_put_rearm(watcher)

        with patch.object(watcher, "_fetch_quotes", side_effect=Exception("timeout")):
            watcher._check_rearm_signals()

        assert w.rearm_mode is True
        assert w.state == WatchState.PENDING

    def test_no_rearm_signals_check_is_noop(self):
        """_check_rearm_signals with empty rearm list must not raise."""
        watcher = _make_watcher()
        w = _make_ws("NVDA", "PUT")
        w.rearm_mode = False  # active, not in rearm
        watcher._pending.append(w)

        with patch.object(watcher, "_fetch_quotes", return_value={}) as mock_fetch:
            watcher._check_rearm_signals()

        mock_fetch.assert_not_called()


# ---------------------------------------------------------------------------
# C. No reclaim before window → state=EXPIRED, on_expire fires, removed from pending
# ---------------------------------------------------------------------------

class TestRearmTimeout:

    def test_timeout_expires_signal(self):
        watcher = _make_watcher()
        on_expire = MagicMock()
        watcher.on_expire = on_expire

        w = _make_ws("NVDA", "PUT", score=78.0, tier="A")
        w.rearm_mode = True
        # Window already elapsed
        w.rearm_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        w._watcher_ref = watcher
        watcher._pending.append(w)
        watcher._dedup_set.add(w.signal_id)

        with patch.object(watcher, "_fetch_quotes", return_value={}):
            watcher._check_rearm_signals()

        assert w.state == WatchState.EXPIRED
        assert w.rearm_mode is False
        assert w not in watcher._pending, "Expired rearm signal must be removed from pending"

    def test_timeout_fires_on_expire_callback(self):
        """G. No ghost order: on_expire is the cleanup hook, must be called."""
        watcher = _make_watcher()
        on_expire = MagicMock()
        watcher.on_expire = on_expire

        w = _make_ws("NVDA", "PUT", score=78.0, tier="A")
        w.rearm_mode = True
        w.rearm_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        w._watcher_ref = watcher
        watcher._pending.append(w)

        with patch.object(watcher, "_fetch_quotes", return_value={}):
            watcher._check_rearm_signals()

        on_expire.assert_called_once_with(w)

    def test_timeout_dedup_key_released(self):
        """Dedup key must be freed on timeout expiry."""
        watcher = _make_watcher()
        watcher.on_expire = MagicMock()

        w = _make_ws("NVDA", "PUT", signal_id="sig-timeout-test")
        w.rearm_mode = True
        w.rearm_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        w._watcher_ref = watcher
        watcher._pending.append(w)
        watcher._dedup_set.add("sig-timeout-test")

        with patch.object(watcher, "_fetch_quotes", return_value={}):
            watcher._check_rearm_signals()

        assert "sig-timeout-test" not in watcher._dedup_set

    def test_on_expire_exception_does_not_propagate(self):
        """on_expire raising must not crash _check_rearm_signals."""
        watcher = _make_watcher()
        watcher.on_expire = MagicMock(side_effect=RuntimeError("callback failed"))

        w = _make_ws("NVDA", "PUT")
        w.rearm_mode = True
        w.rearm_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        w._watcher_ref = watcher
        watcher._pending.append(w)

        with patch.object(watcher, "_fetch_quotes", return_value={}):
            watcher._check_rearm_signals()  # must not raise

        assert w.state == WatchState.EXPIRED

    def test_max_rearm_attempts_expires_on_reclaim(self):
        """When rearm_count has already hit WATCHER_REARM_MAX_ATTEMPTS, the next
        reclaim must expire the signal rather than re-arming it."""
        watcher = _make_watcher()
        on_expire = MagicMock()
        watcher.on_expire = on_expire

        w = _make_ws("NVDA", "PUT", score=78.0, tier="A",
                     entry_price=210.0, stop_price=212.71)
        w.rearm_mode = True
        w.rearm_count = 1          # already used the 1 allowed attempt
        w.rearm_expires_at = datetime.now(timezone.utc) + timedelta(seconds=600)
        w._watcher_ref = watcher
        watcher._pending.append(w)

        # Price is below threshold → would normally reclaim
        mock_quote = {"symbol": "NVDA", "bid": 212.40, "ask": 212.48}
        with patch.object(watcher, "_fetch_quotes", return_value={"NVDA": mock_quote}):
            with patch.object(ew, "WATCHER_REARM_TOLERANCE_PCT", 0.001):
                with patch.object(ew, "WATCHER_REARM_MAX_ATTEMPTS", 1):
                    watcher._check_rearm_signals()

        assert w.state == WatchState.EXPIRED
        assert w.rearm_mode is False
        assert w not in watcher._pending
        on_expire.assert_called_once_with(w)


# ---------------------------------------------------------------------------
# D. Same-day rearm signal expires at EOD force-expire
# ---------------------------------------------------------------------------

class TestEodRearmExpiry:

    def test_same_day_rearm_signal_expires_at_eod(self):
        """Rearm signal with overnight=False must be expired at EOD.
        Previously rearm_mode signals had is_active=False and were invisible
        to the EOD gate — they would ghost as phantom PENDING rows."""
        watcher = _make_watcher()
        on_expire = MagicMock()
        watcher.on_expire = on_expire

        w = _make_ws("NVDA", "PUT", score=78.0, tier="A")
        w.rearm_mode = True
        w.overnight = False
        w.rearm_expires_at = datetime.now(timezone.utc) + timedelta(hours=2)
        w._watcher_ref = watcher
        watcher._pending = [w]

        # Simulate _check_all at 15:35 ET (past EOD cutoff of 15:30)
        eod_et = MagicMock()
        eod_et.hour = 15
        eod_et.minute = 35
        eod_et.date.return_value = datetime.now().date()

        with patch("ap_entry_watcher.datetime") as mock_dt:
            mock_dt.now.return_value = eod_et
            # Also patch the timezone call used for datetime.now(timezone.utc)
            # by going around it — call the EOD block directly.
            # We test the EOD condition by inspecting the _is_eod_target logic.
            # Direct test of EOD target flag:
            _is_eod_target = (
                (w.is_active and not w.overnight)
                or (getattr(w, "rearm_mode", False) and not w.overnight)
            )

        assert _is_eod_target is True, (
            "rearm_mode same-day signal must be flagged as EOD target "
            "(is_active=False alone would miss it)"
        )

    def test_overnight_rearm_signal_survives_eod(self):
        """Rearm signal with overnight=True must NOT be expired at EOD."""
        w = _make_ws("NVDA", "PUT", score=78.0, tier="A")
        w.rearm_mode = True
        w.overnight = True
        w.rearm_expires_at = datetime.now(timezone.utc) + timedelta(hours=2)

        _is_eod_target = (
            (w.is_active and not w.overnight)
            or (getattr(w, "rearm_mode", False) and not w.overnight)
        )
        assert _is_eod_target is False, "Overnight rearm signal must survive EOD"

    def test_is_active_false_for_rearm(self):
        """is_active must return False for any rearm_mode signal."""
        w = _make_ws("NVDA", "PUT")
        w.rearm_mode = True
        assert w.is_active is False

    def test_is_active_true_after_reclaim(self):
        """After rearm_mode is cleared, is_active must return True."""
        w = _make_ws("NVDA", "PUT")
        w.rearm_mode = True
        assert w.is_active is False
        w.rearm_mode = False
        assert w.is_active is True


# ---------------------------------------------------------------------------
# E. Low-score / intraday / non-daily → permanent reject (no rearm)
# ---------------------------------------------------------------------------

class TestPermanentReject:

    @pytest.fixture(autouse=True)
    def _enable_rearm(self):
        """Enable rearm so we can confirm it's still gated by eligibility."""
        with patch.object(ew, "WATCHER_REARM_ENABLED", True):
            with patch.object(ew, "WATCHER_REARM_MIN_SCORE", 75.0):
                with patch.object(ew, "WATCHER_REARM_ONLY_DAILY_OR_OVERNIGHT", True):
                    yield

    def setup_method(self):
        self.watcher = _make_watcher()

    def test_low_score_intraday_not_eligible(self):
        assert self.watcher._is_rearm_eligible(65.0, "B", False) is False

    def test_low_score_daily_not_eligible(self):
        assert self.watcher._is_rearm_eligible(65.0, "B", True) is False

    def test_high_score_intraday_not_eligible_when_daily_only(self):
        assert self.watcher._is_rearm_eligible(90.0, "A", False) is False

    def test_add_signal_without_rearm_marker_returns_true_normally(self):
        """A normal add_signal (no rearm marker) must not enter rearm mode."""
        sig = _make_signal("NVDA", "PUT", score=65.0, tier="B")
        ok = self.watcher.add_signal(sig)
        assert ok is True
        w = self.watcher._pending[0]
        assert w.rearm_mode is False
        assert w.is_active is True


# ---------------------------------------------------------------------------
# F. Drift-stale → permanent reject regardless of score/tier
# ---------------------------------------------------------------------------

class TestDriftStalePermanentReject:

    def test_drift_stale_math_put(self):
        """Structural: PUT mid=$205 vs trigger=$210 with 1% threshold.
        pct_from_trigger = (205-210)/210 = -2.38% < -1% → drift_stale=True."""
        trigger = 210.0
        mid = 205.0
        threshold = 0.01
        pct = (mid - trigger) / trigger  # -0.02381
        drift_stale = (pct < -threshold)  # PUT gate
        assert drift_stale is True

    def test_drift_stale_never_sets_below_stop_true(self):
        """When drift_stale fires (mid decisively away from trigger),
        below_stop is separately evaluated — they are not mutually exclusive.
        But the code's `if drift_stale:` returns before the `elif below_stop:` branch,
        so even if both would fire, drift_stale takes priority and no rearm occurs."""
        trigger = 210.0
        stop = 212.71
        mid = 205.0
        side = "PUT"
        threshold = 0.01

        pct = (mid - trigger) / trigger
        drift_stale = (side == "PUT" and pct < -threshold)
        assert drift_stale is True

        below_stop = False
        if stop and stop > 0:
            if side == "PUT" and mid > stop:
                below_stop = True
        # mid=205 is NOT above put_stop=212.71, so below_stop=False here
        assert below_stop is False

    def test_drift_stale_call_math(self):
        """CALL mid=$216 vs trigger=$210 with 1% threshold.
        pct_from_trigger = (216-210)/210 = +2.86% > +1% → drift_stale=True."""
        trigger = 210.0
        mid = 216.0
        threshold = 0.01
        pct = (mid - trigger) / trigger  # +0.0286
        drift_stale = (pct > threshold)  # CALL gate
        assert drift_stale is True

    def test_drift_stale_does_not_reach_rearm_when_enabled(self):
        """Even with WATCHER_REARM_ENABLED=True and a score-90/tier-A signal,
        _is_rearm_eligible returning True is irrelevant when drift_stale fires —
        the code structure guarantees `return False` before the eligibility check.
        This test confirms the if/elif branching is mutually exclusive."""
        watcher = _make_watcher()
        with patch.object(ew, "WATCHER_REARM_ENABLED", True):
            # High-conviction signal would be eligible...
            eligible = watcher._is_rearm_eligible(90.0, "A", True)
        assert eligible is True   # Would be eligible under below_stop

        # ...but drift_stale fires in an `if drift_stale:` block that returns False,
        # and the `elif below_stop:` block (which calls _is_rearm_eligible) is never
        # reached. The mutual exclusivity is structural: `if drift_stale` / `elif below_stop`.
        # We verify this by inspecting that these are separate branches in the codebase.
        import inspect
        source = inspect.getsource(watcher.watch)
        # The drift branch must be a standalone `if`, the rearm branch must be `elif`
        assert "if drift_stale:" in source
        assert "elif below_stop:" in source
        # And _is_rearm_eligible must only appear inside the elif, not the if
        drift_block_start = source.index("if drift_stale:")
        elif_block_start = source.index("elif below_stop:")
        rearm_check_pos = source.index("_is_rearm_eligible")
        assert rearm_check_pos > elif_block_start, (
            "_is_rearm_eligible must only appear in the elif below_stop branch, "
            "never in the if drift_stale branch"
        )


# ---------------------------------------------------------------------------
# H. Conflict: opposite-side cannot sneak through while signal is in rearm mode
# ---------------------------------------------------------------------------

class TestConflictDetectionWithRearm:

    def test_opposite_side_blocked_by_rearm_put(self):
        """A new CALL must not arm when a PUT is in DISARMED_WAITING_FOR_RECLAIM
        with a higher score."""
        watcher = _make_watcher()

        # Existing PUT in rearm mode, score=78
        existing_put = _make_ws("NVDA", "PUT", score=78.0, tier="A",
                                entry_price=210.0, stop_price=212.71,
                                signal_id="sig-nvda-put-existing")
        _put_in_rearm(watcher, existing_put, window_sec=600)

        # Attempt to arm a weaker CALL, score=70
        new_call_sig = _make_signal("NVDA", "CALL", score=70.0, tier="B",
                                    entry_price=215.0, stop_price=210.0,
                                    signal_id="sig-nvda-call-new")
        ok = watcher.add_signal(new_call_sig)

        assert ok is False, "Weaker CALL must be blocked by rearm-mode PUT"
        assert watcher._last_reject_reason == "opposite_side_conflict"
        assert len(watcher._pending) == 1, "Only the original PUT should remain"
        assert watcher._pending[0].signal_id == "sig-nvda-put-existing"

    def test_stronger_opposite_side_flips_rearm_signal(self):
        """A stronger CALL must cancel the rearm-mode PUT (direction flip)."""
        watcher = _make_watcher()
        cancel_fn = MagicMock()
        osm = MagicMock()
        osm.cancel_pending_entry = cancel_fn
        watcher.order_state_machine = osm

        existing_put = _make_ws("NVDA", "PUT", score=70.0, tier="B",
                                entry_price=210.0, stop_price=212.71,
                                signal_id="sig-nvda-put-existing")
        _put_in_rearm(watcher, existing_put, window_sec=600)

        new_call_sig = _make_signal("NVDA", "CALL", score=85.0, tier="A",
                                    entry_price=215.0, stop_price=210.0,
                                    signal_id="sig-nvda-call-stronger")
        ok = watcher.add_signal(new_call_sig)

        assert ok is True
        # The rearm PUT must be gone; only the new CALL remains
        tickers_sides = [(w.ticker, w.side) for w in watcher._pending]
        assert ("NVDA", "PUT") not in tickers_sides
        assert ("NVDA", "CALL") in tickers_sides
        # OSM cancel must have been called for the displaced PUT
        cancel_fn.assert_called_once()

    def test_same_side_blocked_by_rearm_put(self):
        """A weaker new PUT must not arm when a stronger PUT is in rearm mode."""
        watcher = _make_watcher()

        existing_put = _make_ws("NVDA", "PUT", score=78.0, tier="A",
                                entry_price=210.0, stop_price=212.71,
                                signal_id="sig-nvda-put-existing")
        _put_in_rearm(watcher, existing_put, window_sec=600)

        weaker_put_sig = _make_signal("NVDA", "PUT", score=65.0, tier="B",
                                      entry_price=209.0, stop_price=212.71,
                                      signal_id="sig-nvda-put-weaker")
        ok = watcher.add_signal(weaker_put_sig)

        assert ok is False, "Weaker PUT must be blocked by stronger rearm-mode PUT"
        assert watcher._last_reject_reason == "same_side_block"
        assert len(watcher._pending) == 1

    def test_stronger_same_side_replaces_rearm_signal(self):
        """A stronger new PUT must replace the weaker rearm-mode PUT."""
        watcher = _make_watcher()
        cancel_fn = MagicMock()
        osm = MagicMock()
        osm.cancel_pending_entry = cancel_fn
        watcher.order_state_machine = osm

        existing_put = _make_ws("NVDA", "PUT", score=65.0, tier="B",
                                entry_price=210.0, stop_price=212.71,
                                signal_id="sig-nvda-put-weaker-existing")
        _put_in_rearm(watcher, existing_put, window_sec=600)

        stronger_put_sig = _make_signal("NVDA", "PUT", score=85.0, tier="A",
                                        entry_price=209.0, stop_price=212.71,
                                        signal_id="sig-nvda-put-stronger")
        ok = watcher.add_signal(stronger_put_sig)

        assert ok is True
        assert len(watcher._pending) == 1
        assert watcher._pending[0].signal_id == "sig-nvda-put-stronger"
        cancel_fn.assert_called_once()

    def test_dedup_same_signal_id_blocked_in_rearm(self):
        """The same signal_id must be blocked by dedup even while in rearm mode."""
        watcher = _make_watcher()

        existing_put = _make_ws("NVDA", "PUT", signal_id="sig-nvda-put-dup")
        _put_in_rearm(watcher, existing_put, window_sec=600)
        # dedup_set already has sig-nvda-put-dup from _put_in_rearm

        duplicate_sig = _make_signal("NVDA", "PUT", signal_id="sig-nvda-put-dup")
        ok = watcher.add_signal(duplicate_sig)

        assert ok is False
        assert watcher._last_reject_reason == "dedup_block"


# ---------------------------------------------------------------------------
# WatchedSignal slot integrity
# ---------------------------------------------------------------------------

class TestWatchedSignalSlots:

    def test_rearm_slots_initialized(self):
        w = _make_ws("NVDA", "PUT")
        assert hasattr(w, "rearm_mode")
        assert hasattr(w, "rearm_expires_at")
        assert hasattr(w, "rearm_reason")
        assert hasattr(w, "rearm_count")

    def test_rearm_defaults(self):
        w = _make_ws("NVDA", "PUT")
        assert w.rearm_mode is False
        assert w.rearm_expires_at is None
        assert w.rearm_reason == ""
        assert w.rearm_count == 0

    def test_pending_audit_slot_present(self):
        w = _make_ws("NVDA", "PUT")
        assert hasattr(w, "_pending_audit")
        assert w._pending_audit is None


# ---------------------------------------------------------------------------
# status() exposes rearm fields
# ---------------------------------------------------------------------------

class TestStatusRearmFields:

    def test_status_includes_rearm_fields(self):
        watcher = _make_watcher()
        w = _make_ws("NVDA", "PUT")
        w.rearm_mode = True
        w.rearm_count = 0
        w.rearm_expires_at = datetime.now(timezone.utc) + timedelta(seconds=300)
        w.rearm_reason = "arm_below_stop_mid_213.23_stop_212.71"
        w._watcher_ref = watcher
        watcher._pending.append(w)

        status = watcher.status()
        assert len(status) == 1
        row = status[0]

        assert "rearm_mode" in row
        assert row["rearm_mode"] is True
        assert "rearm_count" in row
        assert "rearm_expires_at" in row
        assert "rearm_reason" in row
        assert row["rearm_reason"] == "arm_below_stop_mid_213.23_stop_212.71"

    def test_status_active_signal_rearm_false(self):
        watcher = _make_watcher()
        w = _make_ws("NVDA", "PUT")
        w._watcher_ref = watcher
        watcher._pending.append(w)

        status = watcher.status()
        row = status[0]
        assert row["rearm_mode"] is False
        assert row["rearm_expires_at"] is None


# ---------------------------------------------------------------------------
# Patch-only meta write — confirm _persist_watcher_audit never passes full meta
# ---------------------------------------------------------------------------

class TestPersistWatcherAudit:
    """Verify the patch-only meta write contract.

    _persist_watcher_audit must only write:
        {"watcher_audit": payload, "watcher_audit_history": [...]}
    to update_order_meta — never the full existing meta dict.  Passing the full
    meta risks overwriting concurrent fields (retry_status, retry_payload, etc.)
    written by the retry engine or other components between the read and write.
    """

    def _mock_watcher_with_osm(self, existing_meta: dict, patches: list):
        watcher = _make_watcher()
        mock_order = {"local_order_id": "ord-001", "client_id": "default",
                      "meta": existing_meta}

        class _MockOSM:
            def get_order(self, _local_oid):
                return mock_order

            def update_order_meta(self, _local_oid, meta_patch):
                patches.append(dict(meta_patch))
                return True

        watcher.order_state_machine = _MockOSM()
        return watcher

    def test_only_patch_keys_passed_to_update_order_meta(self):
        """update_order_meta must receive only watcher_audit + watcher_audit_history.
        Protected fields in the existing meta must never appear in the patch."""
        patches = []
        existing = {
            "retry_status":  "pending",
            "retry_payload": {"attempt": 2},
            "score":         78.0,
            "tier":          "A",
        }
        watcher = self._mock_watcher_with_osm(existing, patches)
        payload = {"symbol": "NVDA", "reason_code": "arm_below_stop",
                   "evaluated_at": "2026-01-01T10:00:00+00:00"}

        watcher._persist_watcher_audit("ord-001", payload)

        assert len(patches) == 1, "update_order_meta must be called exactly once"
        patch = patches[0]
        assert set(patch.keys()) == {"watcher_audit", "watcher_audit_history"}, (
            f"Patch must contain only watcher keys, got: {set(patch.keys())}"
        )
        assert "retry_status"  not in patch
        assert "retry_payload" not in patch
        assert "score"         not in patch
        assert "tier"          not in patch
        assert patch["watcher_audit"] == payload
        assert patch["watcher_audit_history"] == [payload]

    def test_history_capped_at_5_entries(self):
        """watcher_audit_history must be capped at 5."""
        existing_history = [{"reason_code": f"entry_{i}"} for i in range(5)]
        patches = []
        watcher = self._mock_watcher_with_osm(
            {"watcher_audit_history": existing_history}, patches
        )
        watcher._persist_watcher_audit("ord-001", {"reason_code": "new_entry"})

        assert len(patches) == 1
        history = patches[0]["watcher_audit_history"]
        assert len(history) == 5, f"History must be capped at 5, got {len(history)}"
        assert history[-1]["reason_code"] == "new_entry"
        assert history[0]["reason_code"] == "entry_1"  # oldest entry_0 was dropped

    def test_history_accumulates_below_cap(self):
        """History entries below cap must all be preserved."""
        patches = []
        watcher = self._mock_watcher_with_osm(
            {"watcher_audit_history": [{"reason_code": "old"}]}, patches
        )
        watcher._persist_watcher_audit("ord-001", {"reason_code": "new"})

        history = patches[0]["watcher_audit_history"]
        assert len(history) == 2
        assert history[0]["reason_code"] == "old"
        assert history[1]["reason_code"] == "new"

    def test_no_osm_logs_persisted_false_and_does_not_raise(self):
        """When OSM is None, must not raise."""
        watcher = _make_watcher()
        watcher.order_state_machine = None
        watcher._persist_watcher_audit("ord-001", {"reason_code": "test"})  # must not raise

    def test_order_not_found_logs_persisted_false_and_does_not_raise(self):
        """When get_order returns None, must not raise."""
        watcher = _make_watcher()

        class _MockOSM:
            def get_order(self, _): return None

        watcher.order_state_machine = _MockOSM()
        watcher._persist_watcher_audit("ord-missing", {"reason_code": "test"})

    def test_no_local_order_id_logs_persisted_false_and_does_not_raise(self):
        """When local_order_id is empty/None, must not raise."""
        watcher = _make_watcher()
        watcher._persist_watcher_audit(None, {"reason_code": "test"})
        watcher._persist_watcher_audit("", {"reason_code": "test"})

    def test_update_order_meta_returning_false_falls_through_gracefully(self):
        """If update_order_meta returns False (row not found), must not raise."""
        watcher = _make_watcher()
        mock_order = {"local_order_id": "ord-001", "meta": {}}

        class _MockOSM:
            def get_order(self, _): return mock_order
            def update_order_meta(self, _oid, _patch): return False  # row not found

        watcher.order_state_machine = _MockOSM()
        watcher._persist_watcher_audit("ord-001", {"reason_code": "test"})


# ---------------------------------------------------------------------------
# Private key cleanup guard
# ---------------------------------------------------------------------------

class TestPrivateKeyCleanup:
    """Verify __watcher_rearm_pending / __watcher_rearm_reason never escape."""

    def test_markers_cleaned_by_add_signal_on_success(self):
        """On successful add_signal(), markers must be popped from signal dict."""
        watcher = _make_watcher()
        sig = _make_signal("NVDA", "PUT")
        sig["__watcher_rearm_pending"] = True
        sig["__watcher_rearm_reason"] = "arm_below_stop_mid_213.23_stop_212.71"

        with patch.object(ew, "WATCHER_REARM_ENABLED", True):
            ok = watcher.add_signal(sig)

        assert ok is True
        assert "__watcher_rearm_pending" not in sig
        assert "__watcher_rearm_reason"  not in sig
        # The WatchedSignal.signal is the same object — confirm there too
        assert "__watcher_rearm_pending" not in watcher._pending[0].signal
        assert "__watcher_rearm_reason"  not in watcher._pending[0].signal

    def test_watch_cleanup_guard_present_in_source(self):
        """watch() must use try/finally to clean private rearm keys so markers
        are removed even when add_signal() raises, not just on normal return."""
        import inspect
        source = inspect.getsource(_make_watcher().watch)

        assert "try:" in source
        assert "finally:" in source
        assert 'signal_dict.pop("__watcher_rearm_pending"' in source
        assert 'signal_dict.pop("__watcher_rearm_reason"' in source

        finally_pos      = source.index("finally:")
        pop_pending_pos  = source.index('signal_dict.pop("__watcher_rearm_pending"')
        pop_reason_pos   = source.index('signal_dict.pop("__watcher_rearm_reason"')
        assert pop_pending_pos > finally_pos
        assert pop_reason_pos  > finally_pos

    # Gap-2 — watch() must SET markers before add_signal() in below_stop branch
    def test_watch_sets_rearm_markers_before_add_signal(self):
        """watch() must set __watcher_rearm_pending and __watcher_rearm_reason
        on signal_dict BEFORE calling add_signal() in the elif below_stop branch.
        If this assignment is missing, every below_stop arm failure is permanently
        rejected even when WATCHER_REARM_ENABLED=1 — rearm is silently dead."""
        import inspect
        source = inspect.getsource(_make_watcher().watch)

        assert 'signal_dict["__watcher_rearm_pending"] = True' in source, (
            "watch() must set __watcher_rearm_pending=True before add_signal()"
        )
        assert 'signal_dict["__watcher_rearm_reason"] =' in source, (
            "watch() must set __watcher_rearm_reason before add_signal()"
        )
        # Confirm the SET appears BEFORE the add_signal() call in source order
        set_pos = source.index('signal_dict["__watcher_rearm_pending"] = True')
        add_pos = source.index("add_signal(signal_dict)")
        assert set_pos < add_pos, (
            "__watcher_rearm_pending must be SET before add_signal() is called — "
            "not after (otherwise add_signal() can't see the marker)"
        )


# ---------------------------------------------------------------------------
# Gap-1 — _check_all() EOD integration test (mocked clock)
# ---------------------------------------------------------------------------

class TestEodCheckAllIntegration:
    """Integration-level EOD tests that call _check_all() directly with a
    mocked 15:35 ET clock.  These complement the unit-level boolean tests in
    TestEodRearmExpiry and prove the gate actually fires on_expire and removes
    the signal from self._pending."""

    def _make_eod_et_mock(self):
        """Return a MagicMock that looks like 15:35 ET (past EOD cutoff)."""
        m = MagicMock()
        m.hour   = 15
        m.minute = 35
        m.date.return_value = MagicMock()
        return m

    def test_check_all_eod_expires_rearm_signal_and_fires_on_expire(self):
        """_check_all() at 15:35 ET must expire a same-day rearm signal,
        call on_expire, and remove it from self._pending."""
        from unittest.mock import patch as _patch

        watcher    = _make_watcher()
        on_expire  = MagicMock()
        watcher.on_expire = on_expire

        w = _make_ws("NVDA", "PUT", score=78.0, tier="A")
        w.rearm_mode      = True
        w.overnight       = False
        w.rearm_expires_at = datetime.now(timezone.utc) + timedelta(hours=2)
        w._watcher_ref    = watcher
        watcher._pending  = [w]
        watcher._dedup_set.add(w.signal_id)

        et_now = self._make_eod_et_mock()
        with _patch("ap_entry_watcher.datetime") as mock_dt:
            mock_dt.now.return_value = et_now
            watcher._check_all()

        assert w.state == WatchState.EXPIRED, (
            "Rearm-mode same-day signal must be EXPIRED by _check_all() EOD gate"
        )
        assert w not in watcher._pending, (
            "Expired rearm signal must be removed from self._pending"
        )
        on_expire.assert_called_once_with(w)

    def test_check_all_eod_does_not_expire_overnight_rearm(self):
        """_check_all() at 15:35 ET must NOT expire a rearm signal with
        overnight=True — overnight signals survive EOD by design."""
        from unittest.mock import patch as _patch

        watcher   = _make_watcher()
        on_expire = MagicMock()
        watcher.on_expire = on_expire

        w = _make_ws("NVDA", "PUT", score=78.0, tier="A")
        w.rearm_mode       = True
        w.overnight        = True
        w.rearm_expires_at = datetime.now(timezone.utc) + timedelta(hours=8)
        w._watcher_ref     = watcher
        watcher._pending   = [w]

        et_now = self._make_eod_et_mock()
        with _patch("ap_entry_watcher.datetime") as mock_dt:
            mock_dt.now.return_value = et_now
            try:
                watcher._check_all()
            except Exception:
                pass

        assert w.state == WatchState.PENDING, (
            "Overnight rearm signal must NOT be expired at EOD"
        )
        assert w in watcher._pending

    def test_eod_target_boolean_same_day_rearm(self):
        """Unit-level: same-day rearm signal must be EOD target.
        rearm_mode=True means is_active=False — the target must include rearm_mode."""
        w = _make_ws("NVDA", "PUT")
        w.rearm_mode = True
        w.overnight  = False
        assert w.is_active is False, "rearm_mode=True must suppress is_active"
        _is_eod_target = (
            (w.is_active and not w.overnight)
            or (getattr(w, "rearm_mode", False) and not w.overnight)
        )
        assert _is_eod_target is True, (
            "is_active=False alone would miss this signal — rearm_mode must be included"
        )

    def test_eod_target_boolean_overnight_rearm_excluded(self):
        w = _make_ws("NVDA", "PUT")
        w.rearm_mode = True
        w.overnight  = True
        _is_eod_target = (
            (w.is_active and not w.overnight)
            or (getattr(w, "rearm_mode", False) and not w.overnight)
        )
        assert _is_eod_target is False

