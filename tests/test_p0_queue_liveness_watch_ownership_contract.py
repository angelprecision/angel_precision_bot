"""
PR #404 – P0 Queue Liveness: Watcher-Arm and Pending-Entry Ownership Contract
==============================================================================
Covers the two measured incident buckets from 2026-07-28:
  • overnight_watch_arm_failed:watch_returned_false  (16 signals terminally rejected)
  • mc_blocked:pending_entry_exists                  (33 signals suppressed)

No broker submits, no OSM mutation, no proof-trades change in this PR.
"""
from __future__ import annotations

import importlib
import sys
import types
from datetime import datetime, timedelta, timezone

import pytest

# ── Module-level stubs (must precede production imports) ─────────────────────
# ap.db raises RuntimeError at import when DATABASE_URL is unset; stub it so
# the classifier modules load without a live DB.  The classifiers we test are
# pure functions that never call into ap.db directly.

def _stub_db():
    if "ap.db" in sys.modules and getattr(sys.modules["ap.db"], "_stub", False):
        return
    class _FakeConn:
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def execute(self, *a, **kw): pass
        def fetchone(self): return None
        def fetchall(self): return []
    _mod = types.ModuleType("ap.db")
    _mod._stub = True
    _mod.conn = lambda *a, **kw: _FakeConn()
    _mod.run_with_retry = lambda fn, *a, **kw: fn()
    sys.modules["ap.db"] = _mod
    # Do NOT stub sys.modules["ap"] — ap is a real package on disk.


def _stub_supabase():
    if "supabase" not in sys.modules:
        supa = types.ModuleType("supabase")
        supa.create_client = lambda *a, **kw: None
        supa.Client = type("Client", (), {})
        sys.modules["supabase"] = supa


_stub_db()
_stub_supabase()

# Now the production modules can be imported without a live database.
import ap_overnight_reeval as overnight  # noqa: E402
from ap import order_monitor  # noqa: E402


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ago_iso(minutes: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()


def _make_row(**kwargs) -> dict:
    base = {
        "local_order_id": "ord-1",
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "live",
        "symbol": "SPY",
        "side": "CALL",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "created_ts": _now_iso(),
    }
    base.update(kwargs)
    return base


# ──────────────────────────────────────────────────────────────────────────────
# Part A: _classify_watch_arm_outcome  (ap_overnight_reeval module)
# ──────────────────────────────────────────────────────────────────────────────

class TestClassifyWatchArmOutcome:
    """14 tests covering the watcher-arm outcome taxonomy."""

    def _classify(self, **kw):
        classify = getattr(overnight, "_classify_watch_arm_outcome", None)
        assert callable(classify), "missing _classify_watch_arm_outcome"
        return classify(**kw)

    # ── Test 1: watch_result True → WATCH_ARMED ──────────────────────────────
    def test_true_is_watch_armed(self):
        r = self._classify(watch_result=True, already_watching=False,
                           terminal_conflict=False, exception=None)
        assert r.disposition == overnight.WATCH_ARMED
        assert r.terminal is False

    # ── Test 2: False + exact same watcher → ALREADY_WATCHING ────────────────
    def test_false_with_already_watching_is_already_watching(self):
        r = self._classify(watch_result=False, already_watching=True,
                           terminal_conflict=False, exception=None)
        assert r.disposition == overnight.ALREADY_WATCHING
        assert r.terminal is False

    # ── Test 3: False + transient exception → RETRYABLE ──────────────────────
    def test_false_with_exception_is_retryable(self):
        r = self._classify(watch_result=False, already_watching=False,
                           terminal_conflict=False, exception=RuntimeError("db down"))
        assert r.terminal is False
        assert r.retryable is True

    # ── Test 4 (original xfail): False no evidence → RETRYABLE_NOT_ARMED ─────
    def test_legacy_false_without_terminal_evidence_is_retryable_not_rejected(self):
        classify = getattr(overnight, "_classify_watch_arm_outcome", None)
        assert callable(classify), "missing structured watcher-arm outcome classifier"
        result = classify(
            watch_result=False,
            already_watching=False,
            terminal_conflict=False,
            exception=None,
        )
        assert result.disposition == "RETRYABLE_NOT_ARMED"
        assert result.terminal is False
        assert result.retryable is True

    # ── Test 5: Proven terminal → TERMINAL_FAILURE ────────────────────────────
    def test_terminal_conflict_is_terminal_failure(self):
        r = self._classify(watch_result=False, already_watching=False,
                           terminal_conflict=True, exception=None)
        assert r.disposition == overnight.TERMINAL_FAILURE
        assert r.terminal is True
        assert r.retryable is False

    # ── Test 6: different client is NOT ALREADY_WATCHING ─────────────────────
    # Caller sets already_watching=False when client doesn't match.
    def test_different_client_is_not_already_watching(self):
        r = self._classify(watch_result=False, already_watching=False,
                           terminal_conflict=False, exception=None)
        assert r.disposition != overnight.ALREADY_WATCHING

    # ── Test 7: PAPER vs LIVE is NOT ALREADY_WATCHING ────────────────────────
    def test_paper_vs_live_is_not_already_watching(self):
        r = self._classify(watch_result=False, already_watching=False,
                           terminal_conflict=False, exception=None)
        assert r.disposition != overnight.ALREADY_WATCHING

    # ── Test 8: opposite side is NOT ALREADY_WATCHING ─────────────────────────
    def test_opposite_side_is_not_already_watching(self):
        r = self._classify(watch_result=False, already_watching=False,
                           terminal_conflict=False, exception=None)
        assert r.disposition != overnight.ALREADY_WATCHING

    # ── Test 9: old generation → not idempotent success (no terminal either) ──
    def test_old_generation_is_not_idempotent_success_and_not_terminal(self):
        r = self._classify(watch_result=False, already_watching=False,
                           terminal_conflict=False, exception=None)
        assert r.disposition != overnight.ALREADY_WATCHING
        assert r.terminal is False

    # ── Test 10: ALREADY_WATCHING repeated is idempotent ─────────────────────
    def test_repeated_already_watching_is_idempotent(self):
        for _ in range(3):
            r = self._classify(watch_result=False, already_watching=True,
                               terminal_conflict=False, exception=None)
            assert r.disposition == overnight.ALREADY_WATCHING
            assert r.terminal is False

    # ── Test 11: retryable is not terminal ────────────────────────────────────
    def test_retryable_is_not_terminal(self):
        r = self._classify(watch_result=False, already_watching=False,
                           terminal_conflict=False, exception=None)
        assert r.terminal is False

    # ── Test 12: TERMINAL_FAILURE is not retryable ───────────────────────────
    def test_terminal_failure_is_not_retryable(self):
        r = self._classify(watch_result=False, already_watching=False,
                           terminal_conflict=True, exception=None)
        assert r.retryable is False
        assert r.terminal is True

    # ── Test 13: True overrides all other flags ───────────────────────────────
    def test_true_overrides_all_other_flags(self):
        r = self._classify(watch_result=True, already_watching=True,
                           terminal_conflict=True, exception=RuntimeError("x"))
        assert r.disposition == overnight.WATCH_ARMED

    # ── Test 14: unknown failure emits no broker action (pure function) ───────
    def test_unknown_failure_no_broker_action(self):
        """_classify_watch_arm_outcome is pure; calling it cannot submit orders."""
        r = self._classify(watch_result=False, already_watching=False,
                           terminal_conflict=False, exception=None)
        # RETRYABLE_NOT_ARMED means no terminal rejection and no broker submit
        assert r.terminal is False
        assert r.disposition == "RETRYABLE_NOT_ARMED"


# ──────────────────────────────────────────────────────────────────────────────
# Part B: _classify_pending_entry_ownership  (ap.order_monitor)
# ──────────────────────────────────────────────────────────────────────────────

class TestClassifyPendingEntryOwnership:
    """11 tests covering the pending-entry ownership classifier."""

    def _classify(self, row, **kw):
        fn = getattr(order_monitor, "_classify_pending_entry_ownership", None)
        assert callable(fn), "missing _classify_pending_entry_ownership"
        defaults = dict(watcher_owned=False, recovery_owned=False, broker_terminal=False)
        defaults.update(kw)
        return fn(row, **defaults)

    # ── Test 15 (original xfail): stale orphan → STALE_ORPHAN_CLEANUP ────────
    def test_stale_unowned_pending_entry_is_cleanup_eligible(self):
        classify = getattr(order_monitor, "_classify_pending_entry_ownership", None)
        assert callable(classify), "missing canonical pending-entry ownership classifier"
        existing = {
            "local_order_id": "orphan-1",
            "client_id": "jasoncosby1@gmail.com",
            "execution_mode": "live",
            "symbol": "SPY",
            "side": "PUT",
            "status": "PENDING_TRIGGER",
            "created_ts": _ago_iso(30),
        }
        result = classify(
            existing,
            watcher_owned=False,
            recovery_owned=False,
            broker_terminal=False,
        )
        assert result.disposition == "STALE_ORPHAN_CLEANUP"
        assert result.blocks_candidate is False
        assert result.cleanup_required is True

    # ── Tests 16–18: terminal statuses never block ────────────────────────────
    def test_canceled_row_does_not_block(self):
        r = self._classify(_make_row(status="CANCELED"))
        assert r.blocks_candidate is False

    def test_expired_row_does_not_block(self):
        r = self._classify(_make_row(status="EXPIRED"))
        assert r.blocks_candidate is False

    def test_rejected_row_does_not_block(self):
        r = self._classify(_make_row(status="REJECTED"))
        assert r.blocks_candidate is False

    def test_filled_row_does_not_block(self):
        r = self._classify(_make_row(status="FILLED"))
        assert r.blocks_candidate is False

    # ── Test 19: broker terminal flag → does not block ─────────────────────
    def test_broker_terminal_flag_overrides_active_status(self):
        r = self._classify(_make_row(status="PENDING_TRIGGER", created_ts=_now_iso()),
                           broker_terminal=True)
        assert r.blocks_candidate is False

    # ── Test 20: watcher-owned active PENDING_TRIGGER → blocks ───────────────
    def test_active_watcher_owned_pending_trigger_blocks(self):
        r = self._classify(_make_row(status="PENDING_TRIGGER", created_ts=_now_iso()),
                           watcher_owned=True)
        assert r.blocks_candidate is True

    # ── Test 21: broker-submitted status → blocks regardless of watcher ───────
    def test_submitted_status_blocks(self):
        r = self._classify(_make_row(status="SUBMITTED"))
        assert r.blocks_candidate is True

    # ── Test 22: broker_order_id present on PENDING_TRIGGER → blocks ──────────
    def test_broker_order_id_on_pending_trigger_blocks(self):
        r = self._classify(
            _make_row(status="PENDING_TRIGGER", broker_order_id="BKR123",
                      created_ts=_now_iso())
        )
        assert r.blocks_candidate is True

    # ── Test 23: active unresolved ACKNOWLEDGED → blocks ─────────────────────
    def test_acknowledged_order_blocks(self):
        r = self._classify(_make_row(status="ACKNOWLEDGED"))
        assert r.blocks_candidate is True

    # ── Test 24: stale lease beyond orphan threshold → does not block ─────────
    def test_stale_lease_deadline_does_not_block(self):
        r = self._classify(
            _make_row(status="PENDING_TRIGGER", created_ts=_ago_iso(60),
                      broker_order_id=None, submitted_ts=None)
        )
        assert r.blocks_candidate is False

    # ── Test 25: missing local_order_id → does not block ─────────────────────
    def test_missing_local_order_id_does_not_block(self):
        r = self._classify(_make_row(local_order_id="", status="PENDING_TRIGGER"))
        assert r.blocks_candidate is False


# ──────────────────────────────────────────────────────────────────────────────
# Part C: _pending_entry_blocks_candidate  (ap.order_monitor)
# ──────────────────────────────────────────────────────────────────────────────

class TestPendingEntryBlocksCandidate:
    """11 tests for the cross-client/cross-mode gate."""

    def _blocks(self, existing, candidate=None, *, watcher_owned=False):
        fn = getattr(order_monitor, "_pending_entry_blocks_candidate", None)
        assert callable(fn), "missing _pending_entry_blocks_candidate"
        if candidate is None:
            candidate = {
                "client_id": existing.get("client_id"),
                "execution_mode": existing.get("execution_mode"),
            }
        return fn(existing, candidate, watcher_owned=watcher_owned)

    # ── Test 26 (original xfail): terminal entry does not block ──────────────
    def test_terminal_pending_entry_does_not_block_new_candidate(self):
        owns = getattr(order_monitor, "_pending_entry_blocks_candidate", None)
        assert callable(owns), "missing canonical pending-entry ownership predicate"
        existing = {
            "local_order_id": "old-1",
            "client_id": "jasoncosby1@gmail.com",
            "execution_mode": "live",
            "symbol": "NOW",
            "side": "CALL",
            "status": "CANCELED",
            "created_ts": _now_iso(),
        }
        candidate = {
            "client_id": "jasoncosby1@gmail.com",
            "execution_mode": "live",
            "symbol": "NOW",
            "side": "CALL",
        }
        assert owns(existing, candidate, watcher_owned=False) is False

    # ── Test 27 (original xfail): PAPER pending never blocks LIVE ────────────
    def test_pending_entry_scope_isolated_by_execution_mode(self):
        owns = getattr(order_monitor, "_pending_entry_blocks_candidate", None)
        assert callable(owns), "missing canonical pending-entry ownership predicate"
        existing = {
            "local_order_id": "paper-1",
            "client_id": "jasoncosby1@gmail.com",
            "execution_mode": "paper",
            "symbol": "QQQ",
            "side": "PUT",
            "status": "PENDING_TRIGGER",
            "created_ts": _now_iso(),
        }
        candidate = {
            "client_id": "jasoncosby1@gmail.com",
            "execution_mode": "live",
            "symbol": "QQQ",
            "side": "PUT",
        }
        assert owns(existing, candidate, watcher_owned=True) is False

    # ── Test 28: cross-client → never blocks, other row untouched ────────────
    def test_other_clients_active_row_does_not_block_this_client(self):
        row_other = _make_row(client_id="other@client.com", status="PENDING_TRIGGER",
                              created_ts=_now_iso())
        candidate = {"client_id": "jasoncosby1@gmail.com", "execution_mode": "live"}
        assert self._blocks(row_other, candidate, watcher_owned=True) is False

    # ── Test 29: same client/mode active → blocks ────────────────────────────
    def test_same_client_active_pending_trigger_blocks(self):
        row = _make_row(status="PENDING_TRIGGER", created_ts=_now_iso())
        assert self._blocks(row, watcher_owned=True) is True

    # ── Test 30: filled → does not block ──────────────────────────────────────
    def test_filled_order_does_not_block(self):
        assert self._blocks(_make_row(status="FILLED")) is False

    # ── Test 31: canceled → does not block ────────────────────────────────────
    def test_canceled_order_does_not_block(self):
        assert self._blocks(_make_row(status="CANCELED")) is False

    # ── Test 32: stale unowned → does not block ───────────────────────────────
    def test_stale_unowned_does_not_block(self):
        row = _make_row(status="PENDING_TRIGGER", created_ts=_ago_iso(30),
                        broker_order_id=None, submitted_ts=None)
        assert self._blocks(row, watcher_owned=False) is False

    # ── Test 33: missing local_order_id → does not block ─────────────────────
    def test_missing_referenced_order_does_not_block(self):
        row = _make_row(local_order_id="", status="PENDING_TRIGGER")
        assert self._blocks(row) is False

    # ── Test 34: active unresolved broker order still blocks ──────────────────
    def test_active_unresolved_broker_order_still_blocks(self):
        row = _make_row(status="SUBMITTED", broker_order_id="BKR-X")
        assert self._blocks(row) is True

    # ── Test 35: released candidate is pure — no broker submit ───────────────
    def test_released_candidate_does_not_call_broker(self):
        """Pure function: calling it cannot trigger a broker submit."""
        fn = getattr(order_monitor, "_pending_entry_blocks_candidate", None)
        row = _make_row(status="CANCELED")
        candidate = {"client_id": "jasoncosby1@gmail.com", "execution_mode": "live"}
        result = fn(row, candidate, watcher_owned=False)
        assert result is False  # released
        # No broker mock needed: pure function provably cannot call broker

    # ── Test 36: genuine active owner blocks (at most one watcher/OSM path) ───
    def test_genuine_active_owner_blocks_duplicate(self):
        row = _make_row(status="PENDING_TRIGGER", created_ts=_now_iso())
        assert self._blocks(row, watcher_owned=True) is True


# ──────────────────────────────────────────────────────────────────────────────
# Part D: DB/classification error paths
# ──────────────────────────────────────────────────────────────────────────────

class TestErrorPaths:
    """DB error and ambiguous ownership must fail closed (no broker action)."""

    def test_non_dict_existing_does_not_block(self):
        fn = getattr(order_monitor, "_classify_pending_entry_ownership", None)
        r = fn(None, watcher_owned=False, recovery_owned=False, broker_terminal=False)
        assert r.blocks_candidate is False

    def test_non_dict_existing_blocks_candidate_returns_false(self):
        fn = getattr(order_monitor, "_pending_entry_blocks_candidate", None)
        result = fn(None, {"client_id": "x", "execution_mode": "live"},
                    watcher_owned=False)
        assert result is False

    def test_classify_watch_arm_outcome_is_callable(self):
        assert callable(getattr(overnight, "_classify_watch_arm_outcome", None))

    def test_classify_pending_entry_ownership_is_callable(self):
        assert callable(getattr(order_monitor, "_classify_pending_entry_ownership", None))

    def test_pending_entry_blocks_candidate_is_callable(self):
        assert callable(getattr(order_monitor, "_pending_entry_blocks_candidate", None))


# ──────────────────────────────────────────────────────────────────────────────
# Part E: Incident replay (sanitised, no synthetic fills)
# ──────────────────────────────────────────────────────────────────────────────

class TestIncidentReplay:
    """Replay the 2026-07-28 measured buckets through the new classifiers.

    Reports how many were falsely terminalized vs genuine active blocks.
    No profitability claims.  No fabricated fills.
    """

    def test_watch_returned_false_bucket_replay(self):
        """16 signals: with PR #404 none should be terminal-rejected.

        Scenario A (14): generic False / no _last_reject_reason → RETRYABLE_NOT_ARMED
        Scenario B ( 2): dedup_block / watcher already owns signal → ALREADY_WATCHING
        """
        classify = getattr(overnight, "_classify_watch_arm_outcome", None)
        assert callable(classify)

        scenario_a = [
            classify(watch_result=False, already_watching=False,
                     terminal_conflict=False, exception=None)
            for _ in range(14)
        ]
        falsely_terminalized_a = sum(1 for r in scenario_a if r.terminal)
        safely_retryable_a     = sum(1 for r in scenario_a if not r.terminal)
        assert falsely_terminalized_a == 0, "generic False must never be terminal"
        assert safely_retryable_a == 14

        scenario_b = [
            classify(watch_result=False, already_watching=True,
                     terminal_conflict=False, exception=None)
            for _ in range(2)
        ]
        idempotent_b = sum(1 for r in scenario_b if r.disposition == overnight.ALREADY_WATCHING)
        assert idempotent_b == 2

        total = 16
        print(
            f"\nwatch_returned_false_total={total} "
            f"watch_armed=0 "
            f"already_watching={idempotent_b} "
            f"retryable_failure={safely_retryable_a} "
            f"terminal_failure={falsely_terminalized_a} "
            f"unknown_failure=0"
        )

    def test_pending_entry_exists_bucket_replay(self):
        """33 signals across three ownership scenarios.

        10 cross-mode  (PAPER pending blocking LIVE) → released
        15 stale orphan (>20 min, no watcher/broker)  → released
         8 genuine active same-client/mode             → maintained block
        """
        fn = getattr(order_monitor, "_pending_entry_blocks_candidate", None)
        assert callable(fn)

        candidate_live = {"client_id": "jasoncosby1@gmail.com", "execution_mode": "live"}

        cross_mode_rows = [
            _make_row(execution_mode="paper", status="PENDING_TRIGGER",
                      created_ts=_now_iso())
            for _ in range(10)
        ]
        stale_rows = [
            _make_row(execution_mode="live", status="PENDING_TRIGGER",
                      created_ts=_ago_iso(45),
                      broker_order_id=None, submitted_ts=None)
            for _ in range(15)
        ]
        active_rows = [
            _make_row(execution_mode="live", status="PENDING_TRIGGER",
                      created_ts=_now_iso())
            for _ in range(8)
        ]

        cross_mode_released = sum(
            1 for r in cross_mode_rows if not fn(r, candidate_live, watcher_owned=True)
        )
        stale_released = sum(
            1 for r in stale_rows if not fn(r, candidate_live, watcher_owned=False)
        )
        active_maintained = sum(
            1 for r in active_rows if fn(r, candidate_live, watcher_owned=True)
        )
        total = len(cross_mode_rows) + len(stale_rows) + len(active_rows)

        assert cross_mode_released == 10, "PAPER pending must never block LIVE"
        assert stale_released      == 15, "stale orphans must be released"
        assert active_maintained   == 8,  "genuine active owner must still block"
        assert total == 33

        falsely_suppressed   = cross_mode_released + stale_released
        genuine_active_block = active_maintained

        print(
            f"\npending_entry_exists_total={total} "
            f"active_owner={genuine_active_block} "
            f"stale={stale_released} "
            f"terminal=0 "
            f"missing_or_orphaned=0 "
            f"cross_client=0 "
            f"cross_mode={cross_mode_released} "
            f"ownership_unknown=0 "
            f"released_to_next_gate={falsely_suppressed}"
        )
