"""
tests/test_simulation.py — Failure Simulation Test Harness
===========================================================
Repeatable tests for all critical failure modes.
Runs against the paper/sim broker — no real orders placed.

Run all:
    python -m pytest tests/test_simulation.py -v

Run one:
    python -m pytest tests/test_simulation.py::test_stale_entry_cancel -v

Environment:
    BOT_MODE=PAPER
    DATABASE_URL=<your supabase URL>
    ALLOW_INTERNAL_MASTER_CONTROL=1   (for tests that build their own MC)
"""

from __future__ import annotations

import os
import sys
import time
import uuid
import threading
import pytest

# DATABASE_URL must be set BEFORE any ap.db import (2026-05-26 audit fix).
# Several tests here import paths that eagerly read DATABASE_URL. Without
# this setdefault, 7 tests ERROR with a misleading runtime error instead
# of running. The value is a non-routable placeholder — tests that need
# a real DB will still skip cleanly via @requires_real_db below.
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_simulation",
)
os.environ.setdefault("ENCRYPTION_KEY", "test-key-for-simulation-2026")

# Make project root importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("BOT_MODE", "PAPER")
os.environ.setdefault("ALLOW_INTERNAL_MASTER_CONTROL", "1")


# ── DB availability marker (2026-05-26 audit fix) ───────────────────────
# Several test classes here (TestStaleEntryCancel, TestPartialFill,
# TestExitEngineDeath, TestDBFailureLiveMode, TestReconciler, plus
# TestKillSwitch / TestDuplicateSignalBurst / TestStartupRecovery which
# construct APMasterControl whose __init__ seeds dedup from the DB) call
# code that hits a real Postgres. Without a reachable DB those tests
# fail with confusing tracebacks and hang on connection retries.
# Probe once at import time and provide a skip marker.
def _real_db_available() -> bool:
    try:
        from ap.db import conn as _conn
        with _conn() as c:
            c.execute("SELECT 1")
        return True
    except Exception:
        return False


_DB_AVAILABLE = _real_db_available()
requires_real_db = pytest.mark.skipif(
    not _DB_AVAILABLE,
    reason="test requires real Postgres (DATABASE_URL must be reachable)",
)


# ═══════════════════════════════════════════════════════════════════════════════
# FIXTURES & HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

class MockBroker:
    """
    Controllable mock broker.
    Set behavior per-order via self.order_outcomes[broker_id] = status_string
    """
    def __init__(self):
        self.order_outcomes: dict[str, str] = {}   # broker_id → "filled"/"canceled"/etc
        self.cancel_results: dict[str, dict] = {}  # broker_id → dict returned by cancel_order
        self.placed_orders: list[dict] = []
        self._next_id = 1000

    def place_order(self, **kwargs):
        from ap.brokers.broker import BrokerOrderResponse
        broker_id = str(self._next_id)
        self._next_id += 1
        self.placed_orders.append({"broker_id": broker_id, **kwargs})
        return BrokerOrderResponse(
            broker_order_id=broker_id,
            status="ACKNOWLEDGED",
            error=None,
            raw={"id": broker_id, "status": "open"},
        )

    def get_order(self, order_id: str) -> dict:
        status = self.order_outcomes.get(str(order_id), "open")
        return {"id": order_id, "status": status, "exec_quantity": 0, "avg_fill_price": 0.0}

    def get_account_equity(self) -> float:
        return 25000.0

    def cancel_order(self, broker_order_id: str) -> dict:
        if str(broker_order_id) in self.cancel_results:
            return self.cancel_results[str(broker_order_id)]
        # Default: cancel succeeds
        status_after = self.order_outcomes.get(str(broker_order_id), "canceled")
        return {"ok": True, "status": status_after, "broker_order_id": broker_order_id}

    def list_positions(self) -> list:
        return []

    def get_quote(self, symbol: str) -> dict:
        return {"bid": 1.00, "ask": 1.05, "last": 1.02, "iv": 0.30, "iv_rank": 50.0}


def make_signal(ticker="TSLA", score=75, side="CALL", signal_id=None) -> dict:
    return {
        "signal_id": signal_id or str(uuid.uuid4()),
        "ticker":    ticker,
        "symbol":    ticker,
        "side":      side,
        "direction": side,
        "score":     score,
        "ev_score":  score,
        "timeframe": "1d",
        "context_score": 5.0,
        "strategy": "322",
        "source": "test",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 1 — Stale Entry Cancel
# ═══════════════════════════════════════════════════════════════════════════════

@requires_real_db
class TestStaleEntryCancel:
    """
    Submit an order. Force it to never fill.
    Confirm: cancel fires, broker confirms cancel, DB updates correctly.
    """

    def test_broker_confirmed_cancel_updates_db(self):
        """
        order_monitor sees a stale ACKNOWLEDGED order → cancels →
        broker confirms CANCELED → OSM transitions to CANCELED.
        """
        from ap.order_state_machine import APOrderStateMachine
        from ap.order_monitor import APOrderMonitor
        from ap.db import insert_order, get_order_by_id, new_local_order_id, run_with_retry

        client_id  = f"test-stale-{uuid.uuid4().hex[:6]}"
        broker     = MockBroker()
        osm        = APOrderStateMachine(client_id=client_id)
        local_id   = new_local_order_id()
        broker_id  = "STALE001"

        # Insert a stale ACKNOWLEDGED entry order (created 20 min ago)
        run_with_retry(lambda: insert_order(
            local_order_id=local_id,
            client_id=client_id,
            kind="ENTRY",
            symbol="TSLA",
            contract="TSLA260418C00400000",
            direction="CALL",
            qty=1,
            limit_price=2.50,
            reserved_cost=250.0,
            status="ACKNOWLEDGED",
            broker_order_id=broker_id,
        ))

        # Force created_ts to be 20 min ago so monitor sees it as stale
        from ap.db import conn
        run_with_retry(lambda: conn().__enter__().execute(
            "UPDATE orders SET created_ts = NOW() - INTERVAL '20 minutes' "
            "WHERE local_order_id=%s", (local_id,)
        ))

        # Broker: order is still open (never filled)
        broker.order_outcomes[broker_id] = "open"
        # Cancel will return confirmed CANCELED
        broker.cancel_results[broker_id] = {"ok": True, "status": "canceled", "broker_order_id": broker_id}

        # Run order monitor for one cycle
        monitor = APOrderMonitor(
            client_id=client_id,
            broker=broker,
            order_state_machine=osm,
            position_manager=None,
        )
        monitor._check_stale_orders()

        # Verify DB updated
        order = run_with_retry(lambda: get_order_by_id(local_id))
        assert order is not None, "Order should exist in DB"
        assert order["status"] == "CANCELED", (
            f"Expected CANCELED, got {order['status']} — "
            "cancel not applied or broker confirmation check failed"
        )

    def test_unconfirmed_cancel_does_not_update_db(self):
        """
        Broker cancel returns unknown status → DB should NOT be set to CANCELED.
        """
        from ap.order_state_machine import APOrderStateMachine
        from ap.order_monitor import APOrderMonitor
        from ap.db import insert_order, get_order_by_id, new_local_order_id, run_with_retry, conn

        client_id = f"test-nocancel-{uuid.uuid4().hex[:6]}"
        broker    = MockBroker()
        osm       = APOrderStateMachine(client_id=client_id)
        local_id  = new_local_order_id()
        broker_id = "NOCANCEL001"

        run_with_retry(lambda: insert_order(
            local_order_id=local_id,
            client_id=client_id,
            kind="ENTRY",
            symbol="AAPL",
            contract="AAPL260418C00200000",
            direction="CALL",
            qty=1,
            limit_price=1.00,
            reserved_cost=100.0,
            status="ACKNOWLEDGED",
            broker_order_id=broker_id,
        ))
        run_with_retry(lambda: conn().__enter__().execute(
            "UPDATE orders SET created_ts = NOW() - INTERVAL '20 minutes' "
            "WHERE local_order_id=%s", (local_id,)
        ))

        broker.order_outcomes[broker_id] = "open"
        # Cancel returns ambiguous / unknown status
        broker.cancel_results[broker_id] = {"ok": False, "status": "unknown", "broker_order_id": broker_id}

        alerts = []
        monitor = APOrderMonitor(
            client_id=client_id,
            broker=broker,
            order_state_machine=osm,
            position_manager=None,
            alert_fn=lambda msg: alerts.append(msg),
        )
        monitor._check_stale_orders()

        order = run_with_retry(lambda: get_order_by_id(local_id))
        assert order["status"] == "ACKNOWLEDGED", (
            f"DB should stay ACKNOWLEDGED when broker cancel not confirmed, got {order['status']}"
        )
        assert any("NOT broker-confirmed" in a for a in alerts), (
            "Should have fired an alert for unconfirmed cancel"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 2 — Partial Fill
# ═══════════════════════════════════════════════════════════════════════════════

@requires_real_db
class TestPartialFill:
    """Simulate partial fill — verify PARTIAL_FILL written, exit still works."""

    def test_partial_fill_status_written(self):
        from ap.fill_monitor import check_order_status
        from ap.db import insert_order, get_order_by_id, new_local_order_id, run_with_retry

        client_id = f"test-partial-{uuid.uuid4().hex[:6]}"
        broker    = MockBroker()
        local_id  = new_local_order_id()
        broker_id = "PARTIAL001"

        run_with_retry(lambda: insert_order(
            local_order_id=local_id,
            client_id=client_id,
            kind="ENTRY",
            symbol="AMZN",
            contract="AMZN260418C00250000",
            direction="CALL",
            qty=2,
            limit_price=3.00,
            reserved_cost=600.0,
            status="ACKNOWLEDGED",
            broker_order_id=broker_id,
        ))

        # Broker says partially_filled
        broker.order_outcomes[broker_id] = "partially_filled"

        order_row = run_with_retry(lambda: get_order_by_id(local_id))
        result = check_order_status(order_row, broker)

        assert result["status"] == "PARTIAL_FILL", (
            f"Expected PARTIAL_FILL from fill_monitor, got {result['status']}"
        )
        assert "ACK" not in result["status"], "Legacy ACK must not appear"
        assert "PARTIAL" == result["status"] or result["status"] == "PARTIAL_FILL", \
               "Must be canonical PARTIAL_FILL"
        # Exact check:
        assert result["status"] == "PARTIAL_FILL"


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 3 — Kill Switch
# ═══════════════════════════════════════════════════════════════════════════════

@requires_real_db
class TestKillSwitch:
    """Trigger daily loss limit — entries blocked, exits still pass."""

    def _make_mc(self):
        from ap_master_control import APMasterControl
        mc = APMasterControl(
            mode="paper",
            client_id="test-ks",
            score_floor=50.0,
            max_daily_loss=-100.0,   # very tight: lose $100 → kill switch
            max_positions=10,
            max_capital_pct=0.40,
        )
        return mc

    def test_entries_blocked_after_daily_loss(self):
        mc  = self._make_mc()
        sig = make_signal(score=85)

        # Simulate daily loss exceeded
        mc.daily_pnl = -150.0   # below -100 threshold
        mc.check_daily_loss_breach()  # trigger kill switch evaluation (renamed from _check_daily_loss)

        decision = mc.evaluate(sig, client_id="test-ks")
        assert not decision.ok, "Entry should be blocked when daily loss exceeded"
        assert "kill_switch" in decision.reason.lower() or "daily_loss" in decision.reason.lower(), \
            f"Wrong block reason: {decision.reason}"

    def test_exit_not_blocked_by_kill_switch(self):
        from ap_exit_engine import APExitEngine
        broker = MockBroker()
        engine = APExitEngine(broker=broker, email="test-ks")

        # Kill switch engaged
        engine._kill_switch = True

        # Build a mock exit signal with decision_type containing "STOP_HIT"
        mock_exit = {
            "position_id": "pos-001",
            "ticker": "TSLA",
            "contract": "TSLA260418C00400000",
            "decision_type": "STOP_HIT",
            "reason": "stop loss triggered",
            "qty": 1,
        }
        # _is_protective_exit is a module-level function in ap_exit_engine
        # (renamed from APExitEngine._is_protective method). Signature
        # takes a reason string, not a dict.
        from ap_exit_engine import _is_protective_exit
        assert _is_protective_exit(mock_exit["reason"] or mock_exit["decision_type"]), (
            "STOP_HIT should be treated as protective exit, allowed through kill switch"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 4 — Exit Engine Death
# ═══════════════════════════════════════════════════════════════════════════════

@requires_real_db
class TestExitEngineDeath:
    """Stop exit engine → exit_engine_down=True → master_control blocks entries."""

    def test_entries_blocked_when_exit_engine_down(self):
        from ap_master_control import APMasterControl

        mc = APMasterControl(
            mode="paper",
            client_id="test-eed",
            score_floor=50.0,
            max_positions=10,
            max_capital_pct=0.40,
        )
        mc.exit_engine_down = True   # simulates self_healing detection

        sig      = make_signal(score=80)
        decision = mc.evaluate(sig, client_id="test-eed")

        assert not decision.ok, "Entries must be blocked when exit engine is down"
        assert "exit_engine_down" in decision.reason, (
            f"Wrong block reason: {decision.reason}"
        )

    def test_entries_unblocked_when_exit_engine_recovers(self):
        from ap_master_control import APMasterControl

        mc = APMasterControl(
            mode="paper",
            client_id="test-eed2",
            score_floor=50.0,
            max_positions=10,
            max_capital_pct=0.40,
        )
        mc.exit_engine_down = False  # recovered

        sig      = make_signal(score=80)
        decision = mc.evaluate(sig, client_id="test-eed2")

        # Should pass system gates (may still be blocked by other gates — that's ok)
        # Just confirm it's NOT blocked by exit_engine_down
        if not decision.ok:
            assert "exit_engine_down" not in decision.reason, (
                "Should not be blocked by exit_engine_down when engine is healthy"
            )


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 5 — DB Failure in LIVE mode
# ═══════════════════════════════════════════════════════════════════════════════

@requires_real_db
class TestDBFailureLiveMode:
    """DB unavailable at startup → LIVE mode must refuse to start."""

    def test_live_mode_aborts_on_db_failure(self, monkeypatch):
        import ap.db as db_module

        # Patch conn() to raise immediately
        def _bad_conn():
            raise Exception("Connection refused (simulated)")

        monkeypatch.setattr(db_module, "_pool", None)  # clear any pool
        monkeypatch.setenv("BOT_MODE", "LIVE")

        # Patch the pool creation to fail
        original_init_pool = db_module._init_pool
        def _fail_pool():
            raise Exception("Simulated DB failure")
        monkeypatch.setattr(db_module, "_init_pool", _fail_pool)

        with pytest.raises((RuntimeError, Exception)) as exc_info:
            db_module.init_db()

        assert "LIVE" in str(exc_info.value) or "Postgres" in str(exc_info.value) or \
               "unavailable" in str(exc_info.value) or "startup aborted" in str(exc_info.value), \
               f"Expected a LIVE-mode abort error, got: {exc_info.value}"

    def test_paper_mode_survives_db_failure(self, monkeypatch):
        import ap.db as db_module

        monkeypatch.setenv("BOT_MODE", "PAPER")
        monkeypatch.setattr(db_module, "_pool", None)

        def _fail_pool():
            raise Exception("Simulated DB failure")
        monkeypatch.setattr(db_module, "_init_pool", _fail_pool)

        # Should NOT raise in PAPER mode
        try:
            db_module.init_db()
        except RuntimeError as e:
            if "LIVE mode startup aborted" in str(e):
                pytest.fail("init_db raised LIVE-mode error in PAPER mode")
            # Other errors (connection pool issues) are ok — just log them


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 6 — Duplicate Signal Burst
# ═══════════════════════════════════════════════════════════════════════════════

@requires_real_db
class TestDuplicateSignalBurst:
    """Send same signal 5 times — only 1 trade taken."""

    def test_dedup_blocks_all_but_first(self):
        from ap_master_control import APMasterControl

        mc = APMasterControl(
            mode="paper",
            client_id="test-dedup",
            score_floor=50.0,
            max_positions=10,
            max_capital_pct=0.40,
        )

        sig_id = str(uuid.uuid4())
        sig    = make_signal(score=75, signal_id=sig_id)

        results = []
        for _ in range(5):
            # Must use the SAME signal_id each time to test signal dedup
            d = mc.evaluate(dict(sig), client_id="test-dedup")
            results.append(d.ok)

        approved = [r for r in results if r]
        blocked  = [r for r in results if not r]

        # First one may or may not pass other gates — but only 1 should ever pass
        # for the same signal_id
        assert len(approved) <= 1, (
            f"Dedup failed: {len(approved)} signals approved out of 5 identical signals"
        )

    def test_same_setup_dedup_blocks_second(self):
        """Same ticker+direction+timeframe should be blocked as duplicate setup."""
        from ap_master_control import APMasterControl

        mc = APMasterControl(
            mode="paper",
            client_id="test-dedup2",
            score_floor=50.0,
            max_positions=10,
            max_capital_pct=0.40,
        )

        # Send two signals with different IDs but same setup
        sig1 = make_signal(ticker="NVDA", side="CALL", score=80)
        sig2 = make_signal(ticker="NVDA", side="CALL", score=82)  # different ID, same setup
        sig2["signal_id"] = str(uuid.uuid4())

        d1 = mc.evaluate(dict(sig1), client_id="test-dedup2")
        d2 = mc.evaluate(dict(sig2), client_id="test-dedup2")

        if d1.ok:
            # First passed system gates — second must be blocked by setup dedup
            assert not d2.ok, (
                "Second signal with same ticker/direction/timeframe should be blocked by setup dedup"
            )
            assert "dedup" in d2.reason.lower() or "duplicate" in d2.reason.lower(), (
                f"Wrong block reason on dedup: {d2.reason}"
            )


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 7 — Reconciler (broker vs DB mismatch)
# ═══════════════════════════════════════════════════════════════════════════════

@requires_real_db
class TestReconciler:
    """DB says open → broker says filled → reconciler auto-corrects."""

    def test_auto_correct_db_open_when_broker_filled(self):
        from ap.order_state_machine import APOrderStateMachine
        from ap.db import insert_order, get_order_by_id, new_local_order_id, run_with_retry
        from ap_reconciler import APBrokerReconciler

        client_id = f"test-recon-{uuid.uuid4().hex[:6]}"
        broker    = MockBroker()
        osm       = APOrderStateMachine(client_id=client_id)
        local_id  = new_local_order_id()
        broker_id = "RECON001"

        # DB: order is ACKNOWLEDGED (open)
        run_with_retry(lambda: insert_order(
            local_order_id=local_id,
            client_id=client_id,
            kind="ENTRY",
            symbol="GOOGL",
            contract="GOOGL260418C00175000",
            direction="CALL",
            qty=1,
            limit_price=5.00,
            reserved_cost=500.0,
            status="ACKNOWLEDGED",
            broker_order_id=broker_id,
        ))

        # Broker: order is actually filled
        broker.order_outcomes[broker_id] = "filled"

        alerts = []
        reconciler = APBrokerReconciler(
            broker=broker,
            client_id=client_id,
            osm=osm,
            pm=None,
            alert_fn=lambda msg: alerts.append(msg),
        )

        summary = reconciler.run_once()

        order = run_with_retry(lambda: get_order_by_id(local_id))
        assert order["status"] == "FILLED", (
            f"Reconciler should have auto-corrected to FILLED, got {order['status']}"
        )
        assert summary["orders_corrected"] >= 1, "Reconciler should report 1 correction"
        assert any("RECONCILE_AUTO_CORRECT" in a for a in alerts), (
            "Should have fired RECONCILE_AUTO_CORRECT alert"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 8 — Startup Recovery
# ═══════════════════════════════════════════════════════════════════════════════

@requires_real_db
class TestStartupRecovery:
    """Restart recovery re-seeds dedup from DB and restores position count."""

    def test_dedup_reseeded_from_db(self):
        from ap_master_control import APMasterControl
        from ap.db import run_with_retry, conn
        from ap_recovery import APStartupRecovery

        client_id = f"test-recovery-{uuid.uuid4().hex[:6]}"
        broker    = MockBroker()
        mc = APMasterControl(
            mode="paper",
            client_id=client_id,
            score_floor=50.0,
            max_positions=10,
            max_capital_pct=0.40,
        )

        # Seed trade_queue with a recent signal
        sig_id = str(uuid.uuid4())
        run_with_retry(lambda: conn().__enter__().execute(
            """
            INSERT INTO trade_queue
                (signal_id, client_id, ticker, direction, timeframe, status, created_ts, payload)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s)
            ON CONFLICT DO NOTHING
            """,
            (sig_id, client_id, "META", "CALL", "1d", "DONE", "{}"),
        ))

        recovery = APStartupRecovery(
            client_id=client_id,
            broker=broker,
            osm=None,
            pm=None,
            master_control=mc,
        )
        result = recovery.run()

        assert result["dedup_seeded"] >= 1, (
            "Recovery should have reseeded at least 1 signal into dedup"
        )
        # Confirm the seen_signals set now has this signal
        assert f"sig:{sig_id}:{client_id}" in mc._seen_signals, (
            "signal_id should be in master_control._seen_signals after recovery"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# RENAME VERIFICATION (no DB required)
# ═══════════════════════════════════════════════════════════════════════════════
#
# These tests verify the production method renames the suite must track,
# WITHOUT requiring a live Postgres. The previous TestKillSwitch /
# TestExitEngineDeath tests for these renames hang in CI because their
# fixtures call APMasterControl.__init__ which seeds dedup from the DB.
#
# Production renames (audit dated 2026-05-26):
#   ap_master_control.APMasterControl._check_daily_loss
#     -> check_daily_loss_breach (public, no underscore)
#   ap_exit_engine.APExitEngine._is_protective (method)
#     -> ap_exit_engine._is_protective_exit (module-level function)

def test_check_daily_loss_breach_method_exists():
    """check_daily_loss_breach replaced _check_daily_loss in production."""
    from ap_master_control import APMasterControl
    assert hasattr(APMasterControl, "check_daily_loss_breach"), (
        "Production renamed _check_daily_loss -> check_daily_loss_breach; "
        "tests must follow."
    )
    # Confirm the old name is genuinely gone so we don't accept stale code.
    assert not hasattr(APMasterControl, "_check_daily_loss"), (
        "Old method name _check_daily_loss still exists — rename incomplete."
    )


def test_is_protective_exit_function_exists_and_classifies_correctly():
    """_is_protective_exit replaced APExitEngine._is_protective.

    The new signature takes a reason string (not a dict) and returns True
    for protective exit reasons. Verifies the renamed kill-switch carve-out
    still treats STOP_HIT / stop-loss reasons as protective.
    """
    from ap_exit_engine import _is_protective_exit
    # Reasons that MUST classify as protective (allowed through kill switch)
    assert _is_protective_exit("stop loss triggered")
    assert _is_protective_exit("STOP_HIT")
    assert _is_protective_exit("EOD FORCE CLOSE")
    assert _is_protective_exit("MAX_LOSS reached")
    # Reasons that MUST NOT classify as protective
    assert not _is_protective_exit("manual close")
    assert not _is_protective_exit("")


# ═══════════════════════════════════════════════════════════════════════════════
# RUN GUARD
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import subprocess
    result = subprocess.run(
        ["python", "-m", "pytest", __file__, "-v", "--tb=short"],
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    )
    sys.exit(result.returncode)
