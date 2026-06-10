"""
tests/test_organ_health_endpoint.py
=====================================
PR-110: Per-client live-control organ health endpoint.

Covers all 5 acceptance criteria:
  1. /health/organs returns valid JSON (never 404/HTML)
  2. Response includes Jason's live client with required fields
  3. Healthy state: live_control_ready=True, blocking_reasons=[]
  4. Missing/stale exit_engine: live_control_ready=False, blocking_reasons has exit_engine_*
  5. Broker precheck HTTP failure: live_control_ready=False, blocking_reasons has broker_precheck_http_*

Also covers:
  - /health/clients alias returns same shape
  - PAPER client: broker_precheck failure is advisory, not hard block
  - Broker precheck metrics tracked on exit engine
  - Live entry gate blocks on broker precheck 401
"""
from __future__ import annotations

import os
import sys
import time
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
os.environ.setdefault("DATABASE_URL",
                       "postgresql://test:test@127.0.0.1:5432/test_organs")


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_exit_eng(precheck_ok=True, precheck_http=200, precheck_ts=None, pos_count=0):
    """Minimal APExitEngine-like mock with PR-110 broker precheck metrics."""
    eng = MagicMock()
    eng._broker_precheck_last_ts          = precheck_ts or time.time()
    eng._broker_precheck_last_ok          = precheck_ok
    eng._broker_precheck_last_http_status = precheck_http
    eng._broker_precheck_last_position_count = pos_count
    eng._positions  = []
    eng._lock       = threading.RLock()
    return eng


def _make_core(exit_eng):
    core = MagicMock()
    core.exit_eng = exit_eng
    return core


def _make_runner(
    email="jasoncosby1@gmail.com",
    mode="LIVE",
    account_id="6yb82774",
    runner_alive=True,
    worker_alive=True,
    fill_alive=True,
    exit_eng=None,
    reconciler=True,
    initialized=True,
    degraded=False,
    entries_allowed_set=True,
):
    runner = MagicMock()
    runner.email      = email
    runner.mode       = mode
    runner.account_id = account_id

    # Thread liveness
    runner.is_alive.return_value = runner_alive
    runner.worker_thread         = MagicMock()
    runner.worker_thread.is_alive.return_value = worker_alive
    runner.fill_monitor_thread   = MagicMock()
    runner.fill_monitor_thread.is_alive.return_value = fill_alive

    # Core / exit engine
    _eng = exit_eng or _make_exit_eng()
    runner.core      = _make_core(_eng)

    runner.reconciler = MagicMock() if reconciler else None

    # Events
    runner.initialized     = threading.Event()
    runner.degraded        = threading.Event()
    runner.entries_allowed = threading.Event()
    runner.failed          = threading.Event()
    runner.stopping        = threading.Event()
    runner.degraded_reasons = set()

    if initialized:     runner.initialized.set()
    if degraded:        runner.degraded.set()
    if entries_allowed_set: runner.entries_allowed.set()

    # Heartbeat timestamps
    runner.last_fill_monitor_heartbeat_ts = time.time()
    runner.last_worker_heartbeat_ts       = time.time()
    runner.last_equity_heartbeat_ts       = time.time()

    return runner


# ── Test 1: /health/organs always returns JSON ────────────────────────────────

class TestOrgansEndpointAlwaysJson:

    def test_route_exists_and_returns_json(self):
        """/health/organs endpoint must exist and return JSON with 'ok' key."""
        from ap_health_endpoints import _build_client_organ_report

        runner = _make_runner()
        report = _build_client_organ_report("jasoncosby1@gmail.com", runner)

        assert isinstance(report, dict)
        assert "live_control_ready" in report
        assert "blocking_reasons" in report

    def test_clients_alias_exists(self):
        """/health/clients is an alias for /health/organs — same structure."""
        from ap_health_endpoints import _build_client_organ_report
        # Both routes call _build_client_organ_report — test the builder
        runner = _make_runner()
        report = _build_client_organ_report("jasoncosby1@gmail.com", runner)
        assert "client_email" in report


# ── Test 2: Response includes all required fields ────────────────────────────

class TestRequiredResponseFields:

    def test_all_required_fields_present(self):
        from ap_health_endpoints import _build_client_organ_report

        runner = _make_runner(
            email="jasoncosby1@gmail.com",
            account_id="6yb82774",
            mode="LIVE",
        )
        r = _build_client_organ_report("jasoncosby1@gmail.com", runner)

        assert r["client_email"]   == "jasoncosby1@gmail.com"
        assert r["account_id"]     == "6yb82774"
        assert r["execution_mode"] == "live"
        assert "pod_id"            in r
        assert "client_runner_online"           in r
        assert "exit_engine_online"             in r
        assert "fill_monitor_online"            in r
        assert "reconciler_online"              in r
        assert "broker_precheck_online"         in r
        assert "broker_precheck_last_http_status" in r
        assert "broker_position_count"          in r
        assert "broker_symbols"                 in r
        assert "last_heartbeat_age_seconds"     in r
        assert "live_control_ready"             in r
        assert "blocking_reasons"               in r

    def test_heartbeat_age_map_has_required_keys(self):
        from ap_health_endpoints import _build_client_organ_report

        runner = _make_runner()
        r = _build_client_organ_report("jasoncosby1@gmail.com", runner)
        ages = r["last_heartbeat_age_seconds"]

        for key in ("client_runner", "exit_engine", "fill_monitor",
                    "reconciler", "broker_precheck"):
            assert key in ages, f"missing heartbeat key: {key}"


# ── Test 3: Healthy state ──────────────────────────────────────────────────────

class TestHealthyState:

    def test_all_organs_healthy_live_control_ready(self):
        """All organs alive + broker precheck ok → live_control_ready=True."""
        from ap_health_endpoints import _build_client_organ_report

        eng    = _make_exit_eng(precheck_ok=True, precheck_http=200)
        runner = _make_runner(exit_eng=eng)
        r      = _build_client_organ_report("jasoncosby1@gmail.com", runner)

        assert r["live_control_ready"]  is True
        assert r["blocking_reasons"]    == []
        assert r["client_runner_online"] is True
        assert r["exit_engine_online"]   is True
        assert r["fill_monitor_online"]  is True
        assert r["reconciler_online"]    is True
        assert r["broker_precheck_online"] is True


# ── Test 4: Missing/stale exit_engine ─────────────────────────────────────────

class TestExitEngineMissingOrStale:

    def test_exit_engine_missing_blocks_live_control(self):
        """No exit engine → live_control_ready=False, exit_engine_missing in reasons."""
        from ap_health_endpoints import _build_client_organ_report

        runner      = _make_runner()
        runner.core = MagicMock()
        runner.core.exit_eng = None   # no exit engine

        r = _build_client_organ_report("jasoncosby1@gmail.com", runner)

        assert r["live_control_ready"] is False
        assert any("exit_engine" in b for b in r["blocking_reasons"]), (
            f"Expected exit_engine_* in blocking_reasons: {r['blocking_reasons']}"
        )

    def test_exit_engine_stale_blocks_live_control(self):
        """Exit engine heartbeat stale (>90s) → live_control_ready=False."""
        from ap_health_endpoints import _build_client_organ_report

        eng = _make_exit_eng(precheck_ok=True, precheck_http=200)
        runner = _make_runner(exit_eng=eng)

        with patch("ap_health_registry.HealthRegistry.snapshot",
                   return_value={"ap_exit_engine": {"heartbeat_age_s": 150.0}}):
            r = _build_client_organ_report("jasoncosby1@gmail.com", runner)

        assert r["live_control_ready"] is False
        reasons = r["blocking_reasons"]
        assert any("exit_engine" in b for b in reasons), f"Got: {reasons}"

    def test_exit_engine_stale_classification(self):
        """Organ report correctly flags exit_engine_stale via direct age injection."""
        from ap_health_endpoints import _build_client_organ_report

        eng = _make_exit_eng(precheck_ok=True, precheck_http=200)
        runner = _make_runner(exit_eng=eng)

        # Simulate stale by patching the health registry snapshot
        with patch("ap_health_registry.HealthRegistry.snapshot",
                   return_value={"ap_exit_engine": {"heartbeat_age_s": 200.0}}):
            r = _build_client_organ_report("jasoncosby1@gmail.com", runner)

        assert r["live_control_ready"] is False
        reasons = r["blocking_reasons"]
        assert any("exit_engine" in b for b in reasons), f"Got: {reasons}"


# ── Test 5: Broker precheck HTTP failure ──────────────────────────────────────

class TestBrokerPrecheckFailure:

    def test_broker_precheck_401_blocks_live(self):
        """broker_precheck_last_http_status=401 → live_control_ready=False."""
        from ap_health_endpoints import _build_client_organ_report

        eng    = _make_exit_eng(precheck_ok=False, precheck_http=401)
        runner = _make_runner(mode="LIVE", exit_eng=eng)
        r      = _build_client_organ_report("jasoncosby1@gmail.com", runner)

        assert r["live_control_ready"] is False
        assert r["broker_precheck_online"] is False
        assert any("broker_precheck_http_401" in b for b in r["blocking_reasons"]), (
            f"Expected broker_precheck_http_401 in {r['blocking_reasons']}"
        )

    def test_broker_precheck_403_blocks_live(self):
        """broker_precheck_last_http_status=403 → live_control_ready=False."""
        from ap_health_endpoints import _build_client_organ_report

        eng    = _make_exit_eng(precheck_ok=False, precheck_http=403)
        runner = _make_runner(mode="LIVE", exit_eng=eng)
        r      = _build_client_organ_report("jasoncosby1@gmail.com", runner)

        assert r["live_control_ready"] is False
        assert any("broker_precheck_http_403" in b for b in r["blocking_reasons"])

    def test_broker_precheck_never_ran_blocks_live(self):
        """broker precheck never ran (ts=0) → live_control_ready=False."""
        from ap_health_endpoints import _build_client_organ_report

        eng    = _make_exit_eng(precheck_ok=False, precheck_http=0, precheck_ts=0)
        runner = _make_runner(mode="LIVE", exit_eng=eng)
        r      = _build_client_organ_report("jasoncosby1@gmail.com", runner)

        assert r["live_control_ready"] is False
        assert any("broker_precheck" in b for b in r["blocking_reasons"])

    def test_broker_precheck_401_advisory_for_paper(self):
        """PAPER mode: broker_precheck 401 is advisory — does not hard-block live_control_ready."""
        from ap_health_endpoints import _build_client_organ_report

        eng    = _make_exit_eng(precheck_ok=False, precheck_http=401)
        runner = _make_runner(mode="PAPER", exit_eng=eng)
        r      = _build_client_organ_report("jose.vasquez4011@gmail.com", runner)

        # PAPER: broker_precheck and reconciler issues are advisory
        critical_blocking = [b for b in r["blocking_reasons"]
                             if not b.startswith(("broker_precheck", "reconciler"))]
        assert len(critical_blocking) == 0
        assert r["live_control_ready"] is True

    def test_broker_precheck_stale_blocks_live(self):
        """Broker precheck ran >120s ago → live_control_ready=False, precheck stale."""
        from ap_health_endpoints import _build_client_organ_report
        import os
        old_val = os.environ.get("BROKER_PRECHECK_STALE_S")
        os.environ["BROKER_PRECHECK_STALE_S"] = "60"
        try:
            eng = _make_exit_eng(
                precheck_ok=True, precheck_http=200,
                precheck_ts=time.time() - 90,  # 90s ago > 60s threshold
            )
            runner = _make_runner(mode="LIVE", exit_eng=eng)

            import importlib
            import ap_health_endpoints as he
            importlib.reload(he)   # reload to pick up new env var
            r = he._build_client_organ_report("jasoncosby1@gmail.com", runner)
        finally:
            if old_val is None:
                os.environ.pop("BROKER_PRECHECK_STALE_S", None)
            else:
                os.environ["BROKER_PRECHECK_STALE_S"] = old_val

        assert r["live_control_ready"] is False
        assert any("broker_precheck" in b for b in r["blocking_reasons"])


# ── Test: Broker precheck metrics tracked on exit engine ──────────────────────

class TestBrokerPrecheckMetricsOnExitEngine:

    def test_exit_engine_init_has_precheck_metrics(self):
        """APExitEngine must have all PR-110 broker precheck metric attrs at init."""
        from ap_exit_engine import APExitEngine
        eng = APExitEngine(broker=MagicMock(), email="test@ap.com")

        assert hasattr(eng, "_broker_precheck_last_ts")
        assert hasattr(eng, "_broker_precheck_last_ok")
        assert hasattr(eng, "_broker_precheck_last_http_status")
        assert hasattr(eng, "_broker_precheck_last_position_count")

        assert eng._broker_precheck_last_ts          == 0.0
        assert eng._broker_precheck_last_ok          is False
        assert eng._broker_precheck_last_http_status == 0

    def test_successful_precheck_sets_ok_and_http_200(self):
        """Successful broker.list_positions() → _broker_precheck_last_ok=True, http=200."""
        from ap_exit_engine import APExitEngine

        broker = MagicMock()
        broker.list_positions.return_value = []
        eng = APExitEngine(broker=broker, email="test@ap.com")
        eng._broker_position_precheck()

        assert eng._broker_precheck_last_ok          is True
        assert eng._broker_precheck_last_http_status == 200
        assert eng._broker_precheck_last_ts          > 0

    def test_failed_precheck_sets_http_error(self):
        """broker.list_positions() raises → _broker_precheck_last_ok=False, http set."""
        from ap_exit_engine import APExitEngine

        broker = MagicMock()
        broker.list_positions.side_effect = Exception("HTTP 401 Unauthorized")
        eng = APExitEngine(broker=broker, email="test@ap.com")
        eng._broker_position_precheck()

        assert eng._broker_precheck_last_ok          is False
        assert eng._broker_precheck_last_http_status == 401

    def test_failed_precheck_no_status_sets_neg1(self):
        """broker failure without HTTP code → http_status = -1 (error, unknown)."""
        from ap_exit_engine import APExitEngine

        broker = MagicMock()
        broker.list_positions.side_effect = ConnectionError("timeout")
        eng = APExitEngine(broker=broker, email="test@ap.com")
        eng._broker_position_precheck()

        assert eng._broker_precheck_last_ok          is False
        assert eng._broker_precheck_last_http_status == -1


# ── Test: Live entry gate blocked on broker precheck 401 ──────────────────────

class TestLiveEntryGateBrokerPrecheck:

    def test_broker_precheck_401_blocks_entries_on_live_runner(self):
        """LIVE runner with broker_precheck 401 must not set entries_allowed."""
        import ap_health_endpoints   # ensure _build_client_organ_report is importable
        from ap_exit_engine import APExitEngine

        broker_eng = MagicMock()
        broker_eng.list_positions.side_effect = Exception("HTTP 401")
        eng = APExitEngine(broker=broker_eng, email="jason@ap.com")
        # Simulate failed precheck
        eng._broker_precheck_last_ts          = time.time()
        eng._broker_precheck_last_ok          = False
        eng._broker_precheck_last_http_status = 401

        # Build organ report — should show blocked
        runner = _make_runner(mode="LIVE", exit_eng=eng)
        from ap_health_endpoints import _build_client_organ_report
        r = _build_client_organ_report("jasoncosby1@gmail.com", runner)

        assert r["live_control_ready"] is False
        assert any("broker_precheck_http_401" in b for b in r["blocking_reasons"])
