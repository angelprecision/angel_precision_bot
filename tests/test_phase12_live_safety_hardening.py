"""
PR #30 tests: live-safety hardening + config consistency.

Covers the five fixes:
  Fix 1: MAX_CONTRACTS default = 15 (code + docs)
  Fix 2: naming/config consistency (stale docs / 25s hard ceiling)
  Fix 3: per-client broker-error circuit breaker
  Fix 4: same-symbol + same-sector exposure caps
  Fix 5: reconciler heartbeat + RECONCILER_STALE alert

Run:
    pytest tests/test_phase12_live_safety_hardening.py -xvs
"""
from __future__ import annotations

import os
import re
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
EXEC_SRC = (REPO_ROOT / "ap" / "execution.py").read_text()
OM_SRC   = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
CS_SRC   = (REPO_ROOT / "ap" / "contract_selector.py").read_text()
RM_SRC   = (REPO_ROOT / "ap_intelligence" / "agents" / "ap_risk_manager.py").read_text()
SP_SRC   = (REPO_ROOT / "ap_intelligence" / "ap_signal_pipeline.py").read_text()

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_phase12",
)


# ============================================================
# FIX 1: MAX_CONTRACTS = 15
# ============================================================

class TestFix1MaxContracts:
    def test_execution_default_is_15(self):
        assert re.search(
            r'MAX_CONTRACTS\s*=\s*int\(os\.getenv\(\s*"MAX_CONTRACTS"\s*,\s*"15"\s*\)\)',
            EXEC_SRC,
        ), "ap/execution.py must default MAX_CONTRACTS to 15"

    def test_contract_selector_default_is_15(self):
        assert re.search(
            r'_MAX_CONTRACTS_HARD_CAP\s*=\s*int\(os\.getenv\(\s*"MAX_CONTRACTS"\s*,\s*"15"\s*\)\)',
            CS_SRC,
        ), "ap/contract_selector.py must default MAX_CONTRACTS to 15"

    def test_risk_manager_default_is_15(self):
        assert "max_contracts_hard_cap: int = 15," in RM_SRC, \
            "ap_risk_manager default must be 15"

    def test_signal_pipeline_default_is_15(self):
        assert re.search(
            r'max_contracts_hard_cap\s*=\s*int\(os\.getenv\(\s*"MAX_CONTRACTS"\s*,\s*"15"\s*\)\)',
            SP_SRC,
        ), "ap_signal_pipeline must default MAX_CONTRACTS to 15"

    def test_30k_at_308_still_9_contracts(self):
        """Acceptance: $30K @ $3.08 stays 9 (not capped by 15)."""
        from ap.execution import _size_position
        qty, budget, reason = _size_position(30_000.0, 3.08)
        assert qty == 9
        assert reason == "ACCOUNT_EQUITY_PCT"
        assert budget == pytest.approx(3_000.0)

    def test_large_account_capped_at_15(self):
        """Acceptance: $100K @ $5 = raw 20, capped at 15."""
        from ap.execution import _size_position
        qty, budget, reason = _size_position(100_000.0, 5.00)
        assert qty == 15
        assert reason == "MAX_CONTRACTS_CAP"

    def test_no_stale_50_or_10_default(self):
        """No file documents 50 or 10 as the current default."""
        for path in (REPO_ROOT / "ap" / "execution.py",
                     REPO_ROOT / "ap" / "contract_selector.py",
                     REPO_ROOT / "ap_intelligence" / "agents" / "ap_risk_manager.py",
                     REPO_ROOT / "ap_intelligence" / "ap_signal_pipeline.py"):
            src = path.read_text()
            # Look for active defaults (NOT historical comments)
            assert not re.search(
                r'getenv\(\s*"MAX_CONTRACTS"\s*,\s*"(?:50|10|6)"\s*\)',
                src,
            ), f"{path.name} still has stale MAX_CONTRACTS default"


# ============================================================
# FIX 2: naming/config consistency
# ============================================================

class TestFix2NamingConsistency:
    def test_deprecated_constant_marked(self):
        """ENTRY_LIMIT_MAX_AGE_SECONDS must be marked deprecated in
        ap/order_monitor.py; setting it must have no decision-path effect."""
        idx = OM_SRC.find("ENTRY_LIMIT_MAX_AGE_SECONDS")
        assert idx > 0
        # Within the 600 chars BEFORE the declaration, the word 'deprecated' must appear.
        window = OM_SRC[max(0, idx - 600):idx + 200]
        assert "deprecated" in window.lower(), \
            "ENTRY_LIMIT_MAX_AGE_SECONDS must be commented as deprecated"

    def test_canonical_phase2_constants_exist(self):
        for kw in ("ENTRY_REEVAL_AGE_SECONDS",
                   "ENTRY_MAX_AGE_NORMAL",
                   "ENTRY_MAX_AGE_APLUS"):
            assert kw in OM_SRC, f"{kw} must be declared in ap/order_monitor.py"

    def test_runbook_marked_historical(self):
        path = REPO_ROOT / "P1_ENTRY_FIX_RUNBOOK.md"
        assert path.exists()
        text = path.read_text()
        assert "HISTORICAL DOCUMENT" in text, \
            "P1_ENTRY_FIX_RUNBOOK.md must carry a HISTORICAL DOCUMENT header"
        # It should reference the canonical Phase 2 constants in its
        # corrections table.
        assert "ENTRY_REEVAL_AGE_SECONDS" in text
        assert "ENTRY_MAX_AGE_NORMAL" in text

    def test_no_active_doc_says_max_contracts_50(self):
        """Spec: 'No file still documents 50 or 10 as the default operational
        max unless clearly marked as historical.'"""
        for path in REPO_ROOT.rglob("*.md"):
            text = path.read_text()
            # If the file mentions MAX_CONTRACTS=50 or MAX_CONTRACTS=10, it
            # must be in a HISTORICAL block.
            for needle in ("MAX_CONTRACTS=50", "MAX_CONTRACTS=10",
                           "MAX_CONTRACTS = 50", "MAX_CONTRACTS = 10"):
                if needle in text:
                    assert "HISTORICAL" in text, (
                        f"{path} mentions {needle!r} but is not marked HISTORICAL"
                    )


# ============================================================
# FIX 3: broker-error circuit breaker
# ============================================================

@pytest.fixture(autouse=True)
def _reset_breaker_state():
    """Tests must start with a clean breaker."""
    from ap import safety_circuit
    safety_circuit._reset_all_for_tests()
    yield
    safety_circuit._reset_all_for_tests()


class TestFix3BrokerCircuitBreaker:
    def test_threshold_and_window_defaults(self):
        import ap.safety_circuit as sc
        assert sc.BROKER_ERROR_THRESHOLD == 5
        assert sc.BROKER_ERROR_WINDOW_SECS == 120
        assert sc.BROKER_ERROR_CLEAR_AFTER_SECS == 300

    def test_below_threshold_does_not_open(self):
        from ap import safety_circuit as sc
        for _ in range(4):
            assert sc.record_broker_error("client-A", "tcp reset") is False
        assert sc.is_open("client-A") is False

    def test_threshold_burst_opens_circuit(self):
        from ap import safety_circuit as sc
        opens = 0
        for _ in range(5):
            if sc.record_broker_error("client-A", "broker 500"):
                opens += 1
        assert opens == 1, "open event must fire exactly once on the threshold call"
        assert sc.is_open("client-A") is True

    def test_open_event_fires_only_once(self):
        from ap import safety_circuit as sc
        for _ in range(5):
            sc.record_broker_error("client-A", "broker 500")
        # Additional errors don't re-fire the open event.
        for _ in range(10):
            assert sc.record_broker_error("client-A", "broker 500") is False
        assert sc.is_open("client-A") is True

    def test_clear_breaker_resets_state(self):
        from ap import safety_circuit as sc
        for _ in range(5):
            sc.record_broker_error("client-A", "broker 500")
        assert sc.is_open("client-A")
        sc.clear_breaker("client-A")
        assert sc.is_open("client-A") is False
        # Snapshot reflects cleared state
        snap = sc.snapshot("client-A")
        assert snap["broker_error_count"] == 0
        assert snap["open"] is False

    def test_snapshot_carries_all_fields(self):
        from ap import safety_circuit as sc
        for _ in range(5):
            sc.record_broker_error("client-A", "broker 500", op_kind="submit")
        snap = sc.snapshot("client-A")
        assert snap["client_id"] == "client-A"
        assert snap["open"] is True
        assert snap["broker_error_count"] >= 5
        assert snap["window_secs"] == 120
        assert snap["threshold"] == 5
        assert snap["last_error_sample"] == "broker 500"

    def test_client_isolation(self):
        """Errors for client A must NOT open the breaker for client B."""
        from ap import safety_circuit as sc
        for _ in range(5):
            sc.record_broker_error("client-A", "broker 500")
        assert sc.is_open("client-A") is True
        assert sc.is_open("client-B") is False

    def test_auto_clear_after_silence(self, monkeypatch):
        """Auto-clear after BROKER_ERROR_CLEAR_AFTER_SECS of silence."""
        from ap import safety_circuit as sc
        # Open the circuit
        for _ in range(5):
            sc.record_broker_error("client-A", "broker 500")
        assert sc.is_open("client-A")
        # Force the clock forward by patching _now
        original_now = sc._now
        monkeypatch.setattr(sc, "_now",
                            lambda: original_now() + sc.BROKER_ERROR_CLEAR_AFTER_SECS + 1)
        # Next is_open() call auto-clears
        assert sc.is_open("client-A") is False


class TestFix3ProcessSignalIntegration:
    def test_process_signal_returns_circuit_open_when_breaker_open(self):
        """Acceptance: process_signal must return broker_error_circuit_open
        before broker submit when the breaker is open."""
        # The source-shape proof: process_signal imports safety_circuit and
        # returns broker_error_circuit_open before _submit_order_with_retry.
        idx_check  = EXEC_SRC.find("is_open(client_id)")
        # Use the CALL SITE specifically (with float(submit_limit) arg), not
        # the def of _submit_order_with_retry above.
        idx_submit = EXEC_SRC.find(
            "_submit_order_with_retry(broker, symbol, contract, qty, float(submit_limit))"
        )
        assert idx_check > 0, "process_signal must call safety_circuit.is_open"
        assert idx_submit > 0
        assert idx_check < idx_submit, (
            "breaker check must run BEFORE _submit_order_with_retry"
        )
        # Returns the canonical error string.
        assert 'error": "broker_error_circuit_open"' in EXEC_SRC

    def test_broker_submit_failure_records_error(self):
        """A broker_rejected path must feed safety_circuit.record_broker_error."""
        idx_rej = EXEC_SRC.find('"ORDER_REJECTED"')
        assert idx_rej > 0
        window = EXEC_SRC[idx_rej:idx_rej + 800]
        assert "record_broker_error" in window, \
            "ORDER_REJECTED path must feed the circuit breaker"

    def test_breaker_does_not_block_exits(self):
        """The breaker is only consulted in process_signal (entry path).
        Exits, force-exits, reconciler, and admin close-all paths must NOT
        check the breaker.
        """
        # Find all is_open() call sites in the codebase.
        calls = []
        for path in (REPO_ROOT / "ap").rglob("*.py"):
            src = path.read_text()
            for m in re.finditer(r"\bis_open\(", src):
                # Surrounding context: 200 chars
                ctx = src[max(0, m.start() - 200):m.end() + 200]
                calls.append((path.name, ctx))
        # We expect the call ONLY in execution.py (process_signal) and
        # safety_circuit.py (definition).
        for name, ctx in calls:
            assert name in ("execution.py", "safety_circuit.py",
                            # also acceptable test files in the future
                            ), f"unexpected is_open() call in {name}: {ctx[:80]}"


# ============================================================
# FIX 4: exposure caps
# ============================================================

class TestFix4ExposureGate:
    def test_module_imports_and_constants(self):
        from ap import exposure_gate as eg
        assert eg.MAX_OPEN_PER_SYMBOL == 1
        assert eg.MAX_OPEN_PER_SECTOR == 2
        assert "TECH" in set(eg.SECTOR_MAP.values())

    def test_sector_lookup_canonicalizes_case(self):
        from ap.exposure_gate import get_sector
        assert get_sector("qcom") == "TECH"
        assert get_sector("QCOM") == "TECH"
        assert get_sector(" tsla ") == "AUTO"
        assert get_sector("UNKNOWN_TICKER") is None
        assert get_sector("") is None
        assert get_sector(None) is None

    def test_no_open_positions_allows_entry(self):
        from ap.exposure_gate import check_exposure
        r = check_exposure("client-A", "QCOM", open_positions=[])
        assert r["ok"] is True
        assert r["error"] is None
        assert r["open_same_symbol"] == 0
        assert r["sector"] == "TECH"

    def test_same_symbol_already_open_blocks(self):
        from ap.exposure_gate import check_exposure
        r = check_exposure(
            "client-A", "QCOM",
            open_positions=[{"underlying": "QCOM", "symbol": "QCOM250523C00185000"}],
        )
        assert r["ok"] is False
        assert r["error"] == "symbol_exposure_limit"
        assert r["reason_code"] == "SYMBOL_EXPOSURE_LIMIT"
        assert r["open_same_symbol"] == 1

    def test_different_symbol_in_same_sector_below_cap_allows(self):
        from ap.exposure_gate import check_exposure
        r = check_exposure(
            "client-A", "MSFT",
            open_positions=[{"underlying": "AAPL", "symbol": "AAPL..."}],
        )
        assert r["ok"] is True
        assert r["open_same_symbol"] == 0
        assert r["open_same_sector"] == 1  # AAPL is TECH, below cap 2

    def test_third_same_sector_blocks(self):
        from ap.exposure_gate import check_exposure
        r = check_exposure(
            "client-A", "NVDA",
            open_positions=[
                {"underlying": "AAPL"}, {"underlying": "MSFT"},
            ],
        )
        assert r["ok"] is False
        assert r["error"] == "sector_exposure_limit"
        assert r["reason_code"] == "SECTOR_EXPOSURE_LIMIT"
        assert r["open_same_sector"] == 2

    def test_unmapped_symbol_does_not_crash(self):
        """If the candidate symbol has no sector mapping, sector cap is
        skipped and same-symbol cap still applies."""
        from ap.exposure_gate import check_exposure
        r = check_exposure(
            "client-A", "WEIRDTICKER",
            open_positions=[{"underlying": "AAPL"}, {"underlying": "MSFT"}],
        )
        # WEIRDTICKER is unmapped -> sector is None -> sector cap doesn't apply
        # Same-symbol count is 0 -> allow.
        assert r["ok"] is True
        assert r["sector"] is None
        assert r["open_same_sector"] is None

    def test_unmapped_symbol_with_same_symbol_open_still_blocks(self):
        from ap.exposure_gate import check_exposure
        r = check_exposure(
            "client-A", "WEIRDTICKER",
            open_positions=[{"underlying": "WEIRDTICKER"}],
        )
        assert r["ok"] is False
        assert r["error"] == "symbol_exposure_limit"

    def test_db_failure_does_not_block_entry(self, monkeypatch):
        """If the positions read fails, the gate must fail-OPEN (allow).
        process_signal has its own DB-error handling further down."""
        from ap import exposure_gate as eg

        def broken_count(client_id, conn_factory=None):
            raise RuntimeError("db connection lost")
        monkeypatch.setattr(eg, "_count_open_positions", broken_count)

        r = eg.check_exposure("client-A", "QCOM")
        assert r["ok"] is True   # fail-open
        assert r["open_same_symbol"] == 0


class TestFix4ProcessSignalIntegration:
    def test_process_signal_calls_exposure_gate(self):
        assert "from ap import exposure_gate as _eg" in EXEC_SRC
        assert "_eg.check_exposure(client_id, symbol)" in EXEC_SRC

    def test_exposure_gate_runs_before_equity_reserve(self):
        idx_eg     = EXEC_SRC.find("_eg.check_exposure(")
        idx_reserve = EXEC_SRC.find("reserve_equity_if_available(")
        assert idx_eg > 0 and idx_reserve > 0
        assert idx_eg < idx_reserve, \
            "exposure_gate must run BEFORE equity reserve"

    def test_exposure_block_releases_symbol_lock(self):
        """When the gate blocks, we must release the symbol lock so the
        next signal isn't blocked by a leaked lock."""
        idx = EXEC_SRC.find('"symbol_exposure_limit"')
        # Block is wrapped — search a wide window after the gate call.
        idx_call = EXEC_SRC.find("_eg.check_exposure(")
        window = EXEC_SRC[idx_call:idx_call + 1500]
        assert "release_symbol_lock(client_id, symbol)" in window


# ============================================================
# FIX 5: reconciler heartbeat
# ============================================================

@pytest.fixture(autouse=True)
def _reset_heartbeat_state():
    from ap import reconciler_heartbeat
    reconciler_heartbeat._reset_all_for_tests()
    yield
    reconciler_heartbeat._reset_all_for_tests()


class TestFix5ReconcilerHeartbeat:
    def test_sla_default(self):
        from ap.reconciler_heartbeat import RECONCILER_SLA_SECONDS
        assert RECONCILER_SLA_SECONDS == 120

    def test_record_heartbeat_updates_timestamp(self):
        from ap import reconciler_heartbeat as hb
        hb.record_heartbeat("client-A")
        snap = hb.snapshot("client-A")
        assert snap["stale"] is False
        assert snap["age_secs"] is not None
        assert snap["age_secs"] < 1.0
        assert snap["cycles"] == 1

    def test_no_heartbeat_yet_not_stale(self):
        from ap import reconciler_heartbeat as hb
        snap = hb.check_staleness("client-never")
        # We don't alert before the first beat \u2014 startup case.
        assert snap["stale"] is False
        assert snap["should_alert"] is False
        assert snap["last_heartbeat"] is None

    def test_stale_after_sla(self):
        from ap import reconciler_heartbeat as hb
        hb.record_heartbeat("client-A")
        hb._force_heartbeat_age_for_tests("client-A", hb.RECONCILER_SLA_SECONDS + 5)
        snap = hb.check_staleness("client-A")
        assert snap["stale"] is True
        assert snap["should_alert"] is True
        assert snap["age_secs"] >= hb.RECONCILER_SLA_SECONDS

    def test_alert_fires_once_per_stale_episode(self):
        """should_alert must be True ONLY on the first detection per
        stale episode. The caller emits one decision_event, not one per
        tick."""
        from ap import reconciler_heartbeat as hb
        hb.record_heartbeat("client-A")
        hb._force_heartbeat_age_for_tests("client-A", hb.RECONCILER_SLA_SECONDS + 5)
        s1 = hb.check_staleness("client-A")
        s2 = hb.check_staleness("client-A")
        s3 = hb.check_staleness("client-A")
        assert s1["should_alert"] is True
        assert s2["should_alert"] is False
        assert s3["should_alert"] is False
        # All still report stale=True.
        assert all(s["stale"] for s in (s1, s2, s3))

    def test_fresh_heartbeat_clears_alert_latch(self):
        from ap import reconciler_heartbeat as hb
        hb.record_heartbeat("client-A")
        hb._force_heartbeat_age_for_tests("client-A", hb.RECONCILER_SLA_SECONDS + 5)
        assert hb.check_staleness("client-A")["should_alert"] is True
        # Latch is set; second check no longer alerts.
        assert hb.check_staleness("client-A")["should_alert"] is False
        # A fresh heartbeat must clear the latch.
        hb.record_heartbeat("client-A")
        # Force-stale again
        hb._force_heartbeat_age_for_tests("client-A", hb.RECONCILER_SLA_SECONDS + 5)
        assert hb.check_staleness("client-A")["should_alert"] is True

    def test_reconciler_calls_record_heartbeat(self):
        """ap/reconcile.run_reconciliation must call record_heartbeat at
        the end of a successful cycle."""
        rec_src = (REPO_ROOT / "ap" / "reconcile.py").read_text()
        assert "from ap import reconciler_heartbeat" in rec_src
        assert "record_heartbeat(client_id, status=\"ok\")" in rec_src

    def test_order_monitor_emits_critical_on_stale(self):
        """order_monitor._run must check staleness and emit a CRITICAL
        decision_event when should_alert is True. Source-shape proof."""
        assert "from ap import reconciler_heartbeat as _hb" in OM_SRC
        assert "_hb.check_staleness(self.client_id)" in OM_SRC
        assert 'reason_code="RECONCILER_STALE"' in OM_SRC
        assert 'decision="CRITICAL"' in OM_SRC


# ============================================================
# FIX 6: no-regression check (proven behavior preserved)
# ============================================================

class TestFix6Preservation:
    """Spot-checks that the prior fix landmarks are still in place."""

    def test_phase2_adaptive_autocancel_still_present(self):
        # 90s / 120s ceilings + 25s reeval
        assert "ENTRY_REEVAL_AGE_SECONDS" in OM_SRC
        assert "ENTRY_MAX_AGE_NORMAL" in OM_SRC
        assert "ENTRY_MAX_AGE_APLUS" in OM_SRC

    def test_phase3_submit_time_refresh_still_present(self):
        assert "SUBMIT_CHASE_BAND_PCT" in EXEC_SRC
        assert "_refresh_ask_at_submit" in EXEC_SRC

    def test_phase4_account_sizing_still_present(self):
        assert "POSITION_RISK_PCT" in EXEC_SRC
        assert "_size_position(" in EXEC_SRC

    def test_pr29_repeg_first_still_present(self):
        assert "BUG-A FIX (PR #29" in OM_SRC
        # repeg attempted before runaway cancel
        assert "if self._try_repeg(" in OM_SRC

    def test_pr29_symbol_lock_release_still_present(self):
        assert "_release_symbol_lock_for_canceled" in OM_SRC

    def test_pr29_retry_payload_shape_still_present(self):
        src = (REPO_ROOT / "ap" / "post_cancel_retry.py").read_text()
        assert "RETRY_MISSING_TRIGGER_STRIKE" in src
        assert '"symbol":               symbol' in src
