"""
tests/test_p0_live_submit_gates.py

PR #305 — LIVE Submit Safety Gates tests.

Covers all 13 acceptance tests from the audit spec:
    1.  LIVE submit blocks blank execution_mode
    2.  LIVE submit blocks unknown execution_mode
    3.  LIVE submit blocks missing client_id
    4.  Identity preserved through gates (client_id + execution_mode)
    5.  Final market validity blocks CALL target-already-hit
    6.  Final market validity blocks REMAINING_OPPORTUNITY_TOO_SMALL
    7.  Final market validity blocks missing current quote (LIVE)
    8.  Trigger age blocks submit after 120 seconds
    9.  Trigger age allows submit inside window
    +   Additional structural / robustness tests
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

from ap.live_submit_gates import (
    GateOutcome,
    GateResult,
    check_identity_gate,
    check_market_validity_gate,
    check_trigger_age_gate,
    run_all_live_submit_gates,
)


# ─────────────────────────────────────────────────────────────────────────────
# Gate 1 — Identity
# ─────────────────────────────────────────────────────────────────────────────

class TestIdentityGate:
    """LIVE and PAPER both fail closed on identity — identity is not
    mode-conditional."""

    # ── Spec test 1
    def test_01_blocks_blank_execution_mode(self):
        r = check_identity_gate(client_id="jasoncosby1@gmail.com", execution_mode="")
        assert r.passed is False
        assert r.reason_code == GateOutcome.LIVE_SUBMIT_EXECUTION_MODE_UNKNOWN

    # ── Spec test 2
    def test_02_blocks_unknown_execution_mode(self):
        for bad in ("unknown", "sandbox", "test", "LIVE_STAGING"):
            r = check_identity_gate(client_id="jasoncosby1@gmail.com", execution_mode=bad)
            assert r.passed is False, f"Expected block for execution_mode={bad!r}"
            assert r.reason_code == GateOutcome.LIVE_SUBMIT_EXECUTION_MODE_UNKNOWN

    def test_02b_blocks_null_execution_mode(self):
        r = check_identity_gate(client_id="jasoncosby1@gmail.com", execution_mode=None)
        assert r.passed is False
        assert r.reason_code == GateOutcome.LIVE_SUBMIT_EXECUTION_MODE_UNKNOWN

    # ── Spec test 3
    def test_03_blocks_missing_client_id(self):
        for missing in ("", None, "   "):
            r = check_identity_gate(client_id=missing, execution_mode="live")
            assert r.passed is False
            assert r.reason_code == GateOutcome.LIVE_SUBMIT_CLIENT_ID_MISSING

    # ── Spec test 4 (partial — see identity_preserved test in TestGateIntegration)
    def test_04_accepts_valid_live_identity(self):
        r = check_identity_gate(
            client_id="jasoncosby1@gmail.com",
            execution_mode="live",
        )
        assert r.passed is True
        assert r.reason_code == GateOutcome.PASS
        # Identity is stamped in audit for downstream evidence
        assert r.audit["client_id"] == "jasoncosby1@gmail.com"
        assert r.audit["execution_mode"] == "live"

    def test_04b_accepts_valid_paper_identity(self):
        r = check_identity_gate(
            client_id="jose.vasquez4011@gmail.com",
            execution_mode="paper",
        )
        assert r.passed is True

    def test_04c_execution_mode_case_insensitive(self):
        for mode in ("LIVE", "Live", "live", "PAPER", "Paper", "paper"):
            r = check_identity_gate(client_id="a@b.com", execution_mode=mode)
            assert r.passed is True, f"execution_mode={mode!r} should pass"

    def test_04d_client_id_mismatch_across_layers(self):
        """A mismatch between plan.client_id and osm.client_id is a
        catastrophic safety signal — the wrong account could be charged."""
        r = check_identity_gate(
            client_id="jasoncosby1@gmail.com",
            execution_mode="live",
            osm_client_id="somebody_else@gmail.com",
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.LIVE_SUBMIT_CLIENT_ID_MISMATCH
        assert r.audit["mismatch_source"] == "osm"

    def test_04e_execution_mode_mismatch_across_layers(self):
        """Plan says LIVE but OSM says PAPER — never submit."""
        r = check_identity_gate(
            client_id="jasoncosby1@gmail.com",
            execution_mode="live",
            osm_execution_mode="paper",
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.LIVE_SUBMIT_EXECUTION_MODE_MISMATCH

    def test_04f_missing_cross_check_is_not_a_mismatch(self):
        """Absent cross-check IDs are acceptable — only POSITIVE disagreement blocks."""
        r = check_identity_gate(
            client_id="jasoncosby1@gmail.com",
            execution_mode="live",
            osm_client_id="",   # not populated — should NOT block
            watcher_client_id=None,
        )
        assert r.passed is True


# ─────────────────────────────────────────────────────────────────────────────
# Gate 2 — Market Validity
# ─────────────────────────────────────────────────────────────────────────────

class TestMarketValidityGate:
    """LIVE fails closed on stale/missing/reversed geometry. PAPER logs and passes."""

    # ── Spec test 5: CALL target already hit
    def test_05_blocks_target_already_invalid_call(self):
        r = check_market_validity_gate(
            side="CALL",
            trigger_price=100.0,
            stop_price=95.0,
            target_price=105.0,
            current_bid=105.10,
            current_ask=105.20,   # mid = 105.15 >= target 105.0
            execution_mode="live",
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.TARGET_ALREADY_INVALID

    def test_05b_blocks_target_already_invalid_put(self):
        r = check_market_validity_gate(
            side="PUT",
            trigger_price=100.0,
            stop_price=105.0,
            target_price=95.0,
            current_bid=94.90,
            current_ask=94.95,   # mid = 94.925 <= target 95
            execution_mode="live",
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.TARGET_ALREADY_INVALID

    # ── Spec test 6: remaining opportunity too small
    def test_06_blocks_remaining_opportunity_too_small(self):
        # CALL: trigger=100, target=105, current=104.8 → remaining=4% < 10% default
        r = check_market_validity_gate(
            side="CALL",
            trigger_price=100.0,
            stop_price=95.0,
            target_price=105.0,
            current_bid=104.75,
            current_ask=104.85,
            execution_mode="live",
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.REMAINING_OPPORTUNITY_TOO_SMALL

    # ── Spec test 7: missing current quote (LIVE)
    def test_07_blocks_missing_current_quote_live(self):
        r = check_market_validity_gate(
            side="CALL",
            trigger_price=100.0,
            stop_price=95.0,
            target_price=105.0,
            current_bid=None,
            current_ask=None,
            execution_mode="live",
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.CURRENT_PRICE_MISSING

    def test_07b_blocks_zero_bid_or_ask_live(self):
        r = check_market_validity_gate(
            side="CALL", trigger_price=100.0, stop_price=95.0, target_price=105.0,
            current_bid=0.0, current_ask=0.0, execution_mode="live",
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.CURRENT_PRICE_ZERO

    def test_07c_paper_missing_quote_does_not_block(self):
        """PAPER logs the failure but passes — the sandbox flow can continue
        for testing purposes."""
        r = check_market_validity_gate(
            side="CALL", trigger_price=100.0, stop_price=95.0, target_price=105.0,
            current_bid=None, current_ask=None, execution_mode="paper",
        )
        assert r.passed is True   # paper does not block on missing quote

    def test_08_blocks_stale_quote_live(self):
        r = check_market_validity_gate(
            side="CALL", trigger_price=100.0, stop_price=95.0, target_price=105.0,
            current_bid=101.0, current_ask=101.2,
            quote_age_ms=10000,   # 10 seconds — well over default 5s
            execution_mode="live",
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.CURRENT_PRICE_STALE

    def test_09_blocks_call_no_longer_above_trigger(self):
        """Trigger was 100; current mid is now 99.5 — the breach reversed
        before we could submit. Never send a stale entry."""
        r = check_market_validity_gate(
            side="CALL", trigger_price=100.0, stop_price=95.0, target_price=105.0,
            current_bid=99.4, current_ask=99.6, execution_mode="live",
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.CALL_NO_LONGER_ABOVE_TRIGGER

    def test_09b_blocks_put_no_longer_below_trigger(self):
        r = check_market_validity_gate(
            side="PUT", trigger_price=100.0, stop_price=105.0, target_price=95.0,
            current_bid=100.5, current_ask=100.7, execution_mode="live",
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.PUT_NO_LONGER_BELOW_TRIGGER

    def test_10_blocks_call_stop_broken_before_submit(self):
        r = check_market_validity_gate(
            side="CALL", trigger_price=100.0, stop_price=95.0, target_price=105.0,
            current_bid=94.5, current_ask=94.9, execution_mode="live",
        )
        assert r.passed is False
        # This will match CALL_NO_LONGER_ABOVE_TRIGGER since 94 < 100 first
        # — the order of checks is deliberate (trigger check runs before stop)
        assert r.reason_code in (
            GateOutcome.CALL_NO_LONGER_ABOVE_TRIGGER,
            GateOutcome.CALL_STOP_ALREADY_BROKEN,
        )

    def test_11_passes_valid_call_setup(self):
        """CALL trigger=100, target=110, stop=95, mid=101 → passes all rules.
        Remaining = (110-101)/(110-100) = 90% >> 10% threshold."""
        r = check_market_validity_gate(
            side="CALL", trigger_price=100.0, stop_price=95.0, target_price=110.0,
            current_bid=100.95, current_ask=101.05, quote_age_ms=100,
            execution_mode="live",
        )
        assert r.passed is True
        assert r.reason_code == GateOutcome.PASS
        assert r.audit["current_mid"] == pytest.approx(101.0, abs=0.01)

    def test_11b_passes_valid_put_setup(self):
        r = check_market_validity_gate(
            side="PUT", trigger_price=100.0, stop_price=105.0, target_price=90.0,
            current_bid=98.95, current_ask=99.05, quote_age_ms=100,
            execution_mode="live",
        )
        assert r.passed is True


# ─────────────────────────────────────────────────────────────────────────────
# Gate 3 — Trigger Age
# ─────────────────────────────────────────────────────────────────────────────

class TestTriggerAgeGate:
    """LIVE fails closed on any trigger older than ENTRY_TRIGGER_MAX_AGE_SEC (120s default)."""

    def _iso(self, delta_seconds: float) -> str:
        """Return an ISO timestamp N seconds ago (negative=future)."""
        return (datetime.now(timezone.utc) - timedelta(seconds=delta_seconds)).isoformat()

    # ── Spec test 8: age > 120s blocks
    def test_08_blocks_submit_after_120_seconds_live(self):
        r = check_trigger_age_gate(
            trigger_crossed_at=self._iso(180),   # 3 minutes ago
            execution_mode="live",
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.STALE_TRIGGER_BREACH
        assert r.audit["age_seconds"] >= 179

    # ── Spec test 9: age < 120s passes
    def test_09_allows_submit_inside_window_live(self):
        r = check_trigger_age_gate(
            trigger_crossed_at=self._iso(60),   # 1 minute ago
            execution_mode="live",
        )
        assert r.passed is True
        assert r.reason_code == GateOutcome.PASS

    def test_10_paper_stale_trigger_does_not_block(self):
        """PAPER logs but passes."""
        r = check_trigger_age_gate(
            trigger_crossed_at=self._iso(300),
            execution_mode="paper",
        )
        assert r.passed is True

    def test_11_live_missing_trigger_crossed_at_blocks(self):
        """LIVE cannot verify age without the timestamp — fail closed."""
        r = check_trigger_age_gate(
            trigger_crossed_at=None,
            execution_mode="live",
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.STALE_TRIGGER_BREACH
        assert r.audit["missing_trigger_crossed_at"] is True

    def test_12_clock_skew_future_timestamp_blocks_live(self):
        """A trigger_crossed_at in the future is a signal of clock corruption
        or bad data — LIVE must not submit."""
        r = check_trigger_age_gate(
            trigger_crossed_at=self._iso(-30),   # 30 seconds in the future
            execution_mode="live",
        )
        assert r.passed is False
        assert r.audit.get("clock_skew") is True

    def test_13_env_override_max_age(self, monkeypatch):
        """Ops can tune ENTRY_TRIGGER_MAX_AGE_SEC via env without a code change."""
        monkeypatch.setenv("ENTRY_TRIGGER_MAX_AGE_SEC", "30")
        # Force reload so env is picked up — not needed since the gate reads
        # env at call time via _int_env.
        r = check_trigger_age_gate(
            trigger_crossed_at=self._iso(60),   # 60s ago
            execution_mode="live",
        )
        # With max=30, 60s ago is stale
        assert r.passed is False

    def test_14_explicit_max_age_kwarg_overrides_env(self):
        r = check_trigger_age_gate(
            trigger_crossed_at=self._iso(60),
            execution_mode="live",
            max_age_seconds=300,   # explicit override
        )
        assert r.passed is True


# ─────────────────────────────────────────────────────────────────────────────
# Integration — all three gates in order + identity preservation
# ─────────────────────────────────────────────────────────────────────────────

class TestGateIntegration:
    """The composed run_all_live_submit_gates helper."""

    def _iso(self, delta_seconds: float) -> str:
        return (datetime.now(timezone.utc) - timedelta(seconds=delta_seconds)).isoformat()

    def test_04_identity_preserved_through_composed_call(self):
        """Spec test 4: client_id and execution_mode must appear in every gate's
        audit output so the full submit chain has attribution."""
        result, combined = run_all_live_submit_gates(
            client_id="jasoncosby1@gmail.com",
            execution_mode="live",
            side="CALL",
            trigger_price=100.0,
            stop_price=95.0,
            target_price=110.0,
            current_bid=100.95,
            current_ask=101.05,
            quote_age_ms=100,
            trigger_crossed_at=self._iso(30),
        )
        assert result.passed is True
        assert combined["identity_gate"]["client_id"] == "jasoncosby1@gmail.com"
        assert combined["identity_gate"]["execution_mode"] == "live"

    def test_short_circuits_on_identity_failure(self):
        """When identity fails, market_validity and trigger_age must not be called."""
        result, combined = run_all_live_submit_gates(
            client_id="",     # missing — identity fails
            execution_mode="live",
            side="CALL", trigger_price=100.0,
            current_bid=101.0, current_ask=101.2,
            trigger_crossed_at=self._iso(30),
        )
        assert result.passed is False
        assert result.reason_code == GateOutcome.LIVE_SUBMIT_CLIENT_ID_MISSING
        # Only the failing gate should appear in the audit
        assert "identity_gate" in combined
        assert "market_validity_gate" not in combined
        assert "trigger_age_gate" not in combined

    def test_short_circuits_on_market_validity_failure(self):
        """Trigger age gate must not run if market gate fails."""
        result, combined = run_all_live_submit_gates(
            client_id="jasoncosby1@gmail.com",
            execution_mode="live",
            side="CALL", trigger_price=100.0,
            target_price=105.0,
            current_bid=105.5, current_ask=105.7,   # target hit
            trigger_crossed_at=self._iso(30),
        )
        assert result.passed is False
        assert result.reason_code == GateOutcome.TARGET_ALREADY_INVALID
        assert "identity_gate" in combined
        assert "market_validity_gate" in combined
        assert "trigger_age_gate" not in combined


# ─────────────────────────────────────────────────────────────────────────────
# WatchedSignal instrumentation for trigger_crossed_at
# ─────────────────────────────────────────────────────────────────────────────

class TestTriggerCrossedAtStamping:
    """WatchedSignal.check must stamp trigger_crossed_at on FIRST breach, not
    on confirmed trigger — the age gate needs the earliest evidence."""

    def _make_watched(self, side="CALL", trigger=100.0):
        import ap_entry_watcher as ew
        w = ew.WatchedSignal({
            "signal_id": "test",
            "ticker": "GS",
            "side": side,
            "entry_price": trigger,
            "score": 65.0,
            "grade": "B",
        }, overnight=False)
        return w

    def test_call_stamps_trigger_crossed_at_on_first_breach(self):
        w = self._make_watched(side="CALL", trigger=100.0)
        assert w.trigger_crossed_at is None
        # First breach: ask crosses trigger
        w.check(bid=99.5, ask=100.5)
        assert w.trigger_crossed_at is not None
        first_stamp = w.trigger_crossed_at
        # Second breach — must NOT overwrite (age uses FIRST breach)
        w.check(bid=99.7, ask=100.7)
        assert w.trigger_crossed_at == first_stamp, (
            "trigger_crossed_at must not be overwritten by subsequent breaches"
        )

    def test_put_stamps_trigger_crossed_at_on_first_breach(self):
        w = self._make_watched(side="PUT", trigger=100.0)
        assert w.trigger_crossed_at is None
        w.check(bid=99.5, ask=100.5)   # bid <= trigger for PUT
        assert w.trigger_crossed_at is not None

    def test_first_breach_bid_and_ask_stamped(self):
        w = self._make_watched(side="CALL", trigger=100.0)
        w.check(bid=99.5, ask=100.5)
        assert w.first_breach_bid == 99.5
        assert w.first_breach_ask == 100.5


# ─────────────────────────────────────────────────────────────────────────────
# Structural — code integration in ap_execution_core
# ─────────────────────────────────────────────────────────────────────────────

class TestStructuralIntegration:
    """Verify the gates are wired at the correct call site."""

    def test_gates_called_before_submit_existing_entry(self):
        src = open("ap_execution_core.py").read()
        # The gates must be called BEFORE submit_existing_entry
        gates_pos = src.find("from ap.live_submit_gates import")
        submit_pos = src.find(
            "submit_res = self.order_state_machine.submit_existing_entry("
        )
        assert gates_pos > 0 and submit_pos > 0
        assert gates_pos < submit_pos, (
            "Gates must be imported and called BEFORE submit_existing_entry"
        )

    def test_all_three_gate_functions_referenced(self):
        src = open("ap_execution_core.py").read()
        for fn in ("check_identity_gate", "check_market_validity_gate", "check_trigger_age_gate"):
            assert fn in src, f"{fn} not wired into ap_execution_core.py"

    def test_gate_failure_terminalizes(self):
        src = open("ap_execution_core.py").read()
        # Each gate failure path must call _terminalize_breach_failure
        # with a live_submit_gate: prefix
        assert 'live_submit_gate:' in src, "gate failures must terminalize with prefix"
        assert "_terminalize_breach_failure(f\"live_submit_gate:" in src

    def test_module_error_fails_closed_for_live(self):
        """If the gate module itself throws, LIVE must fail closed."""
        src = open("ap_execution_core.py").read()
        assert "LIVE_SUBMIT_GATE_MODULE_ERROR" in src
        assert 'live_submit_gate:MODULE_ERROR' in src

    def test_gates_stamp_audit_on_meta(self):
        """Every gate outcome (pass or fail) writes evidence to orders.meta."""
        src = open("ap_execution_core.py").read()
        # The 'live_submit_gate' key is written for pass and fail paths
        assert '"live_submit_gate"' in src
        assert "all_passed" in src


# ─────────────────────────────────────────────────────────────────────────────
# Robustness — never raise on bad input
# ─────────────────────────────────────────────────────────────────────────────

class TestRobustness:
    """Client-money code — none of the gates may raise on bad input."""

    def test_identity_gate_never_raises(self):
        for bad_cid in (None, "", 12345, ["list"], {"dict": 1}):
            for bad_mode in (None, "", 999, ["list"]):
                try:
                    check_identity_gate(client_id=bad_cid, execution_mode=bad_mode)
                except Exception as e:
                    pytest.fail(f"identity gate raised on ({bad_cid!r}, {bad_mode!r}): {e}")

    def test_market_validity_gate_never_raises(self):
        try:
            check_market_validity_gate(
                side="INVALID_SIDE",
                trigger_price="not_a_number",
                stop_price=None,
                target_price=None,
                current_bid="oops",
                current_ask=None,
                quote_age_ms="text",
                execution_mode="live",
            )
        except Exception as e:
            pytest.fail(f"market gate raised on bad input: {e}")

    def test_trigger_age_gate_never_raises(self):
        for bad in ("not_a_date", "", None, 12345, "2026-13-45"):
            try:
                check_trigger_age_gate(
                    trigger_crossed_at=bad, execution_mode="live",
                )
            except Exception as e:
                pytest.fail(f"trigger age gate raised on {bad!r}: {e}")

    def test_run_all_never_raises(self):
        result, combined = run_all_live_submit_gates(
            client_id=None,
            execution_mode=None,
        )
        # Must return a valid GateResult with failure fields populated
        assert result.passed is False
        assert result.reason_code != GateOutcome.PASS
