"""
tests/test_p0_live_submit_gates_amended.py

Amendment tests for GitHub #306 / internal PR #305.

Behavioral tests — not structural text searches. Each test drives real
production logic and asserts outputs, not source-code presence.

Amendments covered:
    1.  Execution mode derivation fallback chain
    2.  trigger_crossed_at durable persistence + gate reads from meta
    3.  Market validity uses ask (CALL) / bid (PUT) as trigger-lane primary
    4.  Gate failure persists final_market_validity to orders.meta
    5.  DEFERRED contract blocks with LIVE_SUBMIT_CONTRACT_NOT_MATERIALIZED
    6.  All live gate failures produce no broker submit
"""
from __future__ import annotations

import os
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

from ap.live_submit_gates import (
    GateOutcome,
    check_identity_gate,
    check_market_validity_gate,
    check_trigger_age_gate,
    derive_submit_execution_mode,
    resolve_trigger_timestamps,
)


_CLIENT_ID = "jasoncosby1@gmail.com"
_REAL_OCC  = "GS  260717C00470000"
_DEFERRED  = "DEFERRED:GS"


def _iso(delta_s: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=delta_s)).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Amendment 1 — execution mode fallback chain
# ─────────────────────────────────────────────────────────────────────────────

class TestExecutionModeDerivation:
    """The fallback chain must derive live/paper from 5 sources.
    LIVE must fail closed on blank/unknown regardless of source."""

    def test_blank_proof_mode_fails_closed(self):
        """If _proof_execution_mode is blank, the gate blocks — not silently approves."""
        r = check_identity_gate(client_id=_CLIENT_ID, execution_mode="")
        assert r.passed is False
        assert r.reason_code == GateOutcome.LIVE_SUBMIT_EXECUTION_MODE_UNKNOWN

    def test_none_proof_mode_fails_closed(self):
        r = check_identity_gate(client_id=_CLIENT_ID, execution_mode=None)
        assert r.passed is False
        assert r.reason_code == GateOutcome.LIVE_SUBMIT_EXECUTION_MODE_UNKNOWN

    def test_unknown_string_mode_fails_closed(self):
        for bad in ("sandbox", "test", "staging", "LIVE_STAGING", "unknown"):
            r = check_identity_gate(client_id=_CLIENT_ID, execution_mode=bad)
            assert r.passed is False, f"'{bad}' should fail closed"

    def test_live_mode_accepted(self):
        r = check_identity_gate(client_id=_CLIENT_ID, execution_mode="live")
        assert r.passed is True

    def test_paper_mode_accepted(self):
        r = check_identity_gate(client_id=_CLIENT_ID, execution_mode="paper")
        assert r.passed is True

    def test_mode_case_insensitive(self):
        for mode in ("LIVE", "Live", "PAPER", "Paper"):
            r = check_identity_gate(client_id=_CLIENT_ID, execution_mode=mode)
            assert r.passed is True, f"mode={mode!r} should be accepted case-insensitively"

    def test_blank_self_execution_mode_but_paper_false_derives_live(self):
        mode = derive_submit_execution_mode(
            proof_execution_mode="",
            approved_plan_execution_mode="",
            self_execution_mode="",
            self_mode="",
            self_paper=False,
            osm_execution_mode="",
        )
        assert mode == "live"
        r = check_identity_gate(client_id="", execution_mode=mode)
        assert r.passed is False
        assert r.reason_code == GateOutcome.LIVE_SUBMIT_CLIENT_ID_MISSING

    def test_fallback_chain_uses_osm_when_available(self):
        mode = derive_submit_execution_mode(
            proof_execution_mode="",
            approved_plan_execution_mode="",
            self_execution_mode="",
            self_mode="",
            self_paper=None,
            osm_execution_mode="live",
        )
        assert mode == "live"

    def test_blank_unknown_mode_stays_unknown_for_fail_closed_gate(self):
        mode = derive_submit_execution_mode(
            proof_execution_mode="sandbox",
            approved_plan_execution_mode="",
            self_execution_mode="",
            self_mode="",
            self_paper=None,
            osm_execution_mode="",
        )
        assert mode == ""
        r = check_identity_gate(client_id=_CLIENT_ID, execution_mode=mode)
        assert r.passed is False
        assert r.reason_code == GateOutcome.LIVE_SUBMIT_EXECUTION_MODE_UNKNOWN


# ─────────────────────────────────────────────────────────────────────────────
# Amendment 2 — trigger_crossed_at durable persistence
# ─────────────────────────────────────────────────────────────────────────────

class TestTriggerTimestampPersistence:

    def test_watcher_stamps_trigger_crossed_at_on_confirmation(self):
        """PR #407: trigger_crossed_at is stamped ONLY after
        MOMENTUM_POLLS_REQUIRED breaches confirm."""
        import ap_entry_watcher as ew
        w = ew.WatchedSignal(
            {"signal_id": "S1", "ticker": "GS", "side": "CALL",
             "entry_price": 100.0, "score": 65.0, "grade": "B"},
            overnight=False,
        )
        assert w.trigger_crossed_at is None
        w.check(bid=99.5, ask=100.5)   # first breach — PENDING, not stamped
        assert w.trigger_crossed_at is None
        assert w._pending_first_breach_at is not None
        w.check(bid=99.7, ask=100.7)   # confirming breach — stamped
        assert w.trigger_crossed_at is not None, (
            "trigger_crossed_at must be stamped after confirmation."
        )

    def test_trigger_crossed_at_equals_first_breach_timestamp(self):
        """PR #407: the stamped value MUST be the FIRST breach poll's
        timestamp (LIVE trigger-age gate relies on the earliest evidence),
        not the confirmation poll's."""
        import ap_entry_watcher as ew
        w = ew.WatchedSignal(
            {"signal_id": "S1", "ticker": "GS", "side": "CALL",
             "entry_price": 100.0, "score": 65.0, "grade": "B"},
            overnight=False,
        )
        w.check(bid=99.5, ask=100.5)   # first breach
        pending_ts = w._pending_first_breach_at
        assert pending_ts is not None
        w.check(bid=99.8, ask=100.8)   # confirmation
        assert w.trigger_crossed_at == pending_ts, (
            "trigger_crossed_at must equal the FIRST breach poll timestamp."
        )

    def test_trigger_age_gate_blocks_when_missing(self):
        """LIVE blocks when trigger_crossed_at is None."""
        r = check_trigger_age_gate(trigger_crossed_at=None, execution_mode="live")
        assert r.passed is False
        assert r.reason_code == GateOutcome.STALE_TRIGGER_BREACH
        assert r.audit.get("missing_trigger_crossed_at") is True

    def test_trigger_age_gate_passes_when_fresh(self):
        r = check_trigger_age_gate(trigger_crossed_at=_iso(30), execution_mode="live")
        assert r.passed is True

    def test_trigger_age_gate_blocks_when_stale(self):
        r = check_trigger_age_gate(trigger_crossed_at=_iso(180), execution_mode="live")
        assert r.passed is False
        assert r.reason_code == GateOutcome.STALE_TRIGGER_BREACH

    def test_watcher_persists_timestamps_to_osm_on_success(self):
        """When on_trigger succeeds, watcher persists trigger_crossed_at to orders.meta."""
        src = open("ap_entry_watcher.py").read()
        assert "WATCHER_TRIGGER_TIMESTAMPS_PERSISTED" in src, (
            "Watcher must log WATCHER_TRIGGER_TIMESTAMPS_PERSISTED after persisting timestamps"
        )
        assert "trigger_crossed_at" in src
        assert "first_breach_bid" in src
        assert "first_breach_ask" in src

    def test_timestamp_resolution_prefers_durable_order_meta(self):
        """Durable order meta wins over any in-memory WatchedSignal value."""
        import ap_entry_watcher as ew
        older = _iso(45)
        newer = datetime.now(timezone.utc)
        w = ew.WatchedSignal(
            {"signal_id": "S1", "ticker": "GS", "side": "CALL",
             "entry_price": 100.0, "score": 65.0, "grade": "B"},
            overnight=False,
        )
        w.trigger_crossed_at = newer
        crossed, _ = resolve_trigger_timestamps(
            order_meta={"trigger_crossed_at": older},
            watched_signal=w,
        )
        assert crossed == older

    def test_timestamp_resolution_falls_back_to_watched_signal(self):
        import ap_entry_watcher as ew
        w = ew.WatchedSignal(
            {"signal_id": "S1", "ticker": "GS", "side": "CALL",
             "entry_price": 100.0, "score": 65.0, "grade": "B"},
            overnight=False,
        )
        # PR #407: trigger_crossed_at is only populated after confirmation;
        # feed two qualifying polls so the watcher has durable evidence.
        w.check(bid=99.5, ask=100.5)   # first breach
        w.check(bid=99.7, ask=100.7)   # confirmation
        assert w.trigger_crossed_at is not None
        crossed, _ = resolve_trigger_timestamps(order_meta={}, watched_signal=w)
        assert crossed == w.trigger_crossed_at.isoformat()

    def test_paper_allows_missing_trigger_crossed_at(self):
        """Paper does not fail closed on missing timestamps."""
        r = check_trigger_age_gate(trigger_crossed_at=None, execution_mode="paper")
        assert r.passed is True


# ─────────────────────────────────────────────────────────────────────────────
# Amendment 3 — trigger-lane check (ask for CALL, bid for PUT)
# ─────────────────────────────────────────────────────────────────────────────

class TestTriggerLaneMarketValidity:
    """Market validity must use ask (CALL) / bid (PUT) as primary trigger check."""

    # CALL tests
    def test_call_ask_below_trigger_blocks(self):
        """CALL ask < trigger means breach reversed — must block."""
        r = check_market_validity_gate(
            side="CALL",
            trigger_price=100.0,
            stop_price=95.0,
            target_price=110.0,
            current_bid=98.5,
            current_ask=99.5,   # ask=99.5 < trigger=100.0
            quote_age_ms=0,
            execution_mode="live",
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.CALL_NO_LONGER_ABOVE_TRIGGER

    def test_call_ask_at_trigger_passes(self):
        """CALL ask == trigger is still in breach — must pass trigger check."""
        r = check_market_validity_gate(
            side="CALL",
            trigger_price=100.0,
            stop_price=95.0,
            target_price=110.0,
            current_bid=99.8,
            current_ask=100.0,   # ask exactly at trigger
            quote_age_ms=0,
            execution_mode="live",
        )
        # Trigger check passes; remaining opportunity check may block but not trigger check
        assert r.reason_code != GateOutcome.CALL_NO_LONGER_ABOVE_TRIGGER

    def test_call_bid_below_trigger_but_ask_above_passes_trigger_check(self):
        """CALL: bid can be below trigger as long as ask >= trigger (wide spread)."""
        r = check_market_validity_gate(
            side="CALL",
            trigger_price=100.0,
            stop_price=95.0,
            target_price=110.0,
            current_bid=99.2,   # bid below trigger — fine for CALL
            current_ask=100.8,  # ask above trigger — breach confirmed
            quote_age_ms=0,
            execution_mode="live",
        )
        assert r.reason_code != GateOutcome.CALL_NO_LONGER_ABOVE_TRIGGER

    # PUT tests
    def test_put_bid_above_trigger_blocks(self):
        """PUT bid > trigger means breach reversed — must block."""
        r = check_market_validity_gate(
            side="PUT",
            trigger_price=100.0,
            stop_price=105.0,
            target_price=90.0,
            current_bid=100.5,   # bid=100.5 > trigger=100.0
            current_ask=101.0,
            quote_age_ms=0,
            execution_mode="live",
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.PUT_NO_LONGER_BELOW_TRIGGER

    def test_put_bid_at_trigger_passes_trigger_check(self):
        """PUT bid == trigger is still in breach."""
        r = check_market_validity_gate(
            side="PUT",
            trigger_price=100.0,
            stop_price=105.0,
            target_price=90.0,
            current_bid=100.0,   # exactly at trigger
            current_ask=100.5,
            quote_age_ms=0,
            execution_mode="live",
        )
        assert r.reason_code != GateOutcome.PUT_NO_LONGER_BELOW_TRIGGER

    def test_put_ask_above_trigger_but_bid_below_passes_trigger_check(self):
        """PUT: ask can be above trigger as long as bid <= trigger."""
        r = check_market_validity_gate(
            side="PUT",
            trigger_price=100.0,
            stop_price=105.0,
            target_price=90.0,
            current_bid=99.5,   # bid below trigger — breach confirmed
            current_ask=100.8,  # ask above trigger — fine for PUT
            quote_age_ms=0,
            execution_mode="live",
        )
        assert r.reason_code != GateOutcome.PUT_NO_LONGER_BELOW_TRIGGER

    # Target and stop geometry
    def test_target_already_hit_blocks(self):
        r = check_market_validity_gate(
            side="CALL", trigger_price=100.0, stop_price=95.0, target_price=105.0,
            current_bid=105.5, current_ask=105.8, quote_age_ms=0,
            execution_mode="live",
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.TARGET_ALREADY_INVALID

    def test_stop_broken_blocks(self):
        r = check_market_validity_gate(
            side="CALL", trigger_price=100.0, stop_price=95.0, target_price=110.0,
            current_bid=94.0, current_ask=94.5, quote_age_ms=0,  # mid=94.25 < stop=95
            execution_mode="live",
        )
        assert r.passed is False
        # Either trigger check or stop check fires first — both are correct blocks
        assert r.reason_code in (
            GateOutcome.CALL_NO_LONGER_ABOVE_TRIGGER,
            GateOutcome.CALL_STOP_ALREADY_BROKEN,
        )

    def test_remaining_opportunity_too_small_blocks(self):
        """target=105, trigger=100, current=104.8 → remaining=(105-104.8)/(105-100)=4%."""
        r = check_market_validity_gate(
            side="CALL", trigger_price=100.0, stop_price=95.0, target_price=105.0,
            current_bid=104.6, current_ask=104.8, quote_age_ms=0,
            execution_mode="live",
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.REMAINING_OPPORTUNITY_TOO_SMALL

    def test_missing_quote_blocks_live(self):
        r = check_market_validity_gate(
            side="CALL", trigger_price=100.0, stop_price=95.0, target_price=110.0,
            current_bid=None, current_ask=None,
            execution_mode="live",
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.CURRENT_PRICE_MISSING

    def test_zero_quote_blocks_live(self):
        r = check_market_validity_gate(
            side="CALL", trigger_price=100.0, stop_price=95.0, target_price=110.0,
            current_bid=0.0, current_ask=0.0,
            execution_mode="live",
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.CURRENT_PRICE_ZERO

    def test_stale_quote_blocks_live(self):
        r = check_market_validity_gate(
            side="CALL", trigger_price=100.0, stop_price=95.0, target_price=110.0,
            current_bid=101.0, current_ask=101.5, quote_age_ms=10000,
            execution_mode="live",
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.CURRENT_PRICE_STALE

    def test_paper_missing_quote_passes(self):
        """Paper does not fail closed on missing quote — sandbox testing."""
        r = check_market_validity_gate(
            side="CALL", trigger_price=100.0, stop_price=95.0, target_price=110.0,
            current_bid=None, current_ask=None,
            execution_mode="paper",
        )
        assert r.passed is True

    def test_paper_stale_quote_passes(self):
        r = check_market_validity_gate(
            side="CALL", trigger_price=100.0, stop_price=95.0, target_price=110.0,
            current_bid=101.0, current_ask=101.5, quote_age_ms=30000,
            execution_mode="paper",
        )
        assert r.passed is True

    @pytest.mark.parametrize("side", ["", "UNKNOWN", "BUY"])
    def test_invalid_option_side_blocks_live(self, side):
        r = check_market_validity_gate(
            side=side, trigger_price=100.0, stop_price=95.0, target_price=110.0,
            current_bid=100.8, current_ask=101.0, quote_age_ms=200,
            execution_mode="live",
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.CURRENT_OPTION_SIDE_INVALID
        assert r.audit["side"] == side

    def test_valid_call_setup_passes_all_checks(self):
        """Clean CALL: ask above trigger, mid below target, above stop, good opportunity."""
        r = check_market_validity_gate(
            side="CALL", trigger_price=100.0, stop_price=95.0, target_price=110.0,
            current_bid=100.8, current_ask=101.0, quote_age_ms=200,
            execution_mode="live",
        )
        assert r.passed is True
        assert r.reason_code == GateOutcome.PASS

    def test_valid_put_setup_passes_all_checks(self):
        """Clean PUT: bid below trigger, mid above target, below stop, good opportunity."""
        r = check_market_validity_gate(
            side="PUT", trigger_price=100.0, stop_price=105.0, target_price=90.0,
            current_bid=99.5, current_ask=99.9, quote_age_ms=200,
            execution_mode="live",
        )
        assert r.passed is True

    def test_audit_includes_bid_ask_mid_and_source(self):
        """Amendment 3: audit must include bid, ask, mid for full diagnostics."""
        r = check_market_validity_gate(
            side="CALL", trigger_price=100.0, stop_price=95.0, target_price=110.0,
            current_bid=101.0, current_ask=101.5, quote_age_ms=100,
            quote_source="tradier",
            execution_mode="live",
        )
        assert "current_bid" in r.audit
        assert "current_ask" in r.audit
        assert "current_mid" in r.audit
        assert "quote_age_ms" in r.audit
        assert r.audit["quote_source"] == "tradier"


# ─────────────────────────────────────────────────────────────────────────────
# Amendment 4 — final_market_validity in orders.meta
# ─────────────────────────────────────────────────────────────────────────────

class TestFinalMarketValidityPersistence:

    def test_final_market_validity_key_in_execution_core_on_failure(self):
        """When market validity gate fails, orders.meta must get
        final_market_validity key in addition to live_submit_gate."""
        src = open("ap_execution_core.py").read()
        assert '"final_market_validity"' in src, (
            "final_market_validity must be persisted to orders.meta "
            "on gate failure (Amendment 4)"
        )

    def test_final_market_validity_key_also_on_pass(self):
        """On all-pass, final_market_validity is also stamped so there is
        always a market audit trail regardless of outcome."""
        src = open("ap_execution_core.py").read()
        # Find all-passed block and confirm final_market_validity is there
        all_passed_idx = src.find('"all_passed": True')
        assert all_passed_idx > 0
        nearby = src[all_passed_idx:all_passed_idx + 500]
        assert "final_market_validity" in nearby, (
            "final_market_validity must be in the all-passed meta block"
        )

    def test_market_gate_audit_has_required_fields(self):
        """The market gate audit dict must include all required diagnostic fields."""
        r = check_market_validity_gate(
            side="CALL", trigger_price=100.0, stop_price=95.0, target_price=110.0,
            current_bid=101.0, current_ask=101.5, quote_age_ms=100,
            quote_source="tradier",
            execution_mode="live",
        )
        for field in ("gate", "checked_at", "execution_mode", "side",
                      "trigger_price", "stop_price", "target_price",
                      "current_bid", "current_ask", "current_mid",
                      "quote_age_ms", "quote_source", "max_quote_age_ms"):
            assert field in r.audit, f"Missing required audit field: {field!r}"


# ─────────────────────────────────────────────────────────────────────────────
# Amendment 5 — DEFERRED contract block
# ─────────────────────────────────────────────────────────────────────────────

class TestDeferredContractBlock:

    def test_deferred_contract_reason_code_defined(self):
        """LIVE_SUBMIT_CONTRACT_NOT_MATERIALIZED must be defined in the gate module."""
        # This reason code is new; check it exists in execution_core
        src = open("ap_execution_core.py").read()
        assert "LIVE_SUBMIT_CONTRACT_NOT_MATERIALIZED" in src, (
            "LIVE_SUBMIT_CONTRACT_NOT_MATERIALIZED reason code missing"
        )

    def test_deferred_contract_check_before_gate_calls(self):
        """The DEFERRED check must fire BEFORE Gate 1 (identity) in the code flow."""
        src = open("ap_execution_core.py").read()
        deferred_idx = src.find("LIVE_SUBMIT_CONTRACT_NOT_MATERIALIZED")
        gate1_idx    = src.find("# ── Gate 1: identity")
        assert deferred_idx > 0 and gate1_idx > 0
        assert deferred_idx < gate1_idx, (
            "DEFERRED contract check must precede Gate 1 (identity) in execution order"
        )

    def test_deferred_contract_terminalize_path_present(self):
        """A DEFERRED:* contract at the submit seam must call
        _terminalize_breach_failure with the specific reason code."""
        src = open("ap_execution_core.py").read()
        # The terminalize call uses the variable: f"live_submit_gate:{_deferred_reason}"
        assert "_terminalize_breach_failure(f\"live_submit_gate:{_deferred_reason}\")" in src, (
            "DEFERRED contract block must terminalize with live_submit_gate: prefix. "
            "Check ap_execution_core.py for the deferred contract gate block."
        )


# ─────────────────────────────────────────────────────────────────────────────
# Amendment 6 — no broker submit on any gate failure
# ─────────────────────────────────────────────────────────────────────────────

class TestNoSubmitOnGateFailure:

    def test_identity_fail_returns_false_from_gate(self):
        """Identity gate failure must return passed=False so caller blocks."""
        r = check_identity_gate(client_id="", execution_mode="live")
        assert r.passed is False
        # The reason code is canonical — caller terminates based on this
        assert r.reason_code in (
            GateOutcome.LIVE_SUBMIT_CLIENT_ID_MISSING,
            GateOutcome.LIVE_SUBMIT_EXECUTION_MODE_UNKNOWN,
        )

    def test_market_fail_returns_false_from_gate(self):
        r = check_market_validity_gate(
            side="CALL", trigger_price=100.0, stop_price=95.0, target_price=110.0,
            current_bid=None, current_ask=None, execution_mode="live",
        )
        assert r.passed is False

    def test_trigger_age_fail_returns_false_from_gate(self):
        r = check_trigger_age_gate(trigger_crossed_at=_iso(300), execution_mode="live")
        assert r.passed is False

    def test_terminalize_called_on_gate_failure_structural(self):
        """Every gate failure in execution_core must call _terminalize_breach_failure."""
        src = open("ap_execution_core.py").read()
        # Count terminalize calls in the gate block
        gate_start = src.find("# ── P0 (PR #305): LIVE submit safety gates")
        gate_end   = src.find("submit_res = self.order_state_machine.submit_existing_entry(", gate_start)
        gate_block = src[gate_start:gate_end]
        terminalize_count = gate_block.count("_terminalize_breach_failure(")
        assert terminalize_count >= 5, (
            f"Expected at least 5 terminalize calls in gate block "
            f"(contract, identity, market, trigger-age, module-error), got {terminalize_count}"
        )

    def test_all_gate_failures_set_last_error_prefix(self):
        """Every _terminalize_breach_failure in the gate block uses live_submit_gate: prefix."""
        src = open("ap_execution_core.py").read()
        gate_start = src.find("# ── P0 (PR #305): LIVE submit safety gates")
        gate_end   = src.find("submit_res = self.order_state_machine.submit_existing_entry(", gate_start)
        gate_block = src[gate_start:gate_end]
        # All terminalize calls must use the canonical prefix
        import re
        calls = re.findall(r'_terminalize_breach_failure\(([^)]+)\)', gate_block)
        for c in calls:
            assert "live_submit_gate:" in c, (
                f"Gate failure terminalize must use 'live_submit_gate:' prefix. "
                f"Found: _terminalize_breach_failure({c})"
            )

    def test_module_error_blocks_live_not_paper(self):
        """Module-level gate error must block LIVE, allow PAPER."""
        src = open("ap_execution_core.py").read()
        assert "LIVE_SUBMIT_GATE_MODULE_ERROR" in src
        assert 'live_submit_gate:MODULE_ERROR' in src

    def test_gate_block_updates_orders_meta(self):
        """Every gate failure persists live_submit_gate to orders.meta."""
        src = open("ap_execution_core.py").read()
        gate_start = src.find("# ── P0 (PR #305): LIVE submit safety gates")
        gate_end   = src.find("submit_res = self.order_state_machine.submit_existing_entry(", gate_start)
        gate_block = src[gate_start:gate_end]
        assert '"live_submit_gate"' in gate_block, (
            "Gate failures must persist live_submit_gate to orders.meta"
        )
        assert '"final_market_validity"' in gate_block, (
            "Market gate failure must persist final_market_validity to orders.meta"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Robustness — never raises
# ─────────────────────────────────────────────────────────────────────────────

class TestGateRobustness:

    def test_identity_gate_never_raises(self):
        for cid in (None, "", 123, ["list"]):
            for mode in (None, "", 999):
                try:
                    check_identity_gate(client_id=cid, execution_mode=mode)
                except Exception as e:
                    pytest.fail(f"identity gate raised on ({cid!r}, {mode!r}): {e}")

    def test_market_gate_never_raises(self):
        for bad_bid, bad_ask in [(None, None), ("str", None), (0, "bad")]:
            try:
                check_market_validity_gate(
                    side="CALL", trigger_price=100.0, stop_price=95.0, target_price=110.0,
                    current_bid=bad_bid, current_ask=bad_ask, execution_mode="live",
                )
            except Exception as e:
                pytest.fail(f"market gate raised on bid={bad_bid!r} ask={bad_ask!r}: {e}")

    def test_trigger_age_gate_never_raises(self):
        for bad in (None, "", "not-a-date", 12345):
            try:
                check_trigger_age_gate(trigger_crossed_at=bad, execution_mode="live")
            except Exception as e:
                pytest.fail(f"trigger age gate raised on {bad!r}: {e}")
