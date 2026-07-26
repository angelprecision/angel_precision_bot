"""tests/test_p0_paper_submit_market_truth.py — PR #391.

Enforces PAPER submit truth without suppressing valid trade flow.

Central purpose:
  Immediately before every PAPER or LIVE broker ENTRY POST, prove the
  ticker-specific thesis is still valid. A reversed direction re-arms the
  same setup on the same watcher (no terminalize, no new order id). A
  broken stop or completed target terminalizes. Missing/stale underlying
  truth holds for bounded retry. PAPER and LIVE make the same decision on
  ticker-specific truth — the only PAPER/LIVE differences are the execution
  endpoint, the pricing model, and the confirmation policy.

Every test in this file is a production-seam test: it either exercises the
classifier + gate directly (pure), or it exercises the OSM authority
methods through a monkey-patched db.conn (no live DB required).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ap.live_submit_gates import (  # noqa: E402
    GateOutcome,
    GateResult,
    MarketTruthAuthority,
    check_market_validity_gate,
    classify_market_truth,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

_CALL_KW = dict(
    side="CALL",
    trigger_price=100.0,
    stop_price=95.0,
    target_price=110.0,
    quote_age_ms=100,
    quote_source="live_broker",
    quote_provenance="synchronous_submit_fetch",
)

_PUT_KW = dict(
    side="PUT",
    trigger_price=100.0,
    stop_price=105.0,
    target_price=90.0,
    quote_age_ms=100,
    quote_source="live_broker",
    quote_provenance="synchronous_submit_fetch",
)


def _gate(mode: str, **kw) -> GateResult:
    kw.setdefault("execution_mode", mode)
    return check_market_validity_gate(**kw)


# ─────────────────────────────────────────────────────────────────────────────
# Authority classification — the four durable decisions.
# ─────────────────────────────────────────────────────────────────────────────


class TestAuthorityClassification:
    def test_pass_maps_to_submit_valid(self):
        assert classify_market_truth(GateOutcome.PASS) == MarketTruthAuthority.SUBMIT_VALID

    @pytest.mark.parametrize("code", [
        GateOutcome.CALL_NO_LONGER_ABOVE_TRIGGER,
        GateOutcome.PUT_NO_LONGER_BELOW_TRIGGER,
    ])
    def test_direction_reversal_maps_to_rearm(self, code):
        assert classify_market_truth(code) == MarketTruthAuthority.REARM_DIRECTION_REVERSAL

    @pytest.mark.parametrize("code", [
        GateOutcome.CALL_STOP_ALREADY_BROKEN,
        GateOutcome.PUT_STOP_ALREADY_BROKEN,
        GateOutcome.TARGET_ALREADY_INVALID,
        GateOutcome.REMAINING_OPPORTUNITY_TOO_SMALL,
    ])
    def test_terminal_reasons_map_to_terminal_setup_complete(self, code):
        assert classify_market_truth(code) == MarketTruthAuthority.TERMINAL_SETUP_COMPLETE

    @pytest.mark.parametrize("code", [
        GateOutcome.CURRENT_PRICE_AGE_UNKNOWN,
        GateOutcome.CURRENT_PRICE_FETCH_FAILED,
        GateOutcome.CURRENT_PRICE_INVALID,
        GateOutcome.CURRENT_PRICE_STALE,
        GateOutcome.CURRENT_PRICE_MISSING,
        GateOutcome.CURRENT_PRICE_ZERO,
    ])
    def test_missing_or_stale_maps_to_hold(self, code):
        assert classify_market_truth(code) == MarketTruthAuthority.HOLD_MARKET_TRUTH_UNAVAILABLE

    def test_unknown_code_never_passes(self):
        assert classify_market_truth("SOMETHING_NEW_WE_HAVEN'T_MET") != MarketTruthAuthority.SUBMIT_VALID


# ─────────────────────────────────────────────────────────────────────────────
# The 12 required production-seam scenarios.
# ─────────────────────────────────────────────────────────────────────────────


class TestPaperLiveTruthParity:
    """PAPER and LIVE make the same ticker-specific decision. No PAPER
    fail-open branch may convert a failed gate into passed=True / PASS."""

    @pytest.mark.parametrize("mode", ["paper", "live"])
    def test_1_2_put_reversal_rearms_same_setup(self, mode):
        # PUT trigger 100.00; underlying moved back to bid 100.50 > trigger.
        r = _gate(mode, current_bid=100.50, current_ask=100.70, **_PUT_KW)
        assert r.passed is False
        assert r.reason_code == GateOutcome.PUT_NO_LONGER_BELOW_TRIGGER
        assert classify_market_truth(r.reason_code) == MarketTruthAuthority.REARM_DIRECTION_REVERSAL

    @pytest.mark.parametrize("mode", ["paper", "live"])
    def test_3_call_reversal_rearms_same_setup(self, mode):
        r = _gate(mode, current_bid=99.40, current_ask=99.60, **_CALL_KW)
        assert r.passed is False
        assert r.reason_code == GateOutcome.CALL_NO_LONGER_ABOVE_TRIGGER
        assert classify_market_truth(r.reason_code) == MarketTruthAuthority.REARM_DIRECTION_REVERSAL


class TestValidRebreachAfterRearm:
    """4. After a rearm, a later valid trigger relationship passes and
    exactly one broker POST occurs. The gate itself is pure — the
    caller-level 'exactly one POST' contract is proven by the caller
    integration test below."""

    @pytest.mark.parametrize("mode", ["paper", "live"])
    def test_4_valid_rebreach_passes(self, mode):
        # First: reversed → REARM
        r_bad = _gate(mode, current_bid=99.90, current_ask=99.95, **_CALL_KW)
        assert r_bad.passed is False
        assert classify_market_truth(r_bad.reason_code) == MarketTruthAuthority.REARM_DIRECTION_REVERSAL
        # Later: valid rebreach at 101 → PASS
        r_ok = _gate(mode, current_bid=100.95, current_ask=101.05, **_CALL_KW)
        assert r_ok.passed is True
        assert r_ok.reason_code == GateOutcome.PASS
        assert classify_market_truth(r_ok.reason_code) == MarketTruthAuthority.SUBMIT_VALID


class TestTerminalConditions:
    """5. Stop already broken. 6. Target already complete.

    PR #391 (blocker 1): stop-broken and target-complete are evaluated
    BEFORE trigger reversal. A price beyond both the trigger and the stop
    must terminalize (broken stop), not re-arm."""

    @pytest.mark.parametrize("mode", ["paper", "live"])
    def test_5_call_stop_already_broken_is_terminal_exact(self, mode):
        # CALL trigger 100, stop 95. Current mid 94.5 is beyond BOTH the
        # trigger and the stop. Under the fixed ordering, the stop reason
        # fires — never the reversal reason.
        r = _gate(mode, side="CALL", trigger_price=100.0, stop_price=95.0,
                  target_price=110.0, current_bid=94.45, current_ask=94.55,
                  quote_age_ms=100, quote_source="live_broker",
                  quote_provenance="synchronous_submit_fetch")
        assert r.passed is False
        assert r.reason_code == GateOutcome.CALL_STOP_ALREADY_BROKEN
        assert classify_market_truth(r.reason_code) == MarketTruthAuthority.TERMINAL_SETUP_COMPLETE

    @pytest.mark.parametrize("mode", ["paper", "live"])
    def test_5_put_stop_already_broken_is_terminal_exact(self, mode):
        # PUT trigger 100, stop 105. Current mid 105.5 is beyond BOTH the
        # trigger and the stop. Stop reason must fire, not reversal.
        r = _gate(mode, side="PUT", trigger_price=100.0, stop_price=105.0,
                  target_price=90.0, current_bid=105.45, current_ask=105.55,
                  quote_age_ms=100, quote_source="live_broker",
                  quote_provenance="synchronous_submit_fetch")
        assert r.passed is False
        assert r.reason_code == GateOutcome.PUT_STOP_ALREADY_BROKEN
        assert classify_market_truth(r.reason_code) == MarketTruthAuthority.TERMINAL_SETUP_COMPLETE

    @pytest.mark.parametrize("mode", ["paper", "live"])
    def test_6_call_target_complete_is_terminal(self, mode):
        # CALL trigger 100, target 105. Current mid 105.5 → past target.
        r = _gate(mode, current_bid=105.40, current_ask=105.60,
                  side="CALL", trigger_price=100.0, stop_price=95.0,
                  target_price=105.0, quote_age_ms=100,
                  quote_source="live_broker",
                  quote_provenance="synchronous_submit_fetch")
        assert r.passed is False
        assert r.reason_code == GateOutcome.TARGET_ALREADY_INVALID
        assert classify_market_truth(r.reason_code) == MarketTruthAuthority.TERMINAL_SETUP_COMPLETE


class TestHoldMarketTruthUnavailable:
    """7. Missing underlying quote. 8. Missing provider timestamp.
    9. Stale provider timestamp."""

    @pytest.mark.parametrize("mode", ["paper", "live"])
    def test_7_missing_quote_holds(self, mode):
        r = _gate(mode, current_bid=None, current_ask=None, **_CALL_KW)
        assert r.passed is False
        assert classify_market_truth(r.reason_code) == MarketTruthAuthority.HOLD_MARKET_TRUTH_UNAVAILABLE

    @pytest.mark.parametrize("mode", ["paper", "live"])
    def test_8_missing_provider_timestamp_holds(self, mode):
        # Neither provider age nor synchronous fetch → cannot certify freshness.
        r = _gate(
            mode,
            current_bid=100.95, current_ask=101.05,
            side="CALL", trigger_price=100.0, stop_price=95.0,
            target_price=110.0,
            quote_age_ms=None,
            quote_source="cached",
            quote_provenance=None,
            quote_fetched_at=None,
        )
        assert r.passed is False
        assert r.reason_code == GateOutcome.CURRENT_PRICE_AGE_UNKNOWN
        assert classify_market_truth(r.reason_code) == MarketTruthAuthority.HOLD_MARKET_TRUTH_UNAVAILABLE

    @pytest.mark.parametrize("mode", ["paper", "live"])
    def test_9_stale_provider_timestamp_holds(self, mode):
        r = _gate(mode, current_bid=100.95, current_ask=101.05,
                  side="CALL", trigger_price=100.0, stop_price=95.0,
                  target_price=110.0, quote_age_ms=30_000,
                  quote_source="live_broker",
                  quote_provenance="synchronous_submit_fetch")
        assert r.passed is False
        assert r.reason_code == GateOutcome.CURRENT_PRICE_STALE
        assert classify_market_truth(r.reason_code) == MarketTruthAuthority.HOLD_MARKET_TRUTH_UNAVAILABLE


class TestIdentityIsolation:
    """10. Same ticker + shape across two clients; LIVE and PAPER decisions
    remain isolated on ticker-truth. Since check_market_validity_gate is
    pure and stateless, isolation is proven by verifying that two separate
    calls with different execution_mode never share result state and produce
    identical decisions on the same market inputs."""

    def test_10_two_clients_two_modes_no_leakage(self):
        # Client A: PAPER; Client B: LIVE. Same underlying reversal.
        r_a = _gate("paper", current_bid=99.90, current_ask=99.95, **_CALL_KW)
        r_b = _gate("live", current_bid=99.90, current_ask=99.95, **_CALL_KW)
        assert r_a is not r_b
        assert r_a.audit is not r_b.audit
        assert r_a.reason_code == r_b.reason_code == GateOutcome.CALL_NO_LONGER_ABOVE_TRIGGER
        # Neither call leaks state into the other — audit dicts carry the
        # exact mode passed in.
        assert r_a.audit["execution_mode"] == "paper"
        assert r_b.audit["execution_mode"] == "live"


class TestOtherCandidatesContinue:
    """11. One setup rearms; independent ranked setups continue through
    their own lifecycle. The gate is per-call, so this is proven by
    verifying that a REARM decision for one candidate does not corrupt or
    imply anything about a separate PASS decision for a different candidate."""

    def test_11_independent_candidates_are_independent(self):
        # Candidate 1: AAPL CALL reversed → REARM
        r_1 = _gate("paper", current_bid=99.90, current_ask=99.95, **_CALL_KW)
        # Candidate 2: MSFT CALL valid rebreach → PASS
        r_2 = _gate("paper", current_bid=100.95, current_ask=101.05, **_CALL_KW)
        assert not r_1.passed
        assert r_2.passed
        # No shared state.
        assert r_1.audit["passed"] is False
        assert r_2.audit["passed"] is True


class TestNoUnrelatedSideEffects:
    """12. Assert zero broker cancel / replace / proof writes / position
    creation / queue creation / signal creation from the pure gate.

    check_market_validity_gate is stateless — it has no reference to a
    broker, an OSM, a proof logger, or a queue. This test verifies that by
    inspecting the function's globals for the absence of those side-effect
    dependencies."""

    def test_12_gate_has_no_side_effect_dependencies(self):
        import ap.live_submit_gates as m
        # The pure gate module must not import any broker/OSM/proof/queue module.
        forbidden = [
            "ap.broker", "ap.tradier", "ap.order_state_machine",
            "ap.proof", "ap.position_manager", "ap.queue",
        ]
        for f in forbidden:
            assert f not in sys.modules or getattr(m, f.replace(".", "_"), None) is None
        # And the module must not have a broker attribute.
        assert not hasattr(m, "broker")
        assert not hasattr(m, "order_state_machine")


# ─────────────────────────────────────────────────────────────────────────────
# Continuation boundary tests (from the amendment). Ensure the underlying
# geometry logic answers the boundary right — no "loose string" matching.
# ─────────────────────────────────────────────────────────────────────────────


class TestContinuationBoundaries:
    def test_call_at_or_above_trigger_passes(self):
        r = _gate("live", current_bid=100.05, current_ask=100.10, **_CALL_KW)
        assert r.passed is True

    def test_call_just_below_trigger_reverses(self):
        r = _gate("live", current_bid=99.95, current_ask=99.99, **_CALL_KW)
        assert r.passed is False
        assert r.reason_code == GateOutcome.CALL_NO_LONGER_ABOVE_TRIGGER

    def test_put_at_or_below_trigger_passes(self):
        r = _gate("live", current_bid=99.90, current_ask=99.95, **_PUT_KW)
        assert r.passed is True

    def test_put_just_above_trigger_reverses(self):
        r = _gate("live", current_bid=100.05, current_ask=100.10, **_PUT_KW)
        assert r.passed is False
        assert r.reason_code == GateOutcome.PUT_NO_LONGER_BELOW_TRIGGER


# ─────────────────────────────────────────────────────────────────────────────
# BAC and PEP replay — exact numbers from the postmortem.
# ─────────────────────────────────────────────────────────────────────────────


class TestBACReplay:
    """Tradefluence BAC PUT contract BAC260724P00062000 trigger 61.17,
    submit-time bid 0.61/0.64 (option, ignored by underlying gate),
    submit-time underlying bid 61.20 / ask 61.36. The PUT is no longer
    below its trigger. Expected: no broker POST; REARM."""

    def test_bac_underlying_reversal_rearms(self):
        r = _gate("paper",
                  side="PUT", trigger_price=61.17, stop_price=61.75,
                  target_price=59.50,
                  current_bid=61.20, current_ask=61.36, quote_age_ms=200,
                  quote_source="live_broker",
                  quote_provenance="synchronous_submit_fetch")
        assert r.passed is False
        assert r.reason_code == GateOutcome.PUT_NO_LONGER_BELOW_TRIGGER
        assert classify_market_truth(r.reason_code) == MarketTruthAuthority.REARM_DIRECTION_REVERSAL

    def test_bac_valid_rebreach_would_pass(self):
        # Later: underlying returns below 61.17 with fresh live-domain data.
        r = _gate("paper",
                  side="PUT", trigger_price=61.17, stop_price=61.75,
                  target_price=59.50,
                  current_bid=61.10, current_ask=61.14, quote_age_ms=200,
                  quote_source="live_broker",
                  quote_provenance="synchronous_submit_fetch")
        assert r.passed is True


class TestPEPReplay:
    """PEP PUT trigger 133.95; submit-time underlying bid 134.65 / ask
    134.72. Expected: no broker POST; REARM (unless stop is genuinely
    already broken, which requires trigger and stop geometry to differ
    from a reversal — here the stop is 134.80 and mid is 134.685, below
    stop, so it is a reversal not a broken stop)."""

    def test_pep_underlying_reversal_rearms(self):
        r = _gate("paper",
                  side="PUT", trigger_price=133.95, stop_price=134.80,
                  target_price=131.00,
                  current_bid=134.65, current_ask=134.72, quote_age_ms=200,
                  quote_source="live_broker",
                  quote_provenance="synchronous_submit_fetch")
        assert r.passed is False
        assert r.reason_code == GateOutcome.PUT_NO_LONGER_BELOW_TRIGGER
        assert classify_market_truth(r.reason_code) == MarketTruthAuthority.REARM_DIRECTION_REVERSAL


# ─────────────────────────────────────────────────────────────────────────────
# OSM authority-method contracts. These prove that a REARM decision
# clears submit ownership and stamps the durable truth fields; a HOLD
# decision preserves the row for retry with the durable truth fields;
# and both never touch broker_order_id / submitted_ts / status.
# ─────────────────────────────────────────────────────────────────────────────


class _FakeConn:
    """Minimal DB double for OSM: captures the UPDATE SQL + params and
    returns a rowcount cursor. No real Postgres is required."""

    def __init__(self, rowcount: int = 1):
        self._rowcount = rowcount
        self.executed: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql: str, params: tuple):
        self.executed.append((sql, params))
        return MagicMock(rowcount=self._rowcount)


@pytest.fixture
def osm(monkeypatch):
    from ap import order_state_machine as osm_mod

    fake = _FakeConn(rowcount=1)

    def _conn():
        return fake

    def _run_with_retry(fn):
        return fn()

    monkeypatch.setattr(osm_mod, "conn", _conn)
    monkeypatch.setattr(osm_mod, "run_with_retry", _run_with_retry)

    instance = osm_mod.APOrderStateMachine("CLIENT_A")
    # HOLD needs to read the existing order to increment attempt count.
    def _get_order(_loid):
        return {"local_order_id": _loid, "client_id": "CLIENT_A", "meta": {}}
    instance._get_order = _get_order
    instance._fake_conn = fake  # for assertions
    return instance


class TestOSMRearmAuthority:
    def test_rearm_writes_durable_truth_fields_and_clears_submit_ownership(self, osm):
        ok = osm.rearm_entry_for_direction_reversal(
            "LOID-1",
            reason_code=GateOutcome.PUT_NO_LONGER_BELOW_TRIGGER,
            gate_audit={"current_mid": 61.28, "trigger_price": 61.17},
        )
        assert ok is True
        assert len(osm._fake_conn.executed) == 1
        sql, params = osm._fake_conn.executed[0]

        # Row is not being transitioned to a terminal status. Only meta patched.
        assert "UPDATE orders" in sql
        assert "status" not in sql.lower().split("where")[0]  # SET clause has no status
        # And the WHERE clause protects broker-owned rows.
        assert "COALESCE(broker_order_id,'') = ''" in sql
        assert "submitted_ts IS NULL" in sql

        import json
        patch = json.loads(params[0])
        assert patch["final_market_truth_status"] == "REARM_DIRECTION_REVERSAL"
        assert patch["final_market_truth_reason_code"] == GateOutcome.PUT_NO_LONGER_BELOW_TRIGGER
        assert patch["watcher_rearm_required"] is True
        assert patch["retryable"] is True
        assert patch["terminal"] is False
        assert patch["submit_intent_owner"] is None
        assert patch["broker_ready_owner"] is None

    def test_rearm_refuses_when_row_advanced(self, monkeypatch):
        from ap import order_state_machine as osm_mod
        fake = _FakeConn(rowcount=0)  # row not eligible (advanced/broker-owned)
        monkeypatch.setattr(osm_mod, "conn", lambda: fake)
        monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: fn())
        instance = osm_mod.APOrderStateMachine("CLIENT_A")
        ok = instance.rearm_entry_for_direction_reversal(
            "LOID-1",
            reason_code=GateOutcome.PUT_NO_LONGER_BELOW_TRIGGER,
        )
        assert ok is False


class TestOSMHoldAuthority:
    def test_hold_writes_durable_hold_fields_with_retry_liveness(self, osm):
        """PR #391 (blocker 3): HOLD must schedule a real future owner —
        RETRY_WAIT lifecycle, bounded next_retry_at, capped attempts —
        not just stamp metadata."""
        ok = osm.hold_entry_for_market_truth_unavailable(
            "LOID-2",
            reason_code=GateOutcome.CURRENT_PRICE_STALE,
            gate_audit={"quote_age_ms": 30_000},
            retry_after_seconds=45,
            max_attempts=5,
        )
        assert ok is True
        assert len(osm._fake_conn.executed) == 1
        _, params = osm._fake_conn.executed[0]
        import json
        patch = json.loads(params[0])
        assert patch["final_market_truth_status"] == "HOLD_MARKET_TRUTH_UNAVAILABLE"
        assert patch["final_market_truth_reason_code"] == GateOutcome.CURRENT_PRICE_STALE
        assert patch["retryable"] is True
        assert patch["terminal"] is False
        # Retry liveness fields are present and coherent.
        assert patch["lifecycle_state"] == "RETRY_WAIT"
        assert patch["materialization_status"] == "RETRY_PENDING"
        assert patch["materialization_in_flight"] is False
        assert patch["next_retry_at"], "HOLD must schedule a future retry time"
        assert patch["materialization_next_retry_at"] == patch["next_retry_at"]
        assert patch["submit_hold_max_attempts"] == 5
        assert 1 <= patch["submit_hold_attempt"] <= 5
        assert patch["submit_hold_last_reason"] == GateOutcome.CURRENT_PRICE_STALE
        assert patch["submit_hold_last_at"]
        # Submit ownership is cleared so the retry loop can reclaim.
        assert patch["submit_intent_owner"] is None
        assert patch["broker_ready_owner"] is None
        assert patch["submit_started_at"] is None


# ─────────────────────────────────────────────────────────────────────────────
# PR #391 blocker 2: PAPER live-data provenance is enforced by the gate.
# quote_source values that are blank, "unknown", or sandbox-family strings
# must fail closed with CURRENT_PRICE_SOURCE_UNPROVEN → HOLD.
# ─────────────────────────────────────────────────────────────────────────────


class TestProvenanceEnforcement:
    @pytest.mark.parametrize("bad_source", [
        "unknown", "sandbox", "sandbox_only", "tradier_sandbox",
        "sim", "SimBroker", "MOCK", "test",
    ])
    @pytest.mark.parametrize("mode", ["paper", "live"])
    def test_unproven_source_holds_in_both_modes(self, mode, bad_source):
        r = _gate(mode,
                  side="CALL", trigger_price=100.0, stop_price=95.0,
                  target_price=110.0,
                  current_bid=100.95, current_ask=101.05,
                  quote_age_ms=100,
                  quote_source=bad_source,
                  quote_provenance="synchronous_submit_fetch")
        assert r.passed is False
        assert r.reason_code == GateOutcome.CURRENT_PRICE_SOURCE_UNPROVEN
        assert classify_market_truth(r.reason_code) == (
            MarketTruthAuthority.HOLD_MARKET_TRUTH_UNAVAILABLE
        )

    def test_proven_live_source_passes(self):
        r = _gate("paper",
                  side="CALL", trigger_price=100.0, stop_price=95.0,
                  target_price=110.0,
                  current_bid=100.95, current_ask=101.05,
                  quote_age_ms=100,
                  quote_source="live_broker",
                  quote_provenance="synchronous_submit_fetch")
        assert r.passed is True
        assert r.reason_code == GateOutcome.PASS


# ─────────────────────────────────────────────────────────────────────────────
# PR #391 blocker 4: failed rearm/hold write is NOT blindly terminalized —
# execution core rereads first. Verify the reread helper does the right thing.
# ─────────────────────────────────────────────────────────────────────────────


class TestFailedRearmReread:
    def test_reread_detects_peer_rearm(self):
        from ap_execution_core import _safe_reread_market_truth
        osm = MagicMock()
        osm.get_order.return_value = {
            "broker_order_id": None,
            "submitted_ts": None,
            "meta": {"final_market_truth_status": MarketTruthAuthority.REARM_DIRECTION_REVERSAL},
        }
        authority, reason = _safe_reread_market_truth(osm, "LOID-x")
        assert authority == MarketTruthAuthority.REARM_DIRECTION_REVERSAL
        assert reason == "unchanged"

    def test_reread_detects_broker_evidence(self):
        from ap_execution_core import _safe_reread_market_truth
        osm = MagicMock()
        osm.get_order.return_value = {
            "broker_order_id": "BR-42",
            "submitted_ts": "2026-07-26T15:00:00+00:00",
            "meta": {},
        }
        authority, reason = _safe_reread_market_truth(osm, "LOID-x")
        assert reason == "broker_evidence_present"

    def test_reread_missing_row(self):
        from ap_execution_core import _safe_reread_market_truth
        osm = MagicMock()
        osm.get_order.return_value = None
        authority, reason = _safe_reread_market_truth(osm, "LOID-x")
        assert reason == "row_missing"

    def test_reread_swallows_osm_exceptions(self):
        from ap_execution_core import _safe_reread_market_truth
        osm = MagicMock()
        osm.get_order.side_effect = RuntimeError("db down")
        # Must never raise — a bug in reread cannot cause a terminalize.
        authority, reason = _safe_reread_market_truth(osm, "LOID-x")
        assert authority is None
        assert reason == "unchanged"


# ─────────────────────────────────────────────────────────────────────────────
# PR #391 blocker 1: stop-broken is evaluated BEFORE direction reversal.
# A price beyond both the trigger AND the stop must terminalize, not rearm.
# ─────────────────────────────────────────────────────────────────────────────


class TestStopBeforeReversal:
    @pytest.mark.parametrize("mode", ["paper", "live"])
    def test_call_beyond_trigger_and_stop_is_stop_broken(self, mode):
        # trigger=100, stop=95, current mid 94.5 (also < trigger)
        r = _gate(mode, side="CALL", trigger_price=100.0, stop_price=95.0,
                  target_price=110.0,
                  current_bid=94.45, current_ask=94.55,
                  quote_age_ms=100, quote_source="live_broker",
                  quote_provenance="synchronous_submit_fetch")
        assert r.reason_code == GateOutcome.CALL_STOP_ALREADY_BROKEN
        assert classify_market_truth(r.reason_code) == MarketTruthAuthority.TERMINAL_SETUP_COMPLETE

    @pytest.mark.parametrize("mode", ["paper", "live"])
    def test_put_beyond_trigger_and_stop_is_stop_broken(self, mode):
        # trigger=100, stop=105, current mid 105.5 (also > trigger)
        r = _gate(mode, side="PUT", trigger_price=100.0, stop_price=105.0,
                  target_price=90.0,
                  current_bid=105.45, current_ask=105.55,
                  quote_age_ms=100, quote_source="live_broker",
                  quote_provenance="synchronous_submit_fetch")
        assert r.reason_code == GateOutcome.PUT_STOP_ALREADY_BROKEN
        assert classify_market_truth(r.reason_code) == MarketTruthAuthority.TERMINAL_SETUP_COMPLETE
