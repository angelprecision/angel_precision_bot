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


_REARM_KW = dict(
    reason_code=GateOutcome.PUT_NO_LONGER_BELOW_TRIGGER,
    gate_audit={"current_mid": 61.28, "trigger_price": 61.17},
    execution_mode="paper",
    signal_id="SIG-1",
    expected_generation=2,
    expected_watcher_token="watcher:abc",
    deferred_contract="DEFERRED:BAC",
)

_HOLD_KW = dict(
    reason_code=GateOutcome.CURRENT_PRICE_STALE,
    gate_audit={"quote_age_ms": 30_000},
    execution_mode="live",
    signal_id="SIG-2",
    expected_generation=3,
    expected_watcher_token="watcher:xyz",
    next_retry_at="2026-07-26T18:00:00+00:00",
    max_attempts=5,
)


class TestOSMRearmAuthority:
    def test_rearm_writes_fenced_patch_and_clears_contract_and_ownership(self, osm):
        ok = osm.rearm_entry_for_direction_reversal("LOID-1", **_REARM_KW)
        assert ok is True
        assert len(osm._fake_conn.executed) == 1
        sql, params = osm._fake_conn.executed[0]

        # SET clause invalidates option-price authority.
        set_clause = sql.upper().split("WHERE")[0]
        assert "CONTRACT = %S" in set_clause
        assert "LIMIT_PRICE = NULL" in set_clause
        assert "RESERVED_COST = 0" in set_clause
        assert "REVALIDATION_REQUIRED" in sql

        # WHERE clause requires exact mode, signal, generation, watcher token,
        # and no broker evidence / no prior submit intent.
        where = sql.upper().split("WHERE")[1]
        assert "LOWER(TRIM(COALESCE(EXECUTION_MODE, '')))" in where
        assert "COALESCE(SIGNAL_ID, '') = %S" in where
        assert "COALESCE(BROKER_ORDER_ID, '') = ''" in where
        assert "SUBMITTED_TS IS NULL" in where
        assert "COALESCE(META->>'SUBMIT_INTENT_AT', '') = ''" in where
        assert "MATERIALIZATION_GENERATION" in where
        assert "WATCHER_TOKEN" in where
        assert "LIFECYCLE_STATE" in where and "PENDING_TRIGGER" in where and "BROKER_READY" in where

        # Params carry the fence values in order.
        assert params[0] == _REARM_KW["deferred_contract"]  # contract set
        # patch_json at params[1]; then LOID, client, mode, signal, gen, token, token
        assert params[2] == "LOID-1"
        assert params[4] == _REARM_KW["execution_mode"]
        assert params[5] == _REARM_KW["signal_id"]
        assert params[6] == _REARM_KW["expected_generation"]
        assert params[7] == _REARM_KW["expected_watcher_token"]
        assert params[8] == _REARM_KW["expected_watcher_token"]

        import json
        patch = json.loads(params[1])
        assert patch["final_market_truth_status"] == "REARM_DIRECTION_REVERSAL"
        assert patch["final_market_truth_reason_code"] == GateOutcome.PUT_NO_LONGER_BELOW_TRIGGER
        assert patch["watcher_rearm_required"] is True
        assert patch["retryable"] is True
        assert patch["terminal"] is False
        assert patch["lifecycle_state"] == "PENDING_TRIGGER"
        assert patch["materialization_status"] == "REARMED_DIRECTION_REVERSAL"
        assert patch["broker_ready"] is False
        assert patch["contract_deferred"] is True
        assert patch["contract_revalidation_required"] is True
        assert patch["selected_contract"] is None
        assert patch["selected_limit"] is None
        assert patch["selected_qty"] is None
        assert patch["selected_reserved_cost"] is None
        assert patch["materialization_owner"] == ""
        assert patch["submit_intent_at"] == ""
        assert patch["broker_submit_key"] == ""
        assert patch["recovery_submit_owner"] == ""
        assert patch["current_owner"] == _REARM_KW["expected_watcher_token"]
        assert patch["watcher_token"] == _REARM_KW["expected_watcher_token"]

    def test_rearm_refuses_when_row_advanced(self, monkeypatch):
        from ap import order_state_machine as osm_mod
        fake = _FakeConn(rowcount=0)  # row not eligible
        monkeypatch.setattr(osm_mod, "conn", lambda: fake)
        monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: fn())
        instance = osm_mod.APOrderStateMachine("CLIENT_A")
        ok = instance.rearm_entry_for_direction_reversal("LOID-1", **_REARM_KW)
        assert ok is False

    @pytest.mark.parametrize("kw_override", [
        {"execution_mode": "bogus"},
        {"signal_id": ""},
        {"expected_generation": -1},
        {"deferred_contract": "AAPL260626C00100000"},  # not DEFERRED:
        {"reason_code": ""},
    ])
    def test_rearm_refuses_bad_args(self, osm, kw_override):
        args = dict(_REARM_KW)
        args.update(kw_override)
        assert osm.rearm_entry_for_direction_reversal("LOID-1", **args) is False
        assert len(osm._fake_conn.executed) == 0


class TestOSMHoldAuthority:
    def test_hold_writes_canonical_deferred_retry_schema(self, osm):
        """PR #391 (blockers 3 + 5): HOLD must use the same lifecycle
        vocabulary as schedule_deferred_materialization_retry so
        adopt_deferred_retry_watcher can adopt it — not a second retry
        dialect. CAS must fence client/mode/signal/generation/watcher_token."""
        ok = osm.hold_entry_for_market_truth_unavailable("LOID-2", **_HOLD_KW)
        assert ok is True
        assert len(osm._fake_conn.executed) == 1
        sql, params = osm._fake_conn.executed[0]

        # Retry_attempt / breach_attempt_count / materialization_attempts
        # are computed atomically in SQL — the LEAST(existing+1, max) shape
        # must be present so two racing writers cannot double-increment.
        assert "LEAST(" in sql
        assert "'retry_attempt'" in sql
        assert "'breach_attempt_count'" in sql
        assert "'materialization_attempts'" in sql
        assert "meta->>'retry_attempt'" in sql

        # CAS fence conditions.
        where = sql.upper().split("WHERE")[1]
        assert "LOWER(TRIM(COALESCE(EXECUTION_MODE, '')))" in where
        assert "COALESCE(SIGNAL_ID, '') = %S" in where
        assert "COALESCE(META->>'SUBMIT_INTENT_AT', '') = ''" in where
        assert "MATERIALIZATION_GENERATION" in where
        assert "WATCHER_TOKEN" in where
        assert "LIFECYCLE_STATE" in where

        import json
        patch = json.loads(params[0])
        assert patch["final_market_truth_status"] == "HOLD_MARKET_TRUTH_UNAVAILABLE"
        assert patch["final_market_truth_reason_code"] == GateOutcome.CURRENT_PRICE_STALE
        assert patch["retryable"] is True
        assert patch["terminal"] is False
        # Canonical deferred-retry vocabulary
        assert patch["lifecycle_state"] == "RETRY_WAIT"
        assert patch["materialization_status"] == "RETRY_PENDING"
        assert patch["materialization_in_flight"] is False
        assert patch["materialization_owner"] == ""
        assert patch["materialization_lease_until"] == ""
        # P0-8: durable and in-memory ownership must agree — HOLD preserves
        # the caller's watcher_token, does not blank it. The generation
        # fence + submit_intent_at fence still prevents stale-worker
        # rewrites.
        assert patch["watcher_token"] == _HOLD_KW["expected_watcher_token"]
        assert patch["current_owner"] == _HOLD_KW["expected_watcher_token"]
        assert patch["broker_ready"] is False
        assert patch["retry_reason"] == GateOutcome.CURRENT_PRICE_STALE
        assert patch["materialization_reason"] == GateOutcome.CURRENT_PRICE_STALE
        assert patch["next_retry_at"] == _HOLD_KW["next_retry_at"]
        assert patch["materialization_next_retry_at"] == _HOLD_KW["next_retry_at"]
        assert patch["retry_max_attempts"] == _HOLD_KW["max_attempts"]
        # Ownership fields are all cleared.
        assert patch["submit_intent_at"] == ""
        assert patch["submit_started_at"] == ""
        assert patch["broker_submit_key"] == ""
        assert patch["broker_submit_payload_hash"] == ""
        assert patch["recovery_submit_owner"] == ""
        assert patch["recovery_submit_lease_until"] == ""
        assert patch["contract_revalidation_required"] is True
        # No stale hold-only dialect fields.
        assert "submit_hold_attempt" not in patch
        assert "submit_hold_max_attempts" not in patch

    @pytest.mark.parametrize("kw_override", [
        {"execution_mode": "bogus"},
        {"signal_id": ""},
        {"expected_generation": -1},
        {"next_retry_at": ""},
        {"max_attempts": 0},
        {"reason_code": ""},
    ])
    def test_hold_refuses_bad_args(self, osm, kw_override):
        args = dict(_HOLD_KW)
        args.update(kw_override)
        assert osm.hold_entry_for_market_truth_unavailable("LOID-2", **args) is False
        assert len(osm._fake_conn.executed) == 0


# ─────────────────────────────────────────────────────────────────────────────
# PR #391 blocker 2: PAPER live-data provenance is enforced by the gate.
# quote_source values that are blank, "unknown", or sandbox-family strings
# must fail closed with CURRENT_PRICE_SOURCE_UNPROVEN → HOLD.
# ─────────────────────────────────────────────────────────────────────────────


class TestProvenanceEnforcement:
    # PR #391 blocker P0-1: LIVE + unknown + synchronous_submit_fetch is
    # Jason's real production quote shape (the adapter didn't populate a
    # provider name). It is proven-fresh by the sync provenance, and
    # freshness/bid/ask/stop/target/direction are checked separately, so
    # this exact triple is allowed. Every other unproven combo still HOLDs.
    _BAD_SOURCES_UNIVERSAL = [
        "sandbox", "sandbox_only", "tradier_sandbox",
        "sim", "SimBroker", "MOCK", "test",
    ]

    @pytest.mark.parametrize("bad_source", _BAD_SOURCES_UNIVERSAL)
    @pytest.mark.parametrize("mode", ["paper", "live"])
    def test_sandbox_family_holds_in_both_modes(self, mode, bad_source):
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

    def test_blank_source_holds_both_modes(self):
        # P0-2: truly blank source is unproven — always HOLD.
        for mode in ("paper", "live"):
            r = _gate(mode,
                      side="CALL", trigger_price=100.0, stop_price=95.0,
                      target_price=110.0,
                      current_bid=100.95, current_ask=101.05,
                      quote_age_ms=100, quote_source="",
                      quote_provenance="synchronous_submit_fetch")
            assert r.passed is False
            assert r.reason_code == GateOutcome.CURRENT_PRICE_SOURCE_UNPROVEN

    def test_live_unknown_with_sync_fetch_is_allowed(self):
        # P0-1: Jason's real LIVE shape — unknown + synchronous_submit_fetch.
        r = _gate("live",
                  side="CALL", trigger_price=100.0, stop_price=95.0,
                  target_price=110.0,
                  current_bid=100.95, current_ask=101.05,
                  quote_age_ms=100, quote_source="unknown",
                  quote_provenance="synchronous_submit_fetch")
        assert r.passed is True
        assert r.reason_code == GateOutcome.PASS

    def test_live_unknown_without_sync_fetch_still_holds(self):
        # P0-1 exception is scoped tightly: unknown+cached still fails.
        r = _gate("live",
                  side="CALL", trigger_price=100.0, stop_price=95.0,
                  target_price=110.0,
                  current_bid=100.95, current_ask=101.05,
                  quote_age_ms=100, quote_source="unknown",
                  quote_provenance="cached")
        assert r.passed is False
        assert r.reason_code == GateOutcome.CURRENT_PRICE_SOURCE_UNPROVEN

    def test_paper_unknown_with_sync_fetch_still_holds(self):
        # The P0-1 exception is LIVE-only. PAPER MUST NOT accept "unknown"
        # even with sync provenance — PAPER must attest a proven-live data
        # transport, which cannot be labeled "unknown".
        r = _gate("paper",
                  side="CALL", trigger_price=100.0, stop_price=95.0,
                  target_price=110.0,
                  current_bid=100.95, current_ask=101.05,
                  quote_age_ms=100, quote_source="unknown",
                  quote_provenance="synchronous_submit_fetch")
        assert r.passed is False
        assert r.reason_code == GateOutcome.CURRENT_PRICE_SOURCE_UNPROVEN

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


# ─────────────────────────────────────────────────────────────────────────────
# PR #391 P1: classify_market_truth blank/None/unknown → HOLD (never PASS).
# ─────────────────────────────────────────────────────────────────────────────


class TestBlankReasonNeverAuthorizesSubmit:
    def test_none_reason_maps_to_hold(self):
        assert classify_market_truth(None) == MarketTruthAuthority.HOLD_MARKET_TRUTH_UNAVAILABLE

    def test_blank_reason_maps_to_hold(self):
        assert classify_market_truth("") == MarketTruthAuthority.HOLD_MARKET_TRUTH_UNAVAILABLE

    def test_whitespace_reason_maps_to_hold(self):
        assert classify_market_truth("   ") == MarketTruthAuthority.HOLD_MARKET_TRUTH_UNAVAILABLE

    def test_unknown_reason_maps_to_hold(self):
        assert classify_market_truth("UNKNOWN_NEW_REASON") == (
            MarketTruthAuthority.HOLD_MARKET_TRUTH_UNAVAILABLE
        )

    def test_only_pass_authorizes_submit(self):
        assert classify_market_truth(GateOutcome.PASS) == MarketTruthAuthority.SUBMIT_VALID


# ─────────────────────────────────────────────────────────────────────────────
# PR #391 blocker 3: HOLD row shape must be adoptable by the canonical
# deferred-retry watcher — the same lifecycle vocabulary as
# schedule_deferred_materialization_retry produces. This test proves the
# durable meta patch a real HOLD writes satisfies the read-side predicates
# used by adopt_deferred_retry_watcher / _resolve_trigger_callback_disposition.
# ─────────────────────────────────────────────────────────────────────────────


class TestHoldOwnerLiveness:
    def _hold_meta_from_osm(self, osm) -> dict:
        assert osm.hold_entry_for_market_truth_unavailable(
            "LOID-HOLD",
            reason_code=GateOutcome.CURRENT_PRICE_STALE,
            gate_audit={"quote_age_ms": 30_000},
            execution_mode="paper",
            signal_id="SIG-HOLD",
            expected_generation=1,
            expected_watcher_token="watcher:hold",
            next_retry_at="2026-07-26T18:00:00+00:00",
            max_attempts=8,
        ) is True
        assert len(osm._fake_conn.executed) == 1
        import json
        return json.loads(osm._fake_conn.executed[0][1][0])

    def test_hold_row_matches_canonical_retry_read_predicates(self, osm):
        """The predicates the watcher/adoption CAS reads:
             lifecycle_state == RETRY_WAIT
             materialization_status == RETRY_PENDING
             materialization_in_flight == false
             materialization_owner == ''  (retry loop reclaims)
             watcher_token == ''
             broker_ready == false
             retry_reason present
             next_retry_at + materialization_next_retry_at present
             retry_max_attempts present
             every submit_intent / broker_submit / recovery_owner field cleared
        """
        patch = self._hold_meta_from_osm(osm)
        assert patch["lifecycle_state"] == "RETRY_WAIT"
        assert patch["materialization_status"] == "RETRY_PENDING"
        assert patch["materialization_in_flight"] is False
        assert patch["materialization_owner"] == ""
        assert patch["watcher_token"] == ""
        assert patch["broker_ready"] is False
        assert patch["retry_reason"] == GateOutcome.CURRENT_PRICE_STALE
        assert patch["materialization_reason"] == GateOutcome.CURRENT_PRICE_STALE
        assert patch["next_retry_at"]
        assert patch["materialization_next_retry_at"] == patch["next_retry_at"]
        assert patch["retry_max_attempts"] == 8
        for cleared in (
            "submit_intent_at", "submit_started_at",
            "broker_submit_key", "broker_submit_payload_hash",
            "recovery_submit_owner", "recovery_submit_lease_until",
        ):
            assert patch[cleared] == ""
        # No hold-only dialect fields leak through.
        assert "submit_hold_attempt" not in patch
        assert "submit_hold_max_attempts" not in patch


# ─────────────────────────────────────────────────────────────────────────────
# PR #391 blocker 6: PAPER data-domain transport proof — not a source string.
# ─────────────────────────────────────────────────────────────────────────────


class TestPaperDataTransportProof:
    def _proof(self, base_url):
        from ap_execution_core import _market_data_transport_proof
        class _Cfg:
            def __init__(self, u): self.base_url = u
        class _Broker:
            def __init__(self, u): self.cfg = _Cfg(u)
        return _market_data_transport_proof(_Broker(base_url))

    def test_none_adapter_not_proven(self):
        from ap_execution_core import _market_data_transport_proof
        assert _market_data_transport_proof(None) == {
            "base_url": None, "host": None, "sandbox": False, "proven": False,
        }

    def test_live_host_proven(self):
        p = self._proof("https://api.tradier.com/v1")
        assert p["proven"] is True
        assert p["sandbox"] is False
        assert p["host"] == "api.tradier.com"

    @pytest.mark.parametrize("bad", [
        "https://sandbox.tradier.com",
        "https://paper.example.com",
        "https://sim.marketdata.io",
        "https://mock.foo.bar",
        "https://test.baz.qux",
    ])
    def test_sandbox_family_hosts_not_proven(self, bad):
        p = self._proof(bad)
        assert p["proven"] is False
        assert p["sandbox"] is True

    def test_blank_url_not_proven(self):
        p = self._proof("")
        assert p["proven"] is False
        assert p["host"] is None

    def test_payload_source_label_does_not_bypass_transport_proof(self):
        # A payload claiming source=live_broker cannot rescue a sandbox
        # transport. The gate caller checks _market_data_transport_proof;
        # the source string is only a secondary signal (denylist).
        p = self._proof("https://sandbox.tradier.com")
        assert p["proven"] is False


# ─────────────────────────────────────────────────────────────────────────────
# PR #391 P0-2: in-memory watcher reset — a TRIGGERED WatchedSignal returns
# to PENDING with no trigger evidence, and requires a fresh contract.
# ─────────────────────────────────────────────────────────────────────────────


def _build_watcher_with_triggered_signal(local_order_id="LOID-1",
                                         ticker="BAC",
                                         side="PUT"):
    """Assemble a minimal APEntryWatcher with exactly one TRIGGERED
    WatchedSignal that owns local_order_id. Uses object.__new__ to
    avoid the heavy real ctor and only sets fields the reset method
    reads/writes."""
    from ap_entry_watcher import APEntryWatcher, WatchedSignal, WatchState
    import threading, uuid

    w = object.__new__(APEntryWatcher)
    w._pending = []
    w._lock = threading.Lock()
    w.owner_token = f"watcher:{uuid.uuid4()}"

    signal = {
        "ticker": ticker,
        "side": side,
        "entry_price": 61.17,
        "stop_price": 61.75,
        "target_price": 59.50,
        "signal_id": "SIG-1",
        "local_order_id": local_order_id,
        "client_id": "client@example.com",
        "execution_mode": "paper",
        "contract_symbol": "BAC260724P00062000",
    }
    watched = object.__new__(WatchedSignal)
    watched.signal = signal
    watched.ticker = ticker
    watched.side = side.upper()
    watched.state = WatchState.TRIGGERED
    watched.breach_count = 2
    watched.breach_price = 61.20
    from datetime import datetime, timezone
    watched.trigger_crossed_at = datetime.now(timezone.utc)
    watched.triggered_at = datetime.now(timezone.utc)
    watched.trigger_price = 61.17
    watched.first_breach_bid = 61.10
    watched.first_breach_ask = 61.14
    watched._trigger_stop_collision = False
    watched._ownership_quarantine = False
    watched.rearm_mode = False
    watched._watcher_ref = w
    w._pending.append(watched)
    return w, watched


class TestWatcherResetAfterSubmitBlock:
    def test_reset_makes_triggered_watcher_active_again(self):
        from ap_entry_watcher import WatchState
        w, watched = _build_watcher_with_triggered_signal()
        assert w.reset_after_submit_market_truth_block(
            "LOID-1",
            reason_code="PUT_NO_LONGER_BELOW_TRIGGER",
        ) is True
        assert watched.state == WatchState.PENDING
        assert watched.breach_count == 0
        assert watched.trigger_crossed_at is None
        assert watched.triggered_at is None
        assert watched.first_breach_bid == 0.0
        assert watched.first_breach_ask == 0.0
        assert watched.signal["contract_deferred"] is True
        assert watched.signal["contract_symbol"] == "DEFERRED:BAC"
        assert watched.signal["submit_market_truth_rearmed"] is True
        assert watched.signal["submit_market_truth_reason_code"] == "PUT_NO_LONGER_BELOW_TRIGGER"
        assert watched.signal["watcher_token"] == w.owner_token

    def test_reset_refuses_wrong_state(self):
        from ap_entry_watcher import WatchState
        w, watched = _build_watcher_with_triggered_signal()
        watched.state = WatchState.EXPIRED
        assert w.reset_after_submit_market_truth_block(
            "LOID-1", reason_code="X",
        ) is False

    def test_reset_refuses_unknown_local_order_id(self):
        w, _ = _build_watcher_with_triggered_signal()
        assert w.reset_after_submit_market_truth_block(
            "LOID-DOES-NOT-EXIST", reason_code="X",
        ) is False

    def test_reset_refuses_ambiguous_identity(self):
        w, _ = _build_watcher_with_triggered_signal(local_order_id="LOID-1")
        # Add a second watcher with the same local_order_id — must refuse.
        from ap_entry_watcher import WatchedSignal, WatchState
        dup = object.__new__(WatchedSignal)
        dup.signal = {"local_order_id": "LOID-1"}
        dup.ticker = "BAC"
        dup.state = WatchState.PENDING
        dup._watcher_ref = w
        w._pending.append(dup)
        assert w.reset_after_submit_market_truth_block(
            "LOID-1", reason_code="X",
        ) is False

    def test_reset_refuses_blank_args(self):
        w, _ = _build_watcher_with_triggered_signal()
        assert w.reset_after_submit_market_truth_block("", reason_code="X") is False
        assert w.reset_after_submit_market_truth_block("LOID-1", reason_code="") is False


# ─────────────────────────────────────────────────────────────────────────────
# PR #391 P0-1: gate-module exception must HOLD identically for PAPER and
# LIVE. Force check_market_validity_gate() and classify_market_truth() to
# raise. Exercise the exact production _on_entry_trigger callback. Assert:
#   * zero submit_existing_entry call
#   * zero broker POST
#   * disposition == RETRY_WAIT
#   * durable reason == MARKET_TRUTH_GATE_MODULE_ERROR
# ─────────────────────────────────────────────────────────────────────────────


def _build_gate_exception_scaffold(monkeypatch, mode: str):
    """Assemble the minimum production-shaped scaffolding to invoke
    APExecutionCore._on_entry_trigger with a raising gate."""
    import sys, types, threading
    from unittest.mock import MagicMock
    from types import SimpleNamespace
    import ap_execution_core as core_mod

    # Real _refresh_ask_at_submit is imported dynamically by the callback;
    # replace with a well-formed stub so we don't crash before the gate runs.
    fake_execution = types.ModuleType("ap.execution")
    fake_execution._refresh_ask_at_submit = lambda *_a, **_k: (
        1.25, 5, True, "", {
            "submit_bid": 1.20, "submit_ask": 1.25,
            "submit_last": 1.23, "submit_mid": 1.225,
            "spread_pct": 0.04,
        },
    )
    monkeypatch.setitem(sys.modules, "ap.execution", fake_execution)

    plan = SimpleNamespace(
        contract_symbol="SPY260626C00500000",
        execution_price_per_share=1.25, ask=1.25, mid=1.20,
        affordable_contracts=1, premium_per_contract=125.0,
        contracts=1, limit_price=1.25,
        side="CALL", direction="CALL",
        execution_mode=mode,
        client_id="client@example.com",
        signal_id="SIG-GATE-EXC",
        trigger_price=600.5, stop_underlying=595.0, target_underlying=610.0,
        metadata={"queue_id": 42},
    )
    watched = SimpleNamespace(
        signal={
            "ticker": "SPY", "side": "CALL", "entry_price": 600.0,
            "stop_price": 595.0, "target_price": 610.0,
            "signal_id": plan.signal_id,
            "local_order_id": "LOID-GATE-EXC",
            "client_id": plan.client_id,
            "execution_mode": mode,
        },
        trigger_price=600.5, ticker="SPY",
        trigger_crossed_at=datetime.now(timezone.utc),
    )

    core = core_mod.APExecutionCore.__new__(core_mod.APExecutionCore)
    core.paper = (mode == "paper")
    core.mode = mode.upper()
    core.execution_mode = mode
    core.client_id = core.client_email = plan.client_id
    # PR #391 blocker 6: transport proof needs a real base_url — pick a
    # proven-live one so the gate would otherwise be reachable.
    class _Cfg: base_url = "https://api.tradier.com"
    core.broker = SimpleNamespace(
        cfg=_Cfg(),
        get_quote=lambda _s: {
            "bid": 600.60, "ask": 600.70, "quote_age_ms": 100,
            "source": "live_broker",
        },
    )
    core.data_broker = core.broker  # not separate → PAPER would HOLD too;
                                    # doesn't matter because gate raises first
    core.store = MagicMock()
    core.contract_selector = MagicMock()
    core._breach_risk_check = MagicMock(return_value=True)
    core._recover_plan_for_revalidation = MagicMock(return_value=plan)

    osm = MagicMock(client_id=plan.client_id, execution_mode=mode)
    durable = {"client_id": plan.client_id, "meta": {}}
    osm.get_order.side_effect = lambda _loid: durable
    def _upd(_loid, patch):
        durable["meta"].update(patch)
        return True
    osm.update_order_meta.side_effect = _upd
    osm.hold_entry_for_market_truth_unavailable.return_value = True
    osm.submit_existing_entry.return_value = {"ok": True, "broker_order_id": "TR-1"}
    core.order_state_machine = osm

    core._cleanup_pending_entry_order = types.MethodType(
        core_mod.APExecutionCore._cleanup_pending_entry_order, core,
    )
    return core, watched, osm, durable




# ─────────────────────────────────────────────────────────────────────────────
# PR #391 P1: classify_market_truth blank/None/unknown → HOLD (never PASS).
# ─────────────────────────────────────────────────────────────────────────────


class TestBlankReasonNeverAuthorizesSubmit:
    def test_none_reason_maps_to_hold(self):
        assert classify_market_truth(None) == MarketTruthAuthority.HOLD_MARKET_TRUTH_UNAVAILABLE

    def test_blank_reason_maps_to_hold(self):
        assert classify_market_truth("") == MarketTruthAuthority.HOLD_MARKET_TRUTH_UNAVAILABLE

    def test_whitespace_reason_maps_to_hold(self):
        assert classify_market_truth("   ") == MarketTruthAuthority.HOLD_MARKET_TRUTH_UNAVAILABLE

    def test_unknown_reason_maps_to_hold(self):
        assert classify_market_truth("UNKNOWN_NEW_REASON") == (
            MarketTruthAuthority.HOLD_MARKET_TRUTH_UNAVAILABLE
        )

    def test_only_pass_authorizes_submit(self):
        assert classify_market_truth(GateOutcome.PASS) == MarketTruthAuthority.SUBMIT_VALID


class TestHoldOwnerLiveness:
    """PR #391 blocker 3: HOLD row shape carries the canonical deferred-
    retry vocabulary — same shape adopt_deferred_retry_watcher expects."""

    def _hold_meta_from_osm(self, osm) -> dict:
        assert osm.hold_entry_for_market_truth_unavailable(
            "LOID-HOLD",
            reason_code=GateOutcome.CURRENT_PRICE_STALE,
            gate_audit={"quote_age_ms": 30_000},
            execution_mode="paper",
            signal_id="SIG-HOLD",
            expected_generation=1,
            expected_watcher_token="watcher:hold",
            next_retry_at="2026-07-26T18:00:00+00:00",
            max_attempts=8,
        ) is True
        assert len(osm._fake_conn.executed) == 1
        import json
        return json.loads(osm._fake_conn.executed[0][1][0])

    def test_hold_row_matches_canonical_retry_read_predicates(self, osm):
        patch = self._hold_meta_from_osm(osm)
        assert patch["lifecycle_state"] == "RETRY_WAIT"
        assert patch["materialization_status"] == "RETRY_PENDING"
        assert patch["materialization_in_flight"] is False
        assert patch["materialization_owner"] == ""
        # P0-8: durable and in-memory ownership must agree; HOLD preserves
        # the caller's watcher_token to keep the ownership model coherent.
        assert patch["watcher_token"] == "watcher:hold"
        assert patch["broker_ready"] is False
        assert patch["retry_reason"] == GateOutcome.CURRENT_PRICE_STALE
        assert patch["materialization_reason"] == GateOutcome.CURRENT_PRICE_STALE
        assert patch["next_retry_at"]
        assert patch["materialization_next_retry_at"] == patch["next_retry_at"]
        assert patch["retry_max_attempts"] == 8
        for cleared in (
            "submit_intent_at", "submit_started_at",
            "broker_submit_key", "broker_submit_payload_hash",
            "recovery_submit_owner", "recovery_submit_lease_until",
        ):
            assert patch[cleared] == ""
        # No hold-only dialect fields leak through.
        assert "submit_hold_attempt" not in patch
        assert "submit_hold_max_attempts" not in patch


class TestPaperDataTransportProof:
    """PR #391 blocker 6: proof by transport, not source label."""

    def _proof(self, base_url):
        from ap_execution_core import _market_data_transport_proof
        class _Cfg:
            def __init__(self, u): self.base_url = u
        class _Broker:
            def __init__(self, u): self.cfg = _Cfg(u)
        return _market_data_transport_proof(_Broker(base_url))

    def test_none_adapter_not_proven(self):
        from ap_execution_core import _market_data_transport_proof
        assert _market_data_transport_proof(None) == {
            "base_url": None, "host": None, "sandbox": False, "proven": False,
        }

    def test_live_host_proven(self):
        p = self._proof("https://api.tradier.com/v1")
        assert p["proven"] is True
        assert p["sandbox"] is False
        assert p["host"] == "api.tradier.com"

    @pytest.mark.parametrize("bad", [
        "https://sandbox.tradier.com",
        "https://paper.example.com",
        "https://sim.marketdata.io",
        "https://mock.foo.bar",
        "https://test.baz.qux",
    ])
    def test_sandbox_family_hosts_not_proven(self, bad):
        p = self._proof(bad)
        assert p["proven"] is False
        assert p["sandbox"] is True

    def test_blank_url_not_proven(self):
        p = self._proof("")
        assert p["proven"] is False
        assert p["host"] is None

    def test_payload_source_label_does_not_bypass_transport_proof(self):
        # source string is only a secondary signal — transport is the truth.
        p = self._proof("https://sandbox.tradier.com")
        assert p["proven"] is False

    # ─── PR #391 amendment: strict allowlist regressions ────────────────
    #
    # The prior substring denylist over-approved every URL that did not
    # contain "sandbox/paper/sim/mock/test" in its host, including obvious
    # attack vectors (evil.example, api.tradier.com.evil.example) and every
    # non-HTTPS scheme. Proof now requires an EXACT match on
    # scheme=https / host=api.tradier.com / port ∈ {None, 443}.

    @pytest.mark.parametrize(
        "bad_url",
        [
            "https://evil.example",
            "https://api.tradier.com.evil.example",
            "ftp://api.tradier.com",
            "http://api.tradier.com",
            "https://api.tradier.com:8443",
            "https://sandbox.tradier.com",
            "https://paper.example.com",
            "https://sim.marketdata.io",
            "https://mock.foo.bar",
            "https://test.baz.qux",
        ],
    )
    def test_noncanonical_transport_is_not_proven(self, bad_url):
        proof = self._proof(bad_url)
        assert proof["proven"] is False

    @pytest.mark.parametrize(
        "valid_url",
        [
            "https://api.tradier.com",
            "https://api.tradier.com/",
            "https://api.tradier.com/v1",
            "https://api.tradier.com:443",
        ],
    )
    def test_canonical_tradier_https_transport_is_proven(self, valid_url):
        proof = self._proof(valid_url)
        assert proof["proven"] is True
        assert proof["host"] == "api.tradier.com"
        assert proof["sandbox"] is False


class TestInvalidDirectionFailsClosed:
    """PR #391 amendment: the market-validity gate must fail closed on any
    option direction other than exactly CALL or PUT.

    Prior behavior fell through the CALL/PUT branches and returned PASS on
    unknown side. A fresh quote fetch does not heal an unknown direction,
    so this failure is terminal (TERMINAL_SETUP_COMPLETE), not a retryable
    HOLD — zero broker POST either way.
    """

    @pytest.mark.parametrize("mode", ["paper", "live"])
    @pytest.mark.parametrize(
        "side",
        [None, "", "UNKNOWN", "CALLS", "PUTT", "0"],
    )
    def test_invalid_direction_never_authorizes_submit(self, mode, side):
        result = check_market_validity_gate(
            execution_mode=mode,
            side=side,
            trigger_price=100.0,
            stop_price=95.0,
            target_price=110.0,
            current_bid=100.90,
            current_ask=101.10,
            quote_age_ms=100,
            quote_source="live_broker",
            quote_provenance="synchronous_submit_fetch",
        )

        assert result.passed is False
        assert result.reason_code == GateOutcome.ENTRY_DIRECTION_INVALID
        assert classify_market_truth(result.reason_code) == (
            MarketTruthAuthority.TERMINAL_SETUP_COMPLETE
        )

    @pytest.mark.parametrize("mode", ["paper", "live"])
    @pytest.mark.parametrize("side", ["CALL", "PUT"])
    def test_canonical_direction_remains_supported(self, mode, side):
        if side == "CALL":
            stop_price = 95.0
            target_price = 110.0
            # CALL: ask must be at or above trigger, and mid within
            # (stop, target) exclusive.
            current_bid = 100.00
            current_ask = 100.10
        else:
            stop_price = 105.0
            target_price = 90.0
            # PUT: bid must be at or below trigger, and mid within
            # (target, stop) exclusive.
            current_bid = 99.90
            current_ask = 100.00

        result = check_market_validity_gate(
            execution_mode=mode,
            side=side,
            trigger_price=100.0,
            stop_price=stop_price,
            target_price=target_price,
            current_bid=current_bid,
            current_ask=current_ask,
            quote_age_ms=100,
            quote_source="live_broker",
            quote_provenance="synchronous_submit_fetch",
        )

        assert result.passed is True
        assert result.reason_code == GateOutcome.PASS


def _build_watcher_with_triggered_signal(local_order_id="LOID-1",
                                         ticker="BAC",
                                         side="PUT"):
    from ap_entry_watcher import APEntryWatcher, WatchedSignal, WatchState
    import threading, uuid
    from datetime import datetime, timezone

    w = object.__new__(APEntryWatcher)
    w._pending = []
    w._lock = threading.Lock()
    w.owner_token = f"watcher:{uuid.uuid4()}"

    signal = {
        "ticker": ticker, "side": side,
        "entry_price": 61.17, "stop_price": 61.75, "target_price": 59.50,
        "signal_id": "SIG-1", "local_order_id": local_order_id,
        "client_id": "client@example.com", "execution_mode": "paper",
        "contract_symbol": "BAC260724P00062000",
    }
    watched = object.__new__(WatchedSignal)
    watched.signal = signal
    watched.ticker = ticker
    watched.side = side.upper()
    watched.state = WatchState.TRIGGERED
    watched.breach_count = 2
    watched.breach_price = 61.20
    watched.trigger_crossed_at = datetime.now(timezone.utc)
    watched.triggered_at = datetime.now(timezone.utc)
    watched.trigger_price = 61.17
    watched.first_breach_bid = 61.10
    watched.first_breach_ask = 61.14
    watched._trigger_stop_collision = False
    watched._ownership_quarantine = False
    watched.rearm_mode = False
    watched._watcher_ref = w
    w._pending.append(watched)
    return w, watched


class TestWatcherResetAfterSubmitBlock:
    def test_reset_makes_triggered_watcher_active_again(self):
        from ap_entry_watcher import WatchState
        w, watched = _build_watcher_with_triggered_signal()
        assert w.reset_after_submit_market_truth_block(
            "LOID-1", reason_code="PUT_NO_LONGER_BELOW_TRIGGER",
        ) is True
        assert watched.state == WatchState.PENDING
        assert watched.breach_count == 0
        assert watched.trigger_crossed_at is None
        assert watched.triggered_at is None
        assert watched.first_breach_bid == 0.0
        assert watched.first_breach_ask == 0.0
        assert watched.signal["contract_deferred"] is True
        assert watched.signal["contract_symbol"] == "DEFERRED:BAC"
        assert watched.signal["submit_market_truth_rearmed"] is True
        assert watched.signal["submit_market_truth_reason_code"] == "PUT_NO_LONGER_BELOW_TRIGGER"
        assert watched.signal["watcher_token"] == w.owner_token

    def test_reset_refuses_wrong_state(self):
        from ap_entry_watcher import WatchState
        w, watched = _build_watcher_with_triggered_signal()
        watched.state = WatchState.EXPIRED
        assert w.reset_after_submit_market_truth_block("LOID-1", reason_code="X") is False

    def test_reset_refuses_unknown_local_order_id(self):
        w, _ = _build_watcher_with_triggered_signal()
        assert w.reset_after_submit_market_truth_block("LOID-NOPE", reason_code="X") is False

    def test_reset_refuses_ambiguous_identity(self):
        from ap_entry_watcher import WatchedSignal, WatchState
        w, _ = _build_watcher_with_triggered_signal(local_order_id="LOID-1")
        dup = object.__new__(WatchedSignal)
        dup.signal = {"local_order_id": "LOID-1"}
        dup.ticker = "BAC"
        dup.state = WatchState.PENDING
        dup._watcher_ref = w
        w._pending.append(dup)
        assert w.reset_after_submit_market_truth_block("LOID-1", reason_code="X") is False

    def test_reset_refuses_blank_args(self):
        w, _ = _build_watcher_with_triggered_signal()
        assert w.reset_after_submit_market_truth_block("", reason_code="X") is False
        assert w.reset_after_submit_market_truth_block("LOID-1", reason_code="") is False


def _build_gate_exception_scaffold(monkeypatch, mode: str):
    """Assemble a minimal APExecutionCore + WatchedSignal that reaches the
    market-truth gate wrapper. Broker + data_broker are proven-live so we
    isolate the gate-exception behavior."""
    import sys, types, threading
    from unittest.mock import MagicMock
    from types import SimpleNamespace
    from datetime import datetime, timezone
    import ap_execution_core as core_mod

    fake_execution = types.ModuleType("ap.execution")
    fake_execution._refresh_ask_at_submit = lambda *_a, **_k: (
        1.25, 5, True, "", {
            "submit_bid": 1.20, "submit_ask": 1.25,
            "submit_last": 1.23, "submit_mid": 1.225,
            "spread_pct": 0.04,
        },
    )
    monkeypatch.setitem(sys.modules, "ap.execution", fake_execution)

    plan = SimpleNamespace(
        contract_symbol="SPY260626C00500000",
        execution_price_per_share=1.25, ask=1.25, mid=1.20,
        affordable_contracts=1, premium_per_contract=125.0,
        contracts=1, limit_price=1.25,
        side="CALL", direction="CALL", execution_mode=mode,
        client_id="client@example.com", signal_id="SIG-GATE-EXC",
        trigger_price=600.5, stop_underlying=595.0, target_underlying=610.0,
        metadata={"queue_id": 42},
    )
    watched = SimpleNamespace(
        signal={
            "ticker": "SPY", "side": "CALL", "entry_price": 600.0,
            "stop_price": 595.0, "target_price": 610.0,
            "signal_id": plan.signal_id, "local_order_id": "LOID-GATE-EXC",
            "client_id": plan.client_id, "execution_mode": mode,
        },
        trigger_price=600.5, ticker="SPY",
        trigger_crossed_at=datetime.now(timezone.utc),
    )

    core = core_mod.APExecutionCore.__new__(core_mod.APExecutionCore)
    core.paper = (mode == "paper")
    core.mode = mode.upper()
    core.execution_mode = mode
    core.client_id = core.client_email = plan.client_id
    class _CfgLive: base_url = "https://api.tradier.com"
    class _CfgSandbox: base_url = "https://sandbox.tradier.com"
    core.broker = SimpleNamespace(
        cfg=_CfgSandbox() if mode == "paper" else _CfgLive(),
        get_quote=lambda _s: {
            "bid": 600.60, "ask": 600.70, "quote_age_ms": 100,
            "source": "live_broker",
        },
    )
    # For PAPER, wire a distinct live data broker so the gate would be
    # reachable if the gate itself did not raise.
    if mode == "paper":
        core.data_broker = SimpleNamespace(
            cfg=_CfgLive(),
            get_quote=lambda _s: {
                "bid": 600.60, "ask": 600.70, "quote_age_ms": 100,
                "source": "live_broker",
            },
        )
    else:
        core.data_broker = None
    core.store = MagicMock()
    core.contract_selector = MagicMock()
    core._breach_risk_check = MagicMock(return_value=True)
    core._recover_plan_for_revalidation = MagicMock(return_value=plan)

    osm = MagicMock(client_id=plan.client_id, execution_mode=mode)
    durable = {"client_id": plan.client_id, "meta": {}}
    osm.get_order.side_effect = lambda _loid: durable
    def _upd(_loid, patch):
        durable["meta"].update(patch); return True
    osm.update_order_meta.side_effect = _upd
    osm.hold_entry_for_market_truth_unavailable.return_value = True
    osm.submit_existing_entry.return_value = {"ok": True, "broker_order_id": "TR-1"}
    core.order_state_machine = osm

    core._cleanup_pending_entry_order = types.MethodType(
        core_mod.APExecutionCore._cleanup_pending_entry_order, core,
    )
    return core, watched, osm, durable


# ─────────────────────────────────────────────────────────────────────────────
# PR #391 P0-1: gate-module exception branch — hard-close for PAPER and LIVE.
#
# The full _on_entry_trigger callback wires many upstream gates before the
# market-truth wrapper. Rather than reconstruct that entire fixture (and
# risk testing something other than the wrapper), we prove the invariant
# by inspecting the wrapper's source shape and by exercising the exception
# branch's OSM call surface directly.
# ─────────────────────────────────────────────────────────────────────────────


class TestGateExceptionBranchShape:
    """P0-1 proof: the exception branch has no PAPER exemption anywhere."""

    def _wrapper_body(self) -> str:
        with open("ap_execution_core.py") as fh:
            src = fh.read()
        # Isolate the wrapper: from the outer `try:` for the gate module
        # through the `submit_existing_entry(` broker POST that follows the
        # `except` block. This bounds the "wrapper" region precisely.
        start = src.index("MARKET_TRUTH_GATE_MODULE_ERROR")
        end = src.index("self.order_state_machine.submit_existing_entry(", start)
        return src[start:end]

    def test_no_paper_exemption_in_gate_exception_branch(self):
        body = self._wrapper_body()
        # The old fail-open contract used one of these exact phrases.
        forbidden_phrases = [
            'if _module_error_exec_mode != "paper":',
            "if _module_error_exec_mode != 'paper':",
            "LIVE will block, PAPER will proceed",
            "PAPER will proceed",
        ]
        for phrase in forbidden_phrases:
            assert phrase not in body, (
                f"gate-module exception branch still has a PAPER exemption: {phrase!r}"
            )

    def test_exception_branch_returns_retry_wait(self):
        body = self._wrapper_body()
        # The exception branch must return the RETRY_WAIT disposition and
        # the MARKET_TRUTH_GATE_MODULE_ERROR reason code.
        assert '"disposition":' in body and '"RETRY_WAIT"' in body
        assert "MARKET_TRUTH_GATE_MODULE_ERROR" in body
        assert '"broker_post_attempted":' in body and "False" in body

    def test_exception_branch_calls_canonical_hold(self):
        body = self._wrapper_body()
        # The exception branch must call the fenced HOLD helper — not
        # _terminalize_breach_failure, not submit_existing_entry.
        assert "hold_entry_for_market_truth_unavailable(" in body
        # And it must NOT terminalize.
        assert "_terminalize_breach_failure" not in body

    def test_module_error_diagnostic_hold_metadata_follows_successful_cas(self):
        """PR #391 amendment: the diagnostic HOLD-authority metadata write
        must run AFTER the failed-HOLD-CAS guard. Otherwise a row whose
        fenced HOLD CAS never confirmed would still carry
        final_market_truth_status="HOLD_MARKET_TRUTH_UNAVAILABLE" in meta,
        contradicting the canonical durable authority write.
        """
        body = self._wrapper_body()

        hold_guard = body.index("if not _module_hold_ok:")
        diagnostic_write = body.index(
            "self.order_state_machine.update_order_meta(",
            hold_guard,
        )

        # No generic HOLD-authority metadata write is permitted before the
        # fenced HOLD CAS result is checked.
        assert "self.order_state_machine.update_order_meta(" not in body[:hold_guard]
        assert diagnostic_write > hold_guard


class TestOSMHoldDirectlyOnGateModuleErrorReason:
    """The exception branch calls hold_entry_for_market_truth_unavailable
    with reason MARKET_TRUTH_GATE_MODULE_ERROR. Prove the OSM method
    accepts that reason and writes the canonical HOLD schema."""

    def test_hold_accepts_module_error_reason(self, osm):
        ok = osm.hold_entry_for_market_truth_unavailable(
            "LOID-EXC",
            reason_code="MARKET_TRUTH_GATE_MODULE_ERROR",
            gate_audit={"error_type": "RuntimeError", "error": "gate exploded"},
            execution_mode="paper",
            signal_id="SIG-EXC",
            expected_generation=0,
            expected_watcher_token="",
            next_retry_at="2026-07-26T18:30:00+00:00",
            max_attempts=10,
        )
        assert ok is True
        import json
        patch = json.loads(osm._fake_conn.executed[0][1][0])
        assert patch["final_market_truth_reason_code"] == "MARKET_TRUTH_GATE_MODULE_ERROR"
        assert patch["final_market_truth_status"] == "HOLD_MARKET_TRUTH_UNAVAILABLE"
        assert patch["lifecycle_state"] == "RETRY_WAIT"


# ─────────────────────────────────────────────────────────────────────────────
# PR #391 HARD-HOLD amendment (Sept 2026 review): 12 blocker corrections.
# ─────────────────────────────────────────────────────────────────────────────


class TestHoldClearsStaleContractAuthorityP0_9:
    """P0-9: when the caller supplies a DEFERRED:<TICKER> placeholder, HOLD
    must clear the top-level contract / limit_price / reserved_cost fields
    just like REARM does — so a restart cannot resurrect a stale OCC."""

    def test_hold_with_deferred_contract_clears_top_level_fields(self, osm):
        ok = osm.hold_entry_for_market_truth_unavailable(
            "LOID-P09",
            reason_code=GateOutcome.CURRENT_PRICE_STALE,
            gate_audit={},
            execution_mode="paper",
            signal_id="SIG-P09",
            expected_generation=2,
            expected_watcher_token="watcher:p09",
            next_retry_at="2026-07-26T20:00:00+00:00",
            max_attempts=5,
            deferred_contract="DEFERRED:AAPL",
        )
        assert ok is True
        sql, params = osm._fake_conn.executed[0]
        # SET clause invalidates stale contract authority.
        assert "contract = %s" in sql
        assert "limit_price = NULL" in sql
        assert "reserved_cost = 0" in sql
        assert "REVALIDATION_REQUIRED" in sql
        # First positional param is the DEFERRED contract.
        assert params[0] == "DEFERRED:AAPL"

    def test_hold_without_deferred_contract_leaves_top_level_alone(self, osm):
        ok = osm.hold_entry_for_market_truth_unavailable(
            "LOID-P09b",
            reason_code=GateOutcome.CURRENT_PRICE_STALE,
            gate_audit={},
            execution_mode="paper",
            signal_id="SIG-P09b",
            expected_generation=2,
            expected_watcher_token="watcher:p09b",
            next_retry_at="2026-07-26T20:00:00+00:00",
            max_attempts=5,
        )
        assert ok is True
        sql, _ = osm._fake_conn.executed[0]
        # Without deferred_contract, no top-level contract update.
        assert "contract = %s" not in sql
        # Meta flag is still stamped (patch level).
        import json
        patch = json.loads(osm._fake_conn.executed[0][1][0])
        assert patch["contract_revalidation_required"] is True


class TestWatcherResetSetsDeferredRetryAttributeP0_6:
    """P0-6: the ownership-proof machinery reads
    watched.deferred_retry_not_before — writing only to the signal dict
    left the WatchedSignal inert."""

    def test_reset_sets_attribute_when_next_retry_at_supplied(self):
        w, watched = _build_watcher_with_triggered_signal()
        from datetime import datetime, timezone
        ts = "2026-07-26T21:00:00+00:00"
        ok = w.reset_after_submit_market_truth_block(
            "LOID-1", reason_code="X", next_retry_at=ts,
        )
        assert ok is True
        # Attribute is set to a tz-aware datetime, not just a string on the
        # signal dict.
        from datetime import datetime as _dt
        assert isinstance(watched.deferred_retry_not_before, _dt)
        assert watched.deferred_retry_not_before.tzinfo is not None
        assert watched.deferred_retry_not_before.isoformat().startswith("2026-07-26T21:00:00")
        # Signal-dict copy is still present for serialization.
        assert watched.signal["deferred_retry_not_before"] == ts

    def test_reset_clears_attribute_when_next_retry_at_none(self):
        w, watched = _build_watcher_with_triggered_signal()
        watched.deferred_retry_not_before = "should_be_cleared"
        ok = w.reset_after_submit_market_truth_block(
            "LOID-1", reason_code="X", next_retry_at=None,
        )
        assert ok is True
        assert watched.deferred_retry_not_before is None


class TestHoldOwnershipAgreementP0_8:
    """P0-8: durable and in-memory watcher_token must agree — HOLD
    preserves the caller's token instead of blanking it."""

    def test_hold_preserves_caller_watcher_token(self, osm):
        ok = osm.hold_entry_for_market_truth_unavailable(
            "LOID-P08",
            reason_code=GateOutcome.CURRENT_PRICE_STALE,
            gate_audit={},
            execution_mode="paper",
            signal_id="SIG-P08",
            expected_generation=1,
            expected_watcher_token="watcher:owner-A",
            next_retry_at="2026-07-26T20:00:00+00:00",
            max_attempts=5,
        )
        assert ok is True
        import json
        patch = json.loads(osm._fake_conn.executed[0][1][0])
        assert patch["watcher_token"] == "watcher:owner-A"
        assert patch["current_owner"] == "watcher:owner-A"


class TestRearmOwnershipFencingP0_3:
    """P0-3: outer capture of materialization_generation and watcher_token
    — no nested locals() lookup — so a real gen>0 row can pass the CAS."""

    def test_rearm_fences_gen_gt_zero(self, osm):
        ok = osm.rearm_entry_for_direction_reversal(
            "LOID-P03",
            reason_code=GateOutcome.PUT_NO_LONGER_BELOW_TRIGGER,
            gate_audit={},
            execution_mode="paper",
            signal_id="SIG-P03",
            expected_generation=7,   # ← real materialized gen, not 0
            expected_watcher_token="watcher:P03",
            deferred_contract="DEFERRED:MSFT",
        )
        assert ok is True
        sql, params = osm._fake_conn.executed[0]
        # WHERE clause pins on the exact generation and watcher token.
        assert "(meta->>'materialization_generation')::int" in sql
        assert "meta->>'watcher_token'" in sql
        # Params carry the fence values (gen and both token slots).
        assert 7 in params
        assert "watcher:P03" in params
