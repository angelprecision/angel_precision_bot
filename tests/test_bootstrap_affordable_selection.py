"""
P0 — Bootstrap Affordable Contract Selection (PR review fixes 1 + 2)
=====================================================================

Tests every scenario from the PR-review spec:

  1. $2k LIVE bootstrap client, 10% cap, HOOD $3.50 static estimate:
     evaluate() does NOT pre-reject; selector receives $200 budget.
  2. Selector returns $0.50 contract -> 1 contract, real cost $50.
  3. Selector could afford 4 cheap contracts -> still clamped to 1.
  4. Selector returns $1.95 contract -> 1 contract, $195 cost.
  5. Selector returns $3.50 contract -> revalidate_exposure rejects
     with ACTUAL_CONTRACT_COST_EXCEEDS_CLIENT_CAPITAL_LIMIT.
  6. Small LIVE client AFTER bootstrap ends (total_trades >= threshold):
     the static $3.50 fallback alone must NOT recreate the $1,050 false
     reject when remaining_capital > 0.
  7. PAPER non-bootstrap: behaviour unchanged (static pre-block still
     applies; min-contracts floor still enforced).

These are pure unit tests \u2014 no DB, no broker, no Supabase. We stub the
methods APMasterControl calls into so the tests run in <0.5s.

Per spec scope-lock:
  * No changes to scoring, scanners, entries, exits, signal delivery.
  * max_capital_pct is NEVER raised by these tests.
  * Final real-cost revalidation must always be in force.
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import ap_master_control as mc


# ---------------------------------------------------------------------------
# Stubs / fixtures
# ---------------------------------------------------------------------------

def _make_mc(
    *,
    mode: str = "live",
    account_equity: float = 2000.0,
    max_capital_pct: float = 0.10,
    capital_deployed: float = 0.0,
    pending_capital: float = 0.0,
    total_trades: int = 0,
):
    """Build an APMasterControl with all DB/Supabase/PositionManager
    interactions stubbed so evaluate() runs without external services."""
    m = mc.APMasterControl(
        mode=mode,
        score_floor=60.0,
        context_floor=0.0,
        account_equity=account_equity,
        max_capital_pct=max_capital_pct,
        # disable LIVE-only fail-closed checks so the test can drive
        # _pending_capital_from_snapshot_or_db deterministically.
        pending_capital_fail_closed_live=False,
        require_snapshot_freshness_live=False,
    )

    # Stub the snapshot so it returns the exact deployed/pending values
    # the test wants. Every field evaluate() touches is filled.
    m._get_snapshot = MagicMock(return_value={
        "_snapshot_ok":         True,
        "_snapshot_ts":         "2026-06-05T13:00:00Z",
        "_snapshot_age_sec":    0.1,
        "open_count":           0,
        "pending_entries":      0,
        "capital_deployed":     capital_deployed,
        "calls_open":           0,
        "puts_open":            0,
        "trades_today":         0,
        "open_positions":       [],
        "closing_positions":    [],
        "realized_pnl_today":   0.0,
        "total_trades":         total_trades,
        "ticker_open_counts":   {},
        "ticker_pending_counts":{},
    })
    # Stub pending capital to the requested value.
    m._pending_capital_from_snapshot_or_db = MagicMock(return_value=pending_capital)
    # Stub kill-switch as inactive.
    m._kill_switch_fn = lambda: False
    # Stub deployed-per-bucket helpers to 0.
    m._sector_capital_deployed = MagicMock(return_value=0.0)
    m._ticker_capital_deployed = MagicMock(return_value=0.0)
    # Stub capital-utilization logger so we don't hit Supabase.
    m._log_capital_utilization = MagicMock()
    # Stub the equity snapshot to return our test equity.
    m._equity_snapshot = MagicMock(return_value=(account_equity, -500.0))
    # Disable feedback / intel / sizer hooks.
    m.feedback_loop = None
    m.tier_engine = None
    m.sizer = None
    m._signal_store = None
    # Stub idem store
    m._idem_store_signal = MagicMock(return_value=True)
    # Stub DB-backed dedup persistence so we don't need psycopg2 / Supabase.
    m._persist_dedup = MagicMock(return_value=None)
    # Stub options-intel data fetch (otherwise yfinance gets called).
    m._options_intelligence = None
    m._fetch_intel = MagicMock(return_value={
        "available": False, "score": 0.0, "reason": "intel_disabled_in_test",
        "contracts": 0, "setup": {}, "breakdown": {},
    })
    return m


def _hood_signal(score: float = 70.0):
    return {
        "signal_id":   "test-sig-hood-1",
        "ticker":      "HOOD",
        "symbol":      "HOOD",
        "side":        "CALL",
        "direction":   "CALL",
        "score":       score,
        "ev_score":    score,    # LIVE mode requires ev_score > 0
        "tier":        "B",
        "pattern":     "2-2",
        "timeframe":   "1d",
        "trigger":     {"entry": 22.0, "stop": 21.50, "pt1": 23.0},
    }


def _make_plan_from_evaluate(decision):
    """evaluate() returns ControlDecision with .plan on approve; pull it out."""
    assert decision.ok, f"expected approve, got block: {decision.reason}"
    plan = decision.plan
    assert plan is not None
    return plan


# ---------------------------------------------------------------------------
# Test 1: $2k LIVE bootstrap \u2014 HOOD does NOT get pre-rejected
# ---------------------------------------------------------------------------

def test_t1_live_bootstrap_2k_hood_no_pre_reject():
    """The $1,050 false-reject must not happen. evaluate() must approve
    and pass the $200 remaining capital to the selector as the budget."""
    m = _make_mc(
        mode="live",
        account_equity=2000.0,
        max_capital_pct=0.10,
        total_trades=0,        # bootstrap
    )
    decision = m.evaluate(_hood_signal(score=70.0))
    plan = _make_plan_from_evaluate(decision)

    # plan.contracts must start at 1 (bootstrap hard-fixes qty=1)
    assert plan.contracts == 1
    # Selector budget = remaining_capital = $200 ($2000 * 10%)
    assert plan.max_position_usd == pytest.approx(200.0)
    # Sizing context surfaces the spec-required telemetry
    ctx = plan.metadata["sizing_context"]
    assert ctx["bootstrap_mode"] is True
    assert ctx["account_equity"] == 2000.0
    assert ctx["max_capital_pct"] == pytest.approx(0.10)
    assert ctx["max_capital_allowed"] == pytest.approx(200.0)
    assert ctx["capital_deployed"] == 0.0
    assert ctx["pending_capital"] == 0.0
    assert ctx["remaining_capital"] == pytest.approx(200.0)
    assert ctx["intended_contracts"] == 1
    assert ctx["max_affordable_premium"] == pytest.approx(2.0)  # $200 / 100
    assert ctx["static_premium_estimate"] == pytest.approx(3.50)  # HOOD fallback


# ---------------------------------------------------------------------------
# Test 2: Selector returns $0.50 contract -> 1 contract, real cost $50
# ---------------------------------------------------------------------------

def _simulate_selector_mutation(plan, *, premium_per_contract: float,
                                 affordable_contracts: int):
    """Mirror what ap/contract_selector.py:1257-1276 does to the plan."""
    plan.contract_symbol = "HOOD260612C00022000"
    plan.limit_price = premium_per_contract / 100.0
    plan.contracts = affordable_contracts
    plan.max_position_usd = plan.contracts * premium_per_contract
    plan.selector_execution_price = plan.limit_price
    plan.selector_metadata = {
        "contract_symbol":      plan.contract_symbol,
        "pricing_basis":        "mid",
        "premium_per_contract": premium_per_contract,
        "affordable_contracts": affordable_contracts,
    }


def test_t2_selector_returns_50c_contract_one_contract_50_cost():
    m = _make_mc(mode="live", account_equity=2000.0, max_capital_pct=0.10)
    plan = _make_plan_from_evaluate(m.evaluate(_hood_signal()))
    # Selector picks $0.50 contract, would say "afford 4 of these" ($200/$50)
    _simulate_selector_mutation(plan, premium_per_contract=50.0, affordable_contracts=4)
    decision = m.revalidate_exposure(plan)
    assert decision.ok, decision.reason
    # Bootstrap clamp must force contracts back to 1 and cost to $50
    assert plan.contracts == 1
    assert plan.max_position_usd == pytest.approx(50.0)


# ---------------------------------------------------------------------------
# Test 3: Selector could afford 4 cheap contracts -> still clamped to 1
# ---------------------------------------------------------------------------

def test_t3_selector_affordable_4_contracts_bootstrap_still_one():
    """Even when remaining_capital can buy 4 contracts, bootstrap is 1."""
    m = _make_mc(mode="live", account_equity=2000.0, max_capital_pct=0.10)
    plan = _make_plan_from_evaluate(m.evaluate(_hood_signal()))
    # Cheap $0.30 contract -> selector says affordable = 6
    _simulate_selector_mutation(plan, premium_per_contract=30.0, affordable_contracts=6)
    decision = m.revalidate_exposure(plan)
    assert decision.ok
    assert plan.contracts == 1, (
        f"bootstrap clamp failed: selector mutation produced "
        f"{plan.contracts} contracts"
    )
    assert plan.max_position_usd == pytest.approx(30.0)


# ---------------------------------------------------------------------------
# Test 4: Selector returns $1.95 contract -> 1 contract passes
# ---------------------------------------------------------------------------

def test_t4_selector_returns_195_contract_passes():
    m = _make_mc(mode="live", account_equity=2000.0, max_capital_pct=0.10)
    plan = _make_plan_from_evaluate(m.evaluate(_hood_signal()))
    _simulate_selector_mutation(plan, premium_per_contract=195.0, affordable_contracts=1)
    decision = m.revalidate_exposure(plan)
    assert decision.ok, decision.reason
    assert plan.contracts == 1
    assert plan.max_position_usd == pytest.approx(195.0)


# ---------------------------------------------------------------------------
# Test 5: Selector returns $3.50 contract -> revalidate blocks with
# ACTUAL_CONTRACT_COST_EXCEEDS_CLIENT_CAPITAL_LIMIT
# ---------------------------------------------------------------------------

def test_t5_selector_returns_350_contract_blocked_with_canonical_reason():
    m = _make_mc(mode="live", account_equity=2000.0, max_capital_pct=0.10)
    plan = _make_plan_from_evaluate(m.evaluate(_hood_signal()))
    # $3.50 contract (per-share $0.035) -> premium_per_contract = $350
    _simulate_selector_mutation(plan, premium_per_contract=350.0, affordable_contracts=1)
    decision = m.revalidate_exposure(plan)
    assert not decision.ok
    assert decision.reason_code == "ACTUAL_CONTRACT_COST_EXCEEDS_CLIENT_CAPITAL_LIMIT"
    assert "ACTUAL_CONTRACT_COST_EXCEEDS_CLIENT_CAPITAL_LIMIT" in decision.reason
    # Clamp still ran first \u2014 contracts must be 1 even on block
    assert plan.contracts == 1


# ---------------------------------------------------------------------------
# Test 6: Small LIVE client AFTER bootstrap ends \u2014 no static-fallback
# false-rejection.
# ---------------------------------------------------------------------------

def test_t6_post_bootstrap_small_live_no_static_false_reject():
    """A $2k LIVE client with total_trades >= bootstrap threshold (default 20)
    must NOT regress to the static-fallback pre-reject path. The
    affordability flow stays active for every LIVE client."""
    # Bump trades well above the default threshold to leave bootstrap mode.
    bootstrap_threshold = int(os.getenv("BOOTSTRAP_TRADES_THRESHOLD", "20"))
    m = _make_mc(
        mode="live",
        account_equity=2000.0,
        max_capital_pct=0.10,
        total_trades=bootstrap_threshold + 5,   # post-bootstrap
    )
    decision = m.evaluate(_hood_signal(score=70.0))
    # Must approve \u2014 no static-fallback false reject
    assert decision.ok, f"post-bootstrap LIVE should approve, got: {decision.reason}"
    plan = decision.plan
    ctx = plan.metadata["sizing_context"]
    assert ctx["bootstrap_mode"] is False
    assert ctx["affordability_flow"] is True, (
        "review fix #2: LIVE post-bootstrap must use affordability flow"
    )
    assert ctx["max_affordable_premium"] == pytest.approx(2.0)
    # max_position_usd is remaining_capital, not MAX_TRADE_USD env
    assert plan.max_position_usd == pytest.approx(200.0)


def test_t6b_post_bootstrap_selector_finds_cheap_contract_passes():
    """End-to-end variant of T6: post-bootstrap small LIVE picks a $1.20
    contract and revalidation approves. Multi-contract path because we're
    NOT in bootstrap, but the selector budget constrained to $200 means
    selector.affordable_contracts cannot exceed 1 anyway."""
    bootstrap_threshold = int(os.getenv("BOOTSTRAP_TRADES_THRESHOLD", "20"))
    m = _make_mc(
        mode="live",
        account_equity=2000.0,
        max_capital_pct=0.10,
        total_trades=bootstrap_threshold + 5,
    )
    plan = _make_plan_from_evaluate(m.evaluate(_hood_signal()))
    # Simulate selector picking a $1.20 contract; selector itself would
    # cap affordable to 1 because budget=$200, $120 + $120 = $240 > $200.
    _simulate_selector_mutation(plan, premium_per_contract=120.0, affordable_contracts=1)
    decision = m.revalidate_exposure(plan)
    assert decision.ok, decision.reason
    # NOT bootstrap \u2014 clamp does not fire; respect selector's qty
    assert plan.contracts == 1
    assert plan.max_position_usd == pytest.approx(120.0)


# ---------------------------------------------------------------------------
# Test 7: PAPER non-bootstrap behaviour unchanged
# ---------------------------------------------------------------------------

def test_t7_paper_non_bootstrap_static_pre_block_still_applies():
    """PAPER mode must keep the historical static pre-block so paper
    proof-week is comparable. With $2k * 10% = $200 cap and the static
    $3.50 HOOD fallback, evaluate() should reject before selection."""
    bootstrap_threshold = int(os.getenv("BOOTSTRAP_TRADES_THRESHOLD", "20"))
    m = _make_mc(
        mode="paper",
        account_equity=2000.0,
        max_capital_pct=0.10,
        total_trades=bootstrap_threshold + 5,   # PAPER non-bootstrap
    )
    decision = m.evaluate(_hood_signal(score=70.0))
    # PAPER non-bootstrap should still hit the static pre-block
    # (this matches pre-PR behaviour; we are intentionally not changing PAPER).
    assert not decision.ok
    assert "capital_limit" in decision.reason.lower()


# ---------------------------------------------------------------------------
# Test 8: max_capital_pct is NEVER raised by this PR
# ---------------------------------------------------------------------------

def test_t8_max_capital_pct_unchanged():
    m = _make_mc(mode="live", account_equity=2000.0, max_capital_pct=0.10)
    assert m.max_capital_pct == pytest.approx(0.10)
    # After running evaluate, the pct on the instance still must be 0.10
    m.evaluate(_hood_signal())
    assert m.max_capital_pct == pytest.approx(0.10)


# ---------------------------------------------------------------------------
# Test 9: Final real-cost revalidation is never weakened (sanity)
# ---------------------------------------------------------------------------

def test_t9_revalidate_blocks_when_selector_exceeds_cap():
    """Even outside bootstrap, revalidate_exposure must enforce the cap
    with REAL cost. Guards against future regressions."""
    bootstrap_threshold = int(os.getenv("BOOTSTRAP_TRADES_THRESHOLD", "20"))
    m = _make_mc(
        mode="live",
        account_equity=2000.0,
        max_capital_pct=0.10,
        total_trades=bootstrap_threshold + 5,
    )
    plan = _make_plan_from_evaluate(m.evaluate(_hood_signal()))
    # Force selector mutation to a $250 contract (above $200 cap)
    _simulate_selector_mutation(plan, premium_per_contract=250.0, affordable_contracts=1)
    decision = m.revalidate_exposure(plan)
    assert not decision.ok
    assert decision.reason_code == "ACTUAL_CONTRACT_COST_EXCEEDS_CLIENT_CAPITAL_LIMIT"
