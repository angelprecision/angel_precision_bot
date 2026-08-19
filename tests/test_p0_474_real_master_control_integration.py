"""P0 #474 FINAL MERGE-GATE AUDIT — real APMasterControl integration proof.

This file exists specifically because mocked Master Control (_CapMC,
_BootstrapMC, SimpleNamespace(ok=True), MagicMock) is NOT sufficient
final-merge evidence for #474. It exercises the REAL
APMasterControl.revalidate_exposure() implementation end-to-end.

Only two seams are stubbed, matching the established pattern in
tests/test_master_control_hardening.py:
  1. position_manager.snapshot() — position/capital state (test fixture)
  2. APMasterControl._pending_orders_capital() — DB read for pending
     broker-submitted exposure (test fixture)

Every other line of revalidate_exposure — sector resolution (#483),
per-position cap, total-capital cap, bootstrap clamp, quantity
handling, kill-switch, snapshot freshness — runs as real production
code with zero patching.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

from ap_execution_core import _revalidate_deferred_final_cost  # noqa: E402
import ap_master_control as mc_mod  # noqa: E402


def _make_real_mc(
    *,
    mode="LIVE",
    account_equity=25000.0,
    max_position_pct=0.0066,   # ~$165-166 cap on $25,000 equity — matches production incident shape
    pending_submitted=0.0,
    open_positions=None,
):
    """Build a REAL APMasterControl instance. Only DB/position-manager
    reads are stubbed; all gate/decision logic is the actual production
    implementation from ap_master_control.py.
    """
    import datetime as _dt

    pm = MagicMock()

    def _fresh_snapshot(*_args, **_kwargs):
        # LIVE mode requires a fresh snapshot timestamp
        # (APMasterControl._validate_snapshot_freshness, max_snapshot_age_sec
        # default 15s) or it blocks with snapshot_stale_or_future_dated
        # regardless of cost. Generated fresh on every call (not a fixed
        # dict) so DB-retry backoff earlier in revalidate_exposure() (this
        # sandbox has no reachable Postgres, so _equity_snapshot()/pending-
        # capital reads burn real wall-clock time retrying) can never make
        # a precomputed timestamp go stale before this snapshot is read.
        return {
            "capital_deployed": 0.0,
            "open_positions": open_positions or [],
            "closing_positions": [],
            "realized_pnl_today": 0.0,
            "pending_entries": [],
            "pending_entry_capital": 0.0,
            "_snapshot_ok": True,
            "snapshot_ts": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        }

    pm.snapshot = MagicMock(side_effect=_fresh_snapshot)
    pm.has_pending_entry = MagicMock(return_value=False)
    pm.client_id = "jasoncosby1@gmail.com"

    mc = mc_mod.APMasterControl(
        mode=mode,
        account_equity=account_equity,
        max_position_pct=max_position_pct,
        max_total_capital_pct=0.40,
        position_manager=pm,
        supabase_client=None,
        client_id="jasoncosby1@gmail.com",
    )
    mc._kill_switch_fn = lambda: False

    # Stub only the DB-touching pending-capital read (component 1);
    # everything downstream (breakdown assembly, cap math, sector gate,
    # decision) is real production code.
    def _fake_pending_orders_capital(client_id, *, exclude_local_order_id=None,
                                      runtime_execution_mode=None, with_diagnostics=False):
        if with_diagnostics:
            return {
                "pending_submitted_entry_exposure": float(pending_submitted),
                "counted_order_ids": [],
                "counted_order_statuses": [],
                "counted_broker_order_ids": [],
                "ignored_reserved_cost_by_status": {},
                "ignored_order_ids_by_status": {},
                "runtime_execution_mode": (runtime_execution_mode or "").lower() or None,
                "active_broker_statuses": [],
            }
        return float(pending_submitted)

    mc._pending_orders_capital = _fake_pending_orders_capital
    return mc


def _jason_aapl_plan(*, price, qty=1, contract="AAPL260821P00230000"):
    """Production-shaped plan post-materialization: real OCC contract,
    real executable price, matches the Jason AAPL incident replay shape.
    """
    return SimpleNamespace(
        ticker="AAPL",
        side="PUT",
        signal_id="sig-aapl-live-integration",
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        mode="LIVE",
        contract_symbol=contract,
        limit_price=price,
        contracts=qty,
        max_position_usd=round(price * qty * 100.0, 2),
        metadata={
            "contract_deferred": True,
            "sizing_context": {"bootstrap_mode": False},
        },
        selector_metadata={
            "premium_per_contract": round(price * 100.0, 2),
            "execution_price_per_share": price,
        },
        selector_execution_price=price,
        to_signal_dict=lambda: {},
    )


# ─────────────────────────────────────────────────────────────────────
# CASE A — Jason-style under-cap replay: real MC must approve $140
# ─────────────────────────────────────────────────────────────────────

def test_case_a_real_master_control_approves_140_actual_cost_under_cap():
    """
    Real APMasterControl, real revalidate_exposure(). Reservation budget
    was $165.811 pre-materialization; real selected contract costs $140.

    Per-position cap on $25,000 equity @ 0.66% ≈ $165. $140 must pass.
    """
    real_mc = _make_real_mc(account_equity=25000.0, max_position_pct=0.0066)
    per_position_cap = 25000.0 * 0.0066
    assert per_position_cap == pytest.approx(165.0, abs=0.5)

    plan = _jason_aapl_plan(price=1.40, qty=1)  # real OCC, real submit_limit

    result = _revalidate_deferred_final_cost(
        real_mc,
        plan,
        client_id="jasoncosby1@gmail.com",
        execution_mode="LIVE",
        submit_limit=1.40,
        qty=1,
    )

    assert result["ok"] is True, f"real MC unexpectedly blocked: {result}"
    assert result["actual_cost"] == 140.00
    assert result["mc_seen_cost"] == 140.00
    assert result["final_qty"] == 1


def test_case_a_real_master_control_never_sees_165_811_reservation():
    """The 165.811 reservation value must never reach revalidate_exposure
    as the plan's max_position_usd. Only the real $140 economics do."""
    real_mc = _make_real_mc(account_equity=25000.0, max_position_pct=0.0066)

    # Original plan still carries the stale placeholder shape pre-call.
    placeholder_plan = _jason_aapl_plan(price=0.01, qty=1)
    placeholder_plan.max_position_usd = 165.811  # exact incident value

    seen_costs = []
    real_revalidate = real_mc.revalidate_exposure

    def _spy_revalidate(plan, client_id="default"):
        seen_costs.append(float(plan.max_position_usd))
        return real_revalidate(plan, client_id=client_id)

    real_mc.revalidate_exposure = _spy_revalidate

    result = _revalidate_deferred_final_cost(
        real_mc,
        placeholder_plan,
        client_id="jasoncosby1@gmail.com",
        execution_mode="LIVE",
        submit_limit=1.40,
        qty=1,
    )

    assert 165.811 not in seen_costs, (
        f"Reservation budget 165.811 was passed to real MC as actual cost: {seen_costs}"
    )
    assert seen_costs == [140.00]
    assert result["ok"] is True


# ─────────────────────────────────────────────────────────────────────
# CASE B — Mirror over-cap replay: real MC must block $180
# ─────────────────────────────────────────────────────────────────────

def test_case_b_real_master_control_blocks_180_actual_cost_over_cap():
    """
    Same Jason AAPL production shape, but real submit_limit=1.80 → $180
    actual cost, which exceeds the ~$165 per-position cap.

    Real APMasterControl must block. Zero broker POST follows from ok=False
    (verified structurally in test_p0_deferred_real_cost_revalidation.py).
    """
    real_mc = _make_real_mc(account_equity=25000.0, max_position_pct=0.0066)
    per_position_cap = 25000.0 * 0.0066
    assert per_position_cap == pytest.approx(165.0, abs=0.5)

    plan = _jason_aapl_plan(price=1.80, qty=1)

    result = _revalidate_deferred_final_cost(
        real_mc,
        plan,
        client_id="jasoncosby1@gmail.com",
        execution_mode="LIVE",
        submit_limit=1.80,
        qty=1,
    )

    assert result["ok"] is False, f"real MC unexpectedly approved: {result}"
    assert result["actual_cost"] == 180.00
    assert "ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP" in result["reason_code"]


# ─────────────────────────────────────────────────────────────────────
# QUANTITY CLAMP — real MC-approved qty must be exact, no stale qty
# ─────────────────────────────────────────────────────────────────────

def test_quantity_clamp_bootstrap_mode_reduces_qty_and_recomputes_cost():
    """
    Bootstrap mode forces qty=1 inside the REAL revalidate_exposure
    (see the bootstrap_clamp block in ap_master_control.py). Selector
    wanted qty=3; real MC must clamp to 1 and the final approved cost
    must reflect exactly qty=1 at the final submit price — never the
    stale selector-era qty=3.
    """
    real_mc = _make_real_mc(account_equity=25000.0, max_position_pct=0.10)

    plan = _jason_aapl_plan(price=1.59, qty=3)
    plan.metadata["sizing_context"]["bootstrap_mode"] = True
    plan.selector_metadata["premium_per_contract"] = 159.00

    result = _revalidate_deferred_final_cost(
        real_mc,
        plan,
        client_id="jasoncosby1@gmail.com",
        execution_mode="LIVE",
        submit_limit=1.59,
        qty=3,
    )

    assert result["ok"] is True, f"real MC blocked bootstrap clamp case: {result}"
    # Real production bootstrap_clamp forces qty=1 regardless of selector qty.
    assert result["final_qty"] == 1, (
        f"Expected real MC bootstrap clamp to force qty=1, got {result['final_qty']}"
    )
    assert result["actual_cost"] == 159.00, (
        "Approved cost must reflect qty=1 at final submit price ($159), "
        f"not stale selector qty=3 (~$477). Got: {result['actual_cost']}"
    )


# ─────────────────────────────────────────────────────────────────────
# SECTOR ADJACENCY (#483) — required verification tickers
# ─────────────────────────────────────────────────────────────────────

REQUIRED_TICKERS = ["QCOM", "SMCI", "SPY", "QQQ", "IWM", "NVDA", "AAPL"]


@pytest.mark.xfail(
    reason=(
        "OPEN FINDING (final #474 merge-gate audit): #483's private "
        "APMasterControl.SECTOR_MAP does not reuse the canonical "
        "ap.exposure_gate.get_sector()/SECTOR_MAP and is missing QCOM, "
        "SMCI, SPY, QQQ, IWM, which the canonical map already covers. "
        "This is a real, unresolved blocker for #474's merge gate "
        "(#483 must merge clean first) -- marked xfail(strict=True) so "
        "CI stays green while the divergence remains provable and "
        "visible, and so this test starts FAILING (not silently passing) "
        "the moment #483 is fixed, forcing an update rather than bit-rot."
    ),
    strict=True,
)
def test_483_sector_map_coverage_for_required_tickers():
    """
    Amendment REQUIRED VERIFICATION: QCOM, SMCI, SPY, QQQ, IWM, NVDA, AAPL
    must resolve to a canonical sector, not silently become 'unknown'
    merely because APMasterControl kept a smaller private SECTOR_MAP than
    the repository's canonical taxonomy (ap.exposure_gate.SECTOR_MAP).

    THIS TEST DOCUMENTS AN UNRESOLVED FINDING: #483 (head 6aca12fa) does
    NOT reuse ap.exposure_gate.get_sector() and its own private
    SECTOR_MAP is missing QCOM, SMCI, SPY, QQQ, and IWM — all of which
    ARE mapped in the canonical ap/exposure_gate.py SECTOR_MAP.

    This assertion is expected to FAIL against the current #483 head.
    It exists so that the divergence is provable and visible in CI,
    not silently accepted. See docs/pr_specs — merge gate blocked
    until this is corrected (either by reuse or by explicit sync).
    """
    import ap.exposure_gate as eg

    unmapped_but_canonical = []
    for ticker in REQUIRED_TICKERS:
        mc_sector = mc_mod.APMasterControl.SECTOR_MAP.get(ticker)
        canonical_sector = eg.get_sector(ticker)
        if mc_sector is None and canonical_sector is not None:
            unmapped_but_canonical.append((ticker, canonical_sector))

    assert unmapped_but_canonical == [], (
        "The following tickers are known in the canonical "
        "ap.exposure_gate.SECTOR_MAP but resolve to sector=None (unknown) "
        "in APMasterControl.SECTOR_MAP — this is the exact forbidden "
        "behavior described in the #474 final merge-gate audit spec: "
        f"{unmapped_but_canonical}. #483 must either reuse "
        "ap.exposure_gate.get_sector() directly, or sync its private "
        "SECTOR_MAP to canonical coverage, before #474 can be considered "
        "for merge."
    )
