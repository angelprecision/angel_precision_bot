from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import ap_master_control as mc


def _make_mc(
    *,
    mode: str,
    account_equity: float,
    max_capital_pct: float,
    capital_deployed: float = 0.0,
    pending_capital: float = 0.0,
    total_trades: int = 30,
    sector_deployed: float = 0.0,
    ticker_deployed: float = 0.0,
    max_sector_pct: float = 0.25,
    max_ticker_pct: float = 0.25,
):
    m = mc.APMasterControl(
        mode=mode,
        score_floor=60.0,
        context_floor=0.0,
        account_equity=account_equity,
        max_capital_pct=max_capital_pct,
        max_sector_pct=max_sector_pct,
        max_ticker_pct=max_ticker_pct,
        pending_capital_fail_closed_live=False,
        require_snapshot_freshness_live=False,
    )
    m._kill_switch_fn = lambda: False
    m._equity_snapshot = MagicMock(return_value=(account_equity, -500.0))
    m._get_snapshot = MagicMock(return_value={
        "_snapshot_ok": True,
        "_snapshot_ts": "2026-06-11T16:00:00Z",
        "_snapshot_age_sec": 0.1,
        "open_count": 0,
        "pending_entries": 0,
        "capital_deployed": capital_deployed,
        "calls_open": 0,
        "puts_open": 0,
        "trades_today": 0,
        "open_positions": [],
        "closing_positions": [],
        "realized_pnl_today": 0.0,
        "total_trades": total_trades,
        "ticker_open_counts": {},
        "ticker_pending_counts": {},
    })
    m._pending_capital_from_snapshot_or_db = MagicMock(return_value=pending_capital)
    m._get_pending_capital_breakdown = MagicMock(return_value={
        "pending_submitted_entry_exposure": pending_capital,
        "filled_unreconciled_exposure": 0.0,
        "pending_total_capital_reserved": pending_capital,
    })
    m._sector_capital_deployed = MagicMock(return_value=sector_deployed)
    m._ticker_capital_deployed = MagicMock(return_value=ticker_deployed)
    m._log_capital_utilization = MagicMock()
    return m


def _make_plan(
    *,
    client_id: str,
    contracts: int,
    max_position_usd: float,
    mode: str,
    bootstrap_mode: bool = False,
):
    return mc.ApprovedExecutionPlan(
        plan_id=f"plan-{client_id}",
        signal_id=f"sig-{client_id}",
        client_id=client_id,
        ticker="HOOD",
        side="CALL",
        direction="CALL",
        pattern="2-2",
        timeframe="1d",
        contracts=contracts,
        max_position_usd=max_position_usd,
        tier="B",
        score=75.0,
        intel_score=0.0,
        confidence_bucket="HIGH",
        trigger_type="MARKET",
        trigger_price=22.0,
        stop_underlying=21.0,
        target_underlying=24.0,
        contract_symbol="HOOD260612C00022000",
        limit_price=max_position_usd / max(contracts, 1) / 100.0,
        mode=mode.upper(),
        paper_sim=(mode.lower() != "live"),
        metadata={"sizing_context": {"bootstrap_mode": bootstrap_mode}},
    )


def test_paper_affordable_qty_resize_allows_trade():
    m = _make_mc(
        mode="paper",
        account_equity=19_704.0,
        max_capital_pct=0.10,
    )
    plan = _make_plan(
        client_id="tradefluence",
        contracts=3,
        max_position_usd=2_610.0,
        mode="paper",
    )

    decision = m.revalidate_exposure(plan)

    assert decision.ok, decision.reason
    assert plan.contracts == 2
    assert plan.max_position_usd == pytest.approx(1_740.0)


def test_paper_unaffordable_single_contract_blocks_with_new_reason():
    m = _make_mc(
        mode="paper",
        account_equity=19_704.0,
        max_capital_pct=0.10,
    )
    plan = _make_plan(
        client_id="tradefluence",
        contracts=1,
        max_position_usd=2_250.0,
        mode="paper",
    )

    decision = m.revalidate_exposure(plan)

    assert not decision.ok
    assert decision.reason_code == "CAPITAL_LIMIT_CONTRACT_UNAFFORDABLE"
    assert "execution_mode=paper" in decision.reason


def test_live_small_account_resize_still_works_post_bootstrap():
    m = _make_mc(
        mode="live",
        account_equity=1_980.0,
        max_capital_pct=0.10,
        total_trades=30,
    )
    plan = _make_plan(
        client_id="jason",
        contracts=15,
        max_position_usd=1_485.0,
        mode="live",
    )

    decision = m.revalidate_exposure(plan)

    assert decision.ok, decision.reason
    assert plan.contracts == 2
    assert plan.max_position_usd == pytest.approx(198.0)


def test_pending_and_deployed_reduce_remaining_capital_before_resize():
    m = _make_mc(
        mode="paper",
        account_equity=19_704.0,
        max_capital_pct=0.10,
        capital_deployed=100.0,
        pending_capital=400.0,
    )
    plan = _make_plan(
        client_id="tradefluence",
        contracts=3,
        max_position_usd=2_610.0,
        mode="paper",
    )

    decision = m.revalidate_exposure(plan)

    assert decision.ok, decision.reason
    assert plan.contracts == 1
    assert plan.max_position_usd == pytest.approx(870.0)


def test_sector_and_ticker_caps_use_resized_real_cost():
    m = _make_mc(
        mode="paper",
        account_equity=20_000.0,
        max_capital_pct=0.10,
        sector_deployed=500.0,
        ticker_deployed=500.0,
        max_sector_pct=0.15,
        max_ticker_pct=0.15,
    )
    plan = _make_plan(
        client_id="tradefluence",
        contracts=3,
        max_position_usd=2_610.0,
        mode="paper",
    )

    decision = m.revalidate_exposure(plan)

    assert decision.ok, decision.reason
    assert plan.contracts == 2
    assert plan.max_position_usd == pytest.approx(1_740.0)


def test_source_uses_generic_small_account_log_names():
    text = (ROOT / "ap_master_control.py").read_text(encoding="utf-8")
    assert "SMALL_ACCOUNT_AFFORDABLE_QTY_RESIZE" in text
    assert "SMALL_ACCOUNT_CONTRACT_UNAFFORDABLE" in text
    assert "LIVE_SMALL_ACCOUNT_RESIZE" not in text
    assert "LIVE_SMALL_ACCOUNT_UNAFFORDABLE" not in text
