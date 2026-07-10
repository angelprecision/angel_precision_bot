from __future__ import annotations

import json
import logging
import os
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

import ap_master_control as mc


def _make_master_control_for_log() -> mc.APMasterControl:
    obj = mc.APMasterControl.__new__(mc.APMasterControl)
    obj.account_equity = 5000.0
    obj._alert_degraded = MagicMock()
    return obj


def _invoke_capital_log(caplog, *, blocked: bool, block_reason: str):
    records: list[dict] = []
    master_control = _make_master_control_for_log()

    class _FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, _sql, params):
            records.append(json.loads(params[1]))

    def _run_with_retry(fn, **_kwargs):
        fn()

    with patch("ap.db.conn", return_value=_FakeConn()), patch(
        "ap.db.run_with_retry", side_effect=_run_with_retry
    ):
        with caplog.at_level(logging.INFO, logger="ap.master_control"):
            master_control._log_capital_utilization(
                client_id="jason@example.com",
                execution_mode="live",
                ticker="GS",
                signal_id="SIG-GS-1",
                deployed=380.0,
                pending=0.0,
                new_cost=190.0,
                projected=570.0,
                limit=200.0,
                per_trade_budget=200.0,
                total_capital_cap=1000.0,
                current_deployed=380.0,
                pending_reserved=0.0,
                remaining_capacity=430.0,
                pct_used=57.0,
                sector="financials",
                sector_deployed=200.0,
                sector_projected=390.0,
                sector_limit=500.0,
                ticker_deployed=0.0,
                ticker_projected=190.0,
                ticker_limit=250.0,
                blocked=blocked,
                block_reason=block_reason,
            )

    return caplog.text, records[0]


def _make_revalidation_master_control() -> mc.APMasterControl:
    master_control = mc.APMasterControl(
        mode="paper",
        score_floor=60.0,
        context_floor=0.0,
        account_equity=5000.0,
        max_capital_pct=0.10,
        pending_capital_fail_closed_live=False,
        require_snapshot_freshness_live=False,
    )
    master_control._kill_switch_fn = lambda: False
    master_control._equity_snapshot = MagicMock(return_value=(5000.0, 0.0))
    master_control._get_snapshot = MagicMock(
        return_value={
            "_snapshot_ok": True,
            "_snapshot_ts": "2026-07-09T16:00:00Z",
            "_snapshot_age_sec": 0.1,
            "open_count": 0,
            "pending_entries": 0,
            "capital_deployed": 0.0,
            "calls_open": 0,
            "puts_open": 0,
            "trades_today": 0,
            "open_positions": [],
            "closing_positions": [],
            "realized_pnl_today": 0.0,
            "total_trades": 0,
            "ticker_open_counts": {},
            "ticker_pending_counts": {},
        }
    )
    master_control._pending_capital_from_snapshot_or_db = MagicMock(return_value=0.0)
    master_control._get_pending_capital_breakdown = MagicMock(
        return_value={
            "pending_submitted_entry_exposure": 0.0,
            "filled_unreconciled_exposure": 0.0,
            "pending_total_capital_reserved": 0.0,
        }
    )
    master_control._sector_capital_deployed = MagicMock(return_value=0.0)
    master_control._ticker_capital_deployed = MagicMock(return_value=0.0)
    master_control._alert_degraded = MagicMock()
    return master_control


def _make_plan(*, max_position_usd: float) -> mc.ApprovedExecutionPlan:
    return mc.ApprovedExecutionPlan(
        plan_id="plan-capital-1",
        signal_id="sig-capital-1",
        client_id="jason@example.com",
        ticker="GS",
        side="CALL",
        direction="CALL",
        pattern="BREAKOUT",
        timeframe="1d",
        contracts=1,
        max_position_usd=max_position_usd,
        tier="A",
        score=80.0,
        intel_score=0.0,
        confidence_bucket="standard_pool",
        trigger_type="breach",
        trigger_price=500.0,
        stop_underlying=499.0,
        target_underlying=505.0,
        metadata={"local_order_id": "lo-capital-1"},
        mode="PAPER",
        paper_sim=True,
    )


def test_capital_gate_approved_fields_are_separate_and_explicit(caplog):
    text, record = _invoke_capital_log(caplog, blocked=False, block_reason="")

    assert "CAPITAL_GATE" in text
    assert "selected_trade_cost=$190.00 per_trade_budget=$200.00" in text
    assert (
        "projected_portfolio_exposure=$570.00 total_capital_cap=$1000.00"
        in text
    )
    assert "current_deployed=$380.00 pending_reserved=$0.00" in text
    assert "remaining_capacity=$430.00" in text
    assert "decision=APPROVED block_reason=none" in text
    assert record["client_id"] == "jason@example.com"
    assert record["execution_mode"] == "live"
    assert record["selected_trade_cost"] == pytest.approx(190.0)
    assert record["per_trade_budget"] == pytest.approx(200.0)
    assert record["projected_portfolio_exposure"] == pytest.approx(570.0)
    assert record["total_capital_cap"] == pytest.approx(1000.0)
    assert record["remaining_capacity"] == pytest.approx(430.0)
    assert record["schema_version"] == 2
    assert record["legacy_aliases_preserved"] is True
    assert record["decision"] == "APPROVED"


def test_capital_gate_blocked_case_keeps_block_reason_explicit(caplog):
    text, record = _invoke_capital_log(
        caplog,
        blocked=True,
        block_reason="ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP",
    )

    assert "decision=BLOCKED" in text
    assert "block_reason=ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP" in text
    assert record["decision"] == "BLOCKED"
    assert (
        record["block_reason"] == "ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP"
    )


def test_legacy_aliases_match_canonical_values(caplog):
    _text, record = _invoke_capital_log(caplog, blocked=False, block_reason="")

    assert record["deployed"] == pytest.approx(record["current_deployed"])
    assert record["pending"] == pytest.approx(record["pending_reserved"])
    assert record["new_cost"] == pytest.approx(record["selected_trade_cost"])
    assert record["projected"] == pytest.approx(
        record["projected_portfolio_exposure"]
    )
    assert record["limit"] == pytest.approx(record["total_capital_cap"])
    assert record["headroom"] == pytest.approx(record["remaining_capacity"])


def test_capital_gate_never_mixes_trade_and_portfolio_comparisons(caplog):
    text, _record = _invoke_capital_log(caplog, blocked=False, block_reason="")

    assert "CAPITAL_UTIL" not in text
    assert not re.search(r"projected=\$[\d.]+/[\d.]+", text)
    assert "selected_trade_cost=" in text
    assert "per_trade_budget=" in text
    assert "projected_portfolio_exposure=" in text
    assert "total_capital_cap=" in text
    assert " deployed=$" not in text
    assert " pending=$" not in text
    assert " new_cost=$" not in text
    assert " projected=$" not in text
    assert " headroom=$" not in text


def test_structured_record_matches_logged_values(caplog):
    text, record = _invoke_capital_log(caplog, blocked=False, block_reason="")

    assert f"client_id={record['client_id']}" in text
    assert f"execution_mode={record['execution_mode']}" in text
    assert (
        f"selected_trade_cost=${record['selected_trade_cost']:.2f} "
        f"per_trade_budget=${record['per_trade_budget']:.2f}"
    ) in text
    assert (
        f"projected_portfolio_exposure=${record['projected_portfolio_exposure']:.2f} "
        f"total_capital_cap=${record['total_capital_cap']:.2f}"
    ) in text
    assert f"remaining_capacity=${record['remaining_capacity']:.2f}" in text
    assert f"decision={record['decision']}" in text


@pytest.mark.parametrize(
    ("max_position_usd", "expected_ok", "expected_reason_code"),
    [
        (190.0, True, ""),
        (700.0, False, "ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP"),
    ],
)
def test_missing_audit_sink_does_not_change_gate_decision(
    caplog, max_position_usd, expected_ok, expected_reason_code
):
    master_control = _make_revalidation_master_control()
    plan = _make_plan(max_position_usd=max_position_usd)

    def _run_with_retry(fn, **_kwargs):
        fn()

    with patch("ap.db.conn", side_effect=RuntimeError("audit sink unavailable")), patch(
        "ap.db.run_with_retry", side_effect=_run_with_retry
    ):
        with caplog.at_level(logging.WARNING, logger="ap.master_control"):
            decision = master_control.revalidate_exposure(
                plan, client_id="jason@example.com"
            )

    assert decision.ok is expected_ok
    assert decision.reason_code == expected_reason_code
    assert "Capital utilization audit write failed (non-critical)" in caplog.text
    master_control._alert_degraded.assert_called()
