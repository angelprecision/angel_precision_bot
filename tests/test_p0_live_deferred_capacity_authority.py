from __future__ import annotations

import os
import threading
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://dummy:dummy@127.0.0.1:1/dummy")

from ap_master_control import APMasterControl
from ap_execution_core import APExecutionCore


def _capacity_control(*, deployed: float = 0.0, pending: float | None = 0.0):
    control = object.__new__(APMasterControl)
    control.mode = "LIVE"
    control._mode_fn = lambda: "LIVE"
    control.max_position_pct = 0.10
    control.max_total_capital_pct = 0.40
    control.pending_capital_fail_closed_live = True
    control.account_equity = 1709.20
    control.max_daily_loss = -500.0
    control._equity_lock = threading.RLock()
    control._get_snapshot = lambda *_args, **_kwargs: {
        "capital_deployed": deployed,
        "_snapshot_ok": True,
    }
    control._pending_capital_from_snapshot_or_db = lambda *_args, **_kwargs: pending
    control._equity_snapshot = lambda: (control.account_equity, control.max_daily_loss)
    return control


def test_live_deferred_capacity_is_affordability_not_actual_cost():
    control = _capacity_control()
    result = control.get_entry_capacity(
        client_id="jason@example.com",
        execution_mode="LIVE",
        ticker="C",
        signal_id="sig-514",
        exclude_local_order_id="oid-514",
    )
    assert result["ok"] is True
    assert result["execution_mode"] == "live"
    assert result["client_id"] == "jason@example.com"
    assert result["per_trade_budget"] == pytest.approx(170.92)
    assert result["remaining_total_capacity"] == pytest.approx(683.68)
    assert result["selector_budget"] == pytest.approx(170.92)
    assert result["max_affordable_premium"] == pytest.approx(1.7092)
    assert result["exclude_local_order_id"] == "oid-514"
    assert "actual_selected_cost" not in result


def test_live_deferred_capacity_exposes_total_cap_headroom_independently():
    control = _capacity_control(deployed=600.0)
    result = control.get_entry_capacity(
        client_id="jason@example.com",
        execution_mode="live",
        ticker="C",
        signal_id="sig-514-total",
    )
    assert result["ok"] is True
    assert result["per_trade_budget"] == pytest.approx(170.92)
    assert result["remaining_total_capacity"] == pytest.approx(83.68)
    assert result["selector_budget"] == pytest.approx(83.68)

    exhausted = _capacity_control(deployed=684.0)
    blocked = exhausted.get_entry_capacity(
        client_id="jason@example.com",
        execution_mode="live",
        ticker="C",
        signal_id="sig-514-total-block",
    )
    assert blocked["ok"] is False
    assert blocked["reason_code"] == "CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED"


@pytest.mark.parametrize(
    ("mode", "expected_reason"),
    [
        ("staging", "INVALID_EXECUTION_MODE"),
        ("paper", "EXECUTION_MODE_MISMATCH"),
        ("", "INVALID_EXECUTION_MODE"),
    ],
)
def test_live_deferred_capacity_requires_explicit_matching_mode(mode, expected_reason):
    control = _capacity_control()
    result = control.get_entry_capacity(
        client_id="jason@example.com",
        execution_mode=mode,
        ticker="C",
        signal_id="sig-514-mode",
    )
    assert result["ok"] is False
    assert result["reason_code"] == expected_reason


def test_live_deferred_capacity_blocks_unavailable_pending_truth():
    control = _capacity_control(pending=None)
    result = control.get_entry_capacity(
        client_id="jason@example.com",
        execution_mode="live",
        ticker="C",
        signal_id="sig-514-pending",
    )
    assert result["ok"] is False
    assert result["reason_code"] == "PENDING_CAPITAL_UNAVAILABLE"


def _breach_core_for_plan(plan, revalidation_result):
    calls = []
    control = SimpleNamespace(
        _kill_switch_fn=lambda: False,
        revalidate_exposure=lambda *_args, **_kwargs: (
            calls.append(True) or revalidation_result
        ),
    )
    core = object.__new__(APExecutionCore)
    core.mode = "LIVE"
    core.email = "jason@example.com"
    core._max_positions = 5
    core._kill_switch = False
    core.master_control = control
    core._current_open_position_count = lambda: 0
    core._current_pending_entry_count = lambda: 0
    core._recover_plan_for_revalidation = lambda _watched: plan
    core._emit_breach_diag = lambda *_args, **_kwargs: None
    core.store = SimpleNamespace(update_signal_fields=lambda *_args, **_kwargs: None)
    return core, calls


def test_deferred_live_breach_defers_actual_cost_gate_until_materialization():
    plan = SimpleNamespace(
        execution_mode="live",
        contract_symbol="DEFERRED:C",
        metadata={"contract_deferred": True},
    )
    core, calls = _breach_core_for_plan(
        plan,
        SimpleNamespace(ok=False, reason="reservation_is_not_actual_cost"),
    )
    watched = SimpleNamespace(
        signal={"signal_id": "sig-514-breach"},
        ticker="C",
    )

    assert APExecutionCore._breach_risk_check(core, watched) is True
    assert calls == []


def test_materialized_live_breach_keeps_existing_exposure_gate():
    plan = SimpleNamespace(
        execution_mode="live",
        contract_symbol="C260828C00133000",
        metadata={},
    )
    core, calls = _breach_core_for_plan(
        plan,
        SimpleNamespace(ok=True, reason="allowed"),
    )
    watched = SimpleNamespace(
        signal={"signal_id": "sig-514-materialized"},
        ticker="C",
    )

    assert APExecutionCore._breach_risk_check(core, watched) is True
    assert calls == [True]
