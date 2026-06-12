from __future__ import annotations

import os
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
PM_SRC = (REPO_ROOT / "ap" / "position_manager.py").read_text()
MC_SRC = (REPO_ROOT / "ap_master_control.py").read_text()

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_p0_pending_capital_truth",
)
os.environ.setdefault("ENCRYPTION_KEY", "angel-pr-p0-pending-capital-truth")

if "supabase" not in sys.modules:
    supabase_stub = types.ModuleType("supabase")
    supabase_stub.create_client = lambda *args, **kwargs: None
    supabase_stub.Client = type("Client", (), {})
    sys.modules["supabase"] = supabase_stub


@pytest.fixture(scope="module")
def modules():
    import ap.position_manager as pm_mod
    import ap_master_control as mc_mod

    return pm_mod, mc_mod


def _make_mc(mc_mod):
    pm = MagicMock()
    pm.snapshot = MagicMock(return_value={})
    pm.has_pending_entry = MagicMock(return_value=False)
    return mc_mod.APMasterControl(
        mode="paper",
        account_equity=25_000.0,
        position_manager=pm,
        supabase_client=None,
    )


def _pending_diag(amount: float) -> dict:
    return {
        "pending_submitted_entry_exposure": amount,
        "counted_order_ids": ["ord-1"] if amount else [],
        "counted_order_statuses": ["SUBMITTED"] if amount else [],
        "counted_broker_order_ids": ["br-1"] if amount else [],
        "ignored_reserved_cost_by_status": {},
        "ignored_order_ids_by_status": {},
        "runtime_execution_mode": "live",
        "active_broker_statuses": ["SUBMITTED", "OPEN", "ACKNOWLEDGED"],
    }


def test_snapshot_exposes_split_capital_truth_fields():
    for field in [
        "position_capital_deployed",
        "entry_attempt_reserved_cost",
        "filled_unreconciled_entry_capital",
        "ignored_already_reconciled_fill_capital",
        "ignored_already_reconciled_order_ids",
        "ignored_reconciled_match_keys",
        "open_position_ids",
    ]:
        assert field in PM_SRC, f"snapshot must expose {field}"


def test_capital_breakdown_proof_log_has_required_fields():
    idx = MC_SRC.find("CAPITAL_BREAKDOWN_PROOF")
    assert idx > 0, "CAPITAL_BREAKDOWN_PROOF log must exist"
    region = MC_SRC[idx:idx + 1000]
    for field in [
        "capital_deployed",
        "position_capital_deployed",
        "pending_submitted_entry_exposure",
        "filled_unreconciled_entry_capital",
        "ignored_already_reconciled_fill_capital",
        "counted_order_ids",
        "ignored_order_ids_by_status",
        "ignored_already_reconciled_order_ids",
        "ignored_reconciled_match_keys",
        "open_position_ids",
        "runtime_execution_mode",
    ]:
        assert field in region, f"CAPITAL_BREAKDOWN_PROOF missing {field}"


def test_linked_reconciled_open_fill_is_ignored_not_counted(modules):
    pm_mod, _ = modules
    summary = pm_mod._summarize_fill_truth_rows(
        [
            {
                "position_id": "pos-1",
                "id": "ord-1",
                "direction": "CALL",
                "fill_price": 2.50,
                "filled_qty": 1,
                "reserved_cost": None,
            }
        ],
        [{"id": "pos-1", "status": "OPEN"}],
    )
    assert summary["pending_entries"] == 0
    assert summary["filled_unreconciled_entry_capital"] == pytest.approx(0.0)
    assert summary["ignored_already_reconciled_fill_capital"] == pytest.approx(250.0)
    assert summary["ignored_already_reconciled_order_ids"] == ["ord-1"]
    assert summary["ignored_reconciled_match_keys"] == ["ord-1:position_id:pos-1"]


def test_unlinked_but_represented_fill_is_ignored_when_active_position_matches(modules):
    pm_mod, _ = modules
    summary = pm_mod._summarize_fill_truth_rows(
        [
            {
                "position_id": None,
                "broker_order_id": "br-1",
                "contract": "SPY_061226C00550000",
                "direction": "CALL",
                "fill_price": 2.50,
                "filled_qty": 1,
                "reserved_cost": None,
            }
        ],
        [
            {
                "id": "pos-1",
                "status": "OPEN",
                "broker_order_id": "br-1",
                "contract": "SPY_061226C00550000",
            }
        ],
    )
    assert summary["pending_entries"] == 0
    assert summary["filled_unreconciled_entry_capital"] == pytest.approx(0.0)
    assert summary["ignored_already_reconciled_fill_capital"] == pytest.approx(250.0)
    assert summary["ignored_already_reconciled_order_ids"] == ["br-1"]
    assert summary["ignored_reconciled_match_keys"] == ["br-1:broker_order_id:pos-1"]


def test_truly_unreconciled_fill_counts_as_filled_unreconciled_capital(modules):
    pm_mod, _ = modules
    summary = pm_mod._summarize_fill_truth_rows(
        [
            {
                "position_id": None,
                "broker_order_id": "br-2",
                "direction": "PUT",
                "fill_price": 0.61,
                "filled_qty": 1,
                "reserved_cost": None,
            }
        ],
        [],
    )
    assert summary["pending_entries"] == 1
    assert summary["filled_unreconciled_puts"] == 1
    assert summary["filled_unreconciled_entry_capital"] == pytest.approx(61.0)
    assert summary["ignored_already_reconciled_fill_capital"] == pytest.approx(0.0)
    assert summary["ignored_already_reconciled_order_ids"] == []
    assert summary["ignored_reconciled_match_keys"] == []


def test_breakdown_prefers_new_field_over_legacy_pending_entry_capital(modules):
    _, mc_mod = modules
    mc = _make_mc(mc_mod)
    mc._pending_orders_capital = MagicMock(return_value=_pending_diag(0.0))
    snap = {
        "capital_deployed": 250.0,
        "position_capital_deployed": 250.0,
        "filled_unreconciled_entry_capital": 0.0,
        "pending_entry_capital": 250.0,
        "ignored_already_reconciled_fill_capital": 250.0,
        "ignored_already_reconciled_order_ids": ["br-1"],
        "ignored_reconciled_match_keys": ["br-1:broker_order_id:pos-1"],
        "open_position_ids": ["pos-1"],
    }
    breakdown = mc._get_pending_capital_breakdown(
        snap,
        "alice@example.com",
        runtime_execution_mode="live",
    )
    assert breakdown["pending_submitted_entry_exposure"] == pytest.approx(0.0)
    assert breakdown["filled_unreconciled_entry_capital"] == pytest.approx(0.0)
    assert breakdown["pending_total_capital_reserved"] == pytest.approx(0.0)
    assert breakdown["ignored_already_reconciled_fill_capital"] == pytest.approx(250.0)
    assert breakdown["ignored_already_reconciled_order_ids"] == ["br-1"]
    assert breakdown["ignored_reconciled_match_keys"] == ["br-1:broker_order_id:pos-1"]
    assert breakdown["open_position_ids"] == ["pos-1"]


def test_submitted_unfilled_broker_order_counts_only_as_pending_submitted(modules):
    _, mc_mod = modules
    mc = _make_mc(mc_mod)
    mc._pending_orders_capital = MagicMock(return_value=_pending_diag(218.0))
    breakdown = mc._get_pending_capital_breakdown(
        {
            "filled_unreconciled_entry_capital": 0.0,
            "open_position_ids": [],
        },
        "alice@example.com",
        runtime_execution_mode="live",
    )
    assert breakdown["pending_submitted_entry_exposure"] == pytest.approx(218.0)
    assert breakdown["filled_unreconciled_entry_capital"] == pytest.approx(0.0)
    assert breakdown["pending_total_capital_reserved"] == pytest.approx(218.0)
    assert breakdown["counted_order_ids"] == ["ord-1"]


def test_legacy_pending_entry_capital_fallback_remains_when_new_field_missing(modules):
    _, mc_mod = modules
    mc = _make_mc(mc_mod)
    mc._pending_orders_capital = MagicMock(return_value=_pending_diag(0.0))
    breakdown = mc._get_pending_capital_breakdown(
        {"pending_entry_capital": 99.0},
        "alice@example.com",
        runtime_execution_mode="live",
    )
    assert breakdown["filled_unreconciled_entry_capital"] == pytest.approx(99.0)
    assert breakdown["pending_total_capital_reserved"] == pytest.approx(99.0)
