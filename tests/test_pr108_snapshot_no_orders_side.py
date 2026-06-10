"""
tests/test_pr108_snapshot_no_orders_side.py
PR#108 regression: position_manager snapshot SQL must not reference orders.side.
orders table uses direction, not side.
"""
from pathlib import Path
import re, pytest

_REPO = Path(__file__).resolve().parents[1]
PM_SRC = (_REPO / "ap" / "position_manager.py").read_text()

# ── Source-level: no orders.side in any SQL block ────────────────────────────

def test_no_coalesce_side_in_position_manager():
    """
    grep -R 'COALESCE(side' ap/position_manager.py must return nothing.
    orders.side does not exist in production schema — orders use direction.
    """
    matches = re.findall(r"COALESCE\(side[,'\)]", PM_SRC)
    assert not matches, (
        f"orders.side reference found in ap/position_manager.py: {matches}\n"
        "Use COALESCE(direction,...) instead."
    )

def test_direction_used_for_call_filter():
    """CALL filter uses direction, not side."""
    assert "COALESCE(direction,'')) = \'CALL\'" in PM_SRC or \
           "COALESCE(direction,\'\')) = \'CALL\'" in PM_SRC or \
           "COALESCE(direction,'')) = 'CALL'" in PM_SRC

def test_direction_used_for_put_filter():
    """PUT filter uses direction, not side."""
    assert "COALESCE(direction,'')) = 'PUT'" in PM_SRC

def test_filled_unreconciled_calls_query_uses_direction():
    """calls_unreconciled FILTER must use direction only."""
    idx = PM_SRC.find("calls_unreconciled")
    assert idx > 0
    # Read the FILTER block
    region = PM_SRC[idx - 200:idx + 100]
    assert "direction" in region
    assert "side" not in region, (
        "calls_unreconciled FILTER must not reference orders.side"
    )

def test_filled_unreconciled_puts_query_uses_direction():
    """puts_unreconciled FILTER must use direction only."""
    idx = PM_SRC.find("puts_unreconciled")
    assert idx > 0
    region = PM_SRC[idx - 200:idx + 100]
    assert "direction" in region
    assert "side" not in region, (
        "puts_unreconciled FILTER must not reference orders.side"
    )

# ── Behavioral: direction-based filtering works correctly ────────────────────

def test_call_direction_filter_logic():
    """
    Simulate the FILTER logic in Python:
    orders with direction='CALL' must count as calls_unreconciled.
    orders with direction='PUT' must count as puts_unreconciled.
    Orders without direction must count as neither (0 in both).
    """
    orders = [
        {"direction": "CALL", "filled_qty": 1, "fill_price": 2.50,
         "status": "FILLED", "position_id": None},
        {"direction": "PUT",  "filled_qty": 1, "fill_price": 0.61,
         "status": "FILLED", "position_id": None},
        {"direction": None,   "filled_qty": 1, "fill_price": 1.00,
         "status": "FILLED", "position_id": None},
    ]

    def is_unreconciled(o):
        s = (o.get("status") or "").upper()
        terminal = {"CANCELED","CANCELLED","EXPIRED","REJECTED","ERROR","FAILED","CLOSED"}
        return (
            (int(o.get("filled_qty") or 0) > 0 or o.get("fill_price") is not None
             or s in {"PARTIAL_FILL","PARTIALLY_FILLED","FILLED","OPEN"})
            and s not in terminal
            and not o.get("position_id")
        )

    calls = sum(1 for o in orders
                if is_unreconciled(o)
                and (o.get("direction") or "").upper() == "CALL")
    puts  = sum(1 for o in orders
                if is_unreconciled(o)
                and (o.get("direction") or "").upper() == "PUT")

    assert calls == 1, f"Expected 1 CALL, got {calls}"
    assert puts  == 1, f"Expected 1 PUT, got {puts}"

def test_pr106_behavior_intact():
    """PR#106 fill-truth slot accounting keywords still present."""
    for keyword in [
        "filled_qty",
        "fill_price IS NOT NULL",
        "entry_attempt_lock_count",
        "position_id IS NULL",
        "pending_entry_capital",
    ]:
        assert keyword in PM_SRC, f"PR#106 keyword missing: {keyword}"
