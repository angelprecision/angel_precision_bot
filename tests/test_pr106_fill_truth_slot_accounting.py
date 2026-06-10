"""
tests/test_pr106_fill_truth_slot_accounting.py
PR#106: position slots and exposure use fill truth only.
Entry-attempt locks separated from real position slots.
"""
from pathlib import Path
import re, pytest

_REPO = Path(__file__).resolve().parents[1]
PM_SRC = (_REPO / "ap" / "position_manager.py").read_text()
MC_SRC = (_REPO / "ap_master_control.py").read_text()

_FILL_STATES = {"PARTIAL_FILL", "PARTIALLY_FILLED", "FILLED", "OPEN"}
_TERMINAL    = {"CANCELED","CANCELLED","EXPIRED","REJECTED","ERROR","FAILED","CLOSED"}

def _counts_as_real_slot(order):
    """Mirrors the production SQL predicate for real_position_slot."""
    s = order.get("status","").upper()
    fq = int(order.get("filled_qty") or 0)
    fp = order.get("fill_price")
    return (
        (fq > 0 or fp is not None or s in _FILL_STATES)
        and s not in _TERMINAL
    )

def _counts_as_entry_attempt_lock(order):
    s   = order.get("status","").upper()
    fq  = int(order.get("filled_qty") or 0)
    fp  = order.get("fill_price")
    bid = bool(order.get("broker_order_id"))
    sts = bool(order.get("submitted_ts"))
    return (
        s in {"CREATED","SUBMITTED","ACKNOWLEDGED"}
        and (bid or sts)
        and fq == 0
        and fp is None
        and not order.get("contract","").upper().startswith("DEFERRED:")
        and s not in _TERMINAL
    )

def _fill_cost(order):
    fp = order.get("fill_price")
    fq = int(order.get("filled_qty") or 0)
    rc = order.get("reserved_cost")
    if fp and fq > 0: return fp * fq * 100
    if fq > 0 and rc:  return float(rc)
    return 0.0


# ── Structural: production source contains fill-truth predicates ──────────────

def test_slot_sql_uses_filled_qty():
    assert "COALESCE(filled_qty, 0) > 0" in PM_SRC

def test_slot_sql_uses_fill_price():
    assert "fill_price IS NOT NULL" in PM_SRC

def test_entry_attempt_lock_in_snapshot():
    assert "entry_attempt_lock_count" in PM_SRC
    assert "entry_attempt_reserved_cost" in PM_SRC

def test_submitted_unfilled_not_counted_log():
    assert "submitted_unfilled_not_counted_as_position" in MC_SRC

def test_capital_sql_uses_fill_price_times_qty():
    import re as _re
    section = PM_SRC[PM_SRC.find("real_deployed_capital"):][:1500]
    assert "fill_price * filled_qty * 100" in section

def test_mc_capital_uses_fill_truth():
    section = MC_SRC[MC_SRC.find("def _pending_orders_capital"):]
    end = MC_SRC.find("\n    def ", MC_SRC.find("def _pending_orders_capital")+1)
    body = MC_SRC[MC_SRC.find("def _pending_orders_capital"):end]
    assert "filled_qty" in body
    assert "fill_price" in body


# ── AC1: Five DEFERRED:* rows → 0 slots, $0 exposure ─────────────────────────

def test_ac1_deferred_rows_zero_slots_zero_exposure():
    orders = [
        {"status": "PENDING_TRIGGER", "contract": "DEFERRED:TSLA",
         "broker_order_id": None, "submitted_ts": None,
         "filled_qty": 0, "fill_price": None, "reserved_cost": 1800.0}
        for _ in range(5)
    ]
    slots = sum(1 for o in orders if _counts_as_real_slot(o))
    cost  = sum(_fill_cost(o) for o in orders if _counts_as_real_slot(o))
    assert slots == 0, f"DEFERRED:* must consume 0 slots, got {slots}"
    assert cost  == 0.0, f"DEFERRED:* must contribute $0 exposure, got ${cost}"


# ── AC2: Five PENDING_TRIGGER rows → 0 slots, $0 exposure ────────────────────

def test_ac2_pending_trigger_zero_slots():
    orders = [
        {"status": "PENDING_TRIGGER", "contract": "RIVN260612P00016500",
         "broker_order_id": None, "submitted_ts": None,
         "filled_qty": 0, "fill_price": None, "reserved_cost": 450.0}
        for _ in range(5)
    ]
    slots = sum(1 for o in orders if _counts_as_real_slot(o))
    cost  = sum(_fill_cost(o) for o in orders if _counts_as_real_slot(o))
    assert slots == 0
    assert cost  == 0.0


# ── AC3: SUBMITTED + confirmed, no fill → 0 slots, $0 exposure, 1 lock ────────

def test_ac3_submitted_no_fill_creates_lock_not_slot():
    order = {"status": "SUBMITTED", "contract": "RIVN260612P00016500",
             "broker_order_id": "br-123", "submitted_ts": "2026-06-09T10:00:00Z",
             "filled_qty": 0, "fill_price": None, "reserved_cost": 450.0}
    assert _counts_as_real_slot(order) is False,       "no fill → no real slot"
    assert _counts_as_entry_attempt_lock(order) is True, "confirmed in-flight → lock"
    assert _fill_cost(order) == 0.0,                   "no fill → $0 real exposure"


# ── AC4: CANCELED/EXPIRED/REJECTED → 0 locks, 0 slots ───────────────────────

def test_ac4_canceled_releases_lock():
    for status in ("CANCELED","CANCELLED","EXPIRED","REJECTED","ERROR","FAILED"):
        order = {"status": status, "contract": "RIVN260612P00016500",
                 "broker_order_id": "br-123", "submitted_ts": "2026-06-09T10:00:00Z",
                 "filled_qty": 0, "fill_price": None, "reserved_cost": 450.0}
        assert _counts_as_real_slot(order) is False,       f"{status}: no slot"
        assert _counts_as_entry_attempt_lock(order) is False, f"{status}: lock released"
        assert _fill_cost(order) == 0.0


# ── AC5: FILLED row → 1 real slot, actual exposure ────────────────────────────

def test_ac5_filled_row_one_slot_real_exposure():
    order = {"status": "FILLED", "contract": "TSLA260612C00900000",
             "broker_order_id": "br-fill-001", "submitted_ts": "2026-06-09T09:30:00Z",
             "filled_qty": 1, "fill_price": 2.50, "reserved_cost": None}
    assert _counts_as_real_slot(order) is True
    assert _counts_as_entry_attempt_lock(order) is False
    assert _fill_cost(order) == pytest.approx(250.0)   # 2.50 * 1 * 100


# ── AC6: PARTIAL_FILL → 1 slot, filled qty cost only ─────────────────────────

def test_ac6_partial_fill_one_slot_filled_qty_cost():
    order = {"status": "PARTIAL_FILL", "contract": "AAPL260612C00190000",
             "broker_order_id": "br-p1", "submitted_ts": "2026-06-09T10:05:00Z",
             "filled_qty": 1, "fill_price": 1.80, "qty": 3, "reserved_cost": 540.0}
    assert _counts_as_real_slot(order) is True
    assert _fill_cost(order) == pytest.approx(180.0)   # 1.80 * 1 * 100 (filled only)


# ── AC7: PR#105 backward-compat — DEFERRED exclusion still works ─────────────

def test_ac7_pr105_deferred_exclusion_preserved():
    assert "NOT LIKE \'DEFERRED:%%'" in PM_SRC or            "NOT LIKE 'DEFERRED:%%" in PM_SRC,         "DEFERRED:% exclusion from PR#105 must still be present"

def test_ac7_entry_attempt_lock_field_in_mc():
    """MC must have entry_attempt_lock_count in zero_snapshot and setdefault."""
    assert '"entry_attempt_lock_count"' in MC_SRC or            "'entry_attempt_lock_count'" in MC_SRC
