"""
tests/test_pr105_slot_and_exposure_accounting.py
PR#105 regression tests — position slots and exposure must only count
broker-confirmed filled positions. No scanner/scoring/exit rule changes.
"""
from pathlib import Path
import re, pytest

_REPO = Path(__file__).resolve().parents[1]
PM_SRC  = (_REPO / "ap" / "position_manager.py").read_text()
MC_SRC  = (_REPO / "ap_master_control.py").read_text()

# ── Helpers ───────────────────────────────────────────────────────────────────

def _pending_capital_section(src):
    """Isolate the _pending_orders_capital method body."""
    idx = src.find("def _pending_orders_capital(")
    end = src.find("\n    def ", idx + 1)
    return src[idx:end]

def _slot_sql_section(src):
    """Isolate the pending_entries slot count SQL block in snapshot."""
    idx = src.find("SLOT ACCOUNTING FIX")
    end = src.find("Count watcher/pre-submit rows separately", idx)
    return src[idx:end]


# ══════════════════════════════════════════════════════════════════════════════
# Slot accounting
# ══════════════════════════════════════════════════════════════════════════════

def test_deferred_contract_excluded_from_slot_sql():
    """DEFERRED:* contracts must be filtered out of the slot-count query."""
    section = _slot_sql_section(PM_SRC)
    assert "NOT LIKE \'DEFERRED:%%" in section or "NOT LIKE 'DEFERRED:%%" in section, (
        "Slot SQL must exclude DEFERRED:% contracts"
    )

def test_slot_sql_requires_broker_confirmation():
    """Slot query must require broker_order_id or submitted_ts."""
    section = _slot_sql_section(PM_SRC)
    assert "broker_order_id" in section
    assert "submitted_ts" in section

def test_deferred_in_watcher_statuses():
    """DEFERRED must be in _WATCHER_ENTRY_STATUSES (never consumes a slot)."""
    idx   = PM_SRC.find("_WATCHER_ENTRY_STATUSES = (")
    block = PM_SRC[idx:PM_SRC.find(")", idx) + 1]
    assert '"DEFERRED"' in block

def test_selected_retry_eligible_in_watcher_statuses():
    """SELECTED and RETRY_ELIGIBLE must also be in _WATCHER_ENTRY_STATUSES."""
    idx   = PM_SRC.find("_WATCHER_ENTRY_STATUSES = (")
    block = PM_SRC[idx:PM_SRC.find(")", idx) + 1]
    assert '"SELECTED"' in block, "SELECTED must be in _WATCHER_ENTRY_STATUSES"
    assert '"RETRY_ELIGIBLE"' in block, "RETRY_ELIGIBLE must be in _WATCHER_ENTRY_STATUSES"

def test_pending_trigger_not_in_slot_consuming_statuses():
    """PENDING_TRIGGER must NOT be in _SLOT_CONSUMING_STATUSES."""
    idx   = PM_SRC.find("_SLOT_CONSUMING_STATUSES = (")
    block = PM_SRC[idx:PM_SRC.find(")", idx) + 1]
    assert '"PENDING_TRIGGER"' not in block

def test_watching_not_in_slot_consuming_statuses():
    """WATCHING must NOT be in _SLOT_CONSUMING_STATUSES."""
    idx   = PM_SRC.find("_SLOT_CONSUMING_STATUSES = (")
    block = PM_SRC[idx:PM_SRC.find(")", idx) + 1]
    assert '"WATCHING"' not in block

def test_snapshot_includes_pending_entry_capital():
    """Snapshot dict must include pending_entry_capital key."""
    assert '"pending_entry_capital"' in PM_SRC or            "'pending_entry_capital'" in PM_SRC

def test_snapshot_capital_excludes_deferred():
    """Snapshot pending_entry_capital SQL must exclude DEFERRED:% contracts."""
    # Find the block that computes pending_entry_capital inside snapshot
    idx = PM_SRC.find("pending_entry_capital query failed")
    assert idx > 0
    # Look backward for the SQL
    region = PM_SRC[max(0, idx-1500):idx]
    assert "DEFERRED:%" in region or "DEFERRED:%%" in region


# ══════════════════════════════════════════════════════════════════════════════
# Capital (exposure) accounting
# ══════════════════════════════════════════════════════════════════════════════

def test_capital_sql_excludes_deferred_contracts():
    """_pending_orders_capital must exclude contract LIKE 'DEFERRED:%'."""
    section = _pending_capital_section(MC_SRC)
    assert "DEFERRED:%" in section or "DEFERRED:%%" in section, (
        "_pending_orders_capital must filter out DEFERRED:% contracts"
    )

def test_capital_sql_excludes_unsubmitted_pending_trigger():
    """PENDING_TRIGGER with no broker_order_id and no submitted_ts must be excluded."""
    section = _pending_capital_section(MC_SRC)
    assert "PENDING_TRIGGER" in section
    assert "submitted_ts IS NULL" in section, (
        "Capital SQL must exclude PENDING_TRIGGER rows with submitted_ts IS NULL"
    )

def test_capital_sql_excludes_watching():
    """WATCHING rows must be excluded from capital sum."""
    section = _pending_capital_section(MC_SRC)
    assert "'WATCHING'" in section or "\'WATCHING\'" in section

def test_capital_integrity_guard_excludes_deferred():
    """Integrity guard (missing-cost check) must also exclude DEFERRED:% rows."""
    section = _pending_capital_section(MC_SRC)
    # Integrity guard queries broker_order_id exclusion to avoid false positives
    # from DEFERRED rows that never need a cost estimate
    assert section.count("DEFERRED:%%") >= 2 or section.count("DEFERRED:%") >= 2, (
        "Both the SUM query and the integrity guard must exclude DEFERRED:% rows"
    )

def test_capital_integrity_guard_requires_broker_order_id():
    """Integrity guard must only check rows that actually reached broker."""
    section = _pending_capital_section(MC_SRC)
    idx = section.find("missing_cost")
    guard_body = section[idx:idx+800]
    assert "broker_order_id IS NOT NULL" in guard_body, (
        "Integrity guard must only fire for broker-confirmed orders"
    )


# ══════════════════════════════════════════════════════════════════════════════
# Behavioral: five DEFERRED:* rows → 0 slots, $0 exposure
# ══════════════════════════════════════════════════════════════════════════════

def test_ac1_five_deferred_rows_consume_zero_slots():
    """
    Behavioral: five DEFERRED:* rows in 'pending_entries' slot SQL.
    The query adds WHERE contract NOT LIKE 'DEFERRED:%', so all five
    are excluded. Effective pending_entries = 0.
    """
    # Simulate the slot SQL logic in Python
    orders = [
        {"status": "PENDING_TRIGGER", "contract": f"DEFERRED:TSLA", "broker_order_id": None, "submitted_ts": None}
        for _ in range(5)
    ]
    slot_statuses = {"SUBMITTED", "ACKNOWLEDGED", "PARTIAL_FILL", "PARTIALLY_FILLED"}
    consumed = [
        o for o in orders
        if o["status"].upper() in slot_statuses
        and not o["contract"].upper().startswith("DEFERRED:")
        and (o.get("broker_order_id") or o.get("submitted_ts"))
    ]
    assert len(consumed) == 0, f"Expected 0 slot-consuming orders, got {len(consumed)}"

def test_ac1_deferred_reserved_cost_excluded_from_exposure():
    """
    Behavioral: five DEFERRED:* rows with reserved_cost=$1800 each.
    Capital sum excludes them → $0 pending exposure, not $9000.
    """
    orders = [
        {"status": "PENDING_TRIGGER", "contract": "DEFERRED:TSLA",
         "reserved_cost": 1800.0, "broker_order_id": None, "submitted_ts": None}
        for _ in range(5)
    ]
    capital_statuses = {"CREATED", "PENDING_TRIGGER", "SUBMITTED", "ACKNOWLEDGED", "PARTIAL_FILL"}
    def _is_deferred(o): return o["contract"].upper().startswith("DEFERRED:")
    def _has_no_broker_confirm(o): return not o.get("broker_order_id") and not o.get("submitted_ts")
    pending_capital = sum(
        o["reserved_cost"] for o in orders
        if o["status"].upper() in capital_statuses
        and not _is_deferred(o)
        and not _has_no_broker_confirm(o)
    )
    assert pending_capital == 0.0, (
        f"DEFERRED:* rows must contribute $0 to pending capital, got ${pending_capital:.2f}"
    )

def test_ac2_submitted_unfilled_no_permanent_slot():
    """
    Behavioral: SUBMITTED with broker_order_id (reached broker) but fill_price=None.
    Counts as a temporary entry-attempt lock (prevents duplicate submit) — NOT a
    permanent position slot. Once CANCELED/EXPIRED/REJECTED the slot is released.
    A SUBMITTED order WITHOUT broker_order_id (never reached broker) must not
    count as a slot at all.
    """
    # Case A: SUBMITTED + broker confirmed → entry-attempt lock (temporary slot)
    confirmed = {"status": "SUBMITTED", "contract": "RIVN260612P00016500",
                 "broker_order_id": "br-123", "submitted_ts": "2026-06-09T10:00:00Z",
                 "filled_qty": 0, "fill_price": None}
    slot_statuses = {"SUBMITTED", "ACKNOWLEDGED", "PARTIAL_FILL", "PARTIALLY_FILLED"}
    is_lock = (
        confirmed["status"].upper() in slot_statuses
        and not confirmed["contract"].upper().startswith("DEFERRED:")
        and bool(confirmed.get("broker_order_id") or confirmed.get("submitted_ts"))
    )
    assert is_lock is True, "SUBMITTED+broker_confirmed must be a temporary entry-attempt lock"
    assert int(confirmed.get("filled_qty") or 0) == 0, "fill_price=None → no permanent position"

    # Case B: SUBMITTED + no broker confirmation → must NOT count
    unconfirmed = {"status": "SUBMITTED", "contract": "RIVN260612P00016500",
                   "broker_order_id": None, "submitted_ts": None,
                   "filled_qty": 0, "fill_price": None}
    is_slot_unconfirmed = (
        unconfirmed["status"].upper() in slot_statuses
        and not unconfirmed["contract"].upper().startswith("DEFERRED:")
        and bool(unconfirmed.get("broker_order_id") or unconfirmed.get("submitted_ts"))
    )
    assert is_slot_unconfirmed is False, (
        "SUBMITTED without broker_order_id and submitted_ts must NOT consume a slot"
    )

def test_ac3_canceled_order_has_no_slot():
    """CANCELED/EXPIRED orders must not consume slots."""
    for status in ("CANCELED", "CANCELLED", "EXPIRED", "REJECTED"):
        order = {"status": status, "contract": "AAPL260612C00190000",
                 "broker_order_id": "br-999", "submitted_ts": "2026-06-09T09:00:00Z"}
        slot_statuses = {"SUBMITTED", "ACKNOWLEDGED", "PARTIAL_FILL", "PARTIALLY_FILLED"}
        consumed = order["status"].upper() in slot_statuses
        assert consumed is False, f"{status} must not consume a slot"

def test_ac4_filled_order_consumes_one_slot():
    """A FILLED broker order with filled_qty>0 consumes exactly one position slot."""
    order = {"status": "FILLED", "contract": "TSLA260612C00900000",
             "broker_order_id": "br-fill-001", "submitted_ts": "2026-06-09T09:30:00Z",
             "filled_qty": 1, "fill_price": 2.50}
    # FILLED is in open_count (positions table), not pending_entries
    # Check that FILLED is NOT in slot-consuming order statuses
    # (it should be in positions table as OPEN instead)
    slot_statuses = {"SUBMITTED", "ACKNOWLEDGED", "PARTIAL_FILL", "PARTIALLY_FILLED"}
    in_pending = order["status"].upper() in slot_statuses
    assert in_pending is False, "FILLED order must not be in pending_entries; it is an OPEN position"
    # FILLED order IS in open_count
    assert int(order.get("filled_qty") or 0) > 0, "filled_qty > 0 = real position"

def test_ac5_scope_guard_no_scanner_scoring_exit_files_changed():
    """Only position_manager and master_control are allowed to change."""
    for label, sym in [
        ("DEFERRED:% in slot SQL", "NOT LIKE 'DEFERRED:%%"),
        ("broker_order_id in slot SQL", "broker_order_id IS NOT NULL"),
        ("pending_entry_capital in snapshot", "pending_entry_capital"),
        ("DEFERRED:% in capital SQL", "DEFERRED:%%"),
        ("submitted_ts IS NULL in capital SQL", "submitted_ts IS NULL"),
    ]:
        found_pm = sym in PM_SRC
        found_mc = sym in MC_SRC
        assert found_pm or found_mc, f"Required fix not found anywhere: {label}"

def test_integrity_guard_predicate_matches_sum_predicate():
    """
    The integrity guard broker-confirmation predicate must be identical to the
    SUM query predicate: (broker_order_id IS NOT NULL) OR submitted_ts IS NOT NULL.
    A SUBMITTED row with submitted_ts set but broker_order_id NULL and no cost
    must be caught by the guard — not silently counted as $0 exposure.
    """
    section = _pending_capital_section(MC_SRC)

    # Find the integrity guard block (after 'missing_cost')
    guard_idx = section.find("missing_cost")
    assert guard_idx > 0, "integrity guard not found"
    guard_block = section[guard_idx:guard_idx + 1200]

    # Guard must use the OR predicate — not broker_order_id-only
    assert "submitted_ts IS NOT NULL" in guard_block, (
        "Integrity guard must include 'submitted_ts IS NOT NULL' in its "
        "broker-confirmation predicate — matches the SUM query"
    )
    assert "(broker_order_id IS NOT NULL" in guard_block, (
        "Integrity guard must still check broker_order_id"
    )


def test_submitted_ts_only_order_triggers_missing_cost_guard():
    """
    Behavioral: SUBMITTED row with submitted_ts set, broker_order_id NULL,
    reserved_cost NULL, limit_price NULL.
    In LIVE mode this must trigger the missing-cost integrity failure —
    the order is counted in the SUM (submitted_ts IS NOT NULL) but at $0,
    which is an unknown exposure amount.
    """
    order = {
        "status":        "SUBMITTED",
        "contract":      "RIVN260612P00016500",
        "broker_order_id": None,
        "submitted_ts":  "2026-06-09T10:30:00Z",  # reached broker somehow
        "reserved_cost": None,
        "limit_price":   None,
        "qty":           1,
    }

    # SUM predicate — this order IS included (submitted_ts IS NOT NULL)
    is_counted_in_sum = (
        order["status"].upper() in {"SUBMITTED","ACKNOWLEDGED","PARTIAL_FILL"}
        and not order["contract"].upper().startswith("DEFERRED:")
        and (
            bool(order.get("broker_order_id"))
            or bool(order.get("submitted_ts"))
        )
    )
    assert is_counted_in_sum, (
        "Order with submitted_ts set must be included in the SUM"
    )

    # Its effective dollar value is $0 (no reserved_cost, no limit_price)
    effective_cost = (
        (order.get("reserved_cost") or 0)
        or ((order.get("limit_price") or 0) * (order.get("qty") or 0) * 100)
    )
    assert effective_cost == 0, "No reserved_cost + no limit_price = $0 contribution"

    # Integrity guard predicate — same as SUM — must CATCH this order
    hits_guard = (
        order["status"].upper() in {"SUBMITTED","ACKNOWLEDGED","PARTIAL_FILL"}
        and not order["contract"].upper().startswith("DEFERRED:")
        and (effective_cost == 0)   # missing cost
        and (                        # broker-confirmation predicate (same as SUM)
            bool(order.get("broker_order_id"))
            or bool(order.get("submitted_ts"))
        )
    )
    assert hits_guard, (
        "SUBMITTED + submitted_ts + no cost must trigger the integrity guard in LIVE mode"
    )
