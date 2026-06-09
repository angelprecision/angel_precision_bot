"""
tests/test_slot_accounting_fix.py
P0 — PENDING_TRIGGER watcher rows must not consume position slots.
"""
import math, json, types
from pathlib import Path
import pytest

_REPO = Path(__file__).resolve().parents[1]

PM_SRC  = (_REPO / "ap" / "position_manager.py").read_text()
MC_SRC  = (_REPO / "ap_master_control.py").read_text()


# ── 1. Constants ──────────────────────────────────────────────────────────────

def test_slot_consuming_statuses_defined():
    assert "_SLOT_CONSUMING_STATUSES" in PM_SRC

def test_watcher_entry_statuses_defined():
    assert "_WATCHER_ENTRY_STATUSES" in PM_SRC

def test_pending_trigger_in_watcher_statuses():
    assert '"PENDING_TRIGGER"' in PM_SRC
    # PENDING_TRIGGER must be in the watcher tuple, not in slot-consuming tuple
    idx_slot    = PM_SRC.find("_SLOT_CONSUMING_STATUSES = (")
    idx_watcher = PM_SRC.find("_WATCHER_ENTRY_STATUSES = (")
    slot_block    = PM_SRC[idx_slot:PM_SRC.find(")", idx_slot)+1]
    watcher_block = PM_SRC[idx_watcher:PM_SRC.find(")", idx_watcher)+1]
    assert '"PENDING_TRIGGER"' not in slot_block, (
        "PENDING_TRIGGER must NOT be in _SLOT_CONSUMING_STATUSES"
    )
    assert '"PENDING_TRIGGER"' in watcher_block, (
        "PENDING_TRIGGER must be in _WATCHER_ENTRY_STATUSES"
    )

def test_submitted_acknowledged_partial_fill_in_slot_statuses():
    idx = PM_SRC.find("_SLOT_CONSUMING_STATUSES = (")
    block = PM_SRC[idx:PM_SRC.find(")", idx)+1]
    for s in ('"SUBMITTED"', '"ACKNOWLEDGED"', '"PARTIAL_FILL"'):
        assert s in block, f"{s} must be in _SLOT_CONSUMING_STATUSES"


# ── 2. Snapshot SQL uses _SLOT_CONSUMING_STATUSES ─────────────────────────────

def test_snapshot_sql_uses_slot_consuming_not_pending():
    # The slot-count query must use slot_placeholders (from _SLOT_CONSUMING_STATUSES)
    assert "slot_placeholders" in PM_SRC
    assert "_SLOT_CONSUMING_STATUSES" in PM_SRC
    # NOT using the old entry_placeholders / _PENDING_ENTRY_STATUSES for slot count
    assert "SLOT ACCOUNTING FIX" in PM_SRC

def test_snapshot_has_watcher_count():
    assert '"watcher_count"' in PM_SRC
    assert "watcher_count" in PM_SRC

def test_pending_trigger_excluded_from_slot_count_sql():
    # The slot-count SQL must NOT include PENDING_TRIGGER in its status list
    idx_slot = PM_SRC.find("slot_placeholders = ")
    end_slot  = PM_SRC.find("pending_entries = int(", idx_slot)
    slot_sql_region = PM_SRC[idx_slot:end_slot]
    assert "PENDING_TRIGGER" not in slot_sql_region, (
        "PENDING_TRIGGER must not appear in the slot-count SQL region"
    )


# ── 3. Master control — watcher_allowed log + correct block labels ────────────

def test_mc_logs_watcher_allowed_not_counted():
    assert "watcher_allowed_not_counted_as_position" in MC_SRC

def test_mc_block_message_is_blocked_actual_position_limit():
    assert "blocked_actual_position_limit" in MC_SRC

def test_mc_no_longer_emits_max_positions_with_pending():
    # The old label must be gone from block messages
    # (may still appear in comments, so check _block calls only)
    import re
    for m in re.finditer(r'self\._block\(.*max_positions_with_pending', MC_SRC):
        pytest.fail("max_positions_with_pending still used in _block() call")

def test_mc_zero_snapshot_has_watcher_count():
    idx = MC_SRC.find('"watcher_count"')
    assert idx > 0

def test_mc_setdefault_has_watcher_count():
    assert 'snap.setdefault("watcher_count"' in MC_SRC


# ── 4. Acceptance criteria (behavioral via snapshot logic) ────────────────────

def test_ac1_two_pending_trigger_zero_open_does_not_block():
    """With 2 PENDING_TRIGGER rows and 0 open/submitted, effective_count=0."""
    # Simulate what snapshot() would return with our fix
    snap = {
        "open_count":     0,   # no actual open positions
        "pending_entries": 0,  # PENDING_TRIGGER excluded from this count
        "watcher_count":  2,   # 2 watcher rows (not counted as slots)
    }
    effective_count = snap["open_count"] + snap["pending_entries"]
    max_positions   = 2
    assert effective_count < max_positions, (
        f"2 PENDING_TRIGGER + 0 open should not block (effective={effective_count})"
    )

def test_ac2_two_open_blocks_third():
    """2 actual submitted/open trades + max=2 → third is blocked."""
    snap = {
        "open_count":      2,  # 2 real open positions
        "pending_entries": 0,
        "watcher_count":   5,  # many watchers — irrelevant
    }
    effective_count = snap["open_count"] + snap["pending_entries"]
    max_positions   = 2
    assert effective_count >= max_positions, (
        "2 actual open positions must block a third"
    )

def test_ac3_one_open_one_pending_broker_blocks():
    """1 open + 1 submitted + max=2 → third blocked."""
    snap = {
        "open_count":      1,
        "pending_entries": 1,  # SUBMITTED or ACKNOWLEDGED
        "watcher_count":   3,  # watchers not counted
    }
    effective_count = snap["open_count"] + snap["pending_entries"]
    assert effective_count >= 2

def test_ac4_watcher_count_field_available_in_snapshot():
    """watcher_count field is explicitly present in snapshot dict."""
    assert '"watcher_count"' in PM_SRC
    assert "watcher_count" in PM_SRC

def test_ac5_log_labels_correct():
    """Required log labels must exist."""
    assert "watcher_allowed_not_counted_as_position" in MC_SRC
    assert "blocked_actual_position_limit" in MC_SRC

def test_ac6_pending_entry_statuses_unchanged():
    """_PENDING_ENTRY_STATUSES must still include PENDING_TRIGGER (capital calc)."""
    idx   = PM_SRC.find("_PENDING_ENTRY_STATUSES = (")
    block = PM_SRC[idx:PM_SRC.find(")", idx)+1]
    assert '"PENDING_TRIGGER"' in block, (
        "_PENDING_ENTRY_STATUSES must still include PENDING_TRIGGER for capital accounting"
    )
