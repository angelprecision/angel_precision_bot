"""
P0 (PR #259): recovery fill truth guard.

Recovery must NEVER mark an order FILLED / EXIT_FILLED on a broker payload
that lacks positive executed quantity AND positive average fill price. Fake
qty=0 / price=0 fills create DB fill truth without real fill truth, corrupt
position P&L, and can bypass fill-monitor position creation.

Entry and exit guards already existed on main (this suite locks them in as
regression fences); this PR adds the DURABLE forensic meta on blocked rows
(recovery_fill_truth_block: reason, broker_order_id, raw broker status,
extracted values).

Source-level + pure-function tests (repo pattern — importing ap_recovery
wholesale triggers heavy init in some environments; extraction helpers are
imported directly).
"""

import pathlib
import re

from ap_recovery import (
    _extract_explicit_fill_qty,
    _extract_avg_fill_price,
)

_REPO = pathlib.Path(__file__).resolve().parents[1]
SRC = (_REPO / "ap_recovery.py").read_text()

def _guard_block(marker: str) -> str:
    """From the guard's if-condition through its terminating `continue`."""
    m_idx = SRC.rindex(marker)  # the f-string in the guard (last occurrence)
    start = SRC.rindex("if not filled_qty or filled_qty <= 0", 0, m_idx)
    end = SRC.index("continue", m_idx) + len("continue")
    return SRC[start:end]

ENTRY_BLOCK = _guard_block("RECOVERY_FILL_TRUTH_MISSING entry")
EXIT_BLOCK = _guard_block("RECOVERY_EXIT_FILL_TRUTH_MISSING local=")


# ── Spec 1–2, 7: no FILLED transition without positive qty AND price ────────

def test_broker_filled_qty_zero_price_positive_blocks():
    assert _extract_explicit_fill_qty({"exec_quantity": 0}) is None
    # guard condition requires BOTH — qty None ⇒ blocked before transition
    assert "if not filled_qty or filled_qty <= 0 or not avg_fill or avg_fill <= 0:" in SRC


def test_broker_filled_qty_positive_price_zero_blocks():
    assert _extract_avg_fill_price({"avg_fill_price": 0}) is None
    assert _extract_avg_fill_price({"avg_fill_price": "0.00"}) is None


def test_broker_filled_missing_both_blocks():
    assert _extract_explicit_fill_qty({}) is None
    assert _extract_avg_fill_price({}) is None
    assert _extract_explicit_fill_qty({"filled_qty": None}) is None
    assert _extract_avg_fill_price({"avg_fill_price": ""}) is None


def test_broker_filled_valid_both_extracts():
    assert _extract_explicit_fill_qty({"exec_quantity": "2"}) == 2
    assert _extract_avg_fill_price({"avg_fill_price": "1.45"}) == 1.45


def test_price_extractor_never_uses_limit_or_generic_price():
    """Generic price/avg_price can be limit prices — not execution truth."""
    assert _extract_avg_fill_price({"price": 3.10, "avg_price": 3.10}) is None
    assert _extract_avg_fill_price({"limit_price": 3.10}) is None


def test_qty_extractor_falls_through_zero_to_no_positive_key():
    """A zero in one key must not shadow a positive in a later key."""
    assert _extract_explicit_fill_qty({"filled_qty": 0, "exec_quantity": 3}) == 3


# ── Spec: blocked path leaves row for fill_monitor/reconciler ───────────────

def test_entry_block_continues_without_transition():
    assert "leaving for fill_monitor/reconciler" in ENTRY_BLOCK
    assert "osm.transition" not in ENTRY_BLOCK


def test_exit_block_continues_without_transition_or_position_write():
    assert "keeping CLOSING for reconciler/fill_monitor" in EXIT_BLOCK
    assert "osm.transition" not in EXIT_BLOCK
    assert "UPDATE positions" not in EXIT_BLOCK


# ── Spec 3–5: durable diagnostic meta, loud logging ─────────────────────────

def test_blocked_branches_write_durable_meta():
    assert ENTRY_BLOCK.count("_write_fill_truth_blocked_meta") == 1
    assert EXIT_BLOCK.count("_write_fill_truth_blocked_meta") == 1
    assert 'reason="recovery_fill_truth_missing"' in ENTRY_BLOCK
    assert 'reason="recovery_exit_fill_truth_missing"' in EXIT_BLOCK


def test_meta_preserves_broker_id_and_raw_status_and_extractions():
    helper = re.search(
        r"def _write_fill_truth_blocked_meta.*?(?=\ndef )", SRC, re.S
    ).group(0)
    for key in (
        '"broker_order_id"', '"broker_status_raw"',
        '"extracted_filled_qty"', '"extracted_avg_fill_price"',
        '"blocked_at"', '"recorded_by"',
    ):
        assert key in helper


def test_meta_write_is_jsonb_concat_scoped_and_nonfatal():
    helper = re.search(
        r"def _write_fill_truth_blocked_meta.*?(?=\ndef )", SRC, re.S
    ).group(0)
    assert "COALESCE(meta, '{}'::jsonb) || %s::jsonb" in helper
    assert "local_order_id = %s AND client_id = %s" in helper
    assert "except Exception" in helper
    assert "non-fatal" in helper
    # No status mutation, no broker calls, no position writes from diagnostics.
    assert "SET meta =" in helper
    assert "status" not in re.search(r"UPDATE orders(.*?)WHERE", helper, re.S).group(1)


def test_blocked_paths_log_critical():
    assert 'log.critical("[%s] %s", self.client_id, msg)' in ENTRY_BLOCK
    assert 'log.critical("[%s] %s", self.client_id, msg)' in EXIT_BLOCK


# ── Spec 7 fence: only guarded paths may transition to fill states ──────────

def test_no_unguarded_filled_transitions_in_recovery():
    """
    Every transition carrying fill data in ap_recovery must sit AFTER its
    truth guard. Both call sites pass filled_qty/fill_price variables that
    are provably positive at that point (the guard continue-d otherwise).
    """
    fills = [m.start() for m in re.finditer(r"filled_qty=filled_qty", SRC)]
    assert len(fills) == 2
    g1 = SRC.index("RECOVERY_FILL_TRUTH_MISSING entry")
    g2 = SRC.index("RECOVERY_EXIT_FILL_TRUTH_MISSING local=")
    assert g1 < fills[0] < g2 < fills[1]
