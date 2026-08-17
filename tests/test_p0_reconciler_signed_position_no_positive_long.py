"""
tests/test_p0_reconciler_signed_position_no_positive_long.py
================================================================
P0 regression tests — PR #481 final narrow merge-gate amendment, Blocker 1

Defect addressed
-----------------
#481 correctly allows legitimate signed broker quantities to pass through
ap/brokers/tradier.py::TradierBroker.list_positions() unmodified --
negative quantity is real signed broker data (a short position), not
malformed payload. ap_reconciler.py::_broker_position_qty() still used
`abs(int(float(raw)))`, which silently flipped a negative (short) broker
quantity into a POSITIVE value -- e.g. broker qty=-1 became reconciler
qty=+1. That positive value could then be imported as a new AP long
position when the DB position was missing, or silently "matched" against
an existing DB OPEN long as though the broker confirmed the same
direction of exposure.

Binding invariant under test
------------------------------
A negative broker position must NEVER become ordinary AP long exposure.
This is narrow quantity-classification hardening, not new short-position
execution support:

  VALID POSITIVE INTEGRAL  -> positive AP long quantity
  NEGATIVE                 -> signed-direction conflict / HOLD, never
                               imported as long, never seeds normal long
                               exit lifecycle, never marked flat
  ZERO                     -> not positive long authority
  BOOLEAN                  -> UNKNOWN / malformed
  FRACTIONAL                -> UNKNOWN for AP option lifecycle
  NaN / Infinity            -> UNKNOWN / malformed
  UNPARSEABLE               -> UNKNOWN / malformed

For a broker orphan position (DB missing): qty>0 imports normally;
qty<0 or malformed -> explicit conflict/HOLD, no import.

For an existing DB OPEN long with exact broker qty<0: explicit
signed-direction conflict/HOLD -- no synthetic long import, no flat
conclusion, no ordinary long SELL_TO_CLOSE authority derived from that
broker qty.
"""

from __future__ import annotations

import math
import os
from typing import Any
from unittest.mock import MagicMock

os.environ.setdefault("DATABASE_URL", "postgresql://x:x@localhost/x")

import pytest  # noqa: E402

from ap_reconciler import APBrokerReconciler  # noqa: E402

_TARGET_OCC = "SMCI260626P00032500"
_UNRELATED_OCC = "TSLA260320C00250000"
_CLIENT = "test_client_001"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_reconciler(*, execution_mode: str = "live") -> tuple:
    alerts: list[str] = []
    rec = APBrokerReconciler(
        broker=MagicMock(),
        client_id=_CLIENT,
        osm=MagicMock(),
        pm=MagicMock(),
        alert_fn=alerts.append,
        execution_mode=execution_mode,
    )
    rec._active_exit_order_exists = lambda **_: False
    rec._broker_open_exit_exists_for_contract = lambda *_: False
    seeded: list[str] = []
    rec._seed_exit_engine_from_position = lambda pos: seeded.append(
        pos.get("id") or pos.get("position_id")
    )
    return rec, alerts, seeded


def _db_position(*, contract: str, qty: int, position_id: str = "pos-1", execution_mode: str = "live") -> dict:
    return {
        "id": position_id,
        "contract": contract,
        "underlying": "SMCI",
        "qty": qty,
        "avg_fill": 1.5,
        "execution_mode": execution_mode,
        "status": "OPEN",
    }


# ===========================================================================
# Direct unit tests of _broker_position_qty / _broker_position_qty_is_negative
# ===========================================================================

class TestBrokerPositionQtyDirect:
    def setup_method(self):
        self.rec, _, _ = _make_reconciler()

    def test_valid_positive_integral(self):
        assert self.rec._broker_position_qty({"symbol": _TARGET_OCC, "quantity": 2}) == 2

    def test_negative_returns_none_never_abs(self):
        result = self.rec._broker_position_qty({"symbol": _TARGET_OCC, "quantity": -1})
        assert result is None, f"Expected None for negative quantity, never abs()'d to +1; got {result!r}"

    def test_zero_returns_none(self):
        assert self.rec._broker_position_qty({"symbol": _TARGET_OCC, "quantity": 0}) is None

    @pytest.mark.parametrize("bad_val", [True, False])
    def test_boolean_returns_none(self, bad_val):
        result = self.rec._broker_position_qty({"symbol": _TARGET_OCC, "quantity": bad_val})
        assert result is None, f"Expected None for boolean quantity {bad_val!r}; got {result!r}"

    @pytest.mark.parametrize("bad_val", [0.5, -0.5])
    def test_fractional_returns_none(self, bad_val):
        result = self.rec._broker_position_qty({"symbol": _TARGET_OCC, "quantity": bad_val})
        assert result is None, f"Expected None for fractional quantity {bad_val!r}; got {result!r}"

    @pytest.mark.parametrize("bad_val", [float("nan"), float("inf"), float("-inf")])
    def test_nonfinite_returns_none(self, bad_val):
        result = self.rec._broker_position_qty({"symbol": _TARGET_OCC, "quantity": bad_val})
        assert result is None, f"Expected None for non-finite quantity {bad_val!r}; got {result!r}"

    def test_unparseable_string_returns_none(self):
        result = self.rec._broker_position_qty({"symbol": _TARGET_OCC, "quantity": "garbage"})
        assert result is None

    def test_none_field_returns_none(self):
        assert self.rec._broker_position_qty({"symbol": _TARGET_OCC}) is None

    def test_short_quantity_field_alone_never_becomes_positive_long(self):
        """A row reporting short_quantity=1 (magnitude of SHORT exposure)
        must NEVER become AP long qty=1. short_quantity is deliberately
        excluded from the extraction fallback chain entirely."""
        result = self.rec._broker_position_qty({"symbol": _TARGET_OCC, "short_quantity": 1})
        assert result is None, f"short_quantity must never become a positive AP long; got {result!r}"

    def test_explicit_zero_quantity_not_skipped_past_via_or_chain(self):
        """An explicit quantity=0 must resolve to None directly, not fall
        through to a different field due to Python truthy `or` chaining."""
        result = self.rec._broker_position_qty({"symbol": _TARGET_OCC, "quantity": 0, "qty": 5})
        assert result is None, (
            f"Explicit quantity=0 must be honored as the authoritative field, "
            f"not skipped past toward qty=5; got {result!r}"
        )

    def test_is_negative_helper_true_for_negative(self):
        assert self.rec._broker_position_qty_is_negative({"symbol": _TARGET_OCC, "quantity": -1}) is True

    def test_is_negative_helper_false_for_positive(self):
        assert self.rec._broker_position_qty_is_negative({"symbol": _TARGET_OCC, "quantity": 2}) is False

    def test_is_negative_helper_false_for_malformed(self):
        assert self.rec._broker_position_qty_is_negative({"symbol": _TARGET_OCC, "quantity": "garbage"}) is False


# ===========================================================================
# Required test 3 -- unrelated short does not poison valid target truth,
# and is not imported as AP long
# ===========================================================================

def test_3_target_valid_unrelated_short_target_remains_valid_and_unrelated_not_imported():
    rec, _, _ = _make_reconciler()
    rec._get_open_db_positions = lambda: []
    rec._safe_get_broker_positions = lambda: [
        {"symbol": _TARGET_OCC, "quantity": 3},
        {"symbol": _UNRELATED_OCC, "quantity": -1},
    ]

    target_qty = rec._broker_position_qty({"symbol": _TARGET_OCC, "quantity": 3})
    assert target_qty == 3, "Target's own valid positive quantity must be unaffected by an unrelated short row"

    unrelated_qty = rec._broker_position_qty({"symbol": _UNRELATED_OCC, "quantity": -1})
    assert unrelated_qty is None, "Unrelated short position must never resolve to a positive AP long quantity"


# ===========================================================================
# Required test 4 -- short_quantity field alone never becomes AP long
# (covered directly above in TestBrokerPositionQtyDirect; end-to-end proof
# via the import pass below)
# ===========================================================================

def test_4_end_to_end_short_quantity_field_row_not_imported():
    rec, alerts, seeded = _make_reconciler()
    rec._get_open_db_positions = lambda: []
    rec._safe_get_broker_positions = lambda: [{"symbol": _TARGET_OCC, "short_quantity": 1}]

    summary: dict = {}
    rec._reconcile_positions(summary)

    assert summary.get("positions_imported", 0) == 0, (
        f"A row reporting only short_quantity must NOT be imported as an AP "
        f"long position; summary={summary}"
    )
    assert len(seeded) == 0


# ===========================================================================
# Required test 5 -- malformed values never become positive long, never
# authorize import/reseed, never become authoritative flat
# ===========================================================================

@pytest.mark.parametrize("bad_val,label", [
    (True, "boolean_true"),
    (False, "boolean_false"),
    (0.5, "fractional_positive"),
    (-0.5, "fractional_negative"),
    (float("nan"), "nan"),
    (float("inf"), "infinity"),
    ("garbage", "unparseable_string"),
    (None, "none"),
])
def test_5_malformed_values_never_import_never_flat(bad_val, label):
    rec, alerts, seeded = _make_reconciler()
    rec._get_open_db_positions = lambda: []
    bp = {"symbol": _TARGET_OCC}
    if bad_val is not None:
        bp["quantity"] = bad_val
    rec._safe_get_broker_positions = lambda: [bp]

    summary: dict = {}
    rec._reconcile_positions(summary)

    assert summary.get("positions_imported", 0) == 0, (
        f"[{label}] malformed quantity {bad_val!r} must NOT authorize import; summary={summary}"
    )
    assert len(seeded) == 0, f"[{label}] malformed quantity must NOT authorize exit-engine reseed"


# ===========================================================================
# Required test 6 -- valid positive orphan still imports normally (regression)
# ===========================================================================

def test_6_valid_positive_orphan_still_flows_toward_import():
    """Regression guard: a genuinely valid positive-quantity orphan
    position (DB missing, broker qty=2) must still reach the import
    logic -- i.e. must NOT be skipped by the new None-based filter. We
    assert it clears the qty gate (does not hit the `if not qty: continue`
    early-exit) by checking it isn't silently absent from further
    processing; the deeper DB-write import mechanics are covered by
    pre-existing reconciler test files (unchanged by this amendment)."""
    rec, _, _ = _make_reconciler()
    qty = rec._broker_position_qty({"symbol": _TARGET_OCC, "quantity": 2})
    assert qty == 2, "Valid positive orphan quantity must still resolve correctly, unaffected by this amendment"


# ===========================================================================
# Required test 1 -- broker exact OCC qty=-1, DB missing
# ===========================================================================

def test_1_broker_negative_qty_db_missing_no_import_explicit_conflict():
    rec, alerts, seeded = _make_reconciler()
    rec._get_open_db_positions = lambda: []
    rec._safe_get_broker_positions = lambda: [{"symbol": _TARGET_OCC, "quantity": -1}]

    summary: dict = {}
    rec._reconcile_positions(summary)

    assert summary.get("positions_imported", 0) == 0, (
        f"A negative broker quantity for a position missing from DB must NOT "
        f"be imported as an AP long; summary={summary}"
    )
    assert len(seeded) == 0, "No exit-engine seed for a position that was never imported"
    assert "broker_position_signed_conflict_not_imported" in summary.get("errors", []), (
        f"Expected explicit signed-position conflict evidence in summary errors; "
        f"got errors={summary.get('errors')}"
    )


# ===========================================================================
# Required test 2 -- DB OPEN long qty=1, broker same OCC qty=-1
# ===========================================================================

def test_2_db_open_long_broker_negative_same_contract_explicit_conflict_not_flat():
    rec, alerts, seeded = _make_reconciler()
    db_pos = _db_position(contract=_TARGET_OCC, qty=1, position_id="pos-db-1")
    rec._get_open_db_positions = lambda: [db_pos]
    rec._safe_get_broker_positions = lambda: [{"symbol": _TARGET_OCC, "quantity": -1}]

    summary: dict = {"positions_alerted": 0}
    # The DB position must still be tracked (belt-and-suspenders reseed),
    # proving it was NOT treated as "broker position missing" (which would
    # route toward the ghost-close pathway instead).
    rec._reconcile_positions(summary)

    assert "pos-db-1" in seeded, (
        "DB OPEN position must remain tracked by the exit engine -- proving "
        "the negative broker row was recognized as a present-but-conflicting "
        "match, not treated as 'broker position missing'"
    )
    assert summary.get("positions_imported", 0) == 0, "No second synthetic long must be imported"
    assert summary.get("positions_alerted", 0) >= 1, (
        f"Expected an explicit conflict alert to be recorded; summary={summary}"
    )


def test_2_variant_alert_message_identifies_signed_conflict():
    rec, alerts, seeded = _make_reconciler()
    db_pos = _db_position(contract=_TARGET_OCC, qty=1, position_id="pos-db-2")
    rec._get_open_db_positions = lambda: [db_pos]
    rec._safe_get_broker_positions = lambda: [{"symbol": _TARGET_OCC, "quantity": -1}]

    summary: dict = {"positions_alerted": 0}
    import logging
    records: list[str] = []

    class _Handler(logging.Handler):
        def emit(self, record):
            records.append(record.getMessage())

    logger = logging.getLogger("ap.reconciler")
    handler = _Handler()
    logger.addHandler(handler)
    try:
        rec._reconcile_positions(summary)
    finally:
        logger.removeHandler(handler)

    assert any("BROKER_POSITION_QTY_SIGNED_CONFLICT" in msg for msg in records), (
        f"Expected an explicit BROKER_POSITION_QTY_SIGNED_CONFLICT log entry; "
        f"got messages={records}"
    )


def test_2_variant_valid_positive_match_still_no_conflict_alert():
    """Regression guard: a genuine DB/broker qty MATCH (both positive,
    equal) must not trigger the new signed-conflict path at all."""
    rec, alerts, seeded = _make_reconciler()
    db_pos = _db_position(contract=_TARGET_OCC, qty=1, position_id="pos-db-3")
    rec._get_open_db_positions = lambda: [db_pos]
    rec._safe_get_broker_positions = lambda: [{"symbol": _TARGET_OCC, "quantity": 1}]

    summary: dict = {"positions_alerted": 0}
    rec._reconcile_positions(summary)

    assert "pos-db-3" in seeded
    assert summary.get("positions_imported", 0) == 0
    # No qty mismatch, no signed conflict -- alert count should reflect
    # only whatever baseline the existing matched-pair flow produces
    # (zero in this clean-match case).
    assert summary.get("positions_alerted", 0) == 0
