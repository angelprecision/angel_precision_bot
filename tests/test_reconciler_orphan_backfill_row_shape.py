"""
tests/test_reconciler_orphan_backfill_row_shape.py

Targets the live KeyError(0) in backfill_missing_position_links seen on the
running pod:

  filled_order_missing_position_p0
  order=b0f568ab-78c8-406a-a5ec-3a6e2aa38b4c contract=WFC260626P00084000
  create+link failed: KeyError: 0

Root cause: ap.db.conn() uses RealDictCursor (rows are dict-like), but three
closures in ap_reconciler.py used row[0] to extract the inserted/returned id.
RealDictRow has no key 0 → KeyError on every successful insert.

These tests prove the helper handles every shape and that the surrounding
safety properties from the PR spec hold (no broker calls, no
price/PnL/proof_trades mutation, MANUAL_REVIEW_REQUIRED on failure).
"""
from __future__ import annotations

import sys
import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Load ap_reconciler with heavy deps stubbed so we can exercise the helper.
# ---------------------------------------------------------------------------

def _load_reconciler():
    stubs = {
        "ap.db": MagicMock(),
        "ap.brokers": MagicMock(),
        "ap.brokers.tradier": MagicMock(),
        "ap.observability": MagicMock(),
        "ap.health": MagicMock(),
        "ap.order_state_machine": MagicMock(),
        "ap_proof_logger": MagicMock(),
        "yfinance": MagicMock(),
        "requests": MagicMock(),
        "psycopg2": MagicMock(),
        "psycopg2.extras": MagicMock(),
        "psycopg2.pool": MagicMock(),
    }
    name = "ap_reconciler_shim"
    with patch.dict(sys.modules, stubs):
        spec = importlib.util.spec_from_file_location(name, _REPO / "ap_reconciler.py")
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        try:
            spec.loader.exec_module(mod)
        finally:
            sys.modules.pop(name, None)
    return mod


# ---------------------------------------------------------------------------
# 1. _row_first_value handles every DB row shape safely
# ---------------------------------------------------------------------------

class TestRowFirstValueShapes:
    """The fix: a single helper supports dict, tuple, empty rows."""

    def test_dict_shape_returns_named_value(self):
        """RealDictRow / dict — the production path. row[0] used to KeyError."""
        mod = _load_reconciler()
        row = {"id": "pos-123"}
        assert mod._row_first_value(row, "id") == "pos-123"

    def test_realdict_like_shape_returns_named_value(self):
        """Simulate a RealDictRow: dict-subclass that has no key 0."""
        mod = _load_reconciler()
        class _RealDictRow(dict):
            pass
        row = _RealDictRow(id="pos-abc")
        assert mod._row_first_value(row, "id") == "pos-abc"
        # the bare row[0] this fix replaced would have raised KeyError(0)
        with pytest.raises(KeyError):
            _ = row[0]

    def test_tuple_shape_returns_first_element(self):
        """Legacy tuple cursor: the helper falls through to index 0."""
        mod = _load_reconciler()
        row = ("pos-xyz",)
        assert mod._row_first_value(row, "id") == "pos-xyz"

    def test_list_shape_returns_first_element(self):
        mod = _load_reconciler()
        assert mod._row_first_value(["pos-list"], "id") == "pos-list"

    def test_none_returns_none_safely(self):
        """fetchone() returns None when nothing matched — must not crash."""
        mod = _load_reconciler()
        assert mod._row_first_value(None, "id") is None

    def test_dict_missing_key_falls_through_to_index(self):
        """A dict that happens to lack the named key should fall through; for
        a plain dict with no integer keys this safely returns None (no crash)."""
        mod = _load_reconciler()
        # plain dict with no 'id' key and no integer 0 key
        assert mod._row_first_value({"name": "x"}, "id") is None

    def test_empty_tuple_returns_none(self):
        """An empty row is treated as no value, not a crash."""
        mod = _load_reconciler()
        assert mod._row_first_value((), "id") is None


# ---------------------------------------------------------------------------
# 2. Source guards — the three crash sites are fixed; safety properties hold
# ---------------------------------------------------------------------------

class TestSourceFixes:
    _SRC = (_REPO / "ap_reconciler.py").read_text()

    def test_helper_defined(self):
        assert "def _row_first_value(" in self._SRC

    def test_no_row_index_zero_in_backfill(self):
        """The three row[0] crash sites must be gone (replaced by the helper).
        A `row[0]` remaining in the helper docstring/comment is fine; only
        executable code matters."""
        helper_start = self._SRC.find("def _row_first_value(")
        helper_end = self._SRC.find("\ndef _market_hours_interval(", helper_start)
        before = self._SRC[:helper_start]
        after = self._SRC[helper_end:]
        # zero crash sites outside the helper
        assert "row[0]" not in before
        # the only remaining match below the helper must be inside a comment
        for line in after.splitlines():
            if "row[0]" in line:
                stripped = line.lstrip()
                assert stripped.startswith("#"), f"non-comment row[0]: {line!r}"

    def test_manual_review_required_on_failure(self):
        """Per spec: on failure log MANUAL_REVIEW_REQUIRED with the identifying
        fields (order, client, contract, broker_order_id, execution_mode)."""
        assert "MANUAL_REVIEW_REQUIRED" in self._SRC
        # The marker must carry the identifying fields
        idx = self._SRC.find("MANUAL_REVIEW_REQUIRED reason=orphan_backfill_failed")
        assert idx != -1
        block = self._SRC[idx: idx + 500]
        for field in ("order=", "client=", "contract=", "broker_order_id=", "execution_mode=live"):
            assert field in block, f"MANUAL_REVIEW_REQUIRED missing {field}"


# ---------------------------------------------------------------------------
# 3. Safety properties per PR spec — backfill never does dangerous things
# ---------------------------------------------------------------------------

class TestSafetyProperties:
    """The PR spec demands:
      - no broker submit/cancel calls in the backfill path
      - no price/P&L/proof_trades mutation
      - existing positions not duplicated (ON CONFLICT (id) DO NOTHING)
      - client_id and execution_mode preserved
    """

    _SRC = (_REPO / "ap_reconciler.py").read_text()

    def _backfill_body(self):
        i = self._SRC.find("def _backfill_missing_position_links(")
        # bound to next top-level def
        end = self._SRC.find("\n    def ", i + 10)
        return self._SRC[i:end]

    def test_no_broker_submit_calls(self):
        body = self._backfill_body()
        assert "submit_order" not in body
        assert "place_order" not in body
        assert "submit_existing_entry" not in body

    def test_no_broker_cancel_calls(self):
        body = self._backfill_body()
        assert "cancel_order" not in body
        assert "cancel_pending_entry" not in body

    def test_no_proof_trades_mutation(self):
        body = self._backfill_body()
        # backfill must NOT write to proof_trades, only positions/orders linkage
        assert "UPDATE proof_trades" not in body
        assert "INSERT INTO proof_trades" not in body

    def test_no_pnl_or_price_mutation(self):
        body = self._backfill_body()
        # the backfill must not invent prices/pnl
        forbidden = (
            "realized_pnl_pct", "option_pnl_pct", "entry_option_price",
            "exit_option_price", "exit_fill_price",
        )
        for f in forbidden:
            assert f"UPDATE positions SET {f}" not in body, f"forbidden mutation: {f}"

    def test_idempotent_insert_no_duplicate_positions(self):
        body = self._backfill_body()
        # ON CONFLICT (id) DO NOTHING ensures a re-run cannot duplicate
        assert "ON CONFLICT (id) DO NOTHING" in body

    def test_client_id_preserved_in_insert(self):
        body = self._backfill_body()
        # the position INSERT must carry self.client_id
        assert "self.client_id" in body

    def test_execution_mode_preserved_live(self):
        """The path filters orphans to live entries and preserves that mode —
        no broad 'unknown -> live' normalization is done here."""
        # The orphan query in this function targets execution_mode='live'.
        assert "current_live" in self._SRC
