"""Item 4 — verify REEVAL signal_id normalization for the multi-account ledger.

The ledger view joins orders.signal_id -> ap_signals.signal_id by normalizing
the REEVAL:<uuid>:<hex> wrapper with split_part(o.signal_id, ':', 2). That SQL
rule must produce the SAME canonical UUID as the codebase's canonical_signal_id()
helper, otherwise REEVAL orders would fail to join (acceptance criterion 1).

This test proves the two normalizations agree, and that a Python re-implementation
of the view's exact CASE/split_part expression matches as well.

Run: DATABASE_URL=postgresql://test:test@localhost/test python -m pytest tests/test_signal_ledger_reeval.py -v
"""
import os


def _sql_split_part(s, sep, idx):
    """Faithful re-implementation of Postgres split_part (1-indexed)."""
    parts = s.split(sep)
    return parts[idx - 1] if 1 <= idx <= len(parts) else ""


def _view_canonical(order_signal_id):
    """Mirror the view's CASE expression exactly."""
    if order_signal_id.startswith("REEVAL:"):
        return _sql_split_part(order_signal_id, ":", 2)
    return order_signal_id


UUID = "8d9338d0-5dde-4b7b-81ea-208039999b72"


class TestReevalNormalization:
    def test_view_expression_strips_reeval(self):
        assert _view_canonical(f"REEVAL:{UUID}:f4dc44") == UUID

    def test_view_expression_passes_bare_uuid(self):
        assert _view_canonical(UUID) == UUID

    def test_view_matches_code_helper(self):
        from ap_signal_store import canonical_signal_id
        for sid in (UUID, f"REEVAL:{UUID}:f4dc44", f"REEVAL:{UUID}:abc123"):
            assert _view_canonical(sid) == canonical_signal_id(sid), sid

    def test_reeval_and_bare_resolve_equal(self):
        # The whole point: a REEVAL order and the bare signal share one
        # canonical_signal_id so they group onto the same ledger signal.
        assert _view_canonical(f"REEVAL:{UUID}:f4dc44") == _view_canonical(UUID)

    def test_split_part_position_2_is_uuid(self):
        # Guards the exact split_part index the spec mandates.
        assert _sql_split_part(f"REEVAL:{UUID}:f4dc44", ":", 1) == "REEVAL"
        assert _sql_split_part(f"REEVAL:{UUID}:f4dc44", ":", 2) == UUID


class TestViewFileShape:
    def test_view_file_exists_and_readonly(self):
        path = os.path.join(os.path.dirname(__file__), "..", "sql", "views",
                            "ap_multi_account_signal_ledger.sql")
        with open(path) as f:
            sql = f.read()
        # Read-only guarantees: a view, with no mutation verbs in the body.
        assert "CREATE OR REPLACE VIEW ap_multi_account_signal_ledger" in sql
        body = sql.upper()
        for verb in ("INSERT INTO ORDERS", "UPDATE ORDERS", "DELETE FROM ORDERS"):
            assert verb not in body, f"view must be read-only, found {verb}"

    def test_view_uses_split_part_join(self):
        path = os.path.join(os.path.dirname(__file__), "..", "sql", "views",
                            "ap_multi_account_signal_ledger.sql")
        with open(path) as f:
            sql = f.read()
        assert "split_part(o.signal_id, ':', 2)" in sql
        assert "LEFT JOIN ap_signals" in sql
        assert "WHERE o.kind = 'ENTRY'" in sql
