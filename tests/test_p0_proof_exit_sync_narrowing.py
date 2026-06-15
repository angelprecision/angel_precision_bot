from pathlib import Path
import re


SRC = (Path(__file__).resolve().parents[1] / "ap" / "fill_monitor.py").read_text()


def _sync_exit_price_body() -> str:
    start = SRC.find("def _sync_exit_price(order: dict, result: dict):")
    assert start > 0, "_sync_exit_price not found"
    end = SRC.find("\n\n# =============================================================================", start)
    assert end > start, "end of _sync_exit_price not found"
    return SRC[start:end]


def test_primary_position_id_update_is_preserved():
    body = _sync_exit_price_body()
    assert "WHERE position_id = %s" in body


def test_fallback_updates_single_row_via_subquery():
    body = _sync_exit_price_body()
    assert "WHERE id = (" in body
    assert "SELECT id FROM proof_trades" in body
    assert "ORDER BY closed_at DESC" in body
    assert "LIMIT 1" in body


def test_fallback_only_targets_unresolved_orphans():
    body = _sync_exit_price_body()
    assert "(position_id IS NULL OR position_id = '')" in body
    assert "exit_option_price IS NULL" in body


def test_legacy_loss_filter_removed():
    body = _sync_exit_price_body()
    assert "AND win = FALSE" not in body


def test_warning_message_describes_primary_and_fallback_paths():
    body = _sync_exit_price_body()
    assert "primary by position_id=%s and narrowed fallback both empty" in body


def test_rowcount_uses_execute_result_when_available():
    body = _sync_exit_price_body()
    assert 'primary_rowcount = getattr(cur, "rowcount", getattr(c, "rowcount", 0))' in body
    assert 'return getattr(cur2, "rowcount", getattr(c, "rowcount", 0))' in body
    assert "return primary_rowcount" in body
