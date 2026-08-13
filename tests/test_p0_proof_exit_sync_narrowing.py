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


def test_fallback_updates_are_removed():
    body = _sync_exit_price_body()
    assert "WHERE id = (" not in body
    assert "SELECT id FROM proof_trades" not in body
    assert "ORDER BY closed_at DESC" not in body
    assert "LIMIT 1" not in body


def test_sync_requires_exact_position_identity():
    body = _sync_exit_price_body()
    assert "WHERE position_id = %s" in body
    assert "(position_id IS NULL OR position_id = '')" not in body


def test_legacy_loss_filter_removed():
    body = _sync_exit_price_body()
    assert "AND win = FALSE" not in body


def test_warning_message_describes_exact_identity_path():
    body = _sync_exit_price_body()
    assert "(exact position_id=%s did not match)" in body


def test_rowcount_uses_execute_result_when_available():
    body = _sync_exit_price_body()
    assert 'return getattr(cur, "rowcount", getattr(c, "rowcount", 0))' in body
    assert "primary_rowcount" not in body


def test_direct_sync_only_accepts_terminal_exit_fill():
    body = _sync_exit_price_body()
    assert 'result.get("status")' in body
    assert '!= "EXIT_FILLED"' in body
