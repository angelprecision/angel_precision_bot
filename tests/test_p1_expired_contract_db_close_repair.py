from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock


REPO = Path(__file__).resolve().parents[1]
PM_SRC = (REPO / "ap" / "position_manager.py").read_text()
EE_SRC = (REPO / "ap_exit_engine.py").read_text()


def test_close_expired_position_helper_exists():
    assert "def close_expired_position(" in PM_SRC


def test_close_expired_position_marks_terminal_without_fake_fill():
    start = PM_SRC.find("def close_expired_position(")
    end = PM_SRC.find("\n    def ", start + 1)
    body = PM_SRC[start:end]
    stripped = body.replace("close_position_from_exit_fill():", "")
    assert "PositionStatus.EXPIRED" in body
    assert '_add("quantity_remaining", 0)' in body
    assert "exit_price =" not in stripped
    assert "filled_qty =" not in stripped


def test_exit_engine_calls_db_repair_after_local_cleanup():
    idx = EE_SRC.find("def _check_all_positions(")
    end = EE_SRC.find("\n    def ", idx + 1)
    body = EE_SRC[idx:end]
    assert "expired_for_db_cleanup" in body
    assert "close_expired_position(" in body
    assert "EXPIRED_CONTRACT_DB_REPAIR_SKIPPED" in body
    assert "EXPIRED_CONTRACT_DB_REPAIR_FAILED" in body
    assert 'getattr(self, "_position_manager", None)' in body


def test_local_cleanup_is_preserved():
    idx = EE_SRC.find("def _check_all_positions(")
    end = EE_SRC.find("\n    def ", idx + 1)
    body = EE_SRC[idx:end]
    assert "self._positions = [p for p in self._positions if not p.closed]" in body
    assert "self._positions_by_id.pop(_ep.position_id, None)" in body


def test_behavioral_expired_position_calls_private_position_manager():
    from ap_exit_engine import APExitEngine

    class _Broker:
        pass

    engine = APExitEngine(broker=_Broker(), email="test@example.com")
    engine._position_manager = MagicMock()
    engine._broker_position_precheck = lambda: True
    engine._run_sentinels = lambda: None
    engine._emit_exit_event = lambda *a, **k: None

    expired = SimpleNamespace(
        option_symbol="SPY260613C00500000",
        position_id="pos-1",
        closed=False,
        close_reason="",
    )
    engine._positions = [expired]
    engine._positions_by_id = {"pos-1": expired}

    engine._check_all_positions()

    engine._position_manager.close_expired_position.assert_called_once()
