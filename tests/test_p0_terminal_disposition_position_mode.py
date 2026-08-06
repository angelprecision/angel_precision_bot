from __future__ import annotations

import threading
from pathlib import Path

from ap_execution_core import APExecutionCore


class _ModeRecordingPositionManager:
    def __init__(self):
        self.calls = []

    def snapshot(self, *, mode=None):
        self.calls.append(mode)
        return {"open_count": 2, "pending_entries": 3}


def _core(mode: str):
    core = object.__new__(APExecutionCore)
    core.mode = mode.upper()
    core.execution_mode = mode.lower()
    core.email = "client@example.com"
    core.position_manager = _ModeRecordingPositionManager()
    core._pos_lock = threading.Lock()
    core._position_count = 99
    return core


def test_live_position_truth_is_explicitly_mode_scoped():
    core = _core("live")
    assert core._current_open_position_count() == 2
    assert core._current_pending_entry_count() == 3
    assert core.position_manager.calls == ["live", "live"]


def test_paper_position_truth_is_explicitly_mode_scoped():
    core = _core("paper")
    assert core._current_open_position_count() == 2
    assert core._current_pending_entry_count() == 3
    assert core.position_manager.calls == ["paper", "paper"]


def test_invalid_mode_never_queries_unscoped_position_truth():
    core = _core("live")
    core.mode = ""
    core.execution_mode = ""
    assert core._current_open_position_count() == 99
    assert core._current_pending_entry_count() == 0
    assert core.position_manager.calls == []


def test_invalid_selector_result_returns_terminal_disposition():
    source = Path("ap_execution_core.py").read_text()
    start = source.index("if _sel is not None and not _sel_result_valid:")
    end = source.index("A deferred breach retry can start from a durable row", start)
    branch = source[start:end]
    assert 'return _terminalize_deferred_breach_failure(' in branch
    assert '_invalid_reason = "SELECTOR_RESULT_INVALID"' in branch


def test_terminal_helper_declares_its_actual_dict_contract():
    source = Path("ap_execution_core.py").read_text()
    start = source.index("def _terminalize_breach_failure(")
    signature_end = source.index(":\n", start) + 2
    assert ") -> dict:" in source[start:signature_end]
