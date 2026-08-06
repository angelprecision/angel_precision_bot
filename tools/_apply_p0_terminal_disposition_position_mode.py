from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise AssertionError(f"{label}: expected one match, found {count}")
    return text.replace(old, new, 1)


source_path = Path("ap_execution_core.py")
source = source_path.read_text()

anchor = '''    def _current_open_position_count(self) -> int:
'''
helper = '''    def _position_snapshot_mode(self) -> str:
        """Return the canonical mode required by APPositionManager.snapshot().

        Position truth must remain scoped to the runner's exact LIVE/PAPER
        identity.  Never call snapshot without an explicit mode and never
        invent a default when runtime identity is malformed.
        """
        mode = str(
            getattr(self, "execution_mode", None)
            or getattr(self, "mode", None)
            or ""
        ).strip().lower()
        if mode not in {"live", "paper"}:
            raise RuntimeError(f"position_snapshot_execution_mode_invalid:{mode or 'missing'}")
        return mode

    def _current_open_position_count(self) -> int:
'''
source = replace_once(source, anchor, helper, "position snapshot helper")

source = replace_once(
    source,
    '''                snap = self.position_manager.snapshot()
                return int(snap.get("open_count") or 0)
''',
    '''                snap = self.position_manager.snapshot(
                    mode=self._position_snapshot_mode()
                )
                return int(snap.get("open_count") or 0)
''',
    "open position snapshot mode",
)
source = replace_once(
    source,
    '''                snap = self.position_manager.snapshot()
                return int(snap.get("pending_entries") or 0)
''',
    '''                snap = self.position_manager.snapshot(
                    mode=self._position_snapshot_mode()
                )
                return int(snap.get("pending_entries") or 0)
''',
    "pending entry snapshot mode",
)

source = replace_once(
    source,
    '''        def _terminalize_breach_failure(
            reason: str,
            *,
            cleanup_action: str = "expire",
            meta_patch: dict | None = None,
            decision_status: str = "blocked_at_breach",
            context_notes: str | None = None,
            funnel_key: str = "order_failed",
        ) -> None:
''',
    '''        def _terminalize_breach_failure(
            reason: str,
            *,
            cleanup_action: str = "expire",
            meta_patch: dict | None = None,
            decision_status: str = "blocked_at_breach",
            context_notes: str | None = None,
            funnel_key: str = "order_failed",
        ) -> dict:
''',
    "terminal helper return contract",
)

source = replace_once(
    source,
    '''                if _sel is not None and not _sel_result_valid:
                    _invalid_reason = "SELECTOR_RESULT_INVALID"
                    _terminalize_deferred_breach_failure(
''',
    '''                if _sel is not None and not _sel_result_valid:
                    _invalid_reason = "SELECTOR_RESULT_INVALID"
                    return _terminalize_deferred_breach_failure(
''',
    "invalid selector terminal disposition return",
)

source_path.write_text(source)


test_path = Path("tests/test_p0_terminal_disposition_position_mode.py")
test_path.write_text(r'''from __future__ import annotations

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
    end = source.index("# Acceptance Ask Cap", start)
    branch = source[start:end]
    assert 'return _terminalize_deferred_breach_failure(' in branch
    assert '_invalid_reason = "SELECTOR_RESULT_INVALID"' in branch


def test_terminal_helper_declares_its_actual_dict_contract():
    source = Path("ap_execution_core.py").read_text()
    start = source.index("def _terminalize_breach_failure(")
    signature_end = source.index(":\n", start) + 2
    assert ") -> dict:" in source[start:signature_end]
''')
