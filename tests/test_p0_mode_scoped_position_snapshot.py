from __future__ import annotations

import threading

import pytest

from ap_execution_core import APExecutionCore


class _RecordingPositionManager:
    def __init__(self, *, result=None, error=None):
        self.calls = []
        self.result = result or {"open_count": 2, "pending_entries": 3}
        self.error = error

    def snapshot(self, *, mode):
        self.calls.append(mode)
        if self.error is not None:
            raise self.error
        return dict(self.result)


def _core(position_manager, *, mode="LIVE", execution_mode="live"):
    core = object.__new__(APExecutionCore)
    core.mode = mode
    core.execution_mode = execution_mode
    core.email = "client@example.com"
    core.position_manager = position_manager
    core._pos_lock = threading.Lock()
    core._position_count = 99
    core._max_positions = 7
    return core


@pytest.mark.parametrize(
    ("mode", "execution_mode", "expected"),
    [
        ("LIVE", "live", "live"),
        (" live ", " LIVE ", "live"),
        ("PAPER", "paper", "paper"),
    ],
)
def test_position_counts_use_explicit_canonical_mode(mode, execution_mode, expected):
    manager = _RecordingPositionManager()
    core = _core(manager, mode=mode, execution_mode=execution_mode)

    assert core._current_open_position_count() == 2
    assert core._current_pending_entry_count() == 3
    assert manager.calls == [expected, expected]


def test_successful_scoped_snapshot_wins_over_local_fallback():
    manager = _RecordingPositionManager(
        result={"open_count": 0, "pending_entries": 0}
    )
    core = _core(manager)

    assert core._current_open_position_count() == 0
    assert core._current_pending_entry_count() == 0
    assert manager.calls == ["live", "live"]


def test_real_scoped_snapshot_failure_preserves_existing_fallback():
    manager = _RecordingPositionManager(error=RuntimeError("database unavailable"))
    core = _core(manager)

    assert core._current_open_position_count() == 99
    assert core._current_pending_entry_count() == 0
    assert manager.calls == ["live", "live"]


@pytest.mark.parametrize(
    ("mode", "execution_mode"),
    [
        ("", ""),
        ("staging", "staging"),
        ("LIVE", "staging"),
        ("LIVE", "paper"),
    ],
)
def test_invalid_or_conflicting_identity_never_calls_unscoped_snapshot(
    mode, execution_mode
):
    manager = _RecordingPositionManager()
    core = _core(manager, mode=mode, execution_mode=execution_mode)

    # Invalid identity fails closed at the capacity gate; it does not invent
    # PAPER/LIVE and it does not fall back to an unscoped database read.
    assert core._current_open_position_count() == 7
    assert manager.calls == []

    # The pending-count helper also performs no read for invalid identity.
    assert core._current_pending_entry_count() == 0
    assert manager.calls == []
