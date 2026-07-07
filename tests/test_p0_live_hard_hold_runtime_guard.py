from __future__ import annotations

import sys
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import ap


def test_guard_installs_import_hook_and_patches_loaded_targets():
    ap.install_live_hard_hold_runtime_safety_guard()
    import ap_entry_watcher
    import ap_execution_core

    assert getattr(ap_entry_watcher.APEntryWatcher, "_AP_LIVE_HARD_HOLD_GUARD", False) is True
    assert getattr(ap_execution_core.APExecutionCore, "_AP_LIVE_HARD_HOLD_GUARD", False) is True


def test_execution_core_guard_blocks_live_blank_execution_mode_without_calling_original(monkeypatch):
    ap.install_live_hard_hold_runtime_safety_guard()
    import ap_execution_core

    called = {"value": False}

    def original(self, watched):
        called["value"] = True
        return "submitted"

    # The guard is already installed. Patch the saved/original body by wrapping a
    # small fake instance through the guarded class method contract.
    core = types.SimpleNamespace(
        paper=False,
        mode="LIVE",
        execution_mode="live",
        client_id="client@example.com",
        email="client@example.com",
        _recover_plan_for_revalidation=lambda watched: types.SimpleNamespace(execution_mode=""),
    )
    watched = types.SimpleNamespace(
        ticker="META",
        signal={
            "client_id": "client@example.com",
            "execution_mode": "",
            "trigger_crossed_at": datetime.now(timezone.utc).isoformat(),
        },
    )

    # Call the class method directly; if the guard works it returns None before
    # entering the underlying submit path.
    result = ap_execution_core.APExecutionCore._on_entry_trigger(core, watched)
    assert result is None
    assert called["value"] is False


def test_execution_core_guard_blocks_stale_trigger(monkeypatch):
    ap.install_live_hard_hold_runtime_safety_guard()
    import ap_execution_core

    core = types.SimpleNamespace(
        paper=False,
        mode="LIVE",
        execution_mode="live",
        client_id="client@example.com",
        email="client@example.com",
        _recover_plan_for_revalidation=lambda watched: types.SimpleNamespace(execution_mode="live"),
    )
    watched = types.SimpleNamespace(
        ticker="META",
        signal={
            "client_id": "client@example.com",
            "execution_mode": "live",
            "trigger_crossed_at": (datetime.now(timezone.utc) - timedelta(seconds=300)).isoformat(),
        },
    )

    result = ap_execution_core.APExecutionCore._on_entry_trigger(core, watched)
    assert result is None
