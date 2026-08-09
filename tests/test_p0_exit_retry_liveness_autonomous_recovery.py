# tests/test_p0_exit_retry_liveness_autonomous_recovery.py
# =============================================================================
# P0 regression: PR #423 Patch 3 — single-cancellation-owner guarantee.
# When APOrderMonitor is alive, ap.exit_autonomous_recovery must never
# independently cancel a broker exit order it might already be working;
# it must defer and report recovery_owner=order_monitor_stale_exit. When
# the monitor is unavailable/dead/unregistered, autonomous recovery keeps
# its existing exact-identity-fenced independent-cancel fallback.
# =============================================================================

from __future__ import annotations

import os
import sys
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path("/home/claude/angel_precision_bot")
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))

from ap.exit_autonomous_recovery import (  # noqa: E402
    recover_exit_position,
    _order_monitor_alive,
)


def _alive_monitor():
    mon = MagicMock()
    t = threading.Thread(target=lambda: None)
    t.start()
    t.join()
    # A real Thread object whose is_alive() we control via a stand-in.
    fake_thread = MagicMock()
    fake_thread.is_alive.return_value = True
    mon._thread = fake_thread
    return mon


def _dead_monitor():
    mon = MagicMock()
    fake_thread = MagicMock()
    fake_thread.is_alive.return_value = False
    mon._thread = fake_thread
    return mon


def test_order_monitor_alive_helper_true_case():
    assert _order_monitor_alive(_alive_monitor()) is True


def test_order_monitor_alive_helper_false_cases():
    assert _order_monitor_alive(_dead_monitor()) is False
    assert _order_monitor_alive(None) is False
    assert _order_monitor_alive(object()) is False  # no _thread attr at all


def _pos(**overrides):
    defaults = dict(
        position_id="pos-1",
        option_symbol="AVGO260814C00350000",
        pending_exit_local_order_id="loc-1",
        pending_exit_broker_order_id="",  # forces the ambiguous-scan path
        pending_exit_qty=2,
    )
    defaults.update(overrides)
    return MagicMock(**defaults)


def test_ambiguous_multi_match_defers_when_order_monitor_alive(monkeypatch):
    """
    Two ambiguous open exit orders found for the contract, but the order
    monitor is alive: autonomous recovery must NOT independently cancel
    either one, and must report ownership deferred to the order monitor.
    """
    import ap.exit_autonomous_recovery as rec_mod

    monkeypatch.setattr(
        rec_mod, "_matching_open_exit_orders",
        lambda broker, contract, exclude_broker_id=None: [
            ("bro-a", {"status": "working"}), ("bro-b", {"status": "working"}),
        ],
    )
    cancel_spy = MagicMock(return_value=(True, {"status": "canceled"}))
    monkeypatch.setattr(rec_mod, "_cancel_order_with_proof", cancel_spy)

    broker = MagicMock()
    pos = _pos()
    action = recover_exit_position(
        pos, broker=broker, exit_engine=MagicMock(),
        order_monitor=_alive_monitor(),
    )

    cancel_spy.assert_not_called()
    assert action.action == "CONFIRMED_OPEN"
    assert action.details.get("recovery_owner") == "order_monitor_stale_exit"


def test_ambiguous_multi_match_cancels_independently_when_order_monitor_dead(monkeypatch):
    """
    Same ambiguous-multi-match shape, but the order monitor is dead/absent:
    autonomous recovery retains its existing independent-cancel fallback
    (unchanged behavior from before #423).
    """
    import ap.exit_autonomous_recovery as rec_mod

    monkeypatch.setattr(
        rec_mod, "_matching_open_exit_orders",
        lambda broker, contract, exclude_broker_id=None: [
            ("bro-a", {"status": "working"}), ("bro-b", {"status": "working"}),
        ],
    )
    cancel_spy = MagicMock(return_value=(True, {"status": "canceled"}))
    monkeypatch.setattr(rec_mod, "_cancel_order_with_proof", cancel_spy)

    exit_engine = MagicMock()
    broker = MagicMock()
    pos = _pos()

    for om in (_dead_monitor(), None):
        cancel_spy.reset_mock()
        action = recover_exit_position(
            pos, broker=broker, exit_engine=exit_engine, order_monitor=om,
        )
        assert cancel_spy.call_count == 2, f"expected independent cancel fallback for order_monitor={om}"
        assert action.action in ("REPLACEMENT_SAFE", "CLEARED_IN_FLIGHT", "NOOP")


def test_single_open_order_path_tags_ownership_without_ever_canceling(monkeypatch):
    """
    The exact-broker-id single-open-order path never cancels regardless of
    monitor liveness (pre-existing safe behavior) — this test only checks
    the recovery_owner tag changes correctly with monitor liveness.
    """
    import ap.exit_autonomous_recovery as rec_mod

    monkeypatch.setattr(
        rec_mod, "_get_order", lambda broker, bid: {"status": "working"},
    )
    pos = _pos(pending_exit_broker_order_id="bro-known")

    action_alive = recover_exit_position(
        pos, broker=MagicMock(), exit_engine=MagicMock(), order_monitor=_alive_monitor(),
    )
    action_dead = recover_exit_position(
        pos, broker=MagicMock(), exit_engine=MagicMock(), order_monitor=_dead_monitor(),
    )

    assert action_alive.action == "CONFIRMED_OPEN"
    assert action_alive.details.get("recovery_owner") == "order_monitor_stale_exit"
    assert action_dead.details.get("recovery_owner") == "autonomous_recovery"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
