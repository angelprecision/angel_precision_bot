from types import SimpleNamespace
from unittest.mock import MagicMock

import ap_overnight_reeval as overnight
from ap_entry_watcher import APEntryWatcher


def _watched(token="token-a"):
    watched = SimpleNamespace(
        ticker="AAPL",
        signal={
            "signal_id": "sig-001",
            "client_id": "client-1",
            "execution_mode": "paper",
            "canonical_signal_id": "CANON-001",
            "local_order_id": "order-1",
            "overnight_watch_attempt_fenced": True,
            "overnight_watch_attempt_token": token,
            "overnight_watch_attempt_count": 1,
            "overnight_watch_attempt_session_key": "2026-06-12",
            "overnight_watch_attempt_canonical_signal_id": "CANON-001",
        },
    )
    watched._release_dedup_key = MagicMock()
    return watched


def test_callback_claim_forwards_exact_durable_attempt_identity(monkeypatch):
    watcher = APEntryWatcher(None, mode="PAPER")
    watched = _watched()
    claim = MagicMock(return_value=True)
    monkeypatch.setattr(overnight, "_claim_watch_callback_owner", claim)

    assert watcher._claim_durable_overnight_callback_owner(
        watched, reason="trigger_callback_claimed"
    ) is True

    kwargs = claim.call_args.kwargs
    assert kwargs["client_id"] == "client-1"
    assert kwargs["execution_mode"] == "paper"
    assert kwargs["canonical_signal_id"] == "CANON-001"
    assert kwargs["session_key"] == "2026-06-12"
    assert kwargs["local_order_id"] == "order-1"
    assert kwargs["attempt"].token == "token-a"
    assert kwargs["attempt"].attempt_count == 1


def test_terminal_callback_is_not_invoked_after_cross_process_owner_rotation(
    monkeypatch,
):
    watcher = APEntryWatcher(None, mode="PAPER")
    watched = _watched("stale-token-a")
    watcher._pending = [watched]
    callback = MagicMock()
    monkeypatch.setattr(
        watcher, "_claim_durable_overnight_callback_owner", lambda *_a, **_kw: False
    )

    result = watcher._dispatch_completion(watched, callback)

    assert result.outcome == "FAILED"
    assert result.reason_code == "durable_watcher_attempt_ownership_lost"
    callback.assert_not_called()
    assert watcher._pending == []
    watched._release_dedup_key.assert_called_once()


def test_unfenced_ordinary_watcher_keeps_existing_callback_behavior():
    watcher = APEntryWatcher(None, mode="PAPER")
    watched = SimpleNamespace(ticker="AAPL", signal={"local_order_id": "ordinary-1"})

    assert watcher._claim_durable_overnight_callback_owner(
        watched, reason="ordinary_callback"
    ) is True
