from __future__ import annotations

import math
from unittest.mock import MagicMock

import pytest

from ap_entry_watcher import APEntryWatcher, WatchState, WatchedSignal


INVALID_QUOTES = [
    pytest.param(0, id="zero"),
    pytest.param(-1, id="negative"),
    pytest.param(None, id="missing"),
    pytest.param(True, id="true"),
    pytest.param(False, id="false"),
    pytest.param(float("nan"), id="nan"),
    pytest.param(float("inf"), id="positive-infinity"),
    pytest.param(float("-inf"), id="negative-infinity"),
    pytest.param("not-a-price", id="malformed-string"),
]


class _QuoteWatcher(APEntryWatcher):
    def __init__(self, quote_sequence, *, mode="PAPER", order_state_machine=None):
        self._quote_sequence = list(quote_sequence)
        self.broker = MagicMock()
        super().__init__(
            self.broker,
            order_state_machine=order_state_machine,
            require_on_trigger=False,
            mode=mode,
        )
        self._persist_watcher_audit = MagicMock()

    def _fetch_quotes(self, tickers):
        quote = self._quote_sequence.pop(0)
        return {"SPY": dict(quote) if isinstance(quote, dict) else quote}


def _register(watcher: APEntryWatcher, watched: WatchedSignal) -> None:
    watched._watcher_ref = watcher
    watcher._pending.append(watched)
    watcher._dedup_set.add(watched.signal_id)


def _poll_watched(side: str, quote_sequence, *, mode="PAPER", osm=None):
    watcher = _QuoteWatcher(quote_sequence, mode=mode, order_state_machine=osm)
    watched = WatchedSignal(_signal(side=side), overnight=False)
    _register(watcher, watched)
    return watcher, watched


def _signal(*, side: str, entry_price: float = 100.0) -> dict:
    return {
        "ticker": "SPY",
        "side": side,
        "entry_price": entry_price,
        "stop_price": 105.0 if side == "PUT" else 95.0,
        "target_price": 95.0 if side == "PUT" else 105.0,
        "signal_id": f"sig-{side.lower()}-479",
        "canonical_signal_id": f"canonical-{side.lower()}-479",
        "local_order_id": f"local-{side.lower()}-479",
        "client_id": "client@example.com",
        "execution_mode": "paper",
    }


def test_put_zero_quote_cannot_confirm_a_breach():
    watched = WatchedSignal(_signal(side="PUT"), overnight=False)

    assert watched.check(bid=0.0, ask=0.0) == WatchState.PENDING
    assert watched.check(bid=0.0, ask=0.0) != WatchState.TRIGGERED
    assert watched.breach_count == 0
    assert watched.breach_price == 0.0
    assert watched.trigger_price is None
    assert watched.trigger_crossed_at is None


@pytest.mark.parametrize("bad_ask", INVALID_QUOTES)
def test_call_invalid_ask_has_no_breach_authority(bad_ask):
    watched = WatchedSignal(_signal(side="CALL"), overnight=False)

    assert watched.check(bid=99.0, ask=bad_ask) == WatchState.PENDING
    assert watched.breach_count == 0
    assert watched.breach_price == 0.0
    assert watched.trigger_price is None
    assert watched.trigger_crossed_at is None
    assert watched.last_trigger_evidence_reason == "TRIGGER_EVIDENCE_UNAVAILABLE_ASK"


@pytest.mark.parametrize("bad_bid", INVALID_QUOTES)
def test_put_invalid_bid_has_no_breach_authority(bad_bid):
    watched = WatchedSignal(_signal(side="PUT"), overnight=False)

    assert watched.check(bid=bad_bid, ask=101.0) == WatchState.PENDING
    assert watched.breach_count == 0
    assert watched.breach_price == 0.0
    assert watched.trigger_price is None
    assert watched.trigger_crossed_at is None
    assert watched.last_trigger_evidence_reason == "TRIGGER_EVIDENCE_UNAVAILABLE_BID"


def test_call_valid_ask_below_trigger_does_not_breach():
    watched = WatchedSignal(_signal(side="CALL"), overnight=False)

    assert watched.check(bid=99.0, ask=99.99) == WatchState.PENDING
    assert watched.breach_count == 0


def test_put_valid_bid_above_trigger_does_not_breach():
    watched = WatchedSignal(_signal(side="PUT"), overnight=False)

    assert watched.check(bid=100.01, ask=101.0) == WatchState.PENDING
    assert watched.breach_count == 0


@pytest.mark.parametrize(
    ("side", "first_quote", "second_quote", "expected_price"),
    [
        ("CALL", (99.0, 100.0), (99.0, 100.0), 100.0),
        ("PUT", (100.0, 101.0), (100.0, 101.0), 100.0),
    ],
)
def test_two_consecutive_valid_side_quotes_confirm_normally(
    side, first_quote, second_quote, expected_price
):
    watched = WatchedSignal(_signal(side=side), overnight=False)

    assert watched.check(*first_quote) == WatchState.PENDING
    assert watched.breach_count == 1
    assert watched.trigger_crossed_at is None
    assert watched.check(*second_quote) == WatchState.TRIGGERED
    assert watched.trigger_price == expected_price
    assert watched.breach_price == expected_price
    assert watched.trigger_crossed_at is not None


@pytest.mark.parametrize(
    ("side", "first_quote", "invalid_quote", "next_valid_quote"),
    [
        ("CALL", (99.0, 100.0), (99.0, None), (99.0, 100.0)),
        ("PUT", (100.0, 101.0), (None, 101.0), (100.0, 101.0)),
    ],
)
def test_invalid_observation_resets_pending_two_poll_confirmation(
    side, first_quote, invalid_quote, next_valid_quote
):
    watched = WatchedSignal(_signal(side=side), overnight=False)

    assert watched.check(*first_quote) == WatchState.PENDING
    assert watched.breach_count == 1
    assert watched._pending_first_breach_at is not None

    assert watched.check(*invalid_quote) == WatchState.PENDING
    assert watched.breach_count == 0
    assert watched._pending_first_breach_at is None
    assert watched.breach_price == 0.0
    assert watched.trigger_price is None
    assert watched.trigger_crossed_at is None

    # This is a new first observation, not confirmation using poll 1.
    assert watched.check(*next_valid_quote) == WatchState.PENDING
    assert watched.breach_count == 1
    assert watched.check(*next_valid_quote) == WatchState.TRIGGERED


@pytest.mark.parametrize(
    ("side", "quote_sequence"),
    [
        (
            "CALL",
            [
                {"bid": 99.0, "ask": 100.0, "last": 100.0},
                {"bid": 99.0, "ask": None, "last": 105.0},
                {"bid": 99.0, "ask": 100.0, "last": 100.0},
                {"bid": 99.0, "ask": 100.0, "last": 100.0},
            ],
        ),
        (
            "PUT",
            [
                {"bid": 100.0, "ask": 101.0, "last": 100.0},
                {"bid": None, "ask": 101.0, "last": 95.0},
                {"bid": 100.0, "ask": 101.0, "last": 100.0},
                {"bid": 100.0, "ask": 101.0, "last": 100.0},
            ],
        ),
    ],
)
def test_real_active_poll_resets_invalid_side_and_calls_callback_only_after_new_streak(
    side, quote_sequence
):
    watcher, watched = _poll_watched(side, quote_sequence)
    callback = MagicMock(return_value={"disposition": "KEEP_WATCHER"})
    watcher.on_trigger = callback

    watcher._poll_active_signals()
    assert watched.breach_count == 1
    watcher._poll_active_signals()
    assert watched.breach_count == 0
    assert watched.last_trigger_evidence_reason.endswith(
        "_ASK" if side == "CALL" else "_BID"
    )
    watcher._poll_active_signals()
    assert watched.breach_count == 1
    assert callback.call_count == 0
    watcher._poll_active_signals()

    assert callback.call_count == 1
    assert watched.trigger_crossed_at is not None
    assert watched.trigger_price == 100.0
    assert watcher.broker.submit_order.call_count == 0
    assert watcher.broker.cancel_order.call_count == 0


@pytest.mark.parametrize(
    ("side", "quotes"),
    [
        (
            "CALL",
            [
                {"bid": 0.0, "ask": 0.0, "last": 105.0},
                {"bid": 0.0, "ask": 0.0, "last": 105.0},
            ],
        ),
        (
            "PUT",
            [
                {"bid": 0.0, "ask": 0.0, "last": 95.0},
                {"bid": 0.0, "ask": 0.0, "last": 95.0},
            ],
        ),
    ],
)
def test_last_never_substitutes_for_required_side(side, quotes):
    watcher, watched = _poll_watched(side, quotes)
    callback = MagicMock()
    watcher.on_trigger = callback

    watcher._poll_active_signals()
    watcher._poll_active_signals()

    assert callback.call_count == 0
    assert watched.state == WatchState.PENDING
    assert watched.breach_count == 0
    assert watched.trigger_crossed_at is None
    assert watched.trigger_price is None
    assert watcher.broker.submit_order.call_count == 0
    assert watcher.broker.cancel_order.call_count == 0


@pytest.mark.parametrize("side", ["CALL", "PUT"])
def test_valid_callback_preserves_exact_identity_for_paper_and_live(side):
    for mode in ("PAPER", "LIVE"):
        signal = _signal(side=side)
        signal["execution_mode"] = mode.lower()
        osm = MagicMock()
        osm.update_order_meta.return_value = True
        watcher = _QuoteWatcher(
            [
                {"bid": 99.0, "ask": 100.0},
                {"bid": 99.0, "ask": 100.0},
            ],
            mode=mode,
            order_state_machine=osm,
        )
        watched = WatchedSignal(signal, overnight=False)
        _register(watcher, watched)
        captured = []
        watcher.on_trigger = lambda item: captured.append(item) or {
            "disposition": "KEEP_WATCHER"
        }

        watcher._poll_active_signals()
        watcher._poll_active_signals()

        assert len(captured) == 1
        received = captured[0].signal
        assert received["client_id"] == "client@example.com"
        assert received["execution_mode"] == mode.lower()
        assert received["local_order_id"] == f"local-{side.lower()}-479"
        assert captured[0].side == side


@pytest.mark.parametrize("side", ["CALL", "PUT"])
def test_invalid_entry_trigger_is_not_authority(side):
    signal = _signal(side=side, entry_price=math.nan)
    watched = WatchedSignal(signal, overnight=False)

    assert watched.check(99.0, 100.0) == WatchState.PENDING
    assert watched.check(99.0, 100.0) == WatchState.PENDING
    assert watched.breach_count == 0
    assert watched.trigger_crossed_at is None


# ── PR #479 amendment: P0 ordering fix regressions ──────────────────────
#
# Audit finding: the invalid-required-quote early return in check() ran
# BEFORE the per-side stop-invalidation check. On an already-confirmed
# lifecycle (trigger_crossed_at set) that a callback-driven retry
# (RETRY_WAIT / KEEP_WATCHER / RECONCILE_BROKER_INTENT) has returned to
# PENDING, a missing *entry*-side quote (ASK for CALL, BID for PUT) would
# early-return and skip the stop check entirely — even when the
# *opposite*-side quote was valid and had broken the still-active stop.
# That could leave a stale/junk confirmed setup retryable when it should
# already have been killed.


def _confirm_trigger(watched: WatchedSignal, side: str):
    """Drive a fresh WatchedSignal through a normal two-poll confirmation
    and return the durable trigger_crossed_at timestamp."""
    quote = (99.0, 100.0) if side == "CALL" else (100.0, 101.0)
    assert watched.check(*quote) == WatchState.PENDING
    assert watched.check(*quote) == WatchState.TRIGGERED
    assert watched.trigger_crossed_at is not None
    return watched.trigger_crossed_at


def test_call_confirmed_stop_break_invalidates_despite_missing_ask():
    """CALL: confirmed trigger -> callback retry (PENDING) -> next poll has
    an invalid/unavailable ASK but a valid BID that has broken the active
    CALL stop -> must INVALIDATE. Must not silently stay PENDING just
    because the required entry-side quote went missing."""
    watched = WatchedSignal(_signal(side="CALL"), overnight=False)
    confirmed_at = _confirm_trigger(watched, "CALL")

    # Simulate a callback-ownership retry (RETRY_WAIT / KEEP_WATCHER /
    # RECONCILE_BROKER_INTENT) returning the watcher to PENDING while the
    # durable trigger proof is preserved — mirrors
    # APEntryWatcher._poll_active_signals' `w.state = WatchState.PENDING`.
    watched.state = WatchState.PENDING

    # stop_price for CALL is 95.0 (see _signal()); bid=90.0 is well below
    # 95.0 * (1 - WRONG_DIR_BUFFER_PCT) and must break the active stop.
    result = watched.check(bid=90.0, ask=None)

    assert result == WatchState.INVALIDATED
    assert watched.last_trigger_evidence_reason == "TRIGGER_EVIDENCE_UNAVAILABLE_ASK"
    # The original trigger proof is untouched — only the lifecycle died.
    assert watched.trigger_crossed_at == confirmed_at


def test_put_confirmed_stop_break_invalidates_despite_missing_bid():
    """PUT mirror of the CALL case above."""
    watched = WatchedSignal(_signal(side="PUT"), overnight=False)
    confirmed_at = _confirm_trigger(watched, "PUT")

    watched.state = WatchState.PENDING

    # stop_price for PUT is 105.0 (see _signal()); ask=110.0 is well above
    # 105.0 * (1 + WRONG_DIR_BUFFER_PCT) and must break the active stop.
    result = watched.check(bid=None, ask=110.0)

    assert result == WatchState.INVALIDATED
    assert watched.last_trigger_evidence_reason == "TRIGGER_EVIDENCE_UNAVAILABLE_BID"
    assert watched.trigger_crossed_at == confirmed_at


def test_call_confirmed_retry_no_callback_fire_after_stop_invalidation():
    """Integration-level: once the missing-ASK/broken-stop poll invalidates
    a confirmed, retry-pending CALL watcher, the poll loop must mark it
    done and must NOT invoke on_trigger again."""
    quotes = [
        {"bid": 99.0, "ask": 100.0},  # breach candidate
        {"bid": 99.0, "ask": 100.0},  # confirms -> TRIGGERED, callback fires
        {"bid": 90.0, "ask": None},   # retry-pending poll: ASK missing, stop broken
    ]
    watcher, watched = _poll_watched("CALL", quotes)
    callback = MagicMock(return_value={"disposition": "RETRY_WAIT"})
    watcher.on_trigger = callback

    watcher._poll_active_signals()
    watcher._poll_active_signals()
    assert callback.call_count == 1
    assert watched.state == WatchState.PENDING
    assert watched.trigger_crossed_at is not None
    watched.deferred_retry_not_before = None  # clear retry backoff for the test poll

    watcher._poll_active_signals()

    assert watched.state == WatchState.INVALIDATED
    assert callback.call_count == 1  # must not have fired again
    assert watcher.broker.submit_order.call_count == 0


def test_put_confirmed_retry_no_callback_fire_after_stop_invalidation():
    """PUT mirror of the integration-level case above."""
    quotes = [
        {"bid": 100.0, "ask": 101.0},  # breach candidate
        {"bid": 100.0, "ask": 101.0},  # confirms -> TRIGGERED, callback fires
        {"bid": None, "ask": 110.0},   # retry-pending poll: BID missing, stop broken
    ]
    watcher, watched = _poll_watched("PUT", quotes)
    callback = MagicMock(return_value={"disposition": "RETRY_WAIT"})
    watcher.on_trigger = callback

    watcher._poll_active_signals()
    watcher._poll_active_signals()
    assert callback.call_count == 1
    assert watched.state == WatchState.PENDING
    assert watched.trigger_crossed_at is not None
    watched.deferred_retry_not_before = None

    watcher._poll_active_signals()

    assert watched.state == WatchState.INVALIDATED
    assert callback.call_count == 1
    assert watcher.broker.submit_order.call_count == 0


def test_call_confirmed_retry_pending_missing_ask_stays_pending_when_stop_intact():
    """Preservation case: confirmed trigger -> retry PENDING -> required
    entry quote invalid -> stop-side quote valid but NOT broken -> must
    remain safely PENDING. No fabricated trigger, no reconfirmation."""
    watched = WatchedSignal(_signal(side="CALL"), overnight=False)
    confirmed_at = _confirm_trigger(watched, "CALL")
    watched.state = WatchState.PENDING

    # bid=99.0 is well above the CALL stop (95.0) — stop not broken.
    result = watched.check(bid=99.0, ask=None)

    assert result == WatchState.PENDING
    assert watched.last_trigger_evidence_reason == "TRIGGER_EVIDENCE_UNAVAILABLE_ASK"
    assert watched.trigger_crossed_at == confirmed_at


def test_put_confirmed_retry_pending_missing_bid_stays_pending_when_stop_intact():
    """PUT mirror of the preservation case above."""
    watched = WatchedSignal(_signal(side="PUT"), overnight=False)
    confirmed_at = _confirm_trigger(watched, "PUT")
    watched.state = WatchState.PENDING

    # ask=101.0 is well below the PUT stop (105.0) — stop not broken.
    result = watched.check(bid=None, ask=101.0)

    assert result == WatchState.PENDING
    assert watched.last_trigger_evidence_reason == "TRIGGER_EVIDENCE_UNAVAILABLE_BID"
    assert watched.trigger_crossed_at == confirmed_at


@pytest.mark.parametrize("side", ["CALL", "PUT"])
def test_pre_confirmation_missing_quote_still_returns_early_and_resets(side):
    """Regression guard: the PRE-confirmation path (trigger_crossed_at is
    still None) must retain its original behavior exactly — full early
    return with pending-breach continuity reset. The amendment only
    changes behavior for the already-confirmed lifecycle."""
    watched = WatchedSignal(_signal(side=side), overnight=False)
    first_quote = (99.0, 100.0) if side == "CALL" else (100.0, 101.0)

    assert watched.check(*first_quote) == WatchState.PENDING
    assert watched.breach_count == 1
    assert watched._pending_first_breach_at is not None

    invalid_quote = (99.0, None) if side == "CALL" else (None, 101.0)
    result = watched.check(*invalid_quote)

    assert result == WatchState.PENDING
    assert watched.breach_count == 0
    assert watched._pending_first_breach_at is None
    assert watched.breach_price == 0.0
    assert watched.trigger_price is None
    assert watched.trigger_crossed_at is None
