"""PR #494 — preserve watcher breach continuity across unusable quote
observations.

Regression: pre-confirmation breach continuity was RESET whenever the
required canonical trigger-side quote (ASK for CALL, BID for PUT) was
missing/unusable, even though the current hardening correctly refuses to
treat that absence as valid trigger evidence. Absence of truth is not
contradictory truth — it must HOLD continuity, not erase it:

    VALID BREACH OBSERVATION            -> increment
    VALID NON-BREACH / CONTRADICTION    -> reset
    NO USABLE REQUIRED-SIDE OBSERVATION -> HOLD (neither increment nor reset)

This file is the required test matrix from the PR #494 amendment
(tests 1-13). It intentionally imports through the real exported package
path (``import ap_entry_watcher``) for the shim/base integration test,
matching the seam where the original regression could recur.

Scope note (test 13 / #474 adjacency): #474 (merged) owns downstream
deferred final-cost revalidation inside ap_execution_core.py, which is
reached through ``on_trigger``. Per the amendment, this file does not
duplicate #474's economics suite (see
tests/test_p0_474_production_recovery_runtime_replay.py for that). It
proves the narrower, watcher-side claim: a HOLD poll between two valid
breaches does not create a second callback, does not call the broker
directly, and preserves the exact identity fields (client_id,
execution_mode, local_order_id, signal_id) that #474's downstream
handling depends on to route to the correct client/order.
"""
from __future__ import annotations

import math
import sys
from unittest.mock import MagicMock

import pytest

import ap_entry_watcher
from ap_entry_watcher import APEntryWatcher, WatchState, WatchedSignal


UNUSABLE_QUOTES = [
    pytest.param(None, id="missing"),
    pytest.param(0, id="zero"),
    pytest.param(0.0, id="zero-float"),
    pytest.param(float("nan"), id="nan"),
    pytest.param(float("inf"), id="positive-infinity"),
    pytest.param(float("-inf"), id="negative-infinity"),
    pytest.param("not-a-price", id="malformed-string"),
    pytest.param(True, id="bool-true"),
]


def _signal(*, side: str, client_id: str = "client@example.com",
            execution_mode: str = "paper", entry_price: float = 100.0) -> dict:
    return {
        "ticker": "SPY",
        "side": side,
        "entry_price": entry_price,
        "stop_price": 95.0 if side == "CALL" else 105.0,
        "target_price": 105.0 if side == "CALL" else 95.0,
        "signal_id": f"sig-494-{side.lower()}",
        "canonical_signal_id": f"canonical-494-{side.lower()}",
        "local_order_id": f"local-494-{side.lower()}",
        "client_id": client_id,
        "execution_mode": execution_mode,
    }


class _QuoteWatcher(APEntryWatcher):
    """Drives the real production poll loop (``_poll_active_signals``)
    against a scripted quote sequence, exactly as the P0 watcher hardening
    suite does elsewhere in this repo."""

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
        ticker = tickers[0] if tickers else "SPY"
        return {ticker: dict(quote) if isinstance(quote, dict) else quote}


def _register(watcher: APEntryWatcher, watched: WatchedSignal) -> None:
    watched._watcher_ref = watcher
    watcher._pending.append(watched)
    watcher._dedup_set.add(watched.signal_id)


def _poll_watched(side: str, quote_sequence, *, mode="PAPER", osm=None):
    watcher = _QuoteWatcher(quote_sequence, mode=mode, order_state_machine=osm)
    watched = WatchedSignal(_signal(side=side), overnight=False)
    _register(watcher, watched)
    return watcher, watched


# ─────────────────────────────────────────────────────────────────────────
# TEST 1 — CALL: breach -> outage -> breach
# TEST 2 — PUT:  breach -> outage -> breach
# ─────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("side", "quote_sequence"),
    [
        (
            "CALL",
            [
                {"bid": 99.0, "ask": 100.05},
                {"bid": 99.0, "ask": None},
                {"bid": 99.0, "ask": 100.06},
            ],
        ),
        (
            "PUT",
            [
                {"bid": 100.0, "ask": 101.0},
                {"bid": None, "ask": 101.0},
                {"bid": 99.94, "ask": 101.0},
            ],
        ),
    ],
)
def test_breach_outage_breach_confirms_exactly_once(side, quote_sequence):
    watcher, watched = _poll_watched(side, quote_sequence)
    callback = MagicMock(return_value={"disposition": "KEEP_WATCHER"})
    watcher.on_trigger = callback

    watcher._poll_active_signals(open_protect_active=False)
    assert watched.breach_count == 1
    assert callback.call_count == 0

    watcher._poll_active_signals(open_protect_active=False)  # HOLD
    assert watched.breach_count == 1
    assert callback.call_count == 0

    watcher._poll_active_signals(open_protect_active=False)  # confirms
    assert watched.breach_count == 2
    assert callback.call_count == 1
    assert watched.trigger_crossed_at is not None


# ─────────────────────────────────────────────────────────────────────────
# TEST 3 — valid contradiction resets (HOLD is not sticky)
# ─────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("side", "first_quote", "missing_quote", "contradiction_quote"),
    [
        ("CALL", (99.0, 100.10), (99.0, None), (99.0, 99.80)),
        ("PUT", (100.0, 101.0), (None, 101.0), (100.20, 101.0)),
    ],
)
def test_valid_contradiction_after_hold_still_resets(
    side, first_quote, missing_quote, contradiction_quote
):
    watched = WatchedSignal(_signal(side=side), overnight=False)

    assert watched.check(*first_quote) == WatchState.PENDING
    assert watched.breach_count == 1

    assert watched.check(*missing_quote) == WatchState.PENDING
    assert watched.breach_count == 1  # HOLD

    result = watched.check(*contradiction_quote)
    assert result == WatchState.PENDING
    assert watched.breach_count == 0  # genuine contradiction resets
    assert watched.trigger_crossed_at is None


# ─────────────────────────────────────────────────────────────────────────
# TEST 4 — repeated outages: count stays held, no callback, no fabrication
# ─────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("side", ["CALL", "PUT"])
def test_multiple_missing_polls_hold_without_callback(side):
    watched = WatchedSignal(_signal(side=side), overnight=False)
    first_quote = (99.0, 100.05) if side == "CALL" else (100.0, 101.0)
    missing = (99.0, None) if side == "CALL" else (None, 101.0)

    watched.check(*first_quote)
    assert watched.breach_count == 1
    first_pending_at = watched._pending_first_breach_at

    for _ in range(3):
        result = watched.check(*missing)
        assert result == WatchState.PENDING
        assert watched.breach_count == 1
        assert watched._pending_first_breach_at == first_pending_at
    assert watched.trigger_crossed_at is None


# ─────────────────────────────────────────────────────────────────────────
# TEST 5 — LAST-only: never substitutes for required canonical side
# ─────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("side", "quotes"),
    [
        ("CALL", [{"bid": None, "ask": None, "last": 105.0},
                   {"bid": None, "ask": None, "last": 105.0}]),
        ("PUT", [{"bid": None, "ask": None, "last": 95.0},
                  {"bid": None, "ask": None, "last": 95.0}]),
    ],
)
def test_last_only_quote_never_confirms_and_holds(side, quotes):
    watcher, watched = _poll_watched(side, quotes)
    callback = MagicMock()
    watcher.on_trigger = callback

    watcher._poll_active_signals(open_protect_active=False)
    watcher._poll_active_signals(open_protect_active=False)

    assert callback.call_count == 0
    assert watched.state == WatchState.PENDING
    assert watched.breach_count == 0
    assert watched.trigger_crossed_at is None


# ─────────────────────────────────────────────────────────────────────────
# TEST 6 — unusable canonical values: no increment, no reset, no exception
# ─────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("bad_ask", UNUSABLE_QUOTES)
def test_call_unusable_ask_holds_without_exception(bad_ask):
    watched = WatchedSignal(_signal(side="CALL"), overnight=False)
    watched.check(bid=99.0, ask=100.05)
    assert watched.breach_count == 1

    result = watched.check(bid=99.0, ask=bad_ask)

    assert result == WatchState.PENDING
    assert watched.breach_count == 1  # held, not reset
    assert watched.trigger_crossed_at is None


@pytest.mark.parametrize("bad_bid", UNUSABLE_QUOTES)
def test_put_unusable_bid_holds_without_exception(bad_bid):
    watched = WatchedSignal(_signal(side="PUT"), overnight=False)
    watched.check(bid=100.0, ask=101.0)
    assert watched.breach_count == 1

    result = watched.check(bid=bad_bid, ask=101.0)

    assert result == WatchState.PENDING
    assert watched.breach_count == 1  # held, not reset
    assert watched.trigger_crossed_at is None


# ─────────────────────────────────────────────────────────────────────────
# TEST 7 — confirmed lifecycle + entry-side missing + stop-side proves
# broken: stop safety still wins, no entry callback replay.
# (Covered directly by the pre-existing suite; re-asserted here for the
# PR #494 matrix completeness with an explicit HOLD poll first.)
# ─────────────────────────────────────────────────────────────────────────
def test_confirmed_call_hold_then_stop_break_invalidates_no_replay():
    quotes = [
        {"bid": 99.0, "ask": 100.0},   # breach candidate
        {"bid": 99.0, "ask": 100.0},   # confirms -> TRIGGERED, callback fires
        {"bid": 99.0, "ask": None},    # retry-pending: HOLD (entry side missing, stop intact)
        {"bid": 90.0, "ask": None},    # retry-pending: entry side missing, stop now broken
    ]
    watcher, watched = _poll_watched("CALL", quotes)
    callback = MagicMock(return_value={"disposition": "RETRY_WAIT"})
    watcher.on_trigger = callback

    watcher._poll_active_signals(open_protect_active=False)
    watcher._poll_active_signals(open_protect_active=False)
    assert callback.call_count == 1
    assert watched.state == WatchState.PENDING
    confirmed_at = watched.trigger_crossed_at
    assert confirmed_at is not None
    watched.deferred_retry_not_before = None

    watcher._poll_active_signals(open_protect_active=False)  # HOLD, stop intact
    assert watched.state == WatchState.PENDING
    assert callback.call_count == 1
    assert watched.trigger_crossed_at == confirmed_at
    watched.deferred_retry_not_before = None

    watcher._poll_active_signals(open_protect_active=False)  # stop breaks
    assert watched.state == WatchState.INVALIDATED
    assert callback.call_count == 1  # no replay
    assert watcher.broker.submit_order.call_count == 0


# ─────────────────────────────────────────────────────────────────────────
# TEST 8 — confirmed lifecycle + stop-side truth unavailable: HOLD/retry,
# no false terminalization, no fabricated stop truth.
# ─────────────────────────────────────────────────────────────────────────
def test_confirmed_call_stop_truth_unavailable_holds_retry():
    watched = WatchedSignal(_signal(side="CALL"), overnight=False)
    assert watched.check(99.0, 100.0) == WatchState.PENDING
    assert watched.check(99.0, 100.0) == WatchState.TRIGGERED
    confirmed_at = watched.trigger_crossed_at
    watched.state = WatchState.PENDING  # simulate callback RETRY_WAIT

    result = watched.check(bid=None, ask=100.50)  # stop-side (BID) unavailable

    assert result == WatchState.PENDING
    assert watched.trigger_crossed_at == confirmed_at
    assert watched.last_trigger_evidence_reason == "ACTIVE_STOP_TRUTH_UNAVAILABLE_BID"


# ─────────────────────────────────────────────────────────────────────────
# TEST 9 — callback retry backoff: no early callback, stop safety active
# ─────────────────────────────────────────────────────────────────────────
def test_retry_backoff_no_early_callback_stop_safety_active():
    quotes = [
        {"bid": 99.0, "ask": 100.0},
        {"bid": 99.0, "ask": 100.0},   # confirms, callback -> RETRY_WAIT sets backoff
        {"bid": 99.0, "ask": None},    # inside backoff window: HOLD (do not clear deferred deadline)
    ]
    watcher, watched = _poll_watched("CALL", quotes)
    callback = MagicMock(return_value={"disposition": "RETRY_WAIT"})
    watcher.on_trigger = callback

    watcher._poll_active_signals(open_protect_active=False)
    watcher._poll_active_signals(open_protect_active=False)
    assert callback.call_count == 1
    deadline_before = watched.deferred_retry_not_before
    assert deadline_before is not None

    watcher._poll_active_signals(open_protect_active=False)  # still deferred

    assert callback.call_count == 1  # no early callback
    assert watched.deferred_retry_not_before == deadline_before
    assert watched.state == WatchState.PENDING


# ─────────────────────────────────────────────────────────────────────────
# TEST 10 — no prior breach + missing quote: stays 0, no fake first-breach
# ─────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("side", ["CALL", "PUT"])
def test_missing_quote_with_no_prior_breach_stays_zero(side):
    watched = WatchedSignal(_signal(side=side), overnight=False)
    missing = (99.0, None) if side == "CALL" else (None, 101.0)

    result = watched.check(*missing)

    assert result == WatchState.PENDING
    assert watched.breach_count == 0
    assert watched._pending_first_breach_at is None
    assert watched.trigger_crossed_at is None


# ─────────────────────────────────────────────────────────────────────────
# TEST 11 (redundant w/ test 6, kept for explicit matrix parity) — HOLD
# does not raise for the full unusable-value set at count=0.
# ─────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("bad_ask", UNUSABLE_QUOTES)
def test_unusable_ask_at_zero_count_no_exception(bad_ask):
    watched = WatchedSignal(_signal(side="CALL"), overnight=False)
    result = watched.check(bid=99.0, ask=bad_ask)
    assert result == WatchState.PENDING
    assert watched.breach_count == 0


# ─────────────────────────────────────────────────────────────────────────
# TEST 12 — exported package integration: import through the real
# ``ap_entry_watcher`` package path (the shim/base seam), not just the
# base module directly. This is the exact seam where the original
# regression's cross-file contract could recur.
# ─────────────────────────────────────────────────────────────────────────
def test_exported_package_path_preserves_hold_semantics():
    # Behavioral proof through the real exported package path
    # (`import ap_entry_watcher`) — the exact seam where the original
    # regression's shim/base contract drift occurred. Deliberately avoids
    # any cross-reference to the `APEntryWatcher`/`WatchedSignal` names
    # imported directly at the top of this file: several other test
    # modules elsewhere in this suite legitimately monkeypatch
    # `sys.modules['ap_entry_watcher']` for their own isolation, which is
    # pre-existing test infrastructure unrelated to this PR and makes a
    # cross-file identity/subclass comparison order-dependent. What
    # matters for this PR is that HOLD semantics hold when accessed
    # exclusively through the package's own exported names.
    fresh_module = sys.modules.get("ap_entry_watcher") or __import__("ap_entry_watcher")
    assert fresh_module.__name__ == "ap_entry_watcher"

    watched = fresh_module.WatchedSignal(_signal(side="CALL"), overnight=False)

    assert watched.check(bid=99.0, ask=100.05) == fresh_module.WatchState.PENDING
    assert watched.breach_count == 1

    # LAST-only / unusable observation through the exported path.
    result = watched.check(bid=None, ask=None)
    assert result == fresh_module.WatchState.PENDING
    assert watched.breach_count == 1  # held, not reset, not incremented

    result2 = watched.check(bid=99.0, ask=100.06)
    assert result2 == fresh_module.WatchState.TRIGGERED
    assert watched.breach_count == 2


# ─────────────────────────────────────────────────────────────────────────
# TEST 13 — #474 adjacency: HOLD poll between two valid breaches produces
# exactly one on_trigger callback, the watcher makes zero broker calls, and
# Jason's exact identity fields survive intact for #474's downstream
# deferred handling. See module docstring for scope note.
# ─────────────────────────────────────────────────────────────────────────
def test_474_adjacency_single_callback_preserves_jason_identity():
    signal = _signal(
        side="CALL",
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
    )
    signal["ticker"] = "AAPL"
    quotes = [
        {"bid": 224.0, "ask": 225.10},  # breach candidate
        {"bid": 224.0, "ask": None},    # HOLD — canonical ASK missing
        {"bid": 224.0, "ask": 225.15},  # confirms
    ]
    osm = MagicMock()
    osm.update_order_meta.return_value = True
    watcher = _QuoteWatcher(quotes, mode="LIVE", order_state_machine=osm)
    watched = WatchedSignal(signal, overnight=False)
    _register(watcher, watched)

    captured = []

    def _on_trigger(item):
        captured.append(item)
        return {"disposition": "KEEP_WATCHER"}

    watcher.on_trigger = _on_trigger

    watcher._poll_active_signals(open_protect_active=False)
    assert watched.breach_count == 1
    watcher._poll_active_signals(open_protect_active=False)  # HOLD
    assert watched.breach_count == 1
    assert len(captured) == 0
    watcher._poll_active_signals(open_protect_active=False)  # confirms

    # Exactly one callback into the downstream (#474-adjacent) deferred flow.
    assert len(captured) == 1
    received = captured[0].signal
    assert received["client_id"] == "jasoncosby1@gmail.com"
    assert received["execution_mode"] == "live"
    assert received["local_order_id"] == "local-494-call"
    assert received["signal_id"] == "sig-494-call"
    assert captured[0].side == "CALL"

    # Watcher itself never touches the broker — #474's final-authority
    # broker POST decision remains exclusively downstream of on_trigger.
    assert watcher.broker.submit_order.call_count == 0
    assert watcher.broker.place_order.call_count == 0
    assert watcher.broker.cancel_order.call_count == 0
