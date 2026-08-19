"""PR #494 final merge-gate amendment — bounded missing-observation HOLD.

The original #494 implementation (see
tests/test_p0_watcher_missing_observation_hold.py) fixed an unbounded-HOLD
regression: a missing/unusable canonical quote observation no longer reset
pre-confirmation breach continuity to zero. That was directionally correct
but too broad — it let a valid first breach remain eligible to combine with
a later valid breach across an UNBOUNDED period of unknown market truth,
which is not acceptable for LIVE entry confirmation.

This file proves the bounded-continuity model that supersedes the
unbounded HOLD:

    VALID AUTHORITATIVE BREACH
    -> if prior partial continuity is fresh (elapsed <= MAX_GAP since the
       last valid breach observation): increment, continuing the streak.
    -> if prior partial continuity is stale (elapsed > MAX_GAP, or no
       anchor recorded): discard it FIRST, then treat the current valid
       breach as a brand-new first observation.

    VALID AUTHORITATIVE CONTRADICTION / NON-BREACH
    -> reset immediately. No grace period, no bounded-gap consideration.

    NO USABLE REQUIRED-SIDE OBSERVATION
    -> if partial breach exists and is fresh: HOLD.
    -> if partial breach exists and is stale: reset (stay PENDING).
    -> if no partial breach exists: remain PENDING, nothing to do.

#479 remains authoritative for evidence QUALITY (CALL=ASK / PUT=BID only,
no LAST/MID/MARK/opposite-side substitution, no fabricated confirmation).
#494 (both the original HOLD fix and this bounded-gap amendment) changes
ONLY pre-confirmation missing-observation CONTINUITY semantics. Neither
changes evidence authority.

Deterministic clock control uses WatchedSignal._get_watcher_now(), a
narrow instance-level seam added specifically because patching the
module-level `datetime` name proved unreliable in this environment (see
amendment section 11, which explicitly permits exactly this kind of
seam). No time.sleep() is used anywhere in this file.
"""
from __future__ import annotations

import datetime as _dt
from unittest.mock import MagicMock, patch

import pytest

import ap_entry_watcher
from ap_entry_watcher import (
    APEntryWatcher,
    WATCHER_BREACH_CONTINUITY_MAX_GAP_SEC as MAX_GAP,
    WatchState,
    WatchedSignal,
)


T0 = _dt.datetime(2026, 1, 1, 12, 0, 0, tzinfo=_dt.timezone.utc)


def _signal(*, side: str, client_id: str = "client@example.com",
            execution_mode: str = "paper", entry_price: float = 100.0) -> dict:
    return {
        "ticker": "SPY",
        "side": side,
        "entry_price": entry_price,
        "stop_price": 95.0 if side == "CALL" else 105.0,
        "target_price": 105.0 if side == "CALL" else 95.0,
        "signal_id": f"sig-bg-{side.lower()}",
        "canonical_signal_id": f"canon-bg-{side.lower()}",
        "local_order_id": f"local-bg-{side.lower()}",
        "client_id": client_id,
        "execution_mode": execution_mode,
    }


def _at(watched: WatchedSignal, t: _dt.datetime):
    """Context manager: freeze watched._get_watcher_now() at exactly t."""
    return patch.object(watched, "_get_watcher_now", return_value=t)


def _check_at(watched: WatchedSignal, t: _dt.datetime, bid, ask):
    with _at(watched, t):
        return watched.check(bid=bid, ask=ask)


# ─────────────────────────────────────────────────────────────────────────
# TEST A — brief CALL outage holds and confirms within the bounded gap
# TEST B — brief PUT outage holds and confirms within the bounded gap
# ─────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("side", "breach_quote", "missing_quote", "confirm_quote"),
    [
        ("CALL", (99.0, 100.05), (99.0, None), (99.0, 100.06)),
        ("PUT", (100.0, 101.0), (None, 101.0), (99.94, 101.0)),
    ],
)
def test_A_B_brief_outage_holds_and_confirms_within_gap(
    side, breach_quote, missing_quote, confirm_quote
):
    watched = WatchedSignal(_signal(side=side), overnight=False)

    r1 = _check_at(watched, T0, *breach_quote)
    assert r1 == WatchState.PENDING
    assert watched.breach_count == 1

    r2 = _check_at(watched, T0 + _dt.timedelta(seconds=15), *missing_quote)
    assert r2 == WatchState.PENDING
    assert watched.breach_count == 1  # HOLD

    r3 = _check_at(watched, T0 + _dt.timedelta(seconds=30), *confirm_quote)
    assert r3 == WatchState.TRIGGERED
    assert watched.breach_count == 2
    assert watched.trigger_crossed_at is not None


# ─────────────────────────────────────────────────────────────────────────
# TEST C — prolonged CALL outage expires; later breach is a NEW first
# TEST D — prolonged PUT outage expires; later breach is a NEW first
# ─────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("side", "breach1", "missing", "breach2", "breach3"),
    [
        ("CALL", (99.0, 100.05), (99.0, None), (99.0, 100.06), (99.0, 100.07)),
        ("PUT", (100.0, 101.0), (None, 101.0), (99.94, 101.0), (99.93, 101.0)),
    ],
)
def test_C_D_prolonged_outage_expires_new_first_breach_then_confirms(
    side, breach1, missing, breach2, breach3
):
    watched = WatchedSignal(_signal(side=side), overnight=False)

    r1 = _check_at(watched, T0, *breach1)
    assert r1 == WatchState.PENDING
    assert watched.breach_count == 1

    # Missing observation arrives AFTER the bounded gap has elapsed.
    t_stale = T0 + _dt.timedelta(seconds=MAX_GAP + 5)
    r2 = _check_at(watched, t_stale, *missing)
    assert r2 == WatchState.PENDING
    assert watched.breach_count == 0  # stale partial continuity RESET

    r3 = _check_at(watched, t_stale + _dt.timedelta(seconds=1), *breach2)
    assert r3 == WatchState.PENDING
    assert watched.breach_count == 1  # NEW first observation
    assert watched.trigger_crossed_at is None

    r4 = _check_at(watched, t_stale + _dt.timedelta(seconds=16), *breach3)
    assert r4 == WatchState.TRIGGERED
    assert watched.breach_count == 2


# ─────────────────────────────────────────────────────────────────────────
# TEST E — several missing polls inside total gap: count stays 1, timer
# stays anchored to the ORIGINAL first valid breach, no fabrication.
# ─────────────────────────────────────────────────────────────────────────
def test_E_several_missing_polls_inside_total_gap_hold():
    watched = WatchedSignal(_signal(side="CALL"), overnight=False)

    _check_at(watched, T0, 99.0, 100.05)
    assert watched.breach_count == 1
    anchor = watched._last_valid_breach_observation_at
    assert anchor == T0

    for offset in (10, 20, 30, 40):
        r = _check_at(watched, T0 + _dt.timedelta(seconds=offset), 99.0, None)
        assert r == WatchState.PENDING
        assert watched.breach_count == 1
        assert watched._last_valid_breach_observation_at == anchor  # unchanged
        assert watched.trigger_crossed_at is None

    # Still within 45s of the ORIGINAL breach (40s elapsed) -> may still confirm.
    r_final = _check_at(watched, T0 + _dt.timedelta(seconds=44), 99.0, 100.06)
    assert r_final == WatchState.TRIGGERED
    assert watched.breach_count == 2


# ─────────────────────────────────────────────────────────────────────────
# TEST F — time, not poll count, controls expiry: a single missing poll
# arriving after the gap still expires continuity.
# ─────────────────────────────────────────────────────────────────────────
def test_F_time_not_poll_count_controls_expiry():
    watched = WatchedSignal(_signal(side="CALL"), overnight=False)

    _check_at(watched, T0, 99.0, 100.05)
    assert watched.breach_count == 1

    # Only ONE missing poll, but it arrives 50s later (> 45s max).
    r = _check_at(watched, T0 + _dt.timedelta(seconds=50), 99.0, None)
    assert r == WatchState.PENDING
    assert watched.breach_count == 0  # expired despite only one missing poll


# ─────────────────────────────────────────────────────────────────────────
# TEST G (MANDATORY) — delayed valid poll cannot revive stale continuity,
# even with ZERO intermediate missing polls processed.
# ─────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("side", ["CALL", "PUT"])
def test_G_delayed_valid_poll_cannot_revive_stale_continuity(side):
    watched = WatchedSignal(_signal(side=side), overnight=False)
    breach_quote = (99.0, 100.05) if side == "CALL" else (100.0, 101.0)
    later_breach_quote = (99.0, 100.06) if side == "CALL" else (99.94, 101.0)

    r1 = _check_at(watched, T0, *breach_quote)
    assert r1 == WatchState.PENDING
    assert watched.breach_count == 1

    # No missing poll processed at all — the VERY NEXT observation is
    # itself a valid breach, but it arrives well past the bounded gap.
    t2 = T0 + _dt.timedelta(seconds=MAX_GAP + 10)
    r2 = _check_at(watched, t2, *later_breach_quote)

    assert watched.breach_count == 1
    assert r2 == WatchState.PENDING
    assert watched._pending_first_breach_at == t2
    assert watched._last_valid_breach_observation_at == t2
    assert watched.trigger_crossed_at is None


# ─────────────────────────────────────────────────────────────────────────
# TEST H — valid contradiction resets immediately, even mid-HOLD, no
# grace period whatsoever.
# ─────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("side", "breach", "missing", "contradiction"),
    [
        ("CALL", (99.0, 100.10), (99.0, None), (99.0, 99.80)),
        ("PUT", (100.0, 101.0), (None, 101.0), (100.20, 101.0)),
    ],
)
def test_H_valid_contradiction_resets_immediately(side, breach, missing, contradiction):
    watched = WatchedSignal(_signal(side=side), overnight=False)

    _check_at(watched, T0, *breach)
    assert watched.breach_count == 1

    _check_at(watched, T0 + _dt.timedelta(seconds=5), *missing)
    assert watched.breach_count == 1  # HOLD

    r = _check_at(watched, T0 + _dt.timedelta(seconds=10), *contradiction)
    assert r == WatchState.PENDING
    assert watched.breach_count == 0
    # Note: the pre-existing contradiction-reset branch (unchanged by this
    # amendment) only ever clears breach_count itself — _pending_first_breach_at
    # / breach_price / first_breach_bid / first_breach_ask are left stale but
    # inert, since they get freshly overwritten on the next valid breach (see
    # `if self.breach_count == 0:` in the CALL/PUT blocks). This amendment
    # adds only the new continuity-anchor clear to that existing narrow scope
    # (amendment section 9: "preserve/reset the rest of partial evidence
    # consistently with the existing semantics").
    assert watched._last_valid_breach_observation_at is None
    assert watched.trigger_crossed_at is None


# ─────────────────────────────────────────────────────────────────────────
# TEST I — no prior breach + missing observation: stays 0, no fabrication.
# ─────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("side", ["CALL", "PUT"])
def test_I_no_prior_breach_missing_stays_zero(side):
    watched = WatchedSignal(_signal(side=side), overnight=False)
    missing = (99.0, None) if side == "CALL" else (None, 101.0)

    r = _check_at(watched, T0, *missing)

    assert r == WatchState.PENDING
    assert watched.breach_count == 0
    assert watched._pending_first_breach_at is None
    assert watched._last_valid_breach_observation_at is None
    assert watched.trigger_crossed_at is None


# ─────────────────────────────────────────────────────────────────────────
# TEST J — LAST-only observation with a partial breach already pending:
# HOLD within the gap, RESET beyond it; LAST still never confirms either way.
# ─────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("side", ["CALL", "PUT"])
def test_J_last_only_with_partial_breach_hold_then_expire(side):
    watched = WatchedSignal(_signal(side=side), overnight=False)
    breach = (99.0, 100.05) if side == "CALL" else (100.0, 101.0)

    _check_at(watched, T0, *breach)
    assert watched.breach_count == 1

    # LAST crosses trigger but canonical bid/ask both unavailable — inside gap.
    r_inside = _check_at(watched, T0 + _dt.timedelta(seconds=20), None, None)
    assert r_inside == WatchState.PENDING
    assert watched.breach_count == 1  # HOLD only, no increment
    assert watched.trigger_crossed_at is None

    # Same LAST-only shape, now beyond the gap.
    r_outside = _check_at(watched, T0 + _dt.timedelta(seconds=MAX_GAP + 10), None, None)
    assert r_outside == WatchState.PENDING
    assert watched.breach_count == 0  # RESET — stale
    assert watched.trigger_crossed_at is None


# ─────────────────────────────────────────────────────────────────────────
# TEST K — exact boundary: elapsed == MAX_GAP is fresh; MAX_GAP + epsilon
# is stale. Deterministic, no implicit equality behavior.
# ─────────────────────────────────────────────────────────────────────────
def test_K_exact_boundary_fresh_vs_stale():
    # Exactly at MAX_GAP -> still eligible (fresh).
    w_fresh = WatchedSignal(_signal(side="CALL"), overnight=False)
    _check_at(w_fresh, T0, 99.0, 100.05)
    r_fresh = _check_at(w_fresh, T0 + _dt.timedelta(seconds=MAX_GAP), 99.0, 100.06)
    assert r_fresh == WatchState.TRIGGERED
    assert w_fresh.breach_count == 2

    # MAX_GAP + smallest positive delta -> stale.
    w_stale = WatchedSignal(_signal(side="CALL"), overnight=False)
    _check_at(w_stale, T0, 99.0, 100.05)
    r_stale = _check_at(
        w_stale, T0 + _dt.timedelta(seconds=MAX_GAP, milliseconds=1), 99.0, 100.06
    )
    assert r_stale == WatchState.PENDING
    assert w_stale.breach_count == 1  # new first breach, not confirmation
    assert w_stale.trigger_crossed_at is None


# ─────────────────────────────────────────────────────────────────────────
# TEST L — confirmed lifecycle is unaffected by the bounded-gap timer even
# across a span far exceeding MAX_GAP.
# ─────────────────────────────────────────────────────────────────────────
def test_L_confirmed_lifecycle_unaffected_by_continuity_timer():
    watched = WatchedSignal(_signal(side="CALL"), overnight=False)
    _check_at(watched, T0, 99.0, 100.0)
    r_confirm = _check_at(watched, T0 + _dt.timedelta(seconds=15), 99.0, 100.0)
    assert r_confirm == WatchState.TRIGGERED
    confirmed_at = watched.trigger_crossed_at
    assert confirmed_at is not None

    # Callback-driven retry returns watcher to PENDING (simulated).
    watched.state = WatchState.PENDING

    # Advance far beyond MAX_GAP; entry-side truth missing, stop-side intact.
    t_far = T0 + _dt.timedelta(seconds=MAX_GAP * 5)
    result = _check_at(watched, t_far, 99.0, None)  # ASK missing, BID intact (stop=95)

    assert result == WatchState.PENDING
    assert watched.trigger_crossed_at == confirmed_at  # untouched
    assert watched.state == WatchState.PENDING


# ─────────────────────────────────────────────────────────────────────────
# TEST M — confirmed stop-break still invalidates regardless of the timer.
# ─────────────────────────────────────────────────────────────────────────
def test_M_confirmed_stop_break_still_invalidates():
    watched = WatchedSignal(_signal(side="CALL"), overnight=False)
    _check_at(watched, T0, 99.0, 100.0)
    _check_at(watched, T0 + _dt.timedelta(seconds=15), 99.0, 100.0)
    assert watched.state == WatchState.TRIGGERED
    confirmed_at = watched.trigger_crossed_at
    watched.state = WatchState.PENDING

    t_far = T0 + _dt.timedelta(seconds=MAX_GAP * 5)
    # ASK missing (entry-side), BID proves stop (95.0) broken.
    result = _check_at(watched, t_far, 10.0, None)

    assert result == WatchState.INVALIDATED
    assert watched.trigger_crossed_at == confirmed_at


# ─────────────────────────────────────────────────────────────────────────
# TEST N — retry backoff remains active; no early/duplicate callback even
# across a span exceeding MAX_GAP.
# ─────────────────────────────────────────────────────────────────────────
def test_N_retry_backoff_remains_active_across_long_gap():
    osm = MagicMock()
    osm.update_order_meta.return_value = True
    watcher = APEntryWatcher(
        MagicMock(), order_state_machine=osm, require_on_trigger=False, mode="PAPER"
    )
    watched = WatchedSignal(_signal(side="CALL"), overnight=False)
    watched._watcher_ref = watcher
    watcher._pending.append(watched)
    watcher._dedup_set.add(watched.signal_id)
    watcher._persist_watcher_audit = MagicMock()

    callback = MagicMock(return_value={"disposition": "RETRY_WAIT"})
    watcher.on_trigger = callback

    quotes = iter(
        [
            {"bid": 99.0, "ask": 100.0},
            {"bid": 99.0, "ask": 100.0},
        ]
    )
    watcher._fetch_quotes = lambda tickers: {"SPY": next(quotes)}

    with patch.object(watched, "_get_watcher_now", return_value=T0):
        watcher._poll_active_signals(open_protect_active=False)
    with patch.object(
        watched, "_get_watcher_now", return_value=T0 + _dt.timedelta(seconds=15)
    ):
        watcher._poll_active_signals(open_protect_active=False)

    assert callback.call_count == 1
    deadline_before = watched.deferred_retry_not_before
    assert deadline_before is not None

    # Advance far beyond MAX_GAP while still inside the retry backoff window.
    quotes2 = iter([{"bid": 99.0, "ask": 100.0}])
    watcher._fetch_quotes = lambda tickers: {"SPY": next(quotes2)}
    t_far = T0 + _dt.timedelta(seconds=MAX_GAP * 5)
    with patch.object(watched, "_get_watcher_now", return_value=t_far):
        watcher._poll_active_signals(open_protect_active=False)

    assert callback.call_count == 1  # no early/duplicate callback
    assert watched.deferred_retry_not_before == deadline_before
    assert watched.state == WatchState.PENDING


# ─────────────────────────────────────────────────────────────────────────
# TEST O — exported package/shim path: bounded-gap semantics through the
# real `import ap_entry_watcher` entrypoint, both short-gap-confirms and
# long-gap-expires.
# ─────────────────────────────────────────────────────────────────────────
def test_O_exported_package_path_short_gap_and_long_gap():
    fresh_module = ap_entry_watcher

    # Short gap -> HOLD then confirm.
    w1 = fresh_module.WatchedSignal(_signal(side="CALL"), overnight=False)
    with patch.object(w1, "_get_watcher_now", return_value=T0):
        assert w1.check(bid=99.0, ask=100.05) == fresh_module.WatchState.PENDING
    with patch.object(
        w1, "_get_watcher_now", return_value=T0 + _dt.timedelta(seconds=20)
    ):
        assert w1.check(bid=99.0, ask=None) == fresh_module.WatchState.PENDING
        assert w1.breach_count == 1
    with patch.object(
        w1, "_get_watcher_now", return_value=T0 + _dt.timedelta(seconds=30)
    ):
        r = w1.check(bid=99.0, ask=100.06)
    assert r == fresh_module.WatchState.TRIGGERED
    assert w1.breach_count == 2

    # Long gap -> stale continuity reset, new first breach.
    w2 = fresh_module.WatchedSignal(_signal(side="CALL"), overnight=False)
    with patch.object(w2, "_get_watcher_now", return_value=T0):
        w2.check(bid=99.0, ask=100.05)
    assert w2.breach_count == 1
    with patch.object(
        w2, "_get_watcher_now", return_value=T0 + _dt.timedelta(seconds=MAX_GAP + 5)
    ):
        r2 = w2.check(bid=99.0, ask=100.06)
    assert r2 == fresh_module.WatchState.PENDING
    assert w2.breach_count == 1  # new first breach, not confirmation
    assert w2.trigger_crossed_at is None


# ─────────────────────────────────────────────────────────────────────────
# TEST P — #474 Jason-shaped adjacency: short gap confirms with exactly one
# callback and preserved identity; long gap does not prematurely confirm.
# Per the amendment, this does not duplicate #474's own economics suite —
# it proves the narrower watcher-side non-bypass/identity claim, matching
# the scope of the equivalent test in test_p0_watcher_missing_observation_hold.py.
# ─────────────────────────────────────────────────────────────────────────
def test_P_474_jason_adjacency_short_gap_single_callback_identity_preserved():
    signal = _signal(side="CALL", client_id="jasoncosby1@gmail.com", execution_mode="live")
    signal["ticker"] = "AAPL"

    osm = MagicMock()
    osm.update_order_meta.return_value = True
    watcher = APEntryWatcher(
        MagicMock(), order_state_machine=osm, require_on_trigger=False, mode="LIVE"
    )
    watched = WatchedSignal(signal, overnight=False)
    watched._watcher_ref = watcher
    watcher._pending.append(watched)
    watcher._dedup_set.add(watched.signal_id)
    watcher._persist_watcher_audit = MagicMock()

    captured = []

    def _on_trigger(item):
        captured.append(item)
        return {"disposition": "KEEP_WATCHER"}

    watcher.on_trigger = _on_trigger

    quotes = iter(
        [
            {"bid": 224.0, "ask": 225.10},
            {"bid": 224.0, "ask": None},
            {"bid": 224.0, "ask": 225.15},
        ]
    )
    watcher._fetch_quotes = lambda tickers: {tickers[0]: next(quotes)}

    with patch.object(watched, "_get_watcher_now", return_value=T0):
        watcher._poll_active_signals(open_protect_active=False)
    assert watched.breach_count == 1
    with patch.object(
        watched, "_get_watcher_now", return_value=T0 + _dt.timedelta(seconds=15)
    ):
        watcher._poll_active_signals(open_protect_active=False)  # HOLD
    assert watched.breach_count == 1
    assert len(captured) == 0
    with patch.object(
        watched, "_get_watcher_now", return_value=T0 + _dt.timedelta(seconds=30)
    ):
        watcher._poll_active_signals(open_protect_active=False)  # confirms

    assert len(captured) == 1
    received = captured[0].signal
    assert received["client_id"] == "jasoncosby1@gmail.com"
    assert received["execution_mode"] == "live"
    assert received["local_order_id"] == "local-bg-call"
    assert received["signal_id"] == "sig-bg-call"
    assert watcher.broker.submit_order.call_count == 0
    assert watcher.broker.place_order.call_count == 0
    assert watcher.broker.cancel_order.call_count == 0


def test_P_474_jason_adjacency_long_gap_no_premature_confirm():
    signal = _signal(side="CALL", client_id="jasoncosby1@gmail.com", execution_mode="live")
    signal["ticker"] = "AAPL"

    osm = MagicMock()
    osm.update_order_meta.return_value = True
    watcher = APEntryWatcher(
        MagicMock(), order_state_machine=osm, require_on_trigger=False, mode="LIVE"
    )
    watched = WatchedSignal(signal, overnight=False)
    watched._watcher_ref = watcher
    watcher._pending.append(watched)
    watcher._dedup_set.add(watched.signal_id)
    watcher._persist_watcher_audit = MagicMock()

    captured = []
    watcher.on_trigger = lambda item: captured.append(item) or {"disposition": "KEEP_WATCHER"}

    quotes = iter(
        [
            {"bid": 224.0, "ask": 225.10},
            {"bid": 224.0, "ask": 225.12},  # second valid breach, but LONG gap later
        ]
    )
    watcher._fetch_quotes = lambda tickers: {tickers[0]: next(quotes)}

    with patch.object(watched, "_get_watcher_now", return_value=T0):
        watcher._poll_active_signals(open_protect_active=False)
    assert watched.breach_count == 1

    t_far = T0 + _dt.timedelta(seconds=MAX_GAP + 20)
    with patch.object(watched, "_get_watcher_now", return_value=t_far):
        watcher._poll_active_signals(open_protect_active=False)

    assert len(captured) == 0  # must not have confirmed
    assert watched.breach_count == 1  # new first breach, not 2
    assert watched.trigger_crossed_at is None

    # A further timely valid breach may now confirm exactly once.
    quotes2 = iter([{"bid": 224.0, "ask": 225.14}])
    watcher._fetch_quotes = lambda tickers: {tickers[0]: next(quotes2)}
    with patch.object(
        watched, "_get_watcher_now", return_value=t_far + _dt.timedelta(seconds=15)
    ):
        watcher._poll_active_signals(open_protect_active=False)

    assert len(captured) == 1
    assert captured[0].signal["client_id"] == "jasoncosby1@gmail.com"


# ─────────────────────────────────────────────────────────────────────────
# TEST Q — invalid/unavailable entry_trigger does not HOLD partial breach
# forever either; the bounded expiry rule applies uniformly since both
# "unusable required quote" and "unusable entry_trigger" funnel through
# the same _entry_trigger_evidence_unavailable branch. entry_trigger is
# validated once at construction and has no production setter, so this
# test constructs a normal watcher, drives it to a genuine partial breach
# through the public check() API, and only then directly mutates
# entry_trigger to simulate the (not otherwise producible) invalid-trigger
# case, per amendment section 12's guidance to avoid damaging constructor
# invariants while still proving the bounded rule applies.
# ─────────────────────────────────────────────────────────────────────────
def test_Q_invalid_entry_trigger_partial_breach_still_bounded():
    watched = WatchedSignal(_signal(side="CALL"), overnight=False)
    _check_at(watched, T0, 99.0, 100.05)
    assert watched.breach_count == 1

    # Simulate entry_trigger becoming unusable mid-lifecycle (not a
    # realistically producible runtime state — entry_trigger has no
    # production setter — but the amendment requires proving the bounded
    # rule applies uniformly if this ever happened).
    watched.entry_trigger = float("nan")

    # Within the gap -> HOLD as UNKNOWN.
    r_inside = _check_at(watched, T0 + _dt.timedelta(seconds=20), 99.0, 100.10)
    assert r_inside == WatchState.PENDING
    assert watched.breach_count == 1
    assert watched.trigger_crossed_at is None

    # Beyond the gap -> partial continuity must reset; still no fabrication.
    r_outside = _check_at(
        watched, T0 + _dt.timedelta(seconds=MAX_GAP + 5), 99.0, 100.10
    )
    assert r_outside == WatchState.PENDING
    assert watched.breach_count == 0
    assert watched.trigger_crossed_at is None


# ─────────────────────────────────────────────────────────────────────────
# Preserved-invariant proofs (amendment section 19) — evidence authority
# is unchanged by the bounded-gap timer.
# ─────────────────────────────────────────────────────────────────────────
def test_call_remains_ask_authoritative_regardless_of_gap():
    watched = WatchedSignal(_signal(side="CALL"), overnight=False)
    # BID crossing alone (ASK missing) must never breach, gap or no gap.
    r = _check_at(watched, T0, 105.0, None)
    assert r == WatchState.PENDING
    assert watched.breach_count == 0


def test_put_remains_bid_authoritative_regardless_of_gap():
    watched = WatchedSignal(_signal(side="PUT"), overnight=False)
    r = _check_at(watched, T0, None, 90.0)
    assert r == WatchState.PENDING
    assert watched.breach_count == 0


def test_momentum_polls_required_unchanged():
    assert WatchedSignal.MOMENTUM_POLLS_REQUIRED == 2


def test_continuity_max_gap_default_is_45_seconds():
    assert MAX_GAP == 45
