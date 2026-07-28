from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import ap.position_quote_monitor as qpm_module
from ap import touched_profit_confirmation_guard
from ap.position_quote_monitor import APPositionQuoteMonitor


OPTION = "SPY260731C00550000"
UNDERLYING = "SPY"
POSITION_ID = "position-387b"
CLIENT_ID = "jason@example.com"


def _position(
    *,
    mode="live",
    position_id=POSITION_ID,
    option_symbol=OPTION,
    underlying=UNDERLYING,
):
    old = datetime.now(timezone.utc) - timedelta(minutes=5)
    return SimpleNamespace(
        position_id=position_id,
        positionid=position_id,
        execution_mode=mode,
        executionmode=mode,
        ticker=underlying,
        underlying=underlying,
        option_symbol=option_symbol,
        optionsymbol=option_symbol,
        quantity_remaining=1,
        quantityremaining=1,
        closed=False,
        entry_price=0.0,
        entryprice=0.0,
        current_option_price=0.0,
        currentoptionprice=0.0,
        current_underlying=0.0,
        currentunderlying=0.0,
        current_bid=0.0,
        currentbid=0.0,
        current_ask=0.0,
        currentask=0.0,
        last_option_quote_update_ts=old,
        lastoptionquoteupdatets=old,
        last_underlying_quote_update_ts=old,
        lastunderlyingquoteupdatets=old,
        touched_profit=False,
        touchedprofit=False,
    )


class _ExitEngine:
    def __init__(self, positions):
        self.positions = positions
        self._lock = threading.Lock()
        self.quote_arrived_event = threading.Event()
        self._positions_by_id = {
            position.position_id: position
            for position in positions
            if position.position_id
        }

    def active_positions(self):
        return [
            position
            for position in self.positions
            if not position.closed and position.quantity_remaining > 0
        ]


class _RecoveryBroker:
    def __init__(
        self, *, provider_ts=None, recover=True, during_recovery=None
    ):
        self.provider_ts = provider_ts or time.time()
        self.recover = recover
        self.during_recovery = during_recovery
        self.calls = []
        self.submit_order = MagicMock()
        self.cancel_order = MagicMock()

    def get_quotes(self, symbols):
        symbols = list(symbols)
        self.calls.append(symbols)
        if not self.recover or len(symbols) != 2:
            return {}
        if self.during_recovery is not None:
            self.during_recovery()
        return {
            OPTION: {
                "symbol": OPTION,
                "bid": 1.00,
                "ask": 1.10,
                "last": 1.05,
                "bid_date": self.provider_ts,
                "trade_date": self.provider_ts,
            },
            UNDERLYING: {
                "symbol": UNDERLYING,
                "last": 550.0,
                "trade_date": self.provider_ts,
            },
        }


class _StaleOrdinaryBidBroker(_RecoveryBroker):
    def __init__(self, *, provider_ts):
        super().__init__(provider_ts=provider_ts, recover=False)

    def get_quotes(self, symbols):
        symbols = list(symbols)
        self.calls.append(symbols)
        if len(symbols) == 2:
            return {}
        if symbols == [OPTION]:
            return {
                OPTION: {
                    "symbol": OPTION,
                    "bid": 0.25,
                    "ask": 0.90,
                    "mark": 0.80,
                    "last": 0.85,
                    "bid_date": self.provider_ts,
                    "ask_date": self.provider_ts,
                    "trade_date": self.provider_ts,
                }
            }
        if symbols == [UNDERLYING]:
            return {
                UNDERLYING: {
                    "symbol": UNDERLYING,
                    "last": 550.0,
                    "trade_date": time.time(),
                }
            }
        return {}


class _StalerProviderRecoveryBroker:
    """
    Ordinary poll (single-symbol batch): returns empty so the option bid is
    absent and _ordinary_quote_is_fresh returns False, triggering recovery.

    Recovery fetch (two-symbol batch [UNDERLYING, OPTION]): returns provider
    truth that passes _quote_has_fresh_provider_truth (bid > 0, within
    STALE_MAX_SEC) but with timestamps that can be configured to be older
    than the position's existing observation timestamps, exercising the
    value-cohesion gate added in PR #400.

    option_bid defaults to 0.70 to make regression obvious if the gated
    write fires unexpectedly.
    """

    def __init__(
        self,
        *,
        option_provider_ts: float,
        underlying_provider_ts: float,
        option_bid: float = 0.70,
        underlying_last: float = 545.0,
    ):
        self.option_provider_ts = option_provider_ts
        self.underlying_provider_ts = underlying_provider_ts
        self.option_bid = option_bid
        self.underlying_last = underlying_last
        self.calls = []
        self.submit_order = MagicMock()
        self.cancel_order = MagicMock()

    def get_quotes(self, symbols):
        symbols = list(symbols)
        self.calls.append(symbols)
        if len(symbols) != 2:
            return {}
        return {
            OPTION: {
                "symbol": OPTION,
                "bid": self.option_bid,
                "ask": self.option_bid + 0.10,
                "last": self.option_bid + 0.05,
                "bid_date": self.option_provider_ts,
                "trade_date": self.option_provider_ts,
            },
            UNDERLYING: {
                "symbol": UNDERLYING,
                "last": self.underlying_last,
                "trade_date": self.underlying_provider_ts,
            },
        }


def _monitor(position, broker):
    engine = _ExitEngine([position])
    monitor = APPositionQuoteMonitor(
        broker=broker,
        client_id=CLIENT_ID,
        exit_engine=engine,
        poll_interval_sec=0.01,
    )
    monitor._persist_quote_to_db = MagicMock(return_value=False)
    monitor._persist_mfe_mae_to_orders = MagicMock(return_value=False)
    monitor._mark_mfe_mae_unavailable = MagicMock(return_value=False)
    return monitor, engine


class _PartialOrdinaryBroker:
    """
    Returns fresh ordinary data for ONE component (option OR underlying) and
    empty for the other, so exactly one component's ordinary quote is fresh
    and the other triggers recovery. The recovery two-symbol batch returns
    configurable provider truth for BOTH components regardless, so the tests
    can verify that fresh ordinary sources remain authoritative and are not
    displaced by the recovery payload.

    Ordinary single-symbol polls carry provider timestamps at now()
    (definitively fresh).  Recovery batch timestamps are configurable so
    tests can also exercise strictly-newer / older / equal-vs-existing gate
    cases at the same time.
    """

    def __init__(
        self,
        *,
        ordinary_option_fresh: bool,
        ordinary_underlying_fresh: bool,
        ordinary_option_bid: float = 1.05,
        ordinary_underlying_last: float = 555.0,
        recovery_option_bid: float = 0.70,
        recovery_underlying_last: float = 545.0,
        recovery_option_provider_ts=None,
        recovery_underlying_provider_ts=None,
    ):
        now = time.time()
        self.ordinary_option_fresh = ordinary_option_fresh
        self.ordinary_underlying_fresh = ordinary_underlying_fresh
        self.ordinary_option_bid = ordinary_option_bid
        self.ordinary_underlying_last = ordinary_underlying_last
        self.recovery_option_bid = recovery_option_bid
        self.recovery_underlying_last = recovery_underlying_last
        self.recovery_option_provider_ts = (
            recovery_option_provider_ts
            if recovery_option_provider_ts is not None
            else now - 2.0
        )
        self.recovery_underlying_provider_ts = (
            recovery_underlying_provider_ts
            if recovery_underlying_provider_ts is not None
            else now - 2.0
        )
        self.calls = []
        self.submit_order = MagicMock()
        self.cancel_order = MagicMock()

    def get_quotes(self, symbols):
        symbols = list(symbols)
        self.calls.append(symbols)
        now = time.time()
        if len(symbols) == 2:
            # Recovery batch
            return {
                OPTION: {
                    "symbol": OPTION,
                    "bid": self.recovery_option_bid,
                    "ask": self.recovery_option_bid + 0.10,
                    "last": self.recovery_option_bid + 0.05,
                    "bid_date": self.recovery_option_provider_ts,
                    "trade_date": self.recovery_option_provider_ts,
                },
                UNDERLYING: {
                    "symbol": UNDERLYING,
                    "last": self.recovery_underlying_last,
                    "trade_date": self.recovery_underlying_provider_ts,
                },
            }
        if symbols == [OPTION]:
            if self.ordinary_option_fresh:
                return {
                    OPTION: {
                        "symbol": OPTION,
                        "bid": self.ordinary_option_bid,
                        "ask": self.ordinary_option_bid + 0.10,
                        "last": self.ordinary_option_bid + 0.05,
                        "bid_date": now,
                        "trade_date": now,
                    }
                }
            return {}
        if symbols == [UNDERLYING]:
            if self.ordinary_underlying_fresh:
                return {
                    UNDERLYING: {
                        "symbol": UNDERLYING,
                        "last": self.ordinary_underlying_last,
                        "trade_date": now,
                    }
                }
            return {}
        return {}


def test_missing_live_quote_gets_one_uncached_fetch_and_exact_position_apply():
    position = _position()
    broker = _RecoveryBroker()
    monitor, engine = _monitor(position, broker)
    original_position = position

    monitor._refresh_once()

    assert monitor._metrics["direct_recovery_attempts"] == 1
    assert monitor._metrics["direct_recovery_successes"] == 1
    assert broker.calls[-1] == [UNDERLYING, OPTION]
    assert engine.active_positions()[0] is original_position
    assert position.last_option_provider_quote_ts is not None
    assert position.last_underlying_provider_quote_ts is not None
    assert engine.quote_arrived_event.is_set()
    broker.submit_order.assert_not_called()
    broker.cancel_order.assert_not_called()


def test_repeated_failure_inside_cooldown_does_not_fetch_again():
    position = _position()
    broker = _RecoveryBroker(recover=False)
    monitor, _ = _monitor(position, broker)

    monitor._refresh_once()
    combined_after_first = sum(len(call) == 2 for call in broker.calls)
    monitor._refresh_once()

    assert combined_after_first == 1
    assert sum(len(call) == 2 for call in broker.calls) == 1
    assert monitor._metrics["direct_recovery_attempts"] == 1
    assert monitor._metrics["direct_recovery_cooldown_suppressed"] == 1


def test_global_rate_limit_backoff_suppresses_direct_fetch():
    position = _position()
    broker = _RecoveryBroker()
    monitor, _ = _monitor(position, broker)
    original = qpm_module._SHARED_BACKOFF_UNTIL
    try:
        qpm_module._SHARED_BACKOFF_UNTIL = time.time() + 60
        monitor._refresh_once()
    finally:
        qpm_module._SHARED_BACKOFF_UNTIL = original

    assert not any(len(call) == 2 for call in broker.calls)
    assert monitor._metrics["direct_recovery_attempts"] == 0
    assert monitor._metrics["direct_recovery_backoff_suppressed"] == 1


def test_stale_provider_timestamp_is_not_certified_by_fresh_receipt():
    position = _position()
    old_position_ts = position.last_option_quote_update_ts
    broker = _RecoveryBroker(provider_ts=time.time() - 300)
    monitor, engine = _monitor(position, broker)

    monitor._refresh_once()

    assert monitor._metrics["direct_recovery_attempts"] == 1
    assert monitor._metrics["direct_recovery_successes"] == 0
    assert monitor._metrics["direct_recovery_failures"] == 1
    assert position.last_option_quote_update_ts == old_position_ts
    assert position.quote_state in {"stale", "blind"}


def test_missing_live_position_id_never_claims_recovery_success():
    position = _position(position_id="")
    broker = _RecoveryBroker()
    monitor, _ = _monitor(position, broker)

    monitor._refresh_once()

    assert not any(len(call) == 2 for call in broker.calls)
    assert monitor._metrics["direct_recovery_successes"] == 0
    assert monitor._metrics["direct_recovery_failures"] == 1
    assert monitor._last_direct_recovery_result["reason"] == "missing_position_id"


def test_paper_position_never_uses_direct_recovery():
    position = _position(mode="paper")
    broker = _RecoveryBroker()
    monitor, _ = _monitor(position, broker)

    monitor._refresh_once()

    assert not any(len(call) == 2 for call in broker.calls)
    assert monitor._metrics["direct_recovery_attempts"] == 0


def test_removed_position_during_fetch_is_not_mutated_or_woken():
    position = _position()
    old_option_ts = position.last_option_quote_update_ts
    holder = {}

    def remove_position():
        engine = holder["engine"]
        engine.positions.clear()
        engine._positions_by_id.pop(POSITION_ID, None)

    broker = _RecoveryBroker(during_recovery=remove_position)
    monitor, engine = _monitor(position, broker)
    holder["engine"] = engine

    monitor._refresh_once()

    assert position.last_option_quote_update_ts == old_option_ts
    assert not hasattr(position, "last_option_provider_quote_ts")
    assert not engine.quote_arrived_event.is_set()
    assert monitor._metrics["direct_recovery_successes"] == 0
    assert (
        monitor._last_direct_recovery_result["reason"]
        == "position_identity_changed_after_fetch"
    )


def test_replaced_position_during_fetch_is_not_mutated_or_woken():
    position = _position()
    replacement = _position()
    old_option_ts = position.last_option_quote_update_ts
    holder = {}

    def replace_position():
        engine = holder["engine"]
        engine.positions[:] = [replacement]
        engine._positions_by_id[POSITION_ID] = replacement

    broker = _RecoveryBroker(during_recovery=replace_position)
    monitor, engine = _monitor(position, broker)
    holder["engine"] = engine

    monitor._refresh_once()

    assert position.last_option_quote_update_ts == old_option_ts
    assert not hasattr(position, "last_option_provider_quote_ts")
    assert not hasattr(replacement, "last_option_provider_quote_ts")
    assert not engine.quote_arrived_event.is_set()
    assert monitor._metrics["direct_recovery_successes"] == 0


def test_closed_position_during_fetch_is_not_mutated_or_woken():
    position = _position()
    old_option_ts = position.last_option_quote_update_ts

    def close_position():
        position.closed = True
        position.quantity_remaining = 0
        position.quantityremaining = 0

    broker = _RecoveryBroker(during_recovery=close_position)
    monitor, engine = _monitor(position, broker)

    monitor._refresh_once()

    assert position.last_option_quote_update_ts == old_option_ts
    assert not hasattr(position, "last_option_provider_quote_ts")
    assert not engine.quote_arrived_event.is_set()
    assert monitor._metrics["direct_recovery_successes"] == 0


def test_missing_symbol_enters_same_bounded_cooldown():
    position = _position(option_symbol="")
    broker = _RecoveryBroker()
    monitor, _ = _monitor(position, broker)

    monitor._refresh_once()
    monitor._refresh_once()

    assert monitor._metrics["direct_recovery_failures"] == 1
    assert monitor._metrics["direct_recovery_cooldown_suppressed"] == 1
    assert monitor._last_direct_recovery_result["reason"] == "missing_symbol"


def test_tightly_bounded_future_provider_skew_is_accepted():
    position = _position()
    broker = _RecoveryBroker(provider_ts=time.time() + 1)
    monitor, engine = _monitor(position, broker)

    monitor._refresh_once()

    assert monitor._metrics["direct_recovery_successes"] == 1
    assert engine.quote_arrived_event.is_set()


def test_failed_recovery_suppresses_stale_bid_but_keeps_hard_reference():
    position = _position()
    position.entry_price = 1.0
    position.entryprice = 1.0
    position.peak_pnl_pct = 0.12
    position.peakpnlpct = 0.12
    position.max_profit_seen = 0.12
    position.maxprofitseen = 0.12
    prior_bid_ts = datetime.now(timezone.utc) - timedelta(minutes=5)
    position.last_option_bid_update_ts = prior_bid_ts
    position.lastoptionbidupdatets = prior_bid_ts
    broker = _StaleOrdinaryBidBroker(
        provider_ts=time.time() - 300
    )
    monitor, engine = _monitor(position, broker)
    monitor._should_wake = MagicMock(return_value=True)

    monitor._refresh_once()

    assert monitor._metrics["direct_recovery_successes"] == 0
    assert position.current_bid == 0.0
    assert position.currentbid == 0.0
    assert position.last_option_bid_update_ts == prior_bid_ts
    assert position.lastoptionbidupdatets == prior_bid_ts
    assert position.option_bid_valid is False
    assert position.option_quote_fresh is False
    assert position.executable_quote_valid is False
    assert position.peak_pnl_pct == 0.12
    assert position.max_profit_seen == 0.12
    assert position.touched_profit is False
    assert position.hard_exit_reference_price > 0
    assert position.hard_exit_reference_source != "bid"
    monitor._should_wake.assert_called_once_with(OPTION, 0.80)
    assert engine.quote_arrived_event.is_set()


def test_identical_recovery_provider_time_is_one_bid_observation():
    provider_epoch = time.time() - 5
    position = _position()
    broker = _RecoveryBroker(provider_ts=provider_epoch)
    monitor, _ = _monitor(position, broker)
    candidate = SimpleNamespace(
        action="CLOSE_ALL",
        quantity=1,
        reason="TOUCHED_PROFIT_STOP",
        urgency="IMMEDIATE",
        pnl_pct=-0.10,
        reason_code="TOUCHED_PROFIT_STOP",
    )

    def candidate_exit(_position, now_et=None):
        return candidate

    guarded_exit = touched_profit_confirmation_guard.wrap_evaluate_exit(
        candidate_exit,
        exit_decision_cls=SimpleNamespace,
        classify_decision=lambda decision: decision.reason_code,
    )

    monitor._refresh_once()
    provider_ts = qpm_module.normalize_hard_ref_ts(provider_epoch)
    first = guarded_exit(position)

    monitor._last_direct_recovery_attempt_ts.clear()
    monitor._refresh_once()
    repeated = guarded_exit(position)

    assert position.last_option_bid_update_ts == provider_ts
    assert position.last_option_quote_update_ts == provider_ts
    assert position.last_underlying_quote_update_ts == provider_ts
    assert first.reason_code == "TOUCHED_PROFIT_STOP_CONFIRMING"
    assert repeated.reason_code == "TOUCHED_PROFIT_STOP_CONFIRMING"
    assert position._touched_profit_floor_breach_count == 1

    broker.provider_ts = provider_epoch - 2
    monitor._last_direct_recovery_attempt_ts.clear()
    monitor._refresh_once()
    older = guarded_exit(position)

    assert position.last_option_bid_update_ts == provider_ts
    assert position.lastoptionbidupdatets == provider_ts
    assert older.reason_code == "TOUCHED_PROFIT_STOP_CONFIRMING"
    assert position._touched_profit_floor_breach_count == 1

    broker.provider_ts = provider_epoch + 2
    monitor._last_direct_recovery_attempt_ts.clear()
    monitor._refresh_once()
    distinct = guarded_exit(position)

    assert distinct is candidate
    assert distinct.reason_code == "TOUCHED_PROFIT_STOP"


# ── PR #400 value-cohesion gate regressions ───────────────────────────────────
# The four regressions required by the HOLD review (comment 4792832035).
# Each test name encodes the exact invariant it protects.


def test_older_option_provider_ts_cannot_overwrite_bid_or_price_and_cannot_wake():
    """
    Older lower option BID must not overwrite current_bid or
    current_option_price and must not trigger a runner wake.

    Under the correct architecture, when recovery is rejected for the
    option component, oq is suppressed at the SOURCE (bid → 0.0, bid_ts
    popped), identically to the recovery-failure path.  The "BID is cycle
    truth" invariant then writes current_bid = 0.0 (NOT the rejected
    0.70).  The preserved 1.05 is not asserted as the new current_bid
    value — the invariant that matters is that the REJECTED 0.70 never
    reaches current_bid, and the preserved observation timestamp never
    advances to the rejected provider_ts.
    """
    now_epoch = time.time()

    # Plant a recent BID observation so the gate has an existing reference
    # strictly newer than the rejected recovery provider_ts.
    opt_obs_epoch = now_epoch - 3.0
    opt_obs_dt = datetime.fromtimestamp(opt_obs_epoch, tz=timezone.utc)
    position = _position()
    position.current_bid = 1.05
    position.currentbid = 1.05
    position.current_option_price = 1.05
    position.currentoptionprice = 1.05
    position.last_option_bid_update_ts = opt_obs_dt
    position.lastoptionbidupdatets = opt_obs_dt

    # Recovery provider_ts is 8 s ago: OLDER than the existing 3 s obs but
    # within STALE_MAX_SEC=15 s, so _maybe_direct_recover_position → ok=True.
    older_opt_epoch = now_epoch - 8.0
    broker = _StalerProviderRecoveryBroker(
        option_provider_ts=older_opt_epoch,
        underlying_provider_ts=now_epoch - 2.0,  # underlying is fresh
        option_bid=0.70,                          # rejected value — must NOT reach position
    )
    monitor, engine = _monitor(position, broker)

    monitor._refresh_once()

    # The rejected 0.70 must not appear anywhere on the position.
    assert position.current_bid != 0.70, (
        "rejected option recovery bid 0.70 must not reach current_bid"
    )
    assert position.currentbid != 0.70
    assert position.current_option_price != 0.70

    # BID is cycle truth: no fresh executable BID this cycle → 0.0.
    # (This is identical to the pre-existing recovery-failure invariant.)
    assert position.current_bid == 0.0, (
        "suppressed option component must produce current_bid=0.0 per "
        "the BID-cycle-truth invariant, matching recovery-failure semantics"
    )
    assert position.currentbid == 0.0

    # Preserved BID observation timestamp: must not have advanced.
    assert position.last_option_bid_update_ts == opt_obs_dt, (
        "rejected recovery must not advance last_option_bid_update_ts"
    )
    assert position.lastoptionbidupdatets == opt_obs_dt

    # No recovery success increment, no runner wake — recovery_complete=False.
    assert monitor._metrics["direct_recovery_successes"] == 0
    assert not engine.quote_arrived_event.is_set(), (
        "stale option recovery must not wake the exit engine"
    )


def test_older_underlying_provider_ts_cannot_overwrite_underlying_and_cannot_wake():
    """
    Older underlying price cannot overwrite current_underlying and cannot
    create a stop/target wake.

    Position has an existing underlying observation 3 s ago.  Recovery fetch
    returns fresh-enough underlying quote but with trade_date 8 s ago —
    strictly OLDER.  The underlying value must not be overwritten and the
    engine must not be woken.
    """
    now_epoch = time.time()

    und_obs_epoch = now_epoch - 3.0
    und_obs_dt = datetime.fromtimestamp(und_obs_epoch, tz=timezone.utc)
    position = _position()
    position.current_underlying = 555.0
    position.currentunderlying = 555.0
    position.last_underlying_quote_update_ts = und_obs_dt
    position.lastunderlyingquoteupdatets = und_obs_dt

    # Option provider_ts is newer than any existing option obs (none planted)
    # so _apply_opt=True; only the underlying component is stale.
    older_und_epoch = now_epoch - 8.0
    broker = _StalerProviderRecoveryBroker(
        option_provider_ts=now_epoch - 2.0,
        underlying_provider_ts=older_und_epoch,  # older than existing 3 s obs
        underlying_last=530.0,  # lower — must NOT be written
    )
    monitor, engine = _monitor(position, broker)

    monitor._refresh_once()

    assert position.current_underlying == 555.0, (
        "older underlying provider_ts must not overwrite current_underlying"
    )
    assert position.currentunderlying == 555.0

    # recovery_complete = (option_fresh OR _apply_opt) AND (underlying_fresh OR _apply_und)
    #                   = (False OR True) AND (False OR False) = False
    # → no success increment, no stop/target wake.
    assert monitor._metrics["direct_recovery_successes"] == 0
    assert not engine.quote_arrived_event.is_set(), (
        "stale underlying recovery must not wake the exit engine"
    )


def test_equal_provider_ts_second_recovery_leaves_values_and_wake_unchanged():
    """
    Equal provider timestamps produce no additional recovery observation:
    no success increment, no wake event, no timestamp advancement.

    The first recovery succeeds (provider_ts is strictly newer than the
    absent existing observation).  A second call with the identical epoch
    must be rejected by the value-cohesion gate: recovery_complete=False,
    no success, no wake, and the position's existing observation
    timestamps stay pinned at the first call's provider_ts.

    Note on current_bid: under the correct architecture the second call's
    oq is suppressed (bid → 0.0), so current_bid becomes 0.0 per the
    BID-cycle-truth invariant — identically to a recovery-failure cycle.
    The assertion here is that the REJECTED recovery value never reaches
    current_bid, not that current_bid preserves the first-call value.
    """
    provider_epoch = time.time() - 5.0
    provider_ts_expected = qpm_module.normalize_hard_ref_ts(provider_epoch)
    position = _position()
    broker = _RecoveryBroker(provider_ts=provider_epoch)
    monitor, engine = _monitor(position, broker)

    # First call: no existing BID obs → strictly newer → recovery accepted.
    monitor._refresh_once()
    assert monitor._metrics["direct_recovery_successes"] == 1
    assert engine.quote_arrived_event.is_set()
    assert position.last_option_bid_update_ts == provider_ts_expected
    assert position.last_underlying_quote_update_ts == provider_ts_expected

    # Second call: same provider_epoch — equal, NOT strictly newer.  Both
    # components are rejected → recovery_complete=False.  No success, no
    # wake, timestamp not advanced.
    engine.quote_arrived_event.clear()
    monitor._last_direct_recovery_attempt_ts.clear()
    monitor._refresh_once()

    # The rejected recovery bid (1.00 from the broker) must not have
    # produced a second observation — timestamp still pinned at first call.
    assert position.last_option_bid_update_ts == provider_ts_expected, (
        "equal provider_ts must not advance last_option_bid_update_ts"
    )
    assert position.lastoptionbidupdatets == provider_ts_expected
    assert position.last_underlying_quote_update_ts == provider_ts_expected, (
        "equal provider_ts must not advance last_underlying_quote_update_ts"
    )

    # No second recovery success, no wake.
    assert monitor._metrics["direct_recovery_successes"] == 1, (
        "equal provider_ts must not increment direct_recovery_successes"
    )
    assert not engine.quote_arrived_event.is_set(), (
        "equal provider_ts must not trigger a second recovery wake"
    )


def test_strictly_newer_provider_ts_applies_values_and_wakes_exactly_once():
    """
    Strictly newer option and underlying provider truth still applies values
    and wakes the exit engine exactly once.

    Gate regression: verifying the cohesion gate does not block legitimate
    recoveries where both option and underlying provider timestamps are
    strictly newer than the position's existing observations.
    """
    now_epoch = time.time()

    # Plant an existing BID observation 10 s ago.
    opt_obs_epoch = now_epoch - 10.0
    opt_obs_dt = datetime.fromtimestamp(opt_obs_epoch, tz=timezone.utc)
    position = _position()
    position.current_bid = 0.50
    position.currentbid = 0.50
    position.last_option_bid_update_ts = opt_obs_dt
    position.lastoptionbidupdatets = opt_obs_dt

    # Recovery provider_ts is 4 s ago — strictly newer than opt_obs (10 s ago)
    # and within STALE_MAX_SEC=15 s, so both option and underlying gates pass.
    newer_epoch = now_epoch - 4.0
    broker = _RecoveryBroker(provider_ts=newer_epoch)  # bid=1.00
    monitor, engine = _monitor(position, broker)

    monitor._refresh_once()

    # Values must have advanced to the recovered truth.
    assert position.current_bid == 1.00, (
        "strictly newer provider_ts must update current_bid"
    )
    assert position.currentbid == 1.00

    # Recovery completes, wake fires exactly once.
    assert monitor._metrics["direct_recovery_successes"] == 1
    assert engine.quote_arrived_event.is_set()


# ── PR #400 amendment #2: source-level per-component regressions ──────────────
# Reviewer HOLD 4792984468: the earlier gate blocked direct field assignments
# but the rejected recovery payload still reached hard-reference, executable
# truth, peak, touched-profit, persistence, and coverage via oq/uq/bid/ask/
# opt_price/und_last locals.  The fix moved the gate to the SOURCE — the
# first pass — so rejected components never populate those locals in the
# first place, and fresh ordinary components are never displaced.


def test_fresh_ordinary_option_plus_stale_underlying_leaves_option_authoritative():
    """
    Fresh ordinary option + stale underlying: only the underlying is
    recovered; the ordinary option quote remains authoritative.

    The recovery payload includes an option quote too (that's how the
    two-symbol batch works), but because ordinary option is fresh the
    gate leaves oq untouched — recovery option payload never reaches
    current_bid, current_option_price, hard-reference, or executable
    truth.  Underlying, being stale-with-strictly-newer-recovery, is
    applied.
    """
    broker = _PartialOrdinaryBroker(
        ordinary_option_fresh=True,
        ordinary_underlying_fresh=False,
        ordinary_option_bid=1.05,           # fresh ordinary
        recovery_option_bid=0.70,           # must NOT reach position
        recovery_underlying_last=545.0,     # will be applied
    )
    position = _position()
    monitor, engine = _monitor(position, broker)

    monitor._refresh_once()

    # Ordinary option authoritative — recovery option value must not appear.
    assert position.current_bid == 1.05, (
        "fresh ordinary option must remain authoritative"
    )
    assert position.currentbid == 1.05
    assert position.current_bid != 0.70, (
        "recovery option bid 0.70 must not have displaced the fresh ordinary quote"
    )

    # Underlying was recovered — current_underlying updated from recovery.
    assert position.current_underlying == 545.0
    assert position.currentunderlying == 545.0

    # Recovery completed: option fresh + underlying applied.
    assert monitor._metrics["direct_recovery_successes"] == 1
    assert engine.quote_arrived_event.is_set()


def test_stale_option_plus_fresh_ordinary_underlying_leaves_underlying_authoritative():
    """
    Stale option + fresh ordinary underlying: only the option is
    recovered; the ordinary underlying quote remains authoritative.

    Recovery underlying payload (530.0) must never reach current_underlying;
    the fresh ordinary underlying (555.0) stays put.  Option component is
    stale-with-strictly-newer-recovery, so it applies.
    """
    broker = _PartialOrdinaryBroker(
        ordinary_option_fresh=False,
        ordinary_underlying_fresh=True,
        ordinary_underlying_last=555.0,     # fresh ordinary
        recovery_option_bid=0.70,           # will be applied
        recovery_underlying_last=530.0,     # must NOT reach position
    )
    position = _position()
    monitor, engine = _monitor(position, broker)

    monitor._refresh_once()

    # Ordinary underlying authoritative — recovery underlying must not appear.
    assert position.current_underlying == 555.0, (
        "fresh ordinary underlying must remain authoritative"
    )
    assert position.currentunderlying == 555.0
    assert position.current_underlying != 530.0, (
        "recovery underlying 530.0 must not have displaced the fresh ordinary quote"
    )

    # Option was recovered — current_bid updated from recovery.
    assert position.current_bid == 0.70
    assert position.currentbid == 0.70

    # Recovery completed: option applied + underlying fresh.
    assert monitor._metrics["direct_recovery_successes"] == 1
    assert engine.quote_arrived_event.is_set()


def test_rejected_option_recovery_leaves_hard_ref_executable_peak_touched_profit_untouched():
    """
    Older/equal rejected option: hard-reference, executable mark/P&L,
    peak_pnl_pct and touched_profit are all untouched.

    The reviewer's Blocker 1 was that the rejected recovered numeric bid
    could still reach:
      - hard_exit_reference_price / _source / _pnl_pct
      - exit_executable_mark / exit_executable_pnl_pct
      - peak_pnl_pct / max_profit_seen
      - touched_profit lifecycle
    This regression asserts none of that occurs.
    """
    now_epoch = time.time()

    # Plant a recent BID observation so the gate rejects the recovery.
    opt_obs_epoch = now_epoch - 3.0
    opt_obs_dt = datetime.fromtimestamp(opt_obs_epoch, tz=timezone.utc)

    # Plant hard-reference, executable, peak, and touched-profit state that
    # the rejected recovery must not perturb.  Values chosen so that if the
    # rejected 0.70 bid reached executable truth it would clearly move
    # peak/pnl/hard-ref downward.
    position = _position()
    position.entry_price = 1.00
    position.entryprice = 1.00
    position.current_bid = 1.05
    position.currentbid = 1.05
    position.current_option_price = 1.05
    position.currentoptionprice = 1.05
    position.last_option_bid_update_ts = opt_obs_dt
    position.lastoptionbidupdatets = opt_obs_dt

    prior_hard_ref_price = 1.10
    prior_hard_ref_source = "bid"
    prior_hard_ref_pnl_pct = 0.10
    prior_hard_ref_validity = "proven"
    prior_hard_ref_ts = datetime.fromtimestamp(now_epoch - 2.0, tz=timezone.utc)
    position.hard_exit_reference_price = prior_hard_ref_price
    position.hardexitreferenceprice = prior_hard_ref_price
    position.hard_exit_reference_source = prior_hard_ref_source
    position.hardexitreferencesource = prior_hard_ref_source
    position.hard_exit_reference_pnl_pct = prior_hard_ref_pnl_pct
    position.hardexitreferencepnlpct = prior_hard_ref_pnl_pct
    position.hard_exit_reference_validity = prior_hard_ref_validity
    position.hardexitreferencevalidity = prior_hard_ref_validity
    position.hard_exit_reference_ts = prior_hard_ref_ts
    position.hardexitreferencets = prior_hard_ref_ts

    position.peak_pnl_pct = 0.12
    position.peakpnlpct = 0.12
    position.max_profit_seen = 0.12
    position.maxprofitseen = 0.12
    position.touched_profit = True
    position.touchedprofit = True

    # Recovery provider_ts is 8 s ago: OLDER than the existing 3 s obs → gate
    # rejects the option component.  Underlying is fresh-enough recovery so
    # the recovery call succeeds overall.
    older_opt_epoch = now_epoch - 8.0
    broker = _StalerProviderRecoveryBroker(
        option_provider_ts=older_opt_epoch,
        underlying_provider_ts=now_epoch - 2.0,
        option_bid=0.70,          # if this reaches executable truth the peak crashes
        underlying_last=550.0,
    )
    monitor, engine = _monitor(position, broker)

    monitor._refresh_once()

    # Hard-reference must not have been touched.
    assert position.hard_exit_reference_price == prior_hard_ref_price, (
        "rejected recovery bid must not overwrite hard_exit_reference_price"
    )
    assert position.hard_exit_reference_source == prior_hard_ref_source
    assert position.hard_exit_reference_pnl_pct == prior_hard_ref_pnl_pct
    assert position.hard_exit_reference_validity == prior_hard_ref_validity
    assert position.hard_exit_reference_ts == prior_hard_ref_ts

    # Peak / max_profit_seen must not have regressed.  If the rejected 0.70
    # had reached executable P&L, the resulting -30% would not have advanced
    # peak, BUT downstream state (touched_profit lifecycle) could have been
    # perturbed if the rejected numbers were used.
    assert position.peak_pnl_pct == 0.12, (
        "rejected recovery bid must not perturb peak_pnl_pct"
    )
    assert position.max_profit_seen == 0.12

    # Touched-profit state: was True, must remain True (nothing this cycle
    # should have driven a re-evaluation using the rejected value).
    assert position.touched_profit is True, (
        "rejected recovery bid must not perturb touched_profit lifecycle"
    )

    # Executable truth: this cycle has no accepted BID, so exit_executable_*
    # correctly reflects "no fresh executable BID" — but crucially it MUST
    # NOT reflect the rejected 0.70 as if it were accepted.
    exec_mark = getattr(position, "exit_executable_mark", None)
    assert exec_mark != 0.70, (
        "rejected recovery bid must not appear in exit_executable_mark"
    )
    exec_pnl = getattr(position, "exit_executable_pnl_pct", None)
    # If exec_pnl is present, it must not be the rejected-bid computation
    # (0.70 - 1.00) / 1.00 = -0.30.
    if exec_pnl is not None:
        assert abs(float(exec_pnl) - (-0.30)) > 1e-6, (
            "rejected recovery bid must not produce exec_pnl of -30%"
        )

    # No recovery success, no wake.
    assert monitor._metrics["direct_recovery_successes"] == 0
    assert not engine.quote_arrived_event.is_set()


def test_rejected_underlying_recovery_does_not_reach_persistence_availability_or_coverage():
    """
    Older/equal rejected underlying: persistence, availability, and coverage
    all reflect the rejected component as absent — not as fresh recovered
    truth.

    Reviewer's specific concern: even when the direct field write for
    current_underlying is skipped, the local `und_last` could still flow
    into `_persist_quote_to_db(underlying_price=und_last)`, into the
    `_und_available` computation (`und_last > 0`), and into the
    `positions_fully_fresh` coverage counter.
    """
    now_epoch = time.time()

    # Plant a recent underlying observation so the gate rejects the recovery.
    und_obs_epoch = now_epoch - 3.0
    und_obs_dt = datetime.fromtimestamp(und_obs_epoch, tz=timezone.utc)
    position = _position()
    position.entry_price = 1.00
    position.entryprice = 1.00
    position.current_underlying = 555.0
    position.currentunderlying = 555.0
    position.last_underlying_quote_update_ts = und_obs_dt
    position.lastunderlyingquoteupdatets = und_obs_dt

    # Recovery underlying provider_ts is 8 s ago: OLDER than existing 3 s obs.
    # Option recovery is fresh so recovery call succeeds overall.
    older_und_epoch = now_epoch - 8.0
    broker = _StalerProviderRecoveryBroker(
        option_provider_ts=now_epoch - 2.0,
        underlying_provider_ts=older_und_epoch,
        option_bid=1.20,               # legitimately applied
        underlying_last=530.0,         # must NOT reach persistence
    )
    monitor, engine = _monitor(position, broker)

    monitor._refresh_once()

    # Persistence: _persist_quote_to_db must NEVER have been called with
    # the rejected recovery underlying_last=530.0.  It may be called with
    # underlying_price=0.0 (correct — no fresh underlying truth this cycle).
    persist_calls = monitor._persist_quote_to_db.call_args_list
    for call in persist_calls:
        underlying_price_arg = call.kwargs.get("underlying_price")
        assert underlying_price_arg != 530.0, (
            "rejected recovery underlying 530.0 must not reach persistence"
        )

    # Availability: rejected underlying → uq={} → und_last=0 → _und_available
    # must be False on the position and its snapshot.
    assert getattr(position, "underlying_available", True) is False, (
        "rejected recovery underlying must produce underlying_available=False"
    )

    # Coverage: this position must NOT be counted as fully-fresh, since the
    # underlying is stale-with-rejected-recovery.  If the rejected recovery
    # payload had reached _und_available, positions_fully_fresh could have
    # incremented incorrectly.
    assert monitor._metrics["positions_fully_fresh"] == 0, (
        "rejected recovery must not contribute to positions_fully_fresh"
    )

    # Direct assignment: current_underlying stays at the planted 555.0.
    assert position.current_underlying == 555.0, (
        "rejected recovery underlying 530.0 must not have displaced current_underlying"
    )
    assert position.currentunderlying == 555.0

    # No recovery success, no wake.
    assert monitor._metrics["direct_recovery_successes"] == 0
    assert not engine.quote_arrived_event.is_set()
