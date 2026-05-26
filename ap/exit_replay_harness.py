"""
ap/exit_replay_harness.py
==============================================================================
Replay harness for the exit engine.

Feeds a fake option price path into evaluate_exit() and records every
decision. Lets you prove floors/trails fire on known price paths
before risking client money.

Usage:
    from ap.exit_replay_harness import replay_price_path, ReplayResult

    result = replay_price_path(
        entry_price=1.00,
        price_path=[1.05, 1.12, 1.18, 1.25, 1.17, 1.12, 1.08],
        qty=1,
    )
    assert result.exit_fired
    assert result.exit_pnl_pct >= 0.10
==============================================================================
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, List, Optional, Sequence

log = logging.getLogger("ap.exit_replay_harness")


# ---------------------------------------------------------------------------
# Minimal stub objects so we can drive evaluate_exit() without a live runner
# ---------------------------------------------------------------------------

class _StubPosition:
    """Minimal ManagedPosition stub for replay."""

    def __init__(
        self,
        entry_price: float,
        qty: int,
        ticker: str = "SPY",
        side: str = "CALL",
        contract: str = "SPY260515C00500000",
    ) -> None:
        self.position_id          = "replay-0001"
        self.signal_id            = "replay-signal"
        self.client_id            = "replay@test.com"
        self.ticker               = ticker
        self.underlying           = ticker
        self.side                 = side
        self.direction            = side
        self.option_symbol        = contract
        self.contract             = contract
        self.entry_price          = float(entry_price)
        self.avg_fill             = float(entry_price)
        self.qty                  = int(qty)
        self.quantity_remaining   = int(qty)
        self.scale_outs_done      = 0
        self.current_option_price = float(entry_price)
        self.current_bid          = float(entry_price) * 0.97
        self.current_ask          = float(entry_price) * 1.03
        self.current_underlying   = 100.0
        self.underlying_entry     = 100.0
        self.underlying_target    = 999999.0   # never trigger target
        self.underlying_stop      = 0.0        # never trigger stop
        self.option_pnl_pct       = 0.0
        self.underlying_pnl_pct   = 0.0
        self.peak_pnl_pct         = 0.0
        self.max_profit_seen      = 0.0
        self.touched_profit       = False
        self.exit_in_flight       = False
        self.quote_state          = "FRESH"
        self.quote_health         = "FRESH"
        self.last_quote_update    = datetime.now(timezone.utc)
        self.last_option_quote_ts = datetime.now(timezone.utc)
        self.last_und_quote_ts    = datetime.now(timezone.utc)
        self.opened_at            = datetime.now(timezone.utc)
        self.entry_ts             = datetime.now(timezone.utc)
        self.scale_out_orders     = []
        self.pending_exit_action  = None
        self.pending_exit_local_order_id = None
        # PR-A: Fields that ManagedPosition now declares; the stub must
        # carry them too or evaluate_exit raises AttributeError when it
        # reads pos._stop_breach_ts / pos._underlying_stop_breach_ts.
        # is_trend_day is read by the trend-day branch in evaluate_exit;
        # without it the harness silently swallowed AttributeError and
        # never validated hard-stop replays (audit finding, May 2026).
        self._stop_breach_ts             = None
        self._underlying_stop_breach_ts  = None
        self.is_trend_day                = False
        self.trend_direction             = ""
        # PR-B: Execution-core ghost fields now declared on
        # ManagedPosition. Stub must carry the same defaults or any
        # evaluate_exit / _on_position_close path that touches them
        # would raise AttributeError.
        self._exit_submit_ts             = 0.0
        self._exit_attempts              = 0
        self._integrity_logged           = False
        self._proof_staged               = None
        self._proof_finalized            = False
        self.proof_logged                = False
        # ── Hygiene PR: remaining ManagedPosition attrs evaluate_exit reads ──
        # PRs #34 and #35 added 10 attributes above; this block covers the
        # other 36 that evaluate_exit / _eval_exit / helper paths read.
        # Without them, AttributeError is swallowed by the harness and exit
        # decisions silently degrade. Audited against ap_exit_engine.py
        # pos.* reads on 2026-05-26. All defaults match ManagedPosition.
        self.quantity                                  = int(qty)
        self.closed                                    = False
        self.close_reason                              = ""
        self._exit_stuck_count                         = 0
        self._submit_generation                        = 0
        self.last_rejection_ts                         = None
        self.last_exit_rejected                        = False
        self.last_exit_signal_ts                       = None
        self.pending_exit_reason                       = ""
        self.pending_exit_qty                          = 0
        self.pending_exit_filled_qty                   = 0
        self.pending_scale_counted                     = False
        self.pending_exit_broker_order_id              = ""
        self.pending_exit_replace_allowed              = False
        self.pending_exit_replace_reason               = ""
        self.pending_exit_replace_allowed_ts           = None
        self.last_applied_exit_local_order_id          = ""
        self.last_applied_exit_broker_order_id         = ""
        self.last_applied_exit_cum_fill                = 0
        self.last_applied_exit_cum_fill_by_order       = {}
        self.last_callback_identity_missing            = False
        self.last_callback_identity_missing_ts         = None
        self.exit_identity_quarantine                  = False
        self.exit_identity_quarantine_alert_count      = 0
        self.last_exit_identity_quarantine_alert_ts    = None
        self.last_exit_identity_quarantine_resolved_ts = None
        self.last_exit_clear_reason                    = ""
        self.last_exit_clear_local_order_id            = ""
        self.last_exit_clear_broker_order_id           = ""
        self.last_exit_identity_reject_ts              = None
        self.last_quote_update_ts                      = None
        self.last_quote_missing_ts                     = None
        self.last_underlying_quote_update_ts           = None
        self.last_underlying_quote_missing_ts          = None
        self.last_option_quote_update_ts               = None
        self.last_option_quote_missing_ts              = None

    @property
    def is_at_target(self) -> bool:
        return False

    @property
    def is_at_stop(self) -> bool:
        return False

    def update_price(self, price: float) -> None:
        self.current_option_price = price
        self.current_bid          = price * 0.97
        self.current_ask          = price * 1.03
        # Update P&L
        if self.entry_price > 0:
            self.option_pnl_pct = (price - self.entry_price) / self.entry_price
        # Advance peak BEFORE any gate (matches real engine behavior)
        if self.option_pnl_pct > self.peak_pnl_pct:
            self.peak_pnl_pct = self.option_pnl_pct
        if self.option_pnl_pct > self.max_profit_seen:
            self.max_profit_seen = self.option_pnl_pct
        if self.option_pnl_pct > 0:
            self.touched_profit = True
        self.last_quote_update    = datetime.now(timezone.utc)
        self.last_option_quote_ts = datetime.now(timezone.utc)


@dataclass
class ReplayStep:
    step:              int
    price:             float
    option_pnl_pct:    float
    peak_pnl_pct:      float
    touched_profit:    bool
    decision_action:   str
    decision_reason:   str
    decision_qty:      int
    exit_in_flight:    bool


@dataclass
class ReplayResult:
    entry_price:        float
    price_path:         List[float]
    steps:              List[ReplayStep] = field(default_factory=list)
    exit_fired:         bool = False
    exit_step:          int  = -1
    exit_price:         float = 0.0
    exit_pnl_pct:       float = 0.0
    exit_reason:        str  = ""
    peak_pnl_pct:       float = 0.0
    final_price:        float = 0.0
    error:              Optional[str] = None


# ---------------------------------------------------------------------------
# Core replay function
# ---------------------------------------------------------------------------

def replay_price_path(
    entry_price:  float,
    price_path:   Sequence[float],
    *,
    qty:          int   = 1,
    ticker:       str   = "REPLAY",
    side:         str   = "CALL",
    now_et:       Optional[datetime] = None,
    step_seconds: int   = 15,
) -> ReplayResult:
    """
    Feed a price path into evaluate_exit() step by step.
    Returns a ReplayResult with full step trace and exit summary.

    Parameters
    ----------
    entry_price : option premium paid
    price_path  : list of option prices, one per simulated cycle
    qty         : number of contracts
    ticker      : underlying ticker (for logs)
    side        : CALL or PUT
    """
    try:
        from ap_exit_engine import evaluate_exit
    except ImportError as exc:
        return ReplayResult(
            entry_price=entry_price,
            price_path=list(price_path),
            error=f"Could not import evaluate_exit: {exc}",
        )

    pos     = _StubPosition(entry_price, qty, ticker, side)
    result  = ReplayResult(entry_price=entry_price, price_path=list(price_path))
    base_now_et = now_et or datetime.now(timezone.utc)
    # Anchor opened_at 30 minutes before the replay clock so position-age
    # checks don't fail with age=0s (some gates require min hold time).
    pos.opened_at            = base_now_et - timedelta(minutes=30)
    pos.entry_ts             = pos.opened_at
    pos.last_quote_update    = base_now_et
    pos.last_option_quote_ts = base_now_et

    for i, price in enumerate(price_path):
        # Advance the simulated clock each step so confirmation windows
        # (e.g. STOP_BREACH_CONFIRM_SECONDS) can elapse rather than staying
        # pinned at age=0. The engine's internal breach timer reads wall
        # clock via datetime.now(), so tests that need to bypass the timing
        # entirely should set STOP_BREACH_CONFIRM_SECONDS=0 in the env.
        step_now_et = base_now_et + timedelta(seconds=i * step_seconds)
        pos.update_price(price)

        try:
            decision = evaluate_exit(pos, step_now_et)
        except Exception as exc:
            log.warning("evaluate_exit raised at step %d: %s", i, exc)
            decision_action = "ERROR"
            decision_reason = str(exc)
            decision_qty    = 0
        else:
            decision_action = getattr(decision, "action", "HOLD")
            decision_reason = getattr(decision, "reason", "")
            decision_qty    = getattr(decision, "quantity", 0)

        step = ReplayStep(
            step=i,
            price=price,
            option_pnl_pct=pos.option_pnl_pct,
            peak_pnl_pct=pos.peak_pnl_pct,
            touched_profit=pos.touched_profit,
            decision_action=decision_action,
            decision_reason=decision_reason,
            decision_qty=decision_qty,
            exit_in_flight=pos.exit_in_flight,
        )
        result.steps.append(step)
        result.peak_pnl_pct = max(result.peak_pnl_pct, pos.peak_pnl_pct)

        if decision_action not in ("HOLD", "ERROR", None):
            result.exit_fired   = True
            result.exit_step    = i
            result.exit_price   = price
            result.exit_pnl_pct = pos.option_pnl_pct
            result.exit_reason  = decision_reason
            result.final_price  = price
            break

    result.final_price = price_path[-1] if price_path else entry_price
    return result


def print_replay_report(result: ReplayResult) -> None:
    """Print a human-readable replay trace."""
    print(f"\n{'='*60}")
    print(f"REPLAY: entry=${result.entry_price:.2f}  qty based on path")
    print(f"{'='*60}")
    for s in result.steps:
        marker = " ← EXIT" if s.step == result.exit_step else ""
        print(
            f"  [{s.step:02d}] price=${s.price:.2f} "
            f"pnl={s.option_pnl_pct*100:+.1f}% "
            f"peak={s.peak_pnl_pct*100:.1f}% "
            f"touched={s.touched_profit} "
            f"decision={s.decision_action}{marker}"
        )
        if s.decision_action not in ("HOLD", None):
            print(f"       reason: {s.decision_reason}")
    print(f"\nResult: exit_fired={result.exit_fired} "
          f"exit_pnl={result.exit_pnl_pct*100:+.1f}% "
          f"peak={result.peak_pnl_pct*100:.1f}%")
    if result.error:
        print(f"ERROR: {result.error}")
    print()
