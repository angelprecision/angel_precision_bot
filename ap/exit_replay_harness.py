# ap/exit_replay_harness.py
# =============================================================================
# Exit Replay Harness
# =============================================================================
# Deterministically feeds price paths into ap_exit_engine.evaluate_exit().
# This is how we prove profit floors, trails, scale-outs, and stops fire without
# needing live market data or broker calls.
# =============================================================================

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

from ap_exit_engine import ManagedPosition, evaluate_exit


@dataclass
class ReplayStep:
    option_price: float
    bid: float = 0.0
    ask: float = 0.0
    underlying: float = 0.0
    label: str = ""


@dataclass
class ReplayDecision:
    step_index: int
    label: str
    option_price: float
    pnl_pct: float
    peak_pnl_pct: float
    max_profit_seen: float
    touched_profit: bool
    action: str
    quantity: int
    reason: str
    reason_code: str
    urgency: str


@dataclass
class ReplayResult:
    position: ManagedPosition
    decisions: list[ReplayDecision] = field(default_factory=list)

    @property
    def first_action(self) -> Optional[ReplayDecision]:
        for decision in self.decisions:
            if decision.action != "HOLD":
                return decision
        return None

    @property
    def actions(self) -> list[ReplayDecision]:
        return [d for d in self.decisions if d.action != "HOLD"]


def _safe_mid(price: float) -> tuple[float, float]:
    price = float(price)
    if price <= 0:
        return 0.0, 0.0
    # Small synthetic spread around the observed option price.
    return max(0.01, round(price * 0.98, 4)), round(price * 1.02, 4)


def make_position(
    *,
    entry_price: float = 1.00,
    quantity: int = 1,
    ticker: str = "SPY",
    option_symbol: str = "SPY260515C00500000",
    side: str = "CALL",
    underlying_entry: float = 500.0,
    underlying_target: float = 9999.0,
    underlying_stop: float = 0.01,
    opened_at: Optional[datetime] = None,
) -> ManagedPosition:
    return ManagedPosition(
        ticker=ticker,
        option_symbol=option_symbol,
        side=side,
        quantity=quantity,
        entry_price=float(entry_price),
        underlying_entry=float(underlying_entry),
        underlying_target=float(underlying_target),
        underlying_stop=float(underlying_stop),
        position_id="replay-position",
        client_id="replay-client",
        signal_id="replay-signal",
        opened_at=opened_at or datetime.now(timezone.utc),
    )


def apply_replay_step(pos: ManagedPosition, step: ReplayStep) -> None:
    price = float(step.option_price)
    bid = float(step.bid or 0.0)
    ask = float(step.ask or 0.0)
    if price > 0 and (bid <= 0 or ask <= 0):
        bid, ask = _safe_mid(price)

    pos.current_option_price = price
    pos.current_bid = bid
    pos.current_ask = ask
    if step.underlying:
        pos.current_underlying = float(step.underlying)
    elif not pos.current_underlying:
        pos.current_underlying = pos.underlying_entry

    if pos.entry_price > 0 and price > 0:
        pnl = (price - pos.entry_price) / pos.entry_price
        pos.peak_pnl_pct = max(float(pos.peak_pnl_pct or 0.0), pnl)
        pos.max_profit_seen = max(float(pos.max_profit_seen or 0.0), pnl)
        if pnl >= 0.05:
            pos.touched_profit = True


def replay_exit_path(
    path: Iterable[float | ReplayStep],
    *,
    position: Optional[ManagedPosition] = None,
    now_et: Any = None,
) -> ReplayResult:
    pos = position or make_position()
    result = ReplayResult(position=pos)

    for idx, raw_step in enumerate(path):
        if isinstance(raw_step, ReplayStep):
            step = raw_step
        else:
            step = ReplayStep(option_price=float(raw_step), label=f"step_{idx}")

        apply_replay_step(pos, step)
        decision = evaluate_exit(pos, now_et=now_et)
        result.decisions.append(
            ReplayDecision(
                step_index=idx,
                label=step.label or f"step_{idx}",
                option_price=float(pos.current_option_price or 0.0),
                pnl_pct=float(pos.option_pnl_pct or 0.0),
                peak_pnl_pct=float(pos.peak_pnl_pct or 0.0),
                max_profit_seen=float(pos.max_profit_seen or 0.0),
                touched_profit=bool(pos.touched_profit),
                action=str(decision.action),
                quantity=int(decision.quantity or 0),
                reason=str(decision.reason or ""),
                reason_code=str(getattr(decision, "reason_code", "") or ""),
                urgency=str(decision.urgency or ""),
            )
        )
    return result
