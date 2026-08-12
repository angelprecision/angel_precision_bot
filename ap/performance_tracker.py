from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger("ap.performance_tracker")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)


@dataclass
class TradeOutcome:
    client_id: str = "default"
    position_id: str = ""
    signal_id: str = ""
    ticker: str = ""
    side: str = ""
    pattern: str = ""
    timeframe: str = ""
    strategy_type: str = ""
    contracts: int = 0
    entry_price: float = 0.0
    exit_price: float = 0.0
    quantity_closed: int = 0
    realized_pnl: float = 0.0
    pnl_pct: float = 0.0
    win: bool = False
    close_reason: str = ""
    opened_at: str = ""
    closed_at: str = field(default_factory=_now_iso)
    metadata: dict[str, Any] = field(default_factory=dict)


def outcome_from_position(position: Any, *, qty_filled: Any = None, fill_price: Any = None, reason: str = "") -> TradeOutcome:
    entry_price = _float(_get(position, "entry_price", 0.0))
    exit_price = _float(fill_price if fill_price not in (None, "") else (_get(position, "exit_price", None) or _get(position, "current_option_price", 0.0)))
    contracts = _int(_get(position, "quantity", 0) or _get(position, "contracts", 0) or _get(position, "qty", 0))
    qty_closed = _int(qty_filled if qty_filled not in (None, "") else (_get(position, "quantity_remaining", 0) or contracts))
    realized = _float(_get(position, "realized_pnl", 0.0))
    if not realized and entry_price > 0 and exit_price > 0 and qty_closed > 0:
        realized = (exit_price - entry_price) * qty_closed * 100
    pnl_pct = (exit_price - entry_price) / entry_price if entry_price > 0 and exit_price > 0 else 0.0
    meta = _get(position, "metadata", {}) or {}
    if not isinstance(meta, dict):
        meta = {"raw_metadata": repr(meta)}
    opened_at = _get(position, "opened_at", "")
    if hasattr(opened_at, "isoformat"):
        opened_at = opened_at.isoformat()
    return TradeOutcome(
        client_id=str(_get(position, "client_id", "default") or "default"),
        position_id=str(_get(position, "position_id", "") or _get(position, "id", "") or ""),
        signal_id=str(_get(position, "signal_id", "") or ""),
        ticker=str(_get(position, "ticker", "") or _get(position, "underlying", "") or "").upper(),
        side=str(_get(position, "side", "") or "").upper(),
        pattern=str(_get(position, "pattern", "") or meta.get("pattern", "") or ""),
        timeframe=str(_get(position, "timeframe", "") or meta.get("timeframe", "") or ""),
        strategy_type=str(_get(position, "strategy_type", "") or meta.get("strategy_type", "") or ""),
        contracts=contracts,
        entry_price=round(entry_price, 4),
        exit_price=round(exit_price, 4),
        quantity_closed=qty_closed,
        realized_pnl=round(realized, 2),
        pnl_pct=round(pnl_pct, 6),
        win=realized > 0,
        close_reason=str(reason or _get(position, "close_reason", "") or _get(position, "exit_reason", "") or ""),
        opened_at=str(opened_at or ""),
        metadata=meta,
    )


class PerformanceTracker:
    def __init__(self, supabase_client=None):
        self.sb = supabase_client
        self.trades: list[TradeOutcome] = []

    def record_outcome(self, outcome: TradeOutcome) -> None:
        self.trades.append(outcome)
        log.info("TRADE_OUTCOME | client=%s ticker=%s pnl=$%.2f pnl_pct=%.2f%% signal=%s", outcome.client_id, outcome.ticker, outcome.realized_pnl, outcome.pnl_pct * 100, outcome.signal_id)
        self._persist(outcome)
        self._record_intel_outcome(outcome)

    def record_position(self, position: Any, *, qty_filled: Any = None, fill_price: Any = None, reason: str = "") -> TradeOutcome:
        outcome = outcome_from_position(position, qty_filled=qty_filled, fill_price=fill_price, reason=reason)
        self.record_outcome(outcome)
        return outcome

    def _persist(self, outcome: TradeOutcome) -> None:
        """Retain local compatibility statistics without creating a P&L authority.

        No durable P&L table is written here.  The canonical executed outcome
        is ``proof_trades`` and is written by the proof
        lifecycle, not by this legacy compatibility tracker.  Deliberately do
        not touch a database or Supabase client here; repeated missing-table
        errors would be both noisy and misleading.
        """
        log.debug(
            "PERFORMANCE_TRACKER_LOCAL_ONLY client=%s position=%s "
            "canonical_outcome_authority=proof_trades",
            outcome.client_id,
            outcome.position_id,
        )

    def _record_intel_outcome(self, outcome: TradeOutcome) -> None:
        # This path historically sent ticker/signal/P&L guesses into the
        # intelligence plane.  Keep the method for callers that expect it,
        # but make the legacy bridge an explicit no-op with zero training
        # mutation.  Exact proof binding runs only in the background evidence
        # plane through ap.intelligence_outcome_binding.
        log.info(
            "LEGACY_FUZZY_OUTCOME_BINDING_DISABLED client=%s position=%s",
            outcome.client_id,
            outcome.position_id,
        )

    def stats(self, trades: Optional[list[TradeOutcome]] = None) -> dict[str, Any]:
        rows = trades if trades is not None else self.trades
        if not rows:
            return {"total_trades": 0}
        wins = [t for t in rows if t.realized_pnl > 0]
        losses = [t for t in rows if t.realized_pnl <= 0]
        total = len(rows)
        gross_win = sum(t.realized_pnl for t in wins)
        gross_loss = sum(t.realized_pnl for t in losses)
        avg_win = gross_win / len(wins) if wins else 0.0
        avg_loss = gross_loss / len(losses) if losses else 0.0
        win_rate = len(wins) / total if total else 0.0
        expectancy = (win_rate * avg_win) + ((1 - win_rate) * avg_loss)
        profit_factor = gross_win / abs(gross_loss) if gross_loss < 0 else None
        running = 0.0
        peak = 0.0
        max_dd = 0.0
        for t in rows:
            running += t.realized_pnl
            peak = max(peak, running)
            max_dd = min(max_dd, running - peak)
        return {
            "total_trades": total,
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(win_rate, 4),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "expectancy": round(expectancy, 2),
            "profit_factor": round(profit_factor, 3) if profit_factor is not None else None,
            "net_pnl": round(sum(t.realized_pnl for t in rows), 2),
            "max_drawdown": round(max_dd, 2),
        }

    def grouped_stats(self, key: str) -> dict[str, dict[str, Any]]:
        groups: dict[str, list[TradeOutcome]] = {}
        for t in self.trades:
            groups.setdefault(str(getattr(t, key, "") or "UNKNOWN"), []).append(t)
        return {k: self.stats(v) for k, v in groups.items()}


_tracker: Optional[PerformanceTracker] = None


def get_performance_tracker(supabase_client=None) -> PerformanceTracker:
    global _tracker
    if _tracker is None:
        _tracker = PerformanceTracker(supabase_client=supabase_client)
    elif supabase_client is not None and _tracker.sb is None:
        _tracker.sb = supabase_client
    return _tracker


def record_trade_outcome_from_position(position: Any, *, qty_filled: Any = None, fill_price: Any = None, reason: str = "", supabase_client=None) -> TradeOutcome:
    return get_performance_tracker(supabase_client=supabase_client).record_position(position, qty_filled=qty_filled, fill_price=fill_price, reason=reason)


__all__ = ["TradeOutcome", "PerformanceTracker", "get_performance_tracker", "outcome_from_position", "record_trade_outcome_from_position"]
