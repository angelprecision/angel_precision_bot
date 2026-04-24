"""
ap/trace.py — Signal lifecycle tracer
Every signal gets a death certificate or a fill record.

Usage:
    from ap.trace import trace_gate
    trace_gate(signal_id, ticker, "MC_APPROVED", "PASS", score=70.0)
    trace_gate(signal_id, ticker, "IV_GATE", "REJECT", reason="iv_extreme", iv_rank=185.0)
"""

import logging
from typing import Optional

log = logging.getLogger("ap.trace")

# Gate names (ordered by pipeline stage)
GATES = [
    "SIGNAL_RECEIVED",
    "MC_APPROVED",
    "MC_REJECTED",
    "WATCHER_QUEUED",
    "BREACH_CONFIRMED",
    "BREACH_INVALIDATED",
    "CONTRACT_SELECTOR_STARTED",
    "IV_GATE",
    "QUALITY_FILTER",
    "ORDER_CREATED",
    "ORDER_SUBMITTED",
    "ORDER_FILLED",
    "SLIPPAGE_LOGGED",
    "POSITION_OPENED",
    "POSITION_CLOSED",
]


def trace_gate(
    signal_id:    str,
    ticker:       str,
    gate:         str,
    status:       str,          # "PASS" | "REJECT" | "ALLOW" | "FILL" | ...
    reason:       str  = "",
    score:        Optional[float] = None,
    iv_rank:      Optional[float] = None,
    spread_pct:   Optional[float] = None,
    trigger_price: Optional[float] = None,
    contracts:    Optional[int]   = None,
    pnl_pct:      Optional[float] = None,
) -> None:
    """Emit one normalized [TRACE] log line for a signal at a pipeline gate."""
    log.info(
        "[TRACE] signal=%s ticker=%-6s gate=%-28s status=%-8s "
        "reason=%s score=%s iv=%s spread=%s trigger=%s contracts=%s pnl=%s",
        signal_id or "-",
        ticker or "-",
        gate,
        status,
        reason or "-",
        f"{score:.1f}"        if isinstance(score,        (int, float)) else "-",
        f"{iv_rank:.1f}"      if isinstance(iv_rank,      (int, float)) else "-",
        f"{spread_pct:.3f}"   if isinstance(spread_pct,   (int, float)) else "-",
        f"{trigger_price:.2f}" if isinstance(trigger_price, (int, float)) else "-",
        str(contracts)        if contracts is not None                   else "-",
        f"{pnl_pct:.2f}%"    if isinstance(pnl_pct,      (int, float)) else "-",
    )
