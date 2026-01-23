from ap.config import Config
from ap.models import GateResult
from ap.utils import pct

cfg = Config()

def compute_growth(state: dict) -> float:
    init_eq = float(state["initial_equity_run"])
    cur_eq = float(state["current_equity_last"])
    if init_eq <= 0:
        return 0.0
    return (cur_eq - init_eq) / init_eq

def compute_drawdown(state: dict) -> float:
    init_eq = float(state["initial_equity_run"])
    cur_eq = float(state["current_equity_last"])
    if init_eq <= 0:
        return 0.0
    # drawdown expressed as positive fraction when down
    dd = (init_eq - cur_eq) / init_eq
    return max(0.0, dd)

def effective_limits(state: dict) -> dict:
    growth = compute_growth(state)
    max_trades = cfg.MAX_TRADES_PER_DAY
    pos_pct = cfg.BASE_POSITION_PCT
    profit_cap_state = "normal"
    
    # Skip profit cap logic in PAPER mode
    mode = state.get("mode", "SIM")
    if mode == "PAPER":
        return {
            "growth": growth,
            "max_trades": max_trades,
            "pos_pct": pos_pct,
            "profit_cap_state": "normal",
        }
    
    if growth >= cfg.GROWTH_THROTTLE_90:
        profit_cap_state = "hard_stop"
    elif growth >= cfg.GROWTH_THROTTLE_75:
        max_trades = cfg.REDUCED_MAX_TRADES_75
        profit_cap_state = "throttled"
    elif growth >= cfg.GROWTH_THROTTLE_50:
        pos_pct = cfg.REDUCED_POSITION_PCT_50
        profit_cap_state = "throttled"
    
    return {
        "growth": growth,
        "max_trades": max_trades,
        "pos_pct": pos_pct,
        "profit_cap_state": profit_cap_state,
    }

def run_gates(state: dict, open_positions_count: int) -> GateResult:
    # Get mode for conditional checks
    mode = state.get("mode", "SIM")
    
    # 1) Mode gate
    if mode == "READ_ONLY":
        return GateResult(ok=False, reason="MODE_READ_ONLY")
    
    # 2) Kill switch
    if bool(state.get("kill_switch", False)):
        return GateResult(ok=False, reason="KILL_SWITCH_ON")
    
    # 3) Daily stop
    starting_equity_today = float(state["starting_equity_today"])
    realized_today = float(state["realized_pnl_today"])
    if starting_equity_today > 0:
        if realized_today <= -cfg.DAILY_MAX_LOSS_PCT * starting_equity_today:
            return GateResult(ok=False, reason="DAILY_MAX_LOSS_HIT", details={
                "realized_pnl_today": realized_today,
                "limit": -cfg.DAILY_MAX_LOSS_PCT * starting_equity_today
            })
    
    # 4) Run drawdown gates (skip in PAPER mode for testing)
    if mode != "PAPER":
        dd = compute_drawdown(state)
        if dd >= cfg.DRAWDOWN_KILL:
            return GateResult(ok=False, reason="DRAWDOWN_KILL", details={"drawdown_pct": pct(dd)})
        if dd >= cfg.DRAWDOWN_STOP_DAY:
            return GateResult(ok=False, reason="DRAWDOWN_STOP_DAY", details={"drawdown_pct": pct(dd)})
    
    # 5) Growth throttles / profit cap (already skipped in PAPER mode via effective_limits)
    limits = effective_limits(state)
    if limits["profit_cap_state"] == "hard_stop":
        return GateResult(ok=False, reason="PROFIT_CAP_HARD_STOP", details={"growth_pct": pct(limits["growth"])})
    
    # 6) Trades/day
    trades_today = int(state["trades_taken_today"])
    if trades_today >= limits["max_trades"]:
        return GateResult(ok=False, reason="MAX_TRADES_PER_DAY", details={"trades_today": trades_today, "max": limits["max_trades"]})
    
    # 7) Concurrency
    if open_positions_count >= cfg.MAX_CONCURRENT_POSITIONS:
        return GateResult(ok=False, reason="MAX_CONCURRENT_POSITIONS", details={"open": open_positions_count, "max": cfg.MAX_CONCURRENT_POSITIONS})
    
    return GateResult(ok=True, reason="OK", details=limits)
