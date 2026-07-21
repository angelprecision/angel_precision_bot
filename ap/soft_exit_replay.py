"""Deterministic PEP/LULU/ABT incident replays for PR verification."""
from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from ap_exit_engine import APExitEngine, ManagedPosition, evaluate_exit

ET = ZoneInfo("America/New_York")


def _position(ticker, side, entry, underlying_entry, age_min):
    right = "C" if side == "CALL" else "P"
    pos = ManagedPosition(
        ticker=ticker,
        option_symbol=f"{ticker}260731{right}00100000",
        side=side,
        quantity=1,
        entry_price=entry,
        underlying_entry=underlying_entry,
        underlying_target=0.0,
        underlying_stop=0.0,
        position_id=f"replay-{ticker.lower()}",
        client_id="tradefluence.paper@example.com",
        signal_id=f"replay-signal-{ticker.lower()}",
        execution_mode="paper",
        quantity_remaining=1,
        opened_at=datetime.now(timezone.utc) - timedelta(minutes=age_min),
    )
    pos.executable_bid_soft_exit_truth_enabled = True
    return pos


def _engine(pos):
    engine = APExitEngine.__new__(APExitEngine)
    engine._email = pos.client_id
    engine._lock = threading.RLock()
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    engine._request_immediate_quote_retry = lambda _pos: True
    engine._persist_soft_exit_truth_to_db = lambda _pos, force=False: False
    return engine


def _cycle(engine, pos, bid, ask, underlying, ts):
    cycle_id = f"replay-{pos.ticker}-{ts.timestamp()}"
    engine._qpm_cycle_context = {
        "option_quotes": {
            pos.option_symbol: {
                "bid": bid,
                "ask": ask,
                "mark": (bid + ask) / 2,
                "last": (bid + ask) / 2,
            }
        },
        "underlying_quotes": {
            pos.ticker: ({
                "bid": underlying - 0.02,
                "ask": underlying + 0.02,
                "last": underlying,
            } if underlying else {})
        },
        "option_quote_timestamps": {pos.option_symbol: ts},
        "underlying_quote_timestamps": {pos.ticker: ts},
        "original_modes": {pos.position_id: "paper"},
        "original_modes_by_symbol": {pos.option_symbol: "paper"},
        "quote_provider": "tradier",
        "quote_domain": "tradier_live_market_data",
        "snapshot_timestamp": ts,
        "cycle_id": cycle_id,
    }
    engine.apply_exit_decision_snapshots([{
        "position_id": pos.position_id,
        "option_symbol": pos.option_symbol,
        "ticker": pos.ticker,
        "current_bid": bid,
        "current_ask": ask,
        "current_underlying": underlying,
    }])


def run_replays() -> dict:
    base = datetime.now(timezone.utc)
    output = {}

    pep = _position("PEP", "PUT", 1.70, 136.19, 2)
    pep_engine = _engine(pep)
    _cycle(pep_engine, pep, 1.77, 1.83, 133.47, base)
    _cycle(pep_engine, pep, 1.78, 1.84, 133.47, base + timedelta(seconds=2))
    _cycle(pep_engine, pep, 1.52, 1.58, 133.47, base + timedelta(seconds=4))
    pep_decision = evaluate_exit(pep, (base + timedelta(seconds=4)).astimezone(ET))
    output["PEP"] = {
        "action": pep_decision.action,
        "reason_code": pep_decision.reason_code,
        "broker_exit_submitted": False,
        "executable_bid_pnl_pct": round(pep.option_pnl_pct, 4),
        "underlying_midpoint": pep.current_underlying,
        "touched_profit_armed": pep.touched_profit,
    }

    lulu = _position("LULU", "PUT", 2.60, 300.0, 2)
    lulu_engine = _engine(lulu)
    _cycle(lulu_engine, lulu, 2.28, 2.42, 296.90, base)
    lulu_decision = evaluate_exit(lulu, base.astimezone(ET))
    output["LULU"] = {
        "action": lulu_decision.action,
        "reason_code": lulu_decision.reason_code,
        "broker_exit_submitted": False,
        "executable_bid_pnl_pct": round(lulu.option_pnl_pct, 4),
        "underlying_midpoint": lulu.current_underlying,
    }

    abt = _position("ABT", "CALL", 5.00, 101.68, 30)
    abt_engine = _engine(abt)
    _cycle(abt_engine, abt, 4.50, 4.70, 0.0, base)
    missing = evaluate_exit(abt, base.astimezone(ET))
    _cycle(abt_engine, abt, 4.50, 4.70, 102.70, base + timedelta(seconds=2))
    confirming = evaluate_exit(abt, (base + timedelta(seconds=2)).astimezone(ET))
    _cycle(abt_engine, abt, 4.50, 4.70, 100.80, base + timedelta(seconds=4))
    broken = evaluate_exit(abt, (base + timedelta(seconds=4)).astimezone(ET))
    output["ABT"] = {
        "missing_underlying": {
            "action": missing.action,
            "reason_code": missing.reason_code,
            "broker_exit_submitted": False,
        },
        "fresh_confirming_underlying": {
            "action": confirming.action,
            "reason_code": confirming.reason_code,
            "broker_exit_submitted": False,
        },
        "fresh_nonconfirming_underlying": {
            "action": broken.action,
            "reason_code": broken.reason_code,
            "would_submit_existing_exit_authority": broken.should_act,
        },
    }
    return output


if __name__ == "__main__":
    print(json.dumps(run_replays(), indent=2, sort_keys=True))
