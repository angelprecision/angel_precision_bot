# ap/discord_reporter.py (NEW)
"""
Discord trade reporter.
Sends daily summaries of trades taken and P&L to your Discord channel.

Usage:
    from ap.discord_reporter import report_daily_summary
    
    report_daily_summary(
        webhook_url="https://discord.com/api/webhooks/...",
        client_id="default"
    )
"""

import requests
import json
from datetime import datetime, timezone
from typing import Dict, List

from ap.logger import get_logger
from ap.db import conn, run_with_retry

log = get_logger("ap.discord_reporter")


def send_discord_webhook(webhook_url: str, payload: dict) -> bool:
    """
    Send a message to Discord webhook.
    Returns True if successful, False otherwise.
    """
    if not webhook_url:
        log.warning("Discord webhook URL not configured")
        return False
    
    try:
        resp = requests.post(
            webhook_url,
            json=payload,
            timeout=10
        )
        if resp.status_code == 204:
            log.info("Discord webhook sent successfully")
            return True
        else:
            log.warning(f"Discord webhook returned status {resp.status_code}")
            return False
    except Exception as e:
        log.error(f"Failed to send Discord webhook: {e}")
        return False


def get_daily_trades(client_id: str) -> List[dict]:
    """
    Fetch all trades (orders) from today for a client.
    """
    with conn() as c:
        rows = run_with_retry(lambda: c.execute("""
            SELECT * FROM orders
            WHERE client_id = ?
              AND kind = 'ENTRY'
              AND date(created_ts) = date('now')
            ORDER BY created_ts DESC
        """, (client_id,)).fetchall())
    
    return [dict(r) for r in rows]


def get_position_pnl(position_id: str) -> dict:
    """
    Calculate P&L for a closed position.
    """
    with conn() as c:
        pos = run_with_retry(lambda: c.execute(
            "SELECT * FROM positions WHERE id=?",
            (position_id,)
        ).fetchone())
    
    if not pos:
        return {"error": "Position not found"}
    
    pos = dict(pos)
    
    # If position is still open, return unrealized P&L
    if pos["status"] == "OPEN":
        return {
            "position_id": position_id,
            "status": "OPEN",
            "entry_price": float(pos["avg_fill"]),
            "qty": int(pos["qty"]),
            "entry_ts": pos["entry_ts"],
            "error": "Position still open"
        }
    
    # If closed, calculate realized P&L
    if pos["status"] == "CLOSED":
        entry = float(pos["avg_fill"])
        exit_price = float(pos.get("exit_price", 0.0)) or entry
        qty = int(pos["qty"])
        
        # For options, multiply by 100 (contract multiplier)
        pnl = (exit_price - entry) * qty * 100
        pnl_pct = ((exit_price - entry) / entry * 100) if entry > 0 else 0
        
        return {
            "position_id": position_id,
            "status": "CLOSED",
            "entry_price": entry,
            "exit_price": exit_price,
            "qty": qty,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "entry_ts": pos["entry_ts"],
            "exit_ts": pos.get("exit_ts"),
            "exit_reason": pos.get("exit_reason")
        }
    
    return {"error": "Unknown position status"}


def get_daily_summary(client_id: str) -> dict:
    """
    Get today's trading summary for a client.
    """
    with conn() as c:
        # Count trades
        trades_row = run_with_retry(lambda: c.execute("""
            SELECT COUNT(*) as count FROM orders
            WHERE client_id = ?
              AND kind = 'ENTRY'
              AND date(created_ts) = date('now')
        """, (client_id,)).fetchone())
        
        trades_count = int(trades_row["count"]) if trades_row else 0
        
        # Get open positions
        open_row = run_with_retry(lambda: c.execute("""
            SELECT COUNT(*) as count FROM positions
            WHERE client_id = ?
              AND status = 'OPEN'
        """, (client_id,)).fetchone())
        
        open_positions = int(open_row["count"]) if open_row else 0
        
        # Get closed positions today
        closed_rows = run_with_retry(lambda: c.execute("""
            SELECT * FROM positions
            WHERE client_id = ?
              AND status = 'CLOSED'
              AND date(exit_ts) = date('now')
            ORDER BY exit_ts DESC
        """, (client_id,)).fetchall())
        
        closed_positions = [dict(r) for r in closed_rows]
        
        # Calculate P&L
        total_pnl = 0.0
        winning_trades = 0
        losing_trades = 0
        
        for pos in closed_positions:
            entry = float(pos["avg_fill"])
            exit_price = float(pos.get("exit_price", 0.0)) or entry
            qty = int(pos["qty"])
            pnl = (exit_price - entry) * qty * 100
            
            total_pnl += pnl
            if pnl > 0:
                winning_trades += 1
            elif pnl < 0:
                losing_trades += 1
        
        # Get current equity
        state_row = run_with_retry(lambda: c.execute(
            "SELECT current_equity FROM client_state WHERE client_id=?",
            (client_id,)
        ).fetchone())
        
        current_equity = float(state_row["current_equity"]) if state_row and state_row["current_equity"] else 0.0
    
    return {
        "client_id": client_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "trades_taken": trades_count,
        "open_positions": open_positions,
        "closed_today": len(closed_positions),
        "winning_trades": winning_trades,
        "losing_trades": losing_trades,
        "total_pnl": total_pnl,
        "current_equity": current_equity,
        "closed_positions": closed_positions
    }


def format_discord_summary(summary: dict) -> dict:
    """
    Format trading summary as Discord embed message.
    """
    client_id = summary.get("client_id", "default")
    trades = summary.get("trades_taken", 0)
    open_pos = summary.get("open_positions", 0)
    closed = summary.get("closed_today", 0)
    winners = summary.get("winning_trades", 0)
    losers = summary.get("losing_trades", 0)
    pnl = summary.get("total_pnl", 0.0)
    equity = summary.get("current_equity", 0.0)
    
    # Determine color based on P&L
    color = 0x00FF00 if pnl >= 0 else 0xFF0000  # Green if profit, red if loss
    
    embed = {
        "title": f"📊 Daily Trading Summary - {client_id}",
        "color": color,
        "fields": [
            {
                "name": "Trades Taken",
                "value": str(trades),
                "inline": True
            },
            {
                "name": "Open Positions",
                "value": str(open_pos),
                "inline": True
            },
            {
                "name": "Closed Today",
                "value": str(closed),
                "inline": True
            },
            {
                "name": "Winning Trades",
                "value": str(winners),
                "inline": True
            },
            {
                "name": "Losing Trades",
                "value": str(losers),
                "inline": True
            },
            {
                "name": "Win Rate",
                "value": f"{(winners / (winners + losers) * 100) if (winners + losers) > 0 else 0:.1f}%",
                "inline": True
            },
            {
                "name": "Daily P&L",
                "value": f"${pnl:,.2f}",
                "inline": True
            },
            {
                "name": "Current Equity",
                "value": f"${equity:,.2f}",
                "inline": True
            }
        ],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": "Angel Precision Bot"}
    }
    
    return {"embeds": [embed]}


def report_daily_summary(webhook_url: str, client_id: str = "default") -> bool:
    """
    Send daily trading summary to Discord.
    
    Args:
        webhook_url: Discord webhook URL
        client_id: Client identifier (default: "default")
    
    Returns:
        True if successful, False otherwise
    
    Example:
        report_daily_summary(
            webhook_url="https://discord.com/api/webhooks/...",
            client_id="default"
        )
    """
    try:
        summary = get_daily_summary(client_id)
        payload = format_discord_summary(summary)
        return send_discord_webhook(webhook_url, payload)
    except Exception as e:
        log.error(f"Failed to generate daily summary: {e}")
        return False


def report_trade_executed(
    webhook_url: str,
    plan: dict,
    broker_order_id: str = None,
    client_id: str = "default"
) -> bool:
    """
    Send trade executed notification to Discord.
    
    Args:
        webhook_url: Discord webhook URL
        plan: Order plan dict from execution
        broker_order_id: Broker order ID
        client_id: Client identifier
    
    Returns:
        True if successful, False otherwise
    """
    try:
        embed = {
            "title": f"✅ Trade Executed - {plan.get('symbol')}",
            "color": 0x0099FF,
            "fields": [
                {"name": "Symbol", "value": plan.get("symbol"), "inline": True},
                {"name": "Direction", "value": plan.get("direction"), "inline": True},
                {"name": "Contract", "value": plan.get("contract"), "inline": False},
                {"name": "Qty", "value": str(plan.get("qty")), "inline": True},
                {"name": "TP %", "value": f"{plan.get('tp_pct', 0) * 100:.1f}%", "inline": True},
                {"name": "SL %", "value": f"{plan.get('sl_pct', 0) * 100:.1f}%", "inline": True},
                {"name": "Order ID", "value": broker_order_id or "Pending", "inline": False},
                {"name": "Time", "value": datetime.now(timezone.utc).isoformat(), "inline": False},
            ],
            "footer": {"text": f"Client: {client_id}"}
        }
        
        payload = {"embeds": [embed]}
        return send_discord_webhook(webhook_url, payload)
    except Exception as e:
        log.error(f"Failed to send trade notification: {e}")
        return False
