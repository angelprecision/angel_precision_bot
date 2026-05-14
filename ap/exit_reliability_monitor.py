# ap/exit_reliability_monitor.py
# =============================================================================
# Exit Reliability Monitor
# =============================================================================
# Reads exit_decision_ledger and returns the positions that need attention.
# This is designed for admin dashboards, Discord alerts, and pre-live readiness.
# =============================================================================

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

log = logging.getLogger("ap.exit_reliability_monitor")


@dataclass
class ExitReliabilityIssue:
    severity: str
    reason: str
    client_id: str
    position_id: str
    contract: str
    ticker: str
    age_sec: float = 0.0
    peak_pnl_pct: float = 0.0
    option_pnl_pct: float = 0.0
    decision_action: str = ""
    local_order_id: str = ""
    broker_order_id: str = ""
    details: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "severity": self.severity,
            "reason": self.reason,
            "client_id": self.client_id,
            "position_id": self.position_id,
            "contract": self.contract,
            "ticker": self.ticker,
            "age_sec": self.age_sec,
            "peak_pnl_pct": self.peak_pnl_pct,
            "option_pnl_pct": self.option_pnl_pct,
            "decision_action": self.decision_action,
            "local_order_id": self.local_order_id,
            "broker_order_id": self.broker_order_id,
            "details": self.details or {},
        }


def _rows(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    try:
        from ap.db import conn, run_with_retry
    except Exception as exc:
        log.warning("DB layer unavailable for exit reliability monitor: %s", exc)
        return []

    def _fn():
        with conn() as c:
            c.execute(sql, params)
            return c.fetchall()

    try:
        return run_with_retry(_fn) or []
    except Exception as exc:
        log.warning("exit reliability query failed: %s", exc)
        return []


def find_profitable_positions_without_exit_decision(
    *,
    min_peak_pnl_pct: float = 0.15,
    lookback_minutes: int = 240,
) -> list[ExitReliabilityIssue]:
    """Positions that recorded green peak but no non-HOLD exit decision afterward."""
    sql = """
    WITH peaked AS (
        SELECT DISTINCT ON (position_id)
            position_id, client_id, ticker, contract,
            peak_pnl_pct, option_pnl_pct, created_at
        FROM exit_decision_ledger
        WHERE created_at >= NOW() - (%s || ' minutes')::interval
          AND COALESCE(peak_pnl_pct, 0) >= %s
          AND COALESCE(position_id, '') <> ''
        ORDER BY position_id, created_at DESC
    ), acted AS (
        SELECT DISTINCT position_id
        FROM exit_decision_ledger
        WHERE created_at >= NOW() - (%s || ' minutes')::interval
          AND decision_action IN ('CLOSE_ALL', 'SCALE_OUT', 'STOP')
    )
    SELECT p.*,
           EXTRACT(EPOCH FROM (NOW() - p.created_at)) AS age_sec
    FROM peaked p
    LEFT JOIN acted a ON a.position_id = p.position_id
    WHERE a.position_id IS NULL
    ORDER BY p.peak_pnl_pct DESC
    LIMIT 50
    """
    issues = []
    for row in _rows(sql, (lookback_minutes, min_peak_pnl_pct, lookback_minutes)):
        issues.append(ExitReliabilityIssue(
            severity="HIGH",
            reason="green_peak_without_exit_decision",
            client_id=str(row.get("client_id") or ""),
            position_id=str(row.get("position_id") or ""),
            contract=str(row.get("contract") or ""),
            ticker=str(row.get("ticker") or ""),
            age_sec=float(row.get("age_sec") or 0),
            peak_pnl_pct=float(row.get("peak_pnl_pct") or 0),
            option_pnl_pct=float(row.get("option_pnl_pct") or 0),
            details=dict(row),
        ))
    return issues


def find_exit_decisions_without_order(
    *,
    stale_after_sec: int = 10,
    lookback_minutes: int = 240,
) -> list[ExitReliabilityIssue]:
    """Exit decisions that did not produce any later submit/ack/fill event."""
    sql = """
    WITH decisions AS (
        SELECT DISTINCT ON (position_id)
            position_id, client_id, ticker, contract, created_at,
            decision_action, decision_reason, decision_reason_code,
            peak_pnl_pct, option_pnl_pct
        FROM exit_decision_ledger
        WHERE created_at >= NOW() - (%s || ' minutes')::interval
          AND decision_action IN ('CLOSE_ALL', 'SCALE_OUT', 'STOP')
          AND COALESCE(position_id, '') <> ''
        ORDER BY position_id, created_at DESC
    ), orders AS (
        SELECT DISTINCT position_id
        FROM exit_decision_ledger
        WHERE created_at >= NOW() - (%s || ' minutes')::interval
          AND event_type IN ('EXIT_ORDER_SUBMITTED', 'EXIT_ORDER_ACK', 'EXIT_ORDER_FILLED', 'EXIT_ORDER_REJECTED')
    )
    SELECT d.*,
           EXTRACT(EPOCH FROM (NOW() - d.created_at)) AS age_sec
    FROM decisions d
    LEFT JOIN orders o ON o.position_id = d.position_id
    WHERE o.position_id IS NULL
      AND EXTRACT(EPOCH FROM (NOW() - d.created_at)) >= %s
    ORDER BY d.created_at DESC
    LIMIT 50
    """
    issues = []
    for row in _rows(sql, (lookback_minutes, lookback_minutes, stale_after_sec)):
        issues.append(ExitReliabilityIssue(
            severity="CRITICAL",
            reason="exit_decision_without_order_event",
            client_id=str(row.get("client_id") or ""),
            position_id=str(row.get("position_id") or ""),
            contract=str(row.get("contract") or ""),
            ticker=str(row.get("ticker") or ""),
            age_sec=float(row.get("age_sec") or 0),
            peak_pnl_pct=float(row.get("peak_pnl_pct") or 0),
            option_pnl_pct=float(row.get("option_pnl_pct") or 0),
            decision_action=str(row.get("decision_action") or ""),
            details=dict(row),
        ))
    return issues


def find_stale_exit_orders(
    *,
    stale_after_sec: int = 20,
    lookback_minutes: int = 240,
) -> list[ExitReliabilityIssue]:
    """Exit orders submitted/acked but not filled or rejected after threshold."""
    sql = """
    WITH latest_order AS (
        SELECT DISTINCT ON (position_id)
            position_id, client_id, ticker, contract, created_at,
            event_type, decision_action, peak_pnl_pct, option_pnl_pct,
            local_order_id, broker_order_id, broker_status
        FROM exit_decision_ledger
        WHERE created_at >= NOW() - (%s || ' minutes')::interval
          AND event_type IN ('EXIT_ORDER_SUBMITTED', 'EXIT_ORDER_ACK')
          AND COALESCE(position_id, '') <> ''
        ORDER BY position_id, created_at DESC
    ), terminal AS (
        SELECT DISTINCT position_id
        FROM exit_decision_ledger
        WHERE created_at >= NOW() - (%s || ' minutes')::interval
          AND event_type IN ('EXIT_ORDER_FILLED', 'EXIT_ORDER_REJECTED', 'EXIT_ORDER_CANCELED')
    )
    SELECT lo.*,
           EXTRACT(EPOCH FROM (NOW() - lo.created_at)) AS age_sec
    FROM latest_order lo
    LEFT JOIN terminal t ON t.position_id = lo.position_id
    WHERE t.position_id IS NULL
      AND EXTRACT(EPOCH FROM (NOW() - lo.created_at)) >= %s
    ORDER BY age_sec DESC
    LIMIT 50
    """
    issues = []
    for row in _rows(sql, (lookback_minutes, lookback_minutes, stale_after_sec)):
        issues.append(ExitReliabilityIssue(
            severity="CRITICAL",
            reason="stale_exit_order_without_terminal_event",
            client_id=str(row.get("client_id") or ""),
            position_id=str(row.get("position_id") or ""),
            contract=str(row.get("contract") or ""),
            ticker=str(row.get("ticker") or ""),
            age_sec=float(row.get("age_sec") or 0),
            peak_pnl_pct=float(row.get("peak_pnl_pct") or 0),
            option_pnl_pct=float(row.get("option_pnl_pct") or 0),
            decision_action=str(row.get("decision_action") or ""),
            local_order_id=str(row.get("local_order_id") or ""),
            broker_order_id=str(row.get("broker_order_id") or ""),
            details=dict(row),
        ))
    return issues


def exit_reliability_snapshot() -> dict[str, Any]:
    """Dashboard-friendly snapshot."""
    issues: list[ExitReliabilityIssue] = []
    issues.extend(find_profitable_positions_without_exit_decision())
    issues.extend(find_exit_decisions_without_order())
    issues.extend(find_stale_exit_orders())
    return {
        "ok": not any(i.severity in {"HIGH", "CRITICAL"} for i in issues),
        "critical_count": sum(1 for i in issues if i.severity == "CRITICAL"),
        "high_count": sum(1 for i in issues if i.severity == "HIGH"),
        "issues": [i.as_dict() for i in issues],
    }
