from __future__ import annotations

import hashlib
import json
import logging
import os
import subprocess
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from ap.db import conn, run_with_retry

log = logging.getLogger("ap.observability")

EVENT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS decision_events (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    trade_id TEXT,
    position_id TEXT,
    client_id TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    stage TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason_code TEXT,
    explanation TEXT,
    symbol TEXT,
    contract TEXT,
    setup_type TEXT,
    timeframe TEXT,
    strategy_version TEXT,
    config_hash TEXT,
    git_commit TEXT,
    inputs_json JSONB,
    thresholds_json JSONB,
    context_json JSONB
);
CREATE INDEX IF NOT EXISTS idx_decision_events_run_ts ON decision_events (run_id, ts);
CREATE INDEX IF NOT EXISTS idx_decision_events_reason ON decision_events (reason_code, ts);
CREATE INDEX IF NOT EXISTS idx_decision_events_symbol ON decision_events (symbol, ts);
CREATE INDEX IF NOT EXISTS idx_decision_events_candidate ON decision_events (candidate_id, ts);
"""

TRADE_REVIEW_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS trade_reviews (
    trade_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    position_id TEXT,
    client_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    contract TEXT,
    setup_type TEXT,
    timeframe TEXT,
    entry_ts TIMESTAMPTZ,
    exit_ts TIMESTAMPTZ,
    hold_minutes NUMERIC,
    entry_price NUMERIC,
    exit_price NUMERIC,
    qty INTEGER,
    pnl_dollars NUMERIC,
    pnl_pct NUMERIC,
    mae_pct NUMERIC,
    mfe_pct NUMERIC,
    slippage_entry NUMERIC,
    slippage_exit NUMERIC,
    exit_reason_code TEXT,
    exit_reason_text TEXT,
    strategy_version TEXT,
    config_hash TEXT,
    git_commit TEXT,
    notes TEXT
);
"""

COUNTERFACTUAL_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS rejected_counterfactuals (
    candidate_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    contract TEXT,
    setup_type TEXT,
    timeframe TEXT,
    reject_ts TIMESTAMPTZ NOT NULL,
    reject_stage TEXT NOT NULL,
    reject_reason_code TEXT NOT NULL,
    reject_explanation TEXT,
    hypothetical_entry_price NUMERIC,
    hypothetical_stop_price NUMERIC,
    hypothetical_target_price NUMERIC,
    max_favorable_pct NUMERIC,
    max_adverse_pct NUMERIC,
    result_15m_pct NUMERIC,
    result_30m_pct NUMERIC,
    result_eod_pct NUMERIC,
    would_hit_stop BOOLEAN,
    would_hit_target BOOLEAN,
    would_be_profitable BOOLEAN,
    strategy_version TEXT,
    config_hash TEXT,
    git_commit TEXT
);
"""

REASON_CODES = {
    "ENTRY": [
        "DEDUP_BLOCK", "STALE_SIGNAL", "OVERNIGHT_INVALIDATED", "PRICE_DRIFT_EXCEEDED",
        "SCORE_BELOW_THRESHOLD", "SESSION_RULE_BLOCK",
    ],
    "CONTRACT": [
        # Original codes (preserved for backward compat)
        "NO_CHAIN_DATA", "NO_VALID_EXPIRY", "NO_ATM_STRIKE", "SPREAD_TOO_WIDE",
        "OI_TOO_LOW", "VOLUME_TOO_LOW", "DELTA_OUT_OF_RANGE", "DTE_OUT_OF_RANGE",
        "IV_RANK_TOO_HIGH", "EARNINGS_LOCKOUT", "PREMIUM_CAP_EXCEEDED",
        "TIER_CAP_EXCEEDED", "NO_AFFORDABLE_CONTRACT", "FALLBACK_SPREAD_USED",
        "FALLBACK_OI_USED", "NO_CONTRACT_AFTER_FILTERS",
        # PR P1 — truthful selector failure codes (replaces NO_CHAIN_DATA umbrella)
        "CHAIN_EMPTY",               # chain request succeeded but returned 0 rows
        "CHAIN_FETCH_FAILED",        # chain request threw or returned non-200
        "QUOTE_FETCH_FAILED",        # direct-quote fetch failed / unavailable
        "CHAIN_ROW_ZERO_BID_ASK",    # chain had rows but candidate bid/ask was zero
        "DIRECT_QUOTE_ZERO_BID_ASK", # direct quote revalidation returned zero bid/ask
        "QUOTE_ZERO_BID_ASK",        # queue-facing umbrella for zero-quote failures
        "BID_BELOW_MIN",             # bid present but below pro-quality minimum ($0.10)
        "NO_VALID_PLAYBOOK_DTE_CONTRACT",  # DTE ladder exhausted all buckets
        "UNTRADEABLE_FOR_ACCOUNT_SIZE",    # quality contract found but exceeds budget
    ],
    "RISK": [
        "CAPITAL_UTIL_BLOCK", "POSITION_LIMIT_REACHED", "DAILY_STOP_ACTIVE", "KILL_SWITCH_ACTIVE",
        "MAX_CALLS", "MAX_PUTS", "SECTOR_CAP_BLOCK", "TICKER_CAP_BLOCK",
    ],
    "BROKER": [
        "NO_BROKER_ACK", "BROKER_REJECTED", "BROKER_CANCELED", "BROKER_EXPIRED",
        "STALE_ENTRY_TIMEOUT", "STALE_EXIT_TIMEOUT", "PARTIAL_FILL_STALLED",
        "MANUAL_INTERVENTION_REQUIRED",
    ],
    "EXIT": [
        "TP_SCALE_OUT", "RUNNER_TRAIL", "HARD_STOP", "THETA_STOP", "EOD_FORCE_CLOSE",
        "SENTINEL_FORCED_EXIT", "EXIT_RETRY_ALLOWED", "EXIT_RETRY_BLOCKED",
        "TOUCHED_PROFIT_STOP", "SMALL_WIN_LOCK", "UNDERLYING_PROGRESS_EXIT",
        "NEVER_GREEN_STOP", "PROFIT_LOCK", "TRAILING_STOP",
    ],
}

MFE_MAE_COVERAGE_AUDIT_SQL = """
SELECT
  COUNT(*) AS closed_count,
  COUNT(*) FILTER (
    WHERE meta ? 'mfe_pct'
       OR meta ? 'mae_pct'
       OR meta ? 'mfe_mae_unavailable_reason'
  ) AS covered_count
FROM orders
WHERE status IN ('FILLED','CLOSED','CANCELLED','EXPIRED')
  AND created_ts >= now() - interval '30 days';
"""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_run_id(prefix: str = "ap") -> str:
    return f"{prefix}-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{uuid.uuid4().hex[:8]}"


def new_candidate_id() -> str:
    return uuid.uuid4().hex


_GIT_COMMIT_CACHE: Optional[str] = None

def get_git_commit(default: str = "unknown") -> str:
    global _GIT_COMMIT_CACHE
    if _GIT_COMMIT_CACHE is not None:
        return _GIT_COMMIT_CACHE
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
        )
        _GIT_COMMIT_CACHE = out.decode().strip() or default
    except Exception:
        _GIT_COMMIT_CACHE = default
    return _GIT_COMMIT_CACHE


def make_config_hash(config: dict[str, Any]) -> str:
    try:
        payload = json.dumps(config, sort_keys=True, default=str).encode()
        return hashlib.sha256(payload).hexdigest()
    except Exception:
        return "unknown"


def ensure_observability_tables() -> None:
    stmts = [EVENT_TABLE_SQL, TRADE_REVIEW_TABLE_SQL, COUNTERFACTUAL_TABLE_SQL]
    for stmt in stmts:
        def _fn(sql=stmt):
            with conn() as c:
                c.execute(sql)
        run_with_retry(_fn)


def emit_decision_event(
    *,
    run_id: str,
    candidate_id: str,
    client_id: str,
    stage: str,
    decision: str,
    reason_code: Optional[str] = None,
    explanation: Optional[str] = None,
    trade_id: Optional[str] = None,
    position_id: Optional[str] = None,
    symbol: Optional[str] = None,
    contract: Optional[str] = None,
    setup_type: Optional[str] = None,
    timeframe: Optional[str] = None,
    strategy_version: Optional[str] = None,
    config_hash: Optional[str] = None,
    git_commit: Optional[str] = None,
    inputs: Optional[dict[str, Any]] = None,
    thresholds: Optional[dict[str, Any]] = None,
    context: Optional[dict[str, Any]] = None,
    log_level: int = logging.INFO,
) -> dict[str, Any]:
    event = {
        "run_id": run_id,
        "candidate_id": candidate_id,
        "trade_id": trade_id,
        "position_id": position_id,
        "client_id": client_id,
        "timestamp": utc_now_iso(),
        "stage": stage,
        "decision": decision,
        "reason_code": reason_code,
        "explanation": explanation,
        "symbol": symbol,
        "contract": contract,
        "setup_type": setup_type,
        "timeframe": timeframe,
        "strategy_version": strategy_version or os.getenv("AP_STRATEGY_VERSION", "unknown"),
        "config_hash": config_hash or os.getenv("AP_CONFIG_HASH", "unknown"),
        "git_commit": git_commit or get_git_commit(),
        "inputs": inputs or {},
        "thresholds": thresholds or {},
        "context": context or {},
    }
    log.log(log_level, "DECISION_EVENT %s", json.dumps(event, sort_keys=True, default=str))
    try:
        def _insert():
            with conn() as c:
                c.execute(
                    """
                    INSERT INTO decision_events (
                        run_id, candidate_id, trade_id, position_id, client_id, ts,
                        stage, decision, reason_code, explanation,
                        symbol, contract, setup_type, timeframe,
                        strategy_version, config_hash, git_commit,
                        inputs_json, thresholds_json, context_json
                    ) VALUES (
                        %s, %s, %s, %s, %s, NOW(),
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s
                    )
                    """,
                    (
                        event["run_id"], event["candidate_id"], event["trade_id"], event["position_id"], event["client_id"],
                        event["stage"], event["decision"], event["reason_code"], event["explanation"],
                        event["symbol"], event["contract"], event["setup_type"], event["timeframe"],
                        event["strategy_version"], event["config_hash"], event["git_commit"],
                        json.dumps(event["inputs"], default=str),
                        json.dumps(event["thresholds"], default=str),
                        json.dumps(event["context"], default=str),
                    ),
                )
        run_with_retry(_insert)
    except Exception as e:
        log.debug("emit_decision_event DB insert failed (non-critical): %s", e)
    return event


def upsert_trade_review(payload: dict[str, Any]) -> None:
    git_commit = payload.get("git_commit") or get_git_commit()
    def _fn():
        with conn() as c:
            c.execute(
                """
                INSERT INTO trade_reviews (
                    trade_id, run_id, candidate_id, position_id, client_id,
                    symbol, contract, setup_type, timeframe,
                    entry_ts, exit_ts, hold_minutes,
                    entry_price, exit_price, qty, pnl_dollars, pnl_pct,
                    mae_pct, mfe_pct, slippage_entry, slippage_exit,
                    exit_reason_code, exit_reason_text,
                    strategy_version, config_hash, git_commit, notes
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT (trade_id) DO UPDATE SET
                    exit_ts = EXCLUDED.exit_ts,
                    hold_minutes = EXCLUDED.hold_minutes,
                    exit_price = EXCLUDED.exit_price,
                    pnl_dollars = EXCLUDED.pnl_dollars,
                    pnl_pct = EXCLUDED.pnl_pct,
                    mae_pct = EXCLUDED.mae_pct,
                    mfe_pct = EXCLUDED.mfe_pct,
                    slippage_exit = EXCLUDED.slippage_exit,
                    exit_reason_code = EXCLUDED.exit_reason_code,
                    exit_reason_text = EXCLUDED.exit_reason_text,
                    notes = EXCLUDED.notes
                """,
                (
                    payload.get("trade_id"), payload.get("run_id"), payload.get("candidate_id"), payload.get("position_id"), payload.get("client_id"),
                    payload.get("symbol"), payload.get("contract"), payload.get("setup_type"), payload.get("timeframe"),
                    payload.get("entry_ts"), payload.get("exit_ts"), payload.get("hold_minutes"),
                    payload.get("entry_price"), payload.get("exit_price"), payload.get("qty"), payload.get("pnl_dollars"), payload.get("pnl_pct"),
                    payload.get("mae_pct"), payload.get("mfe_pct"), payload.get("slippage_entry"), payload.get("slippage_exit"),
                    payload.get("exit_reason_code"), payload.get("exit_reason_text"),
                    payload.get("strategy_version"), payload.get("config_hash"), git_commit, payload.get("notes"),
                ),
            )
    run_with_retry(_fn)


def upsert_rejected_counterfactual(payload: dict[str, Any]) -> None:
    git_commit = payload.get("git_commit") or get_git_commit()
    def _fn():
        with conn() as c:
            c.execute(
                """
                INSERT INTO rejected_counterfactuals (
                    candidate_id, run_id, client_id, symbol, contract, setup_type, timeframe,
                    reject_ts, reject_stage, reject_reason_code, reject_explanation,
                    hypothetical_entry_price, hypothetical_stop_price, hypothetical_target_price,
                    max_favorable_pct, max_adverse_pct, result_15m_pct, result_30m_pct, result_eod_pct,
                    would_hit_stop, would_hit_target, would_be_profitable,
                    strategy_version, config_hash, git_commit
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (candidate_id) DO UPDATE SET
                    max_favorable_pct = EXCLUDED.max_favorable_pct,
                    max_adverse_pct = EXCLUDED.max_adverse_pct,
                    result_15m_pct = EXCLUDED.result_15m_pct,
                    result_30m_pct = EXCLUDED.result_30m_pct,
                    result_eod_pct = EXCLUDED.result_eod_pct,
                    would_hit_stop = EXCLUDED.would_hit_stop,
                    would_hit_target = EXCLUDED.would_hit_target,
                    would_be_profitable = EXCLUDED.would_be_profitable
                """,
                (
                    payload.get("candidate_id"), payload.get("run_id"), payload.get("client_id"), payload.get("symbol"), payload.get("contract"), payload.get("setup_type"), payload.get("timeframe"),
                    payload.get("reject_ts") or utc_now_iso(), payload.get("reject_stage"), payload.get("reject_reason_code"), payload.get("reject_explanation"),
                    payload.get("hypothetical_entry_price"), payload.get("hypothetical_stop_price"), payload.get("hypothetical_target_price"),
                    payload.get("max_favorable_pct"), payload.get("max_adverse_pct"), payload.get("result_15m_pct"), payload.get("result_30m_pct"), payload.get("result_eod_pct"),
                    payload.get("would_hit_stop"), payload.get("would_hit_target"), payload.get("would_be_profitable"),
                    payload.get("strategy_version"), payload.get("config_hash"), git_commit,
                ),
            )
    run_with_retry(_fn)


MASTER_CONTROL_EXAMPLE = '''
from ap.observability import emit_decision_event

emit_decision_event(
    run_id=run_id,
    candidate_id=signal_id,
    client_id=client_id,
    stage="capital_utilization",
    decision="REJECT" if blocked else "PASS",
    reason_code="CAPITAL_UTIL_BLOCK" if blocked else None,
    explanation=block_reason if blocked else "Capital gate passed",
    symbol=ticker,
    setup_type=signal.get("pattern"),
    timeframe=signal.get("timeframe"),
    inputs={
        "capital_deployed": deployed,
        "pending_capital": pending,
        "estimated_new_cost": new_cost,
        "projected_total": projected,
    },
    thresholds={
        "capital_limit": limit,
        "max_capital_pct": self.max_capital_pct,
    },
    context={
        "open_count": snap.get("open_count"),
        "pending_entries": snap.get("pending_entries"),
    },
)
'''

CONTRACT_SELECTOR_EXAMPLE = '''
from ap.observability import emit_decision_event

emit_decision_event(
    run_id=run_id,
    candidate_id=getattr(plan, "signal_id", candidate_id),
    client_id=plan.client_id,
    stage="contract_filter",
    decision="REJECT",
    reason_code="PREMIUM_CAP_EXCEEDED",
    explanation=f"Rejected because premium {premium_per_contract:.0f} exceeded max cap {tier_cap:.0f}",
    symbol=plan.ticker,
    contract=option_symbol,
    setup_type=plan.pattern,
    timeframe=plan.timeframe,
    inputs={
        "premium_per_contract": premium_per_contract,
        "spread_pct": spread_pct,
        "oi": oi,
        "volume": volume,
        "iv_rank": iv_rank,
        "dte": dte,
        "delta": delta,
    },
    thresholds={
        "tier_cap": tier_cap,
        "max_spread_pct": max_spread_pct,
        "min_oi": min_oi,
        "min_volume": min_volume,
        "max_iv_rank": max_iv_rank,
    },
    context={
        "fallback_spread_used": fallback_spread_used,
        "fallback_oi_used": fallback_oi_used,
    },
)
'''

ORDER_MONITOR_EXAMPLE = '''
from ap.observability import emit_decision_event

emit_decision_event(
    run_id=os.getenv("AP_RUN_ID", "unknown"),
    candidate_id=local_order_id,
    trade_id=local_order_id,
    position_id=position_id,
    client_id=self.client_id,
    stage="exit_monitor",
    decision="ESCALATE",
    reason_code="STALE_EXIT_TIMEOUT",
    explanation=reason,
    symbol=contract,
    contract=contract,
    inputs={
        "status": status,
        "age_secs": age_secs,
        "broker_order_id": broker_oid,
    },
    thresholds={
        "timeout_exit_pending": TIMEOUT_EXIT_PENDING,
        "timeout_exit_ack": TIMEOUT_EXIT_ACK,
    },
)
'''

EXIT_ENGINE_EXAMPLE = '''
from ap.observability import emit_decision_event

reason_code = {
    "SCALE_OUT": "TP_SCALE_OUT",
    "STOP": "HARD_STOP",
    "CLOSE_ALL": "EOD_FORCE_CLOSE" if "EOD" in decision.reason.upper() else "RUNNER_TRAIL",
}.get(decision.action, None)

emit_decision_event(
    run_id=os.getenv("AP_RUN_ID", "unknown"),
    candidate_id=pos.signal_id or pos.position_id,
    trade_id=pos.position_id,
    position_id=pos.position_id,
    client_id=pos.client_id or self.email or "default",
    stage="exit_decision",
    decision="SUBMIT" if decision.should_act else "PASS",
    reason_code=reason_code,
    explanation=decision.reason,
    symbol=pos.ticker,
    contract=pos.option_symbol,
    inputs={
        "option_pnl_pct": pos.option_pnl_pct,
        "peak_pnl_pct": pos.peak_pnl_pct,
        "qty_remaining": pos.quantity_remaining,
        "scale_outs_done": pos.scale_outs_done,
    },
)
'''
