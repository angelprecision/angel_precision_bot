"""
ap.operator.live_execution_journal — PR #90
============================================

READ-ONLY journal over the live-execution truth tables we already collect.
This module is the single source of truth for two questions:

  1. "Given a window, what happened to every live signal from arrival to
     Tradier exit fill?"
  2. "Is this trade officially eligible for live-performance accounting?"

It NEVER inserts, updates, or deletes. It NEVER mutates the trading hot
path. It NEVER touches the scanner, scorer, watcher, sizer, contract
selector, broker submit, or risk gate. It only READS from:

  * ap_signals               (signal arrival metadata)
  * client_signal_opportunities (per-client lifecycle ledger)
  * orders                   (entry/exit order status + broker ids)
  * proof_trades             (closed trade performance)

Tradier Exit Proof Lock (per PR-90 spec, rule §2):

  A live trade is "official" ONLY when ALL of the following are true:

    execution_mode == 'live'
    broker_reconciled is True
    synthetic_entry is False
    entry_price_source == 'TRADIER_ENTRY_FILL'
    exit_price_source in ('TRADIER_EXIT_FILL', 'MANUAL_REPAIR_TRADIER_FILL')
    broker_entry_order_id is not None / not empty
    broker_exit_order_id  is not None / not empty
    broker_entry_filled_qty > 0
    broker_exit_filled_qty  > 0
    entry_option_price   is finite > 0   (entry fill price)
    exit_fill_price      is finite > 0   (exit fill price)

  Anything less => unofficial. The DB DEFAULT FALSE on
  proof_trades.official_live_performance_eligible is the safety floor.

This file is intentionally small. The classifier is a pure function so it
can be reused by future PR-91 (daily archive) and PR-92 (weekly review)
without duplicating logic.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

log = logging.getLogger("ap.operator.live_execution_journal")


# ---------------------------------------------------------------------------
# Enumerations (string constants kept tight so callers can compare cleanly)
# ---------------------------------------------------------------------------

# Allowed *_price_source values. App-side enforcement; DB column is plain TEXT
# so we can add new values without a migration.
PRICE_SOURCE_TRADIER_ENTRY   = "TRADIER_ENTRY_FILL"
PRICE_SOURCE_TRADIER_EXIT    = "TRADIER_EXIT_FILL"
PRICE_SOURCE_PAPER_BROKER    = "PAPER_BROKER_FILL"
PRICE_SOURCE_MANUAL_REPAIR   = "MANUAL_REPAIR_TRADIER_FILL"
PRICE_SOURCE_MISSING_ENTRY   = "MISSING_BROKER_ENTRY_FILL"
PRICE_SOURCE_MISSING_EXIT    = "MISSING_BROKER_EXIT_FILL"
PRICE_SOURCE_LEGACY_UNKNOWN  = "LEGACY_UNKNOWN"

VALID_PRICE_SOURCES = {
    PRICE_SOURCE_TRADIER_ENTRY,
    PRICE_SOURCE_TRADIER_EXIT,
    PRICE_SOURCE_PAPER_BROKER,
    PRICE_SOURCE_MANUAL_REPAIR,
    PRICE_SOURCE_MISSING_ENTRY,
    PRICE_SOURCE_MISSING_EXIT,
    PRICE_SOURCE_LEGACY_UNKNOWN,
}

# Lifecycle stages per spec. Order matters: later stages override earlier
# ones when classifying current_lifecycle_stage.
LIFECYCLE_ORDER = [
    "SIGNAL_RECEIVED",
    "OPPORTUNITY_CREATED",
    "PREFLIGHT_PASSED",
    "PREFLIGHT_BLOCKED",
    "ORDER_CREATED",
    "WATCHER_ARMED",
    "ENTRY_SUBMITTED",
    "ENTRY_ACKED",
    "ENTRY_FILLED",
    "EXIT_SUBMITTED",
    "EXIT_ACKED",
    "EXIT_FILLED",
    "CLOSED",
    "FAILED",
    "UNKNOWN",
]
LIFECYCLE_RANK = {s: i for i, s in enumerate(LIFECYCLE_ORDER)}


# ---------------------------------------------------------------------------
# Tradier Exit Proof Lock — single classification function (pure, testable)
# ---------------------------------------------------------------------------

@dataclass
class OfficialClassification:
    """Result of classify_official() — read-only, no mutation of inputs."""
    is_official: bool
    reasons_unofficial: list[str] = field(default_factory=list)


def _is_pos_finite(x: Any) -> bool:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return False
    return math.isfinite(v) and v > 0.0


def classify_official(row: dict[str, Any]) -> OfficialClassification:
    """Apply the Tradier Exit Proof Lock to a single trade row.

    `row` must be a dict-like with proof_trades columns + (optionally) the
    joined order columns. Missing keys => treated as None (i.e. fail
    closed). NEVER returns True when any required field is missing.

    Returns OfficialClassification with the boolean verdict + a list of
    the specific reasons the trade did not qualify (empty if official).
    """
    reasons: list[str] = []

    # 1. execution_mode must be 'live'
    if (row.get("execution_mode") or "").lower() != "live":
        reasons.append("execution_mode != 'live'")

    # 2. broker_reconciled must be True
    if not bool(row.get("broker_reconciled")):
        reasons.append("broker_reconciled is False")

    # 3. synthetic_entry must be False / None / 0
    if bool(row.get("synthetic_entry")):
        reasons.append("synthetic_entry is True")

    # 4. entry_price_source must be Tradier
    entry_src = (row.get("entry_price_source") or "").upper()
    if entry_src != PRICE_SOURCE_TRADIER_ENTRY:
        reasons.append(f"entry_price_source != TRADIER_ENTRY_FILL ({entry_src or 'NULL'})")

    # 5. exit_price_source must be Tradier OR a manual repair from a Tradier fill
    exit_src = (row.get("exit_price_source") or "").upper()
    if exit_src not in (PRICE_SOURCE_TRADIER_EXIT, PRICE_SOURCE_MANUAL_REPAIR):
        reasons.append(f"exit_price_source != TRADIER_EXIT_FILL/MANUAL_REPAIR ({exit_src or 'NULL'})")

    # 6. broker_entry_order_id present (non-empty string)
    if not (row.get("broker_entry_order_id") or "").strip():
        reasons.append("broker_entry_order_id missing")

    # 7. broker_exit_order_id present
    if not (row.get("broker_exit_order_id") or "").strip():
        reasons.append("broker_exit_order_id missing")

    # 8 + 9. broker_*_filled_qty > 0
    for k in ("broker_entry_filled_qty", "broker_exit_filled_qty"):
        v = row.get(k)
        try:
            if v is None or float(v) <= 0:
                reasons.append(f"{k} missing or zero")
        except (TypeError, ValueError):
            reasons.append(f"{k} not numeric")

    # 10. entry fill price (proof_trades.entry_option_price) > 0
    if not _is_pos_finite(row.get("entry_option_price")):
        reasons.append("entry_option_price missing or non-positive")

    # 11. exit fill price (proof_trades.exit_fill_price) > 0
    if not _is_pos_finite(row.get("exit_fill_price")):
        reasons.append("exit_fill_price missing or non-positive")

    return OfficialClassification(
        is_official=not reasons,
        reasons_unofficial=reasons,
    )


# ---------------------------------------------------------------------------
# Lifecycle derivation
# ---------------------------------------------------------------------------

# Block-reason -> failure-stage map for client_signal_opportunities rows.
# Keys are lowercase substrings; first match wins.
_FAILURE_STAGE_BY_REASON = [
    ("preflight",           "PREFLIGHT_BLOCKED"),
    ("watcher",             "WATCHER_ARMED"),
    ("contract_selector",   "ORDER_CREATED"),
    ("broker_rejected",     "ENTRY_SUBMITTED"),
    ("startup_cleanup",     "ORDER_CREATED"),
    ("capital_limit",       "PREFLIGHT_BLOCKED"),
    ("max_positions",       "PREFLIGHT_BLOCKED"),
    ("sector_cap",          "PREFLIGHT_BLOCKED"),
    ("ticker_cap",          "PREFLIGHT_BLOCKED"),
]


def derive_lifecycle_stage(
    *,
    opp_status: Optional[str],
    order_status: Optional[str],
    exit_order_status: Optional[str],
    proof_closed: bool,
    has_entry_fill: bool,
    has_exit_fill: bool,
) -> str:
    """Pure function: pick the LATEST lifecycle stage we can prove from
    the data. Never returns a stage we don't have evidence for."""
    if proof_closed:
        # A proof_trades row means the close was logged. If the exit fill
        # price made it into proof_trades we're EXIT_FILLED; otherwise the
        # trade closed but the broker fill never reconciled — CLOSED is the
        # honest label.
        return "EXIT_FILLED" if has_exit_fill else "CLOSED"
    if exit_order_status:
        s = exit_order_status.upper()
        if s in ("FILLED", "PARTIALLY_FILLED"):
            return "EXIT_FILLED"
        if s in ("ACKNOWLEDGED", "ACK", "ACKED"):
            return "EXIT_ACKED"
        if s in ("SUBMITTED", "OPEN", "PENDING"):
            return "EXIT_SUBMITTED"
    if has_entry_fill:
        return "ENTRY_FILLED"
    if order_status:
        s = order_status.upper()
        if s in ("FILLED", "PARTIALLY_FILLED"):
            return "ENTRY_FILLED"
        if s in ("ACKNOWLEDGED", "ACK", "ACKED"):
            return "ENTRY_ACKED"
        if s in ("SUBMITTED", "OPEN", "PENDING"):
            return "ENTRY_SUBMITTED"
        if s in ("CANCELED", "CANCELLED", "REJECTED"):
            return "FAILED"
        if s == "PENDING_TRIGGER":
            return "WATCHER_ARMED"
        if s == "CREATED":
            return "ORDER_CREATED"
    if opp_status:
        s = opp_status.upper()
        if s in ("WATCHER_ARMED",):
            return "WATCHER_ARMED"
        if s in ("WATCHER_INVALIDATED",):
            return "FAILED"
        if s in ("CONTRACT_SELECTED", "ORDER_CREATED"):
            return "ORDER_CREATED"
        if s in ("CLIENT_BLOCKED", "STARTUP_CLEANUP_CANCELED_STALE_ORPHAN"):
            return "PREFLIGHT_BLOCKED"
        if s in ("CLIENT_ELIGIBLE", "PREFLIGHT_PASSED"):
            return "PREFLIGHT_PASSED"
        if s == "CREATED":
            return "OPPORTUNITY_CREATED"
    return "UNKNOWN"


def derive_failure_stage_reason(
    *, opp_block_reason: Optional[str], order_last_error: Optional[str],
) -> tuple[Optional[str], Optional[str]]:
    """Determine the (failure_stage, failure_reason) tuple. Returns
    (None, None) when no failure is recorded."""
    candidate = (order_last_error or opp_block_reason or "").strip()
    if not candidate:
        return None, None
    candidate_low = candidate.lower()
    for needle, stage in _FAILURE_STAGE_BY_REASON:
        if needle in candidate_low:
            return stage, candidate
    return "UNKNOWN", candidate


# ---------------------------------------------------------------------------
# Stable client display id (hashed) — preserves PII isolation per spec §6
# ---------------------------------------------------------------------------

def client_display_id(client_id: Optional[str]) -> Optional[str]:
    """Stable 12-char hex prefix of sha256(client_id). Same client always
    maps to the same display id. Admin endpoint still returns raw
    client_id (admin auth required), but UI/dashboards can use this."""
    if not client_id:
        return None
    return hashlib.sha256(client_id.strip().lower().encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Database read paths
# ---------------------------------------------------------------------------

_TRADE_BASE_SELECT = """
SELECT
    o.local_order_id,
    o.client_id,
    o.signal_id,
    o.canonical_signal_id,
    o.symbol,
    o.direction,
    o.score,
    o.tier,
    o.pattern,
    o.timeframe,
    o.qty,
    o.position_id                              AS entry_position_id,
    o.contract                                 AS selected_contract,
    o.status                                   AS entry_order_status,
    o.broker_order_id                          AS broker_entry_order_id_orders,
    o.execution_mode                           AS order_execution_mode,
    o.created_ts                               AS opportunity_created_at,
    o.last_error                               AS order_last_error,
    o.meta                                     AS order_meta,
    exit_o.status                              AS exit_order_status,
    exit_o.broker_order_id                     AS broker_exit_order_id_orders,
    exit_o.last_error                          AS exit_order_last_error,
    p.id                                       AS proof_id,
    p.execution_mode                           AS proof_execution_mode,
    p.broker_reconciled,
    p.synthetic_entry,
    p.entry_option_price,
    p.exit_fill_price,
    p.opened_at                                AS entry_fill_ts,
    p.closed_at                                AS exit_fill_ts,
    p.contracts                                AS proof_contracts,
    p.option_pnl_pct                           AS realized_pnl_pct,
    p.exit_reason,
    p.entry_price_source,
    p.exit_price_source,
    p.broker_entry_order_id                    AS broker_entry_order_id_proof,
    p.broker_exit_order_id                     AS broker_exit_order_id_proof,
    p.broker_entry_fill_ts,
    p.broker_exit_fill_ts,
    p.broker_entry_filled_qty,
    p.broker_exit_filled_qty,
    p.official_live_performance_eligible       AS official_db_default,
    cso.opportunity_status                     AS opp_status,
    cso.miss_stage                             AS opp_miss_stage,
    cso.miss_reason                            AS opp_miss_reason,
    cso.block_reason                           AS opp_block_reason,
    cso.scanner_name                           AS scanner,
    cso.created_ts                             AS opportunity_ledger_created_at,
    m.execution_pod                            AS member_execution_pod
FROM orders o
LEFT JOIN proof_trades p
    ON p.local_order_id = o.local_order_id
LEFT JOIN orders exit_o
    ON exit_o.position_id = o.position_id
   AND exit_o.kind = 'EXIT'
LEFT JOIN client_signal_opportunities cso
    ON cso.canonical_signal_id =
       COALESCE(o.canonical_signal_id, o.signal_id)
   AND cso.client_id = o.client_id
LEFT JOIN members m
    ON m.client_id = o.client_id
WHERE o.kind = 'ENTRY'
"""


def _build_journal_query(
    *,
    start_iso: str,
    end_iso: str,
    client_id: Optional[str],
    symbol: Optional[str],
    canonical_signal_id: Optional[str],
) -> tuple[str, list[Any]]:
    sql = _TRADE_BASE_SELECT
    args: list[Any] = []
    sql += " AND o.created_ts >= %s::timestamptz "
    args.append(start_iso)
    sql += " AND o.created_ts <  %s::timestamptz "
    args.append(end_iso)
    if client_id:
        sql += " AND o.client_id = %s "
        args.append(client_id)
    if symbol:
        sql += " AND UPPER(o.symbol) = UPPER(%s) "
        args.append(symbol)
    if canonical_signal_id:
        sql += " AND COALESCE(o.canonical_signal_id, o.signal_id) = %s "
        args.append(canonical_signal_id)
    sql += " ORDER BY o.created_ts ASC LIMIT 5000 "
    return sql, args


# ---------------------------------------------------------------------------
# Row shaping
# ---------------------------------------------------------------------------

def _shape_trade_row(row: dict[str, Any]) -> dict[str, Any]:
    """Build a single journal trade entry from a joined DB row.

    The output mirrors the spec's trade-row schema exactly. Missing values
    are returned as None — never faked. Caller is responsible for adding
    data_quality warnings for missing required fields.
    """
    # Prefer proof_trades broker ids; fall back to the matching orders row
    # (entry: o.broker_order_id; exit: exit_o.broker_order_id via LEFT JOIN).
    broker_entry_order_id = (
        row.get("broker_entry_order_id_proof")
        or row.get("broker_entry_order_id_orders")
        or None
    )
    broker_exit_order_id = (
        row.get("broker_exit_order_id_proof")
        or row.get("broker_exit_order_id_orders")
        or None
    )

    # Exit-order status from LEFT JOIN. Read-only: returns None when no
    # EXIT-kind row exists yet (e.g. open positions, or legacy rows where
    # the exit was never persisted). _data_quality surfaces the warning.
    exit_order_status = row.get("exit_order_status")

    # ---- pod_id derivation chain ----
    # 1) orders.meta->>'pod_id' (writer-stamped at order creation time)
    # 2) members.execution_pod (canonical per-client assignment)
    # client_signal_opportunities has no pod_id column in current schema, so
    # it is not part of the chain. Returns None if neither source resolves.
    pod_id_value: Optional[str] = None
    om = row.get("order_meta")
    if isinstance(om, dict):
        v = om.get("pod_id")
        if isinstance(v, str) and v.strip():
            pod_id_value = v.strip()
    elif isinstance(om, str) and om.strip():
        # meta may arrive as a JSON string in some code paths
        try:
            parsed = json.loads(om)
            if isinstance(parsed, dict):
                v = parsed.get("pod_id")
                if isinstance(v, str) and v.strip():
                    pod_id_value = v.strip()
        except (ValueError, TypeError):
            pass
    if not pod_id_value:
        m_pod = row.get("member_execution_pod")
        if isinstance(m_pod, str) and m_pod.strip():
            pod_id_value = m_pod.strip()

    # Execution mode: prefer the proof row (closed truth); fall back to
    # the order's stamped mode.
    execution_mode = (
        row.get("proof_execution_mode")
        or row.get("order_execution_mode")
        or "unknown"
    )

    has_entry_fill = _is_pos_finite(row.get("entry_option_price"))
    has_exit_fill  = _is_pos_finite(row.get("exit_fill_price"))
    proof_closed   = row.get("proof_id") is not None

    lifecycle = derive_lifecycle_stage(
        opp_status=row.get("opp_status"),
        order_status=row.get("entry_order_status"),
        exit_order_status=exit_order_status,
        proof_closed=proof_closed,
        has_entry_fill=has_entry_fill,
        has_exit_fill=has_exit_fill,
    )
    failure_stage, failure_reason = derive_failure_stage_reason(
        opp_block_reason=row.get("opp_block_reason"),
        order_last_error=row.get("order_last_error"),
    )

    # Build the classification input. Fall back to orders.broker_order_id
    # so a row with an order but no proof yet still gets classified
    # against the proof lock (it will be unofficial, as expected).
    classify_input = {
        "execution_mode":           execution_mode,
        "broker_reconciled":        row.get("broker_reconciled"),
        "synthetic_entry":          row.get("synthetic_entry"),
        "entry_price_source":       row.get("entry_price_source"),
        "exit_price_source":        row.get("exit_price_source"),
        "broker_entry_order_id":    broker_entry_order_id,
        "broker_exit_order_id":     broker_exit_order_id,
        "broker_entry_filled_qty":  row.get("broker_entry_filled_qty"),
        "broker_exit_filled_qty":   row.get("broker_exit_filled_qty"),
        "entry_option_price":       row.get("entry_option_price"),
        "exit_fill_price":          row.get("exit_fill_price"),
    }
    verdict = classify_official(classify_input)

    return {
        "canonical_signal_id":      row.get("canonical_signal_id") or row.get("signal_id"),
        "signal_id":                row.get("signal_id"),
        "client_id":                row.get("client_id"),
        "client_display_id":        client_display_id(row.get("client_id")),
        "pod_id":                   pod_id_value,
        "symbol":                   row.get("symbol"),
        "direction":                row.get("direction"),
        "pattern":                  row.get("pattern"),
        "timeframe":                row.get("timeframe"),
        "score":                    row.get("score"),
        "tier":                     row.get("tier"),
        "scanner":                  row.get("scanner"),
        "signal_received_at":       _iso(row.get("opportunity_ledger_created_at")),
        "opportunity_created_at":   _iso(row.get("opportunity_created_at")),
        "preflight_status":         row.get("opp_status"),
        "preflight_reason":         row.get("opp_block_reason"),
        "order_local_id":           row.get("local_order_id"),
        "broker_entry_order_id":    broker_entry_order_id,
        "broker_exit_order_id":     broker_exit_order_id,
        "selected_contract":        row.get("selected_contract"),
        "qty":                      row.get("qty"),
        "entry_order_status":       row.get("entry_order_status"),
        "exit_order_status":        exit_order_status,
        "entry_fill_price":         _safe_float(row.get("entry_option_price")),
        "exit_fill_price":          _safe_float(row.get("exit_fill_price")),
        "entry_fill_qty":           _safe_float(row.get("broker_entry_filled_qty")),
        "exit_fill_qty":            _safe_float(row.get("broker_exit_filled_qty")),
        "entry_fill_ts":            _iso(row.get("entry_fill_ts") or row.get("broker_entry_fill_ts")),
        "exit_fill_ts":             _iso(row.get("exit_fill_ts") or row.get("broker_exit_fill_ts")),
        "entry_price_source":       row.get("entry_price_source"),
        "exit_price_source":        row.get("exit_price_source"),
        "broker_reconciled":        bool(row.get("broker_reconciled")) if row.get("broker_reconciled") is not None else None,
        "official_live_performance_eligible": verdict.is_official,
        "official_unofficial_reasons":        verdict.reasons_unofficial,
        "realized_pnl_pct":         _safe_float(row.get("realized_pnl_pct")),
        "exit_reason":              row.get("exit_reason"),
        "current_lifecycle_stage":  lifecycle,
        "failure_stage":            failure_stage,
        "failure_reason":           failure_reason,
        "execution_mode":           execution_mode,
    }


def _safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _iso(x: Any) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, str):
        return x
    if isinstance(x, datetime):
        return x.astimezone(timezone.utc).isoformat()
    return str(x)


# ---------------------------------------------------------------------------
# Action items (deterministic, narrow per spec §5)
# ---------------------------------------------------------------------------

def derive_action_items(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Per spec: only concrete live-execution/proof issues. No vague
    recommendations. Each item carries an actionable count and an
    affected_ids list (capped at 50 to keep the response small)."""
    cap = 50

    def _ids_where(predicate):
        ids = []
        count = 0
        for t in trades:
            if predicate(t):
                count += 1
                if len(ids) < cap:
                    ids.append(t.get("canonical_signal_id") or t.get("signal_id"))
        return count, ids

    items: list[dict[str, Any]] = []

    # 1. Missing Tradier exit fill (LIVE only)
    n, ids = _ids_where(
        lambda t: t.get("execution_mode") == "live"
        and t.get("current_lifecycle_stage") in ("EXIT_FILLED", "CLOSED")
        and (t.get("exit_price_source") or "").upper() in (
            "", PRICE_SOURCE_MISSING_EXIT, PRICE_SOURCE_LEGACY_UNKNOWN,
        )
    )
    if n:
        items.append({
            "code": "MISSING_TRADIER_EXIT_FILL",
            "message": f"{n} live trade(s) closed without a Tradier exit fill source. "
                       "Run reconciliation against broker history.",
            "affected_count": n,
            "affected_ids": ids,
        })

    # 2. broker_reconciled=false on closed live trades
    n, ids = _ids_where(
        lambda t: t.get("execution_mode") == "live"
        and t.get("current_lifecycle_stage") in ("EXIT_FILLED", "CLOSED")
        and t.get("broker_reconciled") is False
    )
    if n:
        items.append({
            "code": "LIVE_TRADE_UNRECONCILED",
            "message": f"{n} live trade(s) closed without broker reconciliation. "
                       "Re-run reconciler on these positions.",
            "affected_count": n,
            "affected_ids": ids,
        })

    # 3. Legacy unknown exit source on live closed trades
    n, ids = _ids_where(
        lambda t: t.get("execution_mode") == "live"
        and (t.get("exit_price_source") or "").upper() == PRICE_SOURCE_LEGACY_UNKNOWN
    )
    if n:
        items.append({
            "code": "LEGACY_UNKNOWN_EXIT_SOURCE",
            "message": f"{n} live trade(s) have LEGACY_UNKNOWN exit_price_source. "
                       "Cannot count for official performance.",
            "affected_count": n,
            "affected_ids": ids,
        })

    # 4. Missing broker order id on closed live trades
    n, ids = _ids_where(
        lambda t: t.get("execution_mode") == "live"
        and t.get("current_lifecycle_stage") in ("EXIT_FILLED", "CLOSED", "ENTRY_FILLED")
        and (not t.get("broker_entry_order_id") or not t.get("broker_exit_order_id"))
    )
    if n:
        items.append({
            "code": "MISSING_BROKER_ORDER_ID",
            "message": f"{n} live trade(s) missing broker_entry_order_id or "
                       "broker_exit_order_id. Reconcile against Tradier order history.",
            "affected_count": n,
            "affected_ids": ids,
        })

    # 5. Submitted but no fill (entry side, > 5 min old)
    n, ids = _ids_where(
        lambda t: t.get("current_lifecycle_stage") in ("ENTRY_SUBMITTED", "ENTRY_ACKED")
        and t.get("entry_fill_price") is None
    )
    if n:
        items.append({
            "code": "ENTRY_SUBMITTED_NO_FILL",
            "message": f"{n} entry order(s) submitted with no fill recorded.",
            "affected_count": n,
            "affected_ids": ids,
        })

    # 6. Failed / rejected
    n, ids = _ids_where(
        lambda t: t.get("current_lifecycle_stage") == "FAILED"
    )
    if n:
        items.append({
            "code": "FAILED_OR_REJECTED",
            "message": f"{n} trade(s) terminated in FAILED state.",
            "affected_count": n,
            "affected_ids": ids,
        })

    # 7. Preflight block
    n, ids = _ids_where(
        lambda t: t.get("current_lifecycle_stage") == "PREFLIGHT_BLOCKED"
    )
    if n:
        items.append({
            "code": "PREFLIGHT_BLOCKED",
            "message": f"{n} signal(s) blocked at preflight.",
            "affected_count": n,
            "affected_ids": ids,
        })

    # 8. Opportunity row missing
    n, ids = _ids_where(
        lambda t: t.get("preflight_status") is None and t.get("execution_mode") == "live"
    )
    if n:
        items.append({
            "code": "OPPORTUNITY_LEDGER_MISSING",
            "message": f"{n} live order(s) without a client_signal_opportunities row. "
                       "Ledger may be lagging.",
            "affected_count": n,
            "affected_ids": ids,
        })

    return items


# ---------------------------------------------------------------------------
# Aggregations
# ---------------------------------------------------------------------------

def _summary(trades: list[dict[str, Any]]) -> dict[str, int]:
    out = {
        "live_signals_seen":             0,
        "live_opportunities_created":    0,
        "preflight_passed":              0,
        "preflight_blocked":             0,
        "orders_created":                0,
        "watchers_armed":                0,
        "entry_confirmed":               0,
        "tradier_submitted":             0,
        "tradier_acked":                 0,
        "tradier_filled_entries":        0,
        "tradier_filled_exits":          0,
        "open_positions":                0,
        "closed_positions":              0,
        "broker_rejected":               0,
        "no_fill":                       0,
        "missing_exit_fill_truth":       0,
        "official_live_trades":          0,
        "unofficial_unreconciled_trades": 0,
    }
    for t in trades:
        mode = t.get("execution_mode")
        if mode == "live":
            out["live_signals_seen"] += 1
        if t.get("preflight_status"):
            out["live_opportunities_created"] += 1
        if t.get("preflight_status") in ("CLIENT_ELIGIBLE", "PREFLIGHT_PASSED", "WATCHER_ARMED"):
            out["preflight_passed"] += 1
        if t.get("current_lifecycle_stage") == "PREFLIGHT_BLOCKED":
            out["preflight_blocked"] += 1
        if t.get("order_local_id"):
            out["orders_created"] += 1
        if t.get("current_lifecycle_stage") == "WATCHER_ARMED":
            out["watchers_armed"] += 1
        if t.get("current_lifecycle_stage") in ("ENTRY_FILLED", "EXIT_FILLED", "CLOSED"):
            out["entry_confirmed"] += 1
        if t.get("current_lifecycle_stage") in ("ENTRY_SUBMITTED", "ENTRY_ACKED", "EXIT_SUBMITTED", "EXIT_ACKED"):
            out["tradier_submitted"] += 1
        if t.get("current_lifecycle_stage") in ("ENTRY_ACKED", "EXIT_ACKED"):
            out["tradier_acked"] += 1
        if t.get("entry_fill_price") is not None and mode == "live":
            out["tradier_filled_entries"] += 1
        if t.get("exit_fill_price") is not None and mode == "live":
            out["tradier_filled_exits"] += 1
        if t.get("current_lifecycle_stage") == "ENTRY_FILLED":
            out["open_positions"] += 1
        if t.get("current_lifecycle_stage") in ("EXIT_FILLED", "CLOSED"):
            out["closed_positions"] += 1
        if t.get("current_lifecycle_stage") == "FAILED":
            out["broker_rejected"] += 1
        if t.get("current_lifecycle_stage") in ("ENTRY_SUBMITTED", "ENTRY_ACKED") and t.get("entry_fill_price") is None:
            out["no_fill"] += 1
        if mode == "live" and t.get("current_lifecycle_stage") in ("EXIT_FILLED", "CLOSED"):
            if t.get("exit_fill_price") is None or (t.get("exit_price_source") or "").upper() in (
                "", PRICE_SOURCE_MISSING_EXIT, PRICE_SOURCE_LEGACY_UNKNOWN,
            ):
                out["missing_exit_fill_truth"] += 1
        if t.get("official_live_performance_eligible"):
            out["official_live_trades"] += 1
        elif mode == "live" and t.get("current_lifecycle_stage") in ("EXIT_FILLED", "CLOSED"):
            out["unofficial_unreconciled_trades"] += 1
    return out


def _breakdown(trades: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    """Group trades by `key` field, return sorted list of {key,count,...}."""
    buckets: dict[Any, dict[str, Any]] = {}
    for t in trades:
        k = t.get(key)
        b = buckets.setdefault(k, {key: k, "count": 0, "official": 0, "unofficial_live": 0})
        b["count"] += 1
        if t.get("official_live_performance_eligible"):
            b["official"] += 1
        elif t.get("execution_mode") == "live":
            b["unofficial_live"] += 1
    return sorted(buckets.values(), key=lambda x: -x["count"])


def _failure_breakdown(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for t in trades:
        stage = t.get("failure_stage")
        if not stage:
            continue
        reason = t.get("failure_reason") or "unknown"
        b = buckets.setdefault(stage, {"failure_stage": stage, "count": 0, "top_reasons": {}})
        b["count"] += 1
        b["top_reasons"][reason] = b["top_reasons"].get(reason, 0) + 1
    out = []
    for b in buckets.values():
        top = sorted(b["top_reasons"].items(), key=lambda kv: -kv[1])[:5]
        out.append({
            "failure_stage": b["failure_stage"],
            "count":         b["count"],
            "top_reasons":   [{"reason": r, "count": c} for r, c in top],
        })
    return sorted(out, key=lambda x: -x["count"])


def _data_quality(trades: list[dict[str, Any]],
                  source_failures: list[str]) -> dict[str, Any]:
    """Compute data_quality block per pinned spec shape."""
    warnings_by_code: dict[str, dict[str, Any]] = {}

    def _warn(code: str, message: str, trade: dict[str, Any]):
        w = warnings_by_code.setdefault(code, {
            "code": code,
            "message": message,
            "affected_count": 0,
            "affected_ids": [],
        })
        w["affected_count"] += 1
        if len(w["affected_ids"]) < 50:
            w["affected_ids"].append(trade.get("canonical_signal_id") or trade.get("signal_id"))

    live_count = 0
    covered = 0
    for t in trades:
        if t.get("execution_mode") == "live":
            live_count += 1
            if t.get("official_live_performance_eligible"):
                covered += 1

        # Concrete warnings — only on values that should not be missing.
        if t.get("execution_mode") == "live" and t.get("current_lifecycle_stage") in ("EXIT_FILLED", "CLOSED"):
            if not t.get("broker_entry_order_id"):
                _warn("missing_broker_entry_order_id",
                      "Closed live trade has no broker_entry_order_id", t)
            if not t.get("broker_exit_order_id"):
                _warn("missing_broker_exit_order_id",
                      "Closed live trade has no broker_exit_order_id", t)
            if t.get("entry_price_source") in (None, PRICE_SOURCE_LEGACY_UNKNOWN):
                _warn("missing_entry_price_source",
                      "Closed live trade has no entry_price_source", t)
            if t.get("exit_price_source") in (None, PRICE_SOURCE_LEGACY_UNKNOWN, PRICE_SOURCE_MISSING_EXIT):
                _warn("missing_exit_price_source",
                      "Closed live trade has no proven exit_price_source", t)
            # New: closed live trade must have an EXIT-kind orders row.
            if t.get("exit_order_status") is None:
                _warn("EXIT_ORDER_NOT_FOUND",
                      "Closed live trade has no matching orders row with kind='EXIT'. "
                      "Reconcile with broker exit history.", t)

        # pod_id should resolve for any trade with a client_id. If it does
        # not, the writer side (or members.execution_pod) is missing data.
        # This warning is severity-low — the journal still returns the row
        # with pod_id=None — but operators need to fix the source.
        if t.get("client_id") and not t.get("pod_id"):
            _warn("POD_ID_UNAVAILABLE",
                  "Trade has no resolvable pod_id (orders.meta.pod_id absent "
                  "and members.execution_pod NULL).", t)

    coverage_pct = round(100.0 * covered / live_count, 2) if live_count else None
    return {
        "warnings":        list(warnings_by_code.values()),
        "coverage_pct":    coverage_pct,
        "source_failures": list(source_failures),
        "has_partial_data": bool(source_failures or warnings_by_code),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_journal(
    *,
    start: Optional[str] = None,
    end:   Optional[str] = None,
    client_id: Optional[str] = None,
    symbol:    Optional[str] = None,
    canonical_signal_id: Optional[str] = None,
    status: Optional[str] = None,
    conn_factory=None,
) -> dict[str, Any]:
    """Build the full Live Execution Journal response.

    Read-only. Returns the spec-pinned shape (summary, trades,
    client_breakdown, symbol_breakdown, failure_breakdown, action_items,
    data_quality). Never raises on a partial-data source failure — instead
    surfaces it in data_quality.source_failures.
    """
    # Default window: today's market open (06:30 PT = 13:30 UTC) -> now.
    # We compute UTC explicitly so this is deterministic for tests.
    if not end:
        end_dt = datetime.now(timezone.utc)
    else:
        end_dt = _parse_iso(end)
    if not start:
        # 24h prior is a defensible default — captures the most recent
        # session even if called outside market hours.
        start_dt = end_dt - timedelta(hours=24)
    else:
        start_dt = _parse_iso(start)

    start_iso = start_dt.isoformat()
    end_iso = end_dt.isoformat()

    sql, args = _build_journal_query(
        start_iso=start_iso, end_iso=end_iso,
        client_id=client_id, symbol=symbol,
        canonical_signal_id=canonical_signal_id,
    )

    source_failures: list[str] = []
    rows: list[dict[str, Any]] = []
    if conn_factory is None:
        try:
            from ap.db import conn as _conn  # local import keeps this module DB-free at unit-test time
            conn_factory = _conn
        except Exception as e:
            log.warning("live_execution_journal: ap.db import failed: %s", e)
            source_failures.append(f"ap.db.conn import failed: {e}")

    if conn_factory is not None:
        try:
            with conn_factory() as cur:
                cur.execute(sql, args)
                # Zip column names with row tuples (works for both real
                # psycopg2 cursors and the test fake).
                desc = [d[0] for d in cur.description] if cur.description else []
                fetched = cur.fetchall()
                rows = []
                for r in fetched:
                    if isinstance(r, dict):
                        rows.append(r)
                    else:
                        rows.append(dict(zip(desc, r)))
        except Exception as e:
            log.warning("live_execution_journal: query failed: %s", e)
            source_failures.append(f"journal query failed: {type(e).__name__}: {e}")

    # Shape rows
    trades = [_shape_trade_row(r) for r in rows]

    # Status filter (post-shape) — checked against current_lifecycle_stage
    if status:
        status_up = status.upper()
        trades = [t for t in trades if t.get("current_lifecycle_stage") == status_up]

    return {
        "window": {"start": start_iso, "end": end_iso},
        "filters": {
            "client_id":           client_id,
            "symbol":              symbol,
            "canonical_signal_id": canonical_signal_id,
            "status":              status,
        },
        "summary":            _summary(trades),
        "trades":             trades,
        "client_breakdown":   _breakdown(trades, "client_id"),
        "symbol_breakdown":   _breakdown(trades, "symbol"),
        "failure_breakdown":  _failure_breakdown(trades),
        "action_items":       derive_action_items(trades),
        "data_quality":       _data_quality(trades, source_failures),
    }


def build_journey_sample(
    *,
    canonical_signal_id: Optional[str] = None,
    client_id: Optional[str] = None,
    conn_factory=None,
) -> dict[str, Any]:
    """Return ONE end-to-end journey for the given canonical_signal_id (and
    optionally client_id). Used by the debug endpoint for forensic
    walk-throughs of a single trade."""
    if not canonical_signal_id:
        return {"ok": False, "error": "canonical_signal_id required"}

    # Wide 14-day window so we can find the trade regardless of when it ran.
    full = build_journal(
        start=(datetime.now(timezone.utc) - timedelta(days=14)).isoformat(),
        end=datetime.now(timezone.utc).isoformat(),
        client_id=client_id,
        canonical_signal_id=canonical_signal_id,
        conn_factory=conn_factory,
    )
    matched = full.get("trades", [])
    return {
        "ok":                  True,
        "canonical_signal_id": canonical_signal_id,
        "client_id":           client_id,
        "match_count":         len(matched),
        "trades":              matched,
        "data_quality":        full.get("data_quality"),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_iso(value: str) -> datetime:
    """Parse an ISO-8601 string into a UTC-aware datetime. Lenient: accepts
    'YYYY-MM-DD' (treated as midnight UTC), naive datetimes (treated as
    UTC), and full RFC3339 strings."""
    if "T" not in value and len(value) <= 10:
        value = value + "T00:00:00+00:00"
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        # Last-ditch fallback: parse just the date portion.
        dt = datetime.fromisoformat(value[:10] + "T00:00:00+00:00")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


__all__ = [
    "build_journal",
    "build_journey_sample",
    "classify_official",
    "derive_lifecycle_stage",
    "derive_failure_stage_reason",
    "client_display_id",
    "LIFECYCLE_ORDER",
    "VALID_PRICE_SOURCES",
    "PRICE_SOURCE_TRADIER_ENTRY",
    "PRICE_SOURCE_TRADIER_EXIT",
    "PRICE_SOURCE_PAPER_BROKER",
    "PRICE_SOURCE_MANUAL_REPAIR",
    "PRICE_SOURCE_MISSING_ENTRY",
    "PRICE_SOURCE_MISSING_EXIT",
    "PRICE_SOURCE_LEGACY_UNKNOWN",
]
