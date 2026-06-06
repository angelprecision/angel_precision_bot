"""
ap/funnel_audit.py
==================
PR89 — Trade Volume Funnel + Scanner Gap Audit (READ-ONLY).

Pure-Python aggregation module for the operator funnel report. This module
performs SELECT-only queries against existing tables and views and folds the
results into a single, deterministic funnel response. It contains NO mutation
paths, NO trading behavior, and NO retry/auto-submit logic.

Design goals
------------
1.  Read-only. Every database access is a SELECT.
2.  Partial-data tolerant. If a source table is missing, return zero counts
    for that stage AND add a `data_quality` warning. NEVER fabricate
    successful zeros that look like "nothing happened" when the truth is
    "we couldn't measure it".
3.  Single source of truth for drop-reason vocabulary. All raw error/reason
    strings are normalized through `normalize_drop_reason()` so the
    dashboard counts are stable even as upstream code adds new variants.
4.  Deterministic action items. Conclusions are emitted in a fixed order
    so the same inputs always produce the same human-readable summary.

Authoritative sources
---------------------
  * ap_signals                        — scanner signal supply + gate decisions
  * orders                            — order lifecycle from creation to fill
  * client_signal_opportunities       — per-client fanout truth
  * ap_multi_account_signal_ledger    — joined view (orders ⨝ ap_signals)
  * members                           — active clients + execution_pod
  * proof_trades                      — exit / post-fill truth (best-effort)

This module does NOT touch:
  * scanner code
  * scoring code
  * quality gates
  * entry confirmation
  * sizing / contract selection
  * broker submit / cancel / replace
  * fanout / opportunity ledger writers
"""

from __future__ import annotations

import logging
import os
from collections import Counter, defaultdict
from datetime import datetime, time as dtime, timedelta, timezone
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo

log = logging.getLogger("ap.funnel_audit")

# Eastern time anchor — market open / close are ET.
ET = ZoneInfo("America/New_York")

# ----------------------------------------------------------------------------
# Lifecycle step ordering (matches PR89 spec §1 funnel)
# ----------------------------------------------------------------------------
FUNNEL_STEPS: tuple[str, ...] = (
    "SCANNER_GENERATED",
    "CLIENT_ELIGIBLE",
    "OPPORTUNITY_CREATED",
    "PREFLIGHT_PASSED",
    "ORDER_CREATED",
    "WATCHER_ARMED",
    "ENTRY_CONFIRMED",
    "BROKER_SUBMITTED",
    "BROKER_ACKED",
    "FILLED",
    "EXITED",
)

# ----------------------------------------------------------------------------
# Drop reason normalization
# ----------------------------------------------------------------------------
# The bot emits dozens of raw reason strings across signal rejection, preflight,
# contract selection, watcher invalidation, broker errors, and reconciliation.
# We map them onto a small, operator-friendly vocabulary so the funnel report
# stays readable. Any reason not matched falls into UNKNOWN — that's a signal
# to add a mapping, not to silently drop data.

NORMALIZED_REASONS: tuple[str, ...] = (
    "SCORE_TOO_LOW",
    "QUALITY_GATE_BLOCK",
    "CLIENT_NOT_APPROVED",
    "AUTHORIZATION_REQUIRED",
    "KILL_SWITCH",
    "ENTRIES_PAUSED",
    "BUYING_POWER",
    "CAPITAL_CAP",
    "DAILY_CAP",
    "LANE_CAP",
    "SAME_SYMBOL_CAP",
    "CONTRACT_SELECTOR_FAILED",
    "SPREAD_TOO_WIDE",
    "QUOTE_STALE",
    "ENTRY_CONFIRMATION_FAILED",
    "WATCHER_INVALIDATED",
    "BROKER_REJECTED",
    "BROKER_NO_FILL",
    "FILL_INTEGRITY_UNPROVEN",
    "POD_DELIVERY_MISSING",
    "OPPORTUNITY_ROW_MISSING",
    "UNKNOWN",
)

# Substring → canonical reason. First match wins, scanned in declaration order
# so more-specific terms shadow more-generic ones (e.g. "buying_power_cap"
# must hit BUYING_POWER, not CAPITAL_CAP).
_REASON_PATTERNS: tuple[tuple[str, str], ...] = (
    # Score / quality gates
    ("score_too_low",                "SCORE_TOO_LOW"),
    ("score_below",                  "SCORE_TOO_LOW"),
    ("score below",                  "SCORE_TOO_LOW"),
    ("below_min_score",              "SCORE_TOO_LOW"),
    ("below threshold",              "SCORE_TOO_LOW"),
    ("min_score",                    "SCORE_TOO_LOW"),
    ("iv_zone_score_too_low",        "SCORE_TOO_LOW"),
    ("quality_gate",                 "QUALITY_GATE_BLOCK"),
    ("hybrid_gate",                  "QUALITY_GATE_BLOCK"),
    ("client_quality_gate",          "QUALITY_GATE_BLOCK"),
    ("gate_blocked",                 "QUALITY_GATE_BLOCK"),
    ("rejected_by_gate",             "QUALITY_GATE_BLOCK"),
    # Client approval / auth
    ("not_approved",                 "CLIENT_NOT_APPROVED"),
    ("client_not_active",            "CLIENT_NOT_APPROVED"),
    ("client_inactive",              "CLIENT_NOT_APPROVED"),
    ("subscription",                 "CLIENT_NOT_APPROVED"),
    ("authorization_required",       "AUTHORIZATION_REQUIRED"),
    ("authorization_missing",        "AUTHORIZATION_REQUIRED"),
    ("auth_required",                "AUTHORIZATION_REQUIRED"),
    # Control state
    ("kill_switch",                  "KILL_SWITCH"),
    ("killswitch",                   "KILL_SWITCH"),
    ("entries_paused",               "ENTRIES_PAUSED"),
    ("entry_paused",                 "ENTRIES_PAUSED"),
    ("trading_paused",               "ENTRIES_PAUSED"),
    # Capital / caps  (order: most specific first)
    ("buying_power",                 "BUYING_POWER"),
    ("insufficient_funds",           "BUYING_POWER"),
    ("insufficient_buying_power",    "BUYING_POWER"),
    ("not_enough_capital",           "BUYING_POWER"),
    ("daily_cap",                    "DAILY_CAP"),
    ("daily_loss_cap",               "DAILY_CAP"),
    ("daily_limit",                  "DAILY_CAP"),
    ("lane_cap",                     "LANE_CAP"),
    ("lane_limit",                   "LANE_CAP"),
    ("same_symbol_cap",              "SAME_SYMBOL_CAP"),
    ("same_symbol",                  "SAME_SYMBOL_CAP"),
    ("symbol_cap",                   "SAME_SYMBOL_CAP"),
    ("capital_cap",                  "CAPITAL_CAP"),
    ("position_cap",                 "CAPITAL_CAP"),
    ("exposure_cap",                 "CAPITAL_CAP"),
    ("max_positions",                "CAPITAL_CAP"),
    # Contract / quote
    ("contract_selector",            "CONTRACT_SELECTOR_FAILED"),
    ("no_contract",                  "CONTRACT_SELECTOR_FAILED"),
    ("no_viable_contract",           "CONTRACT_SELECTOR_FAILED"),
    ("contract_not_found",           "CONTRACT_SELECTOR_FAILED"),
    ("no_strike",                    "CONTRACT_SELECTOR_FAILED"),
    ("spread_too_wide",              "SPREAD_TOO_WIDE"),
    ("wide_spread",                  "SPREAD_TOO_WIDE"),
    ("spread_pct_too_high",          "SPREAD_TOO_WIDE"),
    ("quote_stale",                  "QUOTE_STALE"),
    ("stale_quote",                  "QUOTE_STALE"),
    ("quote_too_old",                "QUOTE_STALE"),
    # Entry confirmation / watcher
    ("entry_confirmation_failed",    "ENTRY_CONFIRMATION_FAILED"),
    ("entry_not_confirmed",          "ENTRY_CONFIRMATION_FAILED"),
    ("confirmation_failed",          "ENTRY_CONFIRMATION_FAILED"),
    ("watcher_invalidated",          "WATCHER_INVALIDATED"),
    ("watcher_expired",              "WATCHER_INVALIDATED"),
    # Broker
    ("broker_rejected",              "BROKER_REJECTED"),
    ("broker_reject",                "BROKER_REJECTED"),
    ("order_rejected",               "BROKER_REJECTED"),
    ("rejected",                     "BROKER_REJECTED"),
    ("broker_no_fill",               "BROKER_NO_FILL"),
    ("no_fill",                      "BROKER_NO_FILL"),
    ("expired",                      "BROKER_NO_FILL"),
    ("canceled",                     "BROKER_NO_FILL"),
    ("cancelled",                    "BROKER_NO_FILL"),
    # Fill integrity / fanout
    ("fill_integrity",               "FILL_INTEGRITY_UNPROVEN"),
    ("fill_unproven",                "FILL_INTEGRITY_UNPROVEN"),
    ("pod_delivery",                 "POD_DELIVERY_MISSING"),
    ("pod_missing",                  "POD_DELIVERY_MISSING"),
    ("opportunity_row_missing",      "OPPORTUNITY_ROW_MISSING"),
    ("missing_opportunity",          "OPPORTUNITY_ROW_MISSING"),
)


def normalize_drop_reason(raw: Any) -> str:
    """Map an arbitrary upstream reason/error string to the operator vocab.

    Returns "UNKNOWN" for falsy or unrecognized input. Never raises.
    """
    if raw is None:
        return "UNKNOWN"
    try:
        s = str(raw).strip().lower()
    except Exception:
        return "UNKNOWN"
    if not s:
        return "UNKNOWN"
    # Direct exact-match against the canonical list (already-normalized input).
    upper = s.upper()
    if upper in NORMALIZED_REASONS:
        return upper
    for needle, mapped in _REASON_PATTERNS:
        if needle in s:
            return mapped
    return "UNKNOWN"


# ----------------------------------------------------------------------------
# Time-window helpers
# ----------------------------------------------------------------------------

def _market_open_today_utc() -> datetime:
    """09:30 ET on the most recent weekday → UTC."""
    now_et = datetime.now(ET)
    # If before market open on a weekday, "today" still means today.
    candidate = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    return candidate.astimezone(timezone.utc)


def resolve_window(start: Optional[str], end: Optional[str]) -> tuple[datetime, datetime]:
    """Parse query params into a (start_utc, end_utc) window.

    Defaults: start = today's 09:30 ET, end = now.
    """
    def _parse(s: str) -> Optional[datetime]:
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    start_dt = _parse(start or "") or _market_open_today_utc()
    end_dt = _parse(end or "") or datetime.now(timezone.utc)
    if end_dt <= start_dt:
        end_dt = start_dt + timedelta(minutes=1)
    return start_dt, end_dt


# ----------------------------------------------------------------------------
# Supabase access — best-effort, partial-data tolerant
# ----------------------------------------------------------------------------

def _sb_client():
    """Lazy-import the queue helper so this module stays import-safe in tests."""
    try:
        from ap.queue import _get_sb_client
    except Exception as e:
        log.warning(f"funnel_audit: could not import _get_sb_client: {e}")
        return None
    return _get_sb_client()


def _safe_select(table: str, sb, *, start_iso: str, end_iso: str,
                 ts_col: str, columns: str = "*",
                 page: int = 2000) -> tuple[list[dict], Optional[str]]:
    """SELECT-only fetch from `table` over the [start_iso, end_iso] window.

    Returns (rows, warning_or_None). Warning is non-None when the table is
    unavailable; rows is [] in that case. We paginate up to a safety cap so
    a single bursty signal day doesn't OOM the report.
    """
    if sb is None:
        return [], f"{table}:supabase_unavailable"

    rows: list[dict] = []
    try:
        offset = 0
        # Cap total rows to keep responses sized for the operator panel.
        MAX_TOTAL = 50_000
        while offset < MAX_TOTAL:
            q = (sb.table(table)
                   .select(columns)
                   .gte(ts_col, start_iso)
                   .lte(ts_col, end_iso)
                   .order(ts_col, desc=False)
                   .range(offset, offset + page - 1))
            batch = q.execute().data or []
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < page:
                break
            offset += page
        return rows, None
    except Exception as e:
        # Most common cause: table doesn't exist in this deployment, or column
        # name differs. Don't crash the funnel — return a data_quality warning.
        log.warning(f"funnel_audit: select {table} failed: {e}")
        return [], f"{table}:{type(e).__name__}:{str(e)[:120]}"


def _safe_select_all(table: str, sb, *, columns: str = "*",
                     filters: Optional[dict] = None) -> tuple[list[dict], Optional[str]]:
    """Unbounded SELECT used for lookup tables like `members`."""
    if sb is None:
        return [], f"{table}:supabase_unavailable"
    try:
        q = sb.table(table).select(columns)
        for k, v in (filters or {}).items():
            q = q.eq(k, v)
        return (q.execute().data or []), None
    except Exception as e:
        log.warning(f"funnel_audit: select-all {table} failed: {e}")
        return [], f"{table}:{type(e).__name__}:{str(e)[:120]}"


# ----------------------------------------------------------------------------
# Row classification helpers (mirror ap_multi_account_signal_ledger view's
# CASE expression, in Python, so we don't depend on the view existing).
# ----------------------------------------------------------------------------

_FILLED_STATES = frozenset({"FILLED", "PARTIALLY_FILLED", "PARTIAL_FILL",
                             "EXIT_FILLED", "EXIT_PARTIAL_FILL"})
_SUBMITTED_STATES = frozenset({"SUBMITTED", "ACKNOWLEDGED", "ACK"})


def _order_bucket(status: Any, broker_order_id: Any, last_error: Any) -> str:
    s = (str(status or "")).upper()
    err = (str(last_error or "")).lower()
    has_broker = bool(broker_order_id)
    if not s:
        return "UNKNOWN"
    if s in _FILLED_STATES:
        return "FILLED"
    if s in _SUBMITTED_STATES and has_broker:
        return "BROKER_SUBMITTED"
    if s == "PENDING_TRIGGER" and not has_broker:
        return "PENDING_TRIGGER_NO_BROKER"
    if s == "CANCELED" and "watcher_invalidated" in err:
        return "WATCHER_INVALIDATED"
    if s == "EXPIRED" and "watcher_expired" in err:
        return "WATCHER_INVALIDATED"  # collapse into WATCHER_INVALIDATED for funnel
    if s == "REJECTED":
        return "REJECTED"
    if s in ("CANCELED", "CANCELLED"):
        return "CANCELED"
    if s in ("EXPIRED", "ERROR"):
        return "TERMINAL_NO_FILL"
    return "OTHER"


def _opp_progressed_at_least(status: Any, target: str) -> bool:
    """Return True if the opportunity row has reached `target` or beyond.

    Uses the same monotonic ranking as ap/opportunity_ledger.STATUS_RANK so
    a FILLED row counts as having progressed past every prior step.
    """
    rank = {
        "CREATED":                    10,
        "CLIENT_ELIGIBLE":            20,
        "PREFLIGHT_WARNING":          30,
        "PREFLIGHT_PASSED":           40,
        "ORDER_CREATED":              50,
        "WATCHER_ARMED":              60,
        "BROKER_SUBMITTED":           70,
        "BROKER_ACKED":               80,
        "WATCHER_INVALIDATED":        90,
        "ENTRY_CONFIRMATION_FAILED":  90,
        "BROKER_REJECTED":            90,
        "EXPIRED":                    90,
        "CANCELED":                   90,
        "MISSED":                     90,
        "CLIENT_SKIPPED":             90,
        "INTERNAL_ERROR":             90,
        "FILLED":                    100,
    }
    cur = rank.get(str(status or "").upper(), 0)
    tgt = rank.get(target, 0)
    return cur >= tgt


# ----------------------------------------------------------------------------
# Main entry point
# ----------------------------------------------------------------------------

def build_funnel_report(
    *,
    start: Optional[str] = None,
    end: Optional[str] = None,
    client_id: Optional[str] = None,
    symbol: Optional[str] = None,
    scanner: Optional[str] = None,
    timeframe: Optional[str] = None,
    mode: Optional[str] = None,
) -> dict[str, Any]:
    """Build the full PR89 trade-volume funnel report.

    Read-only. Returns a dict matching the §1 response shape:
      { ok, window, filters, data_quality, summary, funnel, client_breakdown,
        scanner_breakdown, symbol_breakdown, pod_delivery, action_items }
    """
    start_dt, end_dt = resolve_window(start, end)
    start_iso = start_dt.isoformat()
    end_iso   = end_dt.isoformat()
    mode_norm = (mode or "all").lower().strip()
    if mode_norm not in ("all", "live", "paper", "intraday", "daily"):
        mode_norm = "all"

    sb = _sb_client()

    # ----------------------------------------------------------------------
    # Structured data-quality tracker.
    #
    # We treat ANY source-fetch failure as a fact that must be surfaced to
    # the operator — a missing source can never masquerade as zero activity.
    # Each failure is recorded with: table, stage (which lifecycle stages
    # depend on this source), and a short error string.
    #
    # Backward-compatibility note: older clients read `data_quality` as a
    # list of strings. We keep that list as `data_quality.warnings` so that
    # JSON-shape parity holds; structured failures live under
    # `data_quality.source_failures`. Any consumer can still flatten
    # warnings + source_failures for display.
    # ----------------------------------------------------------------------
    source_failures: list[dict] = []
    warnings: list[str] = []

    # Map each source table to the funnel stages it materially affects, so
    # the operator can read at a glance "this stage's count is 0 because
    # the source was unavailable, not because nothing happened."
    STAGES_BY_SOURCE: dict[str, list[str]] = {
        "ap_signals":                  ["SCANNER_GENERATED", "CLIENT_ELIGIBLE"],
        "orders":                       ["ORDER_CREATED", "WATCHER_ARMED",
                                         "BROKER_SUBMITTED", "FILLED"],
        "client_signal_opportunities":  ["OPPORTUNITY_CREATED", "PREFLIGHT_PASSED",
                                         "ENTRY_CONFIRMED", "BROKER_ACKED"],
        "members":                      ["client_breakdown", "pod_delivery"],
        "proof_trades":                 ["EXITED"],
    }

    def _record_warn(table: str, warn: Optional[str]) -> None:
        """Promote a (table, warn) tuple into the structured tracker."""
        if not warn:
            return
        # warn format from _safe_select / _safe_select_all is "table:detail".
        detail = warn.split(":", 1)[1] if ":" in warn else warn
        source_failures.append({
            "table":  table,
            "stages": list(STAGES_BY_SOURCE.get(table, [])),
            "error":  detail.strip() or "unknown",
        })
        warnings.append(warn)

    # --- 1. Pull ap_signals (scanner supply + gate decisions) -------------
    signals, warn = _safe_select(
        "ap_signals", sb,
        start_iso=start_iso, end_iso=end_iso, ts_col="created_at",
        columns=("signal_id,client_email,ticker,pattern,timeframe,side,score,"
                 "tier,decision_status,context_notes,created_at"),
    )
    _record_warn("ap_signals", warn)

    # --- 2. Pull orders (lifecycle from creation to fill) ------------------
    orders, warn = _safe_select(
        "orders", sb,
        start_iso=start_iso, end_iso=end_iso, ts_col="created_ts",
        columns=("local_order_id,client_id,kind,status,broker_order_id,symbol,"
                 "contract,qty,limit_price,fill_price,filled_qty,score,tier,"
                 "pattern,timeframe,direction,signal_id,canonical_signal_id,"
                 "last_error,created_ts,updated_ts,submitted_ts,meta"),
    )
    _record_warn("orders", warn)

    # --- 3. Pull client_signal_opportunities (per-client fanout truth) -----
    opps, warn = _safe_select(
        "client_signal_opportunities", sb,
        start_iso=start_iso, end_iso=end_iso, ts_col="created_at",
        columns=("canonical_signal_id,signal_id,client_id,symbol,direction,"
                 "timeframe,pattern,score,tier,scanner_type,scanner_name,"
                 "opportunity_status,client_eligibility_status,miss_stage,"
                 "miss_reason,would_block_reason,kill_switch_state,"
                 "entries_paused_state,buying_power_snapshot,cap_snapshot,"
                 "order_local_id,broker_order_id,position_id,quote_age_seconds,"
                 "spread_pct,preflight_enforced,execution_continued,"
                 "retry_status,retry_reason,created_at,updated_at,metadata"),
    )
    _record_warn("client_signal_opportunities", warn)

    # --- 4. Pull members (active clients + execution_pod) ------------------
    members, warn = _safe_select_all(
        "members", sb,
        columns="email,execution_pod,allow_live_trading,approved,subscription_active,status",
    )
    _record_warn("members", warn)
    active_clients = [
        m for m in (members or [])
        if (m.get("approved") in (True, "true", 1, "1")
            or m.get("subscription_active") in (True, "true", 1, "1")
            or (m.get("status") or "").upper() == "ACTIVE")
    ]
    pod_by_client: dict[str, str] = {}
    for m in active_clients:
        email = (m.get("email") or "").strip().lower()
        pod   = (m.get("execution_pod") or "").strip() or "unassigned"
        if email:
            pod_by_client[email] = pod
    n_active_clients = len(active_clients) or 0

    # --- 5. Pull proof_trades (exits / fill-integrity truth) — best effort -
    proofs, warn = _safe_select(
        "proof_trades", sb,
        start_iso=start_iso, end_iso=end_iso, ts_col="created_at",
        columns="trade_id,client_email,signal_id,status,exit_bucket,created_at",
    )
    _record_warn("proof_trades", warn)

    # ----------------------------------------------------------------------
    # Compute partial-data flags now so action_items and summary share the
    # SAME source-of-truth set rather than each recomputing it.
    # ----------------------------------------------------------------------
    unavailable_sources: set[str] = {f["table"] for f in source_failures}
    has_partial_data = bool(unavailable_sources)
    partial_data_sources = sorted(unavailable_sources)
    # Which lifecycle stages cannot be trusted to be "true zero":
    unavailable_stages: set[str] = set()
    for f in source_failures:
        unavailable_stages.update(f.get("stages") or [])

    # --- Apply user filters across all datasets ----------------------------
    cid_lc = (client_id or "").strip().lower()
    sym_uc = (symbol or "").strip().upper()
    scan_lc = (scanner or "").strip().lower()
    tf_lc   = (timeframe or "").strip().lower()

    def _matches_filters(row: dict) -> bool:
        if cid_lc:
            r_cid = (row.get("client_id") or row.get("client_email") or "").lower()
            if cid_lc not in r_cid:
                return False
        if sym_uc:
            r_sym = (row.get("symbol") or row.get("ticker") or "").upper()
            if r_sym != sym_uc:
                return False
        if scan_lc:
            r_scan = ((row.get("scanner_name") or "") + " "
                      + (row.get("scanner_type") or "") + " "
                      + (row.get("pattern") or "")).lower()
            if scan_lc not in r_scan:
                return False
        if tf_lc:
            r_tf = (row.get("timeframe") or "").lower()
            if tf_lc != r_tf:
                return False
        if mode_norm in ("intraday", "daily"):
            r_tf = (row.get("timeframe") or "").lower()
            if mode_norm == "daily" and r_tf and r_tf not in ("d", "1d", "daily"):
                return False
            if mode_norm == "intraday" and r_tf in ("d", "1d", "daily"):
                return False
        if mode_norm in ("live", "paper"):
            meta = row.get("meta") or {}
            r_mode = (meta.get("mode") or "").lower() if isinstance(meta, dict) else ""
            if mode_norm == "live" and r_mode and r_mode != "live":
                return False
            if mode_norm == "paper" and r_mode and r_mode != "paper":
                return False
        return True

    signals_f = [r for r in signals if _matches_filters(r)]
    orders_f  = [r for r in orders  if _matches_filters(r)]
    opps_f    = [r for r in opps    if _matches_filters(r)]
    proofs_f  = [r for r in proofs  if _matches_filters(r)]

    # ----------------------------------------------------------------------
    # FUNNEL COUNTS
    # ----------------------------------------------------------------------
    # SCANNER_GENERATED: every row in ap_signals (one per canonical signal).
    n_scanner = len(signals_f)

    # CLIENT_ELIGIBLE: signals whose decision_status indicates eligibility.
    # Eligible states observed in code: "received", "ARMED", "WATCHING",
    # or any state that produced a client opportunity row. Rejected states
    # ("rejected", "QUALITY_GATE_BLOCK", "SCORE_TOO_LOW") are excluded.
    eligible_signal_ids = set()
    rejected_reasons: Counter = Counter()
    for s in signals_f:
        ds = (s.get("decision_status") or "").lower()
        if ds in ("rejected", "score_too_low", "quality_gate_block", "blocked"):
            rejected_reasons[normalize_drop_reason(s.get("context_notes") or ds)] += 1
            continue
        eligible_signal_ids.add(str(s.get("signal_id") or ""))
    n_client_eligible_signals = len(eligible_signal_ids)

    # Canonical signals = distinct canonical_signal_id values across orders.
    canonical_signals = set()
    for o in orders_f:
        canon = o.get("canonical_signal_id") or o.get("signal_id")
        if canon:
            canonical_signals.add(str(canon))
    # Also include eligible signals that have NO order yet.
    canonical_signals |= eligible_signal_ids
    n_canonical = len(canonical_signals)

    # Expected client opportunities = eligible signals × active clients
    n_expected_opps = n_client_eligible_signals * n_active_clients

    # Opportunity rows actually created
    n_opp_rows = len(opps_f)

    # Preflight buckets (from opportunity rows)
    preflight_passed = preflight_warned = preflight_blocked = 0
    for o in opps_f:
        st = (o.get("opportunity_status") or "").upper()
        if _opp_progressed_at_least(st, "PREFLIGHT_PASSED"):
            preflight_passed += 1
        if st == "PREFLIGHT_WARNING":
            preflight_warned += 1
        if st == "CLIENT_SKIPPED":
            preflight_blocked += 1

    # Order lifecycle counts (entry orders only — ENTRY kind)
    entry_orders = [o for o in orders_f if (o.get("kind") or "").upper() == "ENTRY"]
    n_orders_created = len(entry_orders)

    n_watchers_armed = sum(
        1 for o in entry_orders
        if (o.get("status") or "").upper() in ("PENDING_TRIGGER", "WATCHING", "ARMED")
        or _opp_armed(o)
    )
    n_entry_confirmed = sum(
        1 for o in opps_f
        if _opp_progressed_at_least((o.get("opportunity_status") or ""), "BROKER_SUBMITTED")
    )

    bucket_counts: Counter = Counter()
    for o in entry_orders:
        b = _order_bucket(o.get("status"), o.get("broker_order_id"), o.get("last_error"))
        bucket_counts[b] += 1

    n_broker_submitted = bucket_counts["BROKER_SUBMITTED"] + bucket_counts["FILLED"]
    # ACKED isn't a distinct order-status in this schema; treat as a subset
    # of submitted. The opportunity ledger does distinguish ACKED.
    n_broker_acked = sum(
        1 for o in opps_f
        if (o.get("opportunity_status") or "").upper() == "BROKER_ACKED"
    ) + bucket_counts["FILLED"]
    n_filled    = bucket_counts["FILLED"]
    n_expired   = bucket_counts["TERMINAL_NO_FILL"]
    n_canceled  = bucket_counts["CANCELED"] + bucket_counts["WATCHER_INVALIDATED"]
    n_missed    = sum(
        1 for o in opps_f
        if (o.get("opportunity_status") or "").upper() in ("MISSED", "CLIENT_SKIPPED")
    )

    # EXITED — best-effort from proof_trades
    n_exited = sum(
        1 for p in proofs_f
        if (p.get("status") or "").upper() in ("CLOSED", "EXITED", "FILLED")
        or (p.get("exit_bucket") or "")
    )

    # Fill-rate denominators
    fill_rate_from_eligible = (
        round(n_filled / n_client_eligible_signals, 4)
        if n_client_eligible_signals else 0.0
    )
    fill_rate_from_submitted = (
        round(n_filled / n_broker_submitted, 4)
        if n_broker_submitted else 0.0
    )

    summary = {
        "scanner_signals_total":       n_scanner,
        "client_eligible_signals":     n_client_eligible_signals,
        "canonical_signals":           n_canonical,
        "expected_client_opportunities": n_expected_opps,
        "opportunity_rows_created":    n_opp_rows,
        "clients_preflight_passed":    preflight_passed,
        "clients_preflight_warned":    preflight_warned,
        "clients_preflight_blocked":   preflight_blocked,
        "orders_created":              n_orders_created,
        "watchers_armed":              n_watchers_armed,
        "entry_confirmed":             n_entry_confirmed,
        "broker_submitted":            n_broker_submitted,
        "broker_acked":                n_broker_acked,
        "filled":                      n_filled,
        "expired":                     n_expired,
        "canceled":                    n_canceled,
        "missed":                      n_missed,
        "fill_rate_from_client_eligible": fill_rate_from_eligible,
        "fill_rate_from_broker_submitted": fill_rate_from_submitted,
        "active_clients":              n_active_clients,
        # Partial-data honesty (PR89 readiness amendment):
        "has_partial_data":            has_partial_data,
        "partial_data_sources":        partial_data_sources,
    }

    # ----------------------------------------------------------------------
    # ORDERED FUNNEL (§1.2)
    # ----------------------------------------------------------------------
    funnel = _build_ordered_funnel(
        summary=summary,
        signals=signals_f,
        orders=entry_orders,
        opps=opps_f,
        rejected_reasons=rejected_reasons,
        unavailable_stages=unavailable_stages,
    )

    # ----------------------------------------------------------------------
    # CLIENT BREAKDOWN (§1.3)
    # ----------------------------------------------------------------------
    client_breakdown = _build_client_breakdown(
        active_clients=active_clients,
        opps=opps_f,
        entry_orders=entry_orders,
        n_client_eligible_signals=n_client_eligible_signals,
    )

    # ----------------------------------------------------------------------
    # SCANNER BREAKDOWN (§1.4)
    # ----------------------------------------------------------------------
    scanner_breakdown = _build_scanner_breakdown(
        signals=signals_f, orders=entry_orders, opps=opps_f,
    )

    # ----------------------------------------------------------------------
    # SYMBOL BREAKDOWN (§1.5)
    # ----------------------------------------------------------------------
    symbol_breakdown = _build_symbol_breakdown(
        signals=signals_f, orders=entry_orders, opps=opps_f,
    )

    # ----------------------------------------------------------------------
    # POD DELIVERY (§1.6)
    # ----------------------------------------------------------------------
    pod_delivery = _build_pod_delivery(
        active_clients=active_clients,
        pod_by_client=pod_by_client,
        opps=opps_f,
        eligible_signal_ids=eligible_signal_ids,
        signals=signals_f,
    )

    # ----------------------------------------------------------------------
    # ACTION ITEMS (§1.7) — deterministic operator conclusions
    # ----------------------------------------------------------------------
    action_items = _build_action_items(
        summary=summary,
        funnel=funnel,
        client_breakdown=client_breakdown,
        scanner_breakdown=scanner_breakdown,
        pod_delivery=pod_delivery,
        rejected_reasons=rejected_reasons,
        unavailable_sources=unavailable_sources,
    )

    return {
        "ok":            True,
        "generated_at":  datetime.now(timezone.utc).isoformat(),
        "window": {
            "start_utc": start_iso,
            "end_utc":   end_iso,
            "duration_minutes": round((end_dt - start_dt).total_seconds() / 60, 2),
        },
        "filters": {
            "client_id": client_id or None,
            "symbol":    symbol or None,
            "scanner":   scanner or None,
            "timeframe": timeframe or None,
            "mode":      mode_norm,
        },
        # Structured data quality (PR89 readiness amendment). `warnings`
        # preserves the original list[str] shape for any consumer still
        # reading the old contract; `source_failures` is the new authoritative
        # detail (table, affected stages, error).
        "data_quality": {
            "has_partial_data":      has_partial_data,
            "partial_data_sources":  partial_data_sources,
            "unavailable_stages":    sorted(unavailable_stages),
            "source_failures":       source_failures,
            "warnings":              warnings,
        },
        "summary":           summary,
        "funnel":            funnel,
        "client_breakdown":  client_breakdown,
        "scanner_breakdown": scanner_breakdown,
        "symbol_breakdown":  symbol_breakdown,
        "pod_delivery":      pod_delivery,
        "action_items":      action_items,
    }


# ----------------------------------------------------------------------------
# Section builders
# ----------------------------------------------------------------------------

def _opp_armed(order: dict) -> bool:
    """True if an order has progressed to WATCHER_ARMED or beyond."""
    s = (order.get("status") or "").upper()
    return s in ("PENDING_TRIGGER", "WATCHING", "ARMED", "SUBMITTED",
                 "ACK", "ACKNOWLEDGED", "FILLED",
                 "PARTIAL_FILL", "PARTIALLY_FILLED")


def _top_reasons(counter: Counter, k: int = 3) -> list[dict]:
    return [{"reason": r, "count": c} for r, c in counter.most_common(k)]


def _build_ordered_funnel(*, summary: dict, signals: list[dict],
                          orders: list[dict], opps: list[dict],
                          rejected_reasons: Counter,
                          unavailable_stages: Optional[set[str]] = None,
                          ) -> list[dict]:
    """Build the §1.2 ordered funnel with drop_from_previous + reasons.

    `unavailable_stages` is the set of stage names whose backing source
    failed to load. Each affected stage gets `unavailable=true` in its
    output dict so the dashboard can refuse to red-color a misleading zero.
    """
    unavailable_stages = unavailable_stages or set()
    # Per-step reason collectors
    cli_elig_reasons: Counter = rejected_reasons.copy()  # signals dropped before CLIENT_ELIGIBLE
    opp_create_reasons: Counter = Counter()
    preflight_reasons:  Counter = Counter()
    order_create_reasons: Counter = Counter()
    watcher_arm_reasons:  Counter = Counter()
    entry_confirm_reasons: Counter = Counter()
    broker_submit_reasons: Counter = Counter()
    broker_ack_reasons:    Counter = Counter()
    fill_reasons:          Counter = Counter()
    exit_reasons:          Counter = Counter()

    for o in opps:
        st = (o.get("opportunity_status") or "").upper()
        miss = o.get("miss_reason") or o.get("would_block_reason") or o.get("retry_reason")
        norm = normalize_drop_reason(miss) if miss else None
        if st in ("CREATED", "CLIENT_ELIGIBLE"):
            # row exists but never progressed
            opp_create_reasons[norm or "OPPORTUNITY_ROW_MISSING"] += 1
        elif st == "CLIENT_SKIPPED":
            preflight_reasons[norm or "UNKNOWN"] += 1
        elif st == "PREFLIGHT_WARNING":
            preflight_reasons[norm or "UNKNOWN"] += 1
        elif st in ("PREFLIGHT_PASSED",):
            order_create_reasons[norm or "CONTRACT_SELECTOR_FAILED"] += 1
        elif st == "ORDER_CREATED":
            watcher_arm_reasons[norm or "UNKNOWN"] += 1
        elif st == "WATCHER_ARMED":
            entry_confirm_reasons[norm or "ENTRY_CONFIRMATION_FAILED"] += 1
        elif st == "WATCHER_INVALIDATED":
            entry_confirm_reasons["WATCHER_INVALIDATED"] += 1
        elif st == "ENTRY_CONFIRMATION_FAILED":
            entry_confirm_reasons["ENTRY_CONFIRMATION_FAILED"] += 1
        elif st == "BROKER_SUBMITTED":
            broker_ack_reasons[norm or "BROKER_NO_FILL"] += 1
        elif st == "BROKER_REJECTED":
            broker_submit_reasons["BROKER_REJECTED"] += 1
        elif st == "BROKER_ACKED":
            fill_reasons[norm or "BROKER_NO_FILL"] += 1
        elif st in ("EXPIRED", "CANCELED", "MISSED"):
            fill_reasons[norm or "BROKER_NO_FILL"] += 1

    # Look at orders to collect broker-side reasons
    for o in orders:
        bucket = _order_bucket(o.get("status"), o.get("broker_order_id"), o.get("last_error"))
        norm = normalize_drop_reason(o.get("last_error")) if o.get("last_error") else None
        if bucket == "REJECTED":
            broker_submit_reasons[norm or "BROKER_REJECTED"] += 1
        elif bucket in ("WATCHER_INVALIDATED",):
            entry_confirm_reasons["WATCHER_INVALIDATED"] += 1
        elif bucket in ("TERMINAL_NO_FILL", "CANCELED"):
            fill_reasons[norm or "BROKER_NO_FILL"] += 1
        elif bucket == "PENDING_TRIGGER_NO_BROKER":
            broker_submit_reasons[norm or "UNKNOWN"] += 1

    counts = [
        ("SCANNER_GENERATED",   summary["scanner_signals_total"],     Counter()),
        ("CLIENT_ELIGIBLE",     summary["client_eligible_signals"],   cli_elig_reasons),
        ("OPPORTUNITY_CREATED", summary["opportunity_rows_created"],  opp_create_reasons),
        ("PREFLIGHT_PASSED",    summary["clients_preflight_passed"],  preflight_reasons),
        ("ORDER_CREATED",       summary["orders_created"],            order_create_reasons),
        ("WATCHER_ARMED",       summary["watchers_armed"],            watcher_arm_reasons),
        ("ENTRY_CONFIRMED",     summary["entry_confirmed"],           entry_confirm_reasons),
        ("BROKER_SUBMITTED",    summary["broker_submitted"],          broker_submit_reasons),
        ("BROKER_ACKED",        summary["broker_acked"],              broker_ack_reasons),
        ("FILLED",              summary["filled"],                    fill_reasons),
        ("EXITED",              0,                                    exit_reasons),  # filled is the right denom for exits
    ]

    # Pull EXITED from summary if available (we counted into n_exited earlier
    # via proofs in summary['filled']/etc, but summary doesn't carry exited).
    # We let the section builder reach back via filled fallback.
    funnel: list[dict] = []
    prev_count: Optional[int] = None
    for step, count, reasons in counts:
        is_unavailable = step in unavailable_stages
        if prev_count is None or prev_count == 0:
            drop = 0
            drop_pct = 0.0
        else:
            drop = max(0, prev_count - count)
            drop_pct = round(100.0 * drop / prev_count, 2) if prev_count else 0.0
        entry = {
            "step":              step,
            "count":             int(count),
            "drop_from_previous": int(drop),
            "drop_pct":          drop_pct,
            "top_drop_reasons":  _top_reasons(reasons, k=3),
            "unavailable":       is_unavailable,
        }
        # When the source is unavailable, the count is not meaningful as a
        # "true zero" — mark it so downstream consumers can render it as
        # "—" or with a warning rather than treating it as a real drop.
        funnel.append(entry)
        prev_count = count
    return funnel


def _build_client_breakdown(*, active_clients: list[dict], opps: list[dict],
                            entry_orders: list[dict],
                            n_client_eligible_signals: int) -> list[dict]:
    opps_by_client: dict[str, list[dict]] = defaultdict(list)
    for o in opps:
        cid = (o.get("client_id") or "").lower()
        if cid:
            opps_by_client[cid].append(o)
    orders_by_client: dict[str, list[dict]] = defaultdict(list)
    for o in entry_orders:
        cid = (o.get("client_id") or "").lower()
        if cid:
            orders_by_client[cid].append(o)

    out = []
    for m in active_clients:
        email = (m.get("email") or "").strip().lower()
        if not email:
            continue
        my_opps   = opps_by_client.get(email, [])
        my_orders = orders_by_client.get(email, [])
        preflight_blocks = sum(
            1 for o in my_opps
            if (o.get("opportunity_status") or "").upper() == "CLIENT_SKIPPED"
        )
        orders_created = len(my_orders)
        broker_submitted = sum(
            1 for o in my_orders
            if _order_bucket(o.get("status"), o.get("broker_order_id"), o.get("last_error"))
                in ("BROKER_SUBMITTED", "FILLED")
        )
        fills = sum(
            1 for o in my_orders
            if _order_bucket(o.get("status"), o.get("broker_order_id"), o.get("last_error"))
                == "FILLED"
        )
        misses = sum(
            1 for o in my_opps
            if (o.get("opportunity_status") or "").upper()
                in ("MISSED", "EXPIRED", "CANCELED", "BROKER_REJECTED",
                    "WATCHER_INVALIDATED", "ENTRY_CONFIRMATION_FAILED",
                    "CLIENT_SKIPPED", "INTERNAL_ERROR")
        )
        block_reasons = Counter(
            normalize_drop_reason(o.get("would_block_reason") or o.get("miss_reason"))
            for o in my_opps
            if (o.get("opportunity_status") or "").upper() == "CLIENT_SKIPPED"
        )
        miss_reasons = Counter(
            normalize_drop_reason(o.get("miss_reason") or o.get("would_block_reason"))
            for o in my_opps
            if (o.get("opportunity_status") or "").upper()
               in ("MISSED", "EXPIRED", "CANCELED", "BROKER_REJECTED",
                   "WATCHER_INVALIDATED", "ENTRY_CONFIRMATION_FAILED")
        )
        # Plus order-level last_error misses
        for o in my_orders:
            err = o.get("last_error")
            bucket = _order_bucket(o.get("status"), o.get("broker_order_id"), err)
            if bucket in ("REJECTED", "TERMINAL_NO_FILL", "CANCELED", "WATCHER_INVALIDATED"):
                miss_reasons[normalize_drop_reason(err)] += 1

        out.append({
            "client_id":              email,
            "execution_pod":          (m.get("execution_pod") or "unassigned"),
            "expected_opportunities": n_client_eligible_signals,
            "opportunity_rows":       len(my_opps),
            "preflight_blocks":       preflight_blocks,
            "orders_created":         orders_created,
            "broker_submitted":       broker_submitted,
            "fills":                  fills,
            "misses":                 misses,
            "top_block_reason":       (block_reasons.most_common(1)[0][0] if block_reasons else None),
            "top_miss_reason":        (miss_reasons.most_common(1)[0][0] if miss_reasons else None),
            "fill_rate":              (round(fills / broker_submitted, 4) if broker_submitted else 0.0),
        })
    # Stable sort: clients with the lowest fill_rate (and any volume) first.
    out.sort(key=lambda r: (r["fills"] == 0, r["fill_rate"], -r["orders_created"]))
    return out


def _build_scanner_breakdown(*, signals: list[dict], orders: list[dict],
                             opps: list[dict]) -> list[dict]:
    """Per (scanner_name/scanner_type/timeframe/pattern) breakdown."""
    def _key(row):
        return (
            (row.get("scanner_name") or row.get("pattern") or "unknown"),
            (row.get("scanner_type") or "unknown"),
            (row.get("timeframe")    or "unknown"),
            (row.get("pattern")      or "unknown"),
        )
    sig_by_key: dict[tuple, list[dict]]   = defaultdict(list)
    opp_by_key: dict[tuple, list[dict]]   = defaultdict(list)
    ord_by_key: dict[tuple, list[dict]]   = defaultdict(list)

    for s in signals:
        sig_by_key[_key(s)].append(s)
    for o in opps:
        opp_by_key[_key(o)].append(o)
    for o in orders:
        ord_by_key[_key(o)].append(o)

    keys = set(sig_by_key) | set(opp_by_key) | set(ord_by_key)
    out = []
    for k in keys:
        my_sig = sig_by_key.get(k, [])
        my_opp = opp_by_key.get(k, [])
        my_ord = ord_by_key.get(k, [])
        client_eligible = sum(
            1 for s in my_sig
            if (s.get("decision_status") or "").lower() not in ("rejected",)
        ) or len({(o.get("canonical_signal_id") or o.get("signal_id")) for o in my_opp if o.get("canonical_signal_id") or o.get("signal_id")})
        orders_created  = len(my_ord)
        broker_submitted = sum(
            1 for o in my_ord
            if _order_bucket(o.get("status"), o.get("broker_order_id"), o.get("last_error"))
                in ("BROKER_SUBMITTED", "FILLED")
        )
        fills = sum(
            1 for o in my_ord
            if _order_bucket(o.get("status"), o.get("broker_order_id"), o.get("last_error"))
                == "FILLED"
        )
        scores = [s.get("score") for s in my_sig if isinstance(s.get("score"), (int, float))]
        avg_score = round(sum(scores) / len(scores), 2) if scores else None
        reasons = Counter()
        for s in my_sig:
            ds = (s.get("decision_status") or "").lower()
            if ds == "rejected":
                reasons[normalize_drop_reason(s.get("context_notes") or "rejected")] += 1
        for o in my_opp:
            mr = o.get("miss_reason") or o.get("would_block_reason")
            if mr:
                reasons[normalize_drop_reason(mr)] += 1
        out.append({
            "scanner_name": k[0],
            "scanner_type": k[1],
            "timeframe":    k[2],
            "pattern":      k[3],
            "signals_generated": len(my_sig),
            "client_eligible":   client_eligible,
            "orders_created":    orders_created,
            "broker_submitted":  broker_submitted,
            "fills":             fills,
            "avg_score":         avg_score,
            "win_count":         None,   # populated by future PR with proof_trades join
            "peak_pct":          None,   # populated by future PR with edge intelligence
            "drop_reason_top":   (reasons.most_common(1)[0][0] if reasons else None),
        })
    out.sort(key=lambda r: (-r["fills"], -r["signals_generated"]))
    return out


def _build_symbol_breakdown(*, signals: list[dict], orders: list[dict],
                            opps: list[dict]) -> list[dict]:
    sig_by_sym: dict[str, list[dict]] = defaultdict(list)
    opp_by_sym: dict[str, list[dict]] = defaultdict(list)
    ord_by_sym: dict[str, list[dict]] = defaultdict(list)
    for s in signals:
        sym = (s.get("ticker") or s.get("symbol") or "").upper()
        if sym:
            sig_by_sym[sym].append(s)
    for o in opps:
        sym = (o.get("symbol") or "").upper()
        if sym:
            opp_by_sym[sym].append(o)
    for o in orders:
        sym = (o.get("symbol") or "").upper()
        if sym:
            ord_by_sym[sym].append(o)

    syms = set(sig_by_sym) | set(opp_by_sym) | set(ord_by_sym)
    out = []
    for sym in syms:
        my_sig = sig_by_sym.get(sym, [])
        my_opp = opp_by_sym.get(sym, [])
        my_ord = ord_by_sym.get(sym, [])
        signals_count = len(my_sig)
        client_eligible = sum(
            1 for s in my_sig
            if (s.get("decision_status") or "").lower() not in ("rejected",)
        )
        submitted = sum(
            1 for o in my_ord
            if _order_bucket(o.get("status"), o.get("broker_order_id"), o.get("last_error"))
                in ("BROKER_SUBMITTED", "FILLED")
        )
        filled = sum(
            1 for o in my_ord
            if _order_bucket(o.get("status"), o.get("broker_order_id"), o.get("last_error")) == "FILLED"
        )
        rejected = sum(
            1 for o in my_ord
            if _order_bucket(o.get("status"), o.get("broker_order_id"), o.get("last_error")) == "REJECTED"
        )
        missed = sum(
            1 for o in my_opp
            if (o.get("opportunity_status") or "").upper()
               in ("MISSED", "EXPIRED", "CANCELED", "BROKER_REJECTED",
                   "WATCHER_INVALIDATED", "ENTRY_CONFIRMATION_FAILED",
                   "CLIENT_SKIPPED")
        )
        spreads = [o.get("spread_pct") for o in my_opp if isinstance(o.get("spread_pct"), (int, float))]
        avg_spread = round(sum(spreads) / len(spreads), 4) if spreads else None
        reject_reasons = Counter()
        for o in my_ord:
            err = o.get("last_error")
            if err:
                reject_reasons[normalize_drop_reason(err)] += 1
        out.append({
            "symbol":           sym,
            "signals":          signals_count,
            "client_eligible":  client_eligible,
            "submitted":        submitted,
            "filled":           filled,
            "rejected":         rejected,
            "missed":           missed,
            "avg_spread":       avg_spread,
            "top_reject_reason": (reject_reasons.most_common(1)[0][0] if reject_reasons else None),
        })
    out.sort(key=lambda r: (-r["filled"], -r["signals"]))
    return out


def _build_pod_delivery(*, active_clients: list[dict],
                        pod_by_client: dict[str, str],
                        opps: list[dict],
                        eligible_signal_ids: set[str],
                        signals: list[dict]) -> list[dict]:
    """Per-pod delivery audit.

    Without dedicated scanner_delivery_logs we infer delivery from opportunity
    rows: every active client in a pod is expected to have one opportunity row
    per eligible signal. Missing rows imply the signal didn't reach the pod
    (or the fanout loop dropped it before write).
    """
    clients_by_pod: dict[str, list[str]] = defaultdict(list)
    for email, pod in pod_by_client.items():
        clients_by_pod[pod].append(email)

    out = []
    for pod, members in clients_by_pod.items():
        member_set = set(members)
        pod_opps   = [o for o in opps if (o.get("client_id") or "").lower() in member_set]

        signals_received = {
            (o.get("canonical_signal_id") or o.get("signal_id"))
            for o in pod_opps
            if (o.get("canonical_signal_id") or o.get("signal_id"))
        }
        canonical_signals_seen = signals_received  # 1:1 in this schema
        opportunity_rows_created = len(pod_opps)
        clients_loaded = len(member_set)
        client_opportunities_expected = clients_loaded * len(eligible_signal_ids)
        client_opportunities_created  = opportunity_rows_created
        missing_opportunity_rows = max(
            0, client_opportunities_expected - client_opportunities_created
        )

        last_ts = None
        for o in pod_opps:
            ts = o.get("updated_at") or o.get("created_at")
            if ts and (last_ts is None or str(ts) > str(last_ts)):
                last_ts = ts

        out.append({
            "pod":                       pod,
            "clients_loaded":            clients_loaded,
            "signals_received":          len(signals_received),
            "canonical_signals_seen":    len(canonical_signals_seen),
            "opportunity_rows_created":  opportunity_rows_created,
            "client_opportunities_expected": client_opportunities_expected,
            "client_opportunities_created":  client_opportunities_created,
            "missing_opportunity_rows":  missing_opportunity_rows,
            "last_signal_received_at":   last_ts,
        })
    out.sort(key=lambda r: (-r["missing_opportunity_rows"], r["pod"]))
    return out


# ----------------------------------------------------------------------------
# Action items — deterministic operator conclusions (PR89 §1.7)
# ----------------------------------------------------------------------------

def _build_action_items(*, summary: dict, funnel: list[dict],
                        client_breakdown: list[dict],
                        scanner_breakdown: list[dict],
                        pod_delivery: list[dict],
                        rejected_reasons: Counter,
                        unavailable_sources: Optional[set[str]] = None,
                        ) -> list[dict]:
    """Deterministic operator conclusions.

    PR89 readiness amendment: when a source is unavailable, the conclusions
    that DEPEND on that source MUST say so explicitly rather than treat a
    zero count as a real "nothing happened" signal.
    """
    unavailable_sources = unavailable_sources or set()
    items: list[dict] = []

    # 0. Partial data — always the first item when any source failed.
    #    This is the operator's #1 cue that the rest of the conclusions
    #    must be read with caveats.
    if unavailable_sources:
        items.append({
            "severity": "critical",
            "code":     "PARTIAL_DATA",
            "message":  (
                "Partial data — one or more sources failed: "
                + ", ".join(sorted(unavailable_sources))
                + ". Counts for affected stages cannot be trusted as "
                  "‘true zero’."
            ),
        })

    # 1. Scanner supply — honest about unavailable sources.
    #    If ap_signals AND client_signal_opportunities are both unavailable,
    #    we have NO way to know how many signals the scanner produced.
    cli_elig  = summary["client_eligible_signals"]
    n_scanner = summary["scanner_signals_total"]
    scanner_source_unavailable = (
        "ap_signals" in unavailable_sources
        and "client_signal_opportunities" in unavailable_sources
    )
    if scanner_source_unavailable:
        items.append({
            "severity": "critical",
            "code":     "SCANNER_SOURCE_UNAVAILABLE",
            "message":  ("Scanner source unavailable — cannot determine "
                         "whether scanner supply is low."),
        })
    elif "ap_signals" in unavailable_sources:
        # Partial: signals table missing but opportunities may give a hint.
        items.append({
            "severity": "warn",
            "code":     "SCANNER_SOURCE_PARTIAL",
            "message":  ("Scanner signal source (ap_signals) unavailable — "
                         "scanner-supply assessment is incomplete; only "
                         "opportunity-ledger evidence is being used."),
        })
    elif cli_elig < 5:
        items.append({
            "severity": "warn" if cli_elig > 0 else "critical",
            "code":     "LOW_SCANNER_SUPPLY",
            "message":  f"Scanner supply is low: only {cli_elig} CLIENT_ELIGIBLE signals in window.",
        })

    # 2. Quality-gate bottleneck — only meaningful when scanner data is real.
    if (n_scanner > 0 and cli_elig == 0
            and not scanner_source_unavailable
            and "ap_signals" not in unavailable_sources):
        items.append({
            "severity": "critical",
            "code":     "QUALITY_GATE_BLOCKING_ALL",
            "message":  f"Intraday scanner generated {n_scanner} signals but 0 passed quality gates.",
        })

    # 3. Eligible but missing pod delivery — requires both opps + members.
    pod_source_unavailable = bool(
        unavailable_sources & {"client_signal_opportunities", "members"}
    )
    if pod_source_unavailable:
        items.append({
            "severity": "warn",
            "code":     "POD_DELIVERY_SOURCE_UNAVAILABLE",
            "message":  ("Pod delivery source unavailable — cannot determine "
                         "whether signals reached every pod."),
        })
    elif cli_elig > 0 and pod_delivery:
        worst = pod_delivery[0]
        if worst["missing_opportunity_rows"] > 0:
            items.append({
                "severity": "warn",
                "code":     "POD_DELIVERY_MISSING",
                "message":  (f"Signals are eligible but not reaching pod "
                             f"'{worst['pod']}': {worst['missing_opportunity_rows']} "
                             f"expected opportunity rows missing."),
            })

    # 4. Preflight / buying-power offenders
    for c in client_breakdown:
        if c["preflight_blocks"] >= 4:
            reason = c["top_block_reason"] or "UNKNOWN"
            items.append({
                "severity": "warn",
                "code":     "CLIENT_PREFLIGHT_BLOCKS",
                "message":  (f"Client {c['client_id']} blocked "
                             f"{c['preflight_blocks']} times by "
                             f"{reason.lower()}."),
            })

    # 5. Concentration of drops at a single stage
    biggest_drop = max(
        (f for f in funnel if f["drop_from_previous"] > 0),
        key=lambda f: f["drop_from_previous"],
        default=None,
    )
    if biggest_drop and biggest_drop["top_drop_reasons"]:
        top_reason = biggest_drop["top_drop_reasons"][0]["reason"]
        items.append({
            "severity": "info",
            "code":     "BIGGEST_DROP_STAGE",
            "message":  (f"Most drops occur between "
                         f"{_prev_step(biggest_drop['step'])} → "
                         f"{biggest_drop['step']} "
                         f"(reason: {top_reason.lower()})."),
        })

    # 6. Broker submitted but not filling — requires the orders table.
    sub = summary["broker_submitted"]
    fill = summary["filled"]
    if "orders" in unavailable_sources:
        items.append({
            "severity": "warn",
            "code":     "BROKER_SOURCE_UNAVAILABLE",
            "message":  ("Broker source (orders) unavailable — cannot determine "
                         "whether broker is rejecting or failing to fill."),
        })
    elif sub >= 4 and fill / max(sub, 1) < 0.5:
        items.append({
            "severity": "warn",
            "code":     "BROKER_LOW_FILL_RATE",
            "message":  f"Broker submitted {sub} orders but only {fill} filled.",
        })

    # 7. Most executable scanner
    if scanner_breakdown:
        best = scanner_breakdown[0]
        if best["fills"] > 0:
            items.append({
                "severity": "info",
                "code":     "BEST_SCANNER",
                "message":  (f"{best['scanner_name']} "
                             f"({best['timeframe']}, {best['pattern']}) "
                             f"generated {best['signals_generated']} signals "
                             f"and {best['fills']} filled."),
            })

    # 8. Worst client by missed opportunities
    if client_breakdown:
        worst_client = max(client_breakdown, key=lambda c: c["misses"])
        if worst_client["misses"] > 0:
            reason = worst_client["top_miss_reason"] or "UNKNOWN"
            items.append({
                "severity": "info",
                "code":     "WORST_CLIENT_MISSES",
                "message":  (f"Client {worst_client['client_id']} missed "
                             f"{worst_client['misses']} opportunities "
                             f"(top reason: {reason.lower()})."),
            })

    # 9. If everything else is empty, surface the bare scanner-vs-fill gap —
    #    but ONLY when we have honest scanner + broker data. With partial
    #    data, the PARTIAL_DATA item at position 0 already serves that role.
    if not items:
        items.append({
            "severity": "info",
            "code":     "NO_BOTTLENECK_DETECTED",
            "message":  (f"No single stage dominates the drop. "
                         f"Scanner→Filled: {n_scanner} → {fill}."),
        })

    return items


def _prev_step(step: str) -> str:
    """Return the funnel step immediately before `step`, or '∅' if first."""
    try:
        i = FUNNEL_STEPS.index(step)
    except ValueError:
        return "∅"
    return FUNNEL_STEPS[i - 1] if i > 0 else "∅"
