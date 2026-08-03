"""
ap_morning_handoff_audit.py — Morning Handoff Self-Audit and Auto-Rearm
========================================================================

After overnight_reeval and before market open, every valid non-terminal ENTRY
setup must be watcher-owned, auto-rearmed, or explicitly classified so the
operator never needs manual SQL rescue for rows that should be in the session.

Core invariants (MUST NOT be violated):
  - NEVER call broker.submit_order or any broker write path
  - NEVER transition an order to SUBMITTED
  - NEVER create a new ENTRY order
  - NEVER change contract / limit_price / qty on any row
  - NEVER mutate terminal rows (EXPIRED, FILLED, CANCELED, etc.)
  - NEVER duplicate watcher state for the same local_order_id
  - Idempotent: auditing the same row twice must produce the same result

Run triggers:
  1. Service startup (called from ClientRunner startup flow)
  2. Before market open, ideally 9:25–9:29 ET (called by scheduler or cron)
  3. After overnight_reeval completes
  4. Manually via POST /admin/morning_handoff_audit

Classifications:
  READY_ARMED               — row is valid and watcher already owns it
  AUTO_REARMED              — watcher lost state; ownership re-established
  WAITING_FOR_BREACH        — real contract, watcher re-armed, waiting for trigger
  WAITING_FOR_CONTRACT_AT_BREACH — deferred contract, watcher re-armed, contract
                              selected at breach time
  SKIPPED_TERMINAL          — terminal status; not touched
  SKIPPED_AFTER_CUTOFF      — entry cutoff passed; valid row but session over
  BROKEN_NEEDS_CODE         — no watcher evidence, no broker fields; structural issue
  BLOCKED_RISK              — watcher rejected re-arm (quality / risk gate)

Usage:
    from ap_morning_handoff_audit import run_morning_handoff_audit

    result = run_morning_handoff_audit(
        client_id="jasoncosby1@gmail.com",
        entry_watcher=runner.entry_watcher,
        osm=runner.osm,
        execution_mode="live",
        dry_run=False,
    )
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
import types
from datetime import datetime, timezone, timedelta
from typing import Optional

from ap_entry_watcher import (
    RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN,
    recovery_trigger_evidence_identity_is_proven,
)

log = logging.getLogger("ap.morning_handoff_audit")

# ── Architectural safety proof constant ───────────────────────────────────────
# This module calls entry_watcher.watch(plan, local_order_id) for re-arm.
# Option C safety proof: watch() and add_signal() NEVER call on_trigger.
# on_trigger (→ ExecutionCore → broker.submit_order) fires ONLY from
# entry_watcher._poll_loop(), a background daemon thread. The audit never
# calls entry_watcher.start(), so no trigger callback can fire during the
# audit's watch() call. See _audit_row() for the full proof comment.
_WATCH_IS_TRIGGER_SAFE = True  # source-verified; see ap_entry_watcher.py lines 1554–2380

# EOD entry cutoff — mirrors ap_entry_watcher.EOD_CUTOFF_HOUR/MIN and
# ap/order_monitor._PT_ORPHAN_EOD_CUTOFF_HOUR/MIN.
MHA_EOD_CUTOFF_HOUR = int(os.getenv("MHA_EOD_CUTOFF_HOUR", "15"))
MHA_EOD_CUTOFF_MIN  = int(os.getenv("MHA_EOD_CUTOFF_MIN",  "30"))

# How far back to look for audit-eligible ENTRY rows.
MHA_LOOKBACK_HOURS = int(os.getenv("MHA_LOOKBACK_HOURS", "48"))

# Terminal statuses — these rows are never touched.
_TERMINAL_STATUSES = frozenset({
    "EXPIRED", "FILLED", "CANCELED", "CANCELLED", "CLOSED",
    "CLOSED_REPAIR", "STOPPED", "TAKEN_PROFIT", "ERROR",
    "EXIT_FILLED", "PARTIALLY_FILLED", "FAILED",
})

# Statuses eligible for audit.
_AUDIT_STATUSES = frozenset({"PENDING_TRIGGER", "WATCHING", "CREATED", "RETRY_ELIGIBLE"})

# ── Classification labels ──────────────────────────────────────────────────────

READY_ARMED                     = "READY_ARMED"
AUTO_REARMED                    = "AUTO_REARMED"
WAITING_FOR_BREACH              = "WAITING_FOR_BREACH"
WAITING_FOR_CONTRACT_AT_BREACH  = "WAITING_FOR_CONTRACT_AT_BREACH"
SKIPPED_TERMINAL                = "SKIPPED_TERMINAL"
SKIPPED_AFTER_CUTOFF            = "SKIPPED_AFTER_CUTOFF"
BROKEN_NEEDS_CODE               = "BROKEN_NEEDS_CODE"
BLOCKED_RISK                    = "BLOCKED_RISK"


# ── Watcher evidence detection ─────────────────────────────────────────────────

_REAL_OPTION_CONTRACT_RE = re.compile(r'\d{6}[CP]\d{5,8}')


def _coerce_meta(raw_meta) -> dict:
    if isinstance(raw_meta, dict):
        return dict(raw_meta)
    if isinstance(raw_meta, str) and raw_meta.strip():
        try:
            parsed = json.loads(raw_meta)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
    return {}


def _has_watcher_evidence(order: dict) -> tuple[bool, str]:
    """Return (has_evidence, description) for watcher-held row detection.

    Mirrors the logic in ap/order_monitor._is_valid_watcher_held_pending_trigger.
    Any single item is sufficient:
      1. trigger_price > 0
      2. meta.trigger_type present
      3. meta.watcher_audit present
      4. meta.watcher_audit_history present
      5. Real option contract symbol (regex)
      6. DEFERRED:* contract
      7. meta.contract_deferred = True
    """
    meta = _coerce_meta(order.get("meta"))
    contract = str(order.get("contract") or "").strip()

    # 1. trigger_price
    tp = order.get("trigger_price")
    if tp is not None:
        try:
            if float(tp) > 0:
                return True, f"trigger_price={tp}"
        except (TypeError, ValueError):
            pass

    # 2. meta.trigger_type
    tt = meta.get("trigger_type") or meta.get("trigger_type_label")
    if tt and str(tt).strip():
        return True, f"trigger_type={tt}"

    # 3. meta.watcher_audit
    if meta.get("watcher_audit"):
        return True, "watcher_audit_present"

    # 4. meta.watcher_audit_history
    if meta.get("watcher_audit_history"):
        return True, "watcher_audit_history_present"

    # 5. Real option contract symbol
    if contract and len(contract) >= 8 and _REAL_OPTION_CONTRACT_RE.search(contract):
        return True, f"real_option_contract={contract}"

    # 6. DEFERRED:*
    if contract.upper().startswith("DEFERRED:"):
        return True, f"deferred_contract={contract}"

    # 7. meta.contract_deferred
    if meta.get("contract_deferred"):
        return True, "contract_deferred=True"

    return False, "no_watcher_evidence"


def _is_deferred_contract(contract: str) -> bool:
    return str(contract or "").upper().startswith("DEFERRED:")


def _is_after_eod_cutoff() -> bool:
    """Return True if current ET time is at or past the entry cutoff."""
    try:
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
    except ImportError:
        try:
            import pytz
            et = pytz.timezone("America/New_York")
        except ImportError:
            # Cannot determine ET — conservative: assume NOT past cutoff.
            return False
    now_et = datetime.now(et)
    return (
        now_et.hour > MHA_EOD_CUTOFF_HOUR
        or (now_et.hour == MHA_EOD_CUTOFF_HOUR and now_et.minute >= MHA_EOD_CUTOFF_MIN)
    )


# ── Plan builder (mirrors APStartupRecovery._build_recovery_plan_from_order) ──

def _build_audit_plan(order: dict):
    """Rebuild a minimal watcher plan from an orders row.

    Only used for watcher re-arm — never for broker submission.
    Returns None if the plan cannot be built (missing ticker or trigger).
    """
    meta = _coerce_meta(order.get("meta"))
    contract = (
        order.get("contract")
        or meta.get("selected_contract")
        or meta.get("contract_symbol")
        or ""
    )
    direction = str(
        order.get("direction") or meta.get("direction") or meta.get("side") or "CALL"
    ).upper()
    ticker = str(
        order.get("symbol") or meta.get("symbol") or meta.get("ticker") or ""
    ).upper()
    trigger = (
        order.get("trigger_price")
        if order.get("trigger_price") is not None
        else meta.get("signal_entry_price")
    )

    if not ticker or trigger is None:
        return None

    plan = types.SimpleNamespace(
        signal_id=str(order.get("signal_id") or meta.get("signal_id") or order.get("local_order_id") or ""),
        canonical_signal_id=str(
            order.get("canonical_signal_id") or meta.get("canonical_signal_id") or ""
        ),
        client_id=str(order.get("client_id") or meta.get("client_id") or ""),
        execution_mode=str(
            order.get("execution_mode") or meta.get("execution_mode") or ""
        ).strip().lower(),
        local_order_id=str(order.get("local_order_id") or ""),
        materialization_generation=meta.get("materialization_generation"),
        trigger_crossed_at=meta.get("trigger_crossed_at"),
        plan_id=str(order.get("plan_id") or meta.get("plan_id") or ""),
        ticker=ticker,
        side=direction,
        direction=direction,
        score=float(order.get("score") or meta.get("score") or 65.0),
        tier=str(order.get("tier") or meta.get("tier") or "B"),
        trigger_price=float(trigger) if trigger is not None else None,
        stop_underlying=float(order.get("stop_underlying") or meta.get("stop_underlying") or 0) or None,
        target_underlying=float(order.get("target_underlying") or meta.get("target_underlying") or 0) or None,
        contract_symbol=str(contract or ""),
        pattern=str(order.get("pattern") or meta.get("pattern") or ""),
        timeframe=str(order.get("timeframe") or meta.get("timeframe") or "1d"),
        prior_day_high=meta.get("prior_day_high"),
        prior_day_low=meta.get("prior_day_low"),
        strategy_type=str(meta.get("strategy_type") or ""),
        metadata=dict(meta),
    )

    # Mark deferred contracts explicitly on the plan
    if _is_deferred_contract(str(contract)):
        try:
            if plan.metadata is None:
                plan.metadata = {}
            plan.metadata["contract_deferred"] = True
        except Exception:
            pass

    return plan


# ── OSM meta persistence ────────────────────────────────────────────────────────

def _persist_rearm_meta(osm, local_order_id: str, result: str, reason: str) -> None:
    """Write auto_rearm metadata into orders.meta for the given order.

    Safety:
      - Only writes to meta JSONB, never changes status/contract/qty.
      - Wrapped defensively — failure must never abort the audit.
    """
    ts = datetime.now(timezone.utc).isoformat()
    patch = json.dumps({
        "auto_rearm_attempted_at": ts,
        "auto_rearm_result": result,
        "auto_rearm_reason": reason,
    })
    try:
        from ap.db import conn, run_with_retry

        def _write():
            with conn() as c:
                c.execute(
                    """
                    UPDATE orders
                    SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb
                    WHERE local_order_id = %s
                    """,
                    (patch, local_order_id),
                )
        run_with_retry(_write)
    except Exception as exc:
        log.warning(
            "MORNING_HANDOFF_AUDIT: meta persist failed | order=%s result=%s error=%s",
            local_order_id, result, exc,
        )


# ── Row loader ─────────────────────────────────────────────────────────────────

def _load_audit_rows(client_id: str, execution_mode: str, lookback_hours: int) -> list[dict]:
    """Load ENTRY rows eligible for morning handoff audit.

    Excludes:
      - terminal statuses
      - rows with broker_order_id (already submitted/in-flight)
      - rows with submitted_ts (already submitted)
      - rows with filled_ts (already filled)
    """
    from ap.db import conn, run_with_retry

    cutoff_utc = (
        datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    ).isoformat()

    _mode_lower = str(execution_mode or "").lower()

    def _load():
        with conn() as c:
            c.execute(
                """
                SELECT
                    local_order_id,
                    signal_id,
                    plan_id,
                    symbol,
                    contract,
                    direction,
                    score,
                    tier,
                    trigger_price,
                    stop_underlying,
                    target_underlying,
                    pattern,
                    timeframe,
                    status,
                    broker_order_id,
                    submitted_ts,
                    filled_ts,
                    last_error,
                    meta,
                    created_ts
                FROM orders
                WHERE client_id = %s
                  AND kind = 'ENTRY'
                  AND status IN ('PENDING_TRIGGER', 'WATCHING', 'CREATED', 'RETRY_ELIGIBLE')
                  AND execution_mode = %s
                  AND created_ts >= %s
                  AND broker_order_id IS NULL
                  AND submitted_ts IS NULL
                  AND filled_ts IS NULL
                ORDER BY created_ts ASC
                """,
                (client_id, _mode_lower, cutoff_utc),
            )
            return c.fetchall() or []

    return run_with_retry(_load) or []


# ── Core audit logic ────────────────────────────────────────────────────────────

def _audit_row(
    order: dict,
    *,
    entry_watcher,
    after_cutoff: bool,
    dry_run: bool,
    client_id: str,
    osm,
) -> dict:
    """Classify and optionally re-arm a single ENTRY row.

    Returns a row-level result dict with classification, action, and diagnostics.
    NEVER submits to broker, NEVER creates orders, NEVER mutates terminal rows.
    """
    local_id  = str(order.get("local_order_id") or "").strip()
    contract  = str(order.get("contract") or "").strip()
    status    = str(order.get("status") or "").upper().strip()
    symbol    = str(order.get("symbol") or "").strip()
    last_error = str(order.get("last_error") or "").strip()

    row_result = {
        "local_order_id":  local_id,
        "symbol":          symbol,
        "contract":        contract,
        "status":          status,
        "classification":  None,
        "action":          "none",
        "evidence":        None,
        "rearm_succeeded": None,
        "rearm_reason":    None,
        "dry_run":         dry_run,
    }

    # ── Terminal rows: never touch ────────────────────────────────────────────
    if status in _TERMINAL_STATUSES:
        row_result["classification"] = SKIPPED_TERMINAL
        row_result["action"] = "skip_terminal"
        return row_result

    # ── Already has broker fields: skip (order_monitor / fill_monitor manages) ─
    # (Belt-and-suspenders: the DB query already excludes these, but guard here
    # in case the caller passes a row with these set.)
    if order.get("broker_order_id") or order.get("submitted_ts"):
        row_result["classification"] = "SKIPPED_BROKER_MANAGED"
        row_result["action"] = "skip_broker_managed"
        return row_result

    # ── Watcher evidence check ─────────────────────────────────────────────────
    has_evidence, evidence_desc = _has_watcher_evidence(order)
    row_result["evidence"] = evidence_desc

    if not has_evidence:
        # No watcher evidence and no broker fields — structurally broken.
        row_result["classification"] = BROKEN_NEEDS_CODE
        row_result["action"] = "skip_no_evidence"
        log.warning(
            "MORNING_HANDOFF_ROW client=%s order_id=%s symbol=%s status=%s "
            "contract=%s classification=%s action=%s evidence=%s",
            client_id, local_id, symbol, status, contract,
            BROKEN_NEEDS_CODE, "skip_no_evidence", evidence_desc,
        )
        return row_result

    # ── After cutoff: session over for this row ────────────────────────────────
    if after_cutoff:
        row_result["classification"] = SKIPPED_AFTER_CUTOFF
        row_result["action"] = "skip_after_cutoff"
        log.info(
            "MORNING_HANDOFF_ROW client=%s order_id=%s symbol=%s status=%s "
            "contract=%s classification=%s action=%s",
            client_id, local_id, symbol, status, contract,
            SKIPPED_AFTER_CUTOFF, "skip_after_cutoff",
        )
        return row_result

    # ── No watcher available: cannot re-arm ───────────────────────────────────
    if entry_watcher is None:
        row_result["classification"] = BROKEN_NEEDS_CODE
        row_result["action"] = "skip_no_watcher"
        row_result["rearm_reason"] = "entry_watcher_missing"
        log.warning(
            "MORNING_HANDOFF_ROW client=%s order_id=%s symbol=%s "
            "classification=%s action=skip_no_watcher",
            client_id, local_id, symbol, BROKEN_NEEDS_CODE,
        )
        return row_result

    # ── Already watcher-owned: READY_ARMED ────────────────────────────────────
    try:
        already_owned = (
            hasattr(entry_watcher, "has_order")
            and bool(entry_watcher.has_order(local_id))
        )
    except Exception as _exc:
        already_owned = False
        log.warning(
            "MORNING_HANDOFF_AUDIT: ownership check failed for %s: %s", local_id, _exc
        )

    if already_owned:
        classification = (
            WAITING_FOR_CONTRACT_AT_BREACH if _is_deferred_contract(contract)
            else READY_ARMED
        )
        row_result["classification"] = classification
        row_result["action"] = "already_owned"
        log.info(
            "MORNING_HANDOFF_ROW client=%s order_id=%s symbol=%s status=%s "
            "contract=%s classification=%s action=already_owned evidence=%s",
            client_id, local_id, symbol, status, contract,
            classification, evidence_desc,
        )
        return row_result

    # ── Attempt re-arm (dry_run skips the watcher.watch call) ─────────────────
    if dry_run:
        classification = (
            WAITING_FOR_CONTRACT_AT_BREACH if _is_deferred_contract(contract)
            else WAITING_FOR_BREACH
        )
        row_result["classification"] = classification
        row_result["action"] = "dry_run_would_rearm"
        row_result["rearm_succeeded"] = None  # not attempted
        log.info(
            "MORNING_HANDOFF_ROW client=%s order_id=%s symbol=%s status=%s "
            "contract=%s classification=%s action=dry_run_would_rearm evidence=%s",
            client_id, local_id, symbol, status, contract,
            classification, evidence_desc,
        )
        return row_result

    # Build plan for watcher.watch()
    plan = _build_audit_plan(order)
    if plan is None:
        row_result["classification"] = BROKEN_NEEDS_CODE
        row_result["action"] = "skip_plan_build_failed"
        row_result["rearm_reason"] = "plan_rebuild_failed_missing_ticker_or_trigger"
        log.warning(
            "MORNING_HANDOFF_ROW client=%s order_id=%s symbol=%s "
            "classification=%s action=skip_plan_build_failed",
            client_id, local_id, symbol, BROKEN_NEEDS_CODE,
        )
        return row_result

    # A confirmed timestamp with unproven lifecycle identity is not a reason
    # to clear evidence and continue as pre-breach.  Refuse before the watcher
    # or the audit metadata writer can touch the order.
    if not recovery_trigger_evidence_identity_is_proven(plan, local_id):
        rearm_reason = RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN
        row_result["classification"] = BLOCKED_RISK
        row_result["action"] = "rearm_refused_identity_unproven"
        row_result["rearm_succeeded"] = False
        row_result["rearm_reason"] = rearm_reason
        log.critical(
            "MORNING_HANDOFF_ROW %s client=%s order_id=%s — row unchanged",
            rearm_reason, client_id, local_id,
        )
        return row_result

    # Call watcher.watch() — ownership re-arm only, NOT broker submit
    #
    # ARCHITECTURAL SAFETY PROOF (Option C):
    # watch() → add_signal() register the signal into entry_watcher._pending.
    # The on_trigger callback (→ ExecutionCore → OSM.submit → broker) is ONLY
    # called from entry_watcher._poll_loop(), which runs in a background daemon
    # thread started by entry_watcher.start(). The morning handoff audit NEVER
    # calls entry_watcher.start(), so the poll thread is either:
    #   a) already running from normal startup (background thread, independent
    #      of the audit's watch() call — breach evaluation only fires on the
    #      NEXT poll cycle, not synchronously during watch()), or
    #   b) not yet started (pre-market audit before start()) — no poll, no trigger.
    # Therefore: watch() during the audit cannot and does not synchronously
    # call on_trigger, ExecutionCore, OSM submit, or broker.submit_order.
    # Source-verified: 'on_trigger' does NOT appear in watch() lines 2061–2380
    # or add_signal() lines 1554–2060 of ap_entry_watcher.py.
    #
    # recovery_rearm=True / no_cancel_on_reject=True — safe recovery mode:
    # These flags suppress every staleness/drift/stop rejection and every
    # cancel_pending_entry call inside watch() and add_signal(). The row's
    # contract, qty, limit_price, status are never mutated during the audit.
    rearm_reason = None
    rearm_succeeded = False
    try:
        armed = bool(entry_watcher.watch(
            plan,
            local_id,
            recovery_rearm=True,
            no_cancel_on_reject=True,
        ))
        rearm_succeeded = armed
        rearm_reason = (
            "watch_returned_true"
            if armed
            else getattr(entry_watcher, "_last_reject_reason", "watch_returned_false")
        )
    except Exception as exc:
        rearm_reason = f"watch_exception:{type(exc).__name__}:{exc}"
        log.error(
            "MORNING_HANDOFF_AUDIT: watcher.watch exception | order=%s error=%s",
            local_id, exc,
        )

    row_result["rearm_succeeded"] = rearm_succeeded
    row_result["rearm_reason"] = str(rearm_reason or "")

    if rearm_succeeded:
        classification = (
            WAITING_FOR_CONTRACT_AT_BREACH if _is_deferred_contract(contract)
            else AUTO_REARMED
        )
        action = "auto_rearmed"
    else:
        classification = BLOCKED_RISK
        action = "rearm_failed"

    row_result["classification"] = classification
    row_result["action"] = action

    # Persist auto_rearm metadata into orders.meta
    if osm is not None:
        _persist_rearm_meta(
            osm, local_id,
            result="success" if rearm_succeeded else "failed",
            reason=str(rearm_reason or ""),
        )

    log.info(
        "MORNING_HANDOFF_ROW client=%s order_id=%s symbol=%s status=%s "
        "contract=%s classification=%s action=%s rearm_succeeded=%s "
        "rearm_reason=%s evidence=%s",
        client_id, local_id, symbol, status, contract,
        classification, action, rearm_succeeded, rearm_reason, evidence_desc,
    )
    return row_result


# ── Public entry point ──────────────────────────────────────────────────────────

def run_morning_handoff_audit(
    *,
    client_id: str,
    entry_watcher,
    osm,
    execution_mode: str = "live",
    dry_run: bool = False,
    lookback_hours: int | None = None,
) -> dict:
    """Run the morning handoff audit for one client.

    Classifies all non-terminal ENTRY rows in the audit window and, when safe,
    re-arms watcher ownership.

    Safety invariants enforced:
      - NEVER calls broker.submit_order or any broker write path
      - NEVER transitions an order to SUBMITTED
      - NEVER creates new ENTRY orders
      - NEVER changes contract / limit_price / qty
      - NEVER mutates terminal rows
      - NEVER duplicates watcher state for the same local_order_id
      - Idempotent: safe to call multiple times

    Returns a summary dict with per-client counts and per-row details.
    """
    _start = time.monotonic()
    _lb_hours = lookback_hours if lookback_hours is not None else MHA_LOOKBACK_HOURS
    after_cutoff = _is_after_eod_cutoff()

    summary: dict = {
        "ok":                           True,
        "client_id":                    client_id,
        "execution_mode":               str(execution_mode or "").lower(),
        "scanned":                      0,
        "ready_armed":                  0,
        "auto_rearmed":                 0,
        "waiting_for_breach":           0,
        "waiting_for_contract_at_breach": 0,
        "skipped_terminal":             0,
        "skipped_after_cutoff":         0,
        "blocked_risk":                 0,
        "broken_needs_code":            0,
        "dry_run":                      dry_run,
        "after_cutoff":                 after_cutoff,
        "lookback_hours":               _lb_hours,
        "rows":                         [],
        "errors":                       [],
    }

    try:
        rows = _load_audit_rows(client_id, execution_mode, _lb_hours)
    except Exception as exc:
        log.error("MORNING_HANDOFF_AUDIT: row load failed | client=%s error=%s", client_id, exc)
        summary["ok"] = False
        summary["errors"].append(f"row_load_failed: {exc}")
        return summary

    summary["scanned"] = len(rows)

    for row in rows:
        order = dict(row or {})
        try:
            row_result = _audit_row(
                order,
                entry_watcher=entry_watcher,
                after_cutoff=after_cutoff,
                dry_run=dry_run,
                client_id=client_id,
                osm=osm,
            )
        except Exception as exc:
            local_id = str(order.get("local_order_id") or "?")
            log.error(
                "MORNING_HANDOFF_AUDIT: row audit exception | client=%s order=%s error=%s",
                client_id, local_id, exc,
            )
            row_result = {
                "local_order_id": local_id,
                "symbol": str(order.get("symbol") or ""),
                "contract": str(order.get("contract") or ""),
                "status": str(order.get("status") or ""),
                "classification": BROKEN_NEEDS_CODE,
                "action": "audit_exception",
                "evidence": None,
                "rearm_succeeded": None,
                "rearm_reason": f"audit_exception:{exc}",
                "dry_run": dry_run,
            }
            summary["errors"].append(f"{local_id}: {exc}")

        c = row_result.get("classification")
        if c == READY_ARMED:
            summary["ready_armed"] += 1
        elif c == AUTO_REARMED:
            summary["auto_rearmed"] += 1
        elif c == WAITING_FOR_BREACH:
            summary["waiting_for_breach"] += 1
        elif c == WAITING_FOR_CONTRACT_AT_BREACH:
            summary["waiting_for_contract_at_breach"] += 1
        elif c == SKIPPED_TERMINAL:
            summary["skipped_terminal"] += 1
        elif c == SKIPPED_AFTER_CUTOFF:
            summary["skipped_after_cutoff"] += 1
        elif c == BLOCKED_RISK:
            summary["blocked_risk"] += 1
        elif c in (BROKEN_NEEDS_CODE, None):
            summary["broken_needs_code"] += 1

        summary["rows"].append(row_result)

    elapsed = time.monotonic() - _start

    log.info(
        "MORNING_HANDOFF_AUDIT_SUMMARY client=%s scanned=%d ready=%d rearmed=%d "
        "waiting=%d waiting_deferred=%d cutoff=%d blocked=%d broken=%d "
        "dry_run=%s after_cutoff=%s elapsed=%.2fs",
        client_id,
        summary["scanned"],
        summary["ready_armed"],
        summary["auto_rearmed"],
        summary["waiting_for_breach"],
        summary["waiting_for_contract_at_breach"],
        summary["skipped_after_cutoff"],
        summary["blocked_risk"],
        summary["broken_needs_code"],
        dry_run,
        after_cutoff,
        elapsed,
    )

    return summary
