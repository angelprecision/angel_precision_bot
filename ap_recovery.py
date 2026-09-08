"""
ap_recovery.py — Full Restart Recovery
=======================================
Restores all runtime state for a ClientRunner after a restart:

  1. Open positions   — re-register with APPositionManager
  2. Pending entries  — re-submit stale CREATED/SUBMITTED/ACKNOWLEDGED to
                        fill_monitor by verifying broker status first
  3. Pending exits    — reattach exit protections to all CLOSING positions
  4. Reserved buying power — recompute from open + pending orders
  5. Dedup/session state  — repopulate master_control._seen_signals from
                            recent trade_queue rows (prevents re-trading
                            signals that were already queued pre-restart)

Usage:
    recovery = APStartupRecovery(
        client_id=email,
        broker=broker,
        osm=order_state_machine,
        pm=position_manager,
        master_control=master_control,
        exit_engine=exit_eng,
    )
    recovered = recovery.run()
    log.info("Recovery: %s", recovered)
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
from ap.pending_trigger_classifier import (
    has_canonical_materialization_retry_authority,
    has_conflicting_materialization_retry_authority,
    is_active_materialization_in_flight,
    resolve_materialization_retry_schedule,
)
from ap.pending_trigger_restart_recovery import _RecoveryPlan

log = logging.getLogger("ap.recovery")

# How far back to look for signals to re-seed dedup state
DEDUP_LOOKBACK_HOURS = int(os.getenv("DEDUP_LOOKBACK_HOURS", "8"))

# Statuses that mean an order is still live (entry side)
ENTRY_LIVE_STATUSES  = frozenset({"CREATED", "SUBMITTED", "ACKNOWLEDGED", "PARTIAL_FILL"})

# Statuses that mean an exit is in-flight
EXIT_LIVE_STATUSES   = frozenset({"EXIT_SUBMITTED", "EXIT_ACKNOWLEDGED", "EXIT_PARTIAL_FILL"})

# Position statuses that still consume runtime management after restart.
# PARTIAL and ACTIVE are legacy/scale-out compatible; remaining qty always wins.
ACTIVE_POSITION_STATUSES = ("OPEN", "CLOSING", "PARTIAL", "ACTIVE")

_OCC_SIDE_RE = re.compile(r"\d{6}([CP])\d{8}$")
_VALID_EXECUTION_MODES = frozenset({"PAPER", "LIVE"})


def _safe_int(value, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _extract_explicit_fill_qty(raw: dict) -> Optional[int]:
    """Extract explicit cumulative filled qty from broker payload.

    Startup recovery must never invent qty=0 for a broker-filled order.  A
    missing value means the adapter did not give us enough fill truth yet; leave
    the order for fill_monitor/reconciler instead of terminalizing it.
    """
    for key in (
        "filled_qty",
        "filled_quantity",
        "cumulative_filled_qty",
        "cumulative_filled_quantity",
        "exec_quantity",
        "executed_quantity",
        "filled",
    ):
        if key not in raw:
            continue
        val = raw.get(key)
        if val is None or val == "":
            continue
        try:
            qty = int(float(val))
            if qty > 0:
                return qty
        except Exception:
            continue
    return None


def _extract_avg_fill_price(raw: dict) -> Optional[float]:
    """Extract explicit execution price from broker payload.

    Do not use generic broker "price" / "avg_price" fields here: those can be
    limit/stop prices, not execution truth.
    """
    for key in (
        "avg_fill_price",
        "average_fill_price",
        "fill_price",
        "filled_avg_price",
    ):
        if key not in raw:
            continue
        val = raw.get(key)
        if val is None or val == "":
            continue
        try:
            px = float(val)
            if px > 0:
                return px
        except Exception:
            continue
    return None


def _write_fill_truth_blocked_meta(
    client_id: str,
    local_order_id: str,
    *,
    reason: str,
    broker_order_id: str,
    broker_status: str,
    extracted_qty,
    extracted_price,
) -> None:
    """
    P0 (PR #259): durable diagnostic when recovery REFUSES to mark a
    broker-"filled" order FILLED because the payload lacks positive executed
    quantity and/or average fill price.

    The refusal itself already existed (RECOVERY_FILL_TRUTH_MISSING /
    RECOVERY_EXIT_FILL_TRUTH_MISSING logged, no transition, row left for
    fill_monitor/reconciler). What was missing is a DURABLE record: the
    block lived only in logs and the in-memory result dict, so a row that
    kept failing extraction across restarts carried no forensic trail in
    the database. This helper merges the block reason, the broker order id,
    the raw broker status, and the (invalid) extracted values into
    orders.meta.

    Strictly additive and best-effort: jsonb concat (never overwrites
    existing meta), no status change, no position writes, never raises —
    a diagnostics failure must not affect the recovery pass.
    """
    try:
        from ap.db import conn as _conn, run_with_retry as _rwr

        _patch = json.dumps({
            "recovery_fill_truth_block": {
                "reason": reason,
                "broker_order_id": str(broker_order_id or ""),
                "broker_status_raw": str(broker_status or ""),
                "extracted_filled_qty": extracted_qty,
                "extracted_avg_fill_price": extracted_price,
                "blocked_at": datetime.now(timezone.utc).isoformat(),
                "recorded_by": "ap_recovery",
            }
        })

        def _merge():
            with _conn() as c:
                c.execute(
                    """
                    UPDATE orders
                    SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb
                    WHERE local_order_id = %s AND client_id = %s
                    """,
                    (_patch, local_order_id, client_id),
                )

        _rwr(_merge)
    except Exception as _exc:
        log.debug(
            "[%s] fill-truth block meta write failed (non-fatal): %s",
            client_id, _exc,
        )


def _normalize_execution_mode(value) -> str | None:
    mode = str(value or "").strip().upper()
    return mode if mode in _VALID_EXECUTION_MODES else None


def _resolve_side_from_order_or_meta(order: dict, meta: dict | None = None) -> tuple[str | None, str]:
    """Resolve strategy side without ever defaulting missing direction to CALL."""
    meta = meta or {}
    raw = str(
        order.get("direction")
        or order.get("side")
        or meta.get("direction")
        or meta.get("side")
        or ""
    ).upper().strip()

    if raw in {"CALL", "PUT"}:
        return raw, "field"

    if raw in {"BUY", "LONG", "CALLS", "BULLISH"}:
        return "CALL", "alias"

    if raw in {"SELL", "SHORT", "PUTS", "BEARISH"}:
        return "PUT", "alias"

    contract = str(
        order.get("contract")
        or order.get("symbol")
        or meta.get("selected_contract")
        or meta.get("contract_symbol")
        or meta.get("contract")
        or ""
    ).upper().strip()

    m = _OCC_SIDE_RE.search(contract)
    if m:
        return ("CALL" if m.group(1) == "C" else "PUT"), "occ_contract"

    return None, "unresolved"


def _position_reserved_cost(pos: dict) -> float:
    """Return reserved position capital with fill-cost fallback."""
    for key in ("reserved_cost", "cost"):
        cost = _safe_float(pos.get(key), 0.0)
        if cost > 0:
            return cost

    px = (
        _safe_float(pos.get("avg_fill"), 0.0)
        or _safe_float(pos.get("entry_price"), 0.0)
        or _safe_float(pos.get("fill_price"), 0.0)
    )
    qty = (
        _safe_int(pos.get("quantity_remaining"), 0)
        or _safe_int(pos.get("qty"), 0)
        or _safe_int(pos.get("quantity"), 0)
        or _safe_int(pos.get("contracts"), 0)
    )
    if px > 0 and qty > 0:
        return float(px) * int(qty) * 100.0
    return 0.0


def resume_pre_submit_proof_retry(order_row: dict, osm, execution_core) -> dict:
    """Consume one due PRE_SUBMIT_PROOF_RETRY row without watcher ownership.

    The first CAS advances the durable materialization generation and restores
    BROKER_READY.  The canonical execution-core recovery callback then performs
    the persisted-row handoff proof, fresh market validation, final LIVE gates,
    submit-intent CAS, and broker submit.
    """
    row = dict(order_row or {})
    meta = row.get("meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    now = datetime.now(timezone.utc)
    local_order_id = str(row.get("local_order_id") or "").strip()
    client_id = str(row.get("client_id") or "").strip().lower()
    mode = _normalize_execution_mode(row.get("execution_mode"))
    if not local_order_id or not client_id or mode is None:
        return {"disposition": "TERMINAL_DURABLE", "reason_code": "PROOF_RETRY_INVALID_IDENTITY", "terminal_status": "ERROR"}
    def _parse_ts(value):
        try:
            parsed = datetime.fromisoformat(str(value))
            return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed
        except Exception:
            return None

    due_at = _parse_ts(meta.get("proof_retry_next_at"))
    deadline = _parse_ts(meta.get("proof_retry_deadline") or meta.get("absolute_entry_deadline"))
    attempt = _safe_int(meta.get("proof_retry_attempt"), 0) + 1
    max_attempts = _safe_int(meta.get("proof_retry_max_attempts"), 3)
    if due_at is None or due_at > now:
        return {"disposition": "NOT_DUE", "reason_code": "PROOF_RETRY_NOT_DUE"}
    if deadline is None or now >= deadline:
        return {"disposition": "TERMINAL_DURABLE", "reason_code": "PROOF_RETRY_DEADLINE_EXCEEDED", "terminal_status": "EXPIRED"}
    if attempt > max_attempts:
        return {"disposition": "TERMINAL_DURABLE", "reason_code": "PROOF_RETRY_MAX_ATTEMPTS", "terminal_status": "EXPIRED"}
    if osm is None or execution_core is None:
        return {"disposition": "RETRY_WAIT", "reason_code": "PROOF_RETRY_CONSUMER_UNAVAILABLE"}
    if client_id != str(getattr(osm, "client_id", "") or "").strip().lower():
        return {"disposition": "TERMINAL_DURABLE", "reason_code": "PROOF_RETRY_CLIENT_ID_MISMATCH", "terminal_status": "ERROR"}
    core_mode = _normalize_execution_mode(
        getattr(execution_core, "execution_mode", None) or getattr(execution_core, "mode", None)
    )
    if core_mode != mode:
        return {"disposition": "TERMINAL_DURABLE", "reason_code": "PROOF_RETRY_EXECUTION_MODE_MISMATCH", "terminal_status": "ERROR"}

    generation = _safe_int(meta.get("materialization_generation"), 0)
    owner = f"proof_retry:{client_id}:{local_order_id}:{generation + 1}"
    claim = getattr(osm, "claim_pre_submit_proof_retry", None)
    if not callable(claim) or not claim(
        local_order_id,
        owner=owner,
        expected_generation=generation,
        new_generation=generation + 1,
        attempt=attempt,
        claimed_at=now.isoformat(),
    ):
        return {"disposition": "CLAIM_LOST", "reason_code": "PROOF_RETRY_CLAIM_NOT_ACQUIRED"}

    try:
        persisted = osm.get_order(local_order_id)
    except Exception as exc:
        persisted = None
        read_error = f"read_error:{type(exc).__name__}"
    else:
        read_error = "row_missing" if not isinstance(persisted, dict) else ""
    if not isinstance(persisted, dict):
        delay = max(1, int(os.getenv("PRE_SUBMIT_PROOF_RETRY_DELAY_SECONDS", "5")))
        reschedule = getattr(osm, "persist_pre_submit_proof_retry", None)
        ok = bool(callable(reschedule) and reschedule(
            local_order_id,
            owner=owner,
            generation=generation + 1,
            retry_attempt=attempt,
            max_attempts=max_attempts,
            next_retry_at=(now + timedelta(seconds=delay)).isoformat(),
            retry_deadline=deadline.isoformat(),
            read_error=read_error,
            selected_at=str(meta.get("selected_at") or now.isoformat()),
            selected_quote_at=str(meta.get("selected_quote_at") or now.isoformat()),
        ))
        return {
            "disposition": "RETRY_WAIT" if ok else "TERMINAL_DURABLE",
            "reason_code": read_error if ok else "PROOF_RETRY_RESCHEDULE_FAILED",
            "terminal_status": "ERROR",
        }

    # Run the same persisted-row proof primitives used by the breach callback.
    from ap_execution_core import _classify_materialization_handoff, _classify_order_row_read
    proof_snapshot = {
        "captured": True,
        "selector_contract": str(meta.get("selected_contract") or ""),
        "selector_qty": _safe_int(meta.get("selected_qty"), 0),
        "copied_plan_contract": str(persisted.get("contract") or ""),
        "copied_plan_limit": _safe_float(persisted.get("limit_price"), 0.0),
        "copied_plan_qty": _safe_int(persisted.get("qty"), 0),
    }
    verdict, proof_reason = _classify_order_row_read(
        handoff_snapshot=proof_snapshot,
        order_row_raw=persisted,
        read_error=None,
    )
    if verdict != "PASS":
        return {
            "disposition": "TERMINAL_DURABLE",
            "reason_code": f"PROOF_RETRY_HANDOFF_FAILED:{proof_reason}",
            "terminal_status": "ERROR",
        }
    aligned, mismatch = _classify_materialization_handoff(
        handoff_snapshot=proof_snapshot,
        pre_submit_contract=proof_snapshot["copied_plan_contract"],
        pre_submit_limit=proof_snapshot["copied_plan_limit"],
        pre_submit_qty=proof_snapshot["copied_plan_qty"],
        order_row_contract=str(persisted.get("contract") or ""),
        order_row_limit=_safe_float(persisted.get("limit_price"), 0.0),
        order_row_qty=_safe_int(persisted.get("qty"), 0),
    )
    if not aligned:
        return {
            "disposition": "TERMINAL_DURABLE",
            "reason_code": f"PROOF_RETRY_HANDOFF_MISMATCH:{mismatch}",
            "terminal_status": "ERROR",
        }

    outcome = execution_core.resume_deferred_broker_ready_order(
        local_order_id=local_order_id,
        plan=None,
    ) or {}
    outcome.setdefault("proof_retry_attempt", attempt)
    outcome.setdefault("proof_retry_owner", owner)
    return outcome


class APStartupRecovery:
    """
    Runs once on ClientRunner startup to restore in-memory state from DB.
    Designed to be idempotent — safe to call even if nothing needs recovery.
    """

    def __init__(
        self,
        client_id: str,
        broker,
        osm,              # APOrderStateMachine
        pm,               # APPositionManager
        master_control,   # APMasterControl
        exit_engine=None, # APExitEngine (optional — needed for exit re-attachment)
        entry_watcher=None,  # APEntryWatcher (optional — needed for watcher reseed)
        execution_core=None, # APExecutionCore (optional — required for §2 safe BROKER_READY recovery)
    ):
        self.client_id     = str(client_id or "").strip().lower()
        self.broker        = broker
        self.osm           = osm
        self.pm            = pm
        self.mc            = master_control
        self.exit_engine   = exit_engine
        self.entry_watcher = entry_watcher
        self.execution_core = execution_core

    # ──────────────────────────────────────────────────────────────────────────
    # Entry point
    # ──────────────────────────────────────────────────────────────────────────

    def run(self, *, include_watcher_reseed: bool = True) -> dict:
        result = {
            "client_id":          self.client_id,
            "positions_recovered": 0,
            "entries_verified":    0,
            "entries_corrected":   0,
            "exits_reattached":    0,
            "stale_exit_claims_reconciled": 0,
            "buying_power_reserved": 0.0,
            "dedup_seeded":        0,
            "watchers_requeued":   0,
            "deferred_lifecycles_recovered": 0,
            "exit_fill_reconciliations_attempted": 0,
            "exit_fill_reconciliations_reconciled": 0,
            "exit_fill_reconciliations_quarantined": 0,
            "exit_fill_reconciliations_failed": 0,
            "exit_fill_reconciliations_skipped": 0,
            "errors":              [],
        }

        log.info("[%s] Startup recovery beginning...", self.client_id)

        if self._execution_mode() is None:
            log.error("[%s] RECOVERY_BLOCKED unknown execution_mode", self.client_id)
            result["errors"].append("recovery_unknown_execution_mode")
            return result

        try:
            self._recover_deferred_breach_lifecycles(result)
        except Exception as e:
            log.error("[%s] Deferred breach lifecycle recovery error: %s", self.client_id, e)
            result["errors"].append(f"deferred_lifecycle: {e}")

        try:
            self._retry_canonical_exit_fill_reconciliations(result)
        except Exception as e:
            log.critical(
                "[%s] Canonical EXIT-fill retry discovery failed: %s",
                self.client_id,
                e,
                exc_info=True,
            )
            result["errors"].append(f"exit_fill_reconciliation: {e}")
            result["exit_fill_reconciliations_failed"] += 1

        try:
            self._reconcile_stale_exit_generation_claims(result)
        except Exception as e:
            log.critical(
                "[%s] Stale EXIT claim reconciliation failed: %s",
                self.client_id,
                e,
                exc_info=True,
            )
            result["errors"].append(f"stale_exit_claims: {e}")

        try:
            live_exit_orders = self._recover_exit_fills_that_occurred_during_downtime(result)
        except Exception as e:
            log.error("[%s] Exit fill downtime recovery error: %s", self.client_id, e)
            result["errors"].append(f"exit_fill_downtime: {e}")
            live_exit_orders = []

        try:
            self._recover_positions(result)
        except Exception as e:
            log.error("[%s] Position recovery error: %s", self.client_id, e)
            result["errors"].append(f"positions: {e}")

        try:
            self._verify_pending_entries(result)
        except Exception as e:
            log.error("[%s] Entry verification error: %s", self.client_id, e)
            result["errors"].append(f"entries: {e}")

        try:
            self._reattach_live_exit_protections(result, live_exit_orders)
        except Exception as e:
            log.error("[%s] Exit reattachment error: %s", self.client_id, e)
            result["errors"].append(f"exits: {e}")

        try:
            self._recompute_buying_power(result)
        except Exception as e:
            log.error("[%s] Buying power recompute error: %s", self.client_id, e)
            result["errors"].append(f"buying_power: {e}")

        try:
            self._reseed_dedup(result)
        except Exception as e:
            log.error("[%s] Dedup reseed error: %s", self.client_id, e)
            result["errors"].append(f"dedup: {e}")

        if include_watcher_reseed:
            try:
                self._reseed_watchers(result)
            except Exception as e:
                log.error("[%s] Watcher reseed error: %s", self.client_id, e)
                result["errors"].append(f"watchers: {e}")

        log.info(
            "[%s] Recovery complete | positions=%d entries_verified=%d "
            "entries_corrected=%d exits=%d dedup=%d watchers_requeued=%d buying_power=$%.2f "
            "exit_fill_attempted=%d exit_fill_reconciled=%d exit_fill_quarantined=%d "
            "exit_fill_failed=%d exit_fill_skipped=%d errors=%d",
            self.client_id,
            result["positions_recovered"],
            result["entries_verified"],
            result["entries_corrected"],
            result["exits_reattached"],
            result["dedup_seeded"],
            result["watchers_requeued"],
            result["buying_power_reserved"],
            result["exit_fill_reconciliations_attempted"],
            result["exit_fill_reconciliations_reconciled"],
            result["exit_fill_reconciliations_quarantined"],
            result["exit_fill_reconciliations_failed"],
            result["exit_fill_reconciliations_skipped"],
            len(result["errors"]),
        )
        return result

    def _retry_canonical_exit_fill_reconciliations(self, result: dict) -> None:
        """Run the bounded, client-scoped accounting retry pass once at startup.

        This pass consumes only durable broker-confirmed fill state. It owns no
        broker adapter and cannot submit or cancel orders.
        """
        from ap.exit_fill_truth_guard import retry_pending_exit_fill_reconciliations

        outcomes = retry_pending_exit_fill_reconciliations(
            client_id=self.client_id,
            limit=100,
        )
        result["exit_fill_reconciliations_attempted"] += len(outcomes)
        for outcome in outcomes:
            local_order_id = str(outcome.get("local_order_id") or "")
            status = str(outcome.get("status") or "").strip().upper()
            if bool(outcome.get("reconciled")):
                result["exit_fill_reconciliations_reconciled"] += 1
                continue
            if status == "NOT_CLAIMED":
                result["exit_fill_reconciliations_skipped"] += 1
                log.info(
                    "[%s] STARTUP_EXIT_FILL_RECONCILIATION_ALREADY_CLAIMED order=%s",
                    self.client_id,
                    local_order_id,
                )
                continue
            if status == "QUARANTINED":
                result["exit_fill_reconciliations_quarantined"] += 1
                outcome_result = outcome.get("result") or {}
                diagnostic = (
                    outcome_result.get("proof_reconciliation")
                    or outcome_result
                    or {}
                )
                reason = str(
                    diagnostic.get("reason_code")
                    or status
                    or "EXIT_FILL_RECONCILIATION_UNRESOLVED"
                )
                log.critical(
                    "[%s] STARTUP_EXIT_FILL_RECONCILIATION_UNRESOLVED "
                    "order=%s status=%s reason=%s",
                    self.client_id,
                    local_order_id,
                    status,
                    reason,
                )
                result["errors"].append(
                    f"exit_fill_reconciliation_unresolved:{local_order_id}:{reason}"
                )
                continue
            result["exit_fill_reconciliations_failed"] += 1
            error = str(outcome.get("error") or "unknown retry failure")
            log.critical(
                "[%s] STARTUP_EXIT_FILL_RECONCILIATION_FAILED order=%s error=%s",
                self.client_id,
                local_order_id,
                error,
            )
            result["errors"].append(
                f"exit_fill_reconciliation_failed:{local_order_id}:{error}"
            )

    def recover_deferred_lifecycles(self) -> dict:
        """Lightweight runtime pass for durable deferred-breach ownership."""
        result = {
            "client_id": self.client_id,
            "deferred_lifecycles_recovered": 0,
            "errors": [],
        }
        if self._execution_mode() is None:
            result["errors"].append("recovery_unknown_execution_mode")
            return result
        try:
            self._recover_deferred_breach_lifecycles(result)
        except Exception as exc:
            result["errors"].append(f"deferred_lifecycle:{exc}")
        return result

    def _execution_mode(self) -> str | None:
        return _normalize_execution_mode(getattr(self.mc, "mode", None))

    # ──────────────────────────────────────────────────────────────────────────
    # Active position loading
    # ──────────────────────────────────────────────────────────────────────────

    def _load_active_positions(self) -> list[dict]:
        """Load all economically active position rows for this client."""
        from ap.db import conn, run_with_retry

        def _query():
            with conn() as c:
                c.execute(
                    """
                    SELECT *
                    FROM positions
                    WHERE client_id = %s
                      AND (
                        UPPER(COALESCE(status, '')) IN ('OPEN','CLOSING','PARTIAL','ACTIVE')
                        OR COALESCE(quantity_remaining, 0) > 0
                      )
                    ORDER BY entry_ts DESC NULLS LAST, created_at DESC NULLS LAST
                    """,
                    (self.client_id,),
                )
                return c.fetchall()

        return run_with_retry(_query) or []

    # ──────────────────────────────────────────────────────────────────────────
    # 1. Position recovery
    # ──────────────────────────────────────────────────────────────────────────

    def _recover_positions(self, result: dict):
        """Re-register active positions with PositionManager in-memory state."""
        all_active = self._load_active_positions()

        for pos in all_active:
            pos_id     = pos.get("id")
            underlying = pos.get("underlying") or pos.get("ticker", "?")
            status     = pos.get("status", "OPEN")
            qty        = _safe_int(pos.get("quantity_remaining"), 0) or _safe_int(pos.get("qty"), 0) or _safe_int(pos.get("quantity"), 0)
            direction, side_source = _resolve_side_from_order_or_meta(pos)
            direction_label = direction or "UNKNOWN"

            # Re-register with PositionManager if supported
            try:
                if self.pm and hasattr(self.pm, "register_recovered_position"):
                    self.pm.register_recovered_position(pos)
                elif self.pm and hasattr(self.pm, "add_position"):
                    self.pm.add_position(pos)
            except Exception as e:
                log.error(
                    "[%s] RECOVERY: failed to register recovered position %s: %s",
                    self.client_id, pos_id, e,
                )
                result.setdefault("errors", []).append(f"positions_register:{pos_id}:{e}")
                continue

            # Bump in-memory position count on master_control
            try:
                if hasattr(self.mc, "_position_count"):
                    self.mc._position_count += 1
            except Exception as _e:
                log.warning("recovery_position_count_inc_failed: %s", _e)

            # Bump sector counts on master_control if tracked
            try:
                sector = pos.get("sector", "")
                if sector and hasattr(self.mc, "_sector_counts"):
                    self.mc._sector_counts[sector] = (
                        self.mc._sector_counts.get(sector, 0) + 1
                    )
            except Exception as _e:
                log.warning("recovery_sector_counts_inc_failed: %s", _e)

            log.info(
                "[%s] RECOVERY: position restored | %s %s qty=%d status=%s pos=%s side_source=%s",
                self.client_id, underlying, direction_label, qty, status, pos_id, side_source,
            )
            result["positions_recovered"] += 1

    # ──────────────────────────────────────────────────────────────────────────
    # 2. Pending entry verification
    # ──────────────────────────────────────────────────────────────────────────

    def _verify_pending_entries(self, result: dict):
        """
        For every open entry order in DB, query broker for real status.
        If broker disagrees, advance the OSM to broker truth.
        This prevents phantom 'open' entries from blocking new trades after restart.
        """
        recovery_mode = self._execution_mode()
        if recovery_mode is None:
            result.setdefault("errors", []).append("recovery_unknown_execution_mode")
            return
        from ap.db import get_open_orders_for_reconcile, run_with_retry

        orders = run_with_retry(
            lambda: get_open_orders_for_reconcile(
                client_id=self.client_id,
                execution_mode=recovery_mode.lower(),
            )
        )
        # Only entry orders here
        entry_orders = [o for o in (orders or []) if o.get("kind") == "ENTRY"]
        result["entries_verified"] = len(entry_orders)

        for order in entry_orders:
            local_id   = order.get("local_order_id") or order.get("id")
            broker_oid = order.get("broker_order_id")
            db_status  = (order.get("status") or "").upper()
            contract   = order.get("contract") or "?"

            if not broker_oid or broker_oid in ("N/A", "PENDING", ""):
                log.warning(
                    "[%s] RECOVERY: entry %s has no broker_order_id (db_status=%s) — "
                    "may have been lost pre-restart",
                    self.client_id, local_id, db_status,
                )
                continue
            try:
                broker_raw    = self.broker.get_order(broker_oid) or {}
                broker_status = str(broker_raw.get("status") or "").lower()
            except Exception as e:
                log.warning("[%s] RECOVERY: broker get_order failed for %s: %s",
                            self.client_id, broker_oid, e)
                continue

            from ap_reconciler import BROKER_TO_OSM, BROKER_FILLED, BROKER_TERMINAL

            if broker_status in BROKER_FILLED:
                new_status = BROKER_TO_OSM.get(broker_status)
                if not new_status or new_status == db_status:
                    continue

                filled_qty = _extract_explicit_fill_qty(broker_raw)
                avg_fill   = _extract_avg_fill_price(broker_raw)
                if not filled_qty or filled_qty <= 0 or not avg_fill or avg_fill <= 0:
                    msg = (
                        f"RECOVERY_FILL_TRUTH_MISSING entry local={local_id} "
                        f"broker={broker_oid} contract={contract} broker_status={broker_status} "
                        "missing explicit filled_qty/avg_fill; leaving for fill_monitor/reconciler"
                    )
                    log.critical("[%s] %s", self.client_id, msg)
                    result.setdefault("errors", []).append(msg)
                    # P0 (PR #259): durable forensic trail on the row itself.
                    _write_fill_truth_blocked_meta(
                        self.client_id, local_id,
                        reason="recovery_fill_truth_missing",
                        broker_order_id=broker_oid,
                        broker_status=broker_status,
                        extracted_qty=filled_qty,
                        extracted_price=avg_fill,
                    )
                    continue

                try:
                    ok = self.osm.transition(
                        local_id, new_status,
                        filled_qty=filled_qty,
                        fill_price=avg_fill,
                    )
                    if ok:
                        result["entries_corrected"] += 1
                        log.info(
                            "[%s] RECOVERY: corrected filled entry %s | %s | %s → %s qty=%d avg=$%.4f",
                            self.client_id, local_id, contract, db_status, new_status, filled_qty, avg_fill,
                        )
                except Exception as e:
                    log.error("[%s] RECOVERY: OSM fill transition failed: %s", self.client_id, e)
                continue

            if broker_status in BROKER_TERMINAL:
                new_status = BROKER_TO_OSM.get(broker_status)
                if new_status and new_status != db_status:
                    try:
                        ok = self.osm.transition(
                            local_id, new_status,
                            last_error=f"recovery: broker_status={broker_status}",
                        )
                        if ok:
                            result["entries_corrected"] += 1
                            log.info(
                                "[%s] RECOVERY: corrected terminal entry %s | %s | %s → %s",
                                self.client_id, local_id, contract, db_status, new_status,
                            )
                    except Exception as e:
                        log.error("[%s] RECOVERY: OSM terminal transition failed: %s", self.client_id, e)

    # ──────────────────────────────────────────────────────────────────────────
    # 3. EXIT fill downtime recovery
    # ──────────────────────────────────────────────────────────────────────────

    def _load_closing_positions(self) -> list[dict]:
        from ap.db import list_positions, run_with_retry

        return run_with_retry(
            lambda: list_positions(client_id=self.client_id, status="CLOSING")
        ) or []

    def _resolve_active_exit_order_for_position(
        self,
        position: dict,
        result: dict,
    ) -> dict | None:
        from ap.db import conn, run_with_retry

        pos_id = position.get("id")
        underlying = position.get("underlying") or position.get("ticker", "?")
        pending_local_id = str(position.get("pending_exit_local_order_id") or "").strip()
        pending_broker_id = str(position.get("pending_exit_broker_order_id") or "").strip()

        if pending_local_id:
            def _by_local(local_id=pending_local_id):
                with conn() as c:
                    c.execute(
                        """
                        SELECT *
                        FROM orders
                        WHERE client_id=%s
                          AND local_order_id=%s
                          AND kind='EXIT'
                        LIMIT 2
                        """,
                        (self.client_id, local_id),
                    )
                    return c.fetchall()

            rows = [dict(row) for row in (run_with_retry(_by_local) or [])]
            if not rows:
                msg = (
                    "RECOVERY_PENDING_EXIT_LOCAL_ID_UNRESOLVED "
                    f"client={self.client_id} pos={pos_id} underlying={underlying} "
                    f"pending_local_order_id={pending_local_id}"
                )
                log.critical("[%s] %s", self.client_id, msg)
                result.setdefault("errors", []).append(msg)
                return None
            if len(rows) > 1:
                candidates = [
                    f"{str(row.get('local_order_id') or '')}/{str(row.get('broker_order_id') or '')}"
                    for row in rows
                ]
                msg = (
                    "RECOVERY_PENDING_EXIT_LOCAL_ID_AMBIGUOUS "
                    f"client={self.client_id} pos={pos_id} underlying={underlying} "
                    f"pending_local_order_id={pending_local_id} candidates={candidates}"
                )
                log.critical("[%s] %s", self.client_id, msg)
                result.setdefault("errors", []).append(msg)
                return None
            selected = rows[0]
        else:
            def _by_position(pid=pos_id):
                with conn() as c:
                    c.execute(
                        """
                        SELECT *
                        FROM orders
                        WHERE client_id=%s
                          AND position_id=%s
                          AND kind='EXIT'
                          AND status NOT IN (
                              'EXIT_FILLED',
                              'REJECTED',
                              'CANCELED',
                              'CANCELLED',
                              'EXPIRED',
                              'ERROR'
                          )
                        ORDER BY created_ts DESC
                        LIMIT 2
                        """,
                        (self.client_id, pid),
                    )
                    return c.fetchall()

            rows = [dict(row) for row in (run_with_retry(_by_position) or [])]
            if not rows:
                msg = (
                    f"RECOVERY_CLOSING_POSITION_WITHOUT_ACTIVE_EXIT "
                    f"pos={pos_id} underlying={underlying}"
                )
                log.critical("[%s] %s", self.client_id, msg)
                result.setdefault("errors", []).append(msg)
                return None
            if len(rows) > 1:
                candidates = [
                    f"{str(row.get('local_order_id') or '')}/{str(row.get('broker_order_id') or '')}"
                    for row in rows
                ]
                msg = (
                    "RECOVERY_ACTIVE_EXIT_IDENTITY_AMBIGUOUS "
                    f"client={self.client_id} pos={pos_id} underlying={underlying} "
                    f"candidates={candidates}"
                )
                log.critical("[%s] %s", self.client_id, msg)
                result.setdefault("errors", []).append(msg)
                return None
            selected = rows[0]

        selected_broker_id = str(selected.get("broker_order_id") or "").strip()
        if pending_broker_id and pending_broker_id != selected_broker_id:
            msg = (
                "RECOVERY_PENDING_EXIT_BROKER_ID_MISMATCH "
                f"client={self.client_id} pos={pos_id} pending_local_order_id={pending_local_id} "
                f"expected_broker_order_id={pending_broker_id} "
                f"selected_broker_order_id={selected_broker_id}"
            )
            log.critical("[%s] %s", self.client_id, msg)
            result.setdefault("errors", []).append(msg)
            return None
        return selected

    def _load_persisted_exit_order(self, local_order_id: str) -> dict | None:
        from ap.db import conn, run_with_retry

        def _load(local_id=local_order_id):
            with conn() as c:
                c.execute(
                    """
                    SELECT *
                    FROM orders
                    WHERE client_id=%s AND local_order_id=%s AND kind='EXIT'
                    LIMIT 1
                    """,
                    (self.client_id, local_id),
                )
                return c.fetchone()

        row = run_with_retry(_load)
        return dict(row) if row else None

    def _reconcile_stale_exit_generation_claims(self, result: dict) -> None:
        from ap.db import conn, run_with_retry
        from ap.exit_decision_idempotency_guard import (
            reconcile_stale_exit_generation_claim,
        )

        def _load():
            with conn() as c:
                c.execute(
                    """
                    SELECT generation_key
                    FROM exit_decision_generation_claims
                    WHERE client_id=%s
                      AND claim_state='AMBIGUOUS'
                      AND (
                            COALESCE(last_error,'') LIKE 'STALE_CLAIM_RECONCILIATION_REQUIRED%%'
                         OR (
                                COALESCE(last_error,'') LIKE 'STALE_CLAIM_RECONCILING:%%'
                            AND claimed_at <= NOW() - (300 * INTERVAL '1 second')
                         )
                      )
                    ORDER BY claimed_at ASC
                    """,
                    (self.client_id,),
                )
                return c.fetchall()

        rows = run_with_retry(_load) or []
        for raw in rows:
            generation_key = str((dict(raw) if not isinstance(raw, dict) else raw).get("generation_key") or "").strip()
            if not generation_key:
                continue
            reconciled = reconcile_stale_exit_generation_claim(
                generation_key,
                execution_core=self.execution_core,
                osm=self.osm,
            ) or {}
            if str(reconciled.get("claim_state") or "").strip().upper() != "AMBIGUOUS":
                result["stale_exit_claims_reconciled"] += 1

    def _recover_exit_fills_that_occurred_during_downtime(self, result: dict) -> list[dict]:
        """Route broker-confirmed downtime EXIT fills through the canonical reducer."""
        if self._execution_mode() is None:
            result.setdefault("errors", []).append("recovery_unknown_execution_mode")
            return []
        closings = self._load_closing_positions()
        if not closings:
            return []

        live_exit_orders: list[dict] = []

        for pos in closings:
            pos_id     = pos.get("id")
            underlying = pos.get("underlying") or pos.get("ticker", "?")
            try:
                exit_order = self._resolve_active_exit_order_for_position(pos, result)
            except Exception as e:
                log.error("[%s] RECOVERY: exit order lookup failed for pos %s: %s",
                          self.client_id, pos_id, e)
                continue

            if not exit_order:
                continue

            broker_oid = exit_order.get("broker_order_id")
            local_id   = exit_order.get("local_order_id")

            if not broker_oid or broker_oid in ("N/A", "PENDING", ""):
                log.warning(
                    "[%s] RECOVERY: exit order %s for pos %s has no broker_order_id",
                    self.client_id, local_id, pos_id,
                )
                continue

            # Verify at broker
            try:
                broker_raw    = self.broker.get_order(broker_oid) or {}
                broker_status = str(broker_raw.get("status") or "").lower()
            except Exception as e:
                log.warning("[%s] RECOVERY: broker exit order check failed: %s",
                            self.client_id, e)
                continue

            from ap_reconciler import BROKER_FILLED, BROKER_TERMINAL, BROKER_TO_OSM
            from ap.exit_fill_truth_guard import reconcile_confirmed_exit_fill

            if broker_status in BROKER_FILLED:
                filled_qty = _extract_explicit_fill_qty(broker_raw)
                avg_fill   = _extract_avg_fill_price(broker_raw)
                if not filled_qty or filled_qty <= 0 or not avg_fill or avg_fill <= 0:
                    msg = (
                        f"RECOVERY_EXIT_FILL_TRUTH_MISSING local={local_id} "
                        f"broker={broker_oid} pos={pos_id} broker_status={broker_status} "
                        "missing explicit filled_qty/avg_fill; keeping CLOSING for reconciler/fill_monitor"
                    )
                    log.critical("[%s] %s", self.client_id, msg)
                    result.setdefault("errors", []).append(msg)
                    # P0 (PR #259): durable forensic trail on the row itself.
                    _write_fill_truth_blocked_meta(
                        self.client_id, local_id,
                        reason="recovery_exit_fill_truth_missing",
                        broker_order_id=broker_oid,
                        broker_status=broker_status,
                        extracted_qty=filled_qty,
                        extracted_price=avg_fill,
                    )
                    continue

                try:
                    exit_status = (
                        "EXIT_PARTIAL_FILL"
                        if broker_status == "partially_filled"
                        else "EXIT_FILLED"
                    )
                    self.osm.transition(
                        local_id,
                        exit_status,
                        filled_qty=filled_qty,
                        fill_price=avg_fill,
                    )
                except Exception as e:
                    log.error("[%s] RECOVERY: %s transition failed: %s",
                              self.client_id, broker_status.upper(), e)
                    continue

                persisted_order = self._load_persisted_exit_order(local_id)
                persisted_status = str((persisted_order or {}).get("status") or "").upper()
                if persisted_status not in {"EXIT_FILLED", "EXIT_PARTIAL_FILL"}:
                    msg = (
                        f"RECOVERY_EXIT_FILL_PERSISTED_STATUS_INVALID local={local_id} "
                        f"pos={pos_id} status={persisted_status or 'MISSING'}"
                    )
                    log.critical("[%s] %s", self.client_id, msg)
                    result.setdefault("errors", []).append(msg)
                    continue

                canonical_result = {
                    "status": persisted_order["status"],
                    "broker_order_id": persisted_order["broker_order_id"],
                    "filled_qty": persisted_order["filled_qty"],
                    "fill_price": persisted_order["fill_price"],
                    "filled_ts": persisted_order["filled_ts"],
                }
                try:
                    reconcile_confirmed_exit_fill(persisted_order, canonical_result)
                    log.info(
                        "[%s] RECOVERY: exit reconciled during downtime | pos=%s %s | "
                        "qty=%d avg=$%.2f status=%s",
                        self.client_id,
                        pos_id,
                        underlying,
                        filled_qty,
                        avg_fill,
                        persisted_status,
                    )
                except Exception as e:
                    log.critical(
                        "[%s] STARTUP_EXIT_FILL_CANONICAL_RECONCILIATION_FAILED "
                        "pos=%s order=%s status=%s error=%s",
                        self.client_id,
                        pos_id,
                        local_id,
                        persisted_status,
                        e,
                    )
                    result.setdefault("errors", []).append(
                        f"exit_fill_downtime_reconcile:{local_id}:{e}"
                    )
                if persisted_status == "EXIT_PARTIAL_FILL":
                    live_exit_orders.append(persisted_order)
                continue

            if broker_status in BROKER_TERMINAL:
                try:
                    self.osm.transition(
                        local_id, BROKER_TO_OSM.get(broker_status, "CANCELED"),
                        last_error=f"recovery: broker_status={broker_status}",
                    )
                    # Revert position
                    from ap.db import conn as _conn, run_with_retry

                    def _revert(pid=pos_id):
                        with _conn() as c:
                            c.execute(
                                "UPDATE positions SET status='OPEN', exit_reason=NULL, "
                                "updated_ts=NOW() WHERE id=%s AND client_id=%s",
                                (pid, self.client_id),
                            )

                    run_with_retry(_revert)
                    log.warning(
                        "[%s] RECOVERY: exit was canceled during downtime | "
                        "pos=%s %s reverted to OPEN — EXIT MUST BE RETRIED",
                        self.client_id, pos_id, underlying,
                    )
                    result["exits_reattached"] += 1
                except Exception as e:
                    log.error("[%s] RECOVERY: exit revert failed: %s", self.client_id, e)
                continue

            live_exit_orders.append(exit_order)

        return live_exit_orders

    # ──────────────────────────────────────────────────────────────────────────
    # 4. Live exit protection reattachment
    # ──────────────────────────────────────────────────────────────────────────

    def _reattach_live_exit_protections(self, result: dict, live_exit_orders: list[dict]):
        """Reattach only EXIT orders already proven live by the downtime pass."""
        if self._execution_mode() is None:
            result.setdefault("errors", []).append("recovery_unknown_execution_mode")
            return
        if not live_exit_orders:
            return

        for exit_order in live_exit_orders:
            pos_id = exit_order.get("position_id")
            local_id = exit_order.get("local_order_id")
            underlying = (
                exit_order.get("underlying")
                or exit_order.get("ticker")
                or exit_order.get("contract")
                or "?"
            )

            log.info(
                "[%s] RECOVERY: reattaching live exit protection | "
                "pos=%s %s | order=%s status=%s",
                self.client_id,
                pos_id,
                underlying,
                local_id,
                exit_order.get("status"),
            )
            result["exits_reattached"] += 1

            if self.exit_engine:
                try:
                    if hasattr(self.exit_engine, "seed_from_db"):
                        # Idempotent global seed; safe to call again on startup.
                        self.exit_engine.seed_from_db()
                    elif hasattr(self.exit_engine, "mark_exit_in_flight"):
                        self.exit_engine.mark_exit_in_flight(
                            position_id=pos_id,
                            reason="recovery: exit order live at broker",
                        )
                except Exception as e:
                    log.error(
                        "[%s] RECOVERY: failed to reattach exit protection for pos %s: %s",
                        self.client_id, pos_id, e,
                    )
                    result.setdefault("errors", []).append(f"exits_reattach:{pos_id}:{e}")

    def _reattach_exit_protections(self, result: dict):
        """Compatibility wrapper for callers that still use the legacy name."""
        live_exit_orders = self._recover_exit_fills_that_occurred_during_downtime(result)
        self._reattach_live_exit_protections(result, live_exit_orders)

    # ──────────────────────────────────────────────────────────────────────────
    # 5. Buying power reservation
    # ──────────────────────────────────────────────────────────────────────────

    def _recompute_buying_power(self, result: dict):
        """
        Recompute how much capital is reserved by active positions.
        Updates master_control.account_equity if needed.
        """
        active_positions = self._load_active_positions()

        total_reserved = 0.0
        for pos in active_positions:
            total_reserved += _position_reserved_cost(pos)

        result["buying_power_reserved"] = total_reserved

        # Sync into master_control if it tracks this
        if hasattr(self.mc, "_reserved_capital"):
            self.mc._reserved_capital = total_reserved
        if hasattr(self.mc, "reserved_capital"):
            self.mc.reserved_capital = total_reserved

        log.info(
            "[%s] RECOVERY: buying power reserved=$%.2f across %d active positions",
            self.client_id, total_reserved, len(active_positions),
        )

    # ──────────────────────────────────────────────────────────────────────────
    # 5. Dedup / session state reseed
    # ──────────────────────────────────────────────────────────────────────────

    def _reseed_dedup(self, result: dict):
        """
        Repopulate master_control._seen_signals from recent trade_queue rows.
        Prevents re-processing signals that were already queued before restart.
        """
        from ap.db import run_with_retry, conn

        cutoff_hours = DEDUP_LOOKBACK_HOURS

        def _get_recent():
            with conn() as c:
                c.execute(
                    """
                    SELECT signal_id,
                           payload->>'ticker'            AS ticker,
                           payload->>'direction'         AS direction,
                           payload->>'side'              AS side,
                           payload->>'contract'          AS contract,
                           payload->>'selected_contract' AS selected_contract,
                           payload->>'contract_symbol'   AS contract_symbol,
                           payload->>'timeframe'         AS timeframe,
                           client_id, status
                    FROM trade_queue
                    WHERE client_id=%s
                      AND created_ts > NOW() - (%s * INTERVAL '1 hour')
                      AND status IN ('NEW','PROCESSING','DONE','COMPLETED','ERROR')
                    ORDER BY created_ts DESC
                    LIMIT 500
                    """,
                    (self.client_id, cutoff_hours),
                )
                return c.fetchall()

        try:
            rows = run_with_retry(_get_recent)
        except Exception as e:
            log.warning("[%s] RECOVERY: dedup reseed DB error: %s", self.client_id, e)
            return

        if not hasattr(self.mc, "_seen_signals"):
            log.debug("[%s] RECOVERY: master_control has no _seen_signals set — skipping",
                      self.client_id)
            return

        count = 0
        for row in (rows or []):
            signal_id = row.get("signal_id")
            ticker    = str(row.get("ticker") or "").upper()
            side, side_source = _resolve_side_from_order_or_meta(row)
            timeframe = str(row.get("timeframe") or "1d")

            # Re-add signal_id even when side is unresolved. Do NOT create a
            # ticker:CALL setup key from missing/dirty side metadata.
            _ts = time.time()
            if signal_id:
                self.mc._seen_signals[f"sig:{signal_id}:{self.client_id}"] = _ts
            if ticker and side:
                setup_key = f"{self.client_id}:{ticker}:{side}:{timeframe}"
                self.mc._seen_signals[setup_key] = _ts
            elif ticker:
                log.warning(
                    "[%s] RECOVERY: dedup setup key skipped for ticker=%s signal_id=%s side_source=%s",
                    self.client_id, ticker, signal_id, side_source,
                )
            count += 1

        result["dedup_seeded"] = count
        log.info(
            "[%s] RECOVERY: dedup reseeded with %d signals (lookback=%dh)",
            self.client_id, count, cutoff_hours,
        )

    # ──────────────────────────────────────────────────────────────────────────
    # 6. Watcher reseed — re-queue WATCHING signals so entry_watcher picks up
    #    after a restart. WATCHING in DB means "queued for entry watcher" but
    #    the watcher is in-memory; it loses state on restart.
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _coerce_order_meta(raw_meta) -> dict:
        if isinstance(raw_meta, dict):
            return dict(raw_meta)
        if isinstance(raw_meta, str) and raw_meta.strip():
            try:
                parsed = json.loads(raw_meta)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                return {}
        return {}

    @staticmethod
    def _find_pending_watcher_by_logical_identity(
        entry_watcher, *, local_order_id, client_id, signal_id, execution_mode,
    ):
        """Return the ``_pending`` entry matching this logical signal
        identity, or None. Read-only — callers needing a consistent
        snapshot must hold ``entry_watcher._lock`` around the call.

        Logical identity alone (local_order_id/client_id/signal_id/
        execution_mode) is NOT exact-registration identity: after an
        ownership transition, a different, legitimate watcher object can
        come to hold the same logical identity for the same row. Callers
        that need to distinguish "the registration I created" from "some
        registration matching my row" must additionally compare
        ``_exact_identity_of()`` against a value captured at registration
        time — see ``_capture_just_registered_watcher_id`` /
        ``_evict_just_registered_watcher``.
        """
        _pending = getattr(entry_watcher, "_pending", None)
        if _pending is None:
            return None
        for w in list(_pending):
            _sig = getattr(w, "signal", {}) or {}
            if (
                str(_sig.get("local_order_id") or "").strip() == local_order_id
                and str(_sig.get("client_id") or "").strip().lower()
                == client_id.lower()
                and str(_sig.get("signal_id") or "").strip() == signal_id
                and str(_sig.get("execution_mode") or "").strip().lower()
                == execution_mode.lower()
            ):
                return w
        return None

    @staticmethod
    def _exact_identity_of(w):
        """Return a stable, non-reusable identity for a single watcher
        registration instance.

        Prefers ``_registration_token`` — a UUID stamped once onto every
        real ``WatchedSignal`` at construction (see ap_entry_watcher.py).
        This is required, not merely convenient: CPython immediately
        reuses a garbage-collected object's memory address for the next
        allocation, so bare ``id()`` cannot safely tell "the exact
        registration a recovery actor created" apart from "an unrelated
        registration later allocated at the same freed address" once the
        original object has been dereferenced — which is exactly what
        happens to a losing recovery actor's own watcher between capture
        and rollback. Falls back to ``id()`` only for objects that predate
        this token (legacy test doubles); every production WatchedSignal
        carries the token, so production correctness never depends on the
        fallback.
        """
        token = getattr(w, "_registration_token", None)
        if token is not None:
            return ("token", token)
        return ("id", id(w))

    def _capture_just_registered_watcher_id(
        self, entry_watcher, *, local_order_id, client_id, signal_id, execution_mode,
    ):
        """Snapshot the exact-registration identity (see
        ``_exact_identity_of``) of the watcher this recovery actor just
        registered.

        Must be called immediately after a WATCHER_OWNED outcome, before
        any durable CAS adoption attempt. The returned value is the sole
        proof of "the exact registration this actor created" — later
        rollback (``_evict_just_registered_watcher``) is fenced to it so a
        stale loser can never evict or release dedup ownership belonging
        to a legitimate concurrent winner's replacement registration that
        happens to share the same logical identity.

        Returns None if no matching registration is found at capture time
        (rollback is then a no-op by construction — there is nothing this
        actor can prove it owns).
        """
        _lock = getattr(entry_watcher, "_lock", None)
        try:
            if _lock is not None:
                with _lock:
                    w = self._find_pending_watcher_by_logical_identity(
                        entry_watcher,
                        local_order_id=local_order_id,
                        client_id=client_id,
                        signal_id=signal_id,
                        execution_mode=execution_mode,
                    )
            else:
                w = self._find_pending_watcher_by_logical_identity(
                    entry_watcher,
                    local_order_id=local_order_id,
                    client_id=client_id,
                    signal_id=signal_id,
                    execution_mode=execution_mode,
                )
            return self._exact_identity_of(w) if w is not None else None
        except Exception as exc:
            log.error(
                "[%s] REARM_WATCHER_REQUIRED_CAPTURE_ID_ERROR local_order_id=%s "
                "exc=%s", self.client_id, local_order_id, exc,
            )
            return None

    def _evict_just_registered_watcher(
        self, entry_watcher, *, local_order_id, client_id, signal_id, execution_mode,
        expected_watcher_id,
    ) -> bool:
        """Remove the exact watched entry PTR just registered, when a
        subsequent durable ownership adoption fails.

        No public removal method exists on APEntryWatcher — production code
        there always removes a watched entry the same way (identity-based
        match against ``_pending`` under ``_lock``, then releases its
        dedup key). This reuses that exact idiom rather than inventing a
        new one.

        ``expected_watcher_id`` must be the value captured by
        ``_capture_just_registered_watcher_id`` at registration time — the
        exact-registration fence, per ``_exact_identity_of``. Logical
        identity (local_order_id / client_id / signal_id / execution_mode)
        alone is NOT sufficient: it can legitimately match a *different*
        watcher object after an ownership transition (this actor's
        original registration was evicted elsewhere and a concurrent
        winner registered its own watcher for the same row). This method
        may only remove — and only release dedup for — the single
        registration identity it was handed. A logically-matching object
        that fails the identity check is left completely untouched,
        including its dedup ownership.
        """
        try:
            _pending = getattr(entry_watcher, "_pending", None)
            if _pending is None:
                return True  # nothing this fake/real watcher tracks — vacuously clean
            if expected_watcher_id is None:
                # Nothing was ever proven to belong to this actor at
                # registration time — there is no exact registration this
                # rollback is entitled to touch.
                return True

            _lock = getattr(entry_watcher, "_lock", None)

            def _find_and_evict_if_exact():
                _target = self._find_pending_watcher_by_logical_identity(
                    entry_watcher,
                    local_order_id=local_order_id,
                    client_id=client_id,
                    signal_id=signal_id,
                    execution_mode=execution_mode,
                )
                if _target is None:
                    return "absent", None
                if self._exact_identity_of(_target) != expected_watcher_id:
                    # A legitimate concurrent winner now holds this
                    # logical identity. Never evict it, never release its
                    # dedup key — even though it matches on every logical
                    # field this actor knows about.
                    return "foreign", None
                _target_token = self._exact_identity_of(_target)
                entry_watcher._pending = [
                    p for p in entry_watcher._pending
                    if self._exact_identity_of(p) != _target_token
                ]
                return "evicted", _target

            if _lock is not None:
                with _lock:
                    _status, _removed = _find_and_evict_if_exact()
            else:
                _status, _removed = _find_and_evict_if_exact()

            if _status == "absent":
                return True  # already absent — nothing to evict
            if _status == "foreign":
                log.warning(
                    "[%s] REARM_WATCHER_REQUIRED_EVICTION_SKIPPED_FOREIGN_WINNER "
                    "local_order_id=%s — logical identity now belongs to a "
                    "different registration; dedup ownership left untouched",
                    self.client_id, local_order_id,
                )
                return True

            _release_fn = getattr(_removed, "_release_dedup_key", None)
            if callable(_release_fn):
                try:
                    _release_fn()
                except Exception as _release_exc:
                    log.warning(
                        "[%s] REARM_WATCHER_REQUIRED_EVICTION_RELEASE_DEDUP_FAILED "
                        "local_order_id=%s exc=%s",
                        self.client_id, local_order_id, _release_exc,
                    )
            else:
                _dedup = getattr(entry_watcher, "_dedup_set", None)
                if isinstance(_dedup, set) and signal_id in _dedup:
                    _dedup.discard(signal_id)
            return True
        except Exception as exc:
            log.error(
                "[%s] REARM_WATCHER_REQUIRED_EVICTION_ERROR local_order_id=%s "
                "exc=%s", self.client_id, local_order_id, exc,
            )
            return False

    def _build_recovery_plan_from_order(self, order: dict):
        meta = self._coerce_order_meta(order.get("meta"))

        contract = (
            order.get("contract")
            or meta.get("selected_contract")
            or meta.get("contract_symbol")
            or ""
        )
        direction, side_source = _resolve_side_from_order_or_meta(order, meta)
        if direction not in {"CALL", "PUT"}:
            log.warning(
                "[%s] RECOVERY: invalid_or_missing_side for local_order_id=%s signal_id=%s contract=%s side_source=%s",
                self.client_id,
                order.get("local_order_id"),
                order.get("signal_id"),
                contract,
                side_source,
            )
            return None

        ticker = str(
            order.get("symbol")
            or meta.get("symbol")
            or meta.get("ticker")
            or ""
        ).upper()
        trigger = (
            order.get("trigger_price")
            if order.get("trigger_price") is not None
            else meta.get("signal_entry_price")
        )
        stop = (
            order.get("stop_underlying")
            if order.get("stop_underlying") is not None
            else meta.get("stop_underlying")
        )
        target = (
            order.get("target_underlying")
            if order.get("target_underlying") is not None
            else meta.get("target_underlying")
        )

        metadata = dict(meta)
        metadata["recovery_side_source"] = side_source

        local_order_id = str(order.get("local_order_id") or "")
        signal_id = str(
            order.get("signal_id")
            or meta.get("signal_id")
            or local_order_id
            or ""
        )
        canonical_signal_id = str(
            order.get("canonical_signal_id")
            or meta.get("canonical_signal_id")
            or ""
        )
        materialization_generation = meta.get("materialization_generation")
        try:
            materialization_generation = (
                int(materialization_generation)
                if materialization_generation is not None
                else None
            )
        except (TypeError, ValueError):
            materialization_generation = None

        return _RecoveryPlan(
            signal_id=signal_id,
            canonical_signal_id=canonical_signal_id,
            plan_id=str(order.get("plan_id") or meta.get("plan_id") or ""),
            ticker=ticker,
            side=direction,
            direction=direction,
            entry_trigger=float(trigger or 0) if trigger is not None else None,
            score=float(order.get("score") or meta.get("score") or 65.0),
            tier=str(order.get("tier") or meta.get("tier") or "B"),
            trigger_price=float(trigger or 0) if trigger is not None else None,
            stop_underlying=float(stop or 0) if stop is not None else None,
            target_underlying=float(target or 0) if target is not None else None,
            contract_symbol=str(contract or ""),
            pattern=str(order.get("pattern") or meta.get("pattern") or ""),
            timeframe=str(order.get("timeframe") or meta.get("timeframe") or "1d"),
            prior_day_high=meta.get("prior_day_high"),
            prior_day_low=meta.get("prior_day_low"),
            strategy_type=str(meta.get("strategy_type") or ""),
            metadata=metadata,
            client_id=str(order.get("client_id") or self.client_id),
            execution_mode=str(
                order.get("execution_mode")
                or meta.get("execution_mode")
                or ""
            ).strip().lower(),
            local_order_id=local_order_id,
            materialization_generation=materialization_generation,
            trigger_crossed_at=(
                order.get("trigger_crossed_at")
                or meta.get("trigger_crossed_at")
            ),
            contracts=int(order.get("qty") or meta.get("selected_qty") or 0),
            quantity=int(order.get("qty") or meta.get("selected_qty") or 0),
            limit_price=float(
                order.get("limit_price") or meta.get("selected_limit") or 0
            ),
            max_position_usd=float(
                order.get("reserved_cost")
                or meta.get("selected_reserved_cost")
                or 0
            ),
        )

    def _recover_deferred_breach_lifecycles(self, result: dict) -> None:
        """Resume fenced deferred rows before normal startup entry processing.

        AMENDMENT 1 (execution_mode scoping + identity proof)
        -----------------------------------------------------
        Every load and every mutation in this pass is scoped to the exact
        runner mode. A paper runner MUST NOT observe or mutate LIVE rows,
        and a LIVE runner MUST NOT observe or mutate paper rows, even for
        the same client_id. The runner mode is resolved once at the top;
        the SQL query filters by `LOWER(TRIM(COALESCE(execution_mode,'')))`;
        each row is re-verified in Python (defence in depth); the plan
        built from the row is re-verified before any watcher rearm or
        submit path. The plan builder no longer infers a missing row mode
        from the runner (see `_build_recovery_plan_from_order`), so a row
        with a blank/malformed persisted mode fails identity closed here.
        """
        from ap.db import conn, run_with_retry

        # ── Runner mode resolve (once) ─────────────────────────────────
        recovery_mode = self._execution_mode()  # "PAPER" | "LIVE" | None
        if recovery_mode is None:
            log.error(
                "[%s] RECOVERY_BLOCKED unknown_execution_mode — deferred lifecycle recovery skipped",
                self.client_id,
            )
            result.setdefault("errors", []).append("recovery_unknown_execution_mode")
            return
        recovery_mode_sql = recovery_mode.lower()  # SQL predicate is case-insensitive lower

        # ── OSM identity proof (once) ──────────────────────────────────
        osm_client_id = str(getattr(self.osm, "client_id", "") or "").strip().lower()
        if osm_client_id and osm_client_id != self.client_id:
            log.critical(
                "[%s] RECOVERY_BLOCKED osm_client_id_mismatch osm=%r recovery=%r — "
                "deferred lifecycle recovery skipped",
                self.client_id, osm_client_id, self.client_id,
            )
            result.setdefault("errors", []).append("recovery_osm_client_id_mismatch")
            return

        def _load():
            with conn() as c:
                c.execute(
                    """
                    SELECT local_order_id, client_id, signal_id, plan_id,
                           symbol, contract, direction, kind, score, tier,
                           trigger_price, stop_underlying, target_underlying,
                           pattern, timeframe, execution_mode, qty, limit_price,
                           reserved_cost, status, broker_order_id, submitted_ts, meta
                           , created_ts
                    FROM orders
                    WHERE client_id = %s
                      AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s
                      AND kind = 'ENTRY'
                      AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                      AND (broker_order_id IS NULL OR broker_order_id = '')
                      AND submitted_ts IS NULL
                    ORDER BY created_ts ASC
                    """,
                    (self.client_id, recovery_mode_sql),
                )
                return c.fetchall()

        rows = run_with_retry(_load) or []
        now = datetime.now(timezone.utc)
        recovered = 0

        def _strict_durable_counter(
            meta_dict: dict,
            key: str,
            local_order_id: str,
            *,
            minimum: int = 0,
            missing_default: int | None = None,
        ):
            raw = (meta_dict or {}).get(key)
            if raw is None or raw == "":
                if missing_default is not None:
                    return missing_default
            try:
                value = int(raw)
            except (TypeError, ValueError):
                log.critical(
                    "[%s] RECOVERY_MALFORMED_DURABLE_COUNTER local_order_id=%s "
                    "field=%s raw=%r",
                    self.client_id, local_order_id, key, raw,
                )
                result.setdefault("errors", []).append(
                    f"recovery_malformed_counter:{local_order_id}:{key}"
                )
                return None
            if value < minimum:
                log.critical(
                    "[%s] RECOVERY_INVALID_DURABLE_COUNTER local_order_id=%s "
                    "field=%s value=%r minimum=%d",
                    self.client_id, local_order_id, key, value, minimum,
                )
                result.setdefault("errors", []).append(
                    f"recovery_invalid_counter:{local_order_id}:{key}"
                )
                return None
            return value

        # ── AMENDMENT §7: verified terminalization ─────────────────────
        # terminalize_deferred_breach returns True only when Postgres
        # confirmed rowcount > 0. A False return means the row was NOT
        # terminalized (CAS miss, not-found, or write error) and is still
        # live as PENDING_TRIGGER. Ignoring that return value lets a failed
        # terminalize masquerade as success, silently dropping the row.
        # Every terminalize in this pass now routes through this helper so
        # a failed durable write is surfaced (critical log + errors entry)
        # rather than swallowed.
        def _terminalize_verified(loid, *, reason_code, terminal_status, diagnostics):
            terminalize = getattr(self.osm, "terminalize_deferred_breach", None)
            if not callable(terminalize):
                log.critical(
                    "[%s] RECOVERY_TERMINALIZE_UNAVAILABLE local_order_id=%s "
                    "reason=%s — row NOT terminalized",
                    self.client_id, loid, reason_code,
                )
                result.setdefault("errors", []).append("recovery_terminalize_unavailable")
                return False
            try:
                ok = bool(terminalize(
                    loid,
                    reason_code=reason_code,
                    terminal_status=terminal_status,
                    diagnostics=diagnostics,
                ))
            except Exception as exc:
                log.critical(
                    "[%s] RECOVERY_TERMINALIZE_RAISED local_order_id=%s reason=%s exc=%s",
                    self.client_id, loid, reason_code, exc,
                )
                result.setdefault("errors", []).append("recovery_terminalize_raised")
                return False
            if not ok:
                log.critical(
                    "[%s] RECOVERY_TERMINALIZE_FAILED local_order_id=%s reason=%s "
                    "— durable write returned no rows; row still PENDING_TRIGGER",
                    self.client_id, loid, reason_code,
                )
                result.setdefault("errors", []).append("recovery_terminalize_failed")
            return ok

        def _terminalize_fenced_retry(loid, *, outcome: dict, extra_diagnostics: dict | None = None):
            """Fenced terminal CAS for the exact state carried by the outcome.

            All fencing identity fields MUST come from the TERMINAL_REQUIRED outcome
            dict. No fallback to self.client_id, recovery_mode, or zero values — any
            missing or malformed field returns INVALID_FENCED_OUTCOME and retains
            ownership. This prevents a stale runner with wrong identity from being
            silently substituted into the SQL predicate.

            Returns a classification dict:
              {"result": "TERMINALIZED"}                — fenced CAS succeeded
              {"result": "ALREADY_TERMINAL"}            — row already terminal
              {"result": "CLAIM_LOST"}                  — concurrent winner advanced
              {"result": "OWNERSHIP_ADVANCED"}          — submit intent / broker-ready
              {"result": "WRITE_FAILED"}                — true write failure, retain ownership
              {"result": "FENCED_TERMINAL_UNAVAILABLE"} — OSM method missing; retain
              {"result": "INVALID_FENCED_OUTCOME"}      — outcome missing required fields; retain
            """
            # ── Require all fencing identity fields from the outcome dict ────
            # Never substitute self.client_id, recovery_mode, or zero values.
            # An incomplete outcome means the consumer did not produce reliable
            # expected-state fields; calling the CAS with inferred defaults can
            # terminalize the wrong generation or a different client's row.
            _exp_client = str(outcome.get("expected_client_id") or "").strip().lower()
            _exp_mode = str(outcome.get("expected_execution_mode") or "").strip().lower()
            _exp_lc = str(outcome.get("expected_lifecycle_state") or "").strip().upper()
            _exp_ms = str(outcome.get("expected_materialization_status") or "").strip().upper()
            _exp_signal = str(outcome.get("expected_signal_id") or "").strip()
            _exp_reason = str(outcome.get("reason_code") or "").strip()
            _exp_status = str(outcome.get("terminal_status") or "").strip().upper()
            try:
                _exp_gen = int(outcome["expected_generation"])
            except (KeyError, TypeError, ValueError):
                _exp_gen = None
            try:
                _exp_prior = int(outcome["expected_prior_retry_attempt"])
            except (KeyError, TypeError, ValueError):
                _exp_prior = None
            try:
                _exp_retry = int(outcome["expected_retry_attempt"])
            except (KeyError, TypeError, ValueError):
                _exp_retry = None
            _exp_owner = str(outcome.get("expected_owner") or "").strip()
            _post_claim = (
                _exp_lc == "MATERIALIZING"
                and _exp_ms == "RUNNING"
            )

            _missing = []
            if not _exp_client:
                _missing.append("expected_client_id")
            if _exp_mode not in {"live", "paper"}:
                _missing.append("expected_execution_mode")
            if _post_claim:
                if not _exp_signal:
                    _missing.append("expected_signal_id")
                if not _exp_owner:
                    _missing.append("expected_owner")
                if _exp_retry is None or _exp_retry < 1:
                    _missing.append("expected_retry_attempt>=1")
            else:
                if _exp_lc != "RETRY_WAIT":
                    _missing.append("expected_lifecycle_state=RETRY_WAIT")
                if _exp_ms != "RETRY_PENDING":
                    _missing.append("expected_materialization_status=RETRY_PENDING")
                if _exp_prior is None or _exp_prior < 0:
                    _missing.append("expected_prior_retry_attempt>=0")
            if _exp_gen is None or _exp_gen < 1:
                _missing.append("expected_generation>=1")
            if not _exp_reason:
                _missing.append("reason_code")
            if _exp_status not in {"REJECTED", "EXPIRED", "CANCELED", "ERROR"}:
                _missing.append("terminal_status in {REJECTED,EXPIRED,CANCELED,ERROR}")

            if _missing:
                log.critical(
                    "[%s] RECOVERY_FENCED_TERM_INVALID_OUTCOME local_order_id=%s "
                    "missing_or_invalid=%s — row retained; outcome did not carry "
                    "required fencing fields",
                    self.client_id, loid, _missing,
                )
                result.setdefault("errors", []).append(
                    f"recovery_fenced_outcome_invalid:{','.join(_missing)}"
                )
                return {"result": "INVALID_FENCED_OUTCOME"}

            _fn_name = (
                "terminalize_materialization_retry"
                if _post_claim
                else "terminalize_deferred_retry_if_unchanged"
            )
            _fn = getattr(self.osm, _fn_name, None)
            if not callable(_fn):
                log.critical(
                    "[%s] RECOVERY_FENCED_TERM_UNAVAILABLE local_order_id=%s "
                    "— %s missing from OSM; row retained; deploy OSM update "
                    "to unblock",
                    self.client_id, loid, _fn_name,
                )
                result.setdefault("errors", []).append("recovery_fenced_terminal_unavailable")
                return {"result": "FENCED_TERMINAL_UNAVAILABLE"}

            _ok = False
            try:
                _diagnostics = {
                    **(extra_diagnostics or {}),
                    "recovery_classification": "fenced_retry_terminal",
                    "recovery_owner": outcome.get("owner"),
                }
                if _post_claim:
                    _ok = bool(_fn(
                        loid,
                        reason=_exp_reason,
                        terminal_status=_exp_status,
                        owner=_exp_owner,
                        generation=_exp_gen,
                        retry_attempt=_exp_retry,
                        client_id=_exp_client,
                        execution_mode=_exp_mode,
                        signal_id=_exp_signal,
                        diagnostics=_diagnostics,
                    ))
                else:
                    _ok = bool(_fn(
                        loid,
                        reason_code=_exp_reason,
                        terminal_status=_exp_status,
                        expected_client_id=_exp_client,
                        expected_execution_mode=_exp_mode,
                        expected_generation=_exp_gen,
                        expected_prior_retry_attempt=_exp_prior,
                        diagnostics=_diagnostics,
                    ))
            except Exception as exc:
                log.critical(
                    "[%s] RECOVERY_FENCED_TERM_RAISED local_order_id=%s exc=%s",
                    self.client_id, loid, exc,
                )
                return {"result": "WRITE_FAILED"}

            if _ok:
                log.info(
                    "[%s] RECOVERY_FENCED_TERM_OK local_order_id=%s reason=%s",
                    self.client_id, loid, outcome.get("reason_code"),
                )
                return {"result": "TERMINALIZED"}

            # CAS missed — reread to classify why
            _reread = None
            try:
                _reread = self.osm.get_order(loid)
            except Exception:
                pass
            if not isinstance(_reread, dict):
                return {"result": "WRITE_FAILED"}

            _rr_status = str(_reread.get("status") or "").upper()
            _rr_meta = _reread.get("meta") or {}
            if isinstance(_rr_meta, str):
                try:
                    import json as _j; _rr_meta = _j.loads(_rr_meta)
                except Exception:
                    _rr_meta = {}

            # A: Already terminal
            if _rr_status in {"REJECTED", "EXPIRED", "CANCELED", "ERROR"}:
                log.info(
                    "[%s] RECOVERY_FENCED_TERM_ALREADY_TERMINAL local_order_id=%s "
                    "status=%s — concurrent worker completed terminal write",
                    self.client_id, loid, _rr_status,
                )
                return {"result": "ALREADY_TERMINAL"}

            _rr_gen = _strict_durable_counter(
                _rr_meta or {}, "materialization_generation", loid,
                minimum=0, missing_default=0,
            )
            _rr_attempt = _strict_durable_counter(
                _rr_meta or {}, "retry_attempt", loid,
                minimum=0, missing_default=0,
            )
            if _rr_gen is None or _rr_attempt is None:
                return {"result": "WRITE_FAILED"}
            _exp_gen = int(outcome.get("expected_generation") or 0)
            _exp_attempt_prior = int(
                outcome.get(
                    "expected_retry_attempt"
                    if _post_claim
                    else "expected_prior_retry_attempt"
                )
                or 0
            )

            # B: Concurrent generation or attempt advance
            if _rr_gen > _exp_gen or _rr_attempt > _exp_attempt_prior:
                log.info(
                    "[%s] RECOVERY_FENCED_TERM_CLAIM_LOST local_order_id=%s "
                    "rr_gen=%d exp_gen=%d rr_attempt=%d exp_prior=%d",
                    self.client_id, loid, _rr_gen, _exp_gen, _rr_attempt, _exp_attempt_prior,
                )
                return {"result": "CLAIM_LOST"}

            _rr_lifecycle = str((_rr_meta or {}).get("lifecycle_state") or "").upper()
            _rr_inflight = str((_rr_meta or {}).get("materialization_in_flight") or "false").lower()
            _rr_nested = (
                (_rr_meta or {}).get("materialization")
                if isinstance((_rr_meta or {}).get("materialization"), dict)
                else {}
            )
            _rr_surfaces = ((_rr_meta or {}), _rr_nested)

            def _rr_absent(surface, key):
                raw = surface.get(key) if isinstance(surface, dict) else None
                return raw is None or (isinstance(raw, str) and not raw.strip())

            def _rr_false_or_absent(surface, key):
                raw = surface.get(key) if isinstance(surface, dict) else None
                return _rr_absent(surface, key) or raw is False or (
                    isinstance(raw, str) and raw.strip().lower() == "false"
                )

            def _rr_true_or_absent(surface, key):
                raw = surface.get(key) if isinstance(surface, dict) else None
                return _rr_absent(surface, key) or raw is True or (
                    isinstance(raw, str) and raw.strip().lower() == "true"
                )

            def _rr_optional_equal(surface, key, expected, *, lower=False):
                if _rr_absent(surface, key):
                    return True
                value = str(surface.get(key)).strip()
                return value.lower() == expected if lower else value == expected

            try:
                from ap_canonical_signal import build_canonical_signal_id

                _rr_canonical_signal = str(
                    build_canonical_signal_id(_exp_signal) or ""
                ).strip()
            except Exception:
                _rr_canonical_signal = ""

            _rr_identity_same = (
                bool(_exp_signal)
                and bool(_rr_canonical_signal)
                and str(_reread.get("signal_id") or "").strip() == _exp_signal
                and str(_reread.get("client_id") or "").strip().lower() == _exp_client
                and str(_reread.get("execution_mode") or "").strip().lower() == _exp_mode
                and _rr_optional_equal(
                    _reread, "canonical_signal_id", _rr_canonical_signal
                )
                and all(
                    _rr_optional_equal(surface, "client_id", _exp_client, lower=True)
                    and _rr_optional_equal(surface, "client_email", _exp_client, lower=True)
                    and _rr_optional_equal(surface, "execution_mode", _exp_mode, lower=True)
                    and _rr_optional_equal(surface, "signal_id", _exp_signal)
                    and _rr_optional_equal(surface, "canonical_signal_id", _rr_canonical_signal)
                    for surface in _rr_surfaces
                )
            )

            # A post-claim CAS miss with the exact same active fence is a
            # write/mismatch failure, not ownership advancement.  Retain the
            # recovery owner so a transient DB failure cannot strand a
            # MATERIALIZING/RUNNING row until its lease expires.
            if _post_claim:
                _same_post_claim_fence = (
                    _rr_gen == _exp_gen
                    and _rr_attempt == _exp_attempt_prior
                    and str((_rr_meta or {}).get("materialization_owner") or "").strip()
                    == str(outcome.get("expected_owner") or "").strip()
                    and str((_rr_meta or {}).get("current_owner") or "").strip()
                    == str(outcome.get("expected_owner") or "").strip()
                    and str((_rr_meta or {}).get("watcher_token") or "").strip()
                    == str(outcome.get("expected_owner") or "").strip()
                    and _rr_lifecycle == "MATERIALIZING"
                    and str((_rr_meta or {}).get("materialization_status") or "").upper()
                    == "RUNNING"
                    and _rr_inflight == "true"
                    and _rr_identity_same
                    and all(
                        _rr_optional_equal(surface, "lifecycle_state", "MATERIALIZING")
                        and _rr_optional_equal(surface, "materialization_status", "RUNNING")
                        and _rr_true_or_absent(surface, "materialization_in_flight")
                        and _rr_optional_equal(
                            surface,
                            "materialization_owner",
                            str(outcome.get("expected_owner") or "").strip(),
                        )
                        and _rr_optional_equal(
                            surface,
                            "current_owner",
                            str(outcome.get("expected_owner") or "").strip(),
                        )
                        and _rr_optional_equal(
                            surface,
                            "watcher_token",
                            str(outcome.get("expected_owner") or "").strip(),
                        )
                        and _rr_optional_equal(surface, "materialization_generation", str(_exp_gen))
                        and _rr_optional_equal(surface, "retry_attempt", str(_exp_attempt_prior))
                        for surface in _rr_surfaces
                    )
                    and all(
                        _rr_false_or_absent(surface, "broker_ready")
                        and _rr_false_or_absent(surface, "recovery_submit_fenced")
                        and all(
                            _rr_absent(surface, key)
                            for key in (
                                "submit_intent_at",
                                "broker_submit_key",
                                "broker_submit_payload_hash",
                                "recovery_submit_owner",
                                "recovery_submit_lease_until",
                            )
                        )
                        for surface in _rr_surfaces
                    )
                )
                if _same_post_claim_fence:
                    log.critical(
                        "[%s] RECOVERY_FENCED_TERM_POSTCLAIM_WRITE_FAILED "
                        "local_order_id=%s — exact MATERIALIZING/RUNNING owner "
                        "still present after CAS miss; retaining ownership",
                        self.client_id, loid,
                    )
                    result.setdefault("errors", []).append(
                        f"fenced_postclaim_write_failed:{loid}"
                    )
                    return {"result": "WRITE_FAILED"}

            # C: Submit intent, broker-ready, or active lifecycle
            _rr_lifecycle = str((_rr_meta or {}).get("lifecycle_state") or "").upper()
            _rr_broker_id = str(_reread.get("broker_order_id") or "").strip()
            _rr_broker_ready = str((_rr_meta or {}).get("broker_ready") or "false").lower()
            _rr_inflight = str((_rr_meta or {}).get("materialization_in_flight") or "false").lower()
            _rr_handoff_advanced = any(
                not _rr_false_or_absent(surface, "broker_ready")
                or not _rr_false_or_absent(surface, "recovery_submit_fenced")
                or any(
                    not _rr_absent(surface, key)
                    for key in (
                        "submit_intent_at",
                        "broker_submit_key",
                        "broker_submit_payload_hash",
                        "recovery_submit_owner",
                        "recovery_submit_lease_until",
                    )
                )
                for surface in _rr_surfaces
            )
            _rr_inflight_advanced = any(
                str(surface.get("materialization_in_flight") or "false").strip().lower()
                not in {"false", ""}
                for surface in _rr_surfaces
            )
            _rr_submit_intent = any(
                not _rr_absent(surface, "submit_intent_at")
                for surface in _rr_surfaces
            )
            if (
                _rr_submit_intent
                or _rr_broker_id
                or _reread.get("submitted_ts")
                or _rr_handoff_advanced
                or _rr_inflight_advanced
                or _rr_lifecycle in {
                    "MATERIALIZING", "BROKER_READY", "SUBMITTING",
                    "PRE_SUBMIT_PROOF_RETRY", "SUBMITTED", "ACKNOWLEDGED",
                }
            ):
                log.warning(
                    "[%s] RECOVERY_FENCED_TERM_OWNERSHIP_ADVANCED local_order_id=%s "
                    "lifecycle=%s submit_intent=%r broker_id=%r broker_ready=%r inflight=%r",
                    self.client_id, loid, _rr_lifecycle,
                    bool(_rr_submit_intent), bool(_rr_broker_id),
                    _rr_broker_ready, _rr_inflight,
                )
                return {"result": "OWNERSHIP_ADVANCED"}

            # D: Row still in old RETRY_WAIT state — true write failure
            log.critical(
                "[%s] RECOVERY_FENCED_TERM_WRITE_FAILED local_order_id=%s "
                "— row still RETRY_WAIT; retaining ownership for next pass",
                self.client_id, loid,
            )
            result.setdefault("errors", []).append(f"fenced_term_write_failed:{loid}")
            return {"result": "WRITE_FAILED"}

        # ── AMENDMENT §5: durable ownership on failed/impossible rearm ──
        # A resumable row must never be left ownerless. When a rearm cannot
        # happen (no entry_watcher wired) or the rearm returns False, we do
        # NOT silently drop the row. We record a durable recovery-ownership
        # marker via a non-destructive meta patch so the row is diagnosably
        # owned by the recovery scheduler and a future recovery pass will
        # resume it. Only recovery_* keys are written — contract, qty,
        # limit, selector evidence, tp/sl, direction and every trade-policy
        # field are preserved untouched.
        def _retain_recovery_ownership(loid, *, reason):
            # PR #421 final amendment (§5): prefer the fenced OSM write —
            # it refuses (returns False, no-op) if committed watcher
            # authority (current_owner/watcher_token/watcher_generation)
            # already exists on the row, closing the dual-authority gap a
            # bare update_order_meta merge cannot close. Fall back to the
            # unfenced merge only for OSM doubles that predate the fenced
            # method (test mocks) — real production OrderStateMachine
            # always provides it.
            _fenced_fn = getattr(
                self.osm, "retain_recovery_ownership_if_no_watcher", None
            )
            if callable(_fenced_fn):
                try:
                    ok = bool(_fenced_fn(
                        loid,
                        recovery_owner=f"recovery_scheduler:{self.client_id}",
                        reason=reason,
                        recovery_retention_mode=recovery_mode,
                    ))
                except Exception as exc:
                    log.critical(
                        "[%s] RECOVERY_RETENTION_RAISED local_order_id=%s "
                        "reason=%s exc=%s",
                        self.client_id, loid, reason, exc,
                    )
                    result.setdefault("errors", []).append(
                        "recovery_retention_raised"
                    )
                    return False
                if not ok:
                    log.critical(
                        "[%s] RECOVERY_RETENTION_WRITE_FAILED local_order_id=%s "
                        "reason=%s — durable ownership marker not persisted "
                        "(row missing, or watcher authority already committed "
                        "and the fenced write correctly refused to overwrite it)",
                        self.client_id, loid, reason,
                    )
                    result.setdefault("errors", []).append(
                        "recovery_retention_write_failed"
                    )
                return ok

            update_meta = getattr(self.osm, "update_order_meta", None)
            if not callable(update_meta):
                log.critical(
                    "[%s] RECOVERY_RETENTION_UNAVAILABLE local_order_id=%s reason=%s "
                    "— cannot record durable ownership",
                    self.client_id, loid, reason,
                )
                result.setdefault("errors", []).append("recovery_retention_unavailable")
                return False
            try:
                ok = bool(update_meta(loid, {
                    "recovery_ownership": "recovery_scheduler",
                    "recovery_owner": f"recovery_scheduler:{self.client_id}",
                    "recovery_retained_at": now.isoformat(),
                    "recovery_retention_reason": reason,
                    "recovery_retention_mode": recovery_mode,
                }))
            except Exception as exc:
                log.critical(
                    "[%s] RECOVERY_RETENTION_RAISED local_order_id=%s reason=%s exc=%s",
                    self.client_id, loid, reason, exc,
                )
                result.setdefault("errors", []).append("recovery_retention_raised")
                return False
            if not ok:
                log.critical(
                    "[%s] RECOVERY_RETENTION_WRITE_FAILED local_order_id=%s reason=%s "
                    "— durable ownership marker not persisted",
                    self.client_id, loid, reason,
                )
                result.setdefault("errors", []).append("recovery_retention_write_failed")
            return ok

        for raw in rows:
            order = dict(raw or {})
            local_order_id = str(order.get("local_order_id") or "").strip()
            if not local_order_id:
                continue

            # ── Per-row identity proof (defence in depth vs. SQL filter) ──
            # The SQL predicate above SHOULD prevent any cross-mode or
            # cross-client row from loading. These checks are belt-and-
            # suspenders against DB whitespace, mocked/tested paths, or
            # any future refactor that widens the SELECT. On mismatch we
            # SKIP (never terminalize) because a mode/client mismatch
            # likely means the row belongs to a DIFFERENT runner and
            # terminalizing would destroy someone else's live row.
            row_client_id = str(order.get("client_id") or "").strip().lower()
            if row_client_id and row_client_id != self.client_id:
                log.error(
                    "[%s] RECOVERY_SKIP client_id_mismatch local_order_id=%s row=%r",
                    self.client_id, local_order_id, row_client_id,
                )
                continue
            row_mode = _normalize_execution_mode(order.get("execution_mode"))
            if row_mode is None:
                # Blank/malformed persisted mode. Per amendment §1, this is
                # RECOVERY_INVALID_EXECUTION_MODE. SQL should already have
                # filtered it out; if we're here it's a defensive catch.
                # We QUARANTINE (skip + log) rather than terminalize, because
                # we cannot prove the row is ours without a valid mode field.
                log.error(
                    "[%s] RECOVERY_SKIP RECOVERY_INVALID_EXECUTION_MODE "
                    "local_order_id=%s raw_mode=%r",
                    self.client_id, local_order_id, order.get("execution_mode"),
                )
                continue
            if row_mode != recovery_mode:
                log.error(
                    "[%s] RECOVERY_SKIP execution_mode_mismatch local_order_id=%s "
                    "row_mode=%s recovery_mode=%s",
                    self.client_id, local_order_id, row_mode, recovery_mode,
                )
                continue

            meta = self._coerce_order_meta(order.get("meta"))

            # Conflicting retry authority is unresolved durable truth.  Do
            # not route it into terminalization, watcher takeover, selector,
            # or broker handling merely because trigger evidence is present.
            if has_conflicting_materialization_retry_authority(order):
                log.critical(
                    "[%s] RECOVERY_RETRY_AUTHORITY_CONFLICT local_order_id=%s "
                    "— row held untouched; no claim, selector, broker, cancel, "
                    "or lifecycle mutation",
                    self.client_id, local_order_id,
                )
                result.setdefault("errors", []).append(
                    f"retry_authority_conflict:{local_order_id}"
                )
                continue

            lifecycle = str(meta.get("lifecycle_state") or "").upper()
            materialization_status = str(meta.get("materialization_status") or "").upper()
            watcher_audit = meta.get("watcher_audit")
            watcher_reason = (
                str(watcher_audit.get("reason_code") or "").strip().lower()
                if isinstance(watcher_audit, dict)
                else ""
            )

            # Do this before stale/terminal classification, quote work, or any
            # recovery ownership mutation.  A confirmed timestamp with missing
            # or mismatched lifecycle provenance is not permission to erase the
            # evidence and continue as pre-breach.
            _evidence_row = dict(order)
            _evidence_row["meta"] = meta
            _canonical_retry_after_trigger = (
                watcher_reason == "trigger_ready"
                and has_canonical_materialization_retry_authority(_evidence_row)
            )
            if not recovery_trigger_evidence_identity_is_proven(
                _evidence_row, local_order_id
            ) and not _canonical_retry_after_trigger:
                log.critical(
                    "[%s] %s local_order_id=%s — preserving order unchanged",
                    self.client_id,
                    RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN,
                    local_order_id,
                )
                result.setdefault("errors", []).append(
                    f"{RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN}:{local_order_id}"
                )
                continue

            # Startup deferred-breach cleanup has its own age-based (72h) and
            # terminal-lifecycle terminalization branches immediately below
            # that PTR does not gate on this path.  Route active #524 owners
            # through the shared canonical predicate before those branches
            # can mutate a live materializer.  This guard is the ONE seam
            # retained here for that exact bypass — no partial-marker
            # protection is added anywhere else in this consumer.
            if is_active_materialization_in_flight(_evidence_row):
                result["materialization_in_flight_rows"] = int(
                    result.get("materialization_in_flight_rows", 0) or 0
                ) + 1
                log.info(
                    "PENDING_TRIGGER_MATERIALIZATION_IN_FLIGHT "
                    "local_order_id=%s client_id=%s execution_mode=%s signal_id=%s "
                    "materialization_owner=%s materialization_generation=%s "
                    "materialization_lease_until=%s broker_submission=NOT_ATTEMPTED "
                    "broker_cancel=NOT_ATTEMPTED caller=ap_recovery._recover_deferred_breach_lifecycles",
                    local_order_id,
                    order.get("client_id") or self.client_id,
                    order.get("execution_mode") or recovery_mode,
                    order.get("signal_id") or "",
                    meta.get("materialization_owner") or "",
                    meta.get("materialization_generation") or "",
                    meta.get("materialization_lease_until") or "",
                )
                continue

            created_raw = order.get("created_ts")
            try:
                created_at = (
                    created_raw
                    if isinstance(created_raw, datetime)
                    else datetime.fromisoformat(str(created_raw))
                )
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
                stale_pending = (now - created_at).total_seconds() > 72 * 3600
            except Exception:
                stale_pending = False
            if stale_pending:
                _terminalize_verified(
                    local_order_id,
                    reason_code="RECOVERY_STALE_PENDING_TRIGGER",
                    terminal_status="EXPIRED",
                    diagnostics={"recovery_classification": "stale_over_72h"},
                )
                continue

            if lifecycle in {"REJECTED", "EXPIRED", "CANCELED", "ERROR"}:
                _terminalize_verified(
                    local_order_id,
                    reason_code=str(meta.get("reason_code") or meta.get("final_reason") or "RECOVERY_TERMINAL_STATE"),
                    terminal_status=lifecycle,
                    diagnostics={"recovery_classification": "terminal_meta_pending_row"},
                )
                continue

            # Plan construction deferred: due-retry rows processed first (P0 final amendment)
            # The plan is built only inside should_resume for future-due rearm paths.

            # ── AMENDMENT §6: broker-ambiguity crash window ────────────────
            # If a durable submit intent was persisted (submit_intent_at) but
            # no broker_order_id landed on the row, the process may have
            # crashed AFTER the broker accepted the order but BEFORE the id
            # was committed. A live order may exist at the broker under the
            # durable tag. Such a row MUST NOT be resumed toward resubmission
            # (the §2 path) — that risks a double-submit on a live account.
            # Route it to the fail-closed reconciler, which never resubmits
            # and never terminalizes until the broker-query adoption gate is
            # wired. On RECONCILE_PENDING we retain durable ownership so the
            # row is never lost while it waits for reconciliation.
            if meta.get("submit_intent_at") and not str(order.get("broker_order_id") or "").strip():
                reconcile_fn = None
                if self.execution_core is not None:
                    reconcile_fn = getattr(
                        self.execution_core, "reconcile_deferred_broker_intent", None
                    )
                if not callable(reconcile_fn):
                    # No reconciler available — retain ownership WITHOUT
                    # resubmitting or terminalizing (a live order may exist).
                    log.critical(
                        "[%s] RECOVERY_CRASH_WINDOW reconciler_unavailable "
                        "local_order_id=%s — row retained, NOT resumed "
                        "(possible live broker order)",
                        self.client_id, local_order_id,
                    )
                    _retain_recovery_ownership(
                        local_order_id, reason="crash_window_reconciler_unavailable",
                    )
                    continue
                try:
                    rec = reconcile_fn(local_order_id=local_order_id) or {}
                except Exception as exc:
                    log.error(
                        "[%s] reconcile_deferred_broker_intent raised "
                        "local_order_id=%s exc=%s",
                        self.client_id, local_order_id, exc,
                    )
                    _retain_recovery_ownership(
                        local_order_id, reason="crash_window_reconcile_raised",
                    )
                    continue
                rec_disposition = str(rec.get("disposition") or "").strip().upper()
                if rec_disposition == "ALREADY_RECONCILED":
                    # A broker order id is already present — the order monitor
                    # owns it. Nothing to resume; leave the row untouched.
                    continue
                if rec_disposition == "RECONCILE_PENDING":
                    # Crash window unresolved. NEVER resubmit / terminalize.
                    _retain_recovery_ownership(
                        local_order_id,
                        reason=str(rec.get("reason_code") or "crash_window_reconcile_pending"),
                    )
                    continue
                if rec_disposition == "NOT_IN_CRASH_WINDOW":
                    # This block was entered with durable submit_intent_at.
                    # A contradictory classifier result can never authorize a
                    # replacement POST; retain the row fail-closed.
                    _retain_recovery_ownership(
                        local_order_id,
                        reason=str(
                            rec.get("reason_code")
                            or "crash_window_classifier_contradiction"
                        ),
                    )
                    continue
                else:
                    # KEEP_WATCHER / unknown → retain ownership, do not resume.
                    _retain_recovery_ownership(
                        local_order_id,
                        reason=str(rec.get("reason_code") or "crash_window_keep"),
                    )
                    continue

            if lifecycle in {"BROKER_READY", "SUBMITTING"} and meta.get("broker_ready") is True:
                # ── AMENDMENT §2: NEVER fall through to submit_existing_entry ──
                # The direct submit path bypasses kill switch, exposure
                # revalidation, fresh quote, spread, drift, entry confirmation
                # and account-size cap.  All BROKER_READY / SUBMITTING recovery
                # submits MUST route through the dedicated execution-core
                # scaffold.  While the scaffold's canonical gates are still
                # being wired in, it never calls the broker — it returns a
                # RETRY_WAIT disposition that keeps the row durably owned by
                # the recovery scheduler until either the gates land or a
                # truthful boundary is reached (trigger age, retry exhaustion,
                # identity mismatch, invalid status).
                resume_fn = None
                if self.execution_core is not None:
                    resume_fn = getattr(
                        self.execution_core, "resume_deferred_broker_ready_order", None
                    )
                if not callable(resume_fn):
                    # No dedicated recovery path available — retain the row
                    # WITHOUT terminalizing (engineering incomplete is not a
                    # truthful boundary) and WITHOUT calling the direct
                    # submit path (that's exactly what §2 forbids).  The
                    # next recovery pass will re-attempt.
                    log.critical(
                        "[%s] RECOVERY_BLOCKED "
                        "resume_deferred_broker_ready_order_unavailable "
                        "local_order_id=%s — row retained without action "
                        "(never falling back to submit_existing_entry per §2)",
                        self.client_id, local_order_id,
                    )
                    continue

                # Build and validate the plan before passing to the scaffold.
                # A None plan means the row has malformed or missing identity
                # metadata — the scaffold must not be called with None.
                _br_plan = self._build_recovery_plan_from_order(order)
                if _br_plan is None:
                    log.critical(
                        "[%s] BROKER_READY_RECOVERY_PLAN_INVALID local_order_id=%s "
                        "— plan construction failed; row retained without submit",
                        self.client_id, local_order_id,
                    )
                    _retain_recovery_ownership(
                        local_order_id, reason="broker_ready_recovery_plan_invalid",
                    )
                    result.setdefault("errors", []).append(
                        "broker_ready_recovery_plan_invalid"
                    )
                    continue
                _br_plan_client = str(getattr(_br_plan, "client_id", "") or "").strip().lower()
                _br_plan_mode = str(getattr(_br_plan, "execution_mode", "") or "").strip().upper()
                if _br_plan_client and _br_plan_client != self.client_id:
                    log.error(
                        "[%s] BROKER_READY_RECOVERY_PLAN_CLIENT_MISMATCH "
                        "local_order_id=%s plan_client=%r",
                        self.client_id, local_order_id, _br_plan_client,
                    )
                    _retain_recovery_ownership(
                        local_order_id, reason="broker_ready_recovery_plan_client_mismatch",
                    )
                    continue
                if _br_plan_mode != recovery_mode:
                    log.error(
                        "[%s] BROKER_READY_RECOVERY_PLAN_MODE_MISMATCH "
                        "local_order_id=%s plan_mode=%r recovery_mode=%s",
                        self.client_id, local_order_id, _br_plan_mode, recovery_mode,
                    )
                    _retain_recovery_ownership(
                        local_order_id, reason="broker_ready_recovery_plan_mode_mismatch",
                    )
                    continue

                try:
                    outcome = resume_fn(local_order_id=local_order_id, plan=_br_plan) or {}
                except Exception as exc:
                    log.error(
                        "[%s] resume_deferred_broker_ready_order raised "
                        "local_order_id=%s exc=%s",
                        self.client_id, local_order_id, exc,
                    )
                    continue

                disposition = str(outcome.get("disposition") or "").strip().upper()
                reason_code = str(outcome.get("reason_code") or "RECOVERY_UNKNOWN")

                if disposition == "TERMINAL_DURABLE":
                    # §7: verified — a failed terminalize is surfaced, not
                    # swallowed. On failure the row remains PENDING_TRIGGER
                    # and is retained with durable ownership (§5) so it is
                    # never left ownerless by a silent terminalize miss.
                    _term_ok = _terminalize_verified(
                        local_order_id,
                        reason_code=reason_code,
                        terminal_status=str(outcome.get("terminal_status") or "EXPIRED"),
                        diagnostics={
                            "recovery_classification": "broker_ready_boundary",
                            "recovery_attempt": outcome.get("attempt"),
                            "recovery_max_attempts": outcome.get("max_attempts"),
                            "recovery_owner": outcome.get("owner"),
                        },
                    )
                    if not _term_ok:
                        _retain_recovery_ownership(
                            local_order_id, reason="broker_ready_terminalize_failed",
                        )
                elif disposition == "RETRY_WAIT":
                    # Persist attempt tracking WITHOUT clearing broker_ready
                    # or any selector/contract/quantity/limit field.  The row
                    # remains resumable — the durable BROKER_READY state is
                    # preserved.  See ap_execution_core.resume_deferred_broker_ready_order
                    # for the full field-preservation rationale.
                    # §7: verify the durable write; §5: on failure the row is
                    # still owned (broker_ready meta intact) but we record the
                    # write failure so a silently-lost attempt is diagnosable.
                    update_meta = getattr(self.osm, "update_order_meta", None)
                    _meta_ok = False
                    if callable(update_meta):
                        try:
                            _meta_ok = bool(update_meta(local_order_id, {
                                "recovery_last_attempt_at": now.isoformat(),
                                "recovery_attempt_count": outcome.get("attempt"),
                                "recovery_next_retry_at": outcome.get("next_retry_at"),
                                "recovery_reason_code": reason_code,
                                "recovery_owner": outcome.get("owner"),
                                "recovery_generation": outcome.get("generation"),
                                "recovery_max_attempts": outcome.get("max_attempts"),
                            }))
                        except Exception as exc:
                            log.critical(
                                "[%s] RECOVERY_RETRY_META_RAISED local_order_id=%s exc=%s",
                                self.client_id, local_order_id, exc,
                            )
                            result.setdefault("errors", []).append("recovery_retry_meta_raised")
                    if not _meta_ok:
                        log.critical(
                            "[%s] RECOVERY_RETRY_META_WRITE_FAILED local_order_id=%s "
                            "— retry-tracking marker not persisted; row still "
                            "durably BROKER_READY and owned",
                            self.client_id, local_order_id,
                        )
                        result.setdefault("errors", []).append("recovery_retry_meta_write_failed")
                # KEEP_WATCHER (e.g. RECOVERY_OSM_UNAVAILABLE, row read
                # error, already-submitted) → row untouched.
                continue

            # PRE_SUBMIT_PROOF_RETRY has an explicit scheduler consumer. Both
            # startup recovery and the runtime 20-second pass enter this branch;
            # no in-memory watcher is required for completion.
            if lifecycle == "PRE_SUBMIT_PROOF_RETRY":
                outcome = resume_pre_submit_proof_retry(order, self.osm, self.execution_core)
                disposition = str(outcome.get("disposition") or "").upper()
                reason_code = str(outcome.get("reason_code") or "PROOF_RETRY_UNKNOWN")
                if disposition == "SUBMITTED":
                    recovered += 1
                elif disposition == "TERMINAL_DURABLE":
                    _terminalize_verified(
                        local_order_id,
                        reason_code=reason_code,
                        terminal_status=str(outcome.get("terminal_status") or "EXPIRED"),
                        diagnostics={
                            "recovery_classification": "pre_submit_proof_retry_terminal",
                            "proof_retry_attempt": outcome.get("proof_retry_attempt"),
                            "selected_contract": str(order.get("contract") or ""),
                        },
                    )
                elif disposition not in {"NOT_DUE", "CLAIM_LOST", "RETRY_WAIT", "RECONCILE_PENDING"}:
                    result.setdefault("errors", []).append(
                        f"proof_retry_unknown_disposition:{disposition or 'blank'}"
                    )
                continue

            should_resume = False
            materialization_resume = False
            if lifecycle == "RETRY_WAIT" or materialization_status == "RETRY_PENDING":
                # Rearm immediately; the watcher consumes next_retry_at and
                # remains dormant until due.  Startup recovery is not the only
                # scheduler tick, so a future-due row still has a real owner.
                should_resume = True
                materialization_resume = True
            elif lifecycle == "MATERIALIZING" or materialization_status == "RUNNING":
                lease_raw = meta.get("materialization_lease_until")
                try:
                    lease = datetime.fromisoformat(str(lease_raw))
                    if lease.tzinfo is None:
                        lease = lease.replace(tzinfo=timezone.utc)
                    should_resume = lease <= now
                except Exception:
                    should_resume = True
                materialization_resume = should_resume
            elif lifecycle == "" and materialization_status == "QUEUED":
                should_resume = True
                materialization_resume = True
            elif lifecycle == "" and materialization_status in {"", "WAITING_FOR_TRIGGER"}:
                # Ordinary orphan: the watch() recovery classifier either
                # rearms it, or terminalizes if the live quote proves the move
                # already happened/staleness makes rearm unsafe.
                should_resume = True

            if should_resume:
                # ── P0 AMENDMENT (fix/deferred-retry-due-execution-p0) ───────
                #
                # BLOCKER §3: Apply executable ownership proof to ALL RETRY_WAIT
                # / RETRY_PENDING rows — not just due ones. For future-due rows,
                # the proof verifies that deferred_retry_not_before exists and is
                # plausibly aligned with the durable schedule. A malformed durable
                # timestamp gets a quarantine outcome, never a fall-through to
                # has_order().
                #
                # BLOCKER §4: Due-retry execution does not require a live
                # entry_watcher. An already-due durable retry routes to
                # resume_deferred_materialization_retry through execution_core
                # regardless of watcher availability. The watcher is only needed
                # for future-due rearm scheduling.

                _is_retry_row = (
                    lifecycle == "RETRY_WAIT" or materialization_status == "RETRY_PENDING"
                )

                # ── Parse + validate the durable retry schedule ───────────────
                _durable_next_retry_at = None
                _due_at = None
                _ts_parse_error = False
                if _is_retry_row:
                    _due_at, _schedule_error = resolve_materialization_retry_schedule(meta)
                    if _schedule_error:
                        _ts_parse_error = _schedule_error != "retry_schedule_missing"
                        _due_at = None
                    else:
                        _durable_next_retry_at = _due_at.isoformat() if _due_at else None

                if _is_retry_row and _ts_parse_error:
                    # Blocker §3: malformed durable timestamp — quarantine.
                    log.critical(
                        "[%s] RECOVERY_RETRY_TS_MALFORMED local_order_id=%s "
                        "raw=%r — quarantining via retention",
                        self.client_id, local_order_id, _durable_next_retry_at,
                    )
                    _retain_recovery_ownership(
                        local_order_id, reason="retry_ts_malformed",
                    )
                    continue

                # P0 blocker §2 (new): RETRY_WAIT with no durable retry
                # timestamp is an inconsistent lifecycle state. There is no
                # safe interpretation of:
                #   lifecycle_state = RETRY_WAIT / materialization_status = RETRY_PENDING
                #   next_retry_at   = null
                # It must be quarantined via retention — never trusted through
                # has_order(). A future health pass can repair or terminalize it.
                if _is_retry_row and _durable_next_retry_at is None:
                    log.critical(
                        "[%s] RECOVERY_RETRY_NO_DURABLE_SCHEDULE local_order_id=%s "
                        "lifecycle=%s mstatus=%s — quarantining; no schedule to trust",
                        self.client_id, local_order_id, lifecycle, materialization_status,
                    )
                    _retain_recovery_ownership(
                        local_order_id, reason="retry_wait_no_durable_schedule",
                    )
                    continue

                _retry_generation = None
                _retry_attempt = None
                _retry_max_attempts = None
                if _is_retry_row:
                    _retry_generation = _strict_durable_counter(
                        meta, "materialization_generation", local_order_id,
                        minimum=1, missing_default=1,
                    )
                    _retry_attempt = _strict_durable_counter(
                        meta, "retry_attempt", local_order_id,
                        minimum=0, missing_default=0,
                    )
                    _retry_max_attempts = _strict_durable_counter(
                        meta, "retry_max_attempts", local_order_id,
                        minimum=0, missing_default=0,
                    )
                    if (
                        _retry_generation is None
                        or _retry_attempt is None
                        or _retry_max_attempts is None
                    ):
                        _retain_recovery_ownership(
                            local_order_id, reason="retry_wait_malformed_counter",
                        )
                        continue

                _is_due = _due_at is not None and _due_at <= now

                # ── Due-retry path (blocker §4: watcher not required) ─────────
                if _is_retry_row and _is_due:
                    # Prove executable ownership regardless of watcher state.
                    _proof_result = {"proven": False, "reason_code": "PROOF_UNAVAILABLE"}
                    if self.entry_watcher is not None:
                        _prove = getattr(
                            self.entry_watcher, "prove_materialization_retry_owner", None,
                        )
                        if callable(_prove):
                            try:
                                _proof_result = _prove(
                                    local_order_id,
                                    expected_client_id=self.client_id,
                                    expected_execution_mode=recovery_mode,
                                    expected_watcher_token=str(meta.get("watcher_token") or "").strip() or None,
                                    expected_generation=_retry_generation,
                                    durable_next_retry_at=str(_durable_next_retry_at),
                                    durable_retry_deadline=(
                                        str(meta.get("absolute_entry_deadline"))
                                        if meta.get("absolute_entry_deadline") else None
                                    ),
                                ) or {"proven": False, "reason_code": "PROOF_RETURNED_NONE"}
                            except Exception as _pexc:
                                _proof_result = {
                                    "proven": False,
                                    "reason_code": f"PROOF_EXCEPTION:{type(_pexc).__name__}",
                                }
                    _proven = bool(_proof_result.get("proven"))
                    _proof_reason = str(_proof_result.get("reason_code") or "PROOF_UNKNOWN")
                    try:
                        _grace_seconds = int(os.getenv("DEFERRED_RETRY_OWNER_GRACE_SECONDS", "5"))
                    except (TypeError, ValueError):
                        _grace_seconds = 5
                    _grace_expired = (now - _due_at).total_seconds() > _grace_seconds
                    if _proven and not _grace_expired:
                        continue  # watcher will fire within grace

                    # Blocker §4: attempt takeover through execution_core
                    # whether or not a watcher is available.
                    _resume_fn = None
                    if self.execution_core is not None:
                        _resume_fn = getattr(
                            self.execution_core, "resume_deferred_materialization_retry", None,
                        )
                    if not callable(_resume_fn):
                        log.warning(
                            "[%s] RECOVERY_DUE_RETRY_TAKEOVER_UNAVAILABLE "
                            "local_order_id=%s proof_reason=%s — falling through "
                            "to legacy rearm",
                            self.client_id, local_order_id, _proof_reason,
                        )
                        # Fall through to the future-due / legacy rearm block.
                    else:
                        _expected_generation = int(_retry_generation)
                        _expected_attempt = int(_retry_attempt) + 1
                        _takeover_owner = (
                            f"recovery_retry:{self.client_id}:{local_order_id}:"
                            f"{_expected_generation + 1}"
                        )
                        try:
                            _outcome = _resume_fn(
                                local_order_id=local_order_id,
                                expected_generation=_expected_generation,
                                expected_retry_attempt=_expected_attempt,
                                owner=_takeover_owner,
                            ) or {}
                        except Exception as _resume_exc:
                            log.error(
                                "[%s] resume_deferred_materialization_retry raised "
                                "local_order_id=%s exc=%s",
                                self.client_id, local_order_id, _resume_exc,
                            )
                            _retain_recovery_ownership(
                                local_order_id,
                                reason=f"due_retry_resume_raised:{type(_resume_exc).__name__}",
                            )
                            continue
                        _disp = str(_outcome.get("disposition") or "").strip().upper()
                        _reason = str(_outcome.get("reason_code") or "RETRY_UNKNOWN")
                        log.info(
                            "[%s] RECOVERY_DUE_RETRY_TAKEOVER local_order_id=%s "
                            "proof_reason=%s grace_expired=%s disposition=%s "
                            "reason=%s attempt=%s generation=%s",
                            self.client_id, local_order_id, _proof_reason,
                            _grace_expired, _disp, _reason,
                            _outcome.get("attempt"), _outcome.get("generation"),
                        )
                        if _disp in {"SUBMITTED", "BROKER_READY"}:
                            recovered += 1
                        elif _disp == "TERMINAL_REQUIRED":
                            # P0 FINAL AMENDMENT: fenced terminal CAS using the
                            # exact expected state carried in the outcome dict.
                            # Never uses the broad terminalize_deferred_breach.
                            _fenced = _terminalize_fenced_retry(
                                local_order_id,
                                outcome=_outcome,
                                extra_diagnostics={
                                    "recovery_classification": "due_retry_terminal_required",
                                    "recovery_attempt": _outcome.get("attempt"),
                                    "recovery_max_attempts": _outcome.get("max_attempts"),
                                    "recovery_owner": _outcome.get("owner"),
                                    "recovery_generation": _outcome.get("generation"),
                                },
                            )
                            _fenced_result = _fenced.get("result")
                            if _fenced_result == "WRITE_FAILED":
                                _retain_recovery_ownership(
                                    local_order_id, reason=f"fenced_term_write_failed:{_reason}",
                                )
                            elif _fenced_result in {"FENCED_TERMINAL_UNAVAILABLE", "INVALID_FENCED_OUTCOME"}:
                                _retain_recovery_ownership(
                                    local_order_id, reason=_fenced_result.lower(),
                                )
                            # ALREADY_TERMINAL / CLAIM_LOST / OWNERSHIP_ADVANCED:
                            # row is owned by another worker — no retention, no error
                        elif _disp in {"TERMINAL_DURABLE", "TERMINAL_ALREADY_DURABLE"}:
                            # TERMINAL_ALREADY_DURABLE: canonical downstream already wrote
                            # the terminal state; do NOT issue another terminal write.
                            # TERMINAL_DURABLE: backward-compat path — also no write needed
                            # (the consumer verified the row is already terminal).
                            log.info(
                                "[%s] RECOVERY_DUE_RETRY_ALREADY_TERMINAL local_order_id=%s "
                                "disp=%s reason=%s terminal_status=%s",
                                self.client_id, local_order_id, _disp, _reason,
                                _outcome.get("terminal_status"),
                            )
                            # Verify terminal status in durable row (belt-and-suspenders)
                            try:
                                _at_row = self.osm.get_order(local_order_id)
                                _at_status = str((_at_row or {}).get("status") or "").upper()
                                if _at_status not in {"REJECTED", "EXPIRED", "CANCELED", "ERROR",
                                                      "SUBMITTED", "ACKNOWLEDGED", "FILLED"}:
                                    log.warning(
                                        "[%s] RECOVERY_ALREADY_TERMINAL_VERIFY_MISMATCH "
                                        "local_order_id=%s expected_terminal actual_status=%s",
                                        self.client_id, local_order_id, _at_status,
                                    )
                            except Exception:
                                pass
                        elif _disp == "RETRY_SCHEDULE_FAILED":
                            # P0 blocker §3: schedule write failed — row may be
                            # stranded at MATERIALIZING. Retain durable ownership
                            # so the next health-loop pass does not lose the row.
                            log.critical(
                                "[%s] RECOVERY_DUE_RETRY_SCHEDULE_FAILED "
                                "local_order_id=%s reason=%s — retaining ownership",
                                self.client_id, local_order_id, _reason,
                            )
                            _retain_recovery_ownership(
                                local_order_id, reason=f"due_retry_schedule_failed:{_reason}",
                            )
                            result.setdefault("errors", []).append(
                                f"retry_schedule_failed:{local_order_id}"
                            )
                        elif _disp == "REARM_WATCHER_REQUIRED":
                            # Narrow adapter: verify the exact-generation
                            # handoff, then hand off entirely to the
                            # canonical PendingTriggerRestartRecovery engine.
                            # No classification, watcher-admission, or
                            # retry-persistence logic is duplicated here.
                            _exp_client = str(
                                _outcome.get("expected_client_id") or ""
                            ).strip().lower()
                            _exp_mode = str(
                                _outcome.get("expected_execution_mode") or ""
                            ).strip().lower()
                            _exp_signal = str(
                                _outcome.get("expected_signal_id") or ""
                            ).strip()
                            _exp_canonical = str(
                                _outcome.get("expected_canonical_signal_id") or ""
                            ).strip()
                            _exp_loid = str(
                                _outcome.get("local_order_id") or local_order_id
                            ).strip()
                            def _strict_generation(raw):
                                # Reused for both expected_generation (from
                                # the in-memory callback outcome) and durable
                                # materialization_generation (from JSONB,
                                # which psycopg2 deserializes into the same
                                # Python int/float/bool/str/None shapes) —
                                # one narrow parser, not a generalized
                                # framework. Rejects missing, Boolean, float,
                                # negative, blank, and any string (JSONB
                                # numbers never deserialize as strings, so a
                                # string here is always malformed/decimal/
                                # scientific-notation input, not a valid
                                # generation).
                                return (
                                    raw
                                    if isinstance(raw, int)
                                    and not isinstance(raw, bool)
                                    and raw >= 1
                                    else None
                                )

                            _exp_gen_raw = _outcome.get("expected_generation")
                            _exp_gen = _strict_generation(_exp_gen_raw)

                            def _rwr_retain_exact(reason):
                                # PR #421 final correction: RWR validates a
                                # much larger exact surface than the
                                # general-purpose _retain_recovery_ownership
                                # closure fences (identity, generation,
                                # crash-window state) — a stale actor whose
                                # own snapshot has since diverged from the
                                # durable row must not be able to write
                                # recovery authority just because watcher
                                # fields happen to still read blank. This
                                # re-asserts every fact this pass validated,
                                # atomically, at write time.
                                _retain_fn = getattr(
                                    self.osm,
                                    "retain_rearm_watcher_required_recovery_ownership",
                                    None,
                                )
                                if not callable(_retain_fn):
                                    return _retain_recovery_ownership(
                                        local_order_id,
                                        reason=f"rearm_watcher_required_{reason}",
                                    )
                                _existing_recovery_owner = str(
                                    _rwr_meta.get("recovery_owner") or ""
                                )
                                try:
                                    ok = bool(_retain_fn(
                                        local_order_id,
                                        recovery_owner=_existing_recovery_owner,
                                        reason=f"rearm_watcher_required_{reason}",
                                        recovery_retention_mode=recovery_mode,
                                        client_id=_exp_client,
                                        signal_id=_exp_signal,
                                        execution_mode=_exp_mode,
                                        generation=_exp_gen,
                                        expected_recovery_owner=_existing_recovery_owner,
                                        canonical_signal_id=_exp_canonical or "",
                                    ))
                                except Exception as exc:
                                    log.critical(
                                        "[%s] REARM_WATCHER_REQUIRED_RETENTION_RAISED "
                                        "local_order_id=%s reason=%s exc=%s",
                                        self.client_id, local_order_id, reason, exc,
                                    )
                                    return False
                                if not ok:
                                    log.critical(
                                        "[%s] REARM_WATCHER_REQUIRED_RETENTION_CAS_MISS "
                                        "local_order_id=%s reason=%s — durable state "
                                        "advanced since this actor validated authority; "
                                        "retention correctly refused",
                                        self.client_id, local_order_id, reason,
                                    )
                                return ok

                            def _rwr_fail(reason, *, retain=True):
                                log.critical(
                                    "[%s] REARM_WATCHER_REQUIRED_%s local_order_id=%s "
                                    "— %s",
                                    self.client_id, reason.upper(), local_order_id,
                                    (
                                        "retaining ownership, no watcher registered"
                                        if retain else
                                        "authority already lost — no ownership write"
                                    ),
                                )
                                if retain:
                                    _rwr_retain_exact(reason)
                                result.setdefault("errors", []).append(
                                    f"rearm_watcher_required_{reason}:{local_order_id}"
                                )

                            if (
                                _exp_gen is None or not _exp_client
                                or _exp_mode not in {"live", "paper"}
                                or not _exp_signal or not _exp_loid
                            ):
                                # Cannot even form a valid expectation to
                                # fence a retention write against — no
                                # exact state exists to reassert.
                                _rwr_fail("expected_fields_invalid", retain=False)
                                continue

                            try:
                                _rwr_row = self.osm.get_order(_exp_loid)
                                if not _rwr_row:
                                    # Nothing to retain ownership of.
                                    _rwr_fail("row_missing", retain=False)
                                    continue
                                _rwr_meta = self._coerce_order_meta(
                                    _rwr_row.get("meta")
                                )

                                # Identity (constraint: local_order_id,
                                # client_id, signal_id, canonical_signal_id
                                # when expected, execution_mode).
                                _rwr_canonical = str(
                                    _rwr_row.get("canonical_signal_id")
                                    or _rwr_meta.get("canonical_signal_id")
                                    or ""
                                ).strip()
                                _identity_ok = (
                                    str(_rwr_row.get("local_order_id") or "").strip()
                                    == _exp_loid
                                    and str(_rwr_row.get("client_id") or "")
                                    .strip().lower() == _exp_client
                                    and str(_rwr_row.get("signal_id") or "").strip()
                                    == _exp_signal
                                    and str(_rwr_row.get("execution_mode") or "")
                                    .strip().lower() == _exp_mode
                                    and (
                                        not _exp_canonical
                                        or _rwr_canonical == _exp_canonical
                                    )
                                )
                                if not _identity_ok:
                                    # Authority already proven lost — this
                                    # actor's expected identity no longer
                                    # matches the durable row.
                                    _rwr_fail("identity_mismatch", retain=False)
                                    continue

                                # Exact generation — never >=. Same strict
                                # parser as expected_generation: a Boolean,
                                # float, negative, or otherwise malformed
                                # durable value fails closed rather than
                                # silently coercing (Python's bare int(x)
                                # would accept int(True)==1, int(1.9)==1).
                                _durable_gen = _strict_generation(
                                    _rwr_meta.get("materialization_generation")
                                )
                                if _durable_gen is None:
                                    # Cannot prove ownership against an
                                    # unusable durable generation.
                                    _rwr_fail(
                                        "durable_generation_malformed", retain=False,
                                    )
                                    continue
                                if _durable_gen != _exp_gen:
                                    # A newer pass already advanced this
                                    # row's generation — this actor's
                                    # authority is over, not merely
                                    # unconfirmed.
                                    _rwr_fail(
                                        "generation_advanced_concurrently",
                                        retain=False,
                                    )
                                    continue

                                # Authoritative broker-absence columns +
                                # existing crash-window rules (submit_intent_at
                                # present with no landed broker_order_id, or
                                # broker_ready durably true) — the same
                                # authoritative idiom already used elsewhere
                                # in this file for this exact lifecycle.
                                _rwr_broker_ready = str(
                                    _rwr_meta.get("broker_ready") or "false"
                                ).lower()
                                _authoritative_ok = (
                                    str(_rwr_row.get("kind") or "").strip().upper()
                                    == "ENTRY"
                                    and str(_rwr_row.get("status") or "")
                                    .strip().upper() == "PENDING_TRIGGER"
                                    and not str(
                                        _rwr_row.get("broker_order_id") or ""
                                    ).strip()
                                    and not _rwr_row.get("submitted_ts")
                                    and not _rwr_meta.get("submit_intent_at")
                                    and _rwr_broker_ready in {"false", ""}
                                )
                                if not _authoritative_ok:
                                    # The row advanced past the crash window
                                    # (broker/submission activity) since this
                                    # actor's expectation was formed —
                                    # authority is no longer this actor's to
                                    # retain.
                                    _rwr_fail(
                                        "state_invalid_or_crash_window", retain=False,
                                    )
                                    continue

                                # Sole authority for classification, watcher
                                # admission, and bounded restart-rearm retry.
                                from ap.pending_trigger_restart_recovery import (
                                    PendingTriggerRestartRecovery as _PTR_RWR,
                                    _RowOutcome as _RWR_RowOutcome,
                                )
                                _rwr_ptr = _PTR_RWR(
                                    client_id=self.client_id,
                                    execution_mode=self._execution_mode() or "",
                                    osm=self.osm,
                                    entry_watcher=self.entry_watcher,
                                    broker=self.broker,
                                    caller_source=(
                                        "ap_recovery.due_retry.rearm_watcher_required"
                                    ),
                                )
                                _rwr_outcome = _rwr_ptr.recover_one_row(
                                    dict(_rwr_row),
                                    plan_builder_fn=self._build_recovery_plan_from_order,
                                )
                                log.info(
                                    "[%s] REARM_WATCHER_REQUIRED_PTR_OUTCOME "
                                    "local_order_id=%s outcome=%s",
                                    self.client_id, local_order_id, _rwr_outcome,
                                )

                                if _rwr_outcome == _RWR_RowOutcome.WATCHER_OWNED:
                                    # PTR proved a real watcher registration
                                    # in-process, but writes no durable
                                    # ownership itself. Perform the narrow,
                                    # CAS-fenced adoption so durable state
                                    # truthfully matches: only THEN count the
                                    # row as recovered.
                                    #
                                    # PR #421 final amendment (P0 race 1):
                                    # WATCHER_OWNED does not prove THIS
                                    # invocation created the watcher — PTR's
                                    # own read-only fast path can return it
                                    # for a watcher that already existed
                                    # before this call. Provenance must come
                                    # directly from PTR's own bookkeeping,
                                    # set at the exact moment of registration
                                    # inside PTR — never rediscovered here by
                                    # scanning the registry, which cannot
                                    # distinguish "I created this" from "I
                                    # merely observed this."
                                    _registered_by_this_attempt = bool(
                                        getattr(
                                            _rwr_ptr,
                                            "last_watcher_registered_by_this_attempt",
                                            False,
                                        )
                                    )
                                    _raw_registration_token = (
                                        getattr(
                                            _rwr_ptr, "last_registration_token", None,
                                        )
                                        if _registered_by_this_attempt
                                        else None
                                    )
                                    # _evict_just_registered_watcher compares
                                    # via _exact_identity_of(), which returns
                                    # ("token", <value>) for any object
                                    # carrying a _registration_token — the
                                    # same shape every real WatchedSignal
                                    # produces. PTR reports the raw token
                                    # string; wrap it into that identical
                                    # shape here so the comparison is a
                                    # genuine identity check, not a
                                    # tuple-vs-string mismatch that would
                                    # make every legitimate match look
                                    # "foreign."
                                    _registered_watcher_id = (
                                        ("token", _raw_registration_token)
                                        if _raw_registration_token
                                        else None
                                    )
                                    _real_token = str(
                                        getattr(self.entry_watcher, "owner_token", "")
                                        or ""
                                    ).strip()
                                    _adopted = bool(
                                        _real_token
                                        and self.osm.adopt_direction_reversal_watcher_ownership(
                                            _exp_loid,
                                            recovery_owner=str(
                                                _rwr_meta.get("recovery_owner") or ""
                                            ),
                                            watcher_token=_real_token,
                                            generation=_exp_gen,
                                            signal_id=_exp_signal,
                                            execution_mode=_exp_mode,
                                        )
                                    )
                                    # Do not trust the CAS rowcount alone as
                                    # the final word — durably reread and
                                    # confirm the exact ownership transfer
                                    # actually landed before declaring
                                    # success either.
                                    #
                                    # PR #421 final amendment (P0 race 2): a
                                    # failed/raised reread AFTER a successful
                                    # CAS is not evidence the CAS didn't
                                    # commit. Isolate the reread in its own
                                    # try/except so an exception here lands
                                    # in the same "inconclusive" branch as a
                                    # merely-unconfirmed reread, rather than
                                    # escaping to the outer exception handler
                                    # that would otherwise unconditionally
                                    # retain recovery ownership on top of a
                                    # possibly-already-committed watcher.
                                    _verified = False
                                    _verify_exc = None
                                    if _adopted:
                                        try:
                                            _post = self.osm.get_order(_exp_loid)
                                            _post_meta = self._coerce_order_meta(
                                                (_post or {}).get("meta")
                                            )
                                            _verified = (
                                                str(_post_meta.get("current_owner") or "")
                                                == _real_token
                                                and str(
                                                    _post_meta.get("watcher_token") or ""
                                                ) == _real_token
                                                and not _post_meta.get("recovery_owner")
                                                and not _post_meta.get("recovery_ownership")
                                            )
                                        except Exception as _verify_read_exc:
                                            _verify_exc = _verify_read_exc
                                            _verified = False

                                    if _adopted and _verified:
                                        recovered += 1
                                    elif _adopted and not _verified:
                                        # CAS reported success. A failed or
                                        # inconclusive verification reread is
                                        # NOT authoritative evidence the
                                        # commit didn't land — it only proves
                                        # THIS reread couldn't confirm it.
                                        # Per the required invariant: once
                                        # adoption CAS succeeds, watcher
                                        # ownership is authoritative. Do not
                                        # evict the watcher (it may be the
                                        # very registration that just
                                        # committed durable ownership) and do
                                        # not retain recovery ownership on
                                        # top of it (dual authority). Leave
                                        # durable/runtime state exactly as it
                                        # is; a later pass's read-only
                                        # already-owned fast path will
                                        # positively confirm convergence.
                                        log.critical(
                                            "[%s] REARM_WATCHER_REQUIRED_"
                                            "ADOPTION_VERIFICATION_INCONCLUSIVE "
                                            "local_order_id=%s exc=%r — CAS "
                                            "reported success; leaving watcher "
                                            "ownership authoritative, NOT "
                                            "retaining recovery ownership, NOT "
                                            "evicting the watcher",
                                            self.client_id, local_order_id,
                                            _verify_exc,
                                        )
                                        result.setdefault("errors", []).append(
                                            "rearm_watcher_required_adoption_"
                                            f"verification_inconclusive:{local_order_id}"
                                        )
                                    else:
                                        # _adopted is False: the CAS itself
                                        # did not commit. Rollback authority
                                        # is fenced to exact provenance — only
                                        # a watcher THIS invocation actually
                                        # registered may be evicted. A
                                        # pre-existing watcher this pass
                                        # merely observed via PTR's read-only
                                        # fast path must never be touched:
                                        # this recovery attempt did not
                                        # create it and has no authority over
                                        # it, however its own adoption CAS
                                        # turned out.
                                        if (
                                            _registered_by_this_attempt
                                            and _registered_watcher_id is not None
                                        ):
                                            _evicted = self._evict_just_registered_watcher(
                                                self.entry_watcher,
                                                local_order_id=_exp_loid,
                                                client_id=_exp_client,
                                                signal_id=_exp_signal,
                                                execution_mode=_exp_mode,
                                                expected_watcher_id=_registered_watcher_id,
                                            )
                                            if not _evicted:
                                                log.critical(
                                                    "[%s] REARM_WATCHER_REQUIRED_EVICTION_FAILED "
                                                    "local_order_id=%s — a real watcher may "
                                                    "still be registered against a "
                                                    "recovery-owned row",
                                                    self.client_id, local_order_id,
                                                )
                                        else:
                                            log.warning(
                                                "[%s] REARM_WATCHER_REQUIRED_"
                                                "PREEXISTING_WATCHER_ADOPTION_FAILED "
                                                "local_order_id=%s — a pre-existing "
                                                "watcher (not registered by this "
                                                "recovery attempt) failed durable "
                                                "adoption; preserving the watcher "
                                                "untouched, retaining recovery "
                                                "ownership for a later pass only",
                                                self.client_id, local_order_id,
                                            )
                                        _rwr_fail("ownership_adoption_failed")
                                elif _rwr_outcome in (
                                    _RWR_RowOutcome.REARM_OWNED,
                                    _RWR_RowOutcome.RETRY_OWNED,
                                ):
                                    # Existing canonical bounded restart-rearm
                                    # retry durably established.
                                    recovered += 1
                                elif _rwr_outcome == _RWR_RowOutcome.SKIPPED:
                                    _reread = self.osm.get_order(_exp_loid)
                                    _reread_status = str(
                                        (_reread or {}).get("status") or ""
                                    ).strip().upper()
                                    if not _reread or _reread_status == "PENDING_TRIGGER":
                                        _rwr_fail("skipped_unconfirmed")
                                elif _rwr_outcome == _RWR_RowOutcome.UNRESOLVED:
                                    _rwr_fail("ptr_unresolved")
                                elif _rwr_outcome == _RWR_RowOutcome.MATERIALIZATION_OWNED:
                                    # The deferred materializer remains the
                                    # sole owner.  This branch is explicitly
                                    # read-only so it cannot fall through to
                                    # recovery ownership or watcher handling.
                                    recovered += 1
                                    log.info(
                                        "[%s] REARM_WATCHER_REQUIRED_MATERIALIZATION_IN_FLIGHT "
                                        "local_order_id=%s — preserving active materializer owner",
                                        self.client_id,
                                        local_order_id,
                                    )
                                # TERMINALIZED or any other PTR-owned terminal
                                # result: PTR already durably disposed the
                                # row; no further action for this row.
                            except Exception as _rwr_exc:
                                # PR #421 final exception-path correction:
                                # this catch-all wraps the ENTIRE RWR
                                # validation+processing sequence. An
                                # unexpected exception here proves nothing
                                # about whether this actor's generation/
                                # identity/state expectation still matches
                                # the durable row — it may fire well after
                                # a concurrent actor has already advanced
                                # authority. The old fallback to the
                                # weaker, generic _retain_recovery_ownership
                                # (fenced only on blank watcher fields, not
                                # on the exact generation/identity/state
                                # this pass was validating) could let a
                                # stale actor write recovery ownership onto
                                # a row it no longer owns. Fail closed:
                                # log and record telemetry only, zero
                                # durable ownership mutation.
                                log.error(
                                    "[%s] REARM_WATCHER_REQUIRED_HANDLER_EXCEPTION "
                                    "local_order_id=%s exc=%s — zero ownership "
                                    "mutation; this actor cannot prove it still "
                                    "holds exact generation/identity/state "
                                    "authority after an unexpected exception",
                                    self.client_id, local_order_id, _rwr_exc,
                                )
                                result.setdefault("errors", []).append(
                                    f"rearm_watcher_required_exception:"
                                    f"{local_order_id}:{type(_rwr_exc).__name__}"
                                )
                            # Terminate processing of this row for the
                            # current due-retry iteration either way.
                        # All other dispositions (RETRY_WAIT / CLAIM_LOST /
                        # NOT_DUE / KEEP_WATCHER): row is owned by execution
                        # core; skip rearm.
                        continue

                # ── Future-due / orphan rearm path ────────────────────────────
                # Blocker §3: for future-due RETRY_WAIT rows, prove the watcher
                # has an executable schedule before trusting has_order(). An
                # absent or non-matching in-memory schedule means the poll loop
                # will not fire the retry on time.
                if _is_retry_row and _due_at is not None:
                    _future_proof = {"proven": False, "reason_code": "NO_WATCHER"}
                    if self.entry_watcher is not None:
                        _pf = getattr(self.entry_watcher, "prove_materialization_retry_owner", None)
                        if callable(_pf):
                            try:
                                _future_proof = _pf(
                                    local_order_id,
                                    expected_client_id=self.client_id,
                                    expected_execution_mode=recovery_mode,
                                    expected_watcher_token=str(meta.get("watcher_token") or "").strip() or None,
                                    expected_generation=_retry_generation,
                                    durable_next_retry_at=str(_durable_next_retry_at),
                                    durable_retry_deadline=(
                                        str(meta.get("absolute_entry_deadline"))
                                        if meta.get("absolute_entry_deadline") else None
                                    ),
                                ) or {"proven": False, "reason_code": "PROOF_RETURNED_NONE"}
                            except Exception as _fpexc:
                                _future_proof = {
                                    "proven": False,
                                    "reason_code": f"PROOF_EXCEPTION:{type(_fpexc).__name__}",
                                }
                    if _future_proof.get("proven"):
                        continue  # watcher owns this future-due row

                # ── §5: a resumable row must never be ownerless ───────────────
                if self.entry_watcher is None:
                    _retain_recovery_ownership(
                        local_order_id, reason="entry_watcher_unavailable",
                    )
                    continue
                if hasattr(self.entry_watcher, "has_order") and self.entry_watcher.has_order(local_order_id):
                    # For non-RETRY_WAIT orphans (MATERIALIZING, QUEUED,
                    # WAITING_FOR_TRIGGER), registry presence is sufficient.
                    if not _is_retry_row:
                        continue
                    # RETRY_WAIT rows with no durable timestamp are quarantined
                    # above, so _due_at is always set here. Fall through to rearm.

                # ── Build recovery plan lazily (P0 final amendment) ─────────
                # Plan construction was moved here so that due-retry rows can
                # reach resume_deferred_materialization_retry() above without
                # being blocked by a missing or malformed plan. Only rearm
                # paths (future-due watcher rearm, orphan rearm) need the plan.
                plan = self._build_recovery_plan_from_order(order)
                if plan is None:
                    if _is_retry_row:
                        # A future-due RETRY_WAIT row with an invalid plan
                        # cannot be rearmed — retain ownership so a future pass
                        # can retry after the row is repaired.
                        log.critical(
                            "[%s] RECOVERY_RETRY_REARM_PLAN_INVALID local_order_id=%s "
                            "lifecycle=%s — retaining ownership; row not rearmed",
                            self.client_id, local_order_id, lifecycle,
                        )
                        result.setdefault("errors", []).append(
                            "recovery_retry_rearm_plan_invalid"
                        )
                        _retain_recovery_ownership(
                            local_order_id, reason="retry_rearm_plan_invalid",
                        )
                    # For non-retry orphans: silently skip (pre-existing behaviour
                    # — the plan builder already logged the reason).
                    continue

                # ── Plan-level identity proof ────────────────────────────
                plan_client_id = str(getattr(plan, "client_id", "") or "").strip().lower()
                if plan_client_id and plan_client_id != self.client_id:
                    log.error(
                        "[%s] RECOVERY_SKIP plan_client_id_mismatch local_order_id=%s plan=%r",
                        self.client_id, local_order_id, plan_client_id,
                    )
                    continue
                plan_mode_raw = str(getattr(plan, "execution_mode", "") or "").strip().upper()
                if plan_mode_raw != recovery_mode:
                    log.error(
                        "[%s] RECOVERY_SKIP plan_execution_mode_mismatch local_order_id=%s "
                        "plan_mode=%r recovery_mode=%s",
                        self.client_id, local_order_id, plan_mode_raw, recovery_mode,
                    )
                    continue

                plan.metadata["materialization_generation"] = (
                    int(_retry_generation) if _retry_generation is not None else int(
                        meta.get("materialization_generation") or 1
                    )
                )
                if _retry_attempt is not None:
                    plan.metadata["retry_attempt"] = int(_retry_attempt)
                plan.metadata["contract_deferred"] = True
                armed = bool(self.entry_watcher.watch(
                    plan,
                    local_order_id,
                    recovery_rearm=True,
                    no_cancel_on_reject=True,
                    materialization_resume=materialization_resume,
                ))
                if armed:
                    recovered += 1
                else:
                    _retain_recovery_ownership(
                        local_order_id, reason="watcher_rearm_returned_false",
                    )
        result["deferred_lifecycles_recovered"] = recovered

    def _reseed_watchers(self, result: dict):
        """
        Reset WATCHING signals back to NEW so the queue worker re-processes
        them and re-hands them to APEntryWatcher on startup.

        Only resets signals created today (ET) to avoid re-triggering stale
        multi-day signals that should have expired.
        """
        from ap.db import conn, run_with_retry
        from datetime import datetime, timezone, timedelta
        from zoneinfo import ZoneInfo

        ET = ZoneInfo("America/New_York")
        now_et   = datetime.now(ET)
        # Default 48 hours covers Sunday evening scanner signals for Monday open.
        # The exact reseed window is operator-tunable via
        # STARTUP_WATCHER_RESEED_LOOKBACK_HOURS.
        _lookback_hours = int(os.getenv("STARTUP_WATCHER_RESEED_LOOKBACK_HOURS", "48"))
        cutoff_utc = (now_et.astimezone(timezone.utc) - timedelta(hours=_lookback_hours)).isoformat()

        def _reset():
            # PR #143: tag rescued rows in payload.recovery_rescue so the
            # queue dispatch can recognize them as eligible for PAPER
            # immediate-entry submission after contract selection.
            # Payload is a JSONB column — merge via || so the existing
            # signal_id, ticker, score, etc. are preserved verbatim.
            # The marker is also stamped with a UTC timestamp + the lookback
            # window for audit. LIVE clients are not exempted in this query;
            # the LIVE/PAPER gate lives in queue._dispatch so the LIVE path
            # ignores the marker.
            with conn() as c:
                _marker_payload = (
                    '{"recovery_rescue":true,'
                    '"recovery_rescue_ts":"' + now_et.astimezone(timezone.utc).isoformat() + '",'
                    '"recovery_rescue_lookback_hours":' + str(int(_lookback_hours)) + '}'
                )
                c.execute(
                    """
                    UPDATE trade_queue
                    SET    status  = 'NEW',
                           payload = COALESCE(payload, '{}'::jsonb) || %s::jsonb
                    WHERE  client_id  = %s
                      AND  status     = 'WATCHING'
                      AND  created_ts >= %s
                      AND  NOT EXISTS (
                             SELECT 1
                             FROM orders o
                             WHERE o.client_id = trade_queue.client_id
                               AND o.signal_id = trade_queue.signal_id
                               AND o.kind = 'ENTRY'
                               AND o.status = 'PENDING_TRIGGER'
                               AND o.broker_order_id IS NULL
                               AND o.submitted_ts IS NULL
                               AND o.filled_ts IS NULL
                           )
                    """,
                    (_marker_payload, self.client_id, cutoff_utc),
                )
                return c.rowcount

        def _load_orphaned_pending_trigger_orders():
            with conn() as c:
                # P0 amendment (PR #294): join the paired trade_queue row and
                # exclude any pair whose queue row was moved out of WATCHING
                # by the readiness pass (ARCHIVED / EXPIRED with a
                # READINESS_* last_error). LEFT JOIN keeps historical orders
                # without a queue row visible (legacy safety); the readiness
                # exclusion only fires when a matching queue row exists AND
                # its status/last_error prove a classification decision was
                # made. Nothing is written; only the read set narrows.
                c.execute(
                    """
                    SELECT o.local_order_id,
                           o.signal_id,
                           o.plan_id,
                           o.symbol,
                           o.contract,
                           o.direction,
                           o.score,
                           o.tier,
                           o.trigger_price,
                           o.stop_underlying,
                           o.target_underlying,
                           o.pattern,
                           o.timeframe,
                           o.meta,
                           tq.status     AS _tq_status,
                           tq.last_error AS _tq_last_error
                    FROM orders o
                    LEFT JOIN LATERAL (
                             SELECT status, last_error
                             FROM   trade_queue
                             WHERE  trade_queue.client_id = o.client_id
                               AND  trade_queue.signal_id = o.signal_id
                             ORDER BY created_ts DESC
                             LIMIT 1
                    ) tq ON TRUE
                    WHERE o.client_id = %s
                      AND o.kind = 'ENTRY'
                      AND o.status = 'PENDING_TRIGGER'
                      AND o.created_ts >= %s
                      AND o.broker_order_id IS NULL
                      AND o.submitted_ts IS NULL
                      AND o.filled_ts IS NULL
                      AND (
                            tq.status IS NULL           -- no paired queue row (legacy safety)
                         OR tq.status = 'WATCHING'      -- fresh eligible pair
                      )
                      AND (
                            tq.last_error IS NULL
                         OR (
                                tq.last_error NOT LIKE 'READINESS_ARCHIVED_STALE%%'
                            AND tq.last_error NOT LIKE 'READINESS_ARCHIVED_NON_TRADING_DAY%%'
                            AND tq.last_error NOT LIKE 'READINESS_ARCHIVED_NOT_IN_ALLOWLIST%%'
                            AND tq.last_error NOT LIKE 'READINESS_ORPHANED_ORDER%%'
                         )
                      )
                    ORDER BY o.created_ts ASC
                    """,
                    (self.client_id, cutoff_utc),
                )
                return c.fetchall()

        # ── PR #143 regression fix: LIVE never replays WATCHING rows ─────
        # The _reset() block above resets WATCHING → NEW and tags rows with
        # payload.recovery_rescue=true. PR #143 used this for paper-mode
        # immediate-entry recovery. On 2026-06-16 Jason's LIVE pod replayed
        # 23 WATCHING rows through MC/contract_selector with current (post-
        # trigger) market data. The selector's earnings/IV/chain gates
        # rejected all 23 with contract_selection:no_contract_found. Zero
        # live trades. LIVE must NEVER replay stale signals through the
        # selector — it can only reattach watcher ownership to current-
        # session WATCHING/PENDING_TRIGGER rows.
        #
        # Detection: read mc.mode (canonical mode source per execution_core
        # PR-B / FIX-3). Default to PAPER on lookup failure to preserve PR
        # #143 paper behavior — never silently fall into LIVE replay if mc
        # is mis-shaped.
        _mc_mode = "PAPER"
        try:
            _mc_mode = str(getattr(self.mc, "mode", "PAPER") or "PAPER").upper()
        except Exception:
            _mc_mode = "PAPER"
        _is_live = (_mc_mode == "LIVE")

        # ── P0 amendment (PR #294): readiness-aware reseed gate ────────────
        # A row's presence as a recent orphaned PENDING_TRIGGER is not by
        # itself proof the operator wants it re-armed. When the pre-open
        # WATCHING readiness pass has already ARCHIVED / EXPIRED the paired
        # trade_queue row — because it is stale beyond the cutoff, was
        # generated on a non-trading day (e.g. 2026-07-03 holiday batch), sits
        # outside the entry allowlist, or its paired ENTRY order is already
        # terminal — re-arming its watcher would nullify the classification
        # decision and re-open the very dedup/pipe clog the readiness pass
        # exists to close. The SELECT below therefore joins the paired
        # trade_queue row and filters to (status='WATCHING' or paired queue
        # row missing) so a rearm can only happen against a fresh, eligible,
        # still-WATCHING row. Rows whose queue row was READINESS_* terminalized
        # or moved out of WATCHING are excluded from the read set; that is the
        # first defensive layer. The per-row skip below is the second layer:
        # if a queue row transitions AFTER the SELECT but BEFORE the reseed
        # loop reaches it (concurrent operator action, cron overlap), the
        # per-row check catches it. Both layers preserve the LIVE
        # no-replay invariant; neither writes anywhere; neither raises.
        _READINESS_SKIP_LAST_ERRORS = (
            "READINESS_ARCHIVED_STALE",
            "READINESS_ARCHIVED_NON_TRADING_DAY",
            "READINESS_ARCHIVED_NOT_IN_ALLOWLIST",
            "READINESS_ORPHANED_ORDER",  # matches READINESS_ORPHANED_ORDER_EXPIRED, _CANCELLED, etc.
        )

        def _last_error_is_readiness_skip(last_error) -> bool:
            # Diagnostic-only: never raise, even on corrupt row shapes. We
            # str()-coerce inside a bare try/except so an odd payload (e.g.
            # a shim object whose __str__ raises) falls through as "no
            # readiness marker" and defers to the SELECT filter + downstream
            # checks rather than aborting the whole reseed pass.
            try:
                _le = str(last_error) if last_error is not None else ""
            except Exception:
                return False
            return any(_le.startswith(_p) for _p in _READINESS_SKIP_LAST_ERRORS)

        if _is_live:
            # Audit-required log; emitted before any DB write so it's
            # visible even if downstream paths fail.
            log.warning(
                "LIVE_RECOVERY_REPLAY_SKIPPED client_id=%s mode=%s "
                "reason=live_no_replay_policy lookback_hours=%d cutoff=%s "
                "| WATCHING rows are NOT reset for LIVE clients; only "
                "orphaned PENDING_TRIGGER watcher reattachment will run",
                self.client_id, _mc_mode, _lookback_hours, cutoff_utc[:19],
            )
            count = 0  # No WATCHING rows reset for LIVE.
        else:
            count = run_with_retry(_reset) or 0
        rearmed = 0
        already_verified_owner_rows = 0
        if self.entry_watcher is None:
            log.warning(
                "[%s] RECOVERY: entry_watcher missing — cannot reseed orphaned PENDING_TRIGGER orders",
                self.client_id,
            )
        else:
            rows = run_with_retry(_load_orphaned_pending_trigger_orders) or []
            for row in rows:
                order = dict(row or {})
                local_order_id = str(order.get("local_order_id") or "").strip()
                if not local_order_id:
                    continue

                # P0 amendment (PR #294): per-row readiness guard. The SELECT
                # above already excludes readiness-terminalized pairs; this
                # is the second defensive layer against a queue row that
                # transitioned AFTER the SELECT but BEFORE this loop reached
                # it (concurrent operator action, cron overlap, retry-loop
                # timing). Diagnostic-only: `continue` past the row, do not
                # raise, do not write. `_tq_status`/`_tq_last_error` come
                # from the LEFT JOIN LATERAL; None on rows without a paired
                # queue row (legacy safety — still eligible for reseed).
                try:
                    _tq_status = str(order.get("_tq_status") or "").upper()
                    _tq_last_error = order.get("_tq_last_error")
                    _readiness_skip_reason = None
                    if _tq_status and _tq_status != "WATCHING":
                        _readiness_skip_reason = f"queue_row_not_watching:{_tq_status}"
                    elif _last_error_is_readiness_skip(_tq_last_error):
                        _readiness_skip_reason = f"queue_readiness_terminal:{_tq_last_error}"
                    if _readiness_skip_reason:
                        log.info(
                            "[%s] RECOVERY: readiness_reseed_skip local_order_id=%s "
                            "signal_id=%s reason=%s | queue row was classified out "
                            "of WATCHING by the pre-open readiness pass; watcher "
                            "reseed suppressed (no order/queue/positions/proof "
                            "mutation)",
                            self.client_id, local_order_id,
                            order.get("signal_id"), _readiness_skip_reason,
                        )
                        continue
                except Exception as _gate_exc:
                    # Guard must never raise. On unexpected shape, fall
                    # through to the existing ownership/plan checks — the
                    # SELECT filter is still the primary defence.
                    log.warning(
                        "[%s] RECOVERY: readiness_reseed_guard_error "
                        "local_order_id=%s error=%s | falling through to "
                        "downstream checks",
                        self.client_id, local_order_id, _gate_exc,
                    )
                # PR #421 Blocker 1 completion: parsed once, early, and
                # reused later — never recomputed after PTR runs.
                # Defaults ensure these are always defined even if the
                # early parse/has_order check below raises.
                _reseed_row_meta = {}
                _requires_durable_adoption = False
                try:
                    _reseed_row_meta = self._coerce_order_meta(
                        order.get("meta")
                    )
                    _requires_durable_adoption = bool(
                        _reseed_row_meta.get(
                            "direction_reversal_rearm_requires_watcher"
                        )
                    )
                    _runtime_watcher_exists = bool(
                        hasattr(self.entry_watcher, "has_order")
                        and self.entry_watcher.has_order(local_order_id)
                    )
                    if _runtime_watcher_exists:
                        if not _requires_durable_adoption:
                            # Ordinary already-owned row — unchanged
                            # behavior.
                            log.info(
                                "[%s] RECOVERY: watcher already owns "
                                "local_order_id=%s — skipping duplicate "
                                "reseed",
                                self.client_id, local_order_id,
                            )
                            already_verified_owner_rows += 1
                            continue
                        # PR #421 Blocker 1 completion: a real runtime
                        # watcher already exists, but the durable row is
                        # still in the direction-reversal recovery-owned
                        # state (never converged). The prior fast path
                        # would skip this row forever — a runtime watcher
                        # alone is not release authority, and skipping
                        # here means PTR (and therefore the adoption CAS)
                        # would never run for it. Fall through into the
                        # normal PTR/adoption path instead of continuing;
                        # PTR itself remains the causal authority for
                        # provenance (it will correctly report
                        # last_watcher_registered_by_this_attempt=False
                        # for a watcher it did not just create).
                        log.info(
                            "[%s] RECOVERY: runtime watcher exists but "
                            "direction-reversal durable adoption is still "
                            "required | local_order_id=%s — routing "
                            "through PTR/adoption instead of the "
                            "runtime-only skip",
                            self.client_id, local_order_id,
                        )
                except Exception as exc:
                    log.warning(
                        "[%s] RECOVERY: watcher ownership check failed for local_order_id=%s: %s",
                        self.client_id, local_order_id, exc,
                    )

                # PR #328 — canonical classifier gate before watch().
                # Wires PendingTriggerRestartRecovery as the single decision
                # authority; removes the prior unconditional watch() call that
                # bypassed classification for invalidated/terminal/stale rows.
                try:
                    from ap.pending_trigger_restart_recovery import (
                        PendingTriggerRestartRecovery as _PTR,
                    )
                    _row_dict = dict(order)

                    def _plan_builder(_r):
                        _p = self._build_recovery_plan_from_order(_r)
                        if _p is None:
                            log.warning(
                                "[%s] RECOVERY: cannot rearm local_order_id=%s — invalid_or_missing_side",
                                self.client_id, local_order_id,
                            )
                            return None
                        if not getattr(_p, "ticker", "") or getattr(_p, "trigger_price", None) in (None, 0, 0.0):
                            log.warning(
                                "[%s] RECOVERY: cannot rearm local_order_id=%s — missing ticker or trigger_price",
                                self.client_id, local_order_id,
                            )
                            return None
                        if str(getattr(_p, "contract_symbol", "") or "").upper().startswith("DEFERRED:"):
                            try:
                                if not hasattr(_p, "metadata") or _p.metadata is None:
                                    _p.metadata = {}
                                _p.metadata["contract_deferred"] = True
                            except Exception:
                                pass
                        # The recovery engine and APEntryWatcher.watch() share
                        # the established attribute-based plan contract.
                        return _p

                    _ptr = _PTR(
                        client_id=self.client_id,
                        execution_mode=self._execution_mode() or "",
                        osm=self.osm,
                        entry_watcher=self.entry_watcher,
                        broker=self.broker,
                        caller_source="ap_recovery._reseed_watchers",
                    )
                    _outcome = _ptr.recover_one_row(
                        _row_dict, plan_builder_fn=_plan_builder
                    )
                    from ap.pending_trigger_restart_recovery import _RowOutcome
                    if _outcome == _RowOutcome.WATCHER_OWNED:
                        # PR #421 Blocker 1: WATCHER_OWNED alone does not
                        # prove durable database ownership has converged
                        # from recovery to watcher for a direction-
                        # reversal watcher-required row — PTR may have
                        # only proved/registered a REAL RUNTIME watcher.
                        # A runtime watcher is not release authority; the
                        # durable orders.meta row must actually show the
                        # watcher as current_owner/watcher_token before
                        # this counts as a successful rearm. Ordinary
                        # PENDING_TRIGGER rows that never went through the
                        # recovery-owned direction-reversal rearm state
                        # need no such transfer — WATCHER_OWNED already
                        # means their durable ownership was correct.
                        #
                        # _reseed_row_meta / _requires_durable_adoption
                        # were already parsed once, early, before the
                        # has_order() fast-path decision above. Reused
                        # here rather than re-parsed — PTR's
                        # recover_one_row() does not itself mutate
                        # direction_reversal_rearm_requires_watcher, only
                        # the separate adoption CAS below does, so the
                        # earlier snapshot remains valid.
                        if not _requires_durable_adoption:
                            rearmed += 1
                            log.info(
                                "[%s] RECOVERY: watcher re-armed+verified | "
                                "local_order_id=%s cls=%s",
                                self.client_id, local_order_id, _outcome,
                            )
                        else:
                            def _reseed_strict_generation(raw):
                                # Same strict parser as the due-retry RWR
                                # path: reject missing, Boolean, float,
                                # negative, blank, or string values rather
                                # than silently coercing.
                                return (
                                    raw
                                    if isinstance(raw, int)
                                    and not isinstance(raw, bool)
                                    and raw >= 1
                                    else None
                                )

                            _reseed_registered_by_this_attempt = bool(
                                getattr(
                                    _ptr,
                                    "last_watcher_registered_by_this_attempt",
                                    False,
                                )
                            )
                            _reseed_raw_token = (
                                getattr(_ptr, "last_registration_token", None)
                                if _reseed_registered_by_this_attempt
                                else None
                            )
                            # Wrap into the exact-identity tuple shape
                            # _evict_just_registered_watcher/_exact_identity_of
                            # compare against — a bare token string would
                            # never equal ("token", value) and every
                            # legitimate match would look "foreign".
                            _reseed_registration_id = (
                                ("token", _reseed_raw_token)
                                if _reseed_raw_token
                                else None
                            )
                            _reseed_real_token = str(
                                getattr(self.entry_watcher, "owner_token", "")
                                or ""
                            ).strip()
                            _reseed_signal = str(
                                order.get("signal_id") or ""
                            ).strip()
                            _reseed_mode = str(
                                self._execution_mode() or ""
                            ).strip().lower()
                            _reseed_gen = _reseed_strict_generation(
                                _reseed_row_meta.get(
                                    "materialization_generation"
                                )
                            )
                            _reseed_recovery_owner = str(
                                _reseed_row_meta.get("recovery_owner") or ""
                            )

                            _reseed_adopted = bool(
                                _reseed_real_token
                                and _reseed_gen is not None
                                and _reseed_signal
                                and _reseed_mode in {"live", "paper"}
                                and self.osm.adopt_direction_reversal_watcher_ownership(
                                    local_order_id,
                                    recovery_owner=_reseed_recovery_owner,
                                    watcher_token=_reseed_real_token,
                                    generation=_reseed_gen,
                                    signal_id=_reseed_signal,
                                    execution_mode=_reseed_mode,
                                )
                            )

                            # Do not trust the CAS rowcount alone — durably
                            # reread and confirm the exact ownership
                            # transfer actually landed. Isolate the reread
                            # in its own try/except so a raised exception
                            # lands in the "inconclusive" branch, not a
                            # false "not adopted" classification.
                            _reseed_verified = False
                            _reseed_verify_exc = None
                            if _reseed_adopted:
                                try:
                                    _reseed_post = self.osm.get_order(
                                        local_order_id
                                    )
                                    _reseed_post_meta = self._coerce_order_meta(
                                        (_reseed_post or {}).get("meta")
                                    )
                                    _reseed_verified = (
                                        str(
                                            _reseed_post_meta.get("current_owner")
                                            or ""
                                        ) == _reseed_real_token
                                        and str(
                                            _reseed_post_meta.get("watcher_token")
                                            or ""
                                        ) == _reseed_real_token
                                        and _reseed_strict_generation(
                                            _reseed_post_meta.get(
                                                "watcher_generation"
                                            )
                                        ) == _reseed_gen
                                        and not _reseed_post_meta.get(
                                            "recovery_owner"
                                        )
                                        and not _reseed_post_meta.get(
                                            "recovery_ownership"
                                        )
                                        # Explicit False required — missing
                                        # metadata must not pass. A row
                                        # whose adoption patch never landed
                                        # (or landed against a different
                                        # key shape) would otherwise read
                                        # as "flag absent" and incorrectly
                                        # satisfy `not ...get(...)`.
                                        and _reseed_post_meta.get(
                                            "direction_reversal_rearm_"
                                            "requires_watcher"
                                        ) is False
                                        and str(
                                            _reseed_post_meta.get(
                                                "materialization_status"
                                            ) or ""
                                        ) == "WAITING_FOR_TRIGGER"
                                    )
                                except Exception as _reseed_verify_read_exc:
                                    _reseed_verify_exc = _reseed_verify_read_exc
                                    _reseed_verified = False

                            if _reseed_adopted and _reseed_verified:
                                rearmed += 1
                                log.info(
                                    "[%s] RECOVERY: startup direction-reversal "
                                    "watcher durably adopted+verified | "
                                    "local_order_id=%s",
                                    self.client_id, local_order_id,
                                )
                            elif _reseed_adopted and not _reseed_verified:
                                # CAS reported success; a failed/inconclusive
                                # reread is not evidence the commit didn't
                                # land. Do not evict the watcher, do not
                                # restore recovery ownership, do not count
                                # as rearmed — leave durable/runtime state
                                # exactly as it is for a later pass to
                                # positively confirm.
                                log.critical(
                                    "[%s] STARTUP_RESEED_ADOPTION_VERIFICATION_"
                                    "INCONCLUSIVE local_order_id=%s exc=%r — CAS "
                                    "reported success; leaving watcher "
                                    "ownership authoritative, not retaining "
                                    "recovery ownership, not evicting the "
                                    "watcher",
                                    self.client_id, local_order_id,
                                    _reseed_verify_exc,
                                )
                                result.setdefault("errors", []).append(
                                    "startup_reseed_adoption_verification_"
                                    f"inconclusive:{local_order_id}"
                                )
                            else:
                                # Adoption CAS did not succeed. Rollback
                                # authority is fenced to exact provenance —
                                # only a watcher THIS invocation actually
                                # registered may be evicted. A pre-existing
                                # watcher this pass merely observed must
                                # never be touched; the row remains
                                # recoverable through the existing bounded
                                # recovery mechanism (its durable
                                # recovery-owned state is untouched here).
                                if (
                                    _reseed_registered_by_this_attempt
                                    and _reseed_registration_id is not None
                                ):
                                    _reseed_evicted = (
                                        self._evict_just_registered_watcher(
                                            self.entry_watcher,
                                            local_order_id=local_order_id,
                                            client_id=self.client_id,
                                            signal_id=_reseed_signal,
                                            execution_mode=_reseed_mode,
                                            expected_watcher_id=(
                                                _reseed_registration_id
                                            ),
                                        )
                                    )
                                    if not _reseed_evicted:
                                        log.critical(
                                            "[%s] STARTUP_RESEED_EVICTION_FAILED "
                                            "local_order_id=%s — a real watcher "
                                            "may still be registered against a "
                                            "recovery-owned row",
                                            self.client_id, local_order_id,
                                        )
                                else:
                                    log.warning(
                                        "[%s] STARTUP_RESEED_PREEXISTING_WATCHER_"
                                        "ADOPTION_FAILED local_order_id=%s — a "
                                        "pre-existing watcher (not registered "
                                        "by this startup attempt) failed "
                                        "durable adoption; preserving the "
                                        "watcher untouched",
                                        self.client_id, local_order_id,
                                    )
                                result.setdefault("errors", []).append(
                                    "startup_reseed_ownership_adoption_failed:"
                                    f"{local_order_id}"
                                )
                    elif _outcome == _RowOutcome.RETRY_OWNED:
                        already_verified_owner_rows += 1
                        log.info(
                            "[%s] RECOVERY: retry_owned | local_order_id=%s",
                            self.client_id, local_order_id,
                        )
                    elif _outcome == _RowOutcome.TERMINALIZED:
                        log.info(
                            "[%s] RECOVERY: terminalized | local_order_id=%s",
                            self.client_id, local_order_id,
                        )
                    elif _outcome == _RowOutcome.SKIPPED:
                        log.info(
                            "[%s] RECOVERY: skipped (not PENDING_TRIGGER) | local_order_id=%s",
                            self.client_id, local_order_id,
                        )
                    elif _outcome == _RowOutcome.MATERIALIZATION_OWNED:
                        already_verified_owner_rows += 1
                        log.info(
                            "[%s] RECOVERY: materialization_in_flight_owned | "
                            "local_order_id=%s — preserving active materializer owner",
                            self.client_id,
                            local_order_id,
                        )
                    else:
                        log.critical(
                            "[%s] RECOVERY: UNRESOLVED | local_order_id=%s — operator review required",
                            self.client_id, local_order_id,
                        )
                    continue
                except Exception as _ptr_exc:
                    log.error(
                        "[%s] RECOVERY: PendingTriggerRestartRecovery error local_order_id=%s: %s "
                        "— canonical recovery failed closed",
                        self.client_id, local_order_id, _ptr_exc,
                    )
                    continue

        result["watching_rows_reset"] = int(count or 0)
        result["pending_trigger_watchers_rearmed"] = int(rearmed or 0)
        result["already_verified_owner_rows"] = int(already_verified_owner_rows or 0)
        result["watchers_requeued"] = count + rearmed
        log.info(
            "[%s] RECOVERY: %d WATCHING signals reset to NEW and %d orphaned PENDING_TRIGGER orders re-armed "
            "for watcher reseed (lookback=%dh cutoff=%s)",
            self.client_id, count, rearmed, _lookback_hours, cutoff_utc[:19],
        )
