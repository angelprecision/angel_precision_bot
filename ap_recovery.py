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
            "buying_power_reserved": 0.0,
            "dedup_seeded":        0,
            "watchers_requeued":   0,
            "deferred_lifecycles_recovered": 0,
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
            self._reattach_exit_protections(result)
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
            "entries_corrected=%d exits=%d dedup=%d watchers_requeued=%d buying_power=$%.2f errors=%d",
            self.client_id,
            result["positions_recovered"],
            result["entries_verified"],
            result["entries_corrected"],
            result["exits_reattached"],
            result["dedup_seeded"],
            result["watchers_requeued"],
            result["buying_power_reserved"],
            len(result["errors"]),
        )
        return result

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
        if self._execution_mode() is None:
            result.setdefault("errors", []).append("recovery_unknown_execution_mode")
            return
        from ap.db import get_open_orders_for_reconcile, run_with_retry

        orders = run_with_retry(
            lambda: get_open_orders_for_reconcile(client_id=self.client_id)
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
    # 3. Exit protection reattachment
    # ──────────────────────────────────────────────────────────────────────────

    def _reattach_exit_protections(self, result: dict):
        """
        For every CLOSING position, verify the exit order is still live at
        broker. If the exit filled while we were down, advance the position.
        If exit canceled/expired, revert position to OPEN.
        """
        if self._execution_mode() is None:
            result.setdefault("errors", []).append("recovery_unknown_execution_mode")
            return
        from ap.db import list_positions, run_with_retry, conn

        closings = run_with_retry(
            lambda: list_positions(client_id=self.client_id, status="CLOSING")
        )
        if not closings:
            return

        for pos in closings:
            pos_id     = pos.get("id")
            underlying = pos.get("underlying") or pos.get("ticker", "?")

            # Find the active exit order for this position
            def _get_exit_order(pid=pos_id):
                with conn() as c:
                    c.execute(
                        """
                        SELECT local_order_id, broker_order_id, status
                        FROM orders
                        WHERE client_id=%s AND position_id=%s AND kind='EXIT'
                          AND status NOT IN ('EXIT_FILLED','REJECTED','CANCELED','EXPIRED','ERROR')
                        ORDER BY created_ts DESC LIMIT 1
                        """,
                        (self.client_id, pid),
                    )
                    return c.fetchone()

            try:
                exit_order = run_with_retry(_get_exit_order)
            except Exception as e:
                log.error("[%s] RECOVERY: exit order lookup failed for pos %s: %s",
                          self.client_id, pos_id, e)
                continue

            if not exit_order:
                # CLOSING position with no exit order — needs manual exit
                log.error(
                    "[%s] RECOVERY: CLOSING position %s (%s) has NO active exit order — "
                    "exit must be re-submitted manually",
                    self.client_id, pos_id, underlying,
                )
                continue

            broker_oid = exit_order.get("broker_order_id")
            local_id   = exit_order.get("local_order_id")
            db_status  = exit_order.get("status", "")

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

            if broker_status in BROKER_FILLED:
                # Exit actually filled while we were down — close the position
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
                    self.osm.transition(
                        local_id, "EXIT_FILLED",
                        filled_qty=filled_qty,
                        fill_price=avg_fill,
                    )
                    log.info(
                        "[%s] RECOVERY: exit filled during downtime | pos=%s %s | "
                        "qty=%d avg=$%.2f",
                        self.client_id, pos_id, underlying, filled_qty, avg_fill,
                    )
                    result["exits_reattached"] += 1
                except Exception as e:
                    log.error("[%s] RECOVERY: EXIT_FILLED transition failed: %s",
                              self.client_id, e)

            elif broker_status in BROKER_TERMINAL:
                # Exit canceled — revert position to OPEN so it can be re-exited
                try:
                    self.osm.transition(
                        local_id, BROKER_TO_OSM.get(broker_status, "CANCELED"),
                        last_error=f"recovery: broker_status={broker_status}",
                    )
                    # Revert position
                    from ap.db import conn as _conn
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
            else:
                # Exit is still live at broker — reattach to exit engine if available
                log.info(
                    "[%s] RECOVERY: exit order still live at broker | "
                    "pos=%s %s | broker_status=%s",
                    self.client_id, pos_id, underlying, broker_status,
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

    # ──────────────────────────────────────────────────────────────────────────
    # 4. Buying power reservation
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

        return types.SimpleNamespace(
            signal_id=str(
                order.get("signal_id")
                or meta.get("signal_id")
                or order.get("local_order_id")
                or ""
            ),
            plan_id=str(order.get("plan_id") or meta.get("plan_id") or ""),
            ticker=ticker,
            side=direction,
            direction=direction,
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
            contracts=int(order.get("qty") or meta.get("selected_qty") or 0),
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
                           symbol, contract, direction, score, tier,
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
            lifecycle = str(meta.get("lifecycle_state") or "").upper()
            materialization_status = str(meta.get("materialization_status") or "").upper()

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

            plan = self._build_recovery_plan_from_order(order)
            if plan is None:
                continue

            # ── Plan-level identity proof ────────────────────────────
            # After building the plan, verify the plan carries the exact
            # client_id and execution_mode we expect. The plan builder no
            # longer infers execution_mode from the runner (Amendment 1),
            # so a persisted row with a blank mode surfaces here as an
            # empty plan.execution_mode string and is skipped.
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
                    # Reconciler proved the row never reached the broker
                    # boundary — fall through to the normal resume path below.
                    pass
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

                try:
                    outcome = resume_fn(local_order_id=local_order_id, plan=plan) or {}
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
                # ── AMENDMENT §5: a resumable row must never be ownerless ──
                if self.entry_watcher is None:
                    # No watcher wired — cannot rearm this pass. Record
                    # durable ownership so the row is diagnosably owned by
                    # the recovery scheduler and a future pass resumes it.
                    _retain_recovery_ownership(
                        local_order_id, reason="entry_watcher_unavailable",
                    )
                    continue
                if hasattr(self.entry_watcher, "has_order") and self.entry_watcher.has_order(local_order_id):
                    # Already owned by a live watcher — nothing to do.
                    continue
                plan.metadata["materialization_generation"] = int(
                    meta.get("materialization_generation") or 1
                )
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
                    # Rearm returned False — the watcher did NOT take
                    # ownership. Do not silently drop the row; record durable
                    # recovery ownership so it is resumed on a later pass.
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
                try:
                    if hasattr(self.entry_watcher, "has_order") and self.entry_watcher.has_order(local_order_id):
                        log.info(
                            "[%s] RECOVERY: watcher already owns local_order_id=%s — skipping duplicate reseed",
                            self.client_id, local_order_id,
                        )
                        continue
                except Exception as exc:
                    log.warning(
                        "[%s] RECOVERY: watcher ownership check failed for local_order_id=%s: %s",
                        self.client_id, local_order_id, exc,
                    )

                plan = self._build_recovery_plan_from_order(order)
                if plan is None:
                    log.warning(
                        "[%s] RECOVERY: cannot reseed local_order_id=%s — invalid_or_missing_side",
                        self.client_id, local_order_id,
                    )
                    continue
                if not getattr(plan, "ticker", "") or getattr(plan, "trigger_price", None) in (None, 0, 0.0):
                    log.warning(
                        "[%s] RECOVERY: cannot reseed local_order_id=%s — missing ticker or trigger_price",
                        self.client_id, local_order_id,
                    )
                    continue
                if str(getattr(plan, "contract_symbol", "") or "").upper().startswith("DEFERRED:"):
                    try:
                        if not hasattr(plan, "metadata") or plan.metadata is None:
                            plan.metadata = {}
                        plan.metadata["contract_deferred"] = True
                    except Exception:
                        pass

                # PR #328 — canonical classifier gate before watch().
                # Wires PendingTriggerRestartRecovery as the single decision
                # authority; removes the prior unconditional watch() call that
                # bypassed classification for invalidated/terminal/stale rows.
                try:
                    from ap.pending_trigger_restart_recovery import (
                        PendingTriggerRestartRecovery as _PTR,
                    )
                    _row_dict = dict(order)
                    if "local_order_id" not in _row_dict and local_order_id:
                        _row_dict["local_order_id"] = local_order_id
                    if "client_id" not in _row_dict:
                        _row_dict["client_id"] = self.client_id
                    if "execution_mode" not in _row_dict:
                        _row_dict["execution_mode"] = self._execution_mode() or ""

                    # Build a plan_builder that uses the already-constructed plan.
                    _plan_obj = plan
                    def _plan_builder(_r, _p=_plan_obj):
                        # Convert plan object to dict for the recovery engine.
                        return {
                            "signal_id":      getattr(_p, "signal_id", "") or "",
                            "plan_id":        getattr(_p, "plan_id", "") or "",
                            "local_order_id": getattr(_p, "local_order_id", local_order_id) or local_order_id,
                            "client_id":      getattr(_p, "client_id", self.client_id) or self.client_id,
                            "client_email":   getattr(_p, "client_id", self.client_id) or self.client_id,
                            "execution_mode": self._execution_mode() or "",
                            "ticker":         getattr(_p, "ticker", "") or "",
                            "side":           getattr(_p, "side", "") or "",
                            "entry_price":    float(getattr(_p, "trigger_price", 0) or 0),
                            "stop_price":     float(getattr(_p, "stop_price", 0) or 0),
                            "target_price":   float(getattr(_p, "target_price", 0) or 0),
                            "score":          float(getattr(_p, "score", 0) or 0),
                            "tier":           getattr(_p, "tier", "") or "",
                            "timeframe":      getattr(_p, "timeframe", "") or "",
                            "contracts":      int(getattr(_p, "quantity", 0) or 0),
                            "contract":       getattr(_p, "contract_symbol", "") or "",
                        }

                    _ptr = _PTR(
                        client_id=self.client_id,
                        execution_mode=self._execution_mode() or "",
                        osm=self.osm,
                        entry_watcher=self.entry_watcher,
                        broker=self.broker,
                    )
                    _outcome = _ptr.recover_one_row(
                        _row_dict, plan_builder_fn=_plan_builder
                    )
                    from ap.pending_trigger_restart_recovery import _RowOutcome
                    if _outcome == _RowOutcome.WATCHER_OWNED:
                        rearmed += 1
                        log.info(
                            "[%s] RECOVERY: watcher re-armed+verified | local_order_id=%s cls=%s",
                            self.client_id, local_order_id, _outcome,
                        )
                    elif _outcome == _RowOutcome.RETRY_OWNED:
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
                    else:
                        log.critical(
                            "[%s] RECOVERY: UNRESOLVED | local_order_id=%s — operator review required",
                            self.client_id, local_order_id,
                        )
                    continue
                except Exception as _ptr_exc:
                    log.error(
                        "[%s] RECOVERY: PendingTriggerRestartRecovery error local_order_id=%s: %s "
                        "— falling back to direct watch()",
                        self.client_id, local_order_id, _ptr_exc,
                    )
                    # Fallback: original unconditional watch() for safety during rollout.
                    pass

                try:
                    armed = bool(self.entry_watcher.watch(plan, local_order_id))
                except Exception as exc:
                    log.error(
                        "[%s] RECOVERY: watcher reseed exception | local_order_id=%s signal_id=%s error=%s",
                        self.client_id, local_order_id, order.get("signal_id"), exc,
                    )
                    continue
                if armed:
                    rearmed += 1
                    log.info(
                        "[%s] RECOVERY: watcher re-armed | local_order_id=%s signal_id=%s contract=%s trigger=%s",
                        self.client_id,
                        local_order_id,
                        order.get("signal_id"),
                        getattr(plan, "contract_symbol", ""),
                        getattr(plan, "trigger_price", None),
                    )
                else:
                    log.warning(
                        "[%s] RECOVERY: watcher reseed failed | local_order_id=%s signal_id=%s reason=%s",
                        self.client_id,
                        local_order_id,
                        order.get("signal_id"),
                        getattr(self.entry_watcher, "_last_reject_reason", None),
                    )

        result["watchers_requeued"] = count + rearmed
        log.info(
            "[%s] RECOVERY: %d WATCHING signals reset to NEW and %d orphaned PENDING_TRIGGER orders re-armed "
            "for watcher reseed (lookback=%dh cutoff=%s)",
            self.client_id, count, rearmed, _lookback_hours, cutoff_utc[:19],
        )
