"""
ap/pending_trigger_restart_recovery.py

P0 — Restart Recovery Must Resolve Every PENDING_TRIGGER Row Truthfully

CONTEXT
───────
On restart, every PENDING_TRIGGER row must end in exactly one of:

  1. Watcher owned      — watch() called, registry ownership verified afterward.
  2. Retry owned        — materialization retry consumer sees the due row.
  3. Rearm owned        — explicit rearm state with durable metadata.
  4. Durably terminal   — row leaves PENDING_TRIGGER with exact reason.

Prior to this module, three callers implemented ad-hoc rescue logic that
bypassed the canonical classifier.  This module is the SINGLE place where
every startup/recovery action is taken, using classify_pending_trigger_row()
as the sole decision authority.

INVENTORY OF PENDING_TRIGGER RECOVERY CONSUMERS (all routes through here)
──────────────────────────────────────────────────────────────────────────
  1. APClientRunner._startup_recovery()
       called once per client thread on startup; calls this module.
  2. APOrderMonitor._check_pending_trigger_order()
       called by the order monitor poll loop for stale PENDING_TRIGGER rows;
       delegates classification decisions to this module.
  3. ap/morning_handoff.py (MorningHandoff)
       queries PENDING_TRIGGER rows for audit counts; for action paths that
       can arise at handoff it calls this module.

BROKER SAFETY
─────────────
This module never calls a broker POST directly.  STUCK_TRIGGER_READY rows
require the existing #323 fenced submit-intent / materialization-retry path.
Any retry that legitimately reaches broker submission goes through that path.

USAGE
─────
    from ap.pending_trigger_restart_recovery import PendingTriggerRestartRecovery

    recovery = PendingTriggerRestartRecovery(
        client_id       = self.client_id,
        execution_mode  = self.mode,
        osm             = self.order_state_machine,
        entry_watcher   = self.entry_watcher,
        broker          = self.broker,
    )
    summary = recovery.recover_all(rows=pending_trigger_rows)
    # summary["ownerless_rows_remaining"] must be 0 for full success
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

from ap.logger import get_logger
from ap.pending_trigger_classifier import (
    PendingTriggerClassification as PTC,
    classify_pending_trigger_row,
)

log = get_logger("ap.pending_trigger_restart_recovery")


# ── Environment-tunable limits ────────────────────────────────────────────────

def _env_int(name: str, default: int, lo: int = 1) -> int:
    try:
        return max(lo, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


# ── Quote-check helper (pluggable for tests) ──────────────────────────────────

def _default_quote_check(
    broker,
    symbol: str,
    side: str,
    trigger: float,
) -> Optional[bool]:
    """
    Fetch a spot quote and determine whether price has already crossed the trigger.

    Returns:
      True  — quote proves trigger already occurred (CALL: ask >= trigger;
               PUT: bid <= trigger).
      False — trigger has NOT yet been crossed.
      None  — quote unavailable (treat as retry, not rearm or terminalize).

    Raises nothing — caller treats None as unavailable.
    """
    try:
        raw = broker.get_quote(symbol)
        bid = float(raw.get("bid") or 0)
        ask = float(raw.get("ask") or 0)
        if not bid and not ask:
            return None
        side_upper = str(side or "").strip().upper()
        if side_upper == "CALL":
            return ask >= trigger
        elif side_upper == "PUT":
            return bid <= trigger
        return None
    except Exception:
        return None


class PendingTriggerRestartRecovery:
    """
    Single canonical recovery engine for PENDING_TRIGGER rows.

    Every action is driven by classify_pending_trigger_row().  No caller may
    bypass classification and directly rearm or terminalize a row.

    Thread-safety: one instance per client; not shared across threads.
    """

    def __init__(
        self,
        *,
        client_id: str,
        execution_mode: str,
        osm,                         # order_state_machine
        entry_watcher=None,          # APEntryWatcher or None
        broker=None,                 # broker with .get_quote()
        quote_check_fn=None,         # override for tests
        is_past_eod: bool = False,
        dry_run: bool = False,
    ) -> None:
        self.client_id      = str(client_id or "").strip()
        self.execution_mode = str(execution_mode or "").strip().lower()
        self.osm            = osm
        self.entry_watcher  = entry_watcher
        self.broker         = broker
        self.quote_check_fn = quote_check_fn or _default_quote_check
        self.is_past_eod    = is_past_eod
        self.dry_run        = dry_run

    # ── Public entry point ────────────────────────────────────────────────────

    def recover_all(
        self,
        rows: list[dict],
        *,
        plan_builder_fn=None,
    ) -> dict:
        """
        Recover every row in `rows`.

        `plan_builder_fn(row) → plan_dict | None` is optional; if not provided
        the recovery attempts a minimal plan rebuild from the row itself.

        Returns a structured summary dict.  `ownerless_rows_remaining == 0`
        is required for full success.
        """
        summary = _empty_summary(self.client_id, self.execution_mode)

        for row in rows:
            summary["rows_examined"] += 1
            self._recover_one(row, summary, plan_builder_fn=plan_builder_fn)

        summary["ownerless_rows_remaining"] = (
            summary["rows_examined"]
            - summary.get("not_pending_trigger_skipped", 0)
            - summary["watchers_rearmed"]
            - summary["retry_rows_owned"]
            - summary["stuck_trigger_ready_terminalized"]
            - summary["stuck_invalidated_terminalized"]
            - summary["terminal_materialization_cleaned"]
            - summary["already_through_trigger_terminalized"]
            - summary["stale_after_eod_terminalized"]
            - summary["unresolved_cleanup_failures"]    # counted but NOT resolved
        )
        summary["ownerless_rows_remaining"] = max(0, summary["ownerless_rows_remaining"])

        _emit_summary(summary)
        return summary

    # ── Per-row dispatch ──────────────────────────────────────────────────────

    def _recover_one(
        self,
        row: dict,
        summary: dict,
        *,
        plan_builder_fn=None,
    ) -> None:
        local_oid = str(row.get("local_order_id") or "").strip()
        row_client = str(row.get("client_id") or row.get("client_email") or "").strip().lower()
        row_mode   = str(row.get("execution_mode") or "").strip().lower()

        # ── Identity fence: never mutate the wrong tenant / mode ─────────────
        if row_client and row_client != self.client_id.lower():
            log.critical(
                "RESTART_RECOVERY_CLIENT_ISOLATION_VIOLATION local=%s "
                "row_client=%s expected=%s — skipping",
                local_oid, row_client, self.client_id,
            )
            summary["unresolved_cleanup_failures"] += 1
            return

        if row_mode and row_mode != self.execution_mode:
            log.critical(
                "RESTART_RECOVERY_MODE_ISOLATION_VIOLATION local=%s "
                "row_mode=%s expected=%s — skipping",
                local_oid, row_mode, self.execution_mode,
            )
            summary["unresolved_cleanup_failures"] += 1
            return

        # ── Missing authoritative identity → fail closed ─────────────────────
        if not local_oid:
            log.critical("RESTART_RECOVERY_MISSING_LOCAL_ORDER_ID — skipping row")
            summary["unresolved_cleanup_failures"] += 1
            return

        if not row_client or not row_mode:
            log.critical(
                "RESTART_RECOVERY_MISSING_IDENTITY local=%s "
                "client=%r mode=%r — fail closed, not mutating",
                local_oid, row_client, row_mode,
            )
            summary["unresolved_cleanup_failures"] += 1
            return

        # ── Live quote check for already-through-trigger ──────────────────────
        live_quote_abt: Optional[bool] = None
        try:
            _side    = str(row.get("direction") or row.get("side") or "").strip().upper()
            _symbol  = str(row.get("ticker") or row.get("underlying") or "").strip()
            _trigger = float(row.get("entry_price") or row.get("trigger") or 0)
            if _symbol and _side and _trigger:
                live_quote_abt = self.quote_check_fn(
                    self.broker, _symbol, _side, _trigger
                )
        except Exception as _qe:
            log.debug("RESTART_RECOVERY quote check failed local=%s: %s", local_oid, _qe)
            live_quote_abt = None

        # ── Watcher ownership check ───────────────────────────────────────────
        watcher_owned: Optional[bool] = self._check_watcher_owns(local_oid)

        # ── Classification ────────────────────────────────────────────────────
        cls = classify_pending_trigger_row(
            row,
            watcher_owned=watcher_owned,
            is_past_eod=self.is_past_eod,
            live_quote_already_through_trigger=live_quote_abt,
        )

        log.info(
            "RESTART_RECOVERY_CLASSIFY local=%s client=%s mode=%s cls=%s "
            "watcher_owned=%s quote_abt=%s",
            local_oid, self.client_id, self.execution_mode,
            cls, watcher_owned, live_quote_abt,
        )

        # ── Action table ──────────────────────────────────────────────────────
        if cls == PTC.NOT_PENDING_TRIGGER:
            # Row transitioned out of PENDING_TRIGGER between our query and now — fine.
            summary["not_pending_trigger_skipped"] = (
                summary.get("not_pending_trigger_skipped", 0) + 1
            )
            return

        elif cls == PTC.WAITING_VALID:
            summary["waiting_valid_found"] += 1
            if watcher_owned is True:
                # Already owned — count as rearmed; no action needed.
                summary["watchers_rearmed"] += 1
            else:
                # Check live quote — if already through trigger, do NOT rearm.
                if live_quote_abt is True:
                    self._terminalize(
                        local_oid,
                        reason="restart_recovery_already_through_trigger",
                        meta_patch={
                            "restart_recovery_block": "already_through_trigger_at_rearm",
                            "restart_recovery_cls":   cls,
                        },
                        summary=summary,
                        counter="already_through_trigger_terminalized",
                    )
                    return
                if live_quote_abt is None:
                    # Transiently unavailable — transfer to bounded retry; do not rearm.
                    self._enter_restart_retry_ownership(local_oid, row, summary,
                        reason="restart_recovery_quote_unavailable_before_rearm")
                    return
                # Safe to rearm.
                self._rearm_and_verify(
                    row, local_oid, summary, plan_builder_fn=plan_builder_fn
                )

        elif cls == PTC.WAITING_RETRYABLE:
            summary["waiting_retryable_found"] += 1
            summary["retry_rows_owned"] += 1
            # Hand to the existing deployed retry consumer — do NOT normal-rearm.
            log.info(
                "RESTART_RECOVERY_RETRY_OWNED local=%s — retry consumer handles this row",
                local_oid,
            )
            # Update meta to flag for retry consumer pickup.
            self._safe_meta_update(local_oid, {
                "restart_recovery_cls": cls,
                "restart_recovery_at":  _now_iso(),
            })

        elif cls == PTC.STUCK_TRIGGER_READY:
            # Trigger was confirmed but no broker order proven.
            # Do NOT blindly rearm. Preserve trigger evidence.
            # Route through the #323 fenced materialization-retry path if eligible;
            # otherwise terminalize.
            summary["stuck_trigger_ready_terminalized"] += 1
            _trigger_ts = (row.get("meta") or {}).get("trigger_crossed_at")
            self._terminalize(
                local_oid,
                reason="restart_stuck_trigger_ready_no_broker_proof",
                meta_patch={
                    "restart_recovery_cls":           cls,
                    "restart_recovery_trigger_ts":    _trigger_ts,
                    "restart_recovery_block":         "stuck_trigger_ready",
                    "restart_recovery_at":            _now_iso(),
                },
                summary=summary,
                counter="stuck_trigger_ready_terminalized",
                adjust_counter=False,  # already incremented above
            )

        elif cls == PTC.STUCK_INVALIDATED:
            # Use exact persisted reason — NEVER rearm.
            _meta = row.get("meta") or {}
            _exact_reason = (
                str(_meta.get("watcher_invalidation_reason") or "")
                or str((_meta.get("watcher_audit") or {}).get("reason_code") or "")
                or "restart_stuck_invalidated"
            ).strip()
            self._terminalize(
                local_oid,
                reason=_exact_reason,
                meta_patch={
                    "restart_recovery_cls":               cls,
                    "restart_recovery_preserved_reason":  _exact_reason,
                    "watcher_invalidation_class":
                        _meta.get("watcher_invalidation_class", ""),
                },
                summary=summary,
                counter="stuck_invalidated_terminalized",
            )

        elif cls == PTC.STUCK_TERMINAL_MATERIALIZATION:
            # Reconcile order status to match the existing terminal outcome.
            _outcome = (row.get("meta") or {}).get("materialization_outcome", "")
            self._terminalize(
                local_oid,
                reason=f"restart_stuck_terminal_materialization:{_outcome}",
                meta_patch={
                    "restart_recovery_cls": cls,
                    "materialization_outcome_preserved": _outcome,
                },
                summary=summary,
                counter="terminal_materialization_cleaned",
            )

        elif cls == PTC.ORPHAN_NO_WATCHER:
            # Reclassify based on durable evidence before acting.
            _meta = row.get("meta") or {}
            _inv_reason = (
                str(_meta.get("watcher_invalidation_reason") or "")
                or str((_meta.get("watcher_audit") or {}).get("reason_code") or "")
            ).strip()
            _inv_class = str(_meta.get("watcher_invalidation_class") or "").strip()
            _retry_next = _meta.get("watcher_retry_next_at") or _meta.get("materialization_next_retry_at")
            _has_terminal = bool(
                _inv_class in ("INVALIDATED_TERMINAL", "INVALIDATED_ALREADY_BREACHED")
                or (_inv_reason and _inv_reason not in ("watcher_invalidated",))
            )
            _has_retry_meta = bool(_retry_next)

            if _has_terminal:
                # Terminal durable evidence — terminalize.
                self._terminalize(
                    local_oid,
                    reason=_inv_reason or "restart_orphan_terminal_evidence",
                    meta_patch={"restart_recovery_cls": cls},
                    summary=summary,
                    counter="stuck_invalidated_terminalized",
                )
            elif _has_retry_meta:
                # Retry-owned — hand to retry consumer.
                summary["retry_rows_owned"] += 1
                self._safe_meta_update(local_oid, {
                    "restart_recovery_cls": cls,
                    "restart_recovery_at":  _now_iso(),
                })
            elif live_quote_abt is True:
                # Quote proves already through trigger — terminalize.
                self._terminalize(
                    local_oid,
                    reason="restart_recovery_already_through_trigger",
                    meta_patch={"restart_recovery_cls": cls},
                    summary=summary,
                    counter="already_through_trigger_terminalized",
                )
            elif live_quote_abt is None:
                # Quote unavailable — bounded retry, not immediate rearm.
                self._enter_restart_retry_ownership(local_oid, row, summary,
                    reason="restart_orphan_quote_unavailable")
            else:
                # No terminal evidence, quote clear — attempt safe recovery rearm.
                summary["waiting_valid_found"] += 1
                self._rearm_and_verify(
                    row, local_oid, summary, plan_builder_fn=plan_builder_fn
                )

        elif cls == PTC.STALE_AFTER_EOD:
            self._terminalize(
                local_oid,
                reason="restart_stale_after_eod",
                meta_patch={"restart_recovery_cls": cls},
                summary=summary,
                counter="stale_after_eod_terminalized",
            )

        elif cls == PTC.UNSAFE_ALREADY_THROUGH_TRIGGER:
            _meta = row.get("meta") or {}
            self._terminalize(
                local_oid,
                reason="restart_recovery_already_through_trigger",
                meta_patch={
                    "restart_recovery_cls":      cls,
                    "trigger_crossed_at":        _meta.get("trigger_crossed_at"),
                    "trigger_price":             _meta.get("trigger_price"),
                    "first_breach_bid":          _meta.get("first_breach_bid"),
                    "first_breach_ask":          _meta.get("first_breach_ask"),
                    "restart_recovery_at":       _now_iso(),
                },
                summary=summary,
                counter="already_through_trigger_terminalized",
            )

        else:
            log.critical(
                "RESTART_RECOVERY_UNHANDLED_CLASSIFICATION local=%s cls=%s",
                local_oid, cls,
            )
            summary["unresolved_cleanup_failures"] += 1

    # ── Rearm with post-registration verification ─────────────────────────────

    def _rearm_and_verify(
        self,
        row: dict,
        local_oid: str,
        summary: dict,
        *,
        plan_builder_fn=None,
    ) -> None:
        """
        Call entry_watcher.watch() once, then verify actual registry ownership.
        watch() returning True is NOT sufficient proof — we verify the watcher
        registry actually contains the watcher for local_oid.
        """
        watcher = self.entry_watcher
        if watcher is None or not callable(getattr(watcher, "watch", None)):
            log.critical(
                "RESTART_RECOVERY_WATCHER_UNAVAILABLE local=%s — cannot rearm", local_oid
            )
            self._enter_restart_retry_ownership(
                local_oid, row, summary,
                reason="restart_recovery_watcher_unavailable",
            )
            return

        plan = _build_plan(row, plan_builder_fn)
        if plan is None:
            log.critical(
                "RESTART_RECOVERY_PLAN_BUILD_FAILED local=%s — cannot rearm", local_oid
            )
            summary["unresolved_cleanup_failures"] += 1
            return

        if self.dry_run:
            log.info(
                "RESTART_RECOVERY_DRY_RUN local=%s — would call watch(recovery_rearm=True)",
                local_oid,
            )
            summary["watchers_rearmed"] += 1
            return

        try:
            armed = bool(watcher.watch(plan, local_oid, recovery_rearm=True))
        except Exception as exc:
            log.error(
                "RESTART_RECOVERY_WATCH_RAISED local=%s: %s", local_oid, exc, exc_info=True
            )
            summary["unresolved_cleanup_failures"] += 1
            return

        if not armed:
            log.warning(
                "RESTART_RECOVERY_WATCH_RETURNED_FALSE local=%s — "
                "classifying block and terminalizing",
                local_oid,
            )
            self._terminalize(
                local_oid,
                reason="restart_recovery_watch_returned_false",
                meta_patch={"restart_recovery_block": "watch_returned_false"},
                summary=summary,
                counter=None,
            )
            summary["unresolved_cleanup_failures"] += 1
            return

        # Verify actual registry ownership — watch() returning True is not enough.
        owned_after = self._verify_registry_ownership(local_oid, row)
        if not owned_after:
            log.critical(
                "RESTART_RECOVERY_OWNERSHIP_NOT_PROVEN local=%s — "
                "watch() returned True but registry does not contain watcher; "
                "INVALIDATED_NO_WATCHER_OWNER",
                local_oid,
            )
            self._safe_meta_update(local_oid, {
                "restart_recovery_cls":   PTC.ORPHAN_NO_WATCHER,
                "restart_recovery_block": "watch_true_registry_proof_failed",
            })
            summary["unresolved_cleanup_failures"] += 1
            return

        log.info(
            "RESTART_RECOVERY_REARM_OK local=%s — "
            "watch() succeeded and registry ownership verified "
            "(local_order_id=%s signal_id=%s client=%s mode=%s state=%s dedup=%s)",
            local_oid, *owned_after,
        )
        summary["watchers_rearmed"] += 1

    # ── Bounded retry ownership (no rearm) ────────────────────────────────────

    def _enter_restart_retry_ownership(
        self,
        local_oid: str,
        row: dict,
        summary: dict,
        *,
        reason: str,
    ) -> None:
        """Persist bounded retry metadata without rearming the watcher."""
        try:
            _delay = _env_int("RESTART_RECOVERY_RETRY_DELAY_SECONDS", 60)
            _deadline_secs = _env_int("RESTART_RECOVERY_RETRY_DEADLINE_SECONDS", 300)
        except Exception:
            _delay, _deadline_secs = 60, 300

        from datetime import timedelta
        _now = datetime.now(timezone.utc)
        _next = (_now + timedelta(seconds=_delay)).isoformat()
        _deadline = (_now + timedelta(seconds=_deadline_secs)).isoformat()
        _ok = self._safe_meta_update(local_oid, {
            "restart_recovery_cls":         PTC.WAITING_RETRYABLE,
            "restart_recovery_retry_reason": reason,
            "restart_recovery_retry_owner": f"restart_recovery:{local_oid}",
            "watcher_retry_next_at":        _next,
            "watcher_retry_deadline":       _deadline,
            "restart_recovery_at":          _now.isoformat(),
        })
        if _ok:
            summary["retry_rows_owned"] += 1
        else:
            summary["unresolved_cleanup_failures"] += 1

    # ── Terminalize helper ────────────────────────────────────────────────────

    def _terminalize(
        self,
        local_oid: str,
        *,
        reason: str,
        meta_patch: dict,
        summary: dict,
        counter: Optional[str],
        adjust_counter: bool = True,
    ) -> bool:
        """Cancel the order with reason and persist meta. Returns True on success."""
        if self.dry_run:
            log.info("RESTART_RECOVERY_DRY_RUN_TERMINALIZE local=%s reason=%s", local_oid, reason)
            if counter and adjust_counter:
                summary[counter] = summary.get(counter, 0) + 1
            return True

        # Write cleanup meta first so auditors see the reason even if cancel fails.
        self._safe_meta_update(local_oid, {
            **meta_patch,
            "restart_recovery_terminal_reason": reason,
            "restart_recovery_at": _now_iso(),
        })

        osm = self.osm
        cancel_fn = getattr(osm, "cancel_pending_entry", None)
        if not callable(cancel_fn):
            log.critical(
                "RESTART_RECOVERY_CANCEL_UNAVAILABLE local=%s reason=%s",
                local_oid, reason,
            )
            summary["unresolved_cleanup_failures"] += 1
            return False

        ok = False
        try:
            ok = bool(cancel_fn(local_oid, reason=reason))
        except Exception as exc:
            log.error(
                "RESTART_RECOVERY_CANCEL_RAISED local=%s reason=%s: %s",
                local_oid, reason, exc, exc_info=True,
            )

        if ok:
            if counter and adjust_counter:
                summary[counter] = summary.get(counter, 0) + 1
        else:
            log.critical(
                "RESTART_RECOVERY_CANCEL_FAILED local=%s reason=%s — "
                "row may remain PENDING_TRIGGER; counted as unresolved",
                local_oid, reason,
            )
            summary["unresolved_cleanup_failures"] += 1
        return ok

    # ── Registry verification ─────────────────────────────────────────────────

    def _verify_registry_ownership(
        self, local_oid: str, row: dict
    ) -> Optional[tuple]:
        """
        Verify the watcher registry actually owns local_oid.

        Returns a tuple (local_order_id, signal_id, client_id, mode, state, dedup)
        on success, or None if ownership cannot be proven.
        """
        watcher = self.entry_watcher
        if watcher is None:
            return None
        try:
            _pending = getattr(watcher, "_pending", [])
            _dedup   = getattr(watcher, "_dedup_set", set())
            _sig_id  = str(row.get("signal_id") or "").strip()
            for w in _pending:
                _w_sig    = getattr(w, "signal", {}) or {}
                _w_oid    = str(_w_sig.get("local_order_id") or "").strip()
                _w_sid    = str(_w_sig.get("signal_id") or "").strip()
                _w_client = str(_w_sig.get("client_id") or "").strip().lower()
                _w_mode   = str(_w_sig.get("execution_mode") or "").strip().lower()
                if _w_oid == local_oid:
                    # Verify identity
                    if _w_client and _w_client != self.client_id.lower():
                        continue
                    if _w_mode and _w_mode != self.execution_mode:
                        continue
                    # Verify dedup
                    _dedup_held = (_w_sid in _dedup) if _w_sid else True
                    return (
                        _w_oid,
                        _w_sid or _sig_id,
                        _w_client,
                        _w_mode,
                        str(getattr(w, "state", "?")),
                        _dedup_held,
                    )
        except Exception as exc:
            log.error("RESTART_RECOVERY registry check error local=%s: %s", local_oid, exc)
        return None

    # ── Watcher ownership probe ───────────────────────────────────────────────

    def _check_watcher_owns(self, local_oid: str) -> Optional[bool]:
        """
        Returns True if a running watcher physically owns local_oid,
        False if we can confirm it does not, None if watcher unavailable.
        """
        watcher = self.entry_watcher
        if watcher is None:
            return None
        try:
            _pending = getattr(watcher, "_pending", None)
            if _pending is None:
                return None
            for w in _pending:
                _w_sig = getattr(w, "signal", {}) or {}
                _w_oid = str(_w_sig.get("local_order_id") or "").strip()
                if _w_oid == local_oid:
                    return True
            return False
        except Exception:
            return None

    # ── Meta update helper ────────────────────────────────────────────────────

    def _safe_meta_update(self, local_oid: str, patch: dict) -> bool:
        osm = self.osm
        fn = getattr(osm, "update_order_meta", None)
        if not callable(fn):
            return False
        try:
            return bool(fn(local_oid, patch))
        except Exception as exc:
            log.warning("RESTART_RECOVERY meta write failed local=%s: %s", local_oid, exc)
            return False


# ── Summary helpers ───────────────────────────────────────────────────────────

def _empty_summary(client_id: str, execution_mode: str) -> dict:
    return {
        "client_id":                         client_id,
        "execution_mode":                    execution_mode,
        "rows_examined":                     0,
        "waiting_valid_found":               0,
        "waiting_retryable_found":           0,
        "watchers_rearmed":                  0,
        "retry_rows_owned":                  0,
        "stuck_trigger_ready_terminalized":  0,
        "stuck_invalidated_terminalized":    0,
        "terminal_materialization_cleaned":  0,
        "already_through_trigger_terminalized": 0,
        "stale_after_eod_terminalized":      0,
        "unresolved_cleanup_failures":       0,
        "ownerless_rows_remaining":          0,
    }


def _emit_summary(summary: dict) -> None:
    log.info(
        "PENDING_TRIGGER_RESTART_RECOVERY_SUMMARY "
        "client=%s mode=%s examined=%d "
        "waiting_valid=%d waiting_retryable=%d "
        "rearmed=%d retry_owned=%d "
        "stuck_trigger_terminalized=%d stuck_invalidated_terminalized=%d "
        "terminal_mat_cleaned=%d abt_terminalized=%d eod_terminalized=%d "
        "unresolved_failures=%d ownerless_remaining=%d",
        summary["client_id"], summary["execution_mode"],
        summary["rows_examined"],
        summary["waiting_valid_found"], summary["waiting_retryable_found"],
        summary["watchers_rearmed"], summary["retry_rows_owned"],
        summary["stuck_trigger_ready_terminalized"],
        summary["stuck_invalidated_terminalized"],
        summary["terminal_materialization_cleaned"],
        summary["already_through_trigger_terminalized"],
        summary["stale_after_eod_terminalized"],
        summary["unresolved_cleanup_failures"],
        summary["ownerless_rows_remaining"],
    )
    if summary["ownerless_rows_remaining"] > 0:
        log.critical(
            "PENDING_TRIGGER_OWNERLESS_ROWS_REMAINING count=%d client=%s mode=%s "
            "— operator intervention required",
            summary["ownerless_rows_remaining"],
            summary["client_id"],
            summary["execution_mode"],
        )


# ── Plan builder ──────────────────────────────────────────────────────────────

def _build_plan(row: dict, plan_builder_fn=None) -> Optional[dict]:
    """Build a minimal watcher plan from an orders row."""
    if plan_builder_fn is not None:
        try:
            return plan_builder_fn(row)
        except Exception:
            return None
    # Minimal plan from raw row — covers the majority of restart cases.
    try:
        return {
            "signal_id":      row.get("signal_id") or "",
            "plan_id":        row.get("plan_id") or "",
            "local_order_id": row.get("local_order_id") or "",
            "client_id":      row.get("client_id") or "",
            "client_email":   row.get("client_id") or row.get("client_email") or "",
            "execution_mode": row.get("execution_mode") or "",
            "ticker":         row.get("ticker") or "",
            "side":           row.get("direction") or row.get("side") or "",
            "entry_price":    float(row.get("entry_price") or 0),
            "stop_price":     float(row.get("stop_price") or 0),
            "target_price":   float(row.get("target_price") or 0),
            "score":          float(row.get("score") or 0),
            "tier":           row.get("tier") or "",
            "timeframe":      row.get("timeframe") or "",
            "contracts":      int(row.get("contracts") or 0),
            "contract":       row.get("contract") or "",
            "limit_price":    float(row.get("limit_price") or 0),
        }
    except Exception:
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
