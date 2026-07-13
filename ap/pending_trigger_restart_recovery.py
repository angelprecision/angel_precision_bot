"""
ap/pending_trigger_restart_recovery.py

P0 — Restart Recovery Must Resolve Every PENDING_TRIGGER Row Truthfully

CONSUMER INVENTORY — all restart/recovery action paths wire through here:
  1. ap_recovery.APStartupRecovery._reseed_watchers()
       → called by client_runner._run_startup_recovery() at startup
       → called by morning_handoff via APStartupRecovery._reseed_watchers()
  2. ap.order_monitor.APOrderMonitor._check_pending_trigger_order()
       → called by the order monitor poll loop for stale PENDING_TRIGGER rows

classify_pending_trigger_row() is the SOLE decision authority for every row.
No caller bypasses classification.  No ad-hoc rearm logic survives.

BLOCKER FIXES (PR #328 amendment):
  B2: per-row outcome dict replaces arithmetic; ownerless = count(UNRESOLVED)
  B3: retry ownership uses #323 canonical fields; consumer queries proven
  B4: registry proof requires 6-way identity + state + dedup; structured result
  B5: terminalize rereads order and verifies terminal status before counting
  B6: watch() failure → terminalize OR unresolved, never both
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from ap.logger import get_logger
from ap.pending_trigger_classifier import (
    PendingTriggerClassification as PTC,
    classify_pending_trigger_row,
)

log = get_logger("ap.pending_trigger_restart_recovery")


# ── Per-row outcome constants (Blocker 2) ─────────────────────────────────────

class _RowOutcome:
    WATCHER_OWNED = "WATCHER_OWNED"
    RETRY_OWNED   = "RETRY_OWNED"
    REARM_OWNED   = "REARM_OWNED"
    TERMINALIZED  = "TERMINALIZED"
    UNRESOLVED    = "UNRESOLVED"
    SKIPPED       = "SKIPPED"   # NOT_PENDING_TRIGGER (already resolved)


_TERMINAL_STATUSES = frozenset({"CANCELED", "EXPIRED", "REJECTED", "ERROR"})

# #323 canonical materialization retry fields (Blocker 3)
_MAT_STATUS_FIELD    = "materialization_status"
_MAT_NEXT_RETRY_AT   = "materialization_next_retry_at"
_MAT_RETRY_DEADLINE  = "materialization_retry_deadline"
_MAT_ATTEMPT_COUNT   = "materialization_attempt_count"
_MAT_OWNER_FIELD     = "materialization_owner"
_MAT_RETRY_REASON    = "materialization_retry_reason"


# ── Environment-tunable limits ────────────────────────────────────────────────

def _env_int(name: str, default: int, lo: int = 1) -> int:
    try:
        return max(lo, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


# ── Quote-check helper (pluggable for tests) ──────────────────────────────────

def _default_quote_check(broker, symbol: str, side: str, trigger: float) -> Optional[bool]:
    """True=already through trigger; False=not; None=unavailable."""
    try:
        raw = broker.get_quote(symbol) or {}
        bid = float(raw.get("bid") or 0)
        ask = float(raw.get("ask") or 0)
        if not bid and not ask:
            return None
        if str(side or "").strip().upper() == "CALL":
            return ask >= trigger
        elif str(side or "").strip().upper() == "PUT":
            return bid <= trigger
        return None
    except Exception:
        return None


class PendingTriggerRestartRecovery:
    """
    Single canonical recovery engine for PENDING_TRIGGER rows.

    Every action is driven by classify_pending_trigger_row().
    Every outcome is individually tracked; ownerless = count(UNRESOLVED).
    """

    def __init__(
        self,
        *,
        client_id: str,
        execution_mode: str,
        osm,
        entry_watcher=None,
        broker=None,
        quote_check_fn=None,
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

    def recover_all(self, rows: list[dict], *, plan_builder_fn=None) -> dict:
        """
        Recover every row in `rows`.
        Returns a structured summary.  `ownerless_rows_remaining == 0` required.
        """
        # Blocker 2: per-row outcome tracking; no arithmetic shortcuts.
        _outcomes: dict[str, str] = {}   # local_order_id → _RowOutcome constant

        for row in rows:
            local_oid = str(row.get("local_order_id") or "").strip()
            outcome = self._recover_one(row, plan_builder_fn=plan_builder_fn)
            _outcomes[local_oid or id(row)] = outcome

        summary = _build_summary(self.client_id, self.execution_mode, _outcomes)
        _emit_summary(summary)
        return summary

    def recover_one_row(self, row: dict, *, plan_builder_fn=None) -> str:
        """Single-row entry point for callers that iterate rows themselves.
        Returns the _RowOutcome constant."""
        return self._recover_one(row, plan_builder_fn=plan_builder_fn)

    # ── Per-row dispatch ──────────────────────────────────────────────────────

    def _recover_one(self, row: dict, *, plan_builder_fn=None) -> str:
        local_oid  = str(row.get("local_order_id") or "").strip()
        row_client = str(row.get("client_id") or row.get("client_email") or "").strip().lower()
        row_mode   = str(row.get("execution_mode") or "").strip().lower()

        # Identity fence: never mutate the wrong tenant / mode.
        if row_client and row_client != self.client_id.lower():
            log.critical(
                "RESTART_RECOVERY_CLIENT_ISOLATION_VIOLATION local=%s row=%s expected=%s",
                local_oid, row_client, self.client_id,
            )
            return _RowOutcome.UNRESOLVED

        if row_mode and row_mode != self.execution_mode:
            log.critical(
                "RESTART_RECOVERY_MODE_ISOLATION_VIOLATION local=%s row_mode=%s expected=%s",
                local_oid, row_mode, self.execution_mode,
            )
            return _RowOutcome.UNRESOLVED

        # Missing authoritative identity → fail closed.
        if not local_oid or not row_client or not row_mode:
            log.critical(
                "RESTART_RECOVERY_MISSING_IDENTITY local=%r client=%r mode=%r — fail closed",
                local_oid, row_client, row_mode,
            )
            return _RowOutcome.UNRESOLVED

        # Live quote check.
        live_quote_abt: Optional[bool] = None
        try:
            _side    = str(row.get("direction") or row.get("side") or "").strip().upper()
            _symbol  = str(row.get("ticker") or row.get("underlying") or row.get("symbol") or "").strip()
            _trigger = float(row.get("entry_price") or row.get("trigger_price") or row.get("trigger") or 0)
            if _symbol and _side and _trigger:
                live_quote_abt = self.quote_check_fn(self.broker, _symbol, _side, _trigger)
        except Exception as _qe:
            log.debug("RESTART_RECOVERY quote check failed local=%s: %s", local_oid, _qe)

        # Registry ownership check.
        watcher_owned: Optional[bool] = self._check_watcher_owns(local_oid)

        # Classification.
        cls = classify_pending_trigger_row(
            row,
            watcher_owned=watcher_owned,
            is_past_eod=self.is_past_eod,
            live_quote_already_through_trigger=live_quote_abt,
        )

        log.info(
            "RESTART_RECOVERY_CLASSIFY local=%s cls=%s watcher_owned=%s quote_abt=%s",
            local_oid, cls, watcher_owned, live_quote_abt,
        )

        # Action table.
        if cls == PTC.NOT_PENDING_TRIGGER:
            return _RowOutcome.SKIPPED

        elif cls == PTC.WAITING_VALID:
            if watcher_owned is True:
                # Already owned — verify proof then count.
                proof = self._verify_registry_ownership(local_oid, row)
                if proof and proof.get("dedup_held"):
                    return _RowOutcome.WATCHER_OWNED
                # Proof failed despite watcher reporting owned — treat as orphan.
                log.warning(
                    "RESTART_RECOVERY watcher_owned=True but proof failed local=%s — "
                    "treating as orphan", local_oid,
                )
            # Not owned or proof failed: quote check then rearm.
            if live_quote_abt is True:
                return self._terminalize_with_reason(
                    local_oid, row, "restart_recovery_already_through_trigger",
                    meta_patch={"restart_recovery_cls": cls},
                )
            if live_quote_abt is None:
                return self._enter_canonical_retry(local_oid, row,
                    reason="restart_recovery_quote_unavailable_before_rearm")
            return self._rearm_and_verify(row, local_oid, plan_builder_fn=plan_builder_fn)

        elif cls == PTC.WAITING_RETRYABLE:
            # Hand to #323 deployed consumer.  Do not normal-rearm.
            # Blocker 3: verify existing canonical retry fields are present.
            _meta = row.get("meta") or {}
            _have_status   = str(_meta.get(_MAT_STATUS_FIELD) or "").strip().upper() == "RETRY_PENDING"
            _have_next_at  = bool(_meta.get(_MAT_NEXT_RETRY_AT))
            if _have_status and _have_next_at:
                log.info(
                    "RESTART_RECOVERY_RETRY_OWNED local=%s — #323 consumer will pick up",
                    local_oid,
                )
                self._safe_meta_update(local_oid, {
                    "restart_recovery_cls": cls,
                    "restart_recovery_at":  _now_iso(),
                })
                return _RowOutcome.RETRY_OWNED
            else:
                # Canonical fields missing — need to set them.
                return self._enter_canonical_retry(local_oid, row,
                    reason="restart_recovery_retryable_missing_canonical_fields")

        elif cls == PTC.STUCK_TRIGGER_READY:
            _meta = row.get("meta") or {}
            return self._terminalize_with_reason(
                local_oid, row,
                "restart_stuck_trigger_ready_no_broker_proof",
                meta_patch={
                    "restart_recovery_cls":        cls,
                    "restart_recovery_trigger_ts": _meta.get("trigger_crossed_at"),
                },
            )

        elif cls == PTC.STUCK_INVALIDATED:
            _meta = row.get("meta") or {}
            _exact = (
                str(_meta.get("watcher_invalidation_reason") or "")
                or str((_meta.get("watcher_audit") or {}).get("reason_code") or "")
                or "restart_stuck_invalidated"
            ).strip()
            return self._terminalize_with_reason(
                local_oid, row, _exact,
                meta_patch={
                    "restart_recovery_cls":               cls,
                    "restart_recovery_preserved_reason":  _exact,
                    "watcher_invalidation_class":
                        (_meta.get("meta") or _meta).get("watcher_invalidation_class", ""),
                },
            )

        elif cls == PTC.STUCK_TERMINAL_MATERIALIZATION:
            _outcome = (row.get("meta") or {}).get("materialization_outcome", "")
            return self._terminalize_with_reason(
                local_oid, row,
                f"restart_stuck_terminal_materialization:{_outcome}",
                meta_patch={"restart_recovery_cls": cls,
                            "materialization_outcome_preserved": _outcome},
            )

        elif cls == PTC.ORPHAN_NO_WATCHER:
            return self._handle_orphan(row, local_oid, live_quote_abt, plan_builder_fn)

        elif cls == PTC.STALE_AFTER_EOD:
            return self._terminalize_with_reason(
                local_oid, row, "restart_stale_after_eod",
                meta_patch={"restart_recovery_cls": cls},
            )

        elif cls == PTC.UNSAFE_ALREADY_THROUGH_TRIGGER:
            _meta = row.get("meta") or {}
            return self._terminalize_with_reason(
                local_oid, row, "restart_recovery_already_through_trigger",
                meta_patch={
                    "restart_recovery_cls":   cls,
                    "trigger_crossed_at":     _meta.get("trigger_crossed_at"),
                    "trigger_price":          _meta.get("trigger_price"),
                    "first_breach_bid":       _meta.get("first_breach_bid"),
                    "first_breach_ask":       _meta.get("first_breach_ask"),
                },
            )

        else:
            log.critical(
                "RESTART_RECOVERY_UNHANDLED_CLASSIFICATION local=%s cls=%s",
                local_oid, cls,
            )
            return _RowOutcome.UNRESOLVED

    # ── Orphan reclassification ───────────────────────────────────────────────

    def _handle_orphan(self, row, local_oid, live_quote_abt, plan_builder_fn) -> str:
        _meta = row.get("meta") or {}
        _inv_reason = (
            str(_meta.get("watcher_invalidation_reason") or "")
            or str((_meta.get("watcher_audit") or {}).get("reason_code") or "")
        ).strip()
        _inv_class = str(_meta.get("watcher_invalidation_class") or "").strip()
        _has_terminal = bool(
            _inv_class in ("INVALIDATED_TERMINAL", "INVALIDATED_ALREADY_BREACHED")
            or (_inv_reason and _inv_reason not in ("watcher_invalidated",))
        )
        _mat_status = str(_meta.get(_MAT_STATUS_FIELD) or "").strip().upper()
        _mat_next   = _meta.get(_MAT_NEXT_RETRY_AT)
        _has_canonical_retry = (_mat_status == "RETRY_PENDING" and _mat_next)

        if _has_terminal:
            return self._terminalize_with_reason(
                local_oid, row,
                _inv_reason or "restart_orphan_terminal_evidence",
                meta_patch={"restart_recovery_cls": PTC.ORPHAN_NO_WATCHER},
            )
        if _has_canonical_retry:
            self._safe_meta_update(local_oid, {
                "restart_recovery_cls": PTC.ORPHAN_NO_WATCHER,
                "restart_recovery_at":  _now_iso(),
            })
            return _RowOutcome.RETRY_OWNED
        if live_quote_abt is True:
            return self._terminalize_with_reason(
                local_oid, row,
                "restart_recovery_already_through_trigger",
                meta_patch={"restart_recovery_cls": PTC.ORPHAN_NO_WATCHER},
            )
        if live_quote_abt is None:
            return self._enter_canonical_retry(local_oid, row,
                reason="restart_orphan_quote_unavailable")
        # Quote clear, no terminal evidence → safe recovery rearm.
        return self._rearm_and_verify(row, local_oid, plan_builder_fn=plan_builder_fn)

    # ── Rearm + post-registration verification ────────────────────────────────

    def _rearm_and_verify(self, row: dict, local_oid: str, *, plan_builder_fn=None) -> str:
        """
        Call watch() once, then verify actual registry ownership.
        watch() returning True is NOT proof.

        Blocker 6: watch() failure → terminalize OR unresolved, never both.
        """
        watcher = self.entry_watcher
        if watcher is None or not callable(getattr(watcher, "watch", None)):
            log.critical("RESTART_RECOVERY_WATCHER_UNAVAILABLE local=%s", local_oid)
            return self._enter_canonical_retry(local_oid, row,
                reason="restart_recovery_watcher_unavailable")

        plan = _build_plan(row, plan_builder_fn)
        if plan is None:
            log.critical("RESTART_RECOVERY_PLAN_BUILD_FAILED local=%s", local_oid)
            return _RowOutcome.UNRESOLVED

        if self.dry_run:
            log.info("RESTART_RECOVERY_DRY_RUN local=%s — would watch(recovery_rearm=True)", local_oid)
            return _RowOutcome.WATCHER_OWNED

        try:
            armed = bool(watcher.watch(plan, local_oid, recovery_rearm=True))
        except Exception as exc:
            log.error("RESTART_RECOVERY_WATCH_RAISED local=%s: %s", local_oid, exc, exc_info=True)
            return _RowOutcome.UNRESOLVED

        if not armed:
            log.warning(
                "RESTART_RECOVERY_WATCH_RETURNED_FALSE local=%s — terminalizing",
                local_oid,
            )
            # Blocker 6: terminalize OR unresolved, never both.
            outcome = self._terminalize_with_reason(
                local_oid, row,
                "restart_recovery_watch_returned_false",
                meta_patch={"restart_recovery_block": "watch_returned_false"},
            )
            return outcome  # already TERMINALIZED or UNRESOLVED from _terminalize

        # Blocker 4: verify actual registry ownership — 6-way proof required.
        proof = self._verify_registry_ownership(local_oid, row)
        if proof is None:
            log.critical(
                "RESTART_RECOVERY_OWNERSHIP_NOT_PROVEN local=%s — "
                "watch() returned True but full registry proof failed",
                local_oid,
            )
            self._safe_meta_update(local_oid, {
                "restart_recovery_cls":   PTC.ORPHAN_NO_WATCHER,
                "restart_recovery_block": "watch_true_registry_proof_failed",
            })
            return _RowOutcome.UNRESOLVED

        log.info(
            "RESTART_RECOVERY_REARM_OK local=%s proof=%s",
            local_oid,
            {k: v for k, v in proof.items() if k != "watcher_obj"},
        )
        return _RowOutcome.WATCHER_OWNED

    # ── Canonical retry ownership (#323 fields) ────────────────────────────────

    def _enter_canonical_retry(self, local_oid: str, row: dict, *, reason: str) -> str:
        """
        Blocker 3: write #323 canonical materialization retry fields.

        The deployed consumer in APStartupRecovery.recover_deferred_lifecycles()
        queries rows where meta.materialization_status = 'RETRY_PENDING' and
        meta.materialization_next_retry_at is due.  This method writes exactly
        those fields so the #323 consumer sees the row.
        """
        _delay = _env_int("RESTART_RECOVERY_RETRY_DELAY_SECONDS", 60)
        _deadline_secs = _env_int("RESTART_RECOVERY_RETRY_DEADLINE_SECONDS", 300)
        _now = datetime.now(timezone.utc)
        _next = (_now + timedelta(seconds=_delay)).isoformat()
        _deadline = (_now + timedelta(seconds=_deadline_secs)).isoformat()
        _owner = f"restart_recovery:{local_oid}"

        # Read existing attempt count from canonical fields.
        _meta = row.get("meta") or {}
        _attempts = int(_meta.get(_MAT_ATTEMPT_COUNT) or 0) + 1
        _max = _env_int("RESTART_RECOVERY_RETRY_MAX_ATTEMPTS", 3)
        if _attempts > _max:
            log.warning(
                "RESTART_RECOVERY_RETRY_EXHAUSTED local=%s attempts=%d max=%d — terminalizing",
                local_oid, _attempts, _max,
            )
            return self._terminalize_with_reason(
                local_oid, row,
                f"restart_recovery_retry_exhausted:{reason}",
                meta_patch={"restart_recovery_retry_attempts": _attempts},
            )

        _ok = self._safe_meta_update(local_oid, {
            _MAT_STATUS_FIELD:  "RETRY_PENDING",
            _MAT_NEXT_RETRY_AT: _next,
            _MAT_RETRY_DEADLINE: _deadline,
            _MAT_ATTEMPT_COUNT: _attempts,
            _MAT_OWNER_FIELD:   _owner,
            _MAT_RETRY_REASON:  reason,
            "restart_recovery_at":  _now.isoformat(),
        })
        if not _ok:
            log.critical(
                "RESTART_RECOVERY_CANONICAL_RETRY_WRITE_FAILED local=%s — UNRESOLVED",
                local_oid,
            )
            return _RowOutcome.UNRESOLVED

        log.info(
            "RESTART_RECOVERY_RETRY_OWNED local=%s next_at=%s deadline=%s reason=%s",
            local_oid, _next, _deadline, reason,
        )
        return _RowOutcome.RETRY_OWNED

    # ── Terminalize with reread verification (Blocker 5) ─────────────────────

    def _terminalize_with_reason(
        self,
        local_oid: str,
        row: dict,
        reason: str,
        *,
        meta_patch: Optional[dict] = None,
    ) -> str:
        """
        Cancel the order then REREAD to verify terminal status before counting.

        Blocker 5: boolean return from helper is not sufficient; reread required.
        """
        if self.dry_run:
            log.info("RESTART_RECOVERY_DRY_RUN_TERMINALIZE local=%s reason=%s", local_oid, reason)
            return _RowOutcome.TERMINALIZED

        # Write reason meta before cancel so auditors see it even if cancel fails.
        _patch = dict(meta_patch or {})
        _patch.update({
            "restart_recovery_terminal_reason": reason,
            "restart_recovery_at": _now_iso(),
        })
        self._safe_meta_update(local_oid, _patch)

        osm = self.osm
        cancel_fn = getattr(osm, "cancel_pending_entry", None)
        if not callable(cancel_fn):
            log.critical("RESTART_RECOVERY_CANCEL_UNAVAILABLE local=%s reason=%s", local_oid, reason)
            return _RowOutcome.UNRESOLVED

        _cancel_ok = False
        try:
            _cancel_ok = bool(cancel_fn(local_oid, reason=reason))
        except Exception as exc:
            log.error(
                "RESTART_RECOVERY_CANCEL_RAISED local=%s reason=%s: %s",
                local_oid, reason, exc, exc_info=True,
            )
            return _RowOutcome.UNRESOLVED

        if not _cancel_ok:
            log.critical("RESTART_RECOVERY_CANCEL_RETURNED_FALSE local=%s reason=%s", local_oid, reason)
            return _RowOutcome.UNRESOLVED

        # Blocker 5: reread and verify identity + terminal status.
        get_fn = getattr(osm, "get_order", None)
        if not callable(get_fn):
            log.warning(
                "RESTART_RECOVERY_REREAD_UNAVAILABLE local=%s — "
                "cancel returned True but cannot verify; counting as UNRESOLVED",
                local_oid,
            )
            return _RowOutcome.UNRESOLVED

        try:
            reread = get_fn(local_oid)
        except Exception as exc:
            log.critical(
                "RESTART_RECOVERY_REREAD_RAISED local=%s: %s — UNRESOLVED",
                local_oid, exc,
            )
            return _RowOutcome.UNRESOLVED

        if reread is None:
            log.critical("RESTART_RECOVERY_REREAD_NONE local=%s — UNRESOLVED", local_oid)
            return _RowOutcome.UNRESOLVED

        _rr_status = str(reread.get("status") or "").strip().upper()
        _rr_oid    = str(reread.get("local_order_id") or "").strip()
        _rr_client = str(reread.get("client_id") or reread.get("client_email") or "").strip().lower()
        _rr_mode   = str(reread.get("execution_mode") or "").strip().lower()

        if _rr_oid and _rr_oid != local_oid:
            log.critical("RESTART_RECOVERY_REREAD_OID_MISMATCH local=%s got=%s", local_oid, _rr_oid)
            return _RowOutcome.UNRESOLVED
        if _rr_client and _rr_client != self.client_id.lower():
            log.critical("RESTART_RECOVERY_REREAD_CLIENT_MISMATCH local=%s got=%s", local_oid, _rr_client)
            return _RowOutcome.UNRESOLVED
        if _rr_mode and _rr_mode != self.execution_mode:
            log.critical("RESTART_RECOVERY_REREAD_MODE_MISMATCH local=%s got=%s", local_oid, _rr_mode)
            return _RowOutcome.UNRESOLVED
        if _rr_status not in _TERMINAL_STATUSES:
            log.critical(
                "RESTART_RECOVERY_REREAD_NOT_TERMINAL local=%s status=%s — UNRESOLVED",
                local_oid, _rr_status,
            )
            return _RowOutcome.UNRESOLVED

        log.info("RESTART_RECOVERY_TERMINALIZED local=%s status=%s reason=%s", local_oid, _rr_status, reason)
        return _RowOutcome.TERMINALIZED

    # ── Registry verification — 6-way proof (Blocker 4) ──────────────────────

    def _verify_registry_ownership(self, local_oid: str, row: dict) -> Optional[dict]:
        """
        Blocker 4: structured proof requiring ALL of:
          local_order_id exact match, signal_id exact match, client_id exact match,
          execution_mode exact match, watcher state is PENDING or rearm/retry,
          NOT quarantined, dedup key is held.

        Returns a proof dict on success, None on any failure.
        """
        watcher = self.entry_watcher
        if watcher is None:
            return None

        # Expected identity from the row.
        _exp_sid    = str(row.get("signal_id") or "").strip()
        _exp_client = self.client_id.lower()
        _exp_mode   = self.execution_mode

        try:
            _pending = list(getattr(watcher, "_pending", []))
            _dedup   = getattr(watcher, "_dedup_set", set())

            for w in _pending:
                _wsig    = getattr(w, "signal", {}) or {}
                _w_oid   = str(_wsig.get("local_order_id") or "").strip()
                _w_sid   = str(_wsig.get("signal_id") or "").strip()
                _w_client= str(_wsig.get("client_id") or "").strip().lower()
                _w_mode  = str(_wsig.get("execution_mode") or "").strip().lower()
                _w_state = str(getattr(w, "state", "") or "")
                _quarant = bool(getattr(w, "_ownership_quarantine", False))

                if _w_oid != local_oid:
                    continue
                # local_order_id matched — now check all 5 remaining requirements.
                if _exp_sid and _w_sid and _w_sid != _exp_sid:
                    log.warning(
                        "RESTART_RECOVERY registry signal_id mismatch local=%s "
                        "expected=%s got=%s", local_oid, _exp_sid, _w_sid,
                    )
                    return None
                if _w_client and _w_client != _exp_client:
                    log.warning(
                        "RESTART_RECOVERY registry client_id mismatch local=%s "
                        "expected=%s got=%s", local_oid, _exp_client, _w_client,
                    )
                    return None
                if _w_mode and _w_mode != _exp_mode:
                    log.warning(
                        "RESTART_RECOVERY registry execution_mode mismatch local=%s "
                        "expected=%s got=%s", local_oid, _exp_mode, _w_mode,
                    )
                    return None
                if _quarant:
                    log.warning(
                        "RESTART_RECOVERY registry watcher is quarantined local=%s", local_oid,
                    )
                    return None
                _valid_states = {"PENDING", "REARM", "RETRY"}
                if _w_state.upper() not in _valid_states and _w_state:
                    log.warning(
                        "RESTART_RECOVERY registry watcher state invalid local=%s state=%s",
                        local_oid, _w_state,
                    )
                    return None
                _dedup_key = _w_sid or ""
                _dedup_held = (_dedup_key in _dedup) if _dedup_key else False
                if not _dedup_held:
                    log.warning(
                        "RESTART_RECOVERY registry dedup not held local=%s sid=%s",
                        local_oid, _dedup_key,
                    )
                    return None

                # All 6 checks passed.
                return {
                    "local_order_id":  _w_oid,
                    "signal_id":       _w_sid,
                    "client_id":       _w_client,
                    "execution_mode":  _w_mode,
                    "state":           _w_state,
                    "dedup_held":      True,
                    "watcher_obj":     w,
                }
        except Exception as exc:
            log.error("RESTART_RECOVERY registry check error local=%s: %s", local_oid, exc)
        return None

    # ── Watcher ownership probe ───────────────────────────────────────────────

    def _check_watcher_owns(self, local_oid: str) -> Optional[bool]:
        """Fast probe: does any registered watcher own this local_order_id?"""
        watcher = self.entry_watcher
        if watcher is None:
            return None
        try:
            for w in getattr(watcher, "_pending", []):
                _wsig = getattr(w, "signal", {}) or {}
                if str(_wsig.get("local_order_id") or "").strip() == local_oid:
                    return True
            return False
        except Exception:
            return None

    # ── Meta update helper ────────────────────────────────────────────────────

    def _safe_meta_update(self, local_oid: str, patch: dict) -> bool:
        fn = getattr(self.osm, "update_order_meta", None)
        if not callable(fn):
            return False
        try:
            return bool(fn(local_oid, patch))
        except Exception as exc:
            log.warning("RESTART_RECOVERY meta write failed local=%s: %s", local_oid, exc)
            return False


# ── Summary (Blocker 2: per-row outcomes) ─────────────────────────────────────

def _build_summary(client_id: str, execution_mode: str, outcomes: dict[str, str]) -> dict:
    _all = list(outcomes.values())
    return {
        "client_id":                        client_id,
        "execution_mode":                   execution_mode,
        "rows_examined":                    len(_all),
        "watchers_rearmed":                 _all.count(_RowOutcome.WATCHER_OWNED),
        "retry_rows_owned":                 _all.count(_RowOutcome.RETRY_OWNED),
        "rearm_rows_owned":                 _all.count(_RowOutcome.REARM_OWNED),
        "terminalized":                     _all.count(_RowOutcome.TERMINALIZED),
        "skipped_not_pending_trigger":      _all.count(_RowOutcome.SKIPPED),
        "unresolved_cleanup_failures":      _all.count(_RowOutcome.UNRESOLVED),
        # Blocker 2: ownerless = count(UNRESOLVED) — not arithmetic
        "ownerless_rows_remaining":         _all.count(_RowOutcome.UNRESOLVED),
        "row_outcomes":                     dict(outcomes),
    }


def _emit_summary(summary: dict) -> None:
    log.info(
        "PENDING_TRIGGER_RESTART_RECOVERY_SUMMARY "
        "client=%s mode=%s examined=%d "
        "rearmed=%d retry=%d terminalized=%d skipped=%d "
        "unresolved=%d ownerless=%d",
        summary["client_id"], summary["execution_mode"],
        summary["rows_examined"],
        summary["watchers_rearmed"], summary["retry_rows_owned"],
        summary["terminalized"], summary["skipped_not_pending_trigger"],
        summary["unresolved_cleanup_failures"],
        summary["ownerless_rows_remaining"],
    )
    if summary["ownerless_rows_remaining"] > 0:
        log.critical(
            "PENDING_TRIGGER_OWNERLESS_ROWS_REMAINING count=%d client=%s mode=%s "
            "— operator intervention required",
            summary["ownerless_rows_remaining"],
            summary["client_id"], summary["execution_mode"],
        )


# ── Plan builder ──────────────────────────────────────────────────────────────

def _build_plan(row: dict, plan_builder_fn=None) -> Optional[dict]:
    if plan_builder_fn is not None:
        try:
            return plan_builder_fn(row)
        except Exception:
            return None
    try:
        return {
            "signal_id":      row.get("signal_id") or "",
            "plan_id":        row.get("plan_id") or "",
            "local_order_id": row.get("local_order_id") or "",
            "client_id":      row.get("client_id") or "",
            "client_email":   row.get("client_id") or row.get("client_email") or "",
            "execution_mode": row.get("execution_mode") or "",
            "ticker":         row.get("ticker") or row.get("symbol") or "",
            "side":           row.get("direction") or row.get("side") or "",
            "entry_price":    float(row.get("entry_price") or row.get("trigger_price") or 0),
            "stop_price":     float(row.get("stop_price") or row.get("stop_underlying") or 0),
            "target_price":   float(row.get("target_price") or row.get("target_underlying") or 0),
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
