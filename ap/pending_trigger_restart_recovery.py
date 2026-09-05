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

import json
import os
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from typing import Any, Optional

from ap.broker_submit_identity import canonical_broker_submit_key
from ap.logger import get_logger
from ap.manual_close_reconciliation import (
    ORDERS_AVAILABLE_COMPLETE,
    ORDERS_AVAILABLE_EMPTY,
    fetch_all_current_session_orders,
)
from ap_entry_watcher import (
    RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN,
    recovery_trigger_evidence_identity_is_proven,
)
from ap.pending_trigger_classifier import (
    PendingTriggerClassification as PTC,
    classify_pending_trigger_row,
    has_broker_handoff_evidence,
    is_active_materialization_in_flight,
)
from ap.selector_retry_policy import (
    DeferredMaterializationConfigConflict,
    deferred_retry_count_exhaustion_applies,
    is_retryable_selector_reason,
    is_validity_bound_deferred_retry_reason,
    resolve_deferred_materialization_max_attempts,
    resolve_deferred_retry_deadline,
    resolve_deferred_retry_reason,
)

log = get_logger("ap.pending_trigger_restart_recovery")

# ── Per-row outcome constants (Blocker 2) ─────────────────────────────────────

class _RowOutcome:
    WATCHER_OWNED          = "WATCHER_OWNED"
    RETRY_OWNED            = "RETRY_OWNED"
    REARM_OWNED            = "REARM_OWNED"
    TERMINALIZED           = "TERMINALIZED"
    UNRESOLVED             = "UNRESOLVED"
    SKIPPED                = "SKIPPED"             # NOT_PENDING_TRIGGER (already resolved)
    # PR #521: active deferred materializer owns the attempt; recovery observes
    # and departs read-only. No terminalization, rearm, selector, or broker call.
    MATERIALIZATION_OWNED  = "MATERIALIZATION_OWNED"


_TERMINAL_STATUSES = frozenset({"CANCELED", "EXPIRED", "REJECTED", "ERROR"})

# #323 canonical materialization retry fields — exact shape from ap/deferred_materializer.stamp_retry_pending()
# DO NOT invent fields not present in that function.
_MAT_STATUS_FIELD        = "materialization_status"      # "RETRY_PENDING"
_MAT_NEXT_RETRY_AT       = "materialization_next_retry_at"
_MAT_ATTEMPTS_FIELD      = "materialization_attempts"    # int — canonical attempt counter
_MAT_REASON_FIELD        = "materialization_reason"      # reason_code str
_MAT_LAST_FAILURE_FIELD  = "materialization_last_failure_at"
_MAT_BROKER_READY        = "broker_ready"               # must be False on RETRY_PENDING rows
# Max attempts from the same env var the deferred materializer reads
_MAT_MAX_ATTEMPTS_ENV    = "DEFERRED_MATERIALIZATION_MAX_ATTEMPTS"

# Dedicated pre-breach restart rearm retry fields. These are intentionally
# separate from #323 post-breach materialization retry metadata.
_RR_STATUS_FIELD     = "restart_rearm_status"
_RR_OWNER_FIELD      = "restart_rearm_owner"
_RR_REASON_FIELD     = "restart_rearm_reason"
_RR_ATTEMPT_FIELD    = "restart_rearm_attempt"
_RR_NEXT_AT_FIELD    = "restart_rearm_next_at"
_RR_DEADLINE_FIELD   = "restart_rearm_deadline"
_RR_FIRST_FAILED_AT  = "restart_rearm_first_failed_at"
_RR_LAST_FAILED_AT   = "restart_rearm_last_failed_at"
_RR_CLIENT_FIELD     = "restart_rearm_client_id"
_RR_MODE_FIELD       = "restart_rearm_execution_mode"
_RR_CLOSED_AT        = "restart_rearm_closed_at"
_RR_CLOSE_REASON     = "restart_rearm_close_reason"

_RETRY_MATERIALIZATION = "MATERIALIZATION_RETRY"
_RETRY_RESTART_REARM  = "RESTART_REARM_RETRY"
_RETRY_WATCHER        = "WATCHER_RETRY"


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
        if not _quote_is_fresh(raw):
            return None
        bid = float(raw.get("bid") or 0)
        ask = float(raw.get("ask") or 0)
        if str(side or "").strip().upper() == "CALL":
            if ask <= 0:
                return None
            return ask >= trigger
        elif str(side or "").strip().upper() == "PUT":
            if bid <= 0:
                return None
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
        caller_source: str = "unknown",
    ) -> None:
        self.client_id      = str(client_id or "").strip()
        self.execution_mode = str(execution_mode or "").strip().lower()
        self.osm            = osm
        self.entry_watcher  = entry_watcher
        self.broker         = broker
        self.quote_check_fn = quote_check_fn or _default_quote_check
        self.is_past_eod    = is_past_eod
        self.dry_run        = dry_run
        self.caller_source  = str(caller_source or "unknown").strip() or "unknown"
        self._row_retry_subtypes: dict[str, str] = {}
        self._row_failure_reasons: dict[str, str] = {}
        self._broker_order_tags_snapshot: Optional[tuple[str, set[str]]] = None

        # PR #421 final amendment — watcher provenance. Set fresh at the
        # start of every _recover_one() call and read by the caller
        # IMMEDIATELY after a WATCHER_OWNED outcome, before any durable
        # adoption CAS attempt. This is the sole authoritative source of
        # "did THIS invocation register a new watcher, or did it merely
        # observe one that already existed" — callers must never
        # rediscover provenance later by scanning the watcher registry,
        # matching logical fields, or comparing Python object ids: none of
        # those can distinguish "I created this" from "this already
        # existed and I looked it up."
        self.last_watcher_registered_by_this_attempt: bool = False
        self.last_registration_token: Optional[str] = None

    # ── Public entry point ────────────────────────────────────────────────────

    def recover_all(self, rows: list[dict], *, plan_builder_fn=None) -> dict:
        """
        Recover every row in `rows`.
        Returns a structured summary.  `ownerless_rows_remaining == 0` required.
        """
        # Blocker 2: per-row outcome tracking; no arithmetic shortcuts.
        _outcomes: dict[str, str] = {}   # local_order_id → _RowOutcome constant
        self._row_retry_subtypes = {}
        self._row_failure_reasons = {}

        for row in rows:
            local_oid = str(row.get("local_order_id") or "").strip()
            outcome = self._recover_one(row, plan_builder_fn=plan_builder_fn)
            _outcomes[local_oid or id(row)] = outcome

        summary = _build_summary(
            self.client_id,
            self.execution_mode,
            _outcomes,
            retry_subtypes=self._row_retry_subtypes,
            failure_reasons=self._row_failure_reasons,
        )
        _emit_summary(summary)
        return summary

    def recover_one_row(self, row: dict, *, plan_builder_fn=None) -> str:
        """Single-row entry point for callers that iterate rows themselves.
        Returns the _RowOutcome constant."""
        return self._recover_one(row, plan_builder_fn=plan_builder_fn)

    def prove_restart_rearm_retry_owner(
        self,
        local_oid: str,
        *,
        expected_signal_id: str = "",
    ) -> Optional[dict]:
        """Public read-only proof for readiness and other ownership consumers."""
        return self._verify_restart_rearm_retry_ownership(
            str(local_oid or "").strip(),
            {},
            expected_signal_id=str(expected_signal_id or "").strip(),
            require_late_policy=True,
        )

    def consume_canonical_restart_rearm_retry(
        self,
        local_oid: str,
        *,
        expected_signal_id: str,
    ) -> Optional[str]:
        """Consume one exact late-policy restart-rearm lease.

        ``None`` means the durable row is not the canonical retry shape and
        grants the caller no bypass authority.  Otherwise the returned value
        is the normal ``_RowOutcome`` from this recovery engine.  Due/not-due,
        watcher ownership, market truth, renewal, and terminalization remain
        inside the canonical engine rather than being duplicated by callers.
        """
        local_oid = str(local_oid or "").strip()
        expected_signal_id = str(expected_signal_id or "").strip()
        if not local_oid or not expected_signal_id:
            return None

        proof = self._verify_restart_rearm_retry_ownership(
            local_oid,
            {},
            expected_signal_id=expected_signal_id,
            allow_expired=True,
            require_late_policy=True,
        )
        if proof is None:
            return None

        get_fn = getattr(self.osm, "get_order", None)
        if not callable(get_fn):
            return _RowOutcome.UNRESOLVED
        try:
            reread = get_fn(local_oid)
        except Exception:
            return _RowOutcome.UNRESOLVED
        if not isinstance(reread, dict):
            return _RowOutcome.UNRESOLVED
        if (
            _retry_subtype(reread) != _RETRY_RESTART_REARM
            or not _late_attachment_policy_eligible(reread)
            or str(reread.get("signal_id") or "").strip() != expected_signal_id
        ):
            return _RowOutcome.UNRESOLVED
        return self._recover_one(reread)

    # ── Per-row dispatch ──────────────────────────────────────────────────────

    def _recover_one(self, row: dict, *, plan_builder_fn=None) -> str:
        # PR #421 final amendment: reset watcher provenance for every row.
        # A stale True/token from a PRIOR row must never leak into this
        # one's WATCHER_OWNED handling.
        self.last_watcher_registered_by_this_attempt = False
        self.last_registration_token = None

        local_oid  = str(row.get("local_order_id") or "").strip()
        signal_id  = str(row.get("signal_id") or "").strip()
        row_client = str(row.get("client_id") or "").strip().lower()
        row_mode   = str(row.get("execution_mode") or "").strip().lower()

        # Identity fence: raw durable row identity is required. Callers must
        # never repair missing client/mode from active runner context.
        if not local_oid:
            self._mark_failure("", "missing_local_order_id")
            self._log_identity_failure(
                "RESTART_RECOVERY_MISSING_LOCAL_ORDER_ID",
                local_oid=local_oid,
                signal_id=signal_id,
                durable_client=row_client,
                durable_mode=row_mode,
            )
            return _RowOutcome.UNRESOLVED

        if not row_client:
            self._mark_failure(local_oid, "identity:missing_client_id")
            self._log_identity_failure(
                "RESTART_RECOVERY_MISSING_DURABLE_CLIENT_ID",
                local_oid=local_oid,
                signal_id=signal_id,
                durable_client=row_client,
                durable_mode=row_mode,
            )
            return _RowOutcome.UNRESOLVED

        if not row_mode:
            self._mark_failure(local_oid, "identity:missing_execution_mode")
            self._log_identity_failure(
                "RESTART_RECOVERY_MISSING_DURABLE_EXECUTION_MODE",
                local_oid=local_oid,
                signal_id=signal_id,
                durable_client=row_client,
                durable_mode=row_mode,
            )
            return _RowOutcome.UNRESOLVED

        if row_client != self.client_id.lower():
            self._mark_failure(local_oid, "identity:client_id_mismatch")
            self._log_identity_failure(
                "RESTART_RECOVERY_CLIENT_ID_MISMATCH",
                local_oid=local_oid,
                signal_id=signal_id,
                durable_client=row_client,
                durable_mode=row_mode,
            )
            return _RowOutcome.UNRESOLVED

        if row_mode != self.execution_mode:
            self._mark_failure(local_oid, "identity:execution_mode_mismatch")
            self._log_identity_failure(
                "RESTART_RECOVERY_EXECUTION_MODE_MISMATCH",
                local_oid=local_oid,
                signal_id=signal_id,
                durable_client=row_client,
                durable_mode=row_mode,
            )
            return _RowOutcome.UNRESOLVED

        def _reject_unproven_trigger_evidence() -> str:
            self._mark_failure(
                local_oid, RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN
            )
            self._log_identity_failure(
                RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN,
                local_oid=local_oid,
                signal_id=signal_id,
                durable_client=row_client,
                durable_mode=row_mode,
            )
            return _RowOutcome.UNRESOLVED

        # Confirmed-trigger evidence is a durable lifecycle fact, not a quote
        # hint.  First probe the registry so an already-owned watcher can take
        # its read-only proof path.  This matters for the legacy/crash window
        # where the watcher is healthy but the timestamp and its provenance
        # were not persisted atomically.  No rearm or cleanup is permitted on
        # that path; the exact watcher/order ownership proof is the authority.
        watcher_owned: Optional[bool] = self._check_watcher_owns(local_oid, row)
        _evidence_proven = recovery_trigger_evidence_identity_is_proven(row, local_oid)

        # For an unowned row, keep the fail-closed fence before quote checks,
        # selector work, watcher admission, or any terminal/cleanup action.
        # A row with no timestamp remains an ordinary pre-breach candidate.
        if watcher_owned is not True and not _evidence_proven:
            return _reject_unproven_trigger_evidence()

        # A contradictory broker-ready or submit marker is not permission to
        # classify the row as a zombie.  The broker may have accepted an
        # order before the durable identity write completed.  Hold this row
        # before quote work and before the STUCK cleanup action; the dedicated
        # broker-intent reconciler, when available, is the only authority that
        # may resolve that ambiguity.
        if has_broker_handoff_evidence(row):
            meta = _extract_meta(row)
            self._mark_failure(local_oid, "broker_handoff_ambiguous")
            log.critical(
                "PENDING_TRIGGER_BROKER_HANDOFF_AMBIGUOUS "
                "local_order_id=%s client_id=%s execution_mode=%s signal_id=%s "
                "broker_submission=UNKNOWN broker_cancel=NOT_ATTEMPTED caller=%s "
                "submit_intent_at=%s broker_submit_key=%s "
                "broker_submit_payload_hash=%s broker_ready=%s",
                local_oid,
                row_client,
                row_mode,
                signal_id,
                self.caller_source,
                bool(str(meta.get("submit_intent_at") or "").strip()),
                bool(str(meta.get("broker_submit_key") or "").strip()),
                bool(str(meta.get("broker_submit_payload_hash") or "").strip()),
                meta.get("broker_ready"),
            )
            return _RowOutcome.UNRESOLVED

        # Classify the durable owner before doing any quote work.  An active
        # materializer is already the sole authority for this attempt; even a
        # read-only external quote request is unnecessary and can delay the
        # owner while another cleanup path races the same row.
        cls = classify_pending_trigger_row(
            row,
            watcher_owned=watcher_owned,
            is_past_eod=self.is_past_eod,
            live_quote_already_through_trigger=None,
        )

        def _observe_materialization_owner() -> str:
            meta = row.get("meta") or {}
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except Exception:
                    meta = {}
            if not isinstance(meta, dict):
                meta = {}
            log.info(
                "PENDING_TRIGGER_MATERIALIZATION_IN_FLIGHT "
                "local_order_id=%s client_id=%s execution_mode=%s signal_id=%s "
                "materialization_owner=%s materialization_generation=%s "
                "materialization_lease_until=%s broker_submission=NOT_ATTEMPTED "
                "broker_cancel=NOT_ATTEMPTED caller=%s",
                local_oid,
                row.get("client_id") or self.client_id,
                row.get("execution_mode") or self.execution_mode,
                row.get("signal_id") or "",
                meta.get("materialization_owner") or "",
                meta.get("materialization_generation") or "",
                meta.get("materialization_lease_until") or "",
                self.caller_source,
            )
            return _RowOutcome.MATERIALIZATION_OWNED

        if cls == PTC.MATERIALIZATION_IN_FLIGHT:
            # The shared predicate is repeated here so this early no-quote
            # optimization cannot become a protection bypass if the
            # classifier priority changes later.
            if is_active_materialization_in_flight(row):
                return _observe_materialization_owner()

        if cls == PTC.STUCK_TRIGGER_READY:
            # A crashed phase-one retry is not an abandoned trigger. It has a
            # distinct exact-owner, same-attempt recovery contract and must be
            # resolved before ordinary quote/terminal handling.
            _phase_one_recovery = self._recover_stale_market_truth_pending(
                row, local_oid
            )
            if _phase_one_recovery is not None:
                return _phase_one_recovery

        # Live quote check.  Late-attachment rows deliberately skip this
        # coarse trigger-only probe: after the regular-session boundary the
        # canonical APEntryWatcher owns fresh target/stop/move/continuation
        # classification.  A missing broker.get_quote() result must not mask
        # market truth that the watcher can obtain through its quote adapter.
        live_quote_abt: Optional[bool] = None
        _side    = str(row.get("direction") or row.get("side") or "").strip().upper()
        _symbol  = str(row.get("ticker") or row.get("underlying") or row.get("symbol") or "").strip()
        _trigger = _canonical_underlying_trigger(row)
        if (
            not _late_attachment_policy_eligible(row)
            and not _retry_subtype(row)
            and _symbol
            and _side
            and _trigger is not None
        ):
            try:
                live_quote_abt = self.quote_check_fn(self.broker, _symbol, _side, _trigger)
            except Exception as _qe:
                log.debug("RESTART_RECOVERY quote check failed local=%s: %s", local_oid, _qe)

        # Reclassify after the optional quote check so quote-derived unsafe
        # states retain their existing priority for non-active rows.
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
        if cls == PTC.WAITING_VALID and watcher_owned is True:
            # Already owned — verify proof then count.  This is deliberately
            # before the evidence gate below: it performs no rearm, callback,
            # selector, broker, or cleanup action and preserves the exact row.
            proof = self._verify_registry_ownership(local_oid, row)
            if proof and proof.get("dedup_held"):
                # PR #421 final amendment: this watcher demonstrably existed
                # BEFORE this invocation ran — nothing was registered here.
                # A caller that treated this WATCHER_OWNED the same as a
                # fresh registration could later "roll back" (evict +
                # release dedup) a real, currently-owned watcher merely
                # because this pass observed it. Explicit, not just
                # inherited from the top-of-function reset, so this
                # invariant survives any future refactor of the reset.
                self.last_watcher_registered_by_this_attempt = False
                self.last_registration_token = None
                return _RowOutcome.WATCHER_OWNED
            # Proof failed despite watcher reporting owned — treat as orphan.
            log.warning(
                "RESTART_RECOVERY watcher_owned=True but proof failed local=%s — "
                "treating as orphan", local_oid,
            )

        # PR #521 amendment (audit Finding 3): MATERIALIZATION_IN_FLIGHT is
        # hoisted BEFORE the second evidence fence.
        #
        # Which scenario this protects
        # ────────────────────────────
        # The second fence (line 386) fires when _evidence_proven is False AND
        # the row has passed the first fence (line 301).  The first fence is
        # bypassed ONLY when watcher_owned is True — so the hoist is
        # specifically effective for:
        #
        #   watcher_owned=True  AND  _evidence_proven=False  AND
        #   cls=MATERIALIZATION_IN_FLIGHT
        #
        # Concrete example: the watcher is still registered for the row
        # (trigger callback not yet evicted from _pending) while the
        # deferred materializer concurrently holds the row under its own
        # lease.  The watcher's registration lets it pass the first fence.
        # trigger_crossed_at may have been persisted but
        # trigger_crossed_at_provenance not yet flushed (crash window) →
        # _evidence_proven=False → without the hoist the second fence would
        # fire → UNRESOLVED + false ownerless alarm even though the
        # materializer is alive and the 7-field proof is fully valid.
        #
        # For rows where watcher_owned is None or False, the first fence
        # fires first (line 301) when evidence is also unproven — the hoisted
        # handler is never reached in that path, and UNRESOLVED is correct
        # (fail-closed: no watcher owns it AND trigger identity is unproven).
        #
        # Safety: the classification itself is already fail-closed — any
        # missing/expired proof field produces STUCK_TRIGGER_READY, which
        # then hits the second fence and returns UNRESOLVED as before.  Only
        # rows whose 7-field proof is fully valid reach this branch.
        if cls == PTC.MATERIALIZATION_IN_FLIGHT:
            # Binding invariant: ZERO mutations. No terminalize, rearm,
            # selector, materializer invocation, capacity/revalidation,
            # attempt-counter increment, owner/generation replacement, lease
            # renewal, retry scheduling, broker submit/cancel, or
            # position/proof_trades/queue mutation.
            return _observe_materialization_owner()

        # Every path that would classify, terminalize, retry, or rearm an
        # order with confirmed-trigger evidence still requires durable
        # lifecycle identity.  Only the proven already-owned fast path above
        # and the MATERIALIZATION_IN_FLIGHT read-only path are allowed to
        # return before this fence.
        if not _evidence_proven:
            return _reject_unproven_trigger_evidence()

        if cls == PTC.NOT_PENDING_TRIGGER:
            return _RowOutcome.SKIPPED

        elif cls == PTC.WAITING_VALID:
            # Post-open validity belongs to the canonical watcher, which
            # classifies target/stop/miss/continuation from fresh market truth.
            if _late_attachment_policy_eligible(row):
                return self._rearm_and_verify(
                    row,
                    local_oid,
                    plan_builder_fn=plan_builder_fn,
                )
            # Ordinary restart recovery retains its trigger-only quote fence.
            if live_quote_abt is True:
                return self._terminalize_with_reason(
                    local_oid, row, "restart_recovery_already_through_trigger",
                    meta_patch={"restart_recovery_cls": cls},
                )
            if live_quote_abt is None:
                return self._enter_restart_rearm_retry(local_oid, row,
                    reason="restart_recovery_quote_unavailable_before_rearm")
            return self._rearm_and_verify(row, local_oid, plan_builder_fn=plan_builder_fn)

        elif cls == PTC.WAITING_RETRYABLE:
            return self._handle_retryable(row, local_oid, live_quote_abt, plan_builder_fn)

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
            if _late_attachment_policy_eligible(row):
                return self._rearm_and_verify(
                    row,
                    local_oid,
                    plan_builder_fn=plan_builder_fn,
                )
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
        _rr_status = str(_meta.get(_RR_STATUS_FIELD) or "").strip().upper()
        _rr_next = _meta.get(_RR_NEXT_AT_FIELD)
        _has_restart_rearm_retry = (_rr_status == "RETRY_PENDING" and _rr_next)

        if _has_terminal:
            return self._terminalize_with_reason(
                local_oid, row,
                _inv_reason or "restart_orphan_terminal_evidence",
                meta_patch={"restart_recovery_cls": PTC.ORPHAN_NO_WATCHER},
            )
        if _has_canonical_retry:
            proof = self._verify_materialization_retry_ownership(local_oid, row)
            if proof is not None:
                self._mark_retry_subtype(local_oid, _RETRY_MATERIALIZATION)
                self._safe_meta_update(local_oid, {
                    "restart_recovery_cls": PTC.ORPHAN_NO_WATCHER,
                    "restart_recovery_at":  _now_iso(),
                })
                return _RowOutcome.RETRY_OWNED
            log.critical(
                "RESTART_RECOVERY_ORPHAN_RETRY_OWNERSHIP_UNPROVEN local=%s — UNRESOLVED",
                local_oid,
            )
            return _RowOutcome.UNRESOLVED
        if _has_restart_rearm_retry:
            return self._handle_restart_rearm_retry(row, local_oid, live_quote_abt, plan_builder_fn)
        if _late_attachment_policy_eligible(row):
            return self._rearm_and_verify(
                row,
                local_oid,
                plan_builder_fn=plan_builder_fn,
            )
        if live_quote_abt is True:
            return self._terminalize_with_reason(
                local_oid, row,
                "restart_recovery_already_through_trigger",
                meta_patch={"restart_recovery_cls": PTC.ORPHAN_NO_WATCHER},
            )
        if live_quote_abt is None:
            return self._enter_restart_rearm_retry(local_oid, row,
                reason="restart_orphan_quote_unavailable")
        # Quote clear, no terminal evidence → safe recovery rearm.
        return self._rearm_and_verify(row, local_oid, plan_builder_fn=plan_builder_fn)

    def _handle_retryable(self, row, local_oid, live_quote_abt, plan_builder_fn) -> str:
        subtype = _retry_subtype(row)
        if subtype == _RETRY_MATERIALIZATION:
            proof = self._verify_materialization_retry_ownership(local_oid, row)
            if proof is not None:
                self._mark_retry_subtype(local_oid, _RETRY_MATERIALIZATION)
                log.info("RESTART_RECOVERY_MATERIALIZATION_RETRY_OWNED local=%s proof=%s", local_oid, proof)
                self._safe_meta_update(local_oid, {
                    "restart_recovery_cls": PTC.WAITING_RETRYABLE,
                    "restart_recovery_retry_subtype": _RETRY_MATERIALIZATION,
                    "restart_recovery_at": _now_iso(),
                })
                return _RowOutcome.RETRY_OWNED
            self._mark_failure(local_oid, "retry_verification:materialization")
            log.critical("RESTART_RECOVERY_MATERIALIZATION_RETRY_UNPROVEN local=%s", local_oid)
            return _RowOutcome.UNRESOLVED

        if subtype == _RETRY_RESTART_REARM:
            return self._handle_restart_rearm_retry(row, local_oid, live_quote_abt, plan_builder_fn)

        if subtype == _RETRY_WATCHER:
            proof = self._verify_registry_ownership(local_oid, row)
            if proof is not None:
                self._mark_retry_subtype(local_oid, _RETRY_WATCHER)
                return _RowOutcome.RETRY_OWNED
            self._mark_failure(local_oid, "retry_verification:watcher")
            log.critical("RESTART_RECOVERY_WATCHER_RETRY_UNPROVEN local=%s", local_oid)
            return _RowOutcome.UNRESOLVED

        self._mark_failure(local_oid, "retry_verification:unknown_subtype")
        log.critical("RESTART_RECOVERY_UNKNOWN_RETRY_SUBTYPE local=%s", local_oid)
        return _RowOutcome.UNRESOLVED

    def _handle_restart_rearm_retry(self, row, local_oid, live_quote_abt, plan_builder_fn) -> str:
        proof = self._verify_restart_rearm_retry_ownership(
            local_oid,
            row,
            expected_signal_id=str(row.get("signal_id") or "").strip(),
            allow_expired=True,
        )
        if proof is None:
            self._mark_failure(local_oid, "retry_verification:restart_rearm")
            log.critical("RESTART_RECOVERY_RESTART_REARM_RETRY_UNPROVEN local=%s", local_oid)
            return _RowOutcome.UNRESOLVED

        now = datetime.now(timezone.utc)
        next_at = proof["restart_rearm_next_at_dt"]
        deadline = proof["restart_rearm_deadline_dt"]
        attempt = int(proof["restart_rearm_attempt"])
        max_attempts = _env_int("RESTART_REARM_RETRY_MAX_ATTEMPTS", 6)

        if now < next_at:
            self._mark_retry_subtype(local_oid, _RETRY_RESTART_REARM)
            return _RowOutcome.RETRY_OWNED

        _late_policy = _late_attachment_policy_eligible(row)
        if (now > deadline or attempt >= max_attempts) and not _late_policy:
            return self._terminalize_with_reason(
                local_oid,
                row,
                "restart_rearm_quote_retry_exhausted",
                meta_patch={
                    "restart_recovery_retry_subtype": _RETRY_RESTART_REARM,
                    "restart_rearm_exhausted_attempt": attempt,
                },
            )

        # An expired/exhausted lease is not watcher authority.  The exact
        # late-policy path may create one fresh bounded generation, but this
        # cycle performs no watcher mutation; the renewed next_at is consumed
        # by a later normal monitor tick.
        if _late_policy and (now > deadline or attempt >= max_attempts):
            return self._enter_restart_rearm_retry(
                local_oid,
                row,
                reason="late_attachment_market_truth_unavailable_or_unresolved",
                prior_attempt=0,
                first_failed_at=None,
            )

        # Due late retries return to the same canonical watcher path.  Its
        # fresh regular-session classifier is the sole market-validity
        # authority; watcher=False with a still-pending row renews RETRY/HOLD.
        if _late_policy:
            outcome = self._rearm_and_verify(
                row,
                local_oid,
                plan_builder_fn=plan_builder_fn,
            )
            if outcome in (_RowOutcome.WATCHER_OWNED, _RowOutcome.TERMINALIZED):
                self._safe_meta_update(local_oid, {
                    _RR_STATUS_FIELD: "CLOSED",
                    _RR_CLOSED_AT: _now_iso(),
                    _RR_CLOSE_REASON: (
                        "watcher_owned"
                        if outcome == _RowOutcome.WATCHER_OWNED
                        else "market_truth_terminal"
                    ),
                    "restart_recovery_retry_subtype": _RETRY_RESTART_REARM,
                })
            return outcome

        quote_state = self._quote_state_for_row(row)
        if quote_state is True:
            return self._terminalize_with_reason(
                local_oid,
                row,
                "restart_recovery_already_through_trigger",
                meta_patch={"restart_recovery_retry_subtype": _RETRY_RESTART_REARM},
            )
        if quote_state is False:
            outcome = self._rearm_and_verify(row, local_oid, plan_builder_fn=plan_builder_fn)
            if outcome in (_RowOutcome.WATCHER_OWNED, _RowOutcome.TERMINALIZED):
                self._safe_meta_update(local_oid, {
                    _RR_STATUS_FIELD: "CLOSED",
                    _RR_CLOSED_AT: _now_iso(),
                    _RR_CLOSE_REASON: (
                        "watcher_owned"
                        if outcome == _RowOutcome.WATCHER_OWNED
                        else "market_truth_terminal"
                    ),
                    "restart_recovery_retry_subtype": _RETRY_RESTART_REARM,
                })
            return outcome

        return self._enter_restart_rearm_retry(
            local_oid,
            row,
            reason=proof["restart_rearm_reason"],
            prior_attempt=attempt,
            first_failed_at=proof["restart_rearm_first_failed_at"],
        )

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
            return self._enter_restart_rearm_retry(local_oid, row,
                reason="restart_recovery_watcher_unavailable")

        plan = _build_plan(row, plan_builder_fn)
        if plan is None:
            log.critical("RESTART_RECOVERY_PLAN_BUILD_FAILED local=%s", local_oid)
            return _RowOutcome.UNRESOLVED

        if self.dry_run:
            log.info("RESTART_RECOVERY_DRY_RUN local=%s — would watch(recovery_rearm=True)", local_oid)
            # Nothing was actually registered — no provenance to claim.
            self.last_watcher_registered_by_this_attempt = False
            self.last_registration_token = None
            return _RowOutcome.WATCHER_OWNED

        # Re-check exact runtime ownership immediately before the mutating
        # boundary.  A watcher may have been installed after the earlier row
        # classification; preserve that lifecycle and never install a second
        # watcher.  An exception is unknown ownership truth, not permission to
        # mutate.
        _has_order_fn = getattr(watcher, "has_order", None)
        if callable(_has_order_fn):
            try:
                _runtime_owned = bool(_has_order_fn(local_oid))
            except Exception as exc:
                self._mark_failure(local_oid, "runtime_ownership_lookup_failed")
                log.critical(
                    "RESTART_RECOVERY_RUNTIME_OWNERSHIP_LOOKUP_FAILED "
                    "local=%s error=%s — watcher not called",
                    local_oid, exc,
                )
                return _RowOutcome.UNRESOLVED
            if _runtime_owned:
                proof = self._verify_registry_ownership(local_oid, row)
                if proof is not None:
                    self.last_watcher_registered_by_this_attempt = False
                    self.last_registration_token = None
                    return _RowOutcome.WATCHER_OWNED
                self._mark_failure(local_oid, "runtime_ownership_identity_unproven")
                log.critical(
                    "RESTART_RECOVERY_RUNTIME_OWNERSHIP_IDENTITY_UNPROVEN "
                    "local=%s — existing watcher preserved; second watcher refused",
                    local_oid,
                )
                return _RowOutcome.UNRESOLVED

        try:
            _provenance = {
                "created_by_this_call": False,
                "registration_token": None,
            }
            armed = bool(
                watcher.watch(
                    plan, local_oid, recovery_rearm=True,
                    registration_provenance_out=_provenance,
                )
            )
        except Exception as exc:
            log.error("RESTART_RECOVERY_WATCH_RAISED local=%s: %s", local_oid, exc, exc_info=True)
            return _RowOutcome.UNRESOLVED

        if not armed:
            if getattr(watcher, "_last_reject_reason", None) == (
                RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN
            ):
                self._mark_failure(
                    local_oid, RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN
                )
                log.critical(
                    "RESTART_RECOVERY_%s local=%s — row left unchanged",
                    RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN,
                    local_oid,
                )
                return _RowOutcome.UNRESOLVED

            # The canonical watcher may already have terminalized this exact
            # row from fresh stop/target/decisive-move truth.  Observe that
            # terminal state instead of issuing a second generic cancel that
            # would erase the real market-authority reason.
            _post_row = None
            _get_order = getattr(self.osm, "get_order", None)
            if callable(_get_order):
                try:
                    _post_row = _get_order(local_oid)
                except Exception:
                    _post_row = None
            _post_status = str(
                (_post_row or {}).get("status") if isinstance(_post_row, dict) else ""
            ).strip().upper()
            if _post_status in _TERMINAL_STATUSES:
                log.info(
                    "RESTART_RECOVERY_MARKET_TERMINAL_OBSERVED local=%s status=%s "
                    "reason=%s",
                    local_oid,
                    _post_status,
                    (_post_row or {}).get("last_error") if isinstance(_post_row, dict) else "",
                )
                return _RowOutcome.TERMINALIZED
            if _late_attachment_policy_eligible(row):
                return self._enter_restart_rearm_retry(
                    local_oid,
                    row,
                    reason="late_attachment_market_truth_unavailable_or_unresolved",
                )
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
        # PR #421 final amendment (P0-1): watch() returning True is proof
        # ownership exists — it is NOT proof this invocation created it.
        # watch()'s own recovery_rearm "left alone" path can return True
        # after observing a watcher a CONCURRENT actor registered between
        # this call's earlier registry check and watch()'s internal
        # re-check, without ever calling add_signal(). Provenance must
        # come only from _provenance, which watch() (via add_signal())
        # sets causally, at the exact point of registration — never
        # reconstructed here from the post-call registry proof, which
        # proves ownership but not authorship.
        self.last_watcher_registered_by_this_attempt = bool(
            _provenance.get("created_by_this_call")
        )
        self.last_registration_token = (
            _provenance.get("registration_token")
            if self.last_watcher_registered_by_this_attempt
            else None
        )
        return _RowOutcome.WATCHER_OWNED

    def _enter_restart_rearm_retry(
        self,
        local_oid: str,
        row: dict,
        *,
        reason: str,
        prior_attempt: Optional[int] = None,
        first_failed_at: Optional[str] = None,
    ) -> str:
        if has_broker_handoff_evidence(row) or (
            _has_trigger_or_submit_evidence(row)
            and not _late_attachment_policy_eligible(row)
        ):
            self._mark_failure(local_oid, "restart_rearm_blocked:trigger_or_submit_evidence")
            log.critical("RESTART_RECOVERY_RESTART_REARM_BLOCKED local=%s trigger_or_submit_evidence=true", local_oid)
            return _RowOutcome.UNRESOLVED

        _delay = _env_int("RESTART_REARM_RETRY_DELAY_SECONDS", 30)
        _deadline_secs = _env_int("RESTART_REARM_RETRY_DEADLINE_SECONDS", 180)
        _max = _env_int("RESTART_REARM_RETRY_MAX_ATTEMPTS", 6)
        _now = datetime.now(timezone.utc)
        _attempts = int(prior_attempt if prior_attempt is not None else (_extract_meta(row).get(_RR_ATTEMPT_FIELD) or 0)) + 1
        # A proven late market-data HOLD is not setup invalidity.  Start a new
        # bounded lease generation instead of converting retry exhaustion into
        # a permanent trade decision.  Ordinary restart retries retain their
        # existing terminal exhaustion behavior.
        if _attempts > _max and _late_attachment_policy_eligible(row):
            _attempts = 1
            first_failed_at = None
        elif _attempts > _max:
            return self._terminalize_with_reason(
                local_oid,
                row,
                "restart_rearm_quote_retry_exhausted",
                meta_patch={"restart_rearm_exhausted_attempt": _attempts},
            )

        _deadline_dt = (
            _parse_iso(first_failed_at) + timedelta(seconds=_deadline_secs)
            if first_failed_at and _parse_iso(first_failed_at) is not None
            else _now + timedelta(seconds=_deadline_secs)
        )
        if _deadline_dt <= _now and _late_attachment_policy_eligible(row):
            _attempts = 1
            first_failed_at = None
            _deadline_dt = _now + timedelta(seconds=_deadline_secs)
        elif _deadline_dt <= _now:
            return self._terminalize_with_reason(
                local_oid,
                row,
                "restart_rearm_quote_retry_exhausted",
                meta_patch={"restart_rearm_exhausted_attempt": _attempts},
            )
        _next_dt = min(_now + timedelta(seconds=_delay), _deadline_dt)
        _next = _next_dt.isoformat()
        _deadline = _deadline_dt.isoformat()
        _first_failed = first_failed_at or _now.isoformat()
        _owner = f"restart_rearm:{self.client_id}:{self.execution_mode}:{local_oid}"
        _patch = {
            _RR_STATUS_FIELD: "RETRY_PENDING",
            _RR_OWNER_FIELD: _owner,
            _RR_REASON_FIELD: str(reason or "restart_rearm_quote_unavailable"),
            _RR_ATTEMPT_FIELD: _attempts,
            _RR_NEXT_AT_FIELD: _next,
            _RR_DEADLINE_FIELD: _deadline,
            _RR_FIRST_FAILED_AT: _first_failed,
            _RR_LAST_FAILED_AT: _now.isoformat(),
            _RR_CLIENT_FIELD: self.client_id,
            _RR_MODE_FIELD: self.execution_mode,
            "restart_recovery_retry_subtype": _RETRY_RESTART_REARM,
            "restart_recovery_at": _now.isoformat(),
        }
        if not self._safe_meta_update(local_oid, _patch):
            self._mark_failure(local_oid, "restart_rearm_retry_write_failed")
            return _RowOutcome.UNRESOLVED

        proof = self._verify_restart_rearm_retry_ownership(
            local_oid,
            row,
            expected_owner=_owner,
            expected_next_at=_next,
            expected_deadline=_deadline,
            expected_signal_id=str(row.get("signal_id") or "").strip(),
        )
        if proof is None:
            self._mark_failure(local_oid, "retry_verification:restart_rearm_after_write")
            log.critical("RESTART_RECOVERY_RESTART_REARM_RETRY_NOT_PROVEN local=%s", local_oid)
            return _RowOutcome.UNRESOLVED

        self._mark_retry_subtype(local_oid, _RETRY_RESTART_REARM)
        log.info("RESTART_RECOVERY_RESTART_REARM_RETRY_OWNED local=%s proof=%s", local_oid, proof)
        return _RowOutcome.RETRY_OWNED

    def _load_authoritative_broker_order_tags(self) -> tuple[str, set[str]]:
        """Return a complete current-session Tradier tag snapshot or UNKNOWN.

        Reuse the canonical current-session order authority. Only its
        COMPLETE or EMPTY states can prove that the exact submit tag is
        absent; malformed, unavailable, and incomplete states remain UNKNOWN.
        """
        if self._broker_order_tags_snapshot is not None:
            return self._broker_order_tags_snapshot

        try:
            orders_state, orders = fetch_all_current_session_orders(self.broker)
            if orders_state not in {
                ORDERS_AVAILABLE_COMPLETE,
                ORDERS_AVAILABLE_EMPTY,
            }:
                raise ValueError(
                    f"BROKER_ORDERS_TRUTH_{str(orders_state or 'UNKNOWN').upper()}"
                )
            if not isinstance(orders, list) or any(
                not isinstance(order, dict) for order in orders
            ):
                raise ValueError("BROKER_ORDERS_RESULT_MALFORMED")
            tags = {
                str(order.get("tag") or "").strip()
                for order in orders
                if str(order.get("tag") or "").strip()
            }
            self._broker_order_tags_snapshot = (orders_state, tags)
            return self._broker_order_tags_snapshot
        except Exception as exc:
            log.critical(
                "RESTART_PHASE_ONE_BROKER_TAG_LOOKUP_UNKNOWN client=%s mode=%s "
                "error=%s",
                self.client_id,
                self.execution_mode,
                exc,
            )
            self._broker_order_tags_snapshot = ("UNKNOWN", set())
            return self._broker_order_tags_snapshot

    def _inside_retry_entry_window(self, row: dict) -> bool:
        meta = _extract_meta(row)
        trigger_dt = _parse_iso(
            meta.get("trigger_crossed_at") or meta.get("triggered_at")
        )
        if trigger_dt is None:
            return False
        eastern = ZoneInfo("America/New_York")
        now_utc = datetime.now(timezone.utc)
        now_et = now_utc.astimezone(eastern)
        if trigger_dt.astimezone(eastern).date() != now_et.date():
            return False

        deadline, deadline_error = resolve_deferred_retry_deadline(
            meta,
            now=now_utc,
        )
        if deadline_error or deadline is None or now_utc >= deadline:
            return False
        return True

    def _retry_entry_deadline(self, row: dict) -> "datetime | None":
        """Return the effective retry deadline, failing closed on bad metadata.

        Used by the phase-one bounded-backoff clip (PR #568 amendment §2) to
        refuse writing a ``next_retry_at`` that would land past the entry
        cutoff. Combines the durable absolute deadline (if any) with the
        BREACH_SELECTOR_RETRY_CUTOFF_ET wall-clock ceiling through the shared
        authority resolver so callers only need one comparison. A malformed
        non-empty durable alias or cutoff raises ``ValueError``; callers must
        leave the row unresolved rather than schedule from ambiguous truth.
        """
        meta = _extract_meta(row)
        deadline, deadline_error = resolve_deferred_retry_deadline(meta)
        if deadline_error:
            raise ValueError(deadline_error)
        return deadline

    def _recover_stale_market_truth_pending(
        self, row: dict, local_oid: str
    ) -> Optional[str]:
        """Recover an expired phase-one claim at the same selector attempt."""
        if self.execution_mode not in {"live", "paper"} or self.is_past_eod:
            return None
        meta = _extract_meta(row)
        if not (
            meta.get("materialization_market_truth_pending") is True
            and str(meta.get("lifecycle_state") or "").strip().upper()
            == "MATERIALIZING"
            and str(meta.get("materialization_status") or "").strip().upper()
            == "RUNNING"
            and meta.get("materialization_in_flight") is True
        ):
            return None

        signal_id = str(row.get("signal_id") or "").strip()
        owner = str(meta.get("materialization_owner") or "").strip()
        if (
            not signal_id
            or not owner
            or str(meta.get("current_owner") or "").strip() != owner
            or str(meta.get("watcher_token") or "").strip() != owner
        ):
            return _RowOutcome.UNRESOLVED
        lease = _parse_iso(meta.get("materialization_lease_until"))
        if lease is None or lease >= datetime.now(timezone.utc):
            return _RowOutcome.UNRESOLVED

        raw_counts = (
            meta.get("retry_attempt"),
            meta.get("breach_attempt_count"),
            meta.get("materialization_attempts"),
        )
        if any(isinstance(value, bool) or not isinstance(value, int) for value in raw_counts):
            return _RowOutcome.UNRESOLVED
        attempt = raw_counts[0]
        if attempt < 1 or raw_counts != (attempt, attempt, attempt):
            return _RowOutcome.UNRESOLVED
        generation = meta.get("materialization_generation")
        if isinstance(generation, bool) or not isinstance(generation, int) or generation < 1:
            return _RowOutcome.UNRESOLVED

        selector_failure = meta.get("materialization_selector_failure")
        if not isinstance(selector_failure, dict):
            selector_failure = meta.get("selector_failure")
        if not isinstance(selector_failure, dict):
            return _RowOutcome.UNRESOLVED
        reason, reason_error = resolve_deferred_retry_reason(
            meta,
            selector_failure=selector_failure,
        )
        if reason_error:
            log.warning(
                "RESTART_PHASE_ONE_RETRY_REASON_AUTHORITY_CONFLICT "
                "local=%s error=%s -- leaving row unresolved.",
                local_oid,
                reason_error,
            )
            return _RowOutcome.UNRESOLVED
        reason = reason or ""
        if not reason:
            return None

        # A phase-one claim records the selector attempt that was already
        # earned.  Count-bounded RETRYABLE_DATA reasons may recover that claim
        # only when another selector attempt remains; otherwise the existing
        # STUCK_TRIGGER_READY terminal path is the count-exhausted authority.
        # Validity-bound reasons and proven retryable aggregate reasons retain
        # their existing cutoff/deadline authority instead of this numeric
        # ceiling.
        _validity_bound = is_validity_bound_deferred_retry_reason(reason)
        _count_exhaustion_applies = deferred_retry_count_exhaustion_applies(
            reason, selector_failure=selector_failure
        )
        max_attempts: Optional[int] = None
        if not _validity_bound and _count_exhaustion_applies:
            if not is_retryable_selector_reason(reason):
                return None

        if any(str(meta.get(field) or "").strip() for field in (
            "submit_intent_at",
            "broker_submit_key",
            "broker_submit_payload_hash",
            "recovery_submit_owner",
            "recovery_submit_lease_until",
        )) or meta.get("recovery_submit_fenced") is True:
            return _RowOutcome.UNRESOLVED
        if not self._inside_retry_entry_window(row):
            return None

        broker_status, broker_tags = self._load_authoritative_broker_order_tags()
        exact_tag = canonical_broker_submit_key(local_oid)
        if broker_status not in {
            ORDERS_AVAILABLE_COMPLETE,
            ORDERS_AVAILABLE_EMPTY,
        } or exact_tag in broker_tags:
            self._mark_failure(
                local_oid,
                "phase_one_broker_order_found"
                if exact_tag in broker_tags
                else "phase_one_broker_truth_unknown",
            )
            return _RowOutcome.UNRESOLVED

        if max_attempts is None:
            try:
                max_attempts = resolve_deferred_materialization_max_attempts()
            except DeferredMaterializationConfigConflict:
                return _RowOutcome.UNRESOLVED
        if not _validity_bound and _count_exhaustion_applies:
            if attempt >= max_attempts:
                return None
        max_attempts = max(max_attempts, attempt)
        # PR #568 amendment §2: bounded stepped backoff, not a fixed 8s.
        # Recovery inherits the same per-attempt cadence so a crashed +
        # restored phase-one claim cannot become a tight provider loop.
        from ap.selector_retry_policy import (
            compute_retry_backoff_seconds as _compute_backoff,
        )
        _base_delay = _env_int("BREACH_SELECTOR_RETRY_DELAY_SECONDS", 8)
        delay = max(
            int(_base_delay),
            _compute_backoff(
                attempt,
                reason,
                cfg={"validity_bound_retry_backoff_step1_seconds": _base_delay},
            ),
        )
        _candidate_next_dt = datetime.now(timezone.utc) + timedelta(seconds=delay)
        # PR #568 amendment §2: never schedule past the absolute entry
        # deadline. _inside_retry_entry_window(row) above already blocked
        # the "already past" case; this blocks the "backoff would step
        # past" case introduced by the ladder. Leave the row UNRESOLVED
        # so the ordinary lifecycle (deadline / EOD cutoff) terminates it
        # on the next poll instead of writing a doomed retry.
        try:
            _deadline_dt = self._retry_entry_deadline(row)
        except ValueError as _deadline_exc:
            log.warning(
                "RESTART_PHASE_ONE_RETRY_DEADLINE_AUTHORITY_INVALID "
                "local=%s error=%s -- leaving row unresolved.",
                local_oid,
                _deadline_exc,
            )
            return _RowOutcome.UNRESOLVED
        if _deadline_dt is not None and _candidate_next_dt >= _deadline_dt:
            return _RowOutcome.UNRESOLVED
        next_retry_at = _candidate_next_dt.isoformat()
        recover = getattr(
            self.osm, "recover_stale_market_truth_pending_retry", None
        )
        if not callable(recover):
            return _RowOutcome.UNRESOLVED
        try:
            recovered = bool(recover(
                local_oid,
                expected_owner=owner,
                generation=generation,
                attempt=attempt,
                max_attempts=max_attempts,
                signal_id=signal_id,
                execution_mode=self.execution_mode,
                reason_code=reason,
                next_retry_at=next_retry_at,
                selector_failure=selector_failure,
            ))
        except Exception:
            recovered = False
        if not recovered:
            return _RowOutcome.UNRESOLVED

        proof = self._verify_materialization_retry_ownership(
            local_oid,
            row,
            expected_next_at=next_retry_at,
            expected_attempts=attempt,
            expected_generation=generation,
            expected_reason=reason,
            expected_market_truth_pending=True,
            expected_ownerless=True,
        )
        if proof is None:
            return _RowOutcome.UNRESOLVED
        self._mark_retry_subtype(local_oid, _RETRY_MATERIALIZATION)
        log.warning(
            "RESTART_PHASE_ONE_RETRY_RECOVERED local=%s generation=%s "
            "attempt=%s reason=%s broker_submission=ABSENT selector_calls=0",
            local_oid,
            generation,
            attempt,
            reason,
        )
        return _RowOutcome.RETRY_OWNED

    # ── Canonical retry ownership (#323 fields) ────────────────────────────────

    def _enter_canonical_retry(self, local_oid: str, row: dict, *, reason: str) -> str:
        """
        Fix 1: write the exact fields that ap/deferred_materializer.stamp_retry_pending()
        writes so the deployed #323 consumer can see and process the row.

        Real canonical schema (from stamp_retry_pending):
          materialization_status          = "RETRY_PENDING"
          broker_ready                    = False
          materialization_attempts        = int
          materialization_next_retry_at   = isoformat
          materialization_reason          = str
          materialization_last_failure_at = isoformat

        Removed: materialization_owner, materialization_retry_deadline,
                 materialization_attempt_count, materialization_retry_reason
                 (none of these exist in stamp_retry_pending).
        """
        _delay = _env_int("BREACH_SELECTOR_RETRY_DELAY_SECONDS", 8)
        try:
            _max = resolve_deferred_materialization_max_attempts()
        except DeferredMaterializationConfigConflict as _cfg_conflict:
            log.critical(
                "DEFERRED_MATERIALIZATION_MAX_ATTEMPTS_CONFIG_CONFLICT "
                "local=%s error=%s -- refusing retry claim.",
                local_oid, _cfg_conflict,
            )
            return _RowOutcome.UNRESOLVED
        _now   = datetime.now(timezone.utc)

        _meta     = _extract_meta(row)
        _attempts = int(_meta.get(_MAT_ATTEMPTS_FIELD) or 0) + 1
        _selector_failure = _meta.get("materialization_selector_failure")
        if not isinstance(_selector_failure, dict):
            _selector_failure = {}
        if (
            _attempts > _max
            and deferred_retry_count_exhaustion_applies(
                reason,
                selector_failure=_selector_failure,
            )
        ):
            log.warning(
                "RESTART_RECOVERY_CANONICAL_RETRY_EXHAUSTED local=%s attempts=%d max=%d — terminalizing",
                local_oid, _attempts, _max,
            )
            return self._terminalize_with_reason(
                local_oid, row,
                f"restart_recovery_canonical_retry_exhausted:{reason}",
                meta_patch={"restart_recovery_retry_attempts": _attempts},
            )

        _next = (_now + timedelta(seconds=_delay)).isoformat()
        _ok = self._safe_meta_update(local_oid, {
            _MAT_STATUS_FIELD:       "RETRY_PENDING",
            _MAT_BROKER_READY:       False,
            _MAT_ATTEMPTS_FIELD:     _attempts,
            _MAT_NEXT_RETRY_AT:      _next,
            _MAT_REASON_FIELD:       reason,
            _MAT_LAST_FAILURE_FIELD: _now.isoformat(),
            "restart_recovery_at":   _now.isoformat(),
        })
        if not _ok:
            log.critical(
                "RESTART_RECOVERY_CANONICAL_RETRY_WRITE_FAILED local=%s — UNRESOLVED",
                local_oid,
            )
            return _RowOutcome.UNRESOLVED

        proof = self._verify_materialization_retry_ownership(
            local_oid, row,
            expected_next_at=_next,
            expected_attempts=_attempts,
        )
        if proof is None:
            log.critical(
                "RESTART_RECOVERY_CANONICAL_RETRY_NOT_PROVEN local=%s — UNRESOLVED",
                local_oid,
            )
            return _RowOutcome.UNRESOLVED

        log.info(
            "RESTART_RECOVERY_CANONICAL_RETRY_OWNED local=%s proof=%s reason=%s",
            local_oid, proof, reason,
        )
        self._mark_retry_subtype(local_oid, _RETRY_MATERIALIZATION)
        return _RowOutcome.RETRY_OWNED


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
        _rr_meta   = _extract_meta(reread)

        if not _rr_oid or _rr_oid != local_oid:
            log.critical("RESTART_RECOVERY_REREAD_OID_MISMATCH local=%s got=%s", local_oid, _rr_oid)
            return _RowOutcome.UNRESOLVED
        if not _rr_client or _rr_client != self.client_id.lower():
            log.critical("RESTART_RECOVERY_REREAD_CLIENT_MISMATCH local=%s got=%s", local_oid, _rr_client)
            return _RowOutcome.UNRESOLVED
        if not _rr_mode or _rr_mode != self.execution_mode:
            log.critical("RESTART_RECOVERY_REREAD_MODE_MISMATCH local=%s got=%s", local_oid, _rr_mode)
            return _RowOutcome.UNRESOLVED
        if _rr_status not in _TERMINAL_STATUSES:
            log.critical(
                "RESTART_RECOVERY_REREAD_NOT_TERMINAL local=%s status=%s — UNRESOLVED",
                local_oid, _rr_status,
            )
            return _RowOutcome.UNRESOLVED
        durable_reasons = {
            str(reread.get("last_error") or "").strip(),
            str(_rr_meta.get("restart_recovery_terminal_reason") or "").strip(),
            str(_rr_meta.get("terminal_reason") or "").strip(),
            str(_rr_meta.get("reason_code") or "").strip(),
            str(_rr_meta.get("final_reason") or "").strip(),
            str(_rr_meta.get("watcher_invalidation_reason") or "").strip(),
        }
        watcher_audit = _rr_meta.get("watcher_audit")
        if isinstance(watcher_audit, dict):
            durable_reasons.add(str(watcher_audit.get("reason_code") or "").strip())
        durable_reasons.discard("")
        if reason and str(reason).strip() not in durable_reasons:
            log.critical(
                "RESTART_RECOVERY_REREAD_REASON_MISMATCH local=%s reason=%s durable=%s — UNRESOLVED",
                local_oid, reason, sorted(durable_reasons),
            )
            return _RowOutcome.UNRESOLVED

        log.info("RESTART_RECOVERY_TERMINALIZED local=%s status=%s reason=%s", local_oid, _rr_status, reason)
        return _RowOutcome.TERMINALIZED

    def _verify_materialization_retry_ownership(
        self,
        local_oid: str,
        row: dict,
        *,
        expected_next_at: str = "",
        expected_attempts: int = 0,
        expected_generation: int = 0,
        expected_reason: str = "",
        expected_market_truth_pending: Optional[bool] = None,
        expected_ownerless: bool = False,
    ) -> "dict | None":
        """
        Fix 1: prove canonical #323 retry ownership using the real stamp_retry_pending fields.

        Required fields: materialization_status=RETRY_PENDING, broker_ready=False,
          materialization_attempts (int >= 1), materialization_next_retry_at,
          materialization_reason, materialization_last_failure_at.

        Removed: materialization_owner, materialization_retry_deadline,
                 materialization_attempt_count, materialization_retry_reason.
        """
        get_fn = getattr(self.osm, "get_order", None)
        if not callable(get_fn):
            return None
        try:
            reread = get_fn(local_oid)
        except Exception:
            return None
        if not isinstance(reread, dict):
            return None

        status    = str(reread.get("status") or "").strip().upper()
        rr_oid    = str(reread.get("local_order_id") or "").strip()
        rr_client = str(reread.get("client_id") or reread.get("client_email") or "").strip().lower()
        rr_mode   = str(reread.get("execution_mode") or "").strip().lower()
        contract  = str(reread.get("contract") or "").strip()
        if status != "PENDING_TRIGGER":
            return None
        if not rr_oid or rr_oid != local_oid:
            return None
        if not rr_client or rr_client != self.client_id.lower():
            return None
        if not rr_mode or rr_mode != self.execution_mode:
            return None
        if not contract or not contract.upper().startswith("DEFERRED:"):
            return None
        if reread.get("broker_order_id") or reread.get("submitted_ts"):
            return None

        meta = _extract_meta(reread)
        materialization_outcome = str(
            meta.get("materialization_outcome") or ""
        ).strip().upper()
        if materialization_outcome and materialization_outcome not in {
            "RETRY_LATER_SELECTOR_BUDGET",
            "RETRY_LATER_DATA_UNAVAILABLE",
        }:
            return None
        mat_status   = str(meta.get(_MAT_STATUS_FIELD) or "").strip().upper()
        broker_ready = meta.get(_MAT_BROKER_READY)
        next_at      = str(meta.get(_MAT_NEXT_RETRY_AT) or "").strip()
        _selector_failure = meta.get("materialization_selector_failure")
        if not isinstance(_selector_failure, dict):
            _selector_failure = meta.get("selector_failure")
        if not isinstance(_selector_failure, dict):
            _selector_failure = {}
        reason, reason_error = resolve_deferred_retry_reason(
            meta,
            selector_failure=_selector_failure,
        )
        if reason_error:
            return None
        reason = reason or ""
        last_fail    = str(meta.get(_MAT_LAST_FAILURE_FIELD) or "").strip()
        try:
            attempts = int(meta.get(_MAT_ATTEMPTS_FIELD))
        except (TypeError, ValueError):
            return None

        try:
            _max = resolve_deferred_materialization_max_attempts()
        except DeferredMaterializationConfigConflict as _cfg_conflict:
            log.critical(
                "DEFERRED_MATERIALIZATION_MAX_ATTEMPTS_CONFIG_CONFLICT "
                "local=%s error=%s -- refusing retry claim.",
                local_oid, _cfg_conflict,
            )
            return None
        if mat_status != "RETRY_PENDING":
            return None
        if broker_ready is not False:
            return None
        if attempts < 1:
            return None
        if (
            attempts > _max
            and deferred_retry_count_exhaustion_applies(
                reason,
                selector_failure=_selector_failure,
            )
        ):
            return None
        if not next_at or not reason:
            return None
        next_dt = _parse_iso(next_at)
        if next_dt is None:
            return None
        if expected_next_at and next_at != expected_next_at:
            return None
        if expected_attempts and attempts != expected_attempts:
            return None
        if expected_generation:
            generation = meta.get("materialization_generation")
            if (
                isinstance(generation, bool)
                or not isinstance(generation, int)
                or generation != expected_generation
            ):
                return None
        if expected_reason and reason != expected_reason:
            return None
        if (
            expected_market_truth_pending is not None
            and meta.get("materialization_market_truth_pending")
            is not expected_market_truth_pending
        ):
            return None
        if expected_ownerless and any(
            str(meta.get(field) or "").strip()
            for field in (
                "materialization_owner",
                "materialization_lease_until",
                "current_owner",
                "watcher_token",
                "retry_owner",
                "recovery_owner",
            )
        ):
            return None
        return {
            "local_order_id":                local_oid,
            "materialization_status":        mat_status,
            "materialization_attempts":      attempts,
            "materialization_next_retry_at": next_at,
            "materialization_reason":        reason,
            "materialization_last_failure_at": last_fail,
        }


    def _verify_restart_rearm_retry_ownership(
        self,
        local_oid: str,
        row: dict,
        *,
        expected_owner: Optional[str] = None,
        expected_next_at: Optional[str] = None,
        expected_deadline: Optional[str] = None,
        expected_signal_id: str = "",
        allow_expired: bool = False,
        require_late_policy: bool = False,
    ) -> Optional[dict]:
        """Prove bounded pre-breach restart-rearm retry ownership."""
        get_fn = getattr(self.osm, "get_order", None)
        if not callable(get_fn):
            return None
        try:
            reread = get_fn(local_oid)
        except Exception:
            return None
        if not isinstance(reread, dict):
            return None

        status = str(reread.get("status") or "").strip().upper()
        rr_oid = str(reread.get("local_order_id") or "").strip()
        rr_client = str(reread.get("client_id") or "").strip().lower()
        rr_mode = str(reread.get("execution_mode") or "").strip().lower()
        rr_signal_id = str(reread.get("signal_id") or "").strip()
        if status != "PENDING_TRIGGER":
            return None
        if rr_oid != local_oid:
            return None
        if rr_client != self.client_id.lower():
            return None
        if self.execution_mode not in {"live", "paper"}:
            return None
        if rr_mode not in {"live", "paper"} or rr_mode != self.execution_mode:
            return None
        if not rr_signal_id or not expected_signal_id or rr_signal_id != expected_signal_id:
            return None
        if require_late_policy and not _late_attachment_policy_eligible(reread):
            return None
        if any(
            value is not None
            and not (isinstance(value, str) and not value.strip())
            for value in (
                reread.get("broker_order_id"),
                reread.get("submitted_ts"),
            )
        ):
            return None
        if is_active_materialization_in_flight(reread):
            return None
        # A late-attachment candidate may already have exact trigger-crossing
        # evidence: that is why it needs fresh market truth before a watcher can
        # decide whether the move remains actionable.  Broker handoff evidence
        # is never retry-safe, and ordinary (non-late-policy) rows retain the
        # original pre-breach-only retry fence.
        if has_broker_handoff_evidence(reread) or (
            _has_trigger_or_submit_evidence(reread)
            and not _late_attachment_policy_eligible(reread)
        ):
            return None

        meta = _extract_meta(reread)
        restart_status_raw = meta.get(_RR_STATUS_FIELD)
        owner_raw = meta.get(_RR_OWNER_FIELD)
        reason_raw = meta.get(_RR_REASON_FIELD)
        next_at_raw = meta.get(_RR_NEXT_AT_FIELD)
        deadline_raw = meta.get(_RR_DEADLINE_FIELD)
        first_failed_at_raw = meta.get(_RR_FIRST_FAILED_AT)
        rr_client_meta_raw = meta.get(_RR_CLIENT_FIELD)
        rr_mode_meta_raw = meta.get(_RR_MODE_FIELD)
        if not all(
            isinstance(value, str)
            for value in (
                restart_status_raw,
                owner_raw,
                reason_raw,
                next_at_raw,
                deadline_raw,
                rr_client_meta_raw,
                rr_mode_meta_raw,
            )
        ):
            return None
        restart_status = restart_status_raw.strip().upper()
        owner = owner_raw.strip()
        reason = reason_raw.strip()
        next_at = next_at_raw.strip()
        deadline = deadline_raw.strip()
        first_failed_at = (
            first_failed_at_raw.strip()
            if isinstance(first_failed_at_raw, str)
            else ""
        )
        rr_client_meta = rr_client_meta_raw.strip().lower()
        rr_mode_meta = rr_mode_meta_raw.strip().lower()
        attempt_raw = meta.get(_RR_ATTEMPT_FIELD)
        if type(attempt_raw) is not int:
            return None
        attempt = attempt_raw
        if _RR_ATTEMPT_FIELD in reread:
            top_attempt = reread.get(_RR_ATTEMPT_FIELD)
            if type(top_attempt) is not int or top_attempt != attempt:
                return None
        max_attempts = _env_int("RESTART_REARM_RETRY_MAX_ATTEMPTS", 6)
        retry_deadline_secs = _env_int(
            "RESTART_REARM_RETRY_DEADLINE_SECONDS", 180
        )
        next_dt = _parse_retry_iso(next_at)
        deadline_dt = _parse_retry_iso(deadline)
        if restart_status != "RETRY_PENDING":
            return None
        if not owner or not reason:
            return None
        if attempt < 1 or attempt > max_attempts:
            return None
        if next_dt is None or deadline_dt is None or next_dt > deadline_dt:
            return None
        now = datetime.now(timezone.utc)
        if (
            _late_attachment_policy_eligible(reread)
            and deadline_dt > now + timedelta(seconds=retry_deadline_secs)
        ):
            # A durable late-retry owner is only valid for the configured
            # bounded recovery window.  Without this horizon check a
            # malformed future lease can hide an ownerless PENDING_TRIGGER
            # from readiness and make the monitor skip it indefinitely.
            return None
        if not allow_expired and now > deadline_dt:
            return None
        if rr_client_meta != self.client_id.lower() or rr_mode_meta != self.execution_mode:
            return None
        canonical_owner = (
            f"restart_rearm:{self.client_id}:{self.execution_mode}:{local_oid}"
        )
        if owner != canonical_owner:
            return None
        if expected_owner is not None and owner != expected_owner:
            return None
        if expected_next_at is not None and next_at != expected_next_at:
            return None
        if expected_deadline is not None and deadline != expected_deadline:
            return None
        return {
            "local_order_id": local_oid,
            "restart_rearm_status": restart_status,
            "restart_rearm_owner": owner,
            "restart_rearm_reason": reason,
            "restart_rearm_attempt": attempt,
            "restart_rearm_next_at": next_at,
            "restart_rearm_next_at_dt": next_dt,
            "restart_rearm_deadline": deadline,
            "restart_rearm_deadline_dt": deadline_dt,
            "restart_rearm_first_failed_at": first_failed_at,
        }

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

        # Expected identity from the row — all must be nonempty before scanning.
        _exp_sid    = str(row.get("signal_id") or "").strip()
        _exp_client = self.client_id.lower()
        _exp_mode   = self.execution_mode

        if not local_oid or not _exp_sid or not _exp_client or not _exp_mode:
            log.warning(
                "RESTART_RECOVERY registry proof aborted — incomplete expected identity "
                "local=%s signal_id=%s client=%s mode=%s",
                local_oid, _exp_sid, _exp_client, _exp_mode,
            )
            return None

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
                # local_order_id matched — now enforce all 5 remaining checks strictly.
                # All 4 identity fields must be nonempty AND matching.
                if not _w_client or _w_client != _exp_client:
                    log.warning(
                        "RESTART_RECOVERY registry client_id mismatch local=%s "
                        "expected=%s got=%r", local_oid, _exp_client, _w_client,
                    )
                    return None
                if not _w_mode or _w_mode != _exp_mode:
                    log.warning(
                        "RESTART_RECOVERY registry execution_mode mismatch local=%s "
                        "expected=%s got=%r", local_oid, _exp_mode, _w_mode,
                    )
                    return None
                if not _w_sid or _w_sid != _exp_sid:
                    log.warning(
                        "RESTART_RECOVERY registry signal_id mismatch local=%s "
                        "expected=%s got=%r", local_oid, _exp_sid, _w_sid,
                    )
                    return None
                if _quarant:
                    log.warning(
                        "RESTART_RECOVERY registry watcher is quarantined local=%s", local_oid,
                    )
                    return None
                _valid_states = {"PENDING", "REARM", "RETRY"}
                if _w_state.upper() not in _valid_states:
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

    def _check_watcher_owns(self, local_oid: str, row: dict) -> "bool | None":
        """
        Fix 3: row-aware ownership probe that uses the full 6-way proof.

        Returns True only when all identity checks pass.
        Returns False when watcher exists but proof fails (mismatched client/mode/sid).
        Returns None when watcher is unavailable.
        """
        watcher = self.entry_watcher
        if watcher is None:
            return None
        try:
            _pending = getattr(watcher, "_pending", None)
            if _pending is None:
                return None
        except Exception:
            return None
        proof = self._verify_registry_ownership(local_oid, row)
        if proof is not None:
            return True
        # If any watcher physically holds the local_order_id but proof failed,
        # return False (identity mismatch) rather than None (watcher absent).
        try:
            for w in _pending:
                _wsig = getattr(w, "signal", {}) or {}
                if str(_wsig.get("local_order_id") or "").strip() == local_oid:
                    return False  # present but failed proof
        except Exception:
            pass
        return False

    def _quote_state_for_row(self, row: dict) -> Optional[bool]:
        side = str(row.get("direction") or row.get("side") or "").strip().upper()
        symbol = str(row.get("ticker") or row.get("underlying") or row.get("symbol") or "").strip()
        trigger = _canonical_underlying_trigger(row)
        if not side or not symbol or trigger is None:
            return None
        try:
            return self.quote_check_fn(self.broker, symbol, side, trigger)
        except Exception:
            return None

    def _mark_retry_subtype(self, local_oid: str, subtype: str) -> None:
        if local_oid:
            self._row_retry_subtypes[str(local_oid)] = str(subtype)

    def _mark_failure(self, local_oid: str, reason: str) -> None:
        if local_oid:
            self._row_failure_reasons[str(local_oid)] = str(reason)

    def _log_identity_failure(
        self,
        marker: str,
        *,
        local_oid: str,
        signal_id: str,
        durable_client: str,
        durable_mode: str,
    ) -> None:
        log.critical(
            "%s local_order_id=%s signal_id=%s expected_client=%s durable_client=%s "
            "expected_mode=%s durable_mode=%s caller_source=%s",
            marker,
            local_oid,
            signal_id,
            self.client_id,
            durable_client,
            self.execution_mode,
            durable_mode,
            self.caller_source,
        )

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

def _build_summary(
    client_id: str,
    execution_mode: str,
    outcomes: dict[str, str],
    *,
    retry_subtypes: Optional[dict[str, str]] = None,
    failure_reasons: Optional[dict[str, str]] = None,
) -> dict:
    _all = list(outcomes.values())
    retry_subtypes = retry_subtypes or {}
    failure_reasons = failure_reasons or {}
    unresolved_row_ids = {
        str(row_id): outcome
        for row_id, outcome in outcomes.items()
        if outcome == _RowOutcome.UNRESOLVED
    }
    resolved_row_ids = {
        str(row_id): outcome
        for row_id, outcome in outcomes.items()
        if outcome in {
            _RowOutcome.WATCHER_OWNED,
            _RowOutcome.RETRY_OWNED,
            _RowOutcome.REARM_OWNED,
            _RowOutcome.TERMINALIZED,
            _RowOutcome.SKIPPED,
            _RowOutcome.MATERIALIZATION_OWNED,  # PR #521: active materializer owns
        }
    }
    return {
        "client_id":                        client_id,
        "execution_mode":                   execution_mode,
        "rows_examined":                    len(_all),
        "watchers_rearmed":                 _all.count(_RowOutcome.WATCHER_OWNED),
        "watcher_owned_count":              _all.count(_RowOutcome.WATCHER_OWNED),
        "retry_rows_owned":                 _all.count(_RowOutcome.RETRY_OWNED),
        "materialization_retry_owned_count": sum(
            1 for v in retry_subtypes.values() if v == _RETRY_MATERIALIZATION
        ),
        "restart_rearm_retry_owned_count":  sum(
            1 for v in retry_subtypes.values() if v == _RETRY_RESTART_REARM
        ),
        "watcher_retry_owned_count":         sum(
            1 for v in retry_subtypes.values() if v == _RETRY_WATCHER
        ),
        "rearm_rows_owned":                 _all.count(_RowOutcome.REARM_OWNED),
        "terminalized":                     _all.count(_RowOutcome.TERMINALIZED),
        "terminalized_count":               _all.count(_RowOutcome.TERMINALIZED),
        "skipped_not_pending_trigger":      _all.count(_RowOutcome.SKIPPED),
        "skipped_count":                    _all.count(_RowOutcome.SKIPPED),
        "unresolved_cleanup_failures":      _all.count(_RowOutcome.UNRESOLVED),
        "identity_failure_count":           sum(
            1 for v in failure_reasons.values() if str(v).startswith("identity:")
        ),
        "retry_verification_failure_count": sum(
            1 for v in failure_reasons.values() if str(v).startswith("retry_verification:")
        ),
        # PR #521: rows where an active materializer was observed read-only
        "materialization_in_flight_count":  _all.count(_RowOutcome.MATERIALIZATION_OWNED),
        # Blocker 2: ownerless = count(UNRESOLVED) — not arithmetic
        "ownerless_rows_remaining":         len(unresolved_row_ids),
        "resolved_row_ids":                 resolved_row_ids,
        "unresolved_row_ids":               unresolved_row_ids,
        "retry_subtypes":                   dict(retry_subtypes),
        "failure_reasons":                  dict(failure_reasons),
        "row_outcomes":                     dict(outcomes),
    }


def _emit_summary(summary: dict) -> None:
    log.info(
        "PENDING_TRIGGER_RESTART_RECOVERY_SUMMARY "
        "client=%s mode=%s examined=%d "
        "rearmed=%d retry=%d terminalized=%d skipped=%d "
        "inflight=%d unresolved=%d ownerless=%d",
        summary["client_id"], summary["execution_mode"],
        summary["rows_examined"],
        summary["watchers_rearmed"], summary["retry_rows_owned"],
        summary["terminalized"], summary["skipped_not_pending_trigger"],
        summary["materialization_in_flight_count"],
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

class _RecoveryPlan(SimpleNamespace):
    """Attribute-first restart plan with a read-only legacy ``get`` shim.

    ``APEntryWatcher.watch()`` consumes recovery plans through attributes.
    The shim keeps older recovery observers that only read ``plan.get`` from
    breaking without turning the plan back into a plain dict.
    """

    def get(self, name: str, default=None):
        return getattr(self, name, default)


def _build_plan(row: dict, plan_builder_fn=None) -> Optional[Any]:
    if plan_builder_fn is not None:
        try:
            return plan_builder_fn(row)
        except Exception:
            return None
    try:
        meta = _extract_meta(row)
        late_policy_eligible = _late_attachment_policy_eligible(row)
        plan_metadata = dict(meta)
        # Never forward a truthy string/number/container into APEntryWatcher.
        # The reconstructed plan carries one normalized boolean authority.
        plan_metadata["late_attachment_policy_eligible"] = late_policy_eligible
        signal_id = str(row.get("signal_id") or meta.get("signal_id") or "")
        canonical_signal_id = str(
            row.get("canonical_signal_id")
            or meta.get("canonical_signal_id")
            or ""
        )
        local_order_id = str(row.get("local_order_id") or "")
        client_id = str(row.get("client_id") or meta.get("client_id") or "")
        execution_mode = str(
            row.get("execution_mode") or meta.get("execution_mode") or ""
        ).strip().lower()
        ticker = str(
            row.get("ticker")
            or row.get("symbol")
            or meta.get("ticker")
            or meta.get("symbol")
            or ""
        ).upper()
        side = str(row.get("direction") or row.get("side") or meta.get("side") or "").strip().upper()
        trigger = float(
            _canonical_underlying_trigger(row)
            or row.get("trigger_price")
            or row.get("entry_price")
            or meta.get("trigger_price")
            or meta.get("entry_trigger")
            or meta.get("signal_entry_price")
            or 0
        )
        stop = float(
            row.get("stop_price")
            or row.get("stop_underlying")
            or meta.get("stop_price")
            or meta.get("stop_underlying")
            or 0
        )
        target = float(
            row.get("target_price")
            or row.get("target_underlying")
            or meta.get("target_price")
            or meta.get("target_underlying")
            or 0
        )
        generation = meta.get("materialization_generation")
        try:
            generation = int(generation) if generation is not None else None
        except (TypeError, ValueError):
            generation = None
        return _RecoveryPlan(
            signal_id=signal_id,
            canonical_signal_id=canonical_signal_id,
            plan_id=str(row.get("plan_id") or meta.get("plan_id") or ""),
            ticker=ticker,
            side=side,
            direction=side,
            entry_trigger=trigger,
            trigger_price=trigger,
            stop_underlying=stop,
            stop_price=stop,
            target_underlying=target,
            target_price=target,
            client_id=client_id,
            client_email=client_id or str(row.get("client_email") or ""),
            execution_mode=execution_mode,
            local_order_id=local_order_id,
            materialization_generation=generation,
            trigger_crossed_at=(
                row.get("trigger_crossed_at")
                or meta.get("trigger_crossed_at")
            ),
            late_attachment_policy_eligible=late_policy_eligible,
            metadata=plan_metadata,
            score=float(row.get("score") or meta.get("score") or 0),
            tier=row.get("tier") or meta.get("tier") or "",
            timeframe=row.get("timeframe") or meta.get("timeframe") or "",
            contracts=int(row.get("contracts") or row.get("qty") or meta.get("selected_qty") or 0),
            quantity=int(row.get("quantity") or row.get("qty") or meta.get("selected_qty") or 0),
            contract_symbol=str(
                row.get("contract_symbol")
                or row.get("contract")
                or meta.get("contract_symbol")
                or meta.get("selected_contract")
                or ""
            ),
            contract=str(row.get("contract") or meta.get("selected_contract") or ""),
            limit_price=float(row.get("limit_price") or meta.get("selected_limit") or 0),
        )
    except Exception:
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_meta(row: dict) -> dict:
    meta = (row or {}).get("meta") or {}
    if isinstance(meta, str):
        try:
            import json
            meta = json.loads(meta)
        except Exception:
            meta = {}
    return meta if isinstance(meta, dict) else {}


def _canonical_underlying_trigger(row: dict) -> Optional[float]:
    """Resolve only authoritative underlying trigger fields."""
    meta = _extract_meta(row)
    candidates = (
        meta.get("trigger_price"),
        meta.get("entry_trigger"),
        (row or {}).get("trigger_price"),
        (row or {}).get("entry_trigger"),
        (row or {}).get("plan_trigger_price"),
    )
    for raw in candidates:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return None


def _retry_subtype(row: dict) -> str:
    meta = _extract_meta(row)
    mat_status = str(meta.get(_MAT_STATUS_FIELD) or "").strip().upper()
    rr_status = str(meta.get(_RR_STATUS_FIELD) or "").strip().upper()
    if mat_status == "RETRY_PENDING" or meta.get(_MAT_NEXT_RETRY_AT):
        return _RETRY_MATERIALIZATION
    if rr_status == "RETRY_PENDING" or meta.get(_RR_NEXT_AT_FIELD):
        return _RETRY_RESTART_REARM
    if meta.get("watcher_retry_owner") or meta.get("watcher_retry_next_at"):
        return _RETRY_WATCHER
    return ""


def _late_attachment_policy_eligible(row: dict) -> bool:
    """Return True only for an exact, non-conflicting durable boolean grant.

    Metadata is the normal persisted authority.  A top-level mirror is
    tolerated for legacy/test row shapes only when it is also an exact bool;
    if both surfaces exist they must agree.  Generic truthiness never grants
    the late market-truth lifecycle.
    """
    if not isinstance(row, dict):
        return False
    meta = _extract_meta(row)
    top_present = "late_attachment_policy_eligible" in row
    meta_present = "late_attachment_policy_eligible" in meta
    top_value = row.get("late_attachment_policy_eligible")
    meta_value = meta.get("late_attachment_policy_eligible")

    if top_present and type(top_value) is not bool:
        return False
    if meta_present and type(meta_value) is not bool:
        return False
    if top_present and meta_present and top_value is not meta_value:
        return False
    if meta_present:
        return meta_value is True
    if top_present:
        return top_value is True
    return False


def _has_trigger_or_submit_evidence(row: dict) -> bool:
    meta = _extract_meta(row)
    if (row or {}).get("broker_order_id") or (row or {}).get("submitted_ts"):
        return True
    evidence_keys = (
        "submit_intent_at",
        "broker_submit_key",
        "trigger_confirmed",
        "trigger_crossed_at",
        "materialization_status",
        "materialization_next_retry_at",
        "materialization_owner",
        "materialization_outcome",
    )
    for key in evidence_keys:
        if meta.get(key):
            return True
    watcher_audit = meta.get("watcher_audit")
    if isinstance(watcher_audit, dict) and str(watcher_audit.get("reason_code") or "") == "trigger_ready":
        return True
    return False


def _parse_iso(raw) -> Optional[datetime]:
    if not raw:
        return None
    try:
        text = str(raw).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _parse_retry_iso(raw) -> Optional[datetime]:
    """Parse a durable retry timestamp only when it is timezone-aware."""
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        text = raw.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None or dt.utcoffset() is None:
            return None
        return dt
    except Exception:
        return None


def _quote_is_fresh(raw: dict) -> bool:
    ts = (
        raw.get("timestamp")
        or raw.get("quote_ts")
        or raw.get("updated_at")
        or raw.get("as_of")
    )
    if not ts:
        return True
    dt = _parse_iso(ts)
    if dt is None:
        return False
    max_age = _env_int("RESTART_RECOVERY_QUOTE_MAX_AGE_SECONDS", 120)
    age = datetime.now(timezone.utc) - dt
    return timedelta(seconds=0) <= age <= timedelta(seconds=max_age)
