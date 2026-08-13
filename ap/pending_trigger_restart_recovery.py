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

import os
import json
import math
import re
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from typing import Any, Optional

from ap.logger import get_logger
from ap_entry_watcher import (
    RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN,
    recovery_trigger_evidence_identity_is_proven,
)
from ap.broker_submit_identity import canonical_broker_submit_key
from ap.pending_trigger_classifier import (
    PendingTriggerClassification as PTC,
    classify_pending_trigger_row,
)
from ap.selector_retry_policy import (
    DeferredMaterializationConfigConflict,
    resolve_deferred_materialization_max_attempts,
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

# These are broker-owned timestamps only.  In particular, ``reconciled_at``
# and other local recovery timestamps must never become submission chronology.
_BROKER_SUBMITTED_TIMESTAMP_FIELDS = (
    "broker_submitted_ts",
    "broker_submitted_at",
    "submitted_ts",
    "submitted_at",
    "order_created_at",
    "create_date",
    "created_at",
)
_EXISTING_SUBMITTED_TIMESTAMP_FIELDS = (
    "submitted_ts",
    "broker_submitted_ts",
    "broker_submitted_at",
)
_BROKER_FILLED_TIMESTAMP_FIELDS = (
    "last_fill_date",
    "filled_at",
    "filled_ts",
    "transaction_date",
    "update_date",
    "updated_at",
)

# Broker order listings must carry the actual option contract before a
# DEFERRED:<ticker> recovery row can become broker-owned.  Keep this parser
# strict: a ticker-only ``symbol`` or another placeholder is not an OCC
# contract, and the fill monitor cannot safely derive position identity from
# either one.
_BROKER_OCC_CONTRACT_RE = re.compile(
    r"^(?P<root>[A-Z0-9.]{1,6})(?P<expiration>\d{6})(?P<right>[CP])(?P<strike>\d{8})$"
)
_BROKER_CONTRACT_FIELDS = (
    "option_symbol",
    "contract",
    "contract_symbol",
    "occ_symbol",
    "symbol",
)

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


def _broker_order_contract_values(payload: dict):
    """Yield OCC-looking contract values from normalized or raw broker data."""
    pending = [payload]
    seen: set[int] = set()
    while pending:
        node = pending.pop(0)
        if not isinstance(node, dict) or id(node) in seen:
            continue
        seen.add(id(node))
        for field_name in _BROKER_CONTRACT_FIELDS:
            raw = node.get(field_name)
            if raw is None:
                continue
            normalized = "".join(str(raw).upper().split())
            if normalized:
                yield normalized
        for nested_name in ("raw", "order", "details"):
            nested = node.get(nested_name)
            if isinstance(nested, dict):
                pending.append(nested)
            elif isinstance(nested, list):
                pending.extend(item for item in nested if isinstance(item, dict))


def _validated_broker_occ_contract(
    row: dict, broker_order: dict
) -> tuple[Optional[str], str]:
    """Return the one broker OCC contract proven for this deferred ENTRY."""
    if not isinstance(broker_order, dict):
        return None, "broker_contract_missing_or_invalid"

    valid_contracts: list[str] = []
    for candidate in _broker_order_contract_values(broker_order):
        match = _BROKER_OCC_CONTRACT_RE.fullmatch(candidate)
        if not match:
            continue
        try:
            datetime.strptime(match.group("expiration"), "%y%m%d")
        except (TypeError, ValueError):
            continue
        valid_contracts.append(candidate)

    distinct_contracts = set(valid_contracts)
    if not distinct_contracts:
        return None, "broker_contract_missing_or_invalid"
    if len(distinct_contracts) != 1:
        return None, "broker_contract_ambiguous"
    contract = valid_contracts[0]
    match = _BROKER_OCC_CONTRACT_RE.fullmatch(contract)
    if match is None:  # pragma: no cover - guarded by the loop above
        return None, "broker_contract_missing_or_invalid"

    row_ticker = "".join(
        str(
            row.get("ticker")
            or row.get("symbol")
            or row.get("underlying")
            or (_strict_recovery_meta(row) or {}).get("ticker")
            or ""
        ).upper().split()
    )
    if not row_ticker or match.group("root") != row_ticker:
        return None, "broker_contract_underlying_mismatch"

    direction = str(
        row.get("direction")
        or row.get("side")
        or (_strict_recovery_meta(row) or {}).get("side")
        or ""
    ).strip().upper()
    expected_right = {"CALL": "C", "PUT": "P"}.get(direction)
    if expected_right is None or match.group("right") != expected_right:
        return None, "broker_contract_direction_mismatch"
    return contract, "broker_contract_identity_present"


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
        position_check_fn=None,
        execution_core=None,
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
        self.position_check_fn = position_check_fn
        self.execution_core = execution_core
        self._row_retry_subtypes: dict[str, str] = {}
        self._row_failure_reasons: dict[str, str] = {}

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

        # A synthetic direction-reversal callback intentionally clears the
        # active trigger evidence before handing the row to recovery.  Route
        # that exact recovery-owned state before the ordinary evidence gate;
        # otherwise a valid rearm is mistaken for an unproven trigger and can
        # fall through to terminal cleanup on the next pass.
        _rearm_required_outcome = self._recover_direction_reversal_rearm_if_required(
            row, local_oid, plan_builder_fn=plan_builder_fn
        )
        if _rearm_required_outcome is not None:
            return _rearm_required_outcome

        # A completed direction-reversal handoff leaves the old trigger audit
        # as history while the durable watcher state becomes
        # WAITING_FOR_TRIGGER.  Once the exact runtime watcher proof is
        # present, this is a read-only owned row; do not let the historical
        # trigger_ready label route it back into terminal cleanup or the
        # confirmed-trigger evidence gate.
        _owned_meta = _strict_recovery_meta(row)
        if (
            watcher_owned is True
            and _owned_meta is not None
            and str(_owned_meta.get("materialization_status") or "").strip().upper()
            == "WAITING_FOR_TRIGGER"
            and _owned_meta.get("direction_reversal_rearm_requires_watcher") is False
            and str(_owned_meta.get("current_owner") or "").strip()
            and str(_owned_meta.get("watcher_token") or "").strip()
            and not str(_owned_meta.get("recovery_owner") or "").strip()
            and not str(_owned_meta.get("recovery_ownership") or "").strip()
        ):
            if self._verify_registry_ownership(local_oid, row) is not None:
                return _RowOutcome.WATCHER_OWNED

        # For an unowned row, keep the fail-closed fence before quote checks,
        # selector work, watcher admission, or any terminal/cleanup action.
        # A row with no timestamp remains an ordinary pre-breach candidate.
        if watcher_owned is not True and not _evidence_proven:
            return _reject_unproven_trigger_evidence()

        # A process can die after the exact #445 pre-broker claim commits but
        # before the callback starts.  Resolve that durable in-flight claim
        # before the stale trigger-ready classifier path gets a chance to
        # terminalize it.  The helper is proof-gated and returns None for
        # ordinary rows so the existing action table remains authoritative.
        _prebroker_inflight_outcome = self._recover_prebroker_inflight_claim_if_present(
            row, local_oid
        )
        if _prebroker_inflight_outcome is not None:
            return _prebroker_inflight_outcome

        _meta_for_classification = _extract_meta(row)
        _watcher_reason = str(
            (_meta_for_classification.get("watcher_audit") or {}).get("reason_code")
            if isinstance(_meta_for_classification.get("watcher_audit"), dict)
            else ""
        ).strip()
        _prebroker_proof = {"disposition": "NOT_CANDIDATE", "reason_code": ""}
        if _watcher_reason == "trigger_ready":
            _prebroker_proof = self._prove_stuck_trigger_ready_prebroker(
                row, local_oid
            )

        # Live quote check.
        live_quote_abt: Optional[bool] = None
        _side    = str(row.get("direction") or row.get("side") or "").strip().upper()
        _symbol  = str(row.get("ticker") or row.get("underlying") or row.get("symbol") or "").strip()
        _trigger = _canonical_underlying_trigger(row)
        # A trigger_ready row carries a stale breach decision. Do not spend a
        # fresh quote or let a transient quote result change the proof-gated
        # broker/identity decision below.
        if (
            _watcher_reason != "trigger_ready"
            and not _retry_subtype(row)
            and _symbol
            and _side
            and _trigger is not None
        ):
            try:
                live_quote_abt = self.quote_check_fn(self.broker, _symbol, _side, _trigger)
            except Exception as _qe:
                log.debug("RESTART_RECOVERY quote check failed local=%s: %s", local_oid, _qe)

        # Classification.
        cls = classify_pending_trigger_row(
            row,
            watcher_owned=watcher_owned,
            is_past_eod=self.is_past_eod,
            live_quote_already_through_trigger=live_quote_abt,
            prebroker_recovery_proven=(
                _prebroker_proof.get("disposition") == "PROVEN_ZERO_BROKER"
            ),
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

        # Every path that would classify, terminalize, retry, or rearm an
        # order with confirmed-trigger evidence still requires durable
        # lifecycle identity.  Only the proven already-owned fast path above
        # is allowed to return before this fence.
        if not _evidence_proven:
            return _reject_unproven_trigger_evidence()

        if cls == PTC.NOT_PENDING_TRIGGER:
            return _RowOutcome.SKIPPED

        elif cls == PTC.WAITING_VALID:
            # Not owned or proof failed: quote check then rearm.
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

        elif cls == PTC.STUCK_TRIGGER_READY_PREBROKER_RECOVERABLE:
            return self._recover_stuck_trigger_ready_prebroker(
                row, local_oid, proof=_prebroker_proof, plan_builder_fn=plan_builder_fn
            )

        elif cls == PTC.STUCK_TRIGGER_READY:
            if _prebroker_proof.get("disposition") == "BROKER_MATCH":
                return self._adopt_existing_broker_order(
                    local_oid, row, _prebroker_proof.get("broker_order") or {}
                )
            if _prebroker_proof.get("disposition") == "HOLD":
                self._mark_failure(
                    local_oid,
                    f"prebroker_proof:{_prebroker_proof.get('reason_code') or 'unavailable'}",
                )
                log.critical(
                    "RESTART_RECOVERY_PREBROKER_PROOF_HOLD local=%s reason=%s — "
                    "leaving row unchanged",
                    local_oid,
                    _prebroker_proof.get("reason_code"),
                )
                return _RowOutcome.UNRESOLVED
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

    # ── Exact pre-broker trigger-ready recovery (PR #445) ─────────────────────

    def _recover_prebroker_inflight_claim_if_present(
        self, row: dict, local_oid: str
    ) -> Optional[str]:
        """Converge an exact #445 claim that survived a process crash.

        ``claim_deferred_materialization()`` commits
        ``MATERIALIZING/RUNNING`` before the live callback is invoked.  A
        restart must not issue another claim or callback while that lease is
        live.  After lease expiry, broker and position truth are rechecked;
        an exact broker match is adopted, while proven zero ownership is
        transferred to the existing canonical ``RETRY_PENDING`` CAS using
        the same owner, generation, and retry attempt.

        ``None`` means the row is not in this narrow crash window.  Every
        candidate with malformed or conflicting proof returns UNRESOLVED and
        leaves durable/broker state unchanged.
        """
        initial_meta = _extract_meta(row)
        expected_owner = (
            f"prebroker_recovery:{self.client_id}:{self.execution_mode}:{local_oid}"
        )
        if (
            str(initial_meta.get("lifecycle_state") or "").strip().upper()
            != "MATERIALIZING"
            or str(initial_meta.get(_MAT_STATUS_FIELD) or "").strip().upper()
            != "RUNNING"
            or str(initial_meta.get("materialization_owner") or "").strip()
            != expected_owner
        ):
            return None

        get_order = getattr(self.osm, "get_order", None)
        if not callable(get_order):
            self._mark_failure(local_oid, "prebroker_inflight_reread_unavailable")
            return _RowOutcome.UNRESOLVED
        try:
            current = get_order(local_oid)
        except Exception as exc:
            self._mark_failure(
                local_oid, f"prebroker_inflight_reread_failed:{type(exc).__name__}"
            )
            return _RowOutcome.UNRESOLVED
        if not isinstance(current, dict):
            self._mark_failure(local_oid, "prebroker_inflight_reread_missing")
            return _RowOutcome.UNRESOLVED

        current_status = str(current.get("status") or "").strip().upper()
        if (
            current_status != "PENDING_TRIGGER"
            or str(current.get("broker_order_id") or "").strip()
            or current.get("submitted_ts")
        ):
            # The stale input row has already crossed the broker lifecycle;
            # do not run the pre-broker recovery path against it.
            return _RowOutcome.SKIPPED

        current_meta = _extract_meta(current)
        if (
            str(current_meta.get("lifecycle_state") or "").strip().upper()
            != "MATERIALIZING"
            or str(current_meta.get(_MAT_STATUS_FIELD) or "").strip().upper()
            != "RUNNING"
        ):
            # A peer may have already transferred the claim to canonical
            # retry ownership.  It is resolved, but this stale pass must not
            # re-enter selector/materialization work.
            if (
                str(current_meta.get(_MAT_STATUS_FIELD) or "").strip().upper()
                == "RETRY_PENDING"
                and current_meta.get(_MAT_NEXT_RETRY_AT)
            ):
                self._mark_retry_subtype(local_oid, _RETRY_MATERIALIZATION)
                return _RowOutcome.RETRY_OWNED
            self._mark_failure(local_oid, "prebroker_inflight_claim_changed")
            return _RowOutcome.UNRESOLVED

        proof = self._verify_prebroker_inflight_claim(current, local_oid)
        if proof is None:
            self._mark_failure(local_oid, "prebroker_inflight_claim_unproven")
            log.critical(
                "RESTART_RECOVERY_PREBROKER_INFLIGHT_UNPROVEN local=%s — "
                "leaving row unchanged",
                local_oid,
            )
            return _RowOutcome.UNRESOLVED

        self._mark_retry_subtype(local_oid, _RETRY_MATERIALIZATION)
        now = datetime.now(timezone.utc)
        if proof["lease_until_dt"] > now:
            log.info(
                "RESTART_RECOVERY_PREBROKER_INFLIGHT_LEASE_ACTIVE local=%s "
                "owner=%s generation=%s attempt=%s",
                local_oid,
                proof["owner"],
                proof["generation"],
                proof["attempt"],
            )
            return _RowOutcome.RETRY_OWNED

        position_state, position_reason = self._prebroker_position_truth(
            current, signal_id=proof["signal_id"], local_oid=local_oid
        )
        if position_state != "NO_MATCH":
            self._mark_failure(
                local_oid, f"prebroker_inflight_position:{position_reason}"
            )
            log.critical(
                "RESTART_RECOVERY_PREBROKER_INFLIGHT_POSITION_HOLD local=%s "
                "state=%s reason=%s — leaving row unchanged",
                local_oid,
                position_state,
                position_reason,
            )
            return _RowOutcome.UNRESOLVED

        broker_disposition, broker_order, broker_reason = (
            self._prebroker_inflight_broker_truth(
                current, local_oid, quantity=proof["quantity"]
            )
        )
        if broker_disposition == "HOLD":
            self._mark_failure(
                local_oid, f"prebroker_inflight_broker:{broker_reason}"
            )
            log.critical(
                "RESTART_RECOVERY_PREBROKER_INFLIGHT_BROKER_HOLD local=%s "
                "reason=%s — leaving row unchanged",
                local_oid,
                broker_reason,
            )
            return _RowOutcome.UNRESOLVED
        if broker_disposition == "MATCH":
            return self._adopt_existing_broker_order(
                local_oid, current, broker_order or {}
            )

        # The original claim remains the authority.  This is a transfer to
        # the existing due-retry owner, not a new generation or replacement
        # local order.
        return self._schedule_prebroker_materialization_retry(
            local_oid,
            current,
            owner=proof["owner"],
            generation=proof["generation"],
            attempt=proof["attempt"],
            reason="STUCK_TRIGGER_READY_RECOVERY_CLAIM_EXPIRED_NO_BROKER",
            signal_id=proof["signal_id"],
        )

    def _verify_prebroker_inflight_claim(
        self, row: dict, local_oid: str
    ) -> Optional[dict]:
        """Prove the exact durable post-claim, pre-callback state."""
        if str(row.get("kind") or "").strip().upper() != "ENTRY":
            return None
        if str(row.get("status") or "").strip().upper() != "PENDING_TRIGGER":
            return None
        if str(row.get("local_order_id") or "").strip() != local_oid:
            return None
        if str(row.get("client_id") or "").strip().lower() != self.client_id.lower():
            return None
        if str(row.get("execution_mode") or "").strip().lower() != self.execution_mode:
            return None
        if str(row.get("signal_id") or "").strip() == "":
            return None
        if str(row.get("broker_order_id") or "").strip() or row.get("submitted_ts"):
            return None

        meta = _strict_recovery_meta(row)
        if meta is None:
            return None
        if not str(row.get("contract") or "").strip().upper().startswith("DEFERRED:"):
            return None
        if str(meta.get("lifecycle_state") or "").strip().upper() != "MATERIALIZING":
            return None
        if str(meta.get(_MAT_STATUS_FIELD) or "").strip().upper() != "RUNNING":
            return None
        if meta.get("materialization_in_flight") is not True:
            return None
        if meta.get("broker_ready") is not False:
            return None

        owner = str(meta.get("materialization_owner") or "").strip()
        expected_owner = (
            f"prebroker_recovery:{self.client_id}:{self.execution_mode}:{local_oid}"
        )
        if owner != expected_owner:
            return None
        for owner_field in (
            "current_owner",
            "watcher_token",
            "materialization_retry_owner",
        ):
            if owner_field in meta and str(meta.get(owner_field) or "").strip() not in {
                "",
                expected_owner,
            }:
                return None

        signal_id = str(row.get("signal_id") or "").strip()
        if str(meta.get("signal_id") or signal_id).strip() != signal_id:
            return None
        if not recovery_trigger_evidence_identity_is_proven(row, local_oid):
            return None
        audit = meta.get("watcher_audit")
        if not isinstance(audit, dict) or str(audit.get("reason_code") or "").strip() != "trigger_ready":
            return None

        generation = _strict_positive_int(meta.get("materialization_generation"))
        attempt = _strict_positive_int(meta.get("retry_attempt"))
        if generation is None or attempt is None:
            return None
        for counter_name in ("breach_attempt_count", "materialization_attempts"):
            if counter_name in meta and meta.get(counter_name) not in (None, ""):
                counter = _strict_positive_int(meta.get(counter_name))
                if counter is None or counter != attempt:
                    return None

        # Any submit-intent or position marker makes broker ownership
        # ambiguous; it is never treated as zero broker ownership.
        for key in (
            "submit_intent_at",
            "broker_submit_key",
            "broker_submit_started_at",
            "broker_accepted_at",
            "broker_acceptance",
            "position_id",
            "position_local_order_id",
        ):
            if meta.get(key):
                return None
        if row.get("position_id"):
            return None

        lease_raw = meta.get("materialization_lease_until")
        lease_until_dt = _parse_timezone_aware(lease_raw)
        if lease_until_dt is None:
            return None

        quantity = _strict_positive_int(
            row.get("qty")
            if row.get("qty") is not None
            else row.get("quantity")
            if row.get("quantity") is not None
            else meta.get("selected_qty")
        )
        if quantity is None:
            return None
        return {
            "owner": owner,
            "generation": generation,
            "attempt": attempt,
            "signal_id": signal_id,
            "quantity": quantity,
            "lease_until_dt": lease_until_dt,
        }

    def _prebroker_inflight_broker_truth(
        self, row: dict, local_oid: str, *, quantity: int
    ) -> tuple[str, Optional[dict], str]:
        """Return MATCH, ZERO, or HOLD for exact broker ENTRY ownership."""
        list_orders = getattr(self.broker, "list_orders", None)
        if not callable(list_orders):
            return "HOLD", None, "broker_order_lookup_unavailable"
        try:
            broker_orders = list_orders()
        except Exception as exc:
            return "HOLD", None, f"broker_order_lookup_failed:{type(exc).__name__}"
        if not isinstance(broker_orders, list):
            return "HOLD", None, "broker_order_listing_malformed"

        expected_tag = canonical_broker_submit_key(local_oid)
        exact_matches = []
        for broker_order in broker_orders:
            if not isinstance(broker_order, dict):
                return "HOLD", None, "broker_order_listing_malformed"
            if str(broker_order.get("tag") or "").strip() == expected_tag:
                exact_matches.append(broker_order)
        if len(exact_matches) > 1:
            return "HOLD", None, "multiple_exact_broker_order_matches"
        if not exact_matches:
            return "ZERO", None, "exact_identity_no_broker_order"

        remote = exact_matches[0]
        remote_side = str(remote.get("side") or "").strip().lower().replace("-", "_")
        remote_qty = _strict_broker_quantity(remote.get("quantity"))
        if remote_side != "buy_to_open" or remote_qty != quantity:
            return "HOLD", None, "exact_broker_order_identity_conflict"
        remote_id = str(remote.get("id") or remote.get("order_id") or "").strip()
        if not remote_id:
            return "HOLD", None, "exact_broker_order_id_missing"
        _contract, contract_reason = _validated_broker_occ_contract(row, remote)
        if _contract is None:
            return "HOLD", None, contract_reason
        return "MATCH", remote, "exact_broker_order_identity_present"

    def _prove_stuck_trigger_ready_prebroker(self, row: dict, local_oid: str) -> dict:
        """Prove the one trigger-ready crash window that may continue.

        This is deliberately stricter than the historical classifier.  A
        positive result requires the complete durable plan/identity shape,
        confirmed-trigger provenance, no materialization/submit ownership, no
        matching position, and an authoritative broker order listing with no
        exact local-order tag.  Broker or position truth that cannot be read
        is a HOLD, never permission to submit.
        """

        def _unproven(reason: str) -> dict:
            return {"disposition": "UNPROVEN", "reason_code": str(reason)}

        def _hold(reason: str) -> dict:
            return {"disposition": "HOLD", "reason_code": str(reason)}

        meta = _strict_recovery_meta(row)
        if meta is None:
            return _unproven("malformed_meta")
        if str(row.get("kind") or "").strip().upper() != "ENTRY":
            return _unproven("kind_not_entry")
        if str(row.get("status") or "").strip().upper() != "PENDING_TRIGGER":
            return _unproven("status_not_pending_trigger")
        if str(row.get("broker_order_id") or "").strip() or row.get("submitted_ts"):
            return _unproven("broker_identity_already_present")

        audit = meta.get("watcher_audit")
        if not isinstance(audit, dict):
            return _unproven("watcher_audit_missing_or_malformed")
        if str(audit.get("reason_code") or "").strip() != "trigger_ready":
            return _unproven("watcher_reason_not_trigger_ready")
        if not recovery_trigger_evidence_identity_is_proven(row, local_oid):
            return _unproven(RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN)

        row_client = str(row.get("client_id") or "").strip().lower()
        row_mode = str(row.get("execution_mode") or "").strip().lower()
        if row_client != self.client_id.lower() or row_mode != self.execution_mode:
            return _unproven("identity_mismatch")
        if row_mode not in {"live", "paper"}:
            return _unproven("invalid_execution_mode")

        signal_id = str(row.get("signal_id") or meta.get("signal_id") or "").strip()
        plan_id = str(row.get("plan_id") or meta.get("plan_id") or "").strip()
        ticker = str(
            row.get("ticker")
            or row.get("symbol")
            or meta.get("ticker")
            or meta.get("symbol")
            or ""
        ).strip().upper()
        direction = str(
            row.get("direction") or row.get("side") or meta.get("side") or ""
        ).strip().upper()
        if not signal_id or not plan_id or not ticker or direction not in {"CALL", "PUT"}:
            return _unproven("missing_plan_identity")

        canonical_signal_id = str(
            row.get("canonical_signal_id")
            or meta.get("canonical_signal_id")
            or ""
        ).strip()
        provenance = meta.get("trigger_crossed_at_provenance")
        provenance_canonical = (
            str(provenance.get("canonical_signal_id") or "").strip()
            if isinstance(provenance, dict)
            else ""
        )
        if not provenance_canonical:
            return _unproven("canonical_signal_identity_missing")
        if canonical_signal_id and provenance_canonical != canonical_signal_id:
            return _unproven("canonical_signal_identity_conflict")

        trigger = _first_strict_positive_float(
            (
                meta.get("trigger_price"),
                meta.get("entry_trigger"),
                row.get("trigger_price"),
                row.get("entry_trigger"),
                row.get("plan_trigger_price"),
                row.get("entry_price"),
            )
        )
        if trigger is None:
            return _unproven("missing_or_invalid_trigger_price")

        qty_raw = (
            row.get("qty")
            if row.get("qty") is not None
            else row.get("quantity")
            if row.get("quantity") is not None
            else meta.get("selected_qty")
        )
        qty = _strict_positive_int(qty_raw)
        if qty is None:
            return _unproven("missing_or_invalid_quantity")

        score_raw = row.get("score") if row.get("score") is not None else meta.get("score")
        if score_raw is None or isinstance(score_raw, bool):
            return _unproven("missing_or_invalid_score")
        try:
            if not math.isfinite(float(score_raw)):
                return _unproven("missing_or_invalid_score")
        except (TypeError, ValueError):
            return _unproven("missing_or_invalid_score")
        if not str(row.get("tier") or meta.get("tier") or "").strip():
            return _unproven("missing_tier")
        if not str(row.get("timeframe") or meta.get("timeframe") or "").strip():
            return _unproven("missing_timeframe")

        contract = str(row.get("contract") or "").strip().upper()
        if not contract.startswith("DEFERRED:"):
            return _unproven("contract_not_deferred")
        if "contract_deferred" in meta and meta.get("contract_deferred") is not True:
            return _unproven("contract_deferred_flag_conflict")

        # A prior materializer, retry, broker-ready transition, descendant
        # generation, or direction-reversal owner is a different lifecycle.
        # It must not be re-entered through this initial crash-window claim.
        if str(meta.get("lifecycle_state") or "").strip().upper() not in {""}:
            return _unproven("materialization_lifecycle_already_owned")
        if str(meta.get("materialization_status") or "").strip().upper():
            return _unproven("materialization_status_already_owned")
        if meta.get("materialization_in_flight") is True or meta.get("broker_ready") is True:
            return _unproven("materialization_or_broker_ready_owned")
        if meta.get("direction_reversal_rearm_requires_watcher") is True:
            return _unproven("direction_reversal_requires_watcher")
        if any(
            meta.get(key)
            for key in (
                "materialization_owner",
                "materialization_retry_owner",
                "materialization_outcome",
                "materialization_next_retry_at",
                "submit_intent_at",
                "broker_submit_key",
                "broker_submit_started_at",
                "broker_accepted_at",
                "broker_acceptance",
                "broker_order_id",
            )
        ):
            return _unproven("submit_or_materialization_evidence_present")
        for counter_name in (
            "materialization_generation",
            "retry_attempt",
            "breach_attempt_count",
            "materialization_attempts",
        ):
            if counter_name not in meta or meta.get(counter_name) in (None, ""):
                continue
            counter = _strict_nonnegative_int(meta.get(counter_name))
            if counter is None:
                return _unproven(f"malformed_{counter_name}")
            if counter != 0:
                return _unproven(f"descendant_{counter_name}")

        position_state, position_reason = self._prebroker_position_truth(
            row, signal_id=signal_id, local_oid=local_oid
        )
        if position_state == "HOLD":
            return _hold(position_reason)
        if position_state == "MATCH":
            return _hold(position_reason)

        list_orders = getattr(self.broker, "list_orders", None)
        if not callable(list_orders):
            return _hold("broker_order_lookup_unavailable")
        try:
            broker_orders = list_orders()
        except Exception as exc:
            return _hold(f"broker_order_lookup_failed:{type(exc).__name__}")
        if not isinstance(broker_orders, list):
            return _hold("broker_order_listing_malformed")

        expected_tag = canonical_broker_submit_key(local_oid)
        exact_matches = []
        for broker_order in broker_orders:
            if not isinstance(broker_order, dict):
                return _hold("broker_order_listing_malformed")
            if str(broker_order.get("tag") or "").strip() == expected_tag:
                exact_matches.append(broker_order)
        if len(exact_matches) > 1:
            return _hold("multiple_exact_broker_order_matches")
        if exact_matches:
            remote = exact_matches[0]
            remote_side = str(remote.get("side") or "").strip().lower().replace("-", "_")
            remote_qty = _strict_broker_quantity(remote.get("quantity"))
            if remote_side != "buy_to_open" or remote_qty != qty:
                return _hold("exact_broker_order_identity_conflict")
            remote_id = str(remote.get("id") or remote.get("order_id") or "").strip()
            if not remote_id:
                return _hold("exact_broker_order_id_missing")
            _contract, contract_reason = _validated_broker_occ_contract(row, remote)
            if _contract is None:
                return _hold(contract_reason)
            return {
                "disposition": "BROKER_MATCH",
                "reason_code": "exact_broker_order_identity_present",
                "broker_order": remote,
            }

        return {
            "disposition": "PROVEN_ZERO_BROKER",
            "reason_code": "exact_identity_no_broker_or_position",
            "signal_id": signal_id,
            "plan_id": plan_id,
            "ticker": ticker,
            "direction": direction,
            "quantity": qty,
        }

    def _adopt_existing_broker_order(
        self, local_oid: str, row: dict, broker_order: dict
    ) -> str:
        """Adopt exact broker ownership and hand fill authority to the monitor."""
        transition = getattr(self.osm, "transition", None)
        remote_id = str(
            broker_order.get("id") or broker_order.get("order_id") or ""
        ).strip()
        if not remote_id or not callable(transition):
            self._mark_failure(local_oid, "broker_adoption_unavailable")
            return _RowOutcome.UNRESOLVED

        contract, contract_reason = _validated_broker_occ_contract(row, broker_order)
        if contract is None:
            self._mark_failure(local_oid, f"broker_adoption_{contract_reason}")
            return _RowOutcome.UNRESOLVED

        remote_status = str(broker_order.get("status") or "").strip().lower()
        remote_status = remote_status.replace("-", "_")

        # Recovery time is diagnostic only.  It is never a substitute for
        # broker chronology: a stale order must retain its actual submission
        # age, and an unknown broker timestamp must remain unknown.
        submitted_ts, submitted_ts_source, submitted_ts_malformed = (
            _first_broker_timestamp(
                broker_order,
                _BROKER_SUBMITTED_TIMESTAMP_FIELDS,
            )
        )
        if submitted_ts_malformed:
            submitted_ts = None
            submitted_ts_source = None
        filled_ts, filled_ts_source, filled_ts_malformed = _first_broker_timestamp(
            broker_order,
            _BROKER_FILLED_TIMESTAMP_FIELDS,
        )
        if filled_ts_malformed:
            filled_ts = None
            filled_ts_source = None

        now = _now_iso()
        reconciliation_patch = {
            "reconciled_at": now,
            "recovery_classification": "BROKER_ORDER_ADOPTED",
            "broker_reconcile_status": remote_status,
            "broker_reconcile_response": broker_order,
            "broker_reconcile_contract": contract,
            "broker_submitted_ts": submitted_ts,
            "broker_submitted_ts_source": (
                submitted_ts_source
                or ("malformed_unusable" if submitted_ts_malformed else "unavailable")
            ),
            "broker_filled_ts": filled_ts,
            "broker_filled_ts_source": filled_ts_source or "unavailable",
            "current_owner": "ORDER_MONITOR",
            "lifecycle_state": "SUBMITTED",
        }
        try:
            accepted = bool(
                transition(
                    local_oid,
                    "SUBMITTED",
                    broker_order_id=remote_id,
                    submitted_ts=submitted_ts,
                    contract=contract,
                    meta_patch=reconciliation_patch,
                    expected_contract=str(row.get("contract") or "").strip(),
                )
            )
        except Exception as exc:
            self._mark_failure(local_oid, f"broker_adoption_transition_failed:{type(exc).__name__}")
            return _RowOutcome.UNRESOLVED
        if not accepted:
            self._mark_failure(local_oid, "broker_adoption_transition_failed")
            return _RowOutcome.UNRESOLVED

        try:
            after = self.osm.get_order(local_oid)
        except Exception as exc:
            self._mark_failure(local_oid, f"broker_adoption_reread_failed:{type(exc).__name__}")
            return _RowOutcome.UNRESOLVED
        if not isinstance(after, dict):
            self._mark_failure(local_oid, "broker_adoption_reread_missing")
            return _RowOutcome.UNRESOLVED
        after_meta = _strict_recovery_meta(after)
        raw_evidence = (
            after_meta.get("broker_reconcile_response")
            if isinstance(after_meta, dict)
            else None
        )
        try:
            evidence_matches = (
                isinstance(raw_evidence, dict)
                and json.dumps(raw_evidence, sort_keys=True, default=str)
                == json.dumps(broker_order, sort_keys=True, default=str)
            )
        except Exception:
            evidence_matches = False
        if (
            str(after.get("broker_order_id") or "").strip() != remote_id
            or str(after.get("status") or "").strip().upper() != "SUBMITTED"
            or str(after.get("contract") or "").strip().upper() != contract
            or not evidence_matches
        ):
            self._mark_failure(local_oid, "broker_adoption_durable_identity_missing")
            return _RowOutcome.UNRESOLVED
        return _RowOutcome.SKIPPED

    def _prebroker_position_truth(
        self, row: dict, *, signal_id: str, local_oid: str
    ) -> tuple[str, str]:
        """Return MATCH/NO_MATCH/HOLD for exact persisted position proof.

        The production fallback is an exact client + local-order-or-signal
        query, never a bounded client-wide listing.  A reader result that is
        unavailable, malformed, or cannot prove exact mode/identity is HOLD;
        only an explicitly successful empty result is NO_MATCH.
        """
        if str(row.get("position_id") or "").strip():
            return "MATCH", "matching_position_id_present"
        meta = _strict_recovery_meta(row) or {}
        if str(meta.get("position_id") or "").strip():
            return "MATCH", "matching_position_id_present"

        check_fn = self.position_check_fn
        if callable(check_fn):
            try:
                raw_positions = check_fn(row)
            except Exception as exc:
                return "HOLD", f"position_lookup_failed:{type(exc).__name__}"
            return _interpret_position_truth(
                raw_positions,
                client_id=self.client_id,
                execution_mode=self.execution_mode,
                signal_id=signal_id,
                local_order_id=local_oid,
            )

        for method_name in (
            "get_positions_for_order",
            "get_positions_for_local_order",
            "get_position_for_order",
        ):
            method = getattr(self.osm, method_name, None)
            if not callable(method):
                continue
            try:
                raw_positions = method(local_oid)
            except Exception as exc:
                return "HOLD", f"position_lookup_failed:{type(exc).__name__}"
            return _interpret_position_truth(
                raw_positions,
                client_id=self.client_id,
                execution_mode=self.execution_mode,
                signal_id=signal_id,
                local_order_id=local_oid,
            )

        # The production OSM does not own a position reader. Use the exact
        # client + local_order_id/signal_id authority query when no narrower
        # injected/OSM reader exists. Do not substitute list_positions(): its
        # dashboard-oriented limit can hide an older matching position.
        try:
            from ap.db import get_positions_for_entry_identity

            raw_positions = get_positions_for_entry_identity(
                client_id=self.client_id,
                local_order_id=local_oid,
                signal_id=signal_id,
            )
        except Exception as exc:
            return "HOLD", f"position_lookup_failed:{type(exc).__name__}"
        return _interpret_position_truth(
            raw_positions,
            client_id=self.client_id,
            execution_mode=self.execution_mode,
            signal_id=signal_id,
            local_order_id=local_oid,
        )

    def _recover_stuck_trigger_ready_prebroker(
        self, row: dict, local_oid: str, *, proof: dict, plan_builder_fn=None
    ) -> str:
        """Claim and continue one exact pre-broker trigger-ready row."""
        if proof.get("disposition") != "PROVEN_ZERO_BROKER":
            self._mark_failure(
                local_oid,
                f"prebroker_proof:{proof.get('reason_code') or 'not_proven'}",
            )
            return _RowOutcome.UNRESOLVED
        watcher = self.entry_watcher
        callback = getattr(watcher, "on_trigger", None) if watcher is not None else None
        if not callable(callback):
            self._mark_failure(local_oid, "prebroker_callback_unavailable")
            return _RowOutcome.UNRESOLVED

        plan = _build_plan(row, plan_builder_fn)
        if plan is None:
            self._mark_failure(local_oid, "prebroker_plan_build_failed")
            return _RowOutcome.UNRESOLVED
        meta = _strict_recovery_meta(row) or {}
        trigger_raw = meta.get("trigger_crossed_at") or row.get("trigger_crossed_at")
        trigger_dt = _parse_timezone_aware(trigger_raw)
        trigger_price = _canonical_underlying_trigger(row)
        if trigger_dt is None or trigger_price is None:
            self._mark_failure(local_oid, RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN)
            return _RowOutcome.UNRESOLVED

        owner = f"prebroker_recovery:{self.client_id}:{self.execution_mode}:{local_oid}"
        generation = 1
        attempt = 1
        try:
            lock_ttl = _env_int("DEFERRED_MATERIALIZATION_LOCK_TTL_SECONDS", 120)
            lease_until = (
                datetime.now(timezone.utc) + timedelta(seconds=lock_ttl)
            ).isoformat()
        except Exception:
            return _RowOutcome.UNRESOLVED

        claim = getattr(self.osm, "claim_deferred_materialization", None)
        if not callable(claim):
            self._mark_failure(local_oid, "prebroker_claim_unavailable")
            return _RowOutcome.UNRESOLVED
        try:
            claimed = bool(
                claim(
                    local_oid,
                    owner=owner,
                    new_generation=generation,
                    lease_until=lease_until,
                    trigger_crossed_at=str(trigger_raw),
                    trigger_price=trigger_price,
                    observed_underlying_price=float(
                        meta.get("observed_underlying_price")
                        or meta.get("triggered_underlying_price")
                        or trigger_price
                    ),
                    signal_id=str(row.get("signal_id") or meta.get("signal_id") or ""),
                    execution_mode=self.execution_mode,
                    retry_attempt=attempt,
                )
            )
        except Exception as exc:
            self._mark_failure(local_oid, f"prebroker_claim_failed:{type(exc).__name__}")
            return _RowOutcome.UNRESOLVED
        if not claimed:
            self._mark_failure(local_oid, "prebroker_claim_not_acquired")
            return _RowOutcome.UNRESOLVED

        plan_meta = getattr(plan, "metadata", None) or {}
        if not isinstance(plan_meta, dict):
            plan_meta = {}
        plan_meta = dict(plan_meta)
        plan_meta.update(
            {
                "contract_deferred": True,
                "materialization_generation": generation,
                "materialization_owner": owner,
                "materialization_retry_owner": owner,
                "materialization_retry_attempt": attempt,
                "deferred_breach_selection": True,
                "selection_context": "stuck_trigger_ready_prebroker_recovery",
            }
        )
        try:
            plan.metadata = plan_meta
            plan.materialization_generation = generation
            plan.trigger_crossed_at = trigger_raw
        except Exception:
            self._mark_failure(local_oid, "prebroker_plan_metadata_unwritable")
            return _RowOutcome.UNRESOLVED

        ticker = str(getattr(plan, "ticker", "") or row.get("ticker") or "").upper()
        side = str(getattr(plan, "side", "") or row.get("direction") or "").upper()
        audit = meta.get("watcher_audit") if isinstance(meta.get("watcher_audit"), dict) else {}
        watched = SimpleNamespace(
            signal={
                "signal_id": str(row.get("signal_id") or meta.get("signal_id") or ""),
                "canonical_signal_id": str(
                    row.get("canonical_signal_id")
                    or meta.get("canonical_signal_id")
                    or (meta.get("trigger_crossed_at_provenance") or {}).get(
                        "canonical_signal_id"
                    )
                    or ""
                ),
                "local_order_id": local_oid,
                "client_id": self.client_id,
                "client_email": self.client_id,
                "execution_mode": self.execution_mode,
                "ticker": ticker,
                "side": side,
                "score": getattr(plan, "score", 0),
                "grade": getattr(plan, "tier", ""),
                "tier": getattr(plan, "tier", ""),
                "timeframe": getattr(plan, "timeframe", ""),
                "plan_id": getattr(plan, "plan_id", ""),
                "entry_price": trigger_price,
                "stop_price": getattr(plan, "stop_underlying", None),
                "target_price": getattr(plan, "target_underlying", None),
                "contract_symbol": str(getattr(plan, "contract_symbol", "") or ""),
                "contract_deferred": True,
                "trigger_crossed_at": trigger_raw,
                "metadata": plan_meta,
                "_approved_plan": plan,
                "ownership_kind": "materialization_retry",
                "owner": owner,
                "fenced": True,
                "recovery_submit_fenced": True,
                "recovery_submit_owner": owner,
                "recovery_submit_generation": generation,
                "retry_attempt": attempt,
                "materialization_generation": generation,
                "_recovery_pre_claimed": True,
                "_recovery_pre_claimed_owner": owner,
                "_recovery_pre_claimed_generation": generation,
                "_recovery_pre_claimed_attempt": attempt,
                "_recovery_pre_claimed_client_id": self.client_id,
                "_recovery_pre_claimed_mode": self.execution_mode,
            },
            ticker=ticker,
            side=side,
            trigger_price=trigger_price,
            entry_trigger=trigger_price,
            stop_level=getattr(plan, "stop_underlying", None),
            target_price=getattr(plan, "target_underlying", None),
            trigger_crossed_at=trigger_dt,
            triggered_at=trigger_dt,
            breach_price=float(
                meta.get("observed_underlying_price") or trigger_price
            ),
            last_quote_bid=_finite_float_or_zero(audit.get("current_bid")),
            last_quote_ask=_finite_float_or_zero(audit.get("current_ask")),
            last_quote_age_ms=None,
        )

        callback_result = None
        callback_exception = None
        try:
            callback_result = callback(watched)
        except Exception as exc:
            callback_exception = exc
            log.exception(
                "RESTART_RECOVERY_PREBROKER_CALLBACK_FAILED local=%s", local_oid
            )

        after = None
        try:
            after = self.osm.get_order(local_oid)
        except Exception as exc:
            self._mark_failure(local_oid, f"prebroker_post_callback_read_failed:{type(exc).__name__}")
            return _RowOutcome.UNRESOLVED
        if not isinstance(after, dict):
            self._mark_failure(local_oid, "prebroker_post_callback_row_missing")
            return _RowOutcome.UNRESOLVED

        after_status = str(after.get("status") or "").strip().upper()
        after_meta = _strict_recovery_meta(after)
        if after_meta is None:
            self._mark_failure(local_oid, "prebroker_post_callback_malformed_meta")
            return _RowOutcome.UNRESOLVED

        callback_disposition = (
            str(callback_result.get("disposition") or "").strip().upper()
            if isinstance(callback_result, dict)
            else ""
        )
        if callback_disposition == "REARM_WATCHER_REQUIRED":
            return self._consume_rearm_watcher_required(
                row,
                local_oid,
                after=after,
                callback_result=callback_result,
                owner=owner,
                generation=generation,
                plan_builder_fn=plan_builder_fn,
                adopt_durable=True,
            )
        if callback_disposition == "RECONCILE_BROKER_INTENT":
            return self._consume_reconcile_broker_intent(
                row,
                local_oid,
                after=after,
                callback_result=callback_result,
                owner=owner,
                generation=generation,
            )

        if after.get("broker_order_id") or after.get("submitted_ts") or after_status in {
            "SUBMITTED", "ACK", "ACKNOWLEDGED", "PARTIAL", "PARTIAL_FILL", "FILLED"
        }:
            return _RowOutcome.SKIPPED
        if after_status in _TERMINAL_STATUSES:
            return _RowOutcome.TERMINALIZED

        lifecycle = str(after_meta.get("lifecycle_state") or "").strip().upper()
        if lifecycle == "RETRY_WAIT" and (
            after_meta.get("materialization_next_retry_at") or after_meta.get("next_retry_at")
        ):
            self._mark_retry_subtype(local_oid, _RETRY_MATERIALIZATION)
            return _RowOutcome.RETRY_OWNED
        if after_meta.get("broker_ready") is True and after_status == "PENDING_TRIGGER":
            self._mark_retry_subtype(local_oid, _RETRY_MATERIALIZATION)
            return _RowOutcome.RETRY_OWNED

        # A callback exception or a missing durable callback outcome must move
        # the fenced claim to the existing bounded materialization retry state.
        if lifecycle == "MATERIALIZING" and str(
            after_meta.get("materialization_owner") or ""
        ).strip() == owner:
            reason = (
                f"STUCK_TRIGGER_READY_CALLBACK_EXCEPTION:{type(callback_exception).__name__}"
                if callback_exception is not None
                else "STUCK_TRIGGER_READY_NO_DURABLE_OUTCOME"
            )
            return self._schedule_prebroker_materialization_retry(
                local_oid,
                row,
                owner=owner,
                generation=generation,
                attempt=attempt,
                reason=reason,
                signal_id=str(row.get("signal_id") or meta.get("signal_id") or ""),
            )

        self._mark_failure(
            local_oid,
            "prebroker_callback_no_durable_outcome",
        )
        return _RowOutcome.UNRESOLVED

    def _recover_direction_reversal_rearm_if_required(
        self, row: dict, local_oid: str, *, plan_builder_fn=None
    ) -> Optional[str]:
        """Consume a durable synthetic direction-reversal rearm state.

        ``rearm_deferred_materialization_direction_reversal()`` deliberately
        clears the materialization lifecycle and marks the row as needing a
        real watcher.  A subsequent recovery pass therefore cannot use the
        ordinary ``trigger_ready`` proof/classifier without risking terminal
        cleanup.  Validate the exact row first, then reuse the existing
        watcher registration/adoption adapters.
        """
        meta = _strict_recovery_meta(row)
        if meta is None or meta.get("direction_reversal_rearm_requires_watcher") is not True:
            return None

        # APStartupRecovery has already validated the callback's exact
        # generation and identity before calling this engine, but it does not
        # itself register the watcher.  Consume the durable disposition here
        # so the due-retry path cannot fall through to the historical
        # trigger_ready classifier after active trigger evidence was cleared.

        status = str(row.get("status") or "").strip().upper()
        if status != "PENDING_TRIGGER":
            return None
        if row.get("broker_order_id") or row.get("submitted_ts"):
            return None
        if meta.get("submit_intent_at") or meta.get("broker_ready") is True:
            self._mark_failure(local_oid, "rearm_watcher_required_submit_evidence")
            return _RowOutcome.UNRESOLVED

        generation = _strict_generation(meta.get("materialization_generation"))
        owner = str(meta.get("recovery_owner") or "").strip()
        if (
            generation is None
            or not owner
            or str(meta.get("recovery_ownership") or "").strip()
            != "recovery_scheduler"
            or str(meta.get("lifecycle_state") or "").strip()
            or str(meta.get("materialization_status") or "").strip()
            or str(meta.get("current_owner") or "").strip()
            or str(meta.get("watcher_token") or "").strip()
            or meta.get("watcher_generation") not in (None, "", 0, "0")
        ):
            self._mark_failure(local_oid, "rearm_watcher_required_state_invalid")
            return _RowOutcome.UNRESOLVED

        # A durable rearm is safe to attach only with fresh market truth. A
        # missing quote belongs in the existing bounded restart-rearm retry;
        # it must not be confused with the callback-result handoff, which has
        # already completed its market-truth decision in the same invocation.
        if self._quote_state_for_row(row) is None:
            return self._enter_restart_rearm_retry(
                local_oid,
                row,
                reason="restart_recovery_quote_unavailable_before_rearm",
            )

        return self._consume_rearm_watcher_required(
            row,
            local_oid,
            after=row,
            callback_result=None,
            owner=owner,
            generation=generation,
            plan_builder_fn=plan_builder_fn,
            # APStartupRecovery performs the outer exact-generation adoption;
            # direct/order-monitor consumers complete it here when their OSM
            # exposes the same narrow CAS adapter.
            adopt_durable=(
                self.caller_source.startswith("ap.order_monitor.")
                and callable(
                    getattr(
                        self.osm,
                        "adopt_direction_reversal_watcher_ownership",
                        None,
                    )
                )
            ),
        )

    def _consume_rearm_watcher_required(
        self,
        row: dict,
        local_oid: str,
        *,
        after: dict,
        callback_result: Optional[dict],
        owner: str,
        generation: int,
        plan_builder_fn=None,
        adopt_durable: bool,
    ) -> str:
        """Route RWR through watcher registration and exact ownership CAS."""
        result = callback_result if isinstance(callback_result, dict) else {}
        expected_local = str(result.get("local_order_id") or local_oid).strip()
        expected_client = str(
            result.get("expected_client_id") or self.client_id
        ).strip().lower()
        expected_mode = str(
            result.get("expected_execution_mode") or self.execution_mode
        ).strip().lower()
        expected_signal = str(
            result.get("expected_signal_id") or row.get("signal_id") or ""
        ).strip()
        expected_canonical = str(
            result.get("expected_canonical_signal_id")
            or row.get("canonical_signal_id")
            or (_extract_meta(row) or {}).get("canonical_signal_id")
            or ""
        ).strip()
        expected_generation = _strict_generation(
            result.get("expected_generation") if result else generation
        )
        if (
            expected_local != local_oid
            or expected_client != self.client_id.lower()
            or expected_mode != self.execution_mode
            or not expected_signal
            or expected_generation is None
            or expected_generation != _strict_generation(generation)
        ):
            self._mark_failure(local_oid, "rearm_watcher_required_expectation_invalid")
            return _RowOutcome.UNRESOLVED

        after_meta = _strict_recovery_meta(after)
        if after_meta is None:
            self._mark_failure(local_oid, "rearm_watcher_required_meta_invalid")
            return _RowOutcome.UNRESOLVED
        after_canonical = str(
            after.get("canonical_signal_id")
            or after_meta.get("canonical_signal_id")
            or ""
        ).strip()
        if (
            str(after.get("local_order_id") or "").strip() != expected_local
            or str(after.get("client_id") or "").strip().lower() != expected_client
            or str(after.get("execution_mode") or "").strip().lower() != expected_mode
            or str(after.get("signal_id") or "").strip() != expected_signal
            or (expected_canonical and after_canonical != expected_canonical)
            or str(after.get("kind") or "").strip().upper() != "ENTRY"
            or str(after.get("status") or "").strip().upper() != "PENDING_TRIGGER"
            or after.get("broker_order_id")
            or after.get("submitted_ts")
            or after_meta.get("submit_intent_at")
            or after_meta.get("broker_ready") is True
            or _strict_generation(after_meta.get("materialization_generation"))
            != expected_generation
            or str(after_meta.get("recovery_ownership") or "").strip()
            != "recovery_scheduler"
            or str(after_meta.get("recovery_owner") or "").strip() != str(owner).strip()
            or after_meta.get("direction_reversal_rearm_requires_watcher") is not True
            or str(after_meta.get("lifecycle_state") or "").strip()
            or str(after_meta.get("materialization_status") or "").strip()
            or str(after_meta.get("current_owner") or "").strip()
            or str(after_meta.get("watcher_token") or "").strip()
            or after_meta.get("watcher_generation") not in (None, "", 0, "0")
        ):
            self._mark_failure(local_oid, "rearm_watcher_required_state_mismatch")
            return _RowOutcome.UNRESOLVED

        watcher = self.entry_watcher
        if watcher is None or not callable(getattr(watcher, "watch", None)):
            return (
                _RowOutcome.REARM_OWNED
                if self._retain_rearm_watcher_required(
                    local_oid,
                    after,
                    owner=owner,
                    generation=expected_generation,
                    signal_id=expected_signal,
                    canonical_signal_id=expected_canonical,
                    reason="watcher_unavailable",
                )
                else _RowOutcome.UNRESOLVED
            )

        watcher_outcome = self._rearm_and_verify(
            after,
            local_oid,
            plan_builder_fn=plan_builder_fn,
            preserve_on_reject=True,
        )
        if watcher_outcome != _RowOutcome.WATCHER_OWNED:
            return (
                _RowOutcome.REARM_OWNED
                if self._retain_rearm_watcher_required(
                    local_oid,
                    after,
                    owner=owner,
                    generation=expected_generation,
                    signal_id=expected_signal,
                    canonical_signal_id=expected_canonical,
                    reason="watcher_registration_not_proven",
                )
                else _RowOutcome.UNRESOLVED
            )

        if not adopt_durable:
            return _RowOutcome.WATCHER_OWNED

        watcher_token = str(getattr(watcher, "owner_token", "") or "").strip()
        adopt = getattr(self.osm, "adopt_direction_reversal_watcher_ownership", None)
        adopted = False
        if watcher_token and callable(adopt):
            try:
                adopted = bool(
                    adopt(
                        local_oid,
                        recovery_owner=str(owner).strip(),
                        watcher_token=watcher_token,
                        generation=expected_generation,
                        signal_id=expected_signal,
                        execution_mode=expected_mode,
                    )
                )
            except Exception as exc:
                log.error(
                    "RESTART_RECOVERY_REARM_WATCHER_ADOPTION_FAILED local=%s: %s",
                    local_oid,
                    exc,
                )

        if adopted:
            try:
                adopted_row = self.osm.get_order(local_oid)
            except Exception:
                adopted_row = None
            adopted_meta = _strict_recovery_meta(adopted_row) if isinstance(adopted_row, dict) else None
            if (
                isinstance(adopted_row, dict)
                and adopted_meta is not None
                and str(adopted_row.get("status") or "").strip().upper()
                == "PENDING_TRIGGER"
                and _strict_generation(adopted_meta.get("materialization_generation"))
                == expected_generation
                and str(adopted_meta.get("current_owner") or "").strip()
                == watcher_token
                and str(adopted_meta.get("watcher_token") or "").strip()
                == watcher_token
                and adopted_meta.get("direction_reversal_rearm_requires_watcher") is False
                and not str(adopted_meta.get("recovery_owner") or "").strip()
                and not str(adopted_meta.get("recovery_ownership") or "").strip()
            ):
                return _RowOutcome.WATCHER_OWNED
            # A successful CAS is authoritative even when this verification
            # reread is inconclusive. Do not evict the watcher or overwrite
            # its durable ownership with recovery retention.
            log.critical(
                "RESTART_RECOVERY_REARM_ADOPTION_VERIFY_INCONCLUSIVE local=%s "
                "— CAS succeeded; leaving watcher ownership authoritative",
                local_oid,
            )
            return _RowOutcome.WATCHER_OWNED

        if self.last_watcher_registered_by_this_attempt:
            registration_token = str(self.last_registration_token or "").strip()
            if registration_token:
                self._evict_just_registered_watcher(
                    local_oid,
                    signal_id=expected_signal,
                    client_id=expected_client,
                    execution_mode=expected_mode,
                    expected_registration_token=registration_token,
                )

        return (
            _RowOutcome.REARM_OWNED
            if self._retain_rearm_watcher_required(
                local_oid,
                after,
                owner=owner,
                generation=expected_generation,
                signal_id=expected_signal,
                canonical_signal_id=expected_canonical,
                reason="watcher_ownership_adoption_failed",
            )
            else _RowOutcome.UNRESOLVED
        )

    def _evict_just_registered_watcher(
        self,
        local_oid: str,
        *,
        signal_id: str,
        client_id: str,
        execution_mode: str,
        expected_registration_token: str,
    ) -> bool:
        """Evict only this call's exact watcher registration after CAS loss."""
        watcher = self.entry_watcher
        pending = getattr(watcher, "_pending", None)
        if pending is None:
            return True

        lock = getattr(watcher, "_lock", None)

        def _evict():
            target = None
            for candidate in list(pending):
                signal = getattr(candidate, "signal", {}) or {}
                if (
                    str(signal.get("local_order_id") or "").strip() == local_oid
                    and str(signal.get("signal_id") or "").strip() == signal_id
                    and str(signal.get("client_id") or "").strip().lower()
                    == client_id
                    and str(signal.get("execution_mode") or "").strip().lower()
                    == execution_mode
                ):
                    target = candidate
                    break
            if target is None:
                return True
            if str(getattr(target, "_registration_token", "") or "").strip() != expected_registration_token:
                log.warning(
                    "RESTART_RECOVERY_REARM_EVICTION_FOREIGN_WATCHER local=%s",
                    local_oid,
                )
                return True
            watcher._pending = [candidate for candidate in watcher._pending if candidate is not target]
            release = getattr(target, "_release_dedup_key", None)
            if callable(release):
                release()
            else:
                dedup = getattr(watcher, "_dedup_set", None)
                if isinstance(dedup, set):
                    dedup.discard(signal_id)
            return True

        try:
            if lock is not None:
                with lock:
                    return bool(_evict())
            return bool(_evict())
        except Exception as exc:
            log.error(
                "RESTART_RECOVERY_REARM_EVICTION_FAILED local=%s exc=%s",
                local_oid,
                exc,
            )
            return False

    def _retain_rearm_watcher_required(
        self,
        local_oid: str,
        row: dict,
        *,
        owner: str,
        generation: int,
        signal_id: str,
        canonical_signal_id: str,
        reason: str,
    ) -> bool:
        """Retain a proven RWR state through the existing fenced OSM adapter."""
        retain = getattr(
            self.osm,
            "retain_rearm_watcher_required_recovery_ownership",
            None,
        )
        if callable(retain):
            try:
                return bool(
                    retain(
                        local_oid,
                        recovery_owner=str(owner).strip(),
                        reason=f"rearm_watcher_required_{reason}",
                        recovery_retention_mode=self.execution_mode.upper(),
                        client_id=self.client_id,
                        signal_id=signal_id,
                        execution_mode=self.execution_mode,
                        generation=generation,
                        expected_recovery_owner=str(owner).strip(),
                        canonical_signal_id=canonical_signal_id,
                    )
                )
            except Exception as exc:
                log.error(
                    "RESTART_RECOVERY_REARM_RETENTION_FAILED local=%s: %s",
                    local_oid,
                    exc,
                )
                return False

        retain = getattr(self.osm, "retain_recovery_ownership_if_no_watcher", None)
        if callable(retain):
            try:
                return bool(
                    retain(
                        local_oid,
                        recovery_owner=str(owner).strip(),
                        reason=f"rearm_watcher_required_{reason}",
                        recovery_retention_mode=self.execution_mode.upper(),
                    )
                )
            except Exception as exc:
                log.error(
                    "RESTART_RECOVERY_REARM_GENERIC_RETENTION_FAILED local=%s: %s",
                    local_oid,
                    exc,
                )
                return False
        return False

    def _consume_reconcile_broker_intent(
        self,
        row: dict,
        local_oid: str,
        *,
        after: dict,
        callback_result: dict,
        owner: str,
        generation: int,
    ) -> str:
        """Consume the callback's broker-intent handoff exactly once."""
        after_meta = _strict_recovery_meta(after)
        expected_generation = _strict_generation(generation)
        if (
            after_meta is None
            or expected_generation is None
            or str(after.get("local_order_id") or "").strip() != local_oid
            or str(after.get("client_id") or "").strip().lower() != self.client_id.lower()
            or str(after.get("execution_mode") or "").strip().lower() != self.execution_mode
            or str(after.get("signal_id") or "").strip() != str(row.get("signal_id") or "").strip()
            or _strict_generation(after_meta.get("materialization_generation"))
            != expected_generation
            or not after_meta.get("submit_intent_at")
        ):
            self._mark_failure(local_oid, "reconcile_broker_intent_state_mismatch")
            return _RowOutcome.UNRESOLVED

        if after.get("broker_order_id") and str(after.get("status") or "").strip().upper() in {
            "SUBMITTED", "ACK", "ACKNOWLEDGED", "PARTIAL", "PARTIAL_FILL", "FILLED"
        }:
            return _RowOutcome.SKIPPED

        reconciler = self.execution_core
        if reconciler is None:
            callback = getattr(self.entry_watcher, "on_trigger", None)
            reconciler = getattr(callback, "__self__", None)
        reconcile = getattr(reconciler, "reconcile_deferred_broker_intent", None)
        if not callable(reconcile):
            retained = getattr(self.osm, "retain_recovery_ownership_if_no_watcher", None)
            retained_ok = False
            if callable(retained):
                try:
                    retained_ok = bool(
                        retained(
                            local_oid,
                            recovery_owner=str(owner).strip(),
                            reason="reconcile_broker_intent_consumer_unavailable",
                            recovery_retention_mode=self.execution_mode.upper(),
                        )
                    )
                except Exception:
                    retained_ok = False
            self._mark_failure(local_oid, "reconcile_broker_intent_consumer_unavailable")
            return _RowOutcome.RETRY_OWNED if retained_ok else _RowOutcome.UNRESOLVED

        try:
            reconciliation = reconcile(local_order_id=local_oid) or {}
        except Exception as exc:
            self._mark_failure(
                local_oid,
                f"reconcile_broker_intent_failed:{type(exc).__name__}",
            )
            return _RowOutcome.UNRESOLVED

        disposition = str(reconciliation.get("disposition") or "").strip().upper()
        if disposition in {"ALREADY_RECONCILED", "SUBMITTED"}:
            try:
                reconciled_row = self.osm.get_order(local_oid)
            except Exception:
                reconciled_row = None
            if (
                isinstance(reconciled_row, dict)
                and str(reconciled_row.get("broker_order_id") or "").strip()
                and str(reconciled_row.get("status") or "").strip().upper()
                in {"SUBMITTED", "ACK", "ACKNOWLEDGED", "PARTIAL", "PARTIAL_FILL", "FILLED"}
            ):
                return _RowOutcome.SKIPPED

        if disposition in {"RECONCILE_PENDING", "KEEP_WATCHER"}:
            reason = str(
                reconciliation.get("reason_code")
                or "reconcile_broker_intent_pending"
            )
            retained = getattr(self.osm, "retain_recovery_ownership_if_no_watcher", None)
            if callable(retained):
                try:
                    if bool(
                        retained(
                            local_oid,
                            recovery_owner=str(owner).strip(),
                            reason=reason,
                            recovery_retention_mode=self.execution_mode.upper(),
                        )
                    ):
                        return _RowOutcome.RETRY_OWNED
                except Exception:
                    pass

        self._mark_failure(
            local_oid,
            f"reconcile_broker_intent_unresolved:{disposition or 'blank'}",
        )
        return _RowOutcome.UNRESOLVED

    def _schedule_prebroker_materialization_retry(
        self,
        local_oid: str,
        row: dict,
        *,
        owner: str,
        generation: int,
        attempt: int,
        reason: str,
        signal_id: str,
    ) -> str:
        try:
            max_attempts = resolve_deferred_materialization_max_attempts()
        except DeferredMaterializationConfigConflict as exc:
            self._mark_failure(local_oid, f"prebroker_retry_config_conflict:{exc}")
            return _RowOutcome.UNRESOLVED
        if attempt > max_attempts:
            self._mark_failure(local_oid, "prebroker_retry_attempt_exhausted")
            return _RowOutcome.UNRESOLVED
        delay = _env_int("BREACH_SELECTOR_RETRY_DELAY_SECONDS", 8)
        next_retry_at = (
            datetime.now(timezone.utc) + timedelta(seconds=delay)
        ).isoformat()
        schedule = getattr(self.osm, "schedule_deferred_materialization_retry", None)
        if not callable(schedule):
            self._mark_failure(local_oid, "prebroker_retry_schedule_unavailable")
            return _RowOutcome.UNRESOLVED
        try:
            scheduled = bool(
                schedule(
                    local_oid,
                    owner=owner,
                    generation=generation,
                    reason_code=reason,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    next_retry_at=next_retry_at,
                    selector_failure={
                        "materialization_outcome": "RETRY_LATER_DATA_UNAVAILABLE",
                        "materialization_detail": reason,
                        "entry_path": "DEFERRED_BREACH_MATERIALIZATION",
                        "prebroker_recovery": True,
                        "signal_id": signal_id,
                        "execution_mode": self.execution_mode,
                    },
                    signal_id=signal_id,
                    execution_mode=self.execution_mode,
                )
            )
        except Exception as exc:
            self._mark_failure(local_oid, f"prebroker_retry_schedule_failed:{type(exc).__name__}")
            return _RowOutcome.UNRESOLVED
        if not scheduled:
            self._mark_failure(local_oid, "prebroker_retry_schedule_failed")
            return _RowOutcome.UNRESOLVED
        self._mark_retry_subtype(local_oid, _RETRY_MATERIALIZATION)
        return _RowOutcome.RETRY_OWNED

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
        proof = self._verify_restart_rearm_retry_ownership(local_oid, row, allow_expired=True)
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

        if now > deadline or attempt >= max_attempts:
            return self._terminalize_with_reason(
                local_oid,
                row,
                "restart_rearm_quote_retry_exhausted",
                meta_patch={
                    "restart_recovery_retry_subtype": _RETRY_RESTART_REARM,
                    "restart_rearm_exhausted_attempt": attempt,
                },
            )

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
            if outcome == _RowOutcome.WATCHER_OWNED:
                self._safe_meta_update(local_oid, {
                    _RR_STATUS_FIELD: "CLOSED",
                    _RR_CLOSED_AT: _now_iso(),
                    _RR_CLOSE_REASON: "watcher_owned",
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

    def _rearm_and_verify(
        self,
        row: dict,
        local_oid: str,
        *,
        plan_builder_fn=None,
        preserve_on_reject: bool = False,
    ) -> str:
        """
        Call watch() once, then verify actual registry ownership.
        watch() returning True is NOT proof.

        Blocker 6: watch() failure → terminalize OR unresolved, never both.
        """
        watcher = self.entry_watcher
        if watcher is None or not callable(getattr(watcher, "watch", None)):
            log.critical("RESTART_RECOVERY_WATCHER_UNAVAILABLE local=%s", local_oid)
            if preserve_on_reject:
                return _RowOutcome.UNRESOLVED
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
            if preserve_on_reject:
                self._mark_failure(
                    local_oid, "restart_recovery_watch_returned_false_preserved"
                )
                return _RowOutcome.UNRESOLVED
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
        if _has_trigger_or_submit_evidence(row):
            self._mark_failure(local_oid, "restart_rearm_blocked:trigger_or_submit_evidence")
            log.critical("RESTART_RECOVERY_RESTART_REARM_BLOCKED local=%s trigger_or_submit_evidence=true", local_oid)
            return _RowOutcome.UNRESOLVED

        _delay = _env_int("RESTART_REARM_RETRY_DELAY_SECONDS", 30)
        _deadline_secs = _env_int("RESTART_REARM_RETRY_DEADLINE_SECONDS", 180)
        _max = _env_int("RESTART_REARM_RETRY_MAX_ATTEMPTS", 6)
        _now = datetime.now(timezone.utc)
        _attempts = int(prior_attempt if prior_attempt is not None else (_extract_meta(row).get(_RR_ATTEMPT_FIELD) or 0)) + 1
        if _attempts > _max:
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
        if _deadline_dt <= _now:
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
        )
        if proof is None:
            self._mark_failure(local_oid, "retry_verification:restart_rearm_after_write")
            log.critical("RESTART_RECOVERY_RESTART_REARM_RETRY_NOT_PROVEN local=%s", local_oid)
            return _RowOutcome.UNRESOLVED

        self._mark_retry_subtype(local_oid, _RETRY_RESTART_REARM)
        log.info("RESTART_RECOVERY_RESTART_REARM_RETRY_OWNED local=%s proof=%s", local_oid, proof)
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
        if _attempts > _max:
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
        reason       = str(meta.get(_MAT_REASON_FIELD) or "").strip()
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
        if attempts < 1 or attempts > _max:
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
        allow_expired: bool = False,
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
        if status != "PENDING_TRIGGER":
            return None
        if rr_oid != local_oid:
            return None
        if rr_client != self.client_id.lower():
            return None
        if rr_mode != self.execution_mode:
            return None
        if _has_trigger_or_submit_evidence(reread):
            return None

        meta = _extract_meta(reread)
        restart_status = str(meta.get(_RR_STATUS_FIELD) or "").strip().upper()
        owner = str(meta.get(_RR_OWNER_FIELD) or "").strip()
        reason = str(meta.get(_RR_REASON_FIELD) or "").strip()
        next_at = str(meta.get(_RR_NEXT_AT_FIELD) or "").strip()
        deadline = str(meta.get(_RR_DEADLINE_FIELD) or "").strip()
        first_failed_at = str(meta.get(_RR_FIRST_FAILED_AT) or "").strip()
        rr_client_meta = str(meta.get(_RR_CLIENT_FIELD) or "").strip().lower()
        rr_mode_meta = str(meta.get(_RR_MODE_FIELD) or "").strip().lower()
        try:
            attempt = int(meta.get(_RR_ATTEMPT_FIELD))
        except (TypeError, ValueError):
            return None
        max_attempts = _env_int("RESTART_REARM_RETRY_MAX_ATTEMPTS", 6)
        next_dt = _parse_iso(next_at)
        deadline_dt = _parse_iso(deadline)
        if restart_status != "RETRY_PENDING":
            return None
        if not owner or not reason:
            return None
        if attempt < 1 or attempt > max_attempts:
            return None
        if next_dt is None or deadline_dt is None or next_dt > deadline_dt:
            return None
        if not allow_expired and datetime.now(timezone.utc) > deadline_dt:
            return None
        if rr_client_meta != self.client_id.lower() or rr_mode_meta != self.execution_mode:
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
            metadata=dict(meta),
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


def _strict_recovery_meta(row: dict) -> Optional[dict]:
    """Decode recovery metadata without turning malformed JSON into ``{}``."""
    raw = (row or {}).get("meta")
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            decoded = json.loads(raw)
        except Exception:
            return None
        return dict(decoded) if isinstance(decoded, dict) else None
    return None


def _strict_nonnegative_int(raw) -> Optional[int]:
    """Parse an integer counter while rejecting booleans and fractional values."""
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw if raw >= 0 else None
    if isinstance(raw, str) and raw.strip().isdigit():
        return int(raw.strip())
    return None


def _strict_positive_int(raw) -> Optional[int]:
    value = _strict_nonnegative_int(raw)
    return value if value is not None and value > 0 else None


def _strict_generation(raw) -> Optional[int]:
    """Parse an exact positive materialization generation."""
    return (
        raw
        if isinstance(raw, int)
        and not isinstance(raw, bool)
        and raw >= 1
        else None
    )


def _strict_broker_quantity(raw) -> Optional[int]:
    if isinstance(raw, bool):
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value) or value <= 0 or not value.is_integer():
        return None
    return int(value)


def _finite_float_or_zero(raw) -> float:
    if isinstance(raw, bool):
        return 0.0
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return 0.0
    return value if math.isfinite(value) else 0.0


def _strict_positive_float(raw) -> Optional[float]:
    if isinstance(raw, bool):
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) and value > 0 else None


def _first_strict_positive_float(values) -> Optional[float]:
    for raw in values:
        value = _strict_positive_float(raw)
        if value is not None:
            return value
    return None


def _parse_timezone_aware(raw) -> Optional[datetime]:
    if isinstance(raw, datetime):
        return raw if raw.tzinfo is not None else None
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        text = raw.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        value = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None
    return value if value.tzinfo is not None else None


def _first_broker_timestamp(
    payload: dict,
    field_names: tuple[str, ...],
) -> tuple[Optional[str], Optional[str], bool]:
    """Return the first exact timestamp, its source, and malformed state."""
    if not isinstance(payload, dict):
        return None, None, True
    for field_name in field_names:
        if field_name not in payload:
            continue
        raw = payload.get(field_name)
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            continue
        parsed = _parse_timezone_aware(raw)
        if parsed is None:
            return None, field_name, True
        return parsed.astimezone(timezone.utc).isoformat(), field_name, False
    return None, None, False


def _interpret_position_truth(
    raw_positions,
    *,
    client_id: str,
    execution_mode: str,
    signal_id: str,
    local_order_id: str,
) -> tuple[str, str]:
    """Interpret exact position-reader output without guessing ownership.

    ``[]`` is the only successful zero-result shape.  ``None``, ``False``,
    ``True``, and every other unknown shape mean the authority read did not
    prove anything and therefore remain HOLD.
    """
    if raw_positions is None or raw_positions is False:
        return "HOLD", "position_reader_unavailable_or_unproven"
    if raw_positions is True:
        return "HOLD", "position_reader_response_unrecognized"
    if isinstance(raw_positions, dict):
        positions = [raw_positions]
    elif isinstance(raw_positions, list):
        positions = raw_positions
    else:
        return "HOLD", "position_listing_malformed"

    if not positions:
        return "NO_MATCH", "no_matching_position"

    active_statuses = {"OPEN", "CLOSING", "PARTIAL", "ACTIVE"}
    expected_client = str(client_id or "").strip().lower()
    expected_mode = str(execution_mode or "").strip().lower()
    expected_signal = str(signal_id or "").strip()
    expected_local = str(local_order_id or "").strip()
    if not expected_client or not expected_mode or not expected_signal or not expected_local:
        return "HOLD", "position_identity_context_unproven"

    matches = []
    for position in positions:
        if not isinstance(position, dict) or not position:
            return "HOLD", "position_listing_malformed"
        position_client = str(
            position.get("client_id") or position.get("client_email") or ""
        ).strip().lower()
        if not position_client:
            return "HOLD", "matching_position_client_id_missing"
        if position_client != expected_client:
            return "HOLD", "matching_position_client_id_conflict"
        position_local = str(position.get("local_order_id") or "").strip()
        position_signal = str(position.get("signal_id") or "").strip()
        exact_local = bool(position_local and position_local == expected_local)
        exact_signal = bool(position_signal and position_signal == expected_signal)
        if not exact_local and not exact_signal:
            return "HOLD", "position_identity_mismatch"
        raw_mode = position.get("execution_mode")
        if raw_mode is None or not str(raw_mode).strip():
            return "HOLD", "matching_position_execution_mode_missing"
        position_mode = str(raw_mode).strip().lower()
        if position_mode not in {"live", "paper"}:
            return "HOLD", "matching_position_execution_mode_malformed"
        if position_mode != expected_mode:
            return "HOLD", "matching_position_execution_mode_conflict"
        matches.append((position, exact_local, exact_signal))

    if len(matches) > 1:
        return "HOLD", "multiple_conflicting_position_matches"
    if not matches:
        return "NO_MATCH", "no_matching_position"

    position, exact_local, _exact_signal = matches[0]
    if exact_local:
        return "MATCH", "matching_position_local_order_id"
    position_status = str(position.get("status") or "").strip().upper()
    if not position_status:
        return "HOLD", "matching_position_status_missing"
    if position_status in active_statuses:
        return "MATCH", "matching_position_signal_id"
    return "NO_MATCH", "no_matching_position"


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
    # RUNNING is also a retryable durable state.  The exact #445
    # MATERIALIZING/RUNNING pre-callback claim is proofed before any action;
    # malformed or unrelated RUNNING rows therefore fail closed rather than
    # falling through to unknown_subtype.
    if mat_status in {"RETRY_PENDING", "RUNNING"} or meta.get(_MAT_NEXT_RETRY_AT):
        return _RETRY_MATERIALIZATION
    if rr_status == "RETRY_PENDING" or meta.get(_RR_NEXT_AT_FIELD):
        return _RETRY_RESTART_REARM
    if meta.get("watcher_retry_owner") or meta.get("watcher_retry_next_at"):
        return _RETRY_WATCHER
    return ""


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
    # The trigger_ready audit is historical after the synthetic direction-
    # reversal rearm.  The exact RWR state above is the live recovery
    # authority; do not let that stale diagnostic block its bounded quote
    # retry or make a valid rearm look like active submit evidence.
    _valid_direction_reversal_rwr = (
        meta.get("direction_reversal_rearm_requires_watcher") is True
        and str(meta.get("recovery_ownership") or "").strip()
        == "recovery_scheduler"
        and bool(str(meta.get("recovery_owner") or "").strip())
        and not str(meta.get("lifecycle_state") or "").strip()
        and not str(meta.get("materialization_status") or "").strip()
        and not str(meta.get("current_owner") or "").strip()
        and not str(meta.get("watcher_token") or "").strip()
    )
    if (
        isinstance(watcher_audit, dict)
        and str(watcher_audit.get("reason_code") or "") == "trigger_ready"
        and not _valid_direction_reversal_rwr
    ):
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
