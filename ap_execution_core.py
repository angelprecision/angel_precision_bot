# ap_execution_core.py -- Angel Precision Pure Production Execution Core
# =============================================================================
# Production contract:
#   /signal -> trade_queue -> worker_loop -> APMasterControl
#   -> APContractSelector -> APOrderStateMachine.create_entry_order()
#   -> APEntryWatcher.watch(plan, local_order_id)
#   -> breach -> APOrderStateMachine.submit_existing_entry()
#   -> fill_monitor / position_manager / exit_engine.
#
# ExecutionCore does not select contracts, size entries, create entry orders at
# breach time, or create synthetic positions. It only owns watcher callbacks,
# breach-time risk revalidation, OSM submission of existing orders, exit-engine
# callbacks, signal tracking, and proof/feedback logging.
# =============================================================================

from __future__ import annotations

import os
import re
import time
import uuid
import logging
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from ap_entry_watcher        import APEntryWatcher, WatchedSignal
from ap_exit_engine          import APExitEngine, ManagedPosition
from ap_feedback_loop        import APFeedbackLoop
from ap_tier_engine          import APShadowTracker
from ap_proof_logger         import APProofLogger, funnel
from ap_signal_store         import APSignalStore
from ap_signal_tracker       import APSignalTracker

# Intelligence outcome feedback — optional, fails silently if bridge not deployed
try:
    from intelligence_bridge import record_trade_outcome as _record_intel_outcome
except ImportError:
    _record_intel_outcome = None

log = logging.getLogger("ap.execution_core")

_VALID_EXECUTION_MODES = frozenset({"paper", "live"})


def _normalize_execution_mode(value) -> str | None:
    mode = str(value or "").strip().lower()
    return mode if mode in _VALID_EXECUTION_MODES else None


def _resolve_submit_execution_mode(approved_plan, signal, runtime_mode, paper_flag) -> str | None:
    explicit_values = [
        getattr(approved_plan, "execution_mode", None),
        getattr(approved_plan, "mode", None),
    ]
    if isinstance(signal, dict):
        explicit_values.extend([
            signal.get("execution_mode"),
            signal.get("mode"),
        ])
    for raw_value in explicit_values:
        if not str(raw_value or "").strip():
            continue
        normalized = _normalize_execution_mode(raw_value)
        if normalized is None:
            return None
        return normalized

    runtime_normalized = _normalize_execution_mode(runtime_mode)
    if runtime_normalized is not None:
        return runtime_normalized
    if isinstance(paper_flag, bool):
        return "paper" if paper_flag else "live"
    return None
ET  = ZoneInfo("America/New_York")
_OCC_CONTRACT_RE = re.compile(r"\d{6}[CP]\d{5,8}")

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
BOT_MODE            = (os.getenv("AP_MODE") or os.getenv("BOT_MODE") or "PAPER").upper()
MAX_POSITIONS       = int(os.getenv("MAX_POSITIONS", "7"))

# ── P0: deferred-breach contract-selector retry taxonomy ─────────────────────
# Reason codes in this set mean the chain provider had a temporary data miss —
# NOT a real contract-quality or risk reject.  On a retryable miss the deferred
# row must NOT be expired/cancelled; instead it is rearmed for retry up to
# MAX_BREACH_SELECTOR_RETRIES times (default 3) before giving up.
#
# Quality rejects (SPREAD_TOO_WIDE, DELTA_OUT_OF_RANGE, OI_TOO_LOW, etc.) and
# fundamental blocks (EARNINGS_LOCKOUT, UNTRADEABLE_FOR_ACCOUNT_SIZE, etc.) are
# NOT in this set — those still terminalize on the first attempt, exactly as today.
#
# AMENDMENT (PR #219, Jason LIVE recovery): the original set missed the two
# top-count failure codes on Jason LIVE today — CHAIN_ROW_ZERO_BID_ASK
# (12 SBUX + 29 ACN candidates) and DIRECT_QUOTE_ZERO_BID_ASK (5 SBUX + 4 ACN).
# Both are transient quote-quality signals at market open, not structural
# rejects. NO_VALID_PLAYBOOK_DTE_CONTRACT is the aggregation reason emitted
# after the ladder exhausts all buckets on transient signals; adding it here
# means a second full ladder pass runs after RETRY_DELAY seconds instead of
# terminalizing. QUOTE_FETCH_FAILED and CHAIN_EMPTY are historical variants
# emitted by older selector paths that could still surface in edge cases.
#
# Env overrides:
#   MAX_BREACH_SELECTOR_RETRIES          default 3
#   BREACH_SELECTOR_RETRY_DELAY_SECONDS  default 20
#   BREACH_SELECTOR_RETRY_CUTOFF_ET      default 1530 (= 3:30 PM ET; last-entry
#                                        boundary — see _breach_retry_cutoff_hhmm)
RETRYABLE_BREACH_SELECTOR_REASONS: frozenset = frozenset({
    "NO_CHAIN_DATA",                    # legacy compat — broad code kept until all paths emit exact codes
    "CHAIN_PROVIDER_ERROR",             # Tradier HTTP/network transient failure
    "CHAIN_PROVIDER_EMPTY_EXPIRATIONS", # expirations endpoint returned nothing (can be transient at 9:30–9:36)
    "CHAIN_PROVIDER_EMPTY_OPTIONS",     # chain endpoint returned zero rows for this expiration
    "CHAIN_PARSE_EMPTY",                # chain parsed to zero rows after direction filter
    "NO_EXPIRATION_IN_DTE_WINDOW",      # no eligible expiration in the current DTE probe window
    "DIRECT_QUOTE_UNAVAILABLE",         # direct-quote revalidation fetch failed
    # ── PR #219 amendment: Jason LIVE 2026-07-01 recovery additions ──
    "CHAIN_ROW_ZERO_BID_ASK",           # per-chain-row zero bid or ask — top failure on Jason LIVE today
    "DIRECT_QUOTE_ZERO_BID_ASK",        # direct OCC quote came back zero — 2nd top failure on Jason LIVE
    "QUOTE_FETCH_FAILED",               # quote endpoint returned an error/timeout
    "CHAIN_EMPTY",                      # historical variant emitted when chain returns no rows
    # NOTE: NO_VALID_PLAYBOOK_DTE_CONTRACT is intentionally excluded here.
    # It is the DTE-ladder aggregation reason and may reflect structural quality
    # rejects (OI_TOO_LOW, SPREAD_TOO_WIDE) as well as transient data-miss reasons.
    # When the ladder emits it, execution_core inspects the dte_ladder_audit to
    # determine whether the exhaustion came from data-miss (retryable) or quality
    # rejects (terminal). See _is_ladder_exhaustion_retryable() below.
})


# ── P0 (monday-trade-flow-readiness, amended): acceptance ask-cap resolver ───
# Small-account acceptance-window cap. FAIL-CLOSED contract:
#   • DEFERRED_SMALL_ACCOUNT_FALLBACK unset/0  → (False, None, None): feature
#     off, all cap checks are no-ops, behavior byte-for-byte unchanged.
#   • Flag ON + MAX_CONTRACT_ASK_FOR_JASON_ACCEPTANCE valid (> 0)
#     → (True, cap, None): enforce at BOTH check sites (selection copy AND
#     final pre-submit limit) so a selector ask under cap with a refreshed
#     submit limit over cap can never slip through.
#   • Flag ON + cap env missing / unparsable / <= 0
#     → (True, None, error): MISCONFIGURED. Callers must BLOCK the deferred
#     materialization (terminalize), never proceed uncapped. An operator who
#     turned the acceptance fallback on has declared a capped window; running
#     uncapped inside it is the failure mode this contract forbids.
# Hot-read per call (repo convention for operator flags).
def _acceptance_ask_cap() -> "tuple[bool, float | None, str | None]":
    enabled = os.getenv(
        "DEFERRED_SMALL_ACCOUNT_FALLBACK", "0"
    ).strip().lower() in ("1", "true", "yes")
    if not enabled:
        return False, None, None
    raw = os.getenv("MAX_CONTRACT_ASK_FOR_JASON_ACCEPTANCE", "").strip()
    if not raw:
        return True, None, "acceptance_cap_missing"
    try:
        cap = float(raw)
    except (TypeError, ValueError):
        return True, None, f"acceptance_cap_unparsable:{raw!r}"
    if cap <= 0:
        return True, None, f"acceptance_cap_nonpositive:{cap}"
    return True, cap, None


# ── P0 (monday-trade-flow-readiness, amended): canonical materialization
# outcome. Every triggered deferred row must resolve to EXACTLY ONE of three
# operator-facing outcomes — the acceptance contract for restored trade flow:
#   MATERIALIZED_AND_SUBMITTED     real OCC contract selected AND handed to
#                                  the broker submit path
#   RETRY_LATER_DATA_UNAVAILABLE   transient data-miss (zero quotes / empty
#                                  chain near open); a bounded retry is
#                                  scheduled inside the warmup/entry window
#   TERMINAL_NO_TRADEABLE_CONTRACT everything else terminal: quality rejects,
#                                  budget/cap blocks, retry exhaustion/cutoff,
#                                  selector exceptions, submit failures
# The fine-grained outcome (BREACH_SELECTOR_RETURNED_NONE, OI_TOO_LOW, ...)
# is preserved as materialization_detail — canonicalization ADDS a stable
# summary, it never replaces the honest detail. "No more DEFERRED:* + 0.01 +
# EXPIRED with no broker_order_id and unclear reason."
_MATERIALIZATION_ENTRY_PATH = "DEFERRED_BREACH_MATERIALIZATION"


def _canonical_materialization_outcome(outcome: str) -> str:
    _o = str(outcome or "").strip()
    if _o == "BREACH_BROKER_SUBMITTED":
        return "MATERIALIZED_AND_SUBMITTED"
    # P0 amendment #6 (PR #294, Option B): DEFERRED_ORDER_ROW_UNREADABLE is
    # now mapped to TERMINAL_NO_TRADEABLE_CONTRACT, not RETRY_LATER_DATA_UNAVAILABLE.
    #
    # Why Option B: The unreadable-row block calls _terminalize_breach_failure
    # (cleanup_action="expire"). Amendment #5 labelled this RETRY_LATER but
    # immediately expired the order — operator-facing outcome said "retry later",
    # lifecycle said "expired". That is a lie and a production audit failure.
    #
    # The correct honesty test: at this pipeline stage (past breach, past
    # selector success, inside the pre-submit invariant block), there is no safe
    # bounded-retry path that can replay only the order-row read without
    # re-running the full breach/selector cycle. The existing retry helpers
    # (_build_deferred_retry_schedule_meta, _classify_deferred_breach_retry_decision)
    # operate at breach-detection time, not here. Calling the outcome "retry later"
    # when the lifecycle is "expire" would mislead on-call engineers.
    #
    # Option B: both outcome AND lifecycle are terminal. The detail field
    # MATERIALIZATION_ORDER_ROW_UNREADABLE is specific enough that operators
    # can investigate the infra failure (DB timeout, row not found) without
    # the outcome implying a future attempt is scheduled.
    if _o == "DEFERRED_ORDER_ROW_UNREADABLE":
        return "TERMINAL_NO_TRADEABLE_CONTRACT"
    return "TERMINAL_NO_TRADEABLE_CONTRACT"


# ── P0 amendment #3 (PR #294): pure classifier for the deferred materialization
# handoff proof. Given the three-stage snapshot the process_watcher_breach
# path assembles, return one of:
#   (True,  None)              — handoff clean, submit may proceed
#   (False, "<mismatch_code>") — handoff broke; caller must terminalize
#   (None,  None)              — proof not applicable (selector never
#                                returned a real contract on this trigger);
#                                caller falls through to the standing
#                                DEFERRED_CONTRACT / LIMIT invariants
#
# Pulled out of the breach method so tests can exercise the exact
# production truth table without instantiating the full execution core.
# NO side effects, NO logging, NO writes — the caller owns emission,
# terminalization, and orders.meta persistence.
def _classify_materialization_handoff(
    *,
    handoff_snapshot: dict,
    pre_submit_contract: str,
    pre_submit_limit: float,
    pre_submit_qty: int,
    order_row_contract: "str | None" = None,
) -> "tuple[bool | None, str | None]":
    if not handoff_snapshot or not handoff_snapshot.get("captured"):
        return None, None
    selector_contract = str(handoff_snapshot.get("selector_contract") or "")
    if not selector_contract:
        return None, None
    if selector_contract.upper().startswith("DEFERRED:"):
        # Selector explicitly returned a placeholder — proof is not
        # applicable; the standing DEFERRED_CONTRACT invariant catches it.
        return None, None

    pre_contract = str(pre_submit_contract or "")
    pre_contract_upper = pre_contract.upper()
    if (not pre_contract) or pre_contract_upper.startswith("DEFERRED:"):
        return False, "pre_submit_contract_placeholder_or_missing"
    if pre_contract != selector_contract:
        return False, (
            "pre_submit_contract_diverges_from_selector:"
            f"selector={selector_contract}:pre_submit={pre_contract}"
        )
    if float(pre_submit_limit or 0) <= 0.01:
        return False, f"pre_submit_limit_not_materialized:limit={float(pre_submit_limit or 0):.4f}"
    if int(pre_submit_qty or 0) <= 0:
        return False, f"pre_submit_qty_not_materialized:qty={int(pre_submit_qty or 0)}"

    # P0 amendment #4+#5+#6 (PR #294): third-view check against the persisted
    # `orders` row. When a deferred selector has produced a real OCC contract,
    # the caller now proves the row is readable before reaching this classifier.
    # `order_row_contract=None` only remains not-applicable for paths where the
    # selector never captured a real contract; standing invariants still run.
    # Any non-None value is authoritative:
    #   - DEFERRED:* / empty → the OSM never persisted the materialized
    #     contract; block.
    #   - diverges from selector → the OSM row disagrees with the pipeline;
    #     block. Signature is symmetric with the pre_submit_contract check
    #     so dashboards can compare mismatch reasons directly.
    if order_row_contract is not None:
        order_row_str = str(order_row_contract or "")
        order_row_upper = order_row_str.upper()
        if (not order_row_str) or order_row_upper.startswith("DEFERRED:"):
            return False, "order_row_contract_placeholder_or_missing"
        if order_row_str != selector_contract:
            return False, (
                "order_row_contract_diverges_from_selector:"
                f"selector={selector_contract}:order_row={order_row_str}"
            )

    return True, None


# ── P0 amendment #5+#6 (PR #294): pure classifier for the order-row read
# step of the handoff proof. Separate from _classify_materialization_handoff
# so the two failure modes carry distinct audit detail codes even though
# both now map to TERMINAL_NO_TRADEABLE_CONTRACT (amendment #6 Option B):
#
#   order-row UNREADABLE → TERMINAL_NO_TRADEABLE_CONTRACT
#                          detail: MATERIALIZATION_ORDER_ROW_UNREADABLE
#     (cannot prove the row; no safe retry path at this pipeline stage;
#      lifecycle = expire; outcome = terminal — both consistent)
#   order-row readable but WRONG → TERMINAL_NO_TRADEABLE_CONTRACT
#                                   detail: MATERIALIZATION_COPYBACK_MISMATCH
#     (proved the row and it disagrees; permanent pipeline mismatch)
#
# Returns one of:
#   ("BLOCK_RETRY",  "<reason>") — row unreadable; caller MUST block and
#                                  terminalize with DEFERRED_ORDER_ROW_UNREADABLE
#   ("PASS",         None)       — row readable; extract contract for classifier
#   (None,           None)       — proof not applicable; standing invariants handle
#
# NO side effects, NO logging, NO DB access. Pure truth table.
def _classify_order_row_read(
    *,
    handoff_snapshot: dict,
    order_row_raw: object,
    read_error: "str | None" = None,
) -> "tuple[str | None, str | None]":
    if not handoff_snapshot or not handoff_snapshot.get("captured"):
        return None, None
    sel_contract = str(handoff_snapshot.get("selector_contract") or "")
    if not sel_contract or sel_contract.upper().startswith("DEFERRED:"):
        return None, None   # selector never returned a real contract

    # Selector succeeded with a real OCC contract. The order row MUST be
    # readable. A None row means either the row was not found (no row with
    # that local_order_id exists) or the read raised an exception (caller
    # tracks the distinction in read_error for the audit field — both map
    # to the same BLOCK_RETRY outcome because in either case we cannot
    # prove the persisted contract before broker POST).
    if order_row_raw is None:
        reason = f"read_error:{read_error}" if read_error else "row_not_found"
        return "BLOCK_RETRY", reason

    return "PASS", None


def _read_order_row_for_handoff_proof(
    order_state_machine: object,
    local_order_id: str,
    *,
    attempts: int = 3,
    delay_s: float = 0.05,
) -> "tuple[object | None, str | None, int]":
    """Read the persisted order row for the pre-submit handoff proof.

    A readable row is required before broker POST. A transient None read can
    happen during handoff timing or adapter hiccups, so give it a tiny bounded
    re-read window. Exceptions are returned to the caller as audit detail; the
    caller owns terminalization.
    """
    if order_state_machine is None:
        return None, "osm_not_available", 0
    get_order = getattr(order_state_machine, "get_order", None)
    if not callable(get_order):
        return None, "osm_get_order_not_callable", 0

    max_attempts = max(1, int(attempts or 1))
    last_error: str | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            row = get_order(local_order_id)
        except Exception as exc:
            last_error = str(exc)
            return None, last_error, attempt
        if row is not None:
            return row, None, attempt
        if attempt < max_attempts and delay_s > 0:
            time.sleep(float(delay_s))
    return None, last_error, max_attempts


def _live_confirmation_required() -> bool:
    """
    P0 (PR #262): LIVE submits require an explicit entry-confirmation PASS.

    check_entry_confirmation is a deliberate NO-OP when the plan does not
    carry confirmation_required=True (legacy design). In LIVE that meant a
    submit could reach the broker with ZERO confirmation whenever the
    hybrid gate didn't set the flag or plan metadata was lost through a
    recovery/rescue path — confirmation "disabled unintentionally".

    Default-required in LIVE. Env LIVE_CONFIRMATION_REQUIRED=0 is the only
    bypass and is logged at CRITICAL by the caller. Hot-read per call
    (repo convention for entry-confirmation flags).
    """
    return str(
        os.getenv("LIVE_CONFIRMATION_REQUIRED", "1")
    ).strip().lower() in ("1", "true", "yes")


def _is_ladder_exhaustion_retryable(ladder_audit: Optional[dict]) -> bool:
    """Determine whether a NO_VALID_PLAYBOOK_DTE_CONTRACT exhaustion is retryable.

    Called only when the top-level selector reason_code is
    NO_VALID_PLAYBOOK_DTE_CONTRACT (the DTE ladder ran but found no quality
    survivor across all probed buckets). Retrying makes sense only when the
    per-bucket rejections were all data-miss signals, not structural quality
    verdicts that would fail again regardless of when we retry.

    Logic:
      - Retryable if ALL probed expirations failed with a code in
        RETRYABLE_BREACH_SELECTOR_REASONS.
      - Terminal if ANY probed expiration failed with a structural quality
        code (OI_TOO_LOW, SPREAD_TOO_WIDE, BID_BELOW_MIN, etc.) — those
        verdicts are contract-level and will not improve on a retry.
      - Unknown / missing audit → conservative return of False (do not retry;
        prefer to terminalize rather than loop on unknown failure).
    """
    if not isinstance(ladder_audit, dict):
        return False
    buckets_attempted = ladder_audit.get("buckets_attempted") or []
    if not buckets_attempted:
        return False

    saw_any_retryable = False
    for bucket in buckets_attempted:
        for exp_rec in bucket.get("expirations_probed") or []:
            _sub_fail = exp_rec.get("failure") or {}
            _rc = str(_sub_fail.get("reason_code") or "").strip().upper()
            if not _rc:
                continue
            if _rc not in RETRYABLE_BREACH_SELECTOR_REASONS:
                # Any structural/quality/terminal reject means the exhaustion
                # is not purely data-miss — do not retry.
                return False
            saw_any_retryable = True

    # If we only saw retryable data-miss reasons (or no per-exp reason_code at
    # all), allow the retry so the ladder gets a fresh chance after the delay.
    return saw_any_retryable


# ── P0 (2026-07-02): breach retry cutoff is the LAST-ENTRY boundary, not the
# open-warmup boundary. The original default of 945 (9:45 AM ET) was written
# for the "chain not warmed at 9:30–9:36" incident, but the queue is not even
# released until ~9:45 ET, so no breach could ever occur BEFORE the cutoff —
# the retry system was structurally disabled for the entire session. Forensics
# 2026-06-30 → 2026-07-02: every retryable death (CHAIN_ROW_ZERO_BID_ASK,
# NO_CHAIN_DATA, DIRECT_QUOTE_ZERO_BID_ASK) shows breach_attempt_count=1 with
# cs_status=CONTRACT_SELECTION_DATA_ERROR at 09:46–11:16 ET, i.e. terminalized
# on the first transient miss because now_hhmm >= 945.
#
# New default: 1530 (3:30 PM ET) — the system's own last-entry boundary
# (matches ap_entry_watcher EOD disarm and ap/order_monitor
# _PT_ORPHAN_EOD_CUTOFF). Total retry span per order remains bounded by
# MAX_BREACH_SELECTOR_RETRIES × BREACH_SELECTOR_RETRY_DELAY_SECONDS
# (default 3 × 20s = ~60s), so this cannot cause open-ended retry loops;
# the cutoff only stops NEW retries from being scheduled into the close.
# Env var name is unchanged so the operational kill-switch muscle memory
# ("set BREACH_SELECTOR_RETRY_CUTOFF_ET=0 to stop all retries") still works.
_BREACH_RETRY_CUTOFF_DEFAULT_HHMM = 1530


def _breach_retry_cutoff_hhmm() -> int:
    """
    HHMM (ET) after which NO new deferred-breach selector retries may be
    scheduled. Reads BREACH_SELECTOR_RETRY_CUTOFF_ET; falls back to the
    module default on missing/invalid values (never raises).
    """
    _raw = os.getenv(
        "BREACH_SELECTOR_RETRY_CUTOFF_ET", str(_BREACH_RETRY_CUTOFF_DEFAULT_HHMM)
    )
    try:
        return int(str(_raw).strip())
    except (TypeError, ValueError):
        return _BREACH_RETRY_CUTOFF_DEFAULT_HHMM


def _classify_deferred_breach_retry_decision(
    reason_code: str,
    *,
    queue_local_order_id: str,
    attempt: int,
    max_attempts: int,
    past_cutoff: bool,
    retry_enabled: bool,
    ladder_retryable: bool = False,
) -> dict:
    _reason_code = str(reason_code or "").strip() or "BREACH_SELECTOR_RETURNED_NONE"
    _retryable_reason = (
        _reason_code in RETRYABLE_BREACH_SELECTOR_REASONS or bool(ladder_retryable)
    )
    if (
        _retryable_reason
        and retry_enabled
        and bool(queue_local_order_id)
        and attempt <= max_attempts
        and not past_cutoff
    ):
        return {
            "action": "retry_schedule",
            "reason_code": _reason_code,
            "retryable_reason": True,
        }
    if _retryable_reason and past_cutoff:
        return {
            "action": "retry_cutoff",
            "reason_code": _reason_code,
            "retryable_reason": True,
            "terminal_reason": f"breach_retry_cutoff:{_reason_code}",
        }
    if _retryable_reason and attempt > max_attempts:
        return {
            "action": "retry_exhausted",
            "reason_code": _reason_code,
            "retryable_reason": True,
            "terminal_reason": f"breach_retry_exhausted:{_reason_code}",
        }
    if _retryable_reason and not retry_enabled:
        return {
            "action": "retry_disabled",
            "reason_code": _reason_code,
            "retryable_reason": True,
            "terminal_reason": f"breach_retry_disabled:{_reason_code}",
        }
    if _retryable_reason and not bool(queue_local_order_id):
        return {
            "action": "retry_unavailable",
            "reason_code": _reason_code,
            "retryable_reason": True,
            "terminal_reason": f"breach_retry_unavailable:{_reason_code}",
        }
    return {
        "action": "terminal_quality",
        "reason_code": _reason_code,
        "retryable_reason": False,
    }


def _build_deferred_retry_schedule_meta(
    *,
    reason_code: str,
    selector_audit: Optional[dict],
    attempt: int,
    max_attempts: int,
    delay_seconds: int,
    client_id: str,
    execution_mode: str,
    local_order_id: str,
    signal_id: str,
    now: Optional[datetime] = None,
) -> dict:
    _now = now or datetime.now(timezone.utc)
    return {
        "deferred_retry_scheduled": True,
        "deferred_retry_reason_code": str(reason_code or ""),
        "deferred_retry_attempt": int(attempt),
        "deferred_retry_max_attempts": int(max_attempts),
        "deferred_retry_delay_seconds": int(delay_seconds),
        "deferred_retry_scheduled_at": _now.isoformat(),
        "deferred_retry_next_attempt_at": (
            _now + timedelta(seconds=int(delay_seconds))
        ).isoformat(),
        "last_breach_selector_audit": selector_audit or {},
        "breach_attempt_count": int(attempt),
        "client_id": str(client_id or ""),
        "execution_mode": str(execution_mode or ""),
        "local_order_id": str(local_order_id or ""),
        "signal_id": str(signal_id or ""),
        "contract_selection_status": "CONTRACT_SELECTION_RETRY",
        # P0 amended: canonical tri-outcome stamp — a scheduled retry is the
        # RETRY_LATER_DATA_UNAVAILABLE state until the next attempt resolves
        # it to MATERIALIZED_AND_SUBMITTED or TERMINAL_NO_TRADEABLE_CONTRACT.
        "entry_path": _MATERIALIZATION_ENTRY_PATH,
        "materialization_outcome": "RETRY_LATER_DATA_UNAVAILABLE",
        "materialization_detail": str(reason_code or ""),
    }


def _build_deferred_retry_terminal_meta(
    *,
    terminal_reason: str,
    reason_code: str,
    selector_audit: Optional[dict],
    attempt: int,
    max_attempts: int,
    client_id: str,
    execution_mode: str,
    local_order_id: str,
    signal_id: str,
    now: Optional[datetime] = None,
) -> dict:
    _now = now or datetime.now(timezone.utc)
    return {
        "deferred_retry_scheduled": False,
        "deferred_retry_terminal_reason": str(terminal_reason or ""),
        "deferred_retry_reason_code": str(reason_code or ""),
        "deferred_retry_attempt": int(attempt),
        "deferred_retry_max_attempts": int(max_attempts),
        "last_breach_selector_audit": selector_audit or {},
        "breach_attempt_count": int(attempt),
        "client_id": str(client_id or ""),
        "execution_mode": str(execution_mode or ""),
        "local_order_id": str(local_order_id or ""),
        "signal_id": str(signal_id or ""),
        "last_breach_failure_at": _now.isoformat(),
        # P0 amended: canonical tri-outcome stamp for retry-exhaustion/cutoff/
        # disabled terminals — the fine-grained reason stays in
        # materialization_detail / deferred_retry_terminal_reason.
        "entry_path": _MATERIALIZATION_ENTRY_PATH,
        "materialization_outcome": "TERMINAL_NO_TRADEABLE_CONTRACT",
        "materialization_detail": str(reason_code or ""),
    }


def _build_deferred_retry_stale_abort_meta(
    *,
    current_status: str,
    broker_order_id: str,
    submitted_ts,
    current_contract: str,
    current_attempt: int,
    thread_attempt: int,
    selector_audit: Optional[dict],
    client_id: str,
    execution_mode: str,
    local_order_id: str,
    signal_id: str,
    reason: str,
    now: Optional[datetime] = None,
) -> dict:
    _now = now or datetime.now(timezone.utc)
    _current_attempt = int(current_attempt)
    _thread_attempt = int(thread_attempt)
    return {
        "deferred_retry_scheduled": False,
        "deferred_retry_stale_abort": True,
        "deferred_retry_terminal_reason": f"breach_retry_stale_state_abort:{reason}",
        "deferred_retry_reason_code": str(reason or ""),
        "breach_attempt_count": max(_current_attempt, _thread_attempt),
        "last_breach_selector_audit": selector_audit or {},
        "client_id": str(client_id or ""),
        "execution_mode": str(execution_mode or ""),
        "local_order_id": str(local_order_id or ""),
        "signal_id": str(signal_id or ""),
        "deferred_retry_stale_abort_details": {
            "reason": str(reason or ""),
            "status_changed": str(reason or "") == "status_changed",
            "current_status": str(current_status or ""),
            "broker_id_present": bool(str(broker_order_id or "").strip()),
            "submitted_ts_present": bool(submitted_ts),
            "broker_or_submitted_present": str(reason or "") == "broker_or_submitted_present",
            "no_longer_deferred": str(reason or "") == "no_longer_deferred",
            "attempt_count_advanced": str(reason or "") == "attempt_count_advanced",
            "current_contract": str(current_contract or ""),
            "db_attempt": _current_attempt,
            "thread_attempt": _thread_attempt,
        },
        "last_breach_failure_at": _now.isoformat(),
    }


# ── P0: Deferred materialization audit helper ─────────────────────────────────
# Writes orders.meta.deferred_materialization on every breach-selector attempt
# so every probed expiration, chain size, reject bucket, and account budget is
# permanently captured in the order row — whether or not the trade is filled.
#
# Called on BOTH success and failure paths.  On success the caller supplies the
# real OCC symbol and limit; on failure success=False and failure_reason names
# the blocker.  Best-effort — never raises; observability only.
# ─────────────────────────────────────────────────────────────────────────────
def _write_deferred_materialization_audit(
    osm,
    local_order_id: str,
    *,
    success: bool,
    attempt_ts: str,
    original_contract: str,
    symbol: str,
    side: str,
    execution_mode: str,
    account_budget: float,
    selector_status: "str | None",
    selected_contract: "str | None",
    selected_limit_price: "float | None",
    failure_reason: "str | None",
    stage: "str | None",
    expirations_probed: "list | None",
    chain_rows_total: int,
    survivor_count: int,
    top_reject_buckets: "dict | None",
    nearest_affordable_contract: "str | None" = None,
    best_liquid_contract: "str | None" = None,
    why_best_contract_failed: "str | None" = None,
) -> None:
    """Write orders.meta.deferred_materialization for every breach selector attempt.

    Required fields on every call:
        attempted=True, success, attempt_ts, original_contract, symbol, side,
        execution_mode, account_budget, selector_status, selected_contract,
        selected_limit_price, failure_reason, stage, expirations_probed,
        chain_rows_total, survivor_count, top_reject_buckets.
    Optional enrichment (set when available):
        nearest_affordable_contract, best_liquid_contract, why_best_contract_failed.

    Never raises. Observability only — does not affect any gate or transition.
    """
    try:
        if not osm or not local_order_id:
            return
        _update = getattr(osm, "update_order_meta", None)
        if not callable(_update):
            return
        audit: dict = {
            "attempted":              True,
            "success":                bool(success),
            "attempt_ts":             str(attempt_ts or ""),
            "original_contract":      str(original_contract or ""),
            "symbol":                 str(symbol or ""),
            "side":                   str(side or ""),
            "execution_mode":         str(execution_mode or ""),
            "account_budget":         float(account_budget or 0),
            "selector_status":        str(selector_status) if selector_status else None,
            "selected_contract":      str(selected_contract) if selected_contract else None,
            "selected_limit_price":   float(selected_limit_price) if selected_limit_price is not None else None,
            "failure_reason":         str(failure_reason) if failure_reason else None,
            "stage":                  str(stage) if stage else None,
            "expirations_probed":     list(expirations_probed) if expirations_probed else [],
            "chain_rows_total":       int(chain_rows_total or 0),
            "survivor_count":         int(survivor_count or 0),
            "top_reject_buckets":     dict(top_reject_buckets) if top_reject_buckets else {},
        }
        if nearest_affordable_contract is not None:
            audit["nearest_affordable_contract"] = str(nearest_affordable_contract)
        if best_liquid_contract is not None:
            audit["best_liquid_contract"] = str(best_liquid_contract)
        if why_best_contract_failed is not None:
            audit["why_best_contract_failed"] = str(why_best_contract_failed)
        _update(local_order_id, {"deferred_materialization": audit})
    except Exception:
        pass  # observability must never interrupt the submit path


# ── Mode metadata thresholds ────────────────────────────────────────────────
# PR-B / FIX-8: floors are now env-overridable so they can be tuned
# on Render without a code deploy (essential for proof-week response time).
SCORE_FLOOR_LIVE    = int(os.getenv("SCORE_FLOOR_LIVE",    "75"))   # live: only trade validated setups
SCORE_FLOOR_PAPER   = int(os.getenv("SCORE_FLOOR_PAPER",   "45"))
CONTEXT_FLOOR_LIVE  = float(os.getenv("CONTEXT_FLOOR_LIVE",  "10.0"))
CONTEXT_FLOOR_PAPER = float(os.getenv("CONTEXT_FLOOR_PAPER", "0.0"))

# PR-B / FIX-8: single-source-of-truth for the breakeven band. Previously
# read inline via os.getenv() in both _on_position_close and _finalize_proof
# — same env, two reads, easy drift. Now read once at module load.
# Stored as percent (e.g. -2.0 means -2%); callers may need /100.0 when
# comparing against decimal pnl.
BREAKEVEN_BAND_PCT  = float(os.getenv("BREAKEVEN_BAND_PCT", "-2.0"))

# ── P0: Breach-time entry pricing controls ────────────────────────────────────
# These match the existing process_signal() path in ap/execution.py and bring
# the watcher-breach path to parity.
# ENTRY_PAPER_ASK_CROSS_CENTS: how far above current ask the paper limit sits.
# ENTRY_LIVE_ASK_CROSS_CENTS:  how far above current ask the live limit sits.
# ENTRY_MAX_PRICE_DRIFT_PCT_FROM_PLAN: cancel if ask > plan * (1 + this).
#   Default 0.25 = 25%. Prevents chasing if the option has already run hard
#   between queue time and breach time.
ENTRY_PAPER_ASK_CROSS_CENTS        = float(os.getenv("ENTRY_PAPER_ASK_CROSS_CENTS",        "0.02"))
ENTRY_LIVE_ASK_CROSS_CENTS         = float(os.getenv("ENTRY_LIVE_ASK_CROSS_CENTS",         "0.01"))
ENTRY_MAX_PRICE_DRIFT_PCT_FROM_PLAN = float(os.getenv("ENTRY_MAX_PRICE_DRIFT_PCT_FROM_PLAN", "0.25"))

# ── PR #180: Jason live entry spread + ask-cross precision guard ──────────────
# Final pre-submit precision guard for the named live client. Applies ONLY when:
#   - self.paper is False (live)
#   - client_id matches the PR180_LIVE_CLIENT_IDS allowlist (default: Jason)
#
# Rules (all 4 must hold or submit is blocked / re-priced):
#   1. Hard spread ceiling: spread_pct > PR180_MAX_SPREAD_PCT → BLOCK
#      → ENTRY_SPREAD_TOO_WIDE_LIVE. No LIVE_ASK_CROSS at that width.
#   2. Controlled-limit band: PR180_CONTROLLED_BAND_MIN_PCT < spread_pct <= MAX
#      → limit_price = min(ask, mid + PR180_CONTROLLED_LIMIT_MID_CENTS)
#        (replaces the standard ask + ENTRY_LIVE_ASK_CROSS_CENTS rule)
#   3. Expected mark-to-mid loss floor:
#      expected_mark_loss_pct = (submit_mid - intended_limit) / intended_limit
#      If <= PR180_MAX_EXPECTED_MARK_LOSS_PCT (e.g. -0.06) → BLOCK
#      → ENTRY_EXPECTED_MARK_LOSS_TOO_HIGH.
#   4. Paper unchanged. Non-Jason live unchanged.
#
# Scope: ENTRY-only. Does not affect exits, scanner, or contract selection.
# Risk: One named live client. Env-overridable for emergency rollback without
#       deploy (set PR180_ENABLED=0).
# ─────────────────────────────────────────────────────────────────────────────

PR180_ENABLED                        = os.getenv("PR180_ENABLED", "1") not in ("0", "false", "False", "")
# PR #180 amendment: observe-first rollout.
#   observe  (default) — run the guard, persist all audit fields, log
#                        PR180_ENTRY_PRICING_OBSERVED when it would block,
#                        but do NOT terminalize and do NOT block broker
#                        submit. Reprice is also suppressed unless
#                        PR180_OBSERVE_REPRICE_ENABLED=1 so we can measure
#                        clean baseline impact before changing fill prices.
#   enforce            — full PR #180 behavior: block submits and apply
#                        controlled-band reprice.
PR180_MODE                          = (os.getenv("PR180_MODE", "observe") or "observe").lower().strip()
PR180_OBSERVE_REPRICE_ENABLED      = os.getenv("PR180_OBSERVE_REPRICE_ENABLED", "0") not in ("0", "false", "False", "")
PR180_MAX_SPREAD_PCT                = float(os.getenv("PR180_MAX_SPREAD_PCT",                "0.08"))
PR180_CONTROLLED_BAND_MIN_PCT       = float(os.getenv("PR180_CONTROLLED_BAND_MIN_PCT",       "0.06"))
PR180_CONTROLLED_LIMIT_MID_CENTS    = float(os.getenv("PR180_CONTROLLED_LIMIT_MID_CENTS",    "0.03"))
PR180_MAX_EXPECTED_MARK_LOSS_PCT    = float(os.getenv("PR180_MAX_EXPECTED_MARK_LOSS_PCT",    "-0.06"))
PR180_LIVE_CLIENT_IDS = frozenset(
    s.strip().lower()
    for s in os.getenv("PR180_LIVE_CLIENT_IDS", "jasoncosby1@gmail.com").split(",")
    if s.strip()
)


def _pr180_is_named_live_client(client_id: str, paper: bool) -> bool:
    """True if this submit is for a named live client (currently Jason)."""
    if not PR180_ENABLED:
        return False
    if paper:
        return False
    return (client_id or "").lower().strip() in PR180_LIVE_CLIENT_IDS


def _pr180_jason_live_entry_pricing_guard(
    *,
    submit_bid: float | None,
    submit_mid: float | None,
    submit_ask: float | None,
    spread_pct: float | None,
    proposed_limit: float,
) -> tuple[str, float | None, dict]:
    """
    Pure helper. Returns:
        ("PROCEED",         limit_price, audit_extras)   submit at limit_price
        ("BLOCK",           None,        audit_extras)   block submit with reason in audit_extras
        ("REPRICE_PROCEED", limit_price, audit_extras)   submit at re-priced limit_price

    audit_extras is a dict that should be merged into the orders.meta entry
    pricing audit for full observability of every Jason live submit decision.

    Inputs may be None (degraded quote). When critical fields are missing,
    the guard BLOCKS with ENTRY_QUOTE_INCOMPLETE_LIVE — never proceed on a
    partial quote for a named live client.
    """
    audit: dict = {
        "pr180_active":            True,
        "pr180_max_spread_pct":    PR180_MAX_SPREAD_PCT,
        "pr180_band_min_pct":      PR180_CONTROLLED_BAND_MIN_PCT,
        "pr180_mid_cents":         PR180_CONTROLLED_LIMIT_MID_CENTS,
        "pr180_max_mark_loss_pct": PR180_MAX_EXPECTED_MARK_LOSS_PCT,
        "pr180_input_bid":         submit_bid,
        "pr180_input_mid":         submit_mid,
        "pr180_input_ask":         submit_ask,
        "pr180_input_spread_pct":  spread_pct,
        "pr180_input_proposed":    proposed_limit,
    }

    # Fail-closed: missing critical quote fields → block.
    if submit_mid is None or submit_ask is None or spread_pct is None:
        audit["pr180_block_reason"] = "ENTRY_QUOTE_INCOMPLETE_LIVE"
        return "BLOCK", None, audit
    if submit_mid <= 0 or submit_ask <= 0 or proposed_limit <= 0:
        audit["pr180_block_reason"] = "ENTRY_QUOTE_INCOMPLETE_LIVE"
        return "BLOCK", None, audit

    # Rule 1: hard spread ceiling.
    if spread_pct > PR180_MAX_SPREAD_PCT:
        audit["pr180_block_reason"] = "ENTRY_SPREAD_TOO_WIDE_LIVE"
        return "BLOCK", None, audit

    # Rule 2: controlled-limit band (6%–8% by default).
    final_limit = proposed_limit
    repriced = False
    if spread_pct > PR180_CONTROLLED_BAND_MIN_PCT:
        controlled_cap = round(min(submit_ask, submit_mid + PR180_CONTROLLED_LIMIT_MID_CENTS), 2)
        if final_limit > controlled_cap:
            audit["pr180_controlled_cap"]   = controlled_cap
            audit["pr180_original_limit"]   = proposed_limit
            audit["pr180_repriced_to"]      = controlled_cap
            final_limit = controlled_cap
            repriced = True

    # Rule 3: expected mark-to-mid loss floor (computed on FINAL limit).
    if final_limit <= 0:
        audit["pr180_block_reason"] = "ENTRY_QUOTE_INCOMPLETE_LIVE"
        return "BLOCK", None, audit
    expected_mark_loss_pct = (submit_mid - final_limit) / final_limit
    audit["pr180_expected_mark_loss_pct"] = round(expected_mark_loss_pct, 6)
    if expected_mark_loss_pct <= PR180_MAX_EXPECTED_MARK_LOSS_PCT:
        audit["pr180_block_reason"] = "ENTRY_EXPECTED_MARK_LOSS_TOO_HIGH"
        return "BLOCK", None, audit

    audit["pr180_decision"] = "REPRICE_PROCEED" if repriced else "PROCEED"
    audit["pr180_final_limit"] = final_limit
    return ("REPRICE_PROCEED" if repriced else "PROCEED", final_limit, audit)


# =============================================================================
# EXECUTION CORE
# =============================================================================

class APExecutionCore:
    """
    One instance per client. Orchestrates watcher callbacks, OSM submit of
    already-created entry orders, exit-engine callbacks, and signal logging.
    """

    def __init__(self, broker, supabase_client=None, email: str = "", position_manager=None, order_state_machine=None, data_broker=None, master_control=None, contract_selector=None):
        # PR-B / FIX-8: AP_MODE vs BOT_MODE conflict check at __init__
        # (not at module import — import-time asserts break tests/scripts).
        _ap_mode_env  = (os.environ.get("AP_MODE")  or "").upper()
        _bot_mode_env = (os.environ.get("BOT_MODE") or "").upper()
        if _ap_mode_env and _bot_mode_env and _ap_mode_env != _bot_mode_env:
            raise RuntimeError(
                f"[{email}] AP_MODE={_ap_mode_env!r} conflicts with "
                f"BOT_MODE={_bot_mode_env!r}. Both env vars are set and "
                f"disagree — unsafe before live trading. Set only one, or "
                f"set both to the same value."
            )

        # PR-B / FIX-3: validate master_control FIRST and derive canonical
        # mode BEFORE constructing any submodule. Previously, mode was
        # derived from BOT_MODE, submodules were constructed, then mode
        # was re-derived from master_control — a window where APProofLogger
        # / APExitEngine could see the pre-canonical mode.
        if master_control is None:
            raise RuntimeError(
                f"[{email}] APExecutionCore requires injected master_control. "
                "Production decisions must come from ClientRunner/worker_loop."
            )
        self.master_control = master_control
        log.info("[%s] APExecutionCore using injected master_control", email)

        # PR-B / FIX-3: derive canonical mode from master_control NOW.
        # BOT_MODE is only a last-resort fallback if mc has no .mode attr.
        _mc_mode    = (getattr(master_control, "mode", BOT_MODE) or BOT_MODE).upper()
        self.mode   = _mc_mode
        self.paper  = _mc_mode != "LIVE"
        self._max_positions = getattr(master_control, "max_positions", MAX_POSITIONS)

        # Mode-specific metadata values (derived from canonical mode).
        self._score_floor   = SCORE_FLOOR_PAPER   if self.paper else SCORE_FLOOR_LIVE
        self._context_floor = CONTEXT_FLOOR_PAPER if self.paper else CONTEXT_FLOOR_LIVE

        # Plain attributes (no mode dependency).
        self.broker            = broker
        # P0B paper-quote-truth: attach data_broker onto self.broker so that
        # process_signal (which receives only `broker`) can resolve the live
        # market-data source via getattr(broker, "data_broker", None).
        # data_broker is the live Tradier instance when TRADIER_DATA_TOKEN is
        # set; it equals broker itself when the token is absent (client_runner
        # sets data_broker=broker as a fallback in that case — acceptable
        # because both are then the same object and no silent sandbox leak
        # occurs). This attribute is used only for quote reads, never for
        # order submission.
        if data_broker is not None and data_broker is not broker:
            self.broker.data_broker = data_broker

        # P0 (2026-07-03) — register the underlying quote resolver used by the
        # entry metadata guard's hydration path (ap/underlying_quote_hydration).
        # Quote source preference matches P0B paper-quote-truth: data_broker
        # (live market data) when distinct, else broker itself. Registration is
        # best-effort: failure to register leaves the pre-existing fail-closed
        # behavior fully intact (zero_underlying rejects as before).
        try:
            from ap.underlying_quote_hydration import (
                build_broker_resolver,
                register_quote_resolver,
            )
            _quote_src = data_broker if (data_broker is not None) else broker
            register_quote_resolver(
                build_broker_resolver(_quote_src),
                name=f"{type(_quote_src).__name__}:{'data_broker' if _quote_src is data_broker and data_broker is not broker else 'broker'}",
            )
        except Exception as _hyd_exc:
            log.warning(
                "[%s] underlying quote resolver registration failed (non-fatal, "
                "guard stays fail-closed): %s", email, _hyd_exc,
            )
        self.contract_selector = contract_selector  # wired for breach-time selection of deferred overnight signals
        self.email              = email

        # P0 hotfix — APExecutionCore.client_id was never set, causing
        # AttributeError at breach-time contract selection log calls.
        # `email` IS the canonical client identity — it is the client_id
        # (e.g. "jasoncosby1@gmail.com") passed from ClientRunner.
        # We also set execution_mode as a stable alias of self.mode for
        # use in breach-time logs that interpolate both attributes.
        self.client_id    = email or os.getenv("SINGLE_CLIENT_EMAIL") or ""
        # client_email: prefer the dedicated field if a subclass has set it
        # separately; otherwise alias to client_id (same email address).
        self.client_email = (
            email
            or os.getenv("SINGLE_CLIENT_EMAIL")
            or ""
        )
        self.execution_mode = self.mode  # canonical alias for breach-time log fields
        self.position_manager   = position_manager
        self.order_state_machine = order_state_machine
        self._pos_lock = threading.Lock()
        self._position_count = 0

        # Signal intelligence store + tracker
        # Tracker deduplicates strictly by signal_id so one per client is safe.
        self.store   = APSignalStore(supabase_client, client_email=email)
        self.tracker = APSignalTracker(supabase_client, store=self.store)

        # PR-B / FIX-3 + FIX-4: Submodules constructed AFTER canonical
        # mode + master_control are known. APExitEngine receives
        # master_control at construction time (FIX-4) so the engine
        # never exists without a risk-control reference. APProofLogger
        # receives the canonical mode the first time.
        # PR-C / BUG-EW-5: pass canonical mode to the watcher at
        # construct time so its overnight-revalidation fail-closed-on-LIVE
        # branch actually fires for LIVE clients. Previously the watcher
        # had no self.mode attribute and silently used PAPER fail-open
        # for every client — a live quote outage would arm setups that
        # should have been invalidated.
        self.entry_watcher = APEntryWatcher(
            broker,
            order_state_machine=self.order_state_machine,
            mode=self.mode,  # canonical mode from master_control
        )
        self.exit_eng    = APExitEngine(
            broker,
            email=email,
            data_broker=data_broker,
            master_control=self.master_control,  # FIX-4: construct-time wiring
        )
        self.feedback    = APFeedbackLoop(supabase_client, DISCORD_WEBHOOK_URL, signal_store=self.store)
        self.shadow      = APShadowTracker(supabase_client, DISCORD_WEBHOOK_URL)
        self._sector_counts: dict[str, int] = {}
        self._sector_lock   = threading.Lock()
        self.proof          = APProofLogger(
            supabase_client=supabase_client,
            client_email=email,
            mode="paper" if self.paper else "live",  # canonical from the start
        )
        try:
            from ap_edge_intelligence import APTradeLogger as _ATL
            self._edge_logger = _ATL()
        except Exception:
            self._edge_logger = None

        # PR-B / FIX-3: master_control validation, canonical mode
        # derivation, and submodule construction (above) all happen
        # BEFORE this point. The old "MASTER CONTROL" + "Mode override"
        # blocks that used to live here are gone — work is done upfront.

        # Wire watcher callbacks
        self.entry_watcher.on_trigger    = self._on_entry_trigger
        self.entry_watcher.on_expire     = self._on_signal_expire
        self.entry_watcher.on_invalidate = self._on_signal_invalidate

        # Wire exit callbacks
        self.exit_eng.on_exit  = self._on_position_close
        self.exit_eng.on_scale = self._on_position_scale
        # FIX 2: broker-confirmed fill callback — writes proof with actual fill price
        self.exit_eng.on_exit_fill_confirmed = self._finalize_proof
        # PR-B / FIX-4 belt-and-suspenders: master_control was wired at
        # exit-engine construction time above; re-assign here only if
        # somehow missing (idempotent no-op in normal path). One-way
        # reference: exit engine READS the flag, never sets it.
        try:
            if getattr(self.exit_eng, "master_control", None) is None:
                self.exit_eng.master_control = self.master_control
        except Exception as _e:
            log.warning("exit_eng_master_control_wire_failed: %s", _e)

        log.info(
            f"APExecutionCore initialized for {email} | "
            f"Mode: {self.mode} | "
            f"MaxPos: {self._max_positions} | "
            f"ScoreFloor: {self._score_floor} | "
            f"ContextFloor: {self._context_floor}"
        )

    @property
    def _exit_thread(self) -> Optional[threading.Thread]:
        """Expose exit engine thread so worker_health can check liveness."""
        return self.exit_eng._thread

    def _score_and_context_floors(self) -> tuple[float, float]:
        """Single source of truth for score/context floors per mode."""
        if self.mode == "LIVE":
            return SCORE_FLOOR_LIVE, CONTEXT_FLOOR_LIVE
        return SCORE_FLOOR_PAPER, CONTEXT_FLOOR_PAPER

    def _current_open_position_count(self) -> int:
        """
        Return the most reliable open-position count available.

        Production path uses APPositionManager/Postgres truth. The local
        counter is only a defensive fallback if snapshot() is unavailable.
        """
        if self.position_manager is not None:
            try:
                snap = self.position_manager.snapshot()
                return int(snap.get("open_count") or 0)
            except Exception as exc:
                log.warning(
                    "[%s] position_manager.snapshot failed — falling back to local count: %s",
                    self.email, exc,
                )
        with self._pos_lock:
            return int(self._position_count or 0)

    def _current_pending_entry_count(self) -> int:
        """Return pending entry count from position-manager snapshot when available."""
        if self.position_manager is not None:
            try:
                snap = self.position_manager.snapshot()
                return int(snap.get("pending_entries") or 0)
            except Exception:
                return 0
        return 0

    def _available_position_slots(self) -> int:
        """
        Slots based on broker/DB truth first, local legacy count second.
        This prevents over-dispatch if DB truth reports active/pending exposure.
        """
        open_count = self._current_open_position_count()
        pending_entries = self._current_pending_entry_count()
        return max(0, int(self._max_positions) - open_count - pending_entries)

    def _recover_plan_for_revalidation(self, watched: WatchedSignal):
        """
        Recover a minimal ApprovedExecutionPlan-like object for breach-time
        exposure revalidation when the watcher signal came from queue.watch(plan)
        and did not carry _approved_plan through the signal dict.

        This keeps the queue as the production entry authority while preserving
        LIVE-mode capital protection at breach time. If recovery cannot prove
        a real reserved/limit cost, LIVE mode will block rather than fail open.
        """
        sig = watched.signal or {}
        existing = sig.get("_approved_plan")
        if existing is not None:
            return existing

        local_order_id = str(sig.get("local_order_id") or "")
        if not local_order_id or self.order_state_machine is None:
            return None

        try:
            order = self.order_state_machine.get_order(local_order_id)
        except Exception as exc:
            log.warning(
                "[%s] Could not recover approved plan from OSM order %s: %s",
                watched.ticker, local_order_id, exc,
            )
            return None

        if not order:
            return None

        try:
            qty = int(order.get("qty") or 0)
            reserved = float(order.get("reserved_cost") or 0)
            limit_price = float(order.get("limit_price") or 0)
            real_cost = reserved if reserved > 0 else (limit_price * qty * 100 if limit_price > 0 and qty > 0 else 0.0)
            if real_cost <= 0:
                log.critical(
                    "[%s] Recovered OSM order %s but could not prove real_cost for LIVE breach revalidation",
                    watched.ticker, local_order_id,
                )
                return None

            recovered = SimpleNamespace(
                plan_id=str(order.get("plan_id") or sig.get("plan_id") or local_order_id),
                signal_id=str(order.get("signal_id") or sig.get("signal_id") or local_order_id),
                client_id=str(order.get("client_id") or self.email or "default"),
                ticker=str(order.get("symbol") or watched.ticker),
                side=str(order.get("direction") or watched.side),
                direction=str(order.get("direction") or watched.side),
                pattern=str(sig.get("pattern") or ""),
                timeframe=str(sig.get("timeframe") or "1d"),
                contracts=qty,
                max_position_usd=real_cost,
                tier=str(sig.get("grade") or sig.get("tier") or "B"),
                score=float(sig.get("score") or 0),
                trigger_type="breach",
                trigger_price=getattr(watched, "entry_trigger", watched.trigger_price),
                stop_underlying=watched.stop_level,
                target_underlying=watched.target_price,
                contract_symbol=str(order.get("contract") or sig.get("contract_symbol") or ""),
                limit_price=limit_price if limit_price > 0 else None,
            )
            sig["_approved_plan"] = recovered
            log.info(
                "[%s] Recovered approved plan for breach revalidation from OSM order %s | cost=$%.0f",
                watched.ticker, local_order_id, real_cost,
            )
            return recovered
        except Exception as exc:
            log.warning(
                "[%s] Failed to recover breach revalidation plan from OSM order %s: %s",
                watched.ticker, local_order_id, exc,
            )
            return None

    @staticmethod
    def _is_real_occ_contract(contract_symbol: str, ticker: str = "") -> bool:
        contract_symbol = str(contract_symbol or "").strip().upper()
        ticker = str(ticker or "").strip().upper()
        if not contract_symbol or contract_symbol.startswith("DEFERRED:"):
            return False
        if ticker and contract_symbol == ticker:
            return False
        return bool(_OCC_CONTRACT_RE.search(contract_symbol))

    def _refresh_hydrated_prebreach_plan(
        self,
        *,
        approved_plan,
        sig: dict,
        local_order_id: str,
        ticker: str,
    ) -> bool:
        """Bridge hydrated DB state into the watcher-held approved plan.

        P0 invariant: a successful pre-breach hydration must be consumed by the
        breach callback even if the watcher still holds the original
        DEFERRED:<ticker> plan object.
        """
        if (
            approved_plan is None
            or not local_order_id
            or self.order_state_machine is None
            or not hasattr(self.order_state_machine, "get_order")
        ):
            return False

        try:
            order = self.order_state_machine.get_order(local_order_id)
        except Exception as exc:
            log.debug("[%s] hydrated pre-breach refresh get_order failed for %s: %s", ticker, local_order_id, exc)
            return False

        if not isinstance(order, dict):
            return False

        contract = str(order.get("contract") or "").strip()
        if not self._is_real_occ_contract(contract, ticker):
            return False
        if order.get("broker_order_id") or order.get("submitted_ts"):
            return False

        try:
            limit_price = float(order.get("limit_price") or 0)
        except Exception:
            limit_price = 0.0
        if limit_price <= 0.01:
            return False

        try:
            qty = int(order.get("qty") or 0)
        except Exception:
            qty = 0
        if qty <= 0:
            return False

        try:
            reserved_cost = float(order.get("reserved_cost") or 0)
        except Exception:
            reserved_cost = 0.0
        if reserved_cost <= 0:
            reserved_cost = round(limit_price * qty * 100.0, 2)

        meta = getattr(approved_plan, "metadata", None)
        if not isinstance(meta, dict):
            meta = {}
            try:
                approved_plan.metadata = meta
            except Exception:
                pass

        approved_plan.contract_symbol = contract
        approved_plan.limit_price = limit_price
        approved_plan.contracts = qty
        approved_plan.max_position_usd = reserved_cost

        meta.update({
            "selected_contract": contract,
            "contract_symbol": contract,
            "limit_price": limit_price,
            "contracts": qty,
            "max_position_usd": reserved_cost,
            "reserved_cost": reserved_cost,
            "contract_deferred": False,
            "contract_selection_status": "HYDRATED_PRE_BREACH",
            "contract_materialized_source": "prebreach_hydration",
        })

        sig["contract_deferred"] = False
        sig["contract_symbol"] = contract
        sig["selected_contract"] = contract
        sig["limit_price"] = limit_price
        sig["contracts"] = qty
        sig["reserved_cost"] = reserved_cost
        sig["contract_selection_status"] = "HYDRATED_PRE_BREACH"
        sig["contract_materialized_source"] = "prebreach_hydration"
        sig["_approved_plan"] = approved_plan

        log.info(
            "[%s] PREBREACH_HYDRATION_BRIDGE_APPLIED | local=%s contract=%s limit=%.2f qty=%s reserved=%.2f",
            ticker,
            local_order_id,
            contract,
            limit_price,
            qty,
            reserved_cost,
        )
        return True

    # ── Diagnostic-only helper (PR hotfix/breach-block-diagnostics) ──────────
    # Emits a single structured log line for every silent block / exception
    # path in _breach_risk_check and _on_entry_trigger. The bot is working;
    # this PR adds zero behavior changes. Operators grep Render logs for
    # BREACH_RISK_CHECK_BLOCKED / BREACH_RISK_CHECK_EXCEPTION /
    # WATCHER_ON_TRIGGER_RETURNED / WATCHER_ON_TRIGGER_EXCEPTION /
    # ENTRY_TRIGGER_BLOCKED_RETURN to diagnose stuck PENDING_TRIGGER rows.
    def _emit_breach_diag(
        self,
        event: str,
        *,
        watched: "WatchedSignal",
        reason: str,
        positions_open: object = "n/a",
        pending_entries: object = "n/a",
        max_positions: object = "n/a",
        current_total_exposure: object = "n/a",
        remaining_total_cap: object = "n/a",
        mc_block_reason: str = "",
        exception_type: str = "",
        exception_message: str = "",
        level: str = "warning",
    ) -> None:
        """Emit one structured diagnostic line. Never raises.

        Field set is fixed across emissions so log-grep stays stable:
        client_id, local_order_id, signal_id, symbol, contract, reason,
        positions_open, pending_entries, max_positions, current_total_exposure,
        remaining_total_cap, mc_block_reason, exception_type, exception_message,
        execution_mode.
        """
        try:
            sig             = getattr(watched, "signal", {}) or {}
            client_id_val   = (
                getattr(self, "client_id", None)
                or getattr(self, "email", None)
                or sig.get("client_email")
                or "n/a"
            )
            local_order_id  = sig.get("local_order_id") or "n/a"
            signal_id       = sig.get("signal_id") or "n/a"
            symbol          = getattr(watched, "ticker", None) or sig.get("ticker") or "n/a"
            contract        = (
                (sig.get("plan") or {}).get("contract_symbol")
                or sig.get("contract_symbol")
                or sig.get("contract")
                or "n/a"
            )
            execution_mode  = getattr(self, "mode", "n/a")

            msg = (
                f"{event} client_id={client_id_val} local_order_id={local_order_id} "
                f"signal_id={signal_id} symbol={symbol} contract={contract} "
                f"reason={reason} execution_mode={execution_mode} "
                f"positions_open={positions_open} pending_entries={pending_entries} "
                f"max_positions={max_positions} "
                f"current_total_exposure={current_total_exposure} "
                f"remaining_total_cap={remaining_total_cap} "
                f"mc_block_reason={mc_block_reason or 'n/a'} "
                f"exception_type={exception_type or 'n/a'} "
                f"exception_message={(exception_message or 'n/a')[:200]}"
            )
            if level == "critical":
                log.critical(msg)
            elif level == "error":
                log.error(msg)
            elif level == "info":
                log.info(msg)
            else:
                log.warning(msg)
        except Exception:  # pragma: no cover — never let diagnostics break flow
            try:
                log.warning("BREACH_DIAG_EMIT_FAILED event=%s", event)
            except Exception:
                pass

    def _breach_risk_check(self, watched: WatchedSignal) -> bool:
        """
        Lightweight breach-time safety check.

        IMPORTANT: Do not call master_control.evaluate() here. The signal was
        already approved and armed, and evaluate() performs dedup/persistence
        side effects that are wrong for an in-flight signal. This check only
        verifies kill-switch and current position capacity using live state.
        """
        sig = watched.signal
        ticker = watched.ticker
        signal_id = str(sig.get("signal_id", "") or "")

        if getattr(self, "_kill_switch", False):
            log.critical("[%s] Breach blocked — execution core kill switch active", ticker)
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "blocked_at_breach",
                    "context_notes": "kill_switch_active_at_breach",
                })
            self._emit_breach_diag(
                "BREACH_RISK_CHECK_BLOCKED",
                watched=watched,
                reason="kill_switch_active",
                max_positions=getattr(self, "_max_positions", "n/a"),
                level="critical",
            )
            return False

        if self.master_control is not None:
            try:
                kill_fn = getattr(self.master_control, "_kill_switch_fn", None)
                if kill_fn and kill_fn():
                    log.critical("[%s] Breach blocked — master control kill switch active", ticker)
                    if signal_id:
                        self.store.update_signal_fields(signal_id, {
                            "decision_status": "blocked_at_breach",
                            "context_notes": "master_control_kill_switch_active_at_breach",
                        })
                    self._emit_breach_diag(
                        "BREACH_RISK_CHECK_BLOCKED",
                        watched=watched,
                        reason="master_control_kill_switch_active",
                        max_positions=getattr(self, "_max_positions", "n/a"),
                        level="critical",
                    )
                    return False
            except Exception as exc:
                log.warning("[%s] Kill-switch check failed at breach: %s", ticker, exc)
                self._emit_breach_diag(
                    "BREACH_RISK_CHECK_EXCEPTION",
                    watched=watched,
                    reason="kill_switch_check_exception",
                    max_positions=getattr(self, "_max_positions", "n/a"),
                    exception_type=type(exc).__name__,
                    exception_message=str(exc),
                    level="warning",
                )

        open_count = self._current_open_position_count()
        pending_entries = self._current_pending_entry_count()
        effective_count = open_count + pending_entries
        if effective_count >= int(self._max_positions):
            log.info(
                "[%s] No slot at breach time — open=%s pending=%s max=%s. Blocking queued entry.",
                ticker, open_count, pending_entries, self._max_positions,
            )
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "blocked_at_breach",
                    "context_notes": (
                        f"positions_full_at_breach open={open_count} "
                        f"pending={pending_entries} max={self._max_positions}"
                    ),
                })
            _sector = sig.get("sector") or sig.get("correlation_bucket") or ticker
            try:
                self._cleanup_pending_entry_order(watched, action="cancel", reason="positions_full_at_breach")
            except Exception as _clean_err:
                log.error("[%s] Failed to cleanup pending entry order: %s", ticker, _clean_err)
            self._emit_breach_diag(
                "BREACH_RISK_CHECK_BLOCKED",
                watched=watched,
                reason="positions_full_at_breach",
                positions_open=open_count,
                pending_entries=pending_entries,
                max_positions=self._max_positions,
                level="info",
            )
            return False

        approved_plan = self._recover_plan_for_revalidation(watched)
        if approved_plan is None:
            msg = "approved_plan_missing_at_breach_revalidation"
            if self.mode == "LIVE":
                log.critical(
                    "[%s] LIVE BREACH BLOCK — _approved_plan missing; cannot revalidate exposure safely",
                    ticker,
                )
                if signal_id:
                    self.store.update_signal_fields(signal_id, {
                        "decision_status": "blocked_at_breach",
                        "context_notes": msg,
                    })
                _sector = sig.get("sector") or sig.get("correlation_bucket") or ticker
                self._emit_breach_diag(
                    "BREACH_RISK_CHECK_BLOCKED",
                    watched=watched,
                    reason="approved_plan_missing_at_breach_revalidation",
                    positions_open=open_count,
                    pending_entries=pending_entries,
                    max_positions=self._max_positions,
                    level="critical",
                )
                return False
            log.critical(
                "[%s] PAPER BREACH WARNING — _approved_plan missing; continuing without exposure revalidation",
                ticker,
            )
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "context_notes": msg + "_paper_fail_open",
                })

        if approved_plan is not None and self.master_control is not None:
            try:
                reval = self.master_control.revalidate_exposure(
                    approved_plan,
                    client_id=self.email or "default",
                )
                if not getattr(reval, "ok", False):
                    reason = getattr(reval, "reason", "revalidation_failed")
                    log.info("[%s] Breach exposure revalidation blocked: %s", ticker, reason)
                    if signal_id:
                        self.store.update_signal_fields(signal_id, {
                            "decision_status": "blocked_at_breach",
                            "context_notes": f"exposure_revalidation={reason}",
                        })
                    _sector = sig.get("sector") or sig.get("correlation_bucket") or ticker
                    # Best-effort capacity numbers for diagnostics
                    _cur_total_exp = (
                        getattr(reval, "current_total_exposure", None)
                        if hasattr(reval, "current_total_exposure") else "n/a"
                    )
                    _rem_total_cap = (
                        getattr(reval, "remaining_total_cap", None)
                        if hasattr(reval, "remaining_total_cap") else "n/a"
                    )
                    self._emit_breach_diag(
                        "BREACH_RISK_CHECK_BLOCKED",
                        watched=watched,
                        reason="exposure_revalidation_blocked",
                        positions_open=open_count,
                        pending_entries=pending_entries,
                        max_positions=self._max_positions,
                        current_total_exposure=(
                            _cur_total_exp if _cur_total_exp is not None else "n/a"
                        ),
                        remaining_total_cap=(
                            _rem_total_cap if _rem_total_cap is not None else "n/a"
                        ),
                        mc_block_reason=str(reason),
                        level="warning",
                    )
                    return False
            except Exception as exc:
                if self.mode == "LIVE":
                    log.critical(
                        "[%s] LIVE BREACH BLOCK — exposure revalidation errored: %s",
                        ticker, exc,
                    )
                    if signal_id:
                        self.store.update_signal_fields(signal_id, {
                            "decision_status": "blocked_at_breach",
                            "context_notes": f"exposure_revalidation_error={exc}",
                        })
                    _sector = sig.get("sector") or sig.get("correlation_bucket") or ticker
                    self._emit_breach_diag(
                        "BREACH_RISK_CHECK_EXCEPTION",
                        watched=watched,
                        reason="exposure_revalidation_error_live",
                        positions_open=open_count,
                        pending_entries=pending_entries,
                        max_positions=self._max_positions,
                        exception_type=type(exc).__name__,
                        exception_message=str(exc),
                        level="critical",
                    )
                    return False
                log.warning("[%s] PAPER breach exposure revalidation failed open: %s", ticker, exc)
                self._emit_breach_diag(
                    "BREACH_RISK_CHECK_EXCEPTION",
                    watched=watched,
                    reason="exposure_revalidation_error_paper_fail_open",
                    positions_open=open_count,
                    pending_entries=pending_entries,
                    max_positions=self._max_positions,
                    exception_type=type(exc).__name__,
                    exception_message=str(exc),
                    level="warning",
                )

        return True

    def start(self):
        """Start production orchestration threads only.

        Pure production contract:
          - Queue/worker owns signal evaluation and plan creation.
          - OSM owns order lifecycle.
          - EntryWatcher only waits for breach and calls this core back.
        """
        problems = []
        if self.master_control is None:
            problems.append("master_control_missing")
        if self.order_state_machine is None:
            problems.append("order_state_machine_missing")
        if self.entry_watcher is None:
            problems.append("entry_watcher_missing")
        if getattr(self.entry_watcher, "on_trigger", None) != self._on_entry_trigger:
            problems.append("entry_watcher_on_trigger_not_wired")
        if getattr(self.entry_watcher, "order_state_machine", None) is not self.order_state_machine:
            problems.append("entry_watcher_osm_not_wired")
        if self.exit_eng is None:
            problems.append("exit_engine_missing")
        if self.tracker is None:
            problems.append("tracker_missing")
        # PR-B / FIX-5: exit-engine callback + master_control preflight.
        # If these are missing, positions accumulate with no exits or
        # no risk-control reference — the worst-possible failure mode.
        if self.exit_eng is not None:
            if getattr(self.exit_eng, "on_exit", None) is None:
                problems.append("exit_eng_on_exit_not_wired")
            if getattr(self.exit_eng, "on_scale", None) is None:
                problems.append("exit_eng_on_scale_not_wired")
            if getattr(self.exit_eng, "master_control", None) is None:
                problems.append("exit_eng_master_control_not_wired")
        # PR-B / FIX-5: LIVE mode must not start without a position_manager.
        # In paper, position_manager is optional (local fallback counter
        # is acceptable). In LIVE, broker/DB truth is required.
        if not self.paper and self.position_manager is None:
            problems.append("position_manager_missing")
        if problems:
            raise RuntimeError(f"[{self.email}] APExecutionCore production startup validation failed: {','.join(problems)}")

        self.entry_watcher.start()
        self.exit_eng.start()
        self.tracker.start()
        log.info(f"[{self.email}] Execution core started (PURE_PRODUCTION: watcher + exit engine + tracker; queue is entry authority)")

    def stop(self):
        self.entry_watcher.stop()
        self.exit_eng.stop()
        self.tracker.stop()

    # ── HELPER: paper-mode exec quality fallbacks ─────────────────────────────

    # ── PUBLIC: receive incoming scanner signal ───────────────────────────────

    def receive_signal(self, signal: dict, score_result=None):
        """Compatibility shim only.

        Pure production does not evaluate, rank, or submit entries inside
        APExecutionCore. Incoming scanner signals are routed to the unified
        Postgres queue, where worker_loop + master_control + contract_selector
        + OSM own the entry lifecycle.
        """
        try:
            from ap.queue import enqueue_signal
            signal_id = str(signal.get("signal_id") or uuid.uuid4())
            signal["signal_id"] = signal_id
            ok = enqueue_signal(
                signal,
                client_id=self.email or "default",
                idempotency_key=f"{signal_id}:{self.email or 'default'}",
            )
            log.info(
                "[%s] receive_signal routed to production queue | inserted=%s",
                signal.get("ticker") or signal.get("symbol") or "?",
                ok,
            )
        except Exception as exc:
            log.critical(
                "[%s] receive_signal queue route failed: %s",
                signal.get("ticker") or signal.get("symbol") or "?",
                exc,
            )
        return

    def _mark_breach_failure(
        self,
        watched: WatchedSignal,
        *,
        decision_status: str,
        context_note: str,
        funnel_key: Optional[str] = None,
        cleanup_action: Optional[str] = None,
    ) -> None:
        """Record a breach-time non-entry in every local truth surface.

        Once the watcher fires, the watch has already left the pending queue.
        Therefore every early return must be explicit: signal-store status,
        funnel counter, and OSM cleanup for unsubmitted queue-created orders
        when applicable.
        """
        sig = getattr(watched, "signal", {}) or {}
        signal_id = str(sig.get("signal_id") or "")
        ticker = getattr(watched, "ticker", sig.get("ticker", "?"))

        if signal_id:
            try:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": decision_status,
                    "context_notes": context_note,
                })
            except Exception as exc:
                log.warning("[%s] breach failure signal-store update failed: %s", ticker, exc)

        if funnel_key:
            try:
                funnel.inc(funnel_key)
            except Exception:
                pass

        if cleanup_action in {"expire", "cancel"}:
            try:
                self._cleanup_pending_entry_order(
                    watched,
                    action=cleanup_action,
                    reason=context_note,
                )
            except Exception as exc:
                log.error(
                    "[%s] breach failure OSM cleanup failed | action=%s reason=%s error=%s",
                    ticker, cleanup_action, context_note, exc, exc_info=True,
                )

    # ── CALLBACK: Breach Confirmed -> Execute ─────────────────────────────────

    def _on_entry_trigger(self, watched: WatchedSignal):
        """Called by watcher when price holds above/below trigger for 2 polls.

        Production cohesion rule:
          - Approved at queue time.
          - Revalidated at breach time.
          - Submitted unchanged.

        This method intentionally does NOT re-run contract selection, options
        intelligence, premium-bound checks, sizing, or fresh plan creation.
        Queue/worker + master_control + contract_selector own those decisions.
        """
        sig = watched.signal or {}
        ticker = watched.ticker
        signal_id = str(sig.get("signal_id", "") or "")

        # P0 hotfix — resolve client identity from the signal dict first, then
        # fall back to self.client_id (set in __init__), then self.email.
        # This ensures breach-time logs never throw AttributeError and that
        # the most-specific client identity (from the row/signal) is used even
        # if self.client_id is somehow stale or missing.
        _breach_client_id = (
            str(sig.get("client_id") or sig.get("client_email") or "").strip()
            or getattr(self, "client_id", None)
            or self.email
            or ""
        )
        if not _breach_client_id:
            # Hard guard: log with ticker and write a sentinel last_error so
            # the failure is diagnosable rather than an opaque AttributeError.
            log.warning(
                "[%s] BREACH_TIME_CONTRACT_SELECTION_FAILED "
                "reason=missing_client_id — client identity cannot be resolved; "
                "breach-time selection will continue but logs will be incomplete",
                ticker,
            )

        trigger_price = getattr(watched, "trigger_price", None)
        try:
            trigger_price_for_log = float(trigger_price or 0)
        except Exception:
            trigger_price_for_log = 0.0

        log.info(
            "[%s] Breach confirmed @ $%.2f -- submitting approved queued plan",
            ticker,
            trigger_price_for_log,
        )

        if signal_id:
            self.store.update_status(signal_id, "triggered", timestamp_flag="triggered_at")
        funnel.inc("watcher_triggered")

        # 1) Revalidate only. Never re-run selection/sizing logic here.
        if not self._breach_risk_check(watched):
            funnel.inc("master_control_blocked")
            self._emit_breach_diag(
                "ENTRY_TRIGGER_BLOCKED_RETURN",
                watched=watched,
                reason="breach_risk_check_false",
                max_positions=getattr(self, "_max_positions", "n/a"),
                level="info",
            )
            return

        # 2) Require OSM + existing queue-created local order id.
        if self.order_state_machine is None:
            log.critical("[%s] PRODUCTION_ENTRY_BLOCK — order_state_machine missing at breach", ticker)
            funnel.inc("order_failed")
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "blocked_at_breach",
                    "context_notes": "order_state_machine_missing_at_breach",
                })
            return

        queue_local_order_id = str(sig.get("local_order_id") or "").strip()
        if not queue_local_order_id:
            log.critical("[%s] PRODUCTION_ENTRY_BLOCK — local_order_id missing from watcher signal", ticker)
            funnel.inc("order_failed")
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "blocked_at_breach",
                    "context_notes": "local_order_id_missing_at_breach",
                })
            return

        if not hasattr(self.order_state_machine, "submit_existing_entry"):
            log.critical("[%s] PRODUCTION_ENTRY_BLOCK — OSM missing submit_existing_entry", ticker)
            _reason = "osm_missing_submit_existing_entry"
            funnel.inc("order_failed")
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "blocked_at_breach",
                    "context_notes": _reason,
                })
            self._cleanup_pending_entry_order(watched, action="expire", reason=_reason)
            return

        def _terminalize_breach_failure(
            reason: str,
            *,
            cleanup_action: str = "expire",
            meta_patch: dict | None = None,
            decision_status: str = "blocked_at_breach",
            context_notes: str | None = None,
            funnel_key: str = "order_failed",
        ) -> None:
            if funnel_key:
                funnel.inc(funnel_key)
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": decision_status,
                    "context_notes": context_notes or reason,
                })
            if queue_local_order_id and self.order_state_machine is not None and meta_patch:
                try:
                    update_meta = getattr(self.order_state_machine, "update_order_meta", None)
                    if callable(update_meta):
                        update_meta(queue_local_order_id, meta_patch)
                except Exception as _meta_exc:
                    log.warning("[%s] breach failure meta persist failed: %s", ticker, _meta_exc)
            self._cleanup_pending_entry_order(watched, action=cleanup_action, reason=reason)
            return

        def _terminalize_deferred_breach_failure(reason: str, *, extra_meta: dict | None = None) -> None:
            """Best-effort cleanup for deferred breach failures.

            Acceptance contract:
              a trigger_ready deferred order must either submit with a real
              contract, or end terminal with last_error populated.
            """
            meta_patch = {
                "deferred_breach_failure": True,
                "deferred_breach_reason": reason,
                "local_order_id": queue_local_order_id,
            }
            if extra_meta:
                meta_patch.update(extra_meta)
            _terminalize_breach_failure(
                reason,
                cleanup_action="expire",
                meta_patch=meta_patch,
                context_notes=reason,
            )
            return

        # ── PR3 (no-silent-deferred-trigger-exits): canonical terminal outcome ──
        # Every triggered deferred row MUST leave exactly one explicit TERMINAL
        # outcome so no trigger returns silently. Observability only — it records
        # the outcome the existing code paths already produce; it changes no
        # decision or order action.
        #
        # IMPORTANT (review amendment): "contract selected" is PROGRESS, not a
        # terminal state. The real terminal outcome of a successful deferred
        # entry is BREACH_BROKER_SUBMITTED (or BREACH_SUBMISSION_SKIPPED on a
        # submit failure). So contract-selected is emitted on a SEPARATE,
        # non-terminal channel that does NOT consume the exactly-once terminal
        # slot — otherwise it would block the true terminal outcome that follows.
        #
        # TERMINAL outcomes (exactly one per triggered deferred row):
        #   BREACH_RISK_CHECK_BLOCKED      _breach_risk_check returned False
        #   BREACH_SELECTOR_RETURNED_NONE  selector.select() returned None
        #   BREACH_SELECTOR_EXCEPTION      selector.select() raised
        #   BREACH_SUBMISSION_SKIPPED      submit attempted, OSM returned not-ok
        #   BREACH_BROKER_SUBMITTED        order handed to broker submit path
        #   NO_VALID_PLAYBOOK_DTE_CONTRACT no survivor in any evaluated DTE bucket
        #   UNTRADEABLE_FOR_ACCOUNT_SIZE   quality contract exists but exceeds budget
        #   DATA_MISSING_OI_VOLUME         chain returned with zero OI/volume fields
        #
        # PROGRESS (non-terminal, never consumes the terminal slot):
        #   BREACH_CONTRACT_SELECTED       real OCC contract chosen, proceeding
        #
        # _deferred_outcome["emitted"] is the sentinel the post-trigger guard checks.
        _TERMINAL_DEFERRED_OUTCOMES = frozenset({
            "BREACH_RISK_CHECK_BLOCKED",
            "BREACH_SELECTOR_RETURNED_NONE",
            "BREACH_SELECTOR_EXCEPTION",
            "BREACH_SUBMISSION_SKIPPED",
            "BREACH_BROKER_SUBMITTED",
            "NO_VALID_PLAYBOOK_DTE_CONTRACT",
            "UNTRADEABLE_FOR_ACCOUNT_SIZE",
            "DATA_MISSING_OI_VOLUME",
            # P0 amendment #5+#6 (PR #294 final hardening): terminal failure
            # to prove the persisted `orders` row after selector success.
            # Maps to TERMINAL_NO_TRADEABLE_CONTRACT with
            # MATERIALIZATION_ORDER_ROW_UNREADABLE detail.
            "DEFERRED_ORDER_ROW_UNREADABLE",
        })
        _deferred_outcome = {"emitted": False, "outcome": None, "is_deferred": False}

        def _emit_deferred_progress(
            outcome: str,
            *,
            contract: str = "",
            extra: dict | None = None,
        ) -> None:
            """Log a NON-TERMINAL deferred milestone (e.g. BREACH_CONTRACT_SELECTED).
            Deferred-guarded but does NOT set the exactly-once terminal sentinel,
            so it can never block the real terminal outcome that follows. Never
            raises."""
            if not _deferred_outcome.get("is_deferred"):
                return
            try:
                payload = {
                    "outcome": outcome,
                    "local_order_id": queue_local_order_id or "",
                    "signal_id": signal_id or "",
                    "symbol": ticker,
                    "execution_mode": getattr(self, "mode", "n/a"),
                    "contract": contract or "",
                }
                if extra:
                    for _k, _v in extra.items():
                        payload[_k] = _v
                _fields = " ".join(f"{k}={v}" for k, v in payload.items())
                log.info("DEFERRED_TRIGGER_PROGRESS %s", _fields)
            except Exception:
                pass

        def _emit_deferred_outcome(
            outcome: str,
            *,
            reason: str = "",
            contract: str = "",
            broker_order_id: str = "",
            extra: dict | None = None,
        ) -> None:
            """Emit EXACTLY ONE canonical TERMINAL outcome for a triggered
            DEFERRED row. Never raises. Always includes local_order_id +
            signal_id so the event joins back to the order row.

            Three guards (per review amendments):
              1. Deferred-only: no-op unless this trigger is a deferred entry.
              2. Terminal-only: a non-terminal code (e.g. BREACH_CONTRACT_SELECTED)
                 is rejected here — those go through _emit_deferred_progress so
                 they never consume the terminal slot.
              3. Exactly-once: the first TERMINAL emission wins; later calls are
                 ignored so a row can never carry two terminal outcomes.
            """
            if not _deferred_outcome.get("is_deferred"):
                return
            if outcome not in _TERMINAL_DEFERRED_OUTCOMES:
                # Defensive: a non-terminal code must never reach the terminal
                # channel. Route it to progress logging instead of consuming the
                # exactly-once slot.
                _emit_deferred_progress(outcome, contract=contract, extra=extra)
                return
            if _deferred_outcome.get("emitted"):
                return
            _deferred_outcome["emitted"] = True
            _deferred_outcome["outcome"] = outcome
            try:
                payload = {
                    "outcome": outcome,
                    "local_order_id": queue_local_order_id or "",
                    "signal_id": signal_id or "",
                    "symbol": ticker,
                    "execution_mode": getattr(self, "mode", "n/a"),
                    "reason": reason or "",
                    "contract": contract or "",
                    "broker_order_id": broker_order_id or "",
                }
                if extra:
                    for _k, _v in extra.items():
                        payload[_k] = _v
                _fields = " ".join(f"{k}={v}" for k, v in payload.items())
                if outcome == "BREACH_BROKER_SUBMITTED":
                    log.info("DEFERRED_TRIGGER_OUTCOME %s", _fields)
                else:
                    log.warning("DEFERRED_TRIGGER_OUTCOME %s", _fields)
            except Exception:
                try:
                    log.warning("DEFERRED_TRIGGER_OUTCOME_EMIT_FAILED outcome=%s", outcome)
                except Exception:
                    pass
            # ── P0 (monday-trade-flow-readiness, amended): persist the
            # canonical tri-outcome onto the order row so the operator can
            # answer "what happened to this deferred trigger?" from
            # orders.meta alone — no log spelunking. Best-effort, never
            # raises, exactly-once by construction (this emitter is the
            # exactly-once terminal channel). Selected bid/ask/mid and the
            # final submit limit arrive via `extra` from the call sites and
            # are merged verbatim.
            try:
                if queue_local_order_id and self.order_state_machine is not None:
                    _update_meta = getattr(
                        self.order_state_machine, "update_order_meta", None
                    )
                    if callable(_update_meta):
                        # P0 amendment #2 (PR #294 review): submit-cap
                        # terminals reuse the external UNTRADEABLE_FOR_ACCOUNT_SIZE
                        # code (operator vocabulary continuity) but need to
                        # preserve their fine-grained detail
                        # (ACCEPTANCE_CAP_EXCEEDED_AT_SUBMIT vs
                        # ACCEPTANCE_CAP_MISCONFIGURED). When the caller
                        # supplies materialization_detail_override in extra,
                        # it wins over the raw outcome string; otherwise the
                        # raw outcome remains the detail. Never becomes the
                        # OUTCOME — mapping to MATERIALIZED_AND_SUBMITTED /
                        # TERMINAL_NO_TRADEABLE_CONTRACT still keys off
                        # `outcome`, not the override.
                        _detail = outcome
                        if extra and "materialization_detail_override" in extra:
                            _detail = str(extra.get("materialization_detail_override") or outcome)
                        _canon_meta: dict = {
                            "entry_path": _MATERIALIZATION_ENTRY_PATH,
                            "materialization_outcome": _canonical_materialization_outcome(outcome),
                            "materialization_detail": _detail,
                            "materialization_reason": reason or "",
                            "materialization_contract": contract or "",
                            "materialization_broker_order_id": broker_order_id or "",
                            "materialization_ts": datetime.now(timezone.utc).isoformat(),
                        }
                        if extra:
                            for _mk, _mv in extra.items():
                                if _mk == "materialization_detail_override":
                                    continue  # already consumed above
                                _canon_meta.setdefault(f"materialization_{_mk}", _mv)
                        _update_meta(queue_local_order_id, _canon_meta)
            except Exception as _canon_exc:
                try:
                    log.debug(
                        "materialization outcome meta persist failed (non-fatal): %s",
                        _canon_exc,
                    )
                except Exception:
                    pass

        # 3) Recover the already-approved queue/OSM plan.
        approved_plan = self._recover_plan_for_revalidation(watched)
        if approved_plan is None:
            log.critical("[%s] PRODUCTION_ENTRY_BLOCK — approved plan missing after breach revalidation", ticker)
            # NOTE: _deferred is not yet known here, and an invalid/missing plan
            # is not a deferred-selection outcome — do not emit a deferred
            # outcome. _terminalize_breach_failure records this terminal state.
            _terminalize_breach_failure("approved_plan_missing_after_revalidation")
            return

        _hydration_bridge_applied = self._refresh_hydrated_prebreach_plan(
            approved_plan=approved_plan,
            sig=sig,
            local_order_id=queue_local_order_id,
            ticker=ticker,
        )
        _hydrated_master_control = getattr(self, "master_control", None)
        if _hydration_bridge_applied:
            if _hydrated_master_control is None:
                reason = "hydrated_prebreach_revalidation_unavailable"
                log.critical(
                    "[%s] PRODUCTION_ENTRY_BLOCK — hydration applied but master_control missing; "
                    "cannot revalidate hydrated contract/qty/limit safely",
                    ticker,
                )
                _terminalize_breach_failure(
                    reason,
                    cleanup_action="expire",
                    meta_patch={
                        "hydration_bridge_applied": True,
                        "hydrated_prebreach_revalidation": "unavailable",
                        "local_order_id": queue_local_order_id,
                    },
                    context_notes=reason,
                )
                return
            try:
                _hydrated_reval = _hydrated_master_control.revalidate_exposure(
                    approved_plan,
                    client_id=self.email or "default",
                )
            except Exception as exc:
                reason = f"hydrated_prebreach_revalidation_error:{exc}"
                log.critical(
                    "[%s] PRODUCTION_ENTRY_BLOCK — hydrated pre-breach revalidation errored: %s",
                    ticker,
                    exc,
                )
                _terminalize_breach_failure(
                    reason,
                    cleanup_action="expire",
                    meta_patch={
                        "hydration_bridge_applied": True,
                        "hydrated_prebreach_revalidation": "error",
                        "local_order_id": queue_local_order_id,
                    },
                    context_notes=reason,
                )
                return
            if not getattr(_hydrated_reval, "ok", False):
                _reason = getattr(_hydrated_reval, "reason", "revalidation_failed")
                reason = f"hydrated_prebreach_revalidation_blocked:{_reason}"
                log.critical(
                    "[%s] PRODUCTION_ENTRY_BLOCK — hydrated pre-breach revalidation blocked: %s",
                    ticker,
                    _reason,
                )
                _terminalize_breach_failure(
                    reason,
                    cleanup_action="expire",
                    meta_patch={
                        "hydration_bridge_applied": True,
                        "hydrated_prebreach_revalidation": "blocked",
                        "hydrated_prebreach_revalidation_reason": str(_reason),
                        "local_order_id": queue_local_order_id,
                    },
                    context_notes=reason,
                )
                return

        # 3b) Breach-time contract selection for overnight deferred signals.
        # Pre-market option chains have zero bids — overnight_reeval cannot select
        # contracts before 9:30 AM ET. Those signals are armed with
        # contract_deferred=True. Here at breach (market open, live quotes) we
        # select the contract before the limit_price and contract_symbol checks.
        _sig_meta   = getattr(approved_plan, "metadata", {}) or {}
        _sig_dict   = sig or {}
        _candidate_audit = None  # Item 3 — set if breach-time selection runs
        _contract_sym_raw = str(getattr(approved_plan, "contract_symbol", "") or "").strip()
        _deferred   = (
            bool(_sig_meta.get("contract_deferred"))
            or bool(_sig_dict.get("contract_deferred"))
            or not _contract_sym_raw
            or _contract_sym_raw.upper().startswith("DEFERRED:")  # safety: never submit placeholder
        )
        # Enable deferred-outcome emission only for deferred triggers (amendment:
        # guard deferred logs with _deferred). Non-deferred entries never emit a
        # deferred terminal outcome.
        _deferred_outcome["is_deferred"] = bool(_deferred)

        # ── P0 amendment #3 (PR #294): deferred materialization handoff proof ──
        # A single snapshot dict that captures the three staged views of the
        # deferred materialization pipeline:
        #     stage 1: what the selector returned  (populated at copy-back)
        #     stage 2: what got copied into the plan (populated at copy-back)
        #     stage 3: what the pre-submit invariant sees (compared just
        #              before broker POST)
        # If stage 1 succeeded with a real OCC contract, stages 2 and 3 must
        # agree. Any disagreement blocks the submit and terminalizes with
        # MATERIALIZATION_COPYBACK_MISMATCH. Non-deferred entries never
        # populate this snapshot; the handoff proof is a no-op for them.
        _handoff_snapshot: dict = {
            "captured":               False,
            "selector_contract":      None,
            "selector_bid":           None,
            "selector_ask":           None,
            "selector_mid":           None,
            "selector_premium":       None,
            "selector_qty":           None,
            "copied_plan_contract":   None,
            "copied_plan_limit":      None,
            "copied_plan_qty":        None,
            "copied_plan_max_usd":    None,
            # P0 amendment #4+#5+#6 (PR #294): third view — the persisted
            # `orders` row's contract at pre-submit time. Populated inside the
            # pre-submit block through the fail-closed handoff row reader. When
            # selector success captured a real OCC contract, an unreadable row
            # terminalizes before broker POST; `None` only remains for proof
            # not-applicable paths. Any non-None value is authoritative and
            # participates in the mismatch decision.
            "order_row_contract":     None,
        }
        if _deferred:
            if self.contract_selector is None:
                _reason = "contract_deferred_no_selector"
                log.critical(
                    "[%s] DEFERRED_BREACH_CONTRACT_FAILED — %s",
                    ticker, _reason,
                )
                _emit_deferred_outcome(
                    "BREACH_SELECTOR_RETURNED_NONE",
                    reason=_reason,
                )
                _terminalize_deferred_breach_failure(
                    _reason,
                    extra_meta={"failure_stage": "deferred_contract_selection"},
                )
                log.critical(
                    "[%s] PRODUCTION_ENTRY_BLOCK — contract_deferred=True but no "
                    "contract_selector wired into execution core",
                    ticker,
                )
                return
            try:
                log.info(
                    "[%s] Overnight deferred signal — selecting contract at breach "
                    "with live quotes (trigger=%.4f side=%s)",
                    ticker,
                    float(getattr(approved_plan, "trigger_price", 0) or 0),
                    getattr(approved_plan, "side", "?"),
                )
                # PR1 #166 amendment: explicitly mark this as deferred breach-time
                # selection so the DTE ladder (when DEFERRED_DTE_LADDER=1) applies
                # ONLY here, never to normal non-deferred selector calls. The
                # selector reads plan.metadata["deferred_breach_selection"].
                try:
                    _ap_meta = getattr(approved_plan, "metadata", None)
                    if isinstance(_ap_meta, dict):
                        _ap_meta["deferred_breach_selection"] = True
                        _ap_meta["selection_context"] = "deferred_breach"
                    elif isinstance(approved_plan, dict):
                        approved_plan.setdefault("metadata", {})
                        approved_plan["metadata"]["deferred_breach_selection"] = True
                        approved_plan["metadata"]["selection_context"] = "deferred_breach"
                    else:
                        # object plan with no metadata dict — attach one
                        try:
                            setattr(approved_plan, "metadata", {
                                "deferred_breach_selection": True,
                                "selection_context": "deferred_breach",
                            })
                        except Exception:
                            pass
                except Exception:
                    pass
                _sel = self.contract_selector.select(approved_plan)
                _sel_contract = str(getattr(_sel, "contract_symbol", "") or "").strip()
                _live_contract = str(getattr(approved_plan, "contract_symbol", "") or "").strip()
                _plan_is_placeholder = (not _live_contract) or _live_contract.upper().startswith("DEFERRED:")
                _sel_is_real = bool(_sel_contract) and not _sel_contract.upper().startswith("DEFERRED:")

                if _sel_is_real and _plan_is_placeholder:
                    try:
                        approved_plan.contract_symbol = _sel_contract
                        _sel_price = (
                            getattr(_sel, "execution_price_per_share", None)
                            or getattr(_sel, "ask", None)
                            or getattr(_sel, "mid", None)
                        )
                        if _sel_price:
                            approved_plan.limit_price = float(_sel_price)
                        _sel_qty = int(getattr(_sel, "affordable_contracts", 0) or 0)
                        if _sel_qty > 0:
                            approved_plan.contracts = _sel_qty
                            _prem_per_contract = float(getattr(_sel, "premium_per_contract", 0) or 0)
                            if _prem_per_contract > 0:
                                approved_plan.max_position_usd = _sel_qty * _prem_per_contract
                        _live_contract = str(getattr(approved_plan, "contract_symbol", "") or "").strip()
                        # P0 amendment #3 (PR #294): capture stages 1 & 2 of
                        # the handoff snapshot the moment copy-back completes.
                        # Stage 3 (pre-submit) is compared against these values
                        # inside the pre-submit invariant block, below. Only
                        # populated when the selector actually returned a real
                        # OCC contract — the check requirement.
                        try:
                            _handoff_snapshot["captured"]             = True
                            _handoff_snapshot["selector_contract"]    = _sel_contract or None
                            _handoff_snapshot["selector_bid"]         = float(getattr(_sel, "bid", 0) or 0)
                            _handoff_snapshot["selector_ask"]         = float(getattr(_sel, "ask", 0) or 0)
                            _handoff_snapshot["selector_mid"]         = float(getattr(_sel, "mid", 0) or 0)
                            _handoff_snapshot["selector_premium"]     = float(
                                getattr(_sel, "execution_price_per_share", 0)
                                or getattr(_sel, "ask", 0) or 0
                            )
                            _handoff_snapshot["selector_qty"]         = int(
                                getattr(_sel, "affordable_contracts", 0) or 0
                            )
                            _handoff_snapshot["copied_plan_contract"] = _live_contract or None
                            _handoff_snapshot["copied_plan_limit"]    = float(
                                getattr(approved_plan, "limit_price", 0) or 0
                            )
                            _handoff_snapshot["copied_plan_qty"]      = int(
                                getattr(approved_plan, "contracts", 0) or 0
                            )
                            _handoff_snapshot["copied_plan_max_usd"]  = float(
                                getattr(approved_plan, "max_position_usd", 0) or 0
                            )
                        except Exception as _snap_exc:
                            # Snapshot capture must never break the submit
                            # path. On unexpected shape we log and fall
                            # through; the pre-submit invariant + existing
                            # DEFERRED:*/limit guards remain the definitive
                            # safety net.
                            log.warning(
                                "[%s] handoff snapshot capture failed (non-fatal): %s",
                                ticker, _snap_exc,
                            )
                    except Exception as _copy_exc:
                        log.warning("[%s] deferred breach selected contract copy failed: %s", ticker, _copy_exc)

                # ── P0 (monday-trade-flow-readiness, amended): acceptance cap
                # at SELECTION time. _acceptance_ask_cap() is FAIL-CLOSED:
                # when DEFERRED_SMALL_ACCOUNT_FALLBACK=1 but the cap env is
                # missing/unparsable/<=0, the materialization BLOCKS here as
                # ACCEPTANCE_CAP_MISCONFIGURED — it never proceeds uncapped.
                # When the cap is valid: selected ask must be <= cap and
                # quantity clamps to exactly 1 (acceptance is flow-proof, not
                # P&L). A second enforcement of the SAME cap runs against the
                # final refreshed submit limit in the pre-submit invariant, so
                # selection-time pass + refresh-time drift over cap still
                # blocks before any broker POST. Feature off (flag unset)
                # → this block is a no-op, behavior byte-for-byte unchanged.
                _cap_enabled, _accept_cap, _cap_error = _acceptance_ask_cap()
                if _cap_enabled and _sel_is_real:
                    if _cap_error:
                        _cap_reason = f"ACCEPTANCE_CAP_MISCONFIGURED:{_cap_error}"
                        log.critical(
                            "[%s] DEFERRED_ACCEPTANCE_CAP_MISCONFIGURED %s — "
                            "fail-closed: blocking materialization, no broker POST",
                            ticker, _cap_error,
                        )
                        _emit_deferred_outcome(
                            "UNTRADEABLE_FOR_ACCOUNT_SIZE",
                            reason=_cap_reason,
                            contract=_sel_contract,
                        )
                        _terminalize_deferred_breach_failure(
                            _cap_reason,
                            extra_meta={
                                "failure_stage": "acceptance_ask_cap",
                                "acceptance_cap_error": _cap_error,
                                "selected_contract": _sel_contract,
                            },
                        )
                        return
                    _sel_ask = float(getattr(_sel, "ask", 0) or 0)
                    if _sel_ask <= 0 or _sel_ask > _accept_cap:
                        _cap_reason = (
                            f"acceptance_ask_cap:ask_{_sel_ask:.2f}_"
                            f"cap_{_accept_cap:.2f}"
                        )
                        log.warning(
                            "[%s] DEFERRED_ACCEPTANCE_CAP_BLOCK contract=%s "
                            "ask=%.2f cap=%.2f — terminalizing, no retry",
                            ticker, _sel_contract, _sel_ask, _accept_cap,
                        )
                        _emit_deferred_outcome(
                            "UNTRADEABLE_FOR_ACCOUNT_SIZE",
                            reason=_cap_reason,
                            contract=_sel_contract,
                        )
                        _terminalize_deferred_breach_failure(
                            _cap_reason,
                            extra_meta={
                                "failure_stage": "acceptance_ask_cap",
                                "selected_contract": _sel_contract,
                                "selected_ask": _sel_ask,
                                "acceptance_cap": _accept_cap,
                            },
                        )
                        return
                    # Cap passed — acceptance mode trades exactly one
                    # contract at the selected premium.
                    try:
                        approved_plan.contracts = 1
                        _prem = float(getattr(_sel, "premium_per_contract", 0) or 0)
                        if _prem > 0:
                            approved_plan.max_position_usd = _prem
                        log.info(
                            "[%s] DEFERRED_ACCEPTANCE_CAP_PASS contract=%s "
                            "ask=%.2f cap=%.2f qty=1",
                            ticker, _sel_contract, _sel_ask, _accept_cap,
                        )
                    except Exception as _cap_exc:
                        log.warning(
                            "[%s] acceptance qty clamp failed (non-fatal): %s",
                            ticker, _cap_exc,
                        )

                # ── P0 (hotfix/deferred-breach-selector-reasons, amended): ────
                # Shared helper — builds the selector audit dict and extracts
                # the canonical reason_code/stage from get_last_failure() for
                # use by BOTH deferred-breach failure paths below.
                # get_last_failure() is observability-only and never raises.
                # After a failed select() it holds the last REJECT emitted.
                # After a successful select() that failed to copy (unresolved
                # DEFERRED: placeholder) it is None — the caller must supply
                # an override_reason_code in that case.
                def _build_deferred_selector_audit(
                    *,
                    override_reason_code: str | None = None,
                    override_stage: str | None = None,
                ) -> tuple[str, dict]:
                    """
                    Returns (last_error_string, deferred_selector_audit_dict).
                    Reads selector.get_last_failure() and merges with any
                    caller-supplied override values.
                    override_reason_code is used when the selector succeeded
                    but post-selection validation failed (DEFERRED unresolved).
                    """
                    _sf = None
                    _rc = None
                    _st = None
                    _ex = None
                    try:
                        if hasattr(self.contract_selector, "get_last_failure"):
                            _sf = self.contract_selector.get_last_failure()
                        if isinstance(_sf, dict):
                            _rc = _sf.get("reason_code") or None
                            _st = _sf.get("stage") or None
                            _ex = _sf.get("explanation") or None
                    except Exception as _gf_exc:
                        log.debug(
                            "[%s] get_last_failure() read failed (non-fatal): %s",
                            ticker, _gf_exc,
                        )
                    # Override takes precedence when the selector itself succeeded
                    # (no REJECT emitted) but downstream validation failed.
                    if override_reason_code:
                        _rc = override_reason_code
                    if override_stage:
                        _st = override_stage

                    _error = (
                        f"breach_time_contract_selection:{_rc}"
                        if _rc
                        else "breach_time_contract_selection_no_result"
                    )
                    import datetime as _dt
                    _audit: dict = {
                        "reason_code":         _rc,
                        "stage":               _st,
                        "explanation":         _ex,
                        "budget":              float(getattr(approved_plan, "max_position_usd", 0) or 0),
                        "ticker":              ticker,
                        "side":                str(getattr(approved_plan, "side", "") or ""),
                        "execution_mode":      str(getattr(approved_plan, "execution_mode", "") or ""),
                        "contract_before":     _contract_sym_raw or None,
                        "selected_contract":   _sel_contract or None,
                        "timestamp":           _dt.datetime.now(_dt.timezone.utc).isoformat(),
                        "raw_selector_reason": (
                            _sf.get("raw_reason") if isinstance(_sf, dict) else None
                        ),
                    }
                    # P0 (2026-07-02): config provability. Production ran for
                    # multiple sessions with the DTE ladder silently disabled
                    # (env pin overriding the PR #219 default flip) and there
                    # was NO durable record of the effective flag — zero rows
                    # in `orders` ever carried last_dte_ladder_audit, and the
                    # only way to distinguish "ladder off" from "ladder ran
                    # and lost" was reading Render env by hand. Every deferred
                    # selector audit now records the effective flag and
                    # whether the plan carried the eligibility marker, so
                    # "was the ladder even on?" is answerable from Supabase
                    # alone. Best-effort; never blocks the audit write.
                    try:
                        _audit["dte_ladder_enabled"] = bool(
                            getattr(self.contract_selector, "dte_ladder_enabled", False)
                        )
                        _plan_meta_for_audit = getattr(approved_plan, "metadata", None)
                        _audit["ladder_eligible_marker"] = bool(
                            isinstance(_plan_meta_for_audit, dict)
                            and _plan_meta_for_audit.get("deferred_breach_selection") is True
                        )
                    except Exception as _cfg_exc:
                        log.debug(
                            "[%s] ladder config provability read failed (non-fatal): %s",
                            ticker, _cfg_exc,
                        )
                    # AMENDMENT (PR #219, Jason LIVE recovery): propagate the
                    # last DTE ladder run into order.meta so operator dashboards
                    # can see which expirations were probed and why each bucket
                    # failed. Without this, ladder observability stayed inside
                    # the selector and never made it into Supabase — meaning
                    # post-mortems on Jason's 54 EXPIRED rows had no visibility
                    # into which expirations were tried. Best-effort read.
                    try:
                        if hasattr(self.contract_selector, "get_last_dte_ladder_audit"):
                            _ladder_audit = self.contract_selector.get_last_dte_ladder_audit()
                            if isinstance(_ladder_audit, dict) and _ladder_audit:
                                _audit["last_dte_ladder_audit"] = _ladder_audit
                                # Surface commonly-queried ladder fields at the
                                # top level of the audit so dashboards can query
                                # them without traversing the nested audit dict.
                                _audit["ladder_selected_bucket"]     = _ladder_audit.get("selected_bucket")
                                _audit["ladder_selected_expiration"] = _ladder_audit.get("selected_expiration")
                                _audit["ladder_selected_dte"]        = _ladder_audit.get("selected_dte")
                                _audit["ladder_buckets_attempted"]   = _ladder_audit.get("buckets_attempted")
                                _audit["ladder_bucket_order"]        = _ladder_audit.get("bucket_order")
                    except Exception as _ladder_exc:
                        log.debug(
                            "[%s] get_last_dte_ladder_audit() read failed (non-fatal): %s",
                            ticker, _ladder_exc,
                        )
                    return _error, _audit

                # Path A: selector returned None OR live contract is still empty
                # after copy-back. The selector's REJECT is the blocker.
                if not _sel or not _live_contract:
                    _reason, _deferred_selector_audit = _build_deferred_selector_audit()

                    # ── P0: Retryable breach-selector — do NOT terminalize on transient ──
                    # If the failure reason is a temporary market-data miss (chain not
                    # yet warmed up, Tradier 5xx, empty expirations at 9:30–9:36 ET),
                    # rearm the trigger instead of expiring the row permanently.
                    # Safety contract: no broker submit on retry path, max retries and
                    # entry-cutoff both enforced. Quality rejects (spread, OI, delta,
                    # earnings, account size) still terminalize immediately — they are
                    # NOT in RETRYABLE_BREACH_SELECTOR_REASONS.
                    _obs_rc_a = str(
                        _deferred_selector_audit.get("reason_code")
                        or "BREACH_SELECTOR_RETURNED_NONE"
                    )
                    # PR #219 Fix C: NO_VALID_PLAYBOOK_DTE_CONTRACT is the DTE
                    # ladder aggregation code. Retrying makes sense only when
                    # the per-bucket failures were all data-miss signals. Inspect
                    # the ladder audit to decide, never treat it as unconditionally
                    # retryable.
                    _obs_rc_is_ladder_exhaustion = (
                        _obs_rc_a == "NO_VALID_PLAYBOOK_DTE_CONTRACT"
                    )
                    _ladder_audit_for_retry = (
                        _deferred_selector_audit.get("last_dte_ladder_audit") or {}
                    )
                    _ladder_exhaustion_is_retryable = (
                        _is_ladder_exhaustion_retryable(_ladder_audit_for_retry)
                        if _obs_rc_is_ladder_exhaustion
                        else False
                    )
                    _prior_attempt_a = 0
                    try:
                        _prior_attempt_a = int(
                            (getattr(approved_plan, "metadata", None) or {}).get(
                                "breach_attempt_count", 0
                            ) or 0
                        )
                    except (TypeError, ValueError):
                        _prior_attempt_a = 0
                    _this_attempt_a = _prior_attempt_a + 1
                    # AMENDMENT (PR #219, Jason LIVE recovery): default is now
                    # "1" (ON). Retry only applies to deferred breach selection
                    # AND is further gated by RETRYABLE_BREACH_SELECTOR_REASONS,
                    # queue_local_order_id presence, attempt count, and the
                    # BREACH_SELECTOR_RETRY_CUTOFF_ET wall-clock cap — so this
                    # cannot cause runaway retries for non-deferred flows or
                    # for structural rejections. The env var is preserved as
                    # an emergency kill switch (set to "0" to disable without
                    # a code deploy).
                    _retry_enabled_a    = str(os.getenv("BREACH_SELECTOR_RETRY_ENABLED", "1")).strip().lower() in ("1", "true", "yes")
                    _MAX_RETRIES_A      = int(os.getenv("MAX_BREACH_SELECTOR_RETRIES", "3"))
                    _RETRY_DELAY_A      = int(os.getenv("BREACH_SELECTOR_RETRY_DELAY_SECONDS", "20"))
                    # P0 (2026-07-02): cutoff default moved 945 → 1530. See
                    # _breach_retry_cutoff_hhmm() for the full forensic note.
                    _RETRY_CUTOFF_A     = _breach_retry_cutoff_hhmm()
                    _now_et_a           = datetime.now(ET)
                    _now_hhmm_a         = _now_et_a.hour * 100 + _now_et_a.minute
                    _past_cutoff_a      = _now_hhmm_a >= _RETRY_CUTOFF_A
                    _decision_a = _classify_deferred_breach_retry_decision(
                        _obs_rc_a,
                        queue_local_order_id=str(queue_local_order_id or ""),
                        attempt=_this_attempt_a,
                        max_attempts=_MAX_RETRIES_A,
                        past_cutoff=_past_cutoff_a,
                        retry_enabled=_retry_enabled_a,
                        ladder_retryable=_ladder_exhaustion_is_retryable,
                    )

                    if _decision_a["action"] == "retry_schedule":
                        log.warning(
                            "[%s] DEFERRED_BREACH_SELECTOR_RETRYABLE "
                            "attempt=%d/%d reason=%s delay=%ds cutoff=%d now=%d — rearming",
                            ticker, _this_attempt_a, _MAX_RETRIES_A,
                            _obs_rc_a, _RETRY_DELAY_A, _RETRY_CUTOFF_A, _now_hhmm_a,
                        )
                        # Update attempt count — do NOT expire/cancel the order
                        try:
                            _ap_meta_a = getattr(approved_plan, "metadata", None)
                            if isinstance(_ap_meta_a, dict):
                                _ap_meta_a["breach_attempt_count"] = _this_attempt_a
                        except Exception:
                            pass
                        try:
                            _upd_retry_a = getattr(self.order_state_machine, "update_order_meta", None)
                            if callable(_upd_retry_a) and queue_local_order_id:
                                _upd_retry_a(queue_local_order_id, {
                                    "breach_attempt_count":            _this_attempt_a,
                                    "last_breach_failure_reason":      str(_reason or ""),
                                    "last_breach_failure_reason_code": str(_obs_rc_a),
                                    "last_breach_failure_at":          datetime.now(timezone.utc).isoformat(),
                                    **_build_deferred_retry_schedule_meta(
                                        reason_code=_obs_rc_a,
                                        selector_audit=_deferred_selector_audit or {},
                                        attempt=_this_attempt_a,
                                        max_attempts=_MAX_RETRIES_A,
                                        delay_seconds=_RETRY_DELAY_A,
                                        client_id=_breach_client_id,
                                        execution_mode=str(getattr(approved_plan, "execution_mode", "") or ""),
                                        local_order_id=str(queue_local_order_id or ""),
                                        signal_id=str(getattr(approved_plan, "signal_id", "") or ""),
                                    ),
                                })
                        except Exception as _retry_meta_exc_a:
                            log.debug("[%s] retry meta update non-critical: %s", ticker, _retry_meta_exc_a)
                        try:
                            from ap.queue import write_deferred_breach_last_error
                            _queue_id_retry_a = (
                                sig.get("queue_id")
                                or sig.get("trade_queue_id")
                                or (getattr(approved_plan, "metadata", None) or {}).get("queue_id")
                            )
                            write_deferred_breach_last_error(
                                _queue_id_retry_a,
                                reason_code=f"CONTRACT_SELECTION_RETRY:{_obs_rc_a}",
                                explanation=(
                                    f"retry_scheduled attempt={_this_attempt_a}/{_MAX_RETRIES_A} "
                                    f"delay={_RETRY_DELAY_A}s reason={_reason}"
                                )[:400],
                                attempt=_this_attempt_a,
                                client_id=_breach_client_id,
                                ticker=ticker,
                            )
                        except Exception as _retry_obs_exc_a:
                            log.debug("[%s] retry queue write non-critical: %s", ticker, _retry_obs_exc_a)
                        # Rearm: fire _on_entry_trigger again after delay.
                        # `watched` is still valid — the price already breached.
                        import threading as _threading_retry
                        _retry_key_a = (
                            str(queue_local_order_id or ""),
                            int(_this_attempt_a),
                        )
                        _retry_inflight_a = getattr(self, "_deferred_breach_retry_inflight", None)
                        if not isinstance(_retry_inflight_a, set):
                            _retry_inflight_a = set()
                            setattr(self, "_deferred_breach_retry_inflight", _retry_inflight_a)
                        if _retry_key_a in _retry_inflight_a:
                            log.warning(
                                "[%s] DEFERRED_BREACH_RETRY_DUPLICATE_SUPPRESSED order=%s attempt=%d",
                                ticker, queue_local_order_id, _this_attempt_a,
                            )
                            return
                        _retry_inflight_a.add(_retry_key_a)
                        _watched_ref = watched
                        def _retry_deferred_breach_a(_w=_watched_ref, _t=ticker,
                                                      _att=_this_attempt_a, _d=_RETRY_DELAY_A,
                                                      _retry_key=_retry_key_a,
                                                      _oid=queue_local_order_id):
                            try:
                                time.sleep(_d)
                                # PR #219 Fix B: stale-state guard.
                                # Between the first _on_entry_trigger call and now, the
                                # pending entry row may have been terminalized externally:
                                # - order_monitor EOD cleanup / cancel
                                # - watcher expiry from a separate watcher poll
                                # - a concurrent retry thread that already succeeded
                                # - manual operator DB update
                                # Re-entering _on_entry_trigger on a non-PENDING_TRIGGER
                                # row causes duplicate OSM transitions, phantom broker
                                # submits, or worst-case a double-fill on Jason's LIVE
                                # account. Abort if the order is no longer in a
                                # retry-eligible state.
                                if _oid:
                                    try:
                                        _stale_check_osm = getattr(self, "order_state_machine", None)
                                        _stale_check_fn = getattr(_stale_check_osm, "get_order", None)
                                        if callable(_stale_check_fn):
                                            _stale_row = _stale_check_fn(_oid)
                                            if isinstance(_stale_row, dict):
                                                _current_status = str(
                                                    _stale_row.get("status") or ""
                                                ).upper()
                                                _current_broker_id = str(
                                                    _stale_row.get("broker_order_id") or ""
                                                ).strip()
                                                _current_submitted_ts = _stale_row.get("submitted_ts")
                                                _current_contract = str(
                                                    _stale_row.get("contract") or ""
                                                )
                                                _current_meta = _stale_row.get("meta") or {}
                                                try:
                                                    _current_attempt = int(
                                                        _current_meta.get("breach_attempt_count") or 0
                                                    )
                                                except Exception:
                                                    _current_attempt = 0
                                                _is_still_deferred = (
                                                    _current_contract.startswith("DEFERRED:")
                                                    or bool(_current_meta.get("contract_deferred"))
                                                    or bool(_current_meta.get("deferred_breach_selection"))
                                                )

                                                def _record_stale_abort(_reason_code: str) -> None:
                                                    try:
                                                        _upd_abort = getattr(self.order_state_machine, "update_order_meta", None)
                                                        if callable(_upd_abort):
                                                            _upd_abort(_oid, _build_deferred_retry_stale_abort_meta(
                                                                current_status=_current_status,
                                                                broker_order_id=_current_broker_id,
                                                                submitted_ts=_current_submitted_ts,
                                                                current_contract=_current_contract,
                                                                current_attempt=_current_attempt,
                                                                thread_attempt=int(_att),
                                                                selector_audit=_current_meta.get("last_breach_selector_audit") or {},
                                                                client_id=_breach_client_id,
                                                                execution_mode=str(_current_meta.get("execution_mode") or ""),
                                                                local_order_id=str(_oid or ""),
                                                                signal_id=str(_current_meta.get("signal_id") or signal_id or ""),
                                                                reason=_reason_code,
                                                            ))
                                                    except Exception:
                                                        pass

                                                # Guard 1: status must still be retryable
                                                if _current_status not in {"PENDING_TRIGGER", "CREATED"}:
                                                    _record_stale_abort("status_changed")
                                                    log.warning(
                                                        "[%s] DEFERRED_BREACH_RETRY_STALE_STATE_ABORT "
                                                        "order=%s reason=status_changed "
                                                        "current_status=%s attempt=%d",
                                                        _t, _oid, _current_status, _att,
                                                    )
                                                    return
                                                # Guard 2: must not already have a broker order
                                                if _current_broker_id:
                                                    _record_stale_abort("broker_id_present")
                                                    log.warning(
                                                        "[%s] DEFERRED_BREACH_RETRY_STALE_STATE_ABORT "
                                                        "order=%s reason=broker_id_present "
                                                        "broker_order_id=%s attempt=%d",
                                                        _t, _oid, _current_broker_id, _att,
                                                    )
                                                    return
                                                # Guard 3: must not have been submitted already
                                                if _current_submitted_ts:
                                                    _record_stale_abort("submitted_ts_present")
                                                    log.warning(
                                                        "[%s] DEFERRED_BREACH_RETRY_STALE_STATE_ABORT "
                                                        "order=%s reason=submitted_ts_present "
                                                        "submitted_ts=%s attempt=%d",
                                                        _t, _oid, _current_submitted_ts, _att,
                                                    )
                                                    return
                                                # Guard 4: contract must still be deferred
                                                if not _is_still_deferred:
                                                    _record_stale_abort("no_longer_deferred")
                                                    log.warning(
                                                        "[%s] DEFERRED_BREACH_RETRY_STALE_STATE_ABORT "
                                                        "order=%s reason=no_longer_deferred "
                                                        "contract=%s attempt=%d",
                                                        _t, _oid, _current_contract, _att,
                                                    )
                                                    return
                                                # Guard 5: attempt counter must be consistent.
                                                # If meta.breach_attempt_count > _att it means
                                                # another thread already incremented the counter
                                                # beyond this thread's slot — abort to avoid
                                                # duplicate entry attempts.
                                                if _current_attempt > int(_att):
                                                    _record_stale_abort("attempt_count_advanced")
                                                    log.warning(
                                                        "[%s] DEFERRED_BREACH_RETRY_STALE_STATE_ABORT "
                                                        "order=%s reason=attempt_count_advanced "
                                                        "db_attempt=%d thread_attempt=%d",
                                                        _t, _oid, _current_attempt, _att,
                                                    )
                                                    return
                                    except Exception as _stale_exc:
                                        # Stale check is best-effort — if it fails, proceed
                                        # with the retry rather than silently dropping it.
                                        # The inflight-key dedup set still prevents true
                                        # duplicates within the same process.
                                        log.debug(
                                            "[%s] DEFERRED_BREACH_RETRY stale-state check failed "
                                            "(non-fatal, proceeding): %s",
                                            _t, _stale_exc,
                                        )
                                log.info(
                                    "[%s] DEFERRED_BREACH_RETRY firing attempt=%d",
                                    _t, _att,
                                )
                                self._on_entry_trigger(_w)
                            except Exception as _retry_exc_a:
                                log.error(
                                    "[%s] DEFERRED_BREACH_RETRY thread error attempt=%d: %s",
                                    _t, _att, _retry_exc_a,
                                )
                            finally:
                                try:
                                    getattr(self, "_deferred_breach_retry_inflight", set()).discard(_retry_key)
                                except Exception:
                                    pass
                        _rt_a = _threading_retry.Thread(
                            target=_retry_deferred_breach_a,
                            daemon=True,
                            name=f"deferred_retry_{ticker}_{_this_attempt_a}",
                        )
                        _rt_a.start()
                        return  # do NOT terminalize — retry thread owns the outcome

                    # Not retryable (quality reject, max retries exceeded, or past cutoff).
                    # Classify into specific dashboard taxonomy before terminalizing.
                    _cs_status_a = (
                        "CONTRACT_SELECTION_DATA_ERROR"
                        if _decision_a["retryable_reason"]
                        else "CONTRACT_SELECTION_QUALITY_REJECT"
                    )

                    log.critical(
                        "DEFERRED_BREACH_CONTRACT_SELECTION_FAILED "
                        "client=%s symbol=%s side=%s execution_mode=%s "
                        "budget=%.2f reason=%s stage=%s order_id=%s signal_id=%s "
                        "cs_status=%s attempt=%d/%d",
                        _breach_client_id,
                        ticker,
                        str(getattr(approved_plan, "side", "") or ""),
                        str(getattr(approved_plan, "execution_mode", "") or ""),
                        float(getattr(approved_plan, "max_position_usd", 0) or 0),
                        _reason,
                        _deferred_selector_audit.get("stage") or "unknown",
                        queue_local_order_id or "",
                        str(getattr(approved_plan, "signal_id", "") or ""),
                        _cs_status_a,
                        _this_attempt_a,
                        _MAX_RETRIES_A,
                    )
                    log.critical(
                        "BREACH_TIME_CONTRACT_SELECTION_FAILED "
                        "client=%s ticker=%s reason=%s",
                        _breach_client_id, ticker, _reason,
                    )
                    _final_reason_a = str(_decision_a.get("terminal_reason") or _reason)
                    _emit_deferred_outcome(
                        (
                            "DATA_MISSING_OI_VOLUME"
                            if "vol0_oi0" in str(_reason)
                            else "BREACH_SELECTOR_RETURNED_NONE"
                        ),
                        reason=_final_reason_a,
                        extra={"stage": _deferred_selector_audit.get("stage") or "unknown"},
                    )
                    # ── PR #182 + P0: write selector failure to trade_queue.last_error ──
                    try:
                        from ap.queue import write_deferred_breach_last_error
                        _queue_id_for_obs = (
                            sig.get("queue_id")
                            or sig.get("trade_queue_id")
                            or (getattr(approved_plan, "metadata", None) or {}).get("queue_id")
                        )
                        write_deferred_breach_last_error(
                            _queue_id_for_obs,
                            reason_code=(
                                _decision_a.get("terminal_reason")
                                or str(_obs_rc_a)
                            ),
                            explanation=(
                                _decision_a.get("terminal_reason")
                                or str(_reason or "")
                            )[:400],
                            attempt=_this_attempt_a,
                            client_id=_breach_client_id,
                            ticker=ticker,
                        )
                        try:
                            _upd_a = getattr(self.order_state_machine, "update_order_meta", None)
                            if callable(_upd_a) and queue_local_order_id:
                                _upd_a(queue_local_order_id, {
                                    "breach_attempt_count":            _this_attempt_a,
                                    "last_breach_failure_reason":      str(
                                        _decision_a.get("terminal_reason") or _reason or ""
                                    ),
                                    "last_breach_failure_reason_code": str(_obs_rc_a),
                                    "last_breach_failure_at":          datetime.now(timezone.utc).isoformat(),
                                    "contract_selection_status":       _cs_status_a,
                                    **(
                                        _build_deferred_retry_terminal_meta(
                                            terminal_reason=str(_decision_a.get("terminal_reason") or ""),
                                            reason_code=_obs_rc_a,
                                            selector_audit=_deferred_selector_audit or {},
                                            attempt=_this_attempt_a,
                                            max_attempts=_MAX_RETRIES_A,
                                            client_id=_breach_client_id,
                                            execution_mode=str(getattr(approved_plan, "execution_mode", "") or ""),
                                            local_order_id=str(queue_local_order_id or ""),
                                            signal_id=str(getattr(approved_plan, "signal_id", "") or ""),
                                        )
                                        if _decision_a["retryable_reason"]
                                        else {
                                            "last_breach_selector_audit": _deferred_selector_audit or {},
                                        }
                                    ),
                                })
                        except Exception as _ma_exc:
                            log.debug("[%s] PR182 meta update non-critical: %s", ticker, _ma_exc)
                    except Exception as _obs_a_exc:
                        log.debug("[%s] PR182 write-back non-critical: %s", ticker, _obs_a_exc)
                    # ── end PR #182 + P0 Path A ────────────────────────────────
                    # P0: Write durable materialization audit before terminalizing.
                    # Extracts chain/reject data from plan.metadata["selector_failure"]
                    # (written by _attach_selector_failure in contract_selector) and
                    # DTE ladder buckets from the selector audit.  Best-effort.
                    try:
                        _sel_failure_a = (
                            getattr(approved_plan, "metadata", {}) or {}
                        ).get("selector_failure") or {}
                        _ladder_buckets_a = (
                            _deferred_selector_audit.get("last_dte_ladder_audit") or {}
                        ).get("buckets_attempted")
                        _write_deferred_materialization_audit(
                            self.order_state_machine,
                            queue_local_order_id,
                            success=False,
                            attempt_ts=datetime.now(timezone.utc).isoformat(),
                            original_contract=_contract_sym_raw or f"DEFERRED:{ticker}",
                            symbol=ticker,
                            side=str(getattr(approved_plan, "side", "") or ""),
                            execution_mode=str(getattr(approved_plan, "execution_mode", "") or ""),
                            account_budget=float(_deferred_selector_audit.get("budget") or 0),
                            selector_status=_cs_status_a,
                            selected_contract=None,
                            selected_limit_price=None,
                            failure_reason=_final_reason_a,
                            stage=_deferred_selector_audit.get("stage"),
                            expirations_probed=_ladder_buckets_a,
                            chain_rows_total=int(_sel_failure_a.get("chain_rows") or 0),
                            survivor_count=int(_sel_failure_a.get("survivor_count") or 0),
                            top_reject_buckets=_sel_failure_a.get("top_reject_buckets") or {},
                        )
                    except Exception:
                        pass  # audit write is non-critical
                    _terminalize_deferred_breach_failure(
                        _final_reason_a,
                        extra_meta={
                            "failure_stage":           "deferred_contract_selection",
                            "selected_contract":       _sel_contract or None,
                            "deferred_selector_audit": _deferred_selector_audit,
                            "contract_selection_status": _cs_status_a,
                        },
                    )
                    log.critical(
                        "[%s] PRODUCTION_ENTRY_BLOCK — breach-time contract selection "
                        "returned no contract (reason=%s stage=%s budget=%.2f cs_status=%s)",
                        ticker, _reason,
                        _deferred_selector_audit.get("stage") or "unknown",
                        float(getattr(approved_plan, "max_position_usd", 0) or 0),
                        _cs_status_a,
                    )
                    return

                # Path B: selector returned a result but the plan copy-back failed
                # or the selector wrote a DEFERRED: placeholder — unresolved.
                # The selector itself did not emit a REJECT (it returned a value),
                # so get_last_failure() is None; supply override reason code.
                if _live_contract.upper().startswith("DEFERRED:"):
                    _reason, _deferred_selector_audit = _build_deferred_selector_audit(
                        override_reason_code="DEFERRED_UNRESOLVED_AT_BREACH",
                        override_stage="deferred_copy_back",
                    )
                    # Embed the unresolved placeholder in the reason string so
                    # the order row records which contract was stuck.
                    _reason = f"breach_time_contract_selection:DEFERRED_UNRESOLVED_AT_BREACH:{_live_contract}"
                    _deferred_selector_audit["unresolved_placeholder"] = _live_contract
                    log.critical(
                        "DEFERRED_BREACH_CONTRACT_SELECTION_FAILED "
                        "client=%s symbol=%s side=%s execution_mode=%s "
                        "budget=%.2f reason=%s stage=%s order_id=%s signal_id=%s",
                        _breach_client_id,
                        ticker,
                        str(getattr(approved_plan, "side", "") or ""),
                        str(getattr(approved_plan, "execution_mode", "") or ""),
                        float(getattr(approved_plan, "max_position_usd", 0) or 0),
                        _reason,
                        _deferred_selector_audit.get("stage") or "unknown",
                        queue_local_order_id or "",
                        str(getattr(approved_plan, "signal_id", "") or ""),
                    )
                    _emit_deferred_outcome(
                        "BREACH_SELECTOR_RETURNED_NONE",
                        reason=_reason,
                        contract=_live_contract,
                        extra={"stage": "deferred_copy_back"},
                    )
                    # ── PR #182: write selector failure reason to trade_queue.last_error ──
                    # Path B: selector returned a value but contract is still DEFERRED:
                    # (copy-back failed or returned unresolved placeholder). Write reason
                    # to trade_queue.last_error before terminal cleanup.
                    # Best-effort: failure here must never block the cleanup below.
                    try:
                        from ap.queue import write_deferred_breach_last_error
                        _queue_id_for_obs_b = (
                            sig.get("queue_id")
                            or sig.get("trade_queue_id")
                            or (getattr(approved_plan, "metadata", None) or {}).get("queue_id")
                        )
                        _prior_attempt_b = 0
                        try:
                            _prior_attempt_b = int(
                                (getattr(approved_plan, "metadata", None) or {}).get(
                                    "breach_attempt_count", 0
                                ) or 0
                            )
                        except (TypeError, ValueError):
                            _prior_attempt_b = 0
                        _this_attempt_b = _prior_attempt_b + 1
                        _obs_rc_b = (
                            _deferred_selector_audit.get("reason_code")
                            or "DEFERRED_UNRESOLVED_AT_BREACH"
                        )
                        write_deferred_breach_last_error(
                            _queue_id_for_obs_b,
                            reason_code=str(_obs_rc_b),
                            explanation=str(_reason or "")[:400],
                            attempt=_this_attempt_b,
                            client_id=_breach_client_id,
                            ticker=ticker,
                        )
                        try:
                            _upd_b = getattr(self.order_state_machine, "update_order_meta", None)
                            if callable(_upd_b) and queue_local_order_id:
                                _upd_b(queue_local_order_id, {
                                    "breach_attempt_count":           _this_attempt_b,
                                    "last_breach_failure_reason":     str(_reason or ""),
                                    "last_breach_failure_reason_code": str(_obs_rc_b),
                                    "last_breach_failure_at":         datetime.now(timezone.utc).isoformat(),
                                    "last_breach_selector_audit":     _deferred_selector_audit or {},
                                })
                        except Exception as _mb_exc:
                            log.debug("[%s] PR182 meta update non-critical: %s", ticker, _mb_exc)
                    except Exception as _obs_b_exc:
                        log.debug("[%s] PR182 write-back non-critical: %s", ticker, _obs_b_exc)
                    # ── end PR #182 Path B ─────────────────────────────────────
                    # P0: Write durable materialization audit before terminalizing (Path B).
                    try:
                        _sel_failure_b = (
                            getattr(approved_plan, "metadata", {}) or {}
                        ).get("selector_failure") or {}
                        _ladder_buckets_b = (
                            _deferred_selector_audit.get("last_dte_ladder_audit") or {}
                        ).get("buckets_attempted")
                        _write_deferred_materialization_audit(
                            self.order_state_machine,
                            queue_local_order_id,
                            success=False,
                            attempt_ts=datetime.now(timezone.utc).isoformat(),
                            original_contract=_contract_sym_raw or f"DEFERRED:{ticker}",
                            symbol=ticker,
                            side=str(getattr(approved_plan, "side", "") or ""),
                            execution_mode=str(getattr(approved_plan, "execution_mode", "") or ""),
                            account_budget=float(_deferred_selector_audit.get("budget") or 0),
                            selector_status="DEFERRED_UNRESOLVED_AT_BREACH",
                            selected_contract=None,
                            selected_limit_price=None,
                            failure_reason=_reason,
                            stage="deferred_copy_back",
                            expirations_probed=_ladder_buckets_b,
                            chain_rows_total=int(_sel_failure_b.get("chain_rows") or 0),
                            survivor_count=int(_sel_failure_b.get("survivor_count") or 0),
                            top_reject_buckets=_sel_failure_b.get("top_reject_buckets") or {},
                        )
                    except Exception:
                        pass  # audit write is non-critical
                    _terminalize_deferred_breach_failure(
                        _reason,
                        extra_meta={
                            "failure_stage":           "deferred_contract_selection",
                            "selected_contract":       _sel_contract or None,
                            "approved_contract":       _live_contract,
                            "deferred_selector_audit": _deferred_selector_audit,
                        },
                    )
                    log.critical(
                        "BREACH_TIME_CONTRACT_SELECTION_FAILED "
                        "client=%s ticker=%s reason=%s",
                        _breach_client_id, ticker, _reason,
                    )
                    log.critical(
                        "[%s] PRODUCTION_ENTRY_BLOCK — contract selector left placeholder "
                        "unresolved: %s (reason=%s)",
                        ticker, _live_contract, _reason,
                    )
                    return
                _emit_deferred_progress(
                    "BREACH_CONTRACT_SELECTED",
                    contract=_live_contract,
                    extra={
                        "limit_price": float(getattr(approved_plan, "limit_price", 0) or 0),
                        "qty": int(getattr(approved_plan, "contracts", 0) or 0),
                    },
                )
                # P0: Write durable materialization audit on success path.
                # Captures the real OCC symbol, executable limit, and selector
                # evidence so the row proves materialization happened correctly.
                try:
                    _cand_audit_for_meta = getattr(_sel, "candidate_audit", None) or {}
                    _write_deferred_materialization_audit(
                        self.order_state_machine,
                        queue_local_order_id,
                        success=True,
                        attempt_ts=datetime.now(timezone.utc).isoformat(),
                        original_contract=_contract_sym_raw or f"DEFERRED:{ticker}",
                        symbol=ticker,
                        side=str(getattr(approved_plan, "side", "") or ""),
                        execution_mode=str(getattr(approved_plan, "execution_mode", "") or ""),
                        account_budget=float(getattr(approved_plan, "max_position_usd", 0) or 0),
                        selector_status="CONTRACT_SELECTED",
                        selected_contract=_live_contract,
                        selected_limit_price=float(getattr(approved_plan, "limit_price", 0) or 0),
                        failure_reason=None,
                        stage="contract_selected",
                        expirations_probed=(
                            self.contract_selector.get_last_dte_ladder_audit() or {}
                        ).get("buckets_attempted") if hasattr(self.contract_selector, "get_last_dte_ladder_audit") else None,
                        chain_rows_total=int(_cand_audit_for_meta.get("candidates_considered") or 0),
                        survivor_count=int(_cand_audit_for_meta.get("candidates_considered") or 0),
                        top_reject_buckets=_cand_audit_for_meta.get("rejected_candidate_reasons") or {},
                    )
                except Exception:
                    pass  # audit write is non-critical
                log.info(
                    "[%s] DEFERRED_BREACH_CONTRACT_SELECTED — contract=%s limit=%.2f qty=%s",
                    ticker,
                    _live_contract,
                    float(getattr(approved_plan, "limit_price", 0) or 0),
                    int(getattr(approved_plan, "contracts", 0) or 0),
                )
                log.info(
                    "[%s] Breach-time contract selected: %s @ $%.2f x%s",
                    ticker, _live_contract,
                    float(getattr(approved_plan, "limit_price", 0) or 0),
                    int(getattr(approved_plan, "contracts", 1) or 1),
                )
                # Required structured log for ops confirmation that the existing
                # PENDING_TRIGGER row was finalized in-place (not a new order).
                log.info(
                    "BREACH_TIME_CONTRACT_FINALIZED "
                    "client=%s ticker=%s local_order_id=%s "
                    "old_contract=%s new_contract=%s limit=%.4f qty=%s",
                    _breach_client_id,
                    ticker,
                    queue_local_order_id or "",
                    _contract_sym_raw or "DEFERRED:?",
                    _live_contract,
                    float(getattr(approved_plan, "limit_price", 0) or 0),
                    int(getattr(approved_plan, "contracts", 1) or 1),
                )
                # Item 3 — capture the selector candidate audit (EVIDENCE ONLY).
                # Persisted into orders.meta after a successful submit below.
                try:
                    _candidate_audit = getattr(_sel, "candidate_audit", None)
                except Exception:
                    _candidate_audit = None
            except Exception as _cs_err:
                _reason = f"breach_time_contract_selection_error:{_cs_err}"
                log.critical(
                    "[%s] DEFERRED_BREACH_CONTRACT_FAILED — %s",
                    ticker, _reason,
                )
                _emit_deferred_outcome(
                    "BREACH_SELECTOR_EXCEPTION",
                    reason=_reason,
                    extra={"exception_type": type(_cs_err).__name__},
                )
                _terminalize_deferred_breach_failure(
                    _reason,
                    extra_meta={"failure_stage": "deferred_contract_selection"},
                )
                log.critical(
                    "[%s] PRODUCTION_ENTRY_BLOCK — breach-time contract selection "
                    "error: %s",
                    ticker, _cs_err,
                )
                return

        # 4) Require the approved plan to carry a valid limit price (used as
        #    the drift baseline — the actual submit limit is re-anchored to the
        #    current option ask in step 4b below).
        _plan_limit = getattr(approved_plan, "limit_price", None)
        try:
            _plan_limit = float(_plan_limit)
        except Exception:
            _plan_limit = 0.0

        if _plan_limit <= 0:
            log.critical("[%s] PRODUCTION_ENTRY_BLOCK — approved plan missing valid limit_price", ticker)
            _terminalize_breach_failure("approved_plan_missing_limit_price")
            return

        # Optional hard guards: require contract + nonzero qty from the approved plan.
        approved_contract = str(getattr(approved_plan, "contract_symbol", "") or "").strip()
        try:
            approved_qty = int(getattr(approved_plan, "contracts", 0) or 0)
        except Exception:
            approved_qty = 0

        if not approved_contract:
            log.critical("[%s] PRODUCTION_ENTRY_BLOCK — approved plan missing contract_symbol", ticker)
            _terminalize_breach_failure("approved_plan_missing_contract_symbol")
            return

        if approved_qty <= 0:
            log.critical("[%s] PRODUCTION_ENTRY_BLOCK — approved plan has invalid contracts=%s", ticker, approved_qty)
            _terminalize_breach_failure(f"approved_plan_invalid_contracts={approved_qty}")
            return

        # 4b) P0 FIX: refresh the option contract ask immediately before submit.
        #
        # ROOT CAUSE FIXED HERE: the prior code submitted at approved_plan.limit_price
        # which was set at contract-selection time. For overnight/queued signals that
        # can be hours old. The option ask has moved. Tradier receives a limit far below
        # the current ask. The order sits unfilled. This block brings the watcher-breach
        # path to parity with process_signal() in ap/execution.py which already does
        # this correctly via _refresh_ask_at_submit().
        try:
            from ap.execution import _refresh_ask_at_submit as _breach_refresh_ask
        except ImportError:
            _breach_refresh_ask = None

        _entry_pricing_decision = "ASK_CROSSED"
        _submit_ask = 0.0
        _refresh_ok = False
        _refresh_reason = "import_failed"
        _submit_quote_fields = {
            "submit_bid": None, "submit_ask": None,
            "submit_last": None, "submit_mid": None, "spread_pct": None,
        }

        if _breach_refresh_ask is not None:
            _submit_ask, _quote_age_ms, _refresh_ok, _refresh_reason, _submit_quote_fields =                 _breach_refresh_ask(self.broker, approved_contract)
        else:
            # _refresh_ask_at_submit unavailable — treat as refresh failure
            _refresh_ok = False
            _refresh_reason = "import_failed"
            _quote_age_ms = 0

        if not _refresh_ok or _submit_ask <= 0:
            # Fail closed: never submit at a stale plan price when we can't get
            # a fresh ask. The retry engine can re-arm after a delay.
            _entry_pricing_decision = "QUOTE_REFRESH_FAILED" if not _refresh_ok else "MISSING_ASK"
            log.critical(
                "[%s] ENTRY_PRICING_BLOCK — breach-time quote refresh failed "
                "contract=%s reason=%s refresh_ok=%s submit_ask=%s | blocking submit",
                ticker, approved_contract, _refresh_reason, _refresh_ok, _submit_ask,
            )
            _terminalize_breach_failure(f"breach_quote_refresh_failed:{_refresh_reason}")
            return

        # Spread sanity guard (wide spread = illiquid contract, skip).
        _spread_pct = _submit_quote_fields.get("spread_pct") or 0.0
        _max_spread = float(os.getenv("ENTRY_MAX_SPREAD_PCT", "0.50"))
        if _spread_pct > _max_spread:
            _entry_pricing_decision = "SPREAD_TOO_WIDE"
            log.warning(
                "[%s] ENTRY_PRICING_BLOCK — spread too wide at breach "
                "contract=%s spread_pct=%.3f max=%.3f | blocking submit",
                ticker, approved_contract, _spread_pct, _max_spread,
            )
            _terminalize_breach_failure(f"breach_spread_too_wide:{_spread_pct:.3f}")
            return

        # Drift guard: if the current ask has run more than ENTRY_MAX_PRICE_DRIFT_PCT_FROM_PLAN
        # above the original plan price, the move has already happened — don't chase.
        _drift_pct = (_submit_ask / _plan_limit) - 1.0 if _plan_limit > 0 else 0.0
        if _drift_pct > ENTRY_MAX_PRICE_DRIFT_PCT_FROM_PLAN:
            _entry_pricing_decision = "ENTRY_PRICE_DRIFT_TOO_HIGH"
            log.warning(
                "[%s] ENTRY_PRICING_BLOCK — ask drifted too far from plan at breach "
                "contract=%s plan_limit=%.2f submit_ask=%.2f drift=%.1f%% max=%.1f%% | blocking",
                ticker, approved_contract, _plan_limit, _submit_ask,
                _drift_pct * 100, ENTRY_MAX_PRICE_DRIFT_PCT_FROM_PLAN * 100,
            )
            _terminalize_breach_failure(
                (
                    f"breach_entry_price_drift_too_high:"
                    f"plan={_plan_limit:.2f} ask={_submit_ask:.2f} "
                    f"drift={_drift_pct*100:.1f}%"
                )
            )
            return

        # Compute the broker-submitted limit: ask + mode-appropriate crossing pennies.
        _ask_cross = ENTRY_PAPER_ASK_CROSS_CENTS if self.paper else ENTRY_LIVE_ASK_CROSS_CENTS
        submit_limit = round(_submit_ask + _ask_cross, 2)

        # ── PR #180: Jason live entry spread + ask-cross precision guard ──────
        # Final pre-submit precision check for named live clients only.
        # Paper and non-Jason live skip this entirely (helper returns no-op).
        _pr180_audit_extras: dict = {}
        if _pr180_is_named_live_client(self.client_id, self.paper):
            # PR #180 amendment — production-safe field resolution.
            # _submit_quote_fields may not have every field populated under
            # every refresh path; fall back to the local variables that the
            # ask-refresh produced, then derive a spread if necessary.
            # Order of resolution:
            #   bid: dict["submit_bid"] -> None (computed from ask/mid impossible)
            #   mid: dict["submit_mid"] -> None
            #   ask: dict["submit_ask"] -> local _submit_ask (always set by refresh)
            #   spread_pct: dict["spread_pct"] -> dict["spread_pct_at_submit"]
            #              -> compute from (ask - bid)/mid if bid/mid/ask all present
            _pr180_bid = _submit_quote_fields.get("submit_bid")
            _pr180_mid = _submit_quote_fields.get("submit_mid")
            _pr180_ask = (
                _submit_quote_fields.get("submit_ask")
                if _submit_quote_fields.get("submit_ask") is not None
                else (float(_submit_ask) if _submit_ask else None)
            )
            _pr180_spread = (
                _submit_quote_fields.get("spread_pct")
                if _submit_quote_fields.get("spread_pct") is not None
                else _submit_quote_fields.get("spread_pct_at_submit")
            )
            _pr180_spread_source = (
                "spread_pct"            if _submit_quote_fields.get("spread_pct") is not None
                else "spread_pct_at_submit" if _submit_quote_fields.get("spread_pct_at_submit") is not None
                else "missing"
            )
            # Computed-spread fallback — ONLY when all three of bid/mid/ask are
            # present (non-None and non-zero). Missing bid/mid/ask must still
            # fall through to ENTRY_QUOTE_INCOMPLETE_LIVE in the helper.
            if (
                _pr180_spread is None
                and _pr180_bid is not None and _pr180_bid > 0
                and _pr180_mid is not None and _pr180_mid > 0
                and _pr180_ask is not None and _pr180_ask > 0
            ):
                _pr180_spread = (float(_pr180_ask) - float(_pr180_bid)) / float(_pr180_mid)
                _pr180_spread_source = "computed_from_bid_mid_ask"

            _pr180_decision, _pr180_limit, _pr180_audit_extras = (
                _pr180_jason_live_entry_pricing_guard(
                    submit_bid     = _pr180_bid,
                    submit_mid     = _pr180_mid,
                    submit_ask     = _pr180_ask,
                    spread_pct     = _pr180_spread,
                    proposed_limit = submit_limit,
                )
            )
            # Mirror the resolved spread under both keys so dashboards and
            # downstream queries can find it regardless of which alias they
            # use. pr180_input_spread_pct is already set by the helper.
            _pr180_audit_extras["spread_pct_at_submit"]   = _pr180_spread
            _pr180_audit_extras["pr180_spread_source"]    = _pr180_spread_source
            # PR #180 amendment — persist rollout mode so dashboards can
            # correlate observed-vs-enforced incidents and measure baseline
            # impact before flipping enforcement on.
            _pr180_audit_extras["pr180_mode"]             = PR180_MODE
            _pr180_audit_extras["pr180_observe_reprice_enabled"] = (
                PR180_OBSERVE_REPRICE_ENABLED
            )
            # The helper writes pr180_decision on PROCEED / REPRICE_PROCEED
            # but not on BLOCK; mirror the decision string here so audit
            # always carries it, in either mode.
            _pr180_audit_extras.setdefault("pr180_decision", _pr180_decision)

            if PR180_MODE == "enforce":
                # ── ENFORCE MODE — current PR #180 behavior ─────────────────
                if _pr180_decision == "BLOCK":
                    _pr180_reason = _pr180_audit_extras.get("pr180_block_reason", "PR180_BLOCKED")
                    _entry_pricing_decision = _pr180_reason
                    _pr180_audit_extras["pr180_runtime_action"] = "TERMINALIZED"
                    log.critical(
                        "[%s] PR180_ENTRY_PRICING_BLOCK — %s | mode=enforce | "
                        "contract=%s bid=%s mid=%s ask=%s spread_pct=%s spread_source=%s "
                        "proposed_limit=%.2f client_id=%s",
                        ticker, _pr180_reason, approved_contract,
                        _pr180_bid, _pr180_mid, _pr180_ask, _pr180_spread,
                        _pr180_spread_source, submit_limit, self.client_id,
                    )
                    _terminalize_breach_failure(
                        f"pr180_block:{_pr180_reason.lower()}:"
                        f"spread={_pr180_spread!r}:"
                        f"limit={submit_limit:.2f}"
                    )
                    return
                if _pr180_decision == "REPRICE_PROCEED" and _pr180_limit is not None:
                    _pr180_audit_extras["pr180_runtime_action"] = "REPRICED"
                    log.info(
                        "[%s] PR180_ENTRY_REPRICED — controlled-limit band | mode=enforce | "
                        "contract=%s old_limit=%.2f new_limit=%.2f spread_pct=%.3f "
                        "spread_source=%s client_id=%s",
                        ticker, approved_contract, submit_limit, _pr180_limit,
                        float(_pr180_spread or 0.0), _pr180_spread_source, self.client_id,
                    )
                    submit_limit = _pr180_limit
                else:
                    _pr180_audit_extras["pr180_runtime_action"] = "PASSED"
            else:
                # ── OBSERVE MODE — measure baseline impact, never block ─────
                # The guard ran exactly as in enforce mode and the full audit
                # is captured. We do NOT terminalize and we do NOT reprice
                # (unless PR180_OBSERVE_REPRICE_ENABLED=1). The broker submit
                # proceeds with the original limit price.
                if _pr180_decision == "BLOCK":
                    _pr180_reason = _pr180_audit_extras.get("pr180_block_reason", "PR180_BLOCKED")
                    _pr180_audit_extras["pr180_runtime_action"] = "OBSERVED_WOULD_BLOCK"
                    log.warning(
                        "[%s] PR180_ENTRY_PRICING_OBSERVED — would_block=%s | mode=observe | "
                        "contract=%s bid=%s mid=%s ask=%s spread_pct=%s spread_source=%s "
                        "proposed_limit=%.2f client_id=%s",
                        ticker, _pr180_reason, approved_contract,
                        _pr180_bid, _pr180_mid, _pr180_ask, _pr180_spread,
                        _pr180_spread_source, submit_limit, self.client_id,
                    )
                elif _pr180_decision == "REPRICE_PROCEED" and _pr180_limit is not None:
                    if PR180_OBSERVE_REPRICE_ENABLED:
                        _pr180_audit_extras["pr180_runtime_action"] = "OBSERVED_REPRICED"
                        log.info(
                            "[%s] PR180_ENTRY_REPRICED — controlled-limit band | "
                            "mode=observe | observe_reprice=1 | "
                            "contract=%s old_limit=%.2f new_limit=%.2f spread_pct=%.3f "
                            "spread_source=%s client_id=%s",
                            ticker, approved_contract, submit_limit, _pr180_limit,
                            float(_pr180_spread or 0.0), _pr180_spread_source,
                            self.client_id,
                        )
                        submit_limit = _pr180_limit
                    else:
                        _pr180_audit_extras["pr180_runtime_action"] = "OBSERVED_WOULD_REPRICE"
                        log.info(
                            "[%s] PR180_ENTRY_PRICING_OBSERVED — would_reprice "
                            "to=%.2f | mode=observe | observe_reprice=0 | "
                            "contract=%s current_limit=%.2f spread_pct=%.3f "
                            "spread_source=%s client_id=%s",
                            ticker, _pr180_limit, approved_contract, submit_limit,
                            float(_pr180_spread or 0.0), _pr180_spread_source,
                            self.client_id,
                        )
                else:
                    _pr180_audit_extras["pr180_runtime_action"] = "OBSERVED_PROCEED"
        # ──────────────────────────────────────────────────────────────────────

        # Keep approved_plan in sync so OSM and DB record the correct price.
        try:
            approved_plan.limit_price = submit_limit
        except Exception:
            pass  # plan is a namespace; attribute assignment is always valid

        # Build the entry pricing audit to persist in orders.meta post-submit.
        _entry_pricing_audit = {
            "entry_pricing_audit":         True,
            "selected_contract":           approved_contract,
            "original_limit_price":        _plan_limit,
            "original_selector_ask":       _plan_limit,  # plan_limit = selector ask at selection time
            "submit_bid":                  _submit_quote_fields.get("submit_bid"),
            "submit_mid":                  _submit_quote_fields.get("submit_mid"),
            "submit_ask":                  float(_submit_ask),
            "submit_last":                 _submit_quote_fields.get("submit_last"),
            "submitted_limit_price":       float(submit_limit),
            "limit_vs_submit_ask_pct":     round((_ask_cross / _submit_ask) * 100, 4) if _submit_ask > 0 else None,
            "quote_refreshed_at_submit":   True,
            "quote_age_ms":                int(_quote_age_ms),
            "spread_pct_at_submit":        _submit_quote_fields.get("spread_pct"),
            "sandbox_mode":                self.paper,
            "broker_base_url":             getattr(getattr(self.broker, "cfg", None), "base_url", None),
            "pricing_rule":                "PAPER_ASK_CROSS" if self.paper else "LIVE_ASK_CROSS",
            "ask_cross_cents":             _ask_cross,
            "drift_from_plan_pct":         round(_drift_pct * 100, 4),
            "entry_price_decision":        _entry_pricing_decision,
            "attempt_number":              0,
            "retry_reprice_count":         0,
            # PR #180: persist Jason-live precision guard audit (when active).
            **(_pr180_audit_extras or {}),
        }

        # 5) Submit with the refreshed, ask-anchored limit price.
        log.info(
            "[%s] %s — submitting EXISTING queue order | local=%s contract=%s "
            "plan_ask=%.2f submit_ask=%.2f cross=+%.2f limit=%.2f drift=%.1f%% x%s",
            ticker,
            "PAPER" if self.paper else "LIVE",
            queue_local_order_id,
            approved_contract,
            _plan_limit,
            _submit_ask,
            _ask_cross,
            submit_limit,
            _drift_pct * 100,
            approved_qty,
        )
        if _deferred:
            log.info(
                "[%s] DEFERRED_BREACH_SUBMIT_ATTEMPT — local=%s contract=%s qty=%s limit=%.2f",
                ticker,
                queue_local_order_id,
                approved_contract,
                approved_qty,
                submit_limit,
            )

            # ── P0 (PR #295): CAS-persist real OCC contract into the existing
            # PENDING_TRIGGER order row BEFORE the handoff proof reads it.
            #
            # ROOT CAUSE this fixes:
            #   record_deferred_hydration_result / submit_existing_entry._update_contract_pre_submit
            #   both write the real contract into the orders row, but they are
            #   called AFTER the #294 handoff proof reads the row. So a valid
            #   selector success → plan copy-back arrives at the proof with
            #   orders.contract still = "DEFERRED:<TICKER>" → proof blocks as
            #   MATERIALIZATION_COPYBACK_MISMATCH → no broker POST ever fires.
            #
            # REQUIRED SEQUENCE after this patch:
            #   selector real OCC → copy to approved_plan → refresh ask /
            #   compute final submit_limit → CAS-persist → re-read for proof →
            #   proof passes → submit_existing_entry() → broker POST
            #
            # This block ONLY runs on the deferred path (_deferred=True) and
            # ONLY when the selector previously captured a real OCC contract
            # (_handoff_snapshot["captured"]=True). Non-deferred paths are
            # byte-for-byte unchanged.
            _copyback_write_ok = False
            if _handoff_snapshot.get("captured") and _handoff_snapshot.get("selector_contract"):
                _cb_contract = str(_handoff_snapshot["selector_contract"])
                _cb_limit    = float(submit_limit or 0)
                _cb_qty      = int(getattr(approved_plan, "contracts", 0) or 0)
                _cb_cost     = round(_cb_qty * _cb_limit * 100, 2) if _cb_qty and _cb_limit else 0.0
                if _cb_contract and not _cb_contract.upper().startswith("DEFERRED:") and _cb_limit > 0.01 and _cb_qty > 0:
                    try:
                        _rec_fn = getattr(
                            self.order_state_machine,
                            "record_deferred_hydration_result",
                            None,
                        ) if self.order_state_machine is not None else None
                        if callable(_rec_fn):
                            _copyback_write_ok = bool(_rec_fn(
                                queue_local_order_id,
                                success=True,
                                status="PENDING_TRIGGER",
                                contract=_cb_contract,
                                limit_price=_cb_limit,
                                qty=_cb_qty,
                                reserved_cost=_cb_cost,
                                contract_selection_status="CONTRACT_SELECTED",
                                hydration_meta={
                                    "hydration_stage":             "deferred_breach_pre_submit_copyback",
                                    "materialization_entry_path":  "DEFERRED_BREACH_MATERIALIZATION",
                                    "selector_contract":           _cb_contract,
                                    "copied_plan_contract":        str(getattr(approved_plan, "contract_symbol", "") or ""),
                                    "pre_submit_contract":         str(approved_contract or ""),
                                    "pre_submit_limit":            _cb_limit,
                                    "pre_submit_qty":              _cb_qty,
                                    "client_id":                   _proof_client_id,
                                    "execution_mode":              _proof_execution_mode,
                                    "local_order_id":              str(queue_local_order_id or ""),
                                    "signal_id":                   str(signal_id or ""),
                                },
                            ))
                        else:
                            _copyback_write_ok = False
                    except Exception as _cb_exc:
                        log.warning(
                            "[%s] DEFERRED_MATERIALIZATION_COPYBACK_PERSIST_FAILED "
                            "local_order_id=%s err=%s — will block before submit",
                            ticker, queue_local_order_id, _cb_exc,
                        )
                        _copyback_write_ok = False

                    if _copyback_write_ok:
                        log.info(
                            "DEFERRED_MATERIALIZATION_COPYBACK_PERSISTED "
                            "local_order_id=%s contract=%s limit=%.4f qty=%d",
                            queue_local_order_id, _cb_contract, _cb_limit, _cb_qty,
                        )
                    else:
                        # CAS missed: row was not in the expected PENDING_TRIGGER /
                        # DEFERRED state. Treat as a stale/already-submitted row —
                        # do not broker POST. Emit canonical terminal outcome so
                        # orders.meta is not silent.
                        _cb_fail_reason = "MATERIALIZATION_COPYBACK_WRITE_FAILED"
                        log.critical(
                            "[%s] PRE_SUBMIT_BLOCK — %s | "
                            "local_order_id=%s contract=%s | "
                            "CAS-persist of real OCC contract failed; "
                            "order row may be stale or already submitted; "
                            "broker_order_id=null; terminalizing",
                            ticker, _cb_fail_reason, queue_local_order_id, _cb_contract,
                        )
                        _emit_deferred_outcome(
                            "BREACH_SUBMISSION_SKIPPED",
                            reason=f"materialization_copyback_write_failed:cas_miss_or_stale_order",
                            contract=_cb_contract,
                            extra={
                                "failure_stage":                   "materialization_copyback_persist",
                                "materialization_detail_override": "MATERIALIZATION_COPYBACK_WRITE_FAILED",
                                "mismatch_reason":                 "copyback_persist_failed_before_handoff_proof",
                                "selector_contract":               _cb_contract,
                                "pre_submit_limit":                _cb_limit,
                                "pre_submit_qty":                  _cb_qty,
                                "local_order_id":                  str(queue_local_order_id or ""),
                                "client_id":                       _proof_client_id,
                                "execution_mode":                  _proof_execution_mode,
                            },
                        )
                        _terminalize_breach_failure(_cb_fail_reason)
                        return
                else:
                    # Selector snapshot exists but values are not copyback-ready
                    # (placeholder contract, zero limit, zero qty). The existing
                    # DEFERRED_CONTRACT / LIMIT invariants below will catch this.
                    # Skip the CAS write — do not persist a bad state.
                    log.warning(
                        "[%s] DEFERRED_COPYBACK_SKIP — snapshot values not copyback-ready "
                        "contract=%r limit=%.4f qty=%d; falling through to existing invariants",
                        ticker, _cb_contract, _cb_limit, _cb_qty,
                    )

        # ── P0 follow-up: Entry Confirmation Preflight ──────────────────────────
        # Runs AFTER live ask refresh (live quote available), BEFORE broker submit.
        # For plans with confirmation_required=True (set by PR74 Hybrid Gate),
        # checks: quote age, spread, option fade, underlying reversal.
        # Non-blocking for non-client plans — fast-path if confirmation_required=False.
        _confirm_meta = {}
        try:
            from ap_entry_confirmation import check_entry_confirmation
            _sig_for_confirm = watched.signal or {}
            _plan_meta_for_confirm = getattr(approved_plan, "metadata", {}) or {}
            # P0 (PR #262): LIVE default-required. Force the flag into plan
            # metadata so the legacy no-op path is impossible in LIVE — the
            # confirmation checks ALWAYS run, and only an explicit
            # passed=True reaches the broker. Paper behavior unchanged.
            _is_live_submit = str(
                getattr(self, "execution_mode", "") or getattr(self, "mode", "") or ""
            ).strip().upper() == "LIVE"
            if _is_live_submit:
                if _live_confirmation_required():
                    if isinstance(_plan_meta_for_confirm, dict):
                        _plan_meta_for_confirm["confirmation_required"] = True
                        try:
                            setattr(approved_plan, "metadata", _plan_meta_for_confirm)
                        except Exception:
                            pass
                else:
                    log.critical(
                        "[%s] LIVE_CONFIRMATION_BYPASSED_BY_ENV — "
                        "LIVE_CONFIRMATION_REQUIRED=0 is set; live submit will "
                        "proceed under legacy confirmation semantics. This must "
                        "never be set in normal operation.",
                        ticker,
                    )
            _sandbox = bool(
                _plan_meta_for_confirm.get("sandbox_mode")
                or getattr(self.broker, "sandbox", False)
            )
            _underlying_last = None
            try:
                # Re-use the latest watcher underlying quote if available
                _ul_ask = getattr(watched, "last_quote_ask", None)
                _ul_bid = getattr(watched, "last_quote_bid", None)
                if _ul_ask and _ul_bid and _ul_ask > 0 and _ul_bid > 0:
                    _underlying_last = (_ul_ask + _ul_bid) / 2
                elif _ul_ask and _ul_ask > 0:
                    _underlying_last = _ul_ask
            except Exception:
                pass

            _confirm_result = check_entry_confirmation(
                plan             = approved_plan,
                direction        = str(getattr(approved_plan, "side", "CALL") or "CALL").upper(),
                trigger_price    = float(getattr(approved_plan, "trigger_price", 0) or 0) or None,
                live_bid         = _submit_quote_fields.get("submit_bid"),
                live_ask         = _submit_quote_fields.get("submit_ask"),
                live_quote_age_ms= _quote_age_ms if "_quote_age_ms" in dir() else None,
                underlying_last  = _underlying_last,
                decision_option_price = float(_plan_limit or 0) or None,  # use saved pre-overwrite decision price
                score     = float(_sig_for_confirm.get("score") or 0) or None,
                tier      = str(getattr(approved_plan, "tier", "") or ""),
                timeframe = str(_sig_for_confirm.get("timeframe") or "1d"),
                sandbox_mode = _sandbox,
            )
            # P0 (PR #262): LIVE must block on an unknown/None result with an
            # explicit reason. (The outer except already fails closed on any
            # raise; this names the None-shape case instead of surfacing an
            # AttributeError.)
            if _is_live_submit and (
                _confirm_result is None or not hasattr(_confirm_result, "passed")
            ):
                raise RuntimeError("live_confirmation_error:none_result")
            _confirm_meta = _confirm_result.to_meta(
                started_at   = _confirm_result.metadata.get("live_entry_ts", ""),
                completed_at = __import__("datetime").datetime.now(
                    __import__("datetime").timezone.utc).isoformat(),
            )
            if not _confirm_result.passed:
                _fail_reason = _confirm_result.fail_reason or "entry_confirm_failed"

                # PR #219 Fix A: observe-only daily continuation must NOT terminalize.
                # When daily_continuation_mode="observe" and the fail reason is a
                # daily_continuation_failed:<> code, this is a non-blocking observation
                # signal — the submit path must continue. Only enforce-mode and
                # non-continuation failures should expire the pending entry.
                _observe_only_daily_continuation = (
                    _confirm_meta.get("daily_continuation_mode") == "observe"
                    and str(_fail_reason).startswith("daily_continuation_failed")
                )

                if _observe_only_daily_continuation:
                    log.warning(
                        "[%s] ENTRY_CONFIRM_OBSERVED — daily continuation would block "
                        "in enforce mode but observe mode is active; submit will "
                        "continue | reason=%s",
                        ticker, _fail_reason,
                    )
                    # Persist confirmation meta for observability without blocking.
                    if queue_local_order_id and hasattr(self.order_state_machine, "update_order_meta"):
                        try:
                            self.order_state_machine.update_order_meta(
                                queue_local_order_id, {"entry_confirmation": _confirm_meta})
                        except Exception:
                            pass
                    # Fall through to broker submit — do NOT cleanup, expire, or
                    # write blocked_at_breach.

                else:
                    log.info(
                        "[%s] ENTRY_CONFIRM_BLOCK — %s | client=%s | "
                        "spread=%.3f fade=%.2f reversal=%.3f age=%.1fs",
                        ticker, _fail_reason,
                        str(watched.signal.get("client_id", "?") if watched.signal else "?"),
                        float(_confirm_meta.get("spread_pct") or 0),
                        float(_confirm_meta.get("option_move_pct") or 0),
                        float(_confirm_meta.get("underlying_move_pct") or 0),
                        float(_confirm_meta.get("quote_age_seconds") or 0),
                    )
                    funnel.inc("entry_confirm_blocked")
                    if signal_id:
                        # Only write to known ap_signals columns — no unknown fields
                        self.store.update_signal_fields(signal_id, {
                            "decision_status": "blocked_at_breach",
                            "context_notes":   _fail_reason,
                        })
                    if queue_local_order_id and hasattr(self.order_state_machine, "update_order_meta"):
                        try:
                            self.order_state_machine.update_order_meta(
                                queue_local_order_id, {"entry_confirmation": _confirm_meta})
                        except Exception:
                            pass
                    self._cleanup_pending_entry_order(
                        watched,
                        action="expire",
                        reason=_fail_reason,
                    )
                    # PR81 Final Amendment v2 §3: ENTRY_CONFIRMATION_FAILED ledger write.
                    try:
                        from ap.opportunity_ledger import (
                            update_opportunity, STAGE_ENTRY_CONFIRMATION,
                        )
                        _client_id_for_ledger = str(
                            watched.signal.get("client_id") if watched.signal else ""
                        ) or getattr(self, "client_id", "")
                        _canon = str(
                            (watched.signal or {}).get("canonical_signal_id") or signal_id
                        )
                        update_opportunity(
                            signal_id or _canon, _client_id_for_ledger,
                            "ENTRY_CONFIRMATION_FAILED",
                            canonical_signal_id=_canon,
                            miss_stage=STAGE_ENTRY_CONFIRMATION,
                            miss_reason=_fail_reason,
                            order_local_id=str(queue_local_order_id) if queue_local_order_id else None,
                            entry_confirmation_result=_fail_reason,
                        )
                    except Exception:
                        pass
                    return   # NO BROKER SUBMIT
        except ImportError:
            # When confirmation_required=True, a missing module is NOT safe to skip.
            # Client-eligible trades must not bypass confirmation — fail closed.
            _gate_meta_imp = (getattr(approved_plan, "metadata", {}) or {})
            _hcqg_imp      = _gate_meta_imp.get("hybrid_client_quality_gate") or {}
            # P0 (PR #262): in LIVE the module is required, full stop —
            # reason live_confirmation_required. The plan-flag path below
            # continues to cover client-gated paper flows.
            _live_needs_confirm_imp = (
                str(
                    getattr(self, "execution_mode", "") or getattr(self, "mode", "") or ""
                ).strip().upper() == "LIVE"
                and _live_confirmation_required()
            )
            if _live_needs_confirm_imp or _hcqg_imp.get("confirmation_required"):
                log.critical(
                    "[%s] ENTRY_CONFIRM_MODULE_MISSING — confirmation_required=True "
                    "but ap_entry_confirmation is not deployed. "
                    "Blocking client submit to preserve gate integrity.",
                    ticker,
                )
                self._alert_degraded(
                    "ENTRY_CONFIRM_MODULE_MISSING",
                    severity="CRITICAL",
                    client_id=str(
                        watched.signal.get("client_id", "?") if watched.signal else "?"
                    ),
                    ticker=ticker,
                    signal_id=signal_id,
                    details={"reason": "entry_confirm_module_missing",
                             "confirmation_required": True},
                )
                funnel.inc("entry_confirm_blocked")
                if signal_id:
                    self.store.update_signal_fields(signal_id, {
                        "decision_status": "blocked_at_breach",
                        "context_notes":   "entry_confirm_module_missing",
                    })
                if queue_local_order_id and hasattr(self.order_state_machine,
                                                    "update_order_meta"):
                    try:
                        self.order_state_machine.update_order_meta(
                            queue_local_order_id,
                            {"entry_confirmation": {
                                "confirmation_required": True,
                                "confirmation_passed":   False,
                                "confirmation_fail_reason": "entry_confirm_module_missing",
                            }},
                        )
                    except Exception:
                        pass
                self._cleanup_pending_entry_order(
                    watched,
                    action="expire",
                    reason="entry_confirm_module_missing",
                )
                # PR81 Final Amendment v2 §3: ENTRY_CONFIRMATION_FAILED ledger write.
                try:
                    from ap.opportunity_ledger import (
                        update_opportunity, STAGE_ENTRY_CONFIRMATION,
                    )
                    _client_id_for_ledger = str(
                        watched.signal.get("client_id") if watched.signal else ""
                    ) or getattr(self, "client_id", "")
                    _canon = str(
                        (watched.signal or {}).get("canonical_signal_id") or signal_id
                    )
                    update_opportunity(
                        signal_id or _canon, _client_id_for_ledger,
                        "ENTRY_CONFIRMATION_FAILED",
                        canonical_signal_id=_canon,
                        miss_stage=STAGE_ENTRY_CONFIRMATION,
                        miss_reason="entry_confirm_module_missing",
                        order_local_id=str(queue_local_order_id) if queue_local_order_id else None,
                        entry_confirmation_result="entry_confirm_module_missing",
                    )
                except Exception:
                    pass
                return   # NO BROKER SUBMIT
            # confirmation not required — module absence is safe to skip
            log.debug(
                "ap_entry_confirmation not found — preflight skipped "
                "(confirmation_required=False for this signal)"
            )
        except Exception as _ec_err:
            # Fail-closed for confirmation errors — block the submit
            log.error("[%s] ENTRY_CONFIRM_ERROR — failing closed: %s", ticker, _ec_err)
            _terminalize_breach_failure(
                f"entry_confirm_error:{_ec_err}",
                cleanup_action="expire",
                funnel_key="entry_confirm_blocked",
            )
            # PR81 Final Amendment v2 §3: ENTRY_CONFIRMATION_FAILED ledger write.
            try:
                from ap.opportunity_ledger import (
                    update_opportunity, STAGE_ENTRY_CONFIRMATION,
                )
                _client_id_for_ledger = str(
                    watched.signal.get("client_id") if watched.signal else ""
                ) or getattr(self, "client_id", "")
                _canon = str(
                    (watched.signal or {}).get("canonical_signal_id") or signal_id
                )
                update_opportunity(
                    signal_id or _canon, _client_id_for_ledger,
                    "ENTRY_CONFIRMATION_FAILED",
                    canonical_signal_id=_canon,
                    miss_stage=STAGE_ENTRY_CONFIRMATION,
                    miss_reason=f"entry_confirm_error:{_ec_err}",
                    order_local_id=str(queue_local_order_id) if queue_local_order_id else None,
                    entry_confirmation_result=f"entry_confirm_error:{_ec_err}",
                )
            except Exception:
                pass
            return

        _submit_execution_mode = _resolve_submit_execution_mode(
            approved_plan,
            sig,
            getattr(self, "mode", None),
            getattr(self, "paper", None),
        )
        if _submit_execution_mode is None:
            _terminalize_breach_failure(
                "metadata_invalid:unknown_execution_mode",
                cleanup_action="expire",
                funnel_key="entry_metadata_blocked",
            )
            return

        # ── P0: Deferred materialization pre-submit invariant ──────────────────
        # For deferred orders only: assert that contract resolution and limit-price
        # materialization actually happened before we send any bytes to Tradier.
        # The OSM has its own DEFERRED_CONTRACT_BLOCKED guard as a second layer,
        # but this execution-core-layer guard fires first and uses named reason
        # codes (DEFERRED_CONTRACT_NOT_MATERIALIZED / DEFERRED_LIMIT_NOT_MATERIALIZED)
        # so dashboards can distinguish "selection ran and copy-back failed" from
        # "selection never ran" — broker_order_id remains null in both cases.
        if _deferred:
            _pre_contract = str(approved_contract or "")
            _pre_limit    = float(submit_limit or 0)
            _pre_qty      = int(getattr(approved_plan, "contracts", 0) or 0)

            # ── P0 amendment #6 (PR #294): identity vars hoisted here, BEFORE
            # the order-row read block and before any branch that may call
            # _emit_deferred_outcome() with these fields. Previously they were
            # assigned after the BLOCK_RETRY early-return, causing an
            # UnboundLocalError if osm.get_order() raised or returned None
            # before reaching the assignment. Defensive getattr so an unusual
            # runner shape can never NameError the proof.
            _proof_client_id = str(getattr(self, "client_id", "") or "")
            _proof_execution_mode = str(
                getattr(self, "execution_mode", None)
                or getattr(self, "mode", "")
                or ""
            )

            # ── P0 amendment #5+#6 (PR #294 final hardening): fail-closed
            # order-row read. Two distinct failure details, one terminal
            # lifecycle:
            #
            #   READ FAILURE      → TERMINAL_NO_TRADEABLE_CONTRACT
            #                         detail: MATERIALIZATION_ORDER_ROW_UNREADABLE
            #   READABLE MISMATCH → TERMINAL_NO_TRADEABLE_CONTRACT
            #                         detail: MATERIALIZATION_COPYBACK_MISMATCH
            #
            # Stage A: read the persisted `orders` row. Track the raw row, read
            # error, and bounded read attempts separately so the pure classifier
            # can distinguish "exception" from "row not found" in its audit
            # field. No logging of "non-fatal" here — order row unreadable
            # after selector success IS fatal for this trigger.
            _order_row_raw = None
            _order_row_read_error: "str | None" = None
            _order_row_read_attempts = 0
            if _handoff_snapshot.get("captured") and queue_local_order_id:
                (
                    _order_row_raw,
                    _order_row_read_error,
                    _order_row_read_attempts,
                ) = _read_order_row_for_handoff_proof(
                    self.order_state_machine,
                    queue_local_order_id,
                )
                if _order_row_read_error:
                    log.warning(
                        "[%s] handoff order-row read FAILED local_order_id=%s: %s",
                        ticker, queue_local_order_id, _order_row_read_error,
                    )

            # Stage B: classify the read result using the pure helper.
            _row_read_verdict, _row_read_reason = _classify_order_row_read(
                handoff_snapshot=_handoff_snapshot,
                order_row_raw=_order_row_raw,
                read_error=_order_row_read_error,
            )

            if _row_read_verdict == "BLOCK_RETRY":
                # Cannot prove the persisted row before broker POST.
                # Amendment #6 Option B: both outcome AND lifecycle are terminal.
                # DEFERRED_ORDER_ROW_UNREADABLE → TERMINAL_NO_TRADEABLE_CONTRACT
                # (see _canonical_materialization_outcome for the full rationale).
                # This is the honest choice: there is no safe retry path at this
                # pipeline stage that can replay only the order-row read without
                # re-running the full breach/selector cycle. Expiring the order and
                # stamping a clear terminal detail is better than claiming
                # RETRY_LATER while immediately expiring the row.
                _inv_err = "MATERIALIZATION_ORDER_ROW_UNREADABLE"
                log.critical(
                    "[%s] PRE_SUBMIT_INVARIANT_FAILED — %s | "
                    "reason=%s local_order_id=%s | "
                    "selector proved real OCC contract but persisted "
                    "order row is unreadable before broker POST — "
                    "broker_order_id=null; terminalizing (Option B: terminal)",
                    ticker, _inv_err, _row_read_reason, queue_local_order_id,
                )
                _emit_deferred_outcome(
                    "DEFERRED_ORDER_ROW_UNREADABLE",
                    reason=f"order_row_unreadable:{_row_read_reason}",
                    contract=str(_pre_contract or ""),
                    extra={
                        "failure_stage":                   "handoff_order_row_read",
                        "materialization_detail_override": "MATERIALIZATION_ORDER_ROW_UNREADABLE",
                        "order_row_read_error":            _row_read_reason,
                        "order_row_read_attempts":         _order_row_read_attempts,
                        "selector_contract":               _handoff_snapshot.get("selector_contract"),
                        "selector_bid":                    _handoff_snapshot.get("selector_bid"),
                        "selector_ask":                    _handoff_snapshot.get("selector_ask"),
                        "selector_mid":                    _handoff_snapshot.get("selector_mid"),
                        "copied_plan_contract":            _handoff_snapshot.get("copied_plan_contract"),
                        "pre_submit_contract":             _pre_contract,
                        "pre_submit_limit":                _pre_limit,
                        "pre_submit_qty":                  _pre_qty,
                        "local_order_id":                  str(queue_local_order_id or ""),
                        "client_id":                       _proof_client_id,
                        "execution_mode":                  _proof_execution_mode,
                    },
                )
                _terminalize_breach_failure(_inv_err)
                return

            # Stage C: row is readable (PASS or proof not applicable). Extract
            # order_row_contract into the snapshot for the handoff classifier
            # and for the structured proof log.
            if _row_read_verdict == "PASS" and _order_row_raw is not None:
                try:
                    _row_d = (
                        dict(_order_row_raw)
                        if not isinstance(_order_row_raw, dict)
                        else _order_row_raw
                    )
                    _handoff_snapshot["order_row_contract"] = (
                        str(_row_d.get("contract") or "") or None
                    )
                except Exception as _extract_exc:
                    # Row was readable but contract extraction failed. This
                    # is structurally the same as "row unreadable" — emit the
                    # same terminal detail rather than falling through.
                    _inv_err = "MATERIALIZATION_ORDER_ROW_UNREADABLE"
                    log.critical(
                        "[%s] PRE_SUBMIT_INVARIANT_FAILED — %s | "
                        "contract extraction from order row failed: %s | "
                        "broker_order_id=null; terminalizing",
                        ticker, _inv_err, _extract_exc,
                    )
                    _emit_deferred_outcome(
                        "DEFERRED_ORDER_ROW_UNREADABLE",
                        reason=f"order_row_contract_extraction_failed:{_extract_exc}",
                        contract=str(_pre_contract or ""),
                        extra={
                            "failure_stage":                   "handoff_order_row_extraction",
                            "materialization_detail_override": "MATERIALIZATION_ORDER_ROW_UNREADABLE",
                            "order_row_read_error":            str(_extract_exc),
                            "order_row_read_attempts":         _order_row_read_attempts,
                            "selector_contract":               _handoff_snapshot.get("selector_contract"),
                            "local_order_id":                  str(queue_local_order_id or ""),
                            "client_id":                       _proof_client_id,
                            "execution_mode":                  _proof_execution_mode,
                        },
                    )
                    _terminalize_breach_failure(_inv_err)
                    return

            # ── P0 amendment #3+#4+#5: deferred materialization handoff proof.
            # Runs BEFORE the existing DEFERRED_CONTRACT/LIMIT invariants.
            # All three views are now aligned (selector, copied_plan, order_row)
            # or the earlier stages above have already terminalized this trigger.
            # The classifier is only reached when the order row was readable (or
            # proof is not applicable). A mismatch here means the data is
            # coherent but the pipeline broke — permanent terminal, not retry.
            _handoff_ok, _handoff_mismatch = _classify_materialization_handoff(
                handoff_snapshot=_handoff_snapshot,
                pre_submit_contract=_pre_contract,
                pre_submit_limit=_pre_limit,
                pre_submit_qty=_pre_qty,
                order_row_contract=_handoff_snapshot.get("order_row_contract"),
            )

            # Structured proof log — one line per deferred trigger that
            # reached selector success. Emitted BEFORE any block/return so it
            # is present even when the block below terminalizes the row.
            if _handoff_ok is not None:
                log.info(
                    "DEFERRED_MATERIALIZATION_HANDOFF_PROOF "
                    "symbol=%s client_id=%s execution_mode=%s local_order_id=%s "
                    "selector_contract=%s copied_plan_contract=%s "
                    "order_row_contract=%s pre_submit_contract=%s "
                    "selector_ask=%s pre_submit_limit=%.4f qty=%d "
                    "handoff_ok=%s mismatch_reason=%s",
                    ticker,
                    _proof_client_id,
                    _proof_execution_mode,
                    str(queue_local_order_id or ""),
                    _handoff_snapshot.get("selector_contract"),
                    _handoff_snapshot.get("copied_plan_contract"),
                    _handoff_snapshot.get("order_row_contract"),
                    _pre_contract,
                    _handoff_snapshot.get("selector_ask"),
                    _pre_limit,
                    _pre_qty,
                    "true" if _handoff_ok else "false",
                    _handoff_mismatch or "",
                )

            if _handoff_ok is False:
                _inv_err = "MATERIALIZATION_COPYBACK_MISMATCH"
                log.critical(
                    "[%s] PRE_SUBMIT_INVARIANT_FAILED — %s | mismatch=%s | "
                    "selector_contract=%s copied_plan_contract=%s "
                    "order_row_contract=%s pre_submit_contract=%s "
                    "pre_submit_limit=%.4f qty=%d | "
                    "broker_order_id=null; blocking broker POST",
                    ticker, _inv_err, _handoff_mismatch,
                    _handoff_snapshot.get("selector_contract"),
                    _handoff_snapshot.get("copied_plan_contract"),
                    _handoff_snapshot.get("order_row_contract"),
                    _pre_contract, _pre_limit, _pre_qty,
                )
                _emit_deferred_outcome(
                    "BREACH_SUBMISSION_SKIPPED",
                    reason=f"materialization_copyback_mismatch:{_handoff_mismatch}",
                    contract=str(_pre_contract or ""),
                    extra={
                        "failure_stage":                   "materialization_handoff_proof",
                        "materialization_detail_override": "MATERIALIZATION_COPYBACK_MISMATCH",
                        "mismatch_reason":                 _handoff_mismatch,
                        "selector_contract":               _handoff_snapshot.get("selector_contract"),
                        "selector_bid":                    _handoff_snapshot.get("selector_bid"),
                        "selector_ask":                    _handoff_snapshot.get("selector_ask"),
                        "selector_mid":                    _handoff_snapshot.get("selector_mid"),
                        "selector_premium":                _handoff_snapshot.get("selector_premium"),
                        "selector_qty":                    _handoff_snapshot.get("selector_qty"),
                        "copied_plan_contract":            _handoff_snapshot.get("copied_plan_contract"),
                        "copied_plan_limit":               _handoff_snapshot.get("copied_plan_limit"),
                        "copied_plan_qty":                 _handoff_snapshot.get("copied_plan_qty"),
                        "copied_plan_max_usd":             _handoff_snapshot.get("copied_plan_max_usd"),
                        # P0 amendment #4 (PR #294): third-view field.
                        # Always present in the emit extra (even when None
                        # — that itself is diagnostic: it means the DB read
                        # failed and the pre-submit view drove the mismatch).
                        "order_row_contract":              _handoff_snapshot.get("order_row_contract"),
                        "pre_submit_contract":             _pre_contract,
                        "pre_submit_limit":                _pre_limit,
                        "pre_submit_qty":                  _pre_qty,
                        "local_order_id":                  str(queue_local_order_id or ""),
                        "client_id":                       _proof_client_id,
                        "execution_mode":                  _proof_execution_mode,
                    },
                )
                _terminalize_breach_failure(_inv_err)
                return

            if (not _pre_contract) or _pre_contract.upper().startswith("DEFERRED:"):
                _inv_err = "DEFERRED_CONTRACT_NOT_MATERIALIZED"
                log.critical(
                    "[%s] PRE_SUBMIT_INVARIANT_FAILED — %s | contract=%r | "
                    "broker_order_id=null; blocking broker POST",
                    ticker, _inv_err, _pre_contract,
                )
                _terminalize_breach_failure(_inv_err)
                return
            if _pre_limit <= 0.01:
                _inv_err = "DEFERRED_LIMIT_NOT_MATERIALIZED"
                log.critical(
                    "[%s] PRE_SUBMIT_INVARIANT_FAILED — %s | limit_price=%r | "
                    "broker_order_id=null; blocking broker POST",
                    ticker, _inv_err, _pre_limit,
                )
                _terminalize_breach_failure(_inv_err)
                return

            # ── P0 (monday-trade-flow-readiness, amended): acceptance cap on
            # the FINAL submit limit. The selection-time cap check above can
            # pass and the entry-pricing loop can then refresh the limit
            # upward (ask-cross, drift within ENTRY_MAX_PRICE_DRIFT_PCT). The
            # cap is a hard acceptance-window invariant, so it is re-enforced
            # here against the exact limit that would go to the broker.
            # Same FAIL-CLOSED contract as selection time: flag on + cap
            # invalid → block; flag off → no-op.
            #
            # P0 amendment #2 (PR #294 review): both terminal branches now
            # emit through _emit_deferred_outcome BEFORE the OSM cleanup so
            # the exactly-once terminal channel fires and orders.meta carries
            # canonical entry_path / materialization_outcome /
            # materialization_detail plus the full block-context audit (final
            # submit limit, cap, contract, and the selected quote snapshot
            # when available). The external reason code stays
            # UNTRADEABLE_FOR_ACCOUNT_SIZE for operator vocabulary
            # continuity; the fine-grained cause lives in
            # materialization_detail. Broker POST remains blocked by the
            # subsequent _terminalize_breach_failure that cleans up the
            # order — the canonical stamp is written first so a cleanup
            # exception cannot swallow the audit.
            _cap_enabled, _accept_cap, _cap_error = _acceptance_ask_cap()
            if _cap_enabled:
                # Selected-contract snapshot is only bound on the deferred
                # selection branch; resolve via locals() so the non-deferred
                # path can never NameError while building the audit.
                _sel_snapshot = locals().get("_sel")
                _cap_audit_common = {
                    "failure_stage": "acceptance_ask_cap_pre_submit",
                    "final_submit_limit": float(_pre_limit or 0),
                    "acceptance_cap": (
                        float(_accept_cap) if _accept_cap is not None else None
                    ),
                    "selected_contract": str(_pre_contract or ""),
                    "selected_bid": float(getattr(_sel_snapshot, "bid", 0) or 0),
                    "selected_ask": float(getattr(_sel_snapshot, "ask", 0) or 0),
                    "selected_mid": float(getattr(_sel_snapshot, "mid", 0) or 0),
                    "qty": int(getattr(approved_plan, "contracts", 0) or 0),
                }
                if _cap_error:
                    _inv_err = f"ACCEPTANCE_CAP_MISCONFIGURED:{_cap_error}"
                    log.critical(
                        "[%s] PRE_SUBMIT_INVARIANT_FAILED — %s | fail-closed; "
                        "broker_order_id=null; blocking broker POST",
                        ticker, _inv_err,
                    )
                    _emit_deferred_outcome(
                        "UNTRADEABLE_FOR_ACCOUNT_SIZE",
                        reason=_inv_err,
                        contract=str(_pre_contract or ""),
                        extra={
                            **_cap_audit_common,
                            "acceptance_cap_error": _cap_error,
                            "materialization_detail_override": _inv_err,
                        },
                    )
                    _terminalize_breach_failure(_inv_err)
                    return
                if _pre_limit > _accept_cap:
                    _inv_err = (
                        f"ACCEPTANCE_CAP_EXCEEDED_AT_SUBMIT:"
                        f"limit_{_pre_limit:.2f}_cap_{_accept_cap:.2f}"
                    )
                    log.critical(
                        "[%s] PRE_SUBMIT_INVARIANT_FAILED — %s | selection-time "
                        "cap passed but refreshed submit limit exceeds cap; "
                        "broker_order_id=null; blocking broker POST",
                        ticker, _inv_err,
                    )
                    _emit_deferred_outcome(
                        "UNTRADEABLE_FOR_ACCOUNT_SIZE",
                        reason=_inv_err,
                        contract=str(_pre_contract or ""),
                        extra={
                            **_cap_audit_common,
                            "materialization_detail_override": "ACCEPTANCE_CAP_EXCEEDED_AT_SUBMIT",
                        },
                    )
                    _terminalize_breach_failure(_inv_err)
                    return

        submit_res = self.order_state_machine.submit_existing_entry(
            local_order_id=queue_local_order_id,
            broker=self.broker,
            plan=approved_plan,
            limit_price=submit_limit,
        )

        if submit_res.get("ok"):
            local_order_id = submit_res.get("local_order_id")
            broker_order_id = submit_res.get("broker_order_id")
            # P0: persist entry pricing audit into orders.meta (best-effort).
            if local_order_id and hasattr(self.order_state_machine, "update_order_meta"):
                try:
                    self.order_state_machine.update_order_meta(
                        local_order_id, _entry_pricing_audit
                    )
                except Exception as _audit_exc:
                    log.warning("[%s] entry_pricing_audit persist failed: %s", ticker, _audit_exc)
                # P0 follow-up: persist confirmation preflight metadata on success
                if _confirm_meta:
                    try:
                        self.order_state_machine.update_order_meta(
                            local_order_id, {"entry_confirmation": _confirm_meta})
                    except Exception:
                        pass
            # Item 3 — persist selector candidate audit into orders.meta
            # (EVIDENCE ONLY, best-effort, non-destructive JSONB merge).
            # _candidate_audit is set ONLY when breach-time deferred selection
            # ran above. For preselected (non-deferred) orders the queue stashed
            # the audit on approved_plan.metadata at selection time — fall back
            # to that source so both paths produce orders.meta.selector_candidate_audit.
            try:
                _persist_ca = _candidate_audit
                if not _persist_ca and approved_plan is not None:
                    _pmeta = getattr(approved_plan, "metadata", None) or {}
                    if isinstance(_pmeta, dict):
                        _persist_ca = _pmeta.get("candidate_table") or _pmeta.get("selector_candidate_audit")
                if _persist_ca and local_order_id and hasattr(
                    self.order_state_machine, "update_order_meta"
                ):
                    self.order_state_machine.update_order_meta(
                        local_order_id,
                        {
                            "selector_candidate_audit": _persist_ca,
                            "candidate_table": _persist_ca,
                        },
                    )
            except Exception as _ca_exc:
                log.warning("[%s] candidate_audit persist failed: %s", ticker, _ca_exc)
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "submitted",
                    "context_notes": f"entry_submitted local={local_order_id} broker={broker_order_id}",
                })
            # P1 ENTRY FIX (2026-05-21): tag the original ask submission as
            # entry_attempt=0 so the dashboard log-parser can bucket attempts.
            # entry_attempt=0 = original ask submit (this line)
            # entry_attempt=1 = ask+0.01 repeg (emitted by retry_engine.apply_repeg)
            # entry_attempt=2 = ask+0.02 repeg
            log.info(
                "[%s] Entry submitted via OSM | entry_attempt=0 local=%s broker=%s %sx %s @ $%.2f",
                ticker,
                local_order_id,
                broker_order_id,
                approved_qty,
                approved_contract,
                submit_limit,
            )
            # P0 amended (req: persist materialization audit): final broker
            # limit + selected quote snapshot, merged into orders.meta as
            # materialization_* fields by the emitter. `_sel` is bound ONLY on
            # the deferred selection branch — resolve via locals() so the
            # non-deferred success path (where _emit_deferred_outcome is a
            # no-op anyway) can never NameError while building arguments.
            _sel_snapshot = locals().get("_sel")
            _emit_deferred_outcome(
                "BREACH_BROKER_SUBMITTED",
                contract=str(approved_contract or ""),
                broker_order_id=str(broker_order_id or ""),
                extra={
                    "final_submit_limit": float(submit_limit or 0),
                    "selected_bid": float(getattr(_sel_snapshot, "bid", 0) or 0),
                    "selected_ask": float(getattr(_sel_snapshot, "ask", 0) or 0),
                    "selected_mid": float(getattr(_sel_snapshot, "mid", 0) or 0),
                    "qty": int(approved_qty or 0),
                    # P0 amendment #3+#4 (PR #294): handoff proof fields
                    # persisted on the successful terminal too, so a
                    # dashboard can prove every deferred trigger — whether
                    # it materialized or terminalized — carries the
                    # three-stage snapshot in orders.meta.
                    "handoff_ok":            True,
                    "selector_contract":     _handoff_snapshot.get("selector_contract"),
                    "copied_plan_contract":  _handoff_snapshot.get("copied_plan_contract"),
                    "order_row_contract":    _handoff_snapshot.get("order_row_contract"),
                    "pre_submit_contract":   str(approved_contract or ""),
                    "pre_submit_limit":      float(submit_limit or 0),
                    "pre_submit_qty":        int(approved_qty or 0),
                    "local_order_id":        str(queue_local_order_id or ""),
                    "client_id":             str(getattr(self, "client_id", "") or ""),
                    "execution_mode":        str(getattr(self, "execution_mode", None) or getattr(self, "mode", "") or ""),
                },
            )
            return

        log.error(
            "[%s] Entry submit failed via OSM | local=%s error=%s",
            ticker,
            submit_res.get("local_order_id") or queue_local_order_id,
            submit_res.get("error"),
        )
        _emit_deferred_outcome(
            "BREACH_SUBMISSION_SKIPPED",
            reason=f"osm_submit_existing_entry_failed:{submit_res.get('error')}",
            contract=str(approved_contract or ""),
        )
        funnel.inc("order_failed")
        if signal_id:
            self.store.update_signal_fields(signal_id, {
                "decision_status": "submit_failed",
                "context_notes": f"osm_submit_existing_entry_failed={submit_res.get('error')}",
            })
        try:
            if hasattr(self.order_state_machine, "cancel_pending_entry"):
                self.order_state_machine.cancel_pending_entry(
                    queue_local_order_id,
                    reason=f"submit_failed:{submit_res.get('error')}",
                )
        except Exception as exc:
            log.warning("[%s] OSM cancel after submit failure failed | order=%s error=%s", ticker, queue_local_order_id, exc)
        return

    # ── CALLBACK: Position Closed ─────────────────────────────────────────────

    def _on_position_close(self, pos: ManagedPosition, decision):
        # ── IDEMPOTENCY: skip if position already marked closed ──────────────
        if getattr(pos, "closed", False):
            log.debug("[%s] _on_position_close called but pos.closed=True — skipping", pos.ticker)
            return

        with self._pos_lock:
            self._position_count = max(0, self._position_count - 1)

        sector = getattr(pos, "signal", {}).get("correlation_bucket", "OTHER")
        with self._sector_lock:
            self._sector_counts[sector] = max(0, self._sector_counts.get(sector, 0) - 1)

        # ── EXIT SUBMISSION ─────────────────────────────────────────────────
        sig = getattr(pos, "signal", {})
        _sig_id = str(sig.get("signal_id", ""))
        _urgency = str(getattr(decision, "urgency", "") or "").upper()
        _bid = getattr(pos, "current_bid", 0) or 0
        _ask = getattr(pos, "current_ask", 0) or 0
        _mid = getattr(pos, "current_option_price", 0) or 0

        # PR-B / FIX-2: decision.suggested_limit is the FIRST authority.
        # The exit engine already prices the exit (urgency tier-aware,
        # bid/mid/(mid+bid)/2). When suggested_limit > 0 we honor it
        # verbatim and skip the duplicate execution-core ladder below.
        # The existing ladder logic is preserved as a fallback for
        # decisions that did not produce a suggested_limit (legacy
        # callers, paper sim, etc.).
        #
        # TODO(post-proof-week): delete the duplicate execution-core
        # pricing ladder entirely and make the exit engine the sole
        # pricing authority. Left in place for this PR to avoid
        # fill-side-effects right before proof week.
        _suggested_limit = float(getattr(decision, "suggested_limit", 0) or 0)
        _suggested_limit_locked = False  # True once a valid suggested_limit is locked in

        # EXIT PRICING POLICY
        # ─────────────────────────────────────────────────────────────────
        # IMMEDIATE urgency (hard stop -33%, never-green, EOD) → MARKET ORDER
        #   Must fill. Hard stops and EOD closes cannot miss. No slippage risk.
        #
        # HIGH urgency — PROTECTIVE exits (RUNNER_TRAIL, PROFIT_LOCK, etc.) → BID
        #   Start at bid immediately. These exits protect earned profit; sitting at
        #   mid while price slides back costs more than the bid/mid spread.
        #
        # HIGH urgency — SCALE exits (scale-out W1/W2/W3) → MID
        #   Scale-outs are non-urgent partial closes; mid pricing is fine.
        #   Still escalates to bid at 90s and market at 150s if unfilled.
        #
        # No quote available for IMMEDIATE → BLOCK (never submit zero-price stop)
        # No quote available for HIGH → retry on next quote cycle (not blocking)

        _PROTECTIVE_EXIT_CODES = {
            "RUNNER_TRAIL", "TRAILING_STOP", "PROFIT_LOCK", "TOUCHED_PROFIT_STOP",
            "SMALL_WIN_LOCK", "EOD_FORCE_CLOSE", "HARD_STOP", "STOP_HIT",
            "SENTINEL_FORCED_EXIT", "NEVER_GREEN_STOP", "THETA_STOP", "TIME_STOP",
        }
        _reason_code = str(getattr(decision, "reason_code", "") or "").upper()
        _reason_text = str(getattr(decision, "reason", "") or "").upper()

        _PROTECTIVE_REASON_TEXT_MARKERS = {
            "RUNNER TRAIL", "TRAILING STOP", "PROFIT LOCK", "TOUCHED PROFIT",
            "SMALL WIN", "EOD FORCE CLOSE", "HARD STOP", "STOP HIT",
            "NEVER GREEN", "THETA", "TIME STOP", "SENTINEL",
            # Soft stops — thesis failed or went stale. Must use bid, not mid.
            # These were missing and would get mid pricing (wrong for a stop).
            "THESIS_FAIL_SOFT_STOP", "THESIS_STALE_SOFT_STOP", "SOFT_STOP",
        }

        _is_protective = (
            _reason_code in _PROTECTIVE_EXIT_CODES
            or any(marker in _reason_text for marker in _PROTECTIVE_REASON_TEXT_MARKERS)
        )

        # IMMEDIATE splits into two tiers:
        #   TRUE EMERGENCY → market (HARD_STOP -33%, EOD, SENTINEL/kill-switch)
        #   PROFIT EXIT → aggressive bid-limit, fast step-down (TARGET, TOUCHED
        #     PROFIT, PROFIT LOCK, IMMEDIATE_TP) — never blind-market a winner
        _reason_code_u = str(getattr(decision, "reason_code", "") or "").upper()
        _reason_text_u = str(getattr(decision, "reason", "") or "").upper()
        _TRUE_EMERGENCY_CODES = {
            "HARD_STOP", "EOD_FORCE_CLOSE", "SENTINEL_FORCED_EXIT",
        }
        _TRUE_EMERGENCY_MARKERS = {
            "HARD STOP", "EOD FORCE CLOSE", "EOD FORCED", "SENTINEL FORCED",
            "KILL SWITCH", "EMERGENCY",
        }
        _is_true_emergency = (
            _reason_code_u in _TRUE_EMERGENCY_CODES
            or any(m in _reason_text_u for m in _TRUE_EMERGENCY_MARKERS)
        )
        _use_market = (_urgency == "IMMEDIATE") and _is_true_emergency
        _is_fast_profit_exit = (_urgency == "IMMEDIATE") and not _is_true_emergency

        # Exit attempt tracking — needed by cheap-contract logic below
        _exit_submit_ts   = getattr(pos, "_exit_submit_ts", 0) or 0
        _exit_attempts    = getattr(pos, "_exit_attempts",  0) or 0
        _age_since_submit = time.time() - _exit_submit_ts if _exit_submit_ts else 0
        _ladder_price_set = False  # True once a step-down price is locked in

        # CHEAP CONTRACT HANDLING:
        # Sub-$0.25 options have wide relative spreads and limits can bounce.
        # OLD behavior blind-marketed them on the FIRST exit attempt — that
        # violates no-market-except-emergency and gave away the spread.
        #
        # NEW behavior: cheap contracts still use bid-limit FIRST. Only escalate
        # to market after multiple failed attempts (the limit cascade is real,
        # but one clean bid-limit attempt almost always fills). Default threshold
        # is 0 (disabled) — cheap-market only kicks in if explicitly enabled AND
        # the position has already failed 3+ exit attempts.
        _CHEAP_EXIT_MARKET_THRESHOLD = float(
            os.getenv("CHEAP_EXIT_MARKET_THRESHOLD", "0")  # default OFF
        )
        _cheap_exit_min_attempts = int(
            os.getenv("CHEAP_EXIT_MARKET_MIN_ATTEMPTS", "3")
        )
        _current_opt_price = float(getattr(pos, "current_option_price", 0) or _mid or _bid)
        if (
            not _use_market
            and _CHEAP_EXIT_MARKET_THRESHOLD > 0
            and _current_opt_price > 0
            and _current_opt_price < _CHEAP_EXIT_MARKET_THRESHOLD
            and _exit_attempts >= _cheap_exit_min_attempts
        ):
            _use_market = True
            log.warning(
                "[%s] CHEAP_CONTRACT_MARKET_EXIT — $%.2f < $%.2f AND %d failed "
                "limit attempts — escalating to market | %s",
                pos.ticker, _current_opt_price, _CHEAP_EXIT_MARKET_THRESHOLD,
                _exit_attempts, decision.reason,
            )
        elif (
            _CHEAP_EXIT_MARKET_THRESHOLD > 0
            and _current_opt_price > 0
            and _current_opt_price < _CHEAP_EXIT_MARKET_THRESHOLD
            and _exit_attempts < _cheap_exit_min_attempts
        ):
            log.info(
                "[%s] CHEAP_CONTRACT_BID_LIMIT — $%.2f cheap but attempt %d < %d "
                "— using bid-limit, not market yet | %s",
                pos.ticker, _current_opt_price, _exit_attempts,
                _cheap_exit_min_attempts, decision.reason,
            )

        # LIMIT-TO-MARKET ESCALATION for HIGH urgency exits:
        # EXIT PRICING — step-down bid ladder. Never market except true emergency.
        # Market orders on options fill at ASK. On an intraday dip this means
        # selling at the absolute worst price (BA case: $0.44 worse than bid).
        #
        # Step-down ladder priced from LIVE BID:
        #   First attempt:  current_bid
        #   15–30s unfilled: current_bid - $0.01
        #   30–60s unfilled: current_bid - $0.02
        #   60s+ unfilled:  market ONLY for HARD_STOP / EOD, else hold at bid-0.02
        # (_exit_submit_ts / _exit_attempts / _ladder_price_set defined above)

        # In-flight escalation for HIGH urgency exits
        if not _use_market and _urgency == "HIGH" and getattr(pos, "exit_in_flight", False) and _exit_submit_ts > 0:
            if _age_since_submit >= 60 and _is_protective:
                # 60s+ unfilled on a hard stop/EOD → market (must exit)
                _use_market = True
                log.warning("[%s] EXIT ESCALATED TO MARKET — bid-limit unfilled >60s | %s", pos.ticker, decision.reason)
            elif _age_since_submit >= 30 and _bid > 0:
                # 30–60s → bid - $0.02
                _exit_limit = max(round(_bid - 0.02, 2), 0.01)
                exit_price  = _exit_limit
                _ladder_price_set = True
                log.warning("[%s] EXIT STEP-DOWN bid-$0.02 = $%.2f — unfilled >30s | %s", pos.ticker, _exit_limit, decision.reason)
            elif _age_since_submit >= 15 and _bid > 0:
                # 15–30s → bid - $0.01
                _exit_limit = max(round(_bid - 0.01, 2), 0.01)
                exit_price  = _exit_limit
                _ladder_price_set = True
                log.warning("[%s] EXIT STEP-DOWN bid-$0.01 = $%.2f — unfilled >15s | %s", pos.ticker, _exit_limit, decision.reason)

        # FAST PROFIT-EXIT step-down — tighter than HIGH (profit exits want speed
        # but should never blind-market and give away the spread on a winner).
        # 0–10s: bid | 10–20s: bid-0.01 | 20–40s: bid-0.02 | 40s+: market (take it)
        if not _use_market and _is_fast_profit_exit and getattr(pos, "exit_in_flight", False) and _exit_submit_ts > 0:
            if _age_since_submit >= 40:
                # 40s+ — profit exit truly stuck, take market to lock the gain
                _use_market = True
                log.warning("[%s] PROFIT EXIT → MARKET — bid-limit unfilled >40s, locking gain | %s", pos.ticker, decision.reason)
            elif _age_since_submit >= 20 and _bid > 0:
                _exit_limit = max(round(_bid - 0.02, 2), 0.01)
                exit_price  = _exit_limit
                _ladder_price_set = True
                log.warning("[%s] PROFIT EXIT STEP-DOWN bid-$0.02 = $%.2f — unfilled >20s | %s", pos.ticker, _exit_limit, decision.reason)
            elif _age_since_submit >= 10 and _bid > 0:
                _exit_limit = max(round(_bid - 0.01, 2), 0.01)
                exit_price  = _exit_limit
                _ladder_price_set = True
                log.warning("[%s] PROFIT EXIT STEP-DOWN bid-$0.01 = $%.2f — unfilled >10s | %s", pos.ticker, _exit_limit, decision.reason)

        # PR-B / FIX-2: First-authority check. If the exit engine produced
        # a suggested_limit > 0 AND this is not a true emergency (which
        # must go to market regardless), lock the exit engine's price and
        # skip the ladder. This is the trail-exit spread fix: the engine
        # prices TRAIL at (mid+bid)/2; the execution-core ladder used to
        # override that to bid.
        if not _use_market and _suggested_limit > 0:
            _exit_limit = round(_suggested_limit, 2)
            exit_price  = _exit_limit
            _suggested_limit_locked = True
            # Stamp submit timestamp on FIRST submit so OSM/reconciler aging
            # paths see the same behavior they did under the legacy ladder.
            if not getattr(pos, "exit_in_flight", False) or not _exit_submit_ts:
                pos._exit_submit_ts = time.time()
            pos._exit_attempts = _exit_attempts + 1
            log.info(
                "[%s] EXIT @ suggested_limit=$%.2f (exit-engine authority) "
                "attempt=%d | urgency=%s | %s",
                pos.ticker, _exit_limit, pos._exit_attempts, _urgency, decision.reason,
            )

        if _use_market:
            _exit_limit = None
            exit_price = _bid if _bid > 0 else _mid
            if exit_price <= 0:
                log.critical("[%s] CLOSE BLOCKED — no quote for IMMEDIATE exit | %s", pos.ticker, decision.reason)
                return
            log.info("[%s] MARKET EXIT @ est.$%.2f (bid) | urgency=%s | %s", pos.ticker, exit_price, _urgency, decision.reason)
        elif _suggested_limit_locked:
            # Already priced via decision.suggested_limit — ladder skipped.
            pass
        elif _ladder_price_set and _exit_limit is not None and _exit_limit > 0:
            # Step-down already locked an aggressive price — DO NOT overwrite it
            exit_price = _exit_limit
            log.info("[%s] LADDER PRICE LOCKED @ $%.2f | %s", pos.ticker, _exit_limit, decision.reason)
        elif _bid > 0:
            # First attempt — all exits start at current bid
            _exit_limit = round(_bid, 2)
            exit_price  = _exit_limit
            # Only stamp timer on FIRST submit — never reset while in-flight, or
            # the step-down ladder never ages to 15s/30s/60s (reviewer bug #2)
            if not getattr(pos, "exit_in_flight", False) or not _exit_submit_ts:
                pos._exit_submit_ts = time.time()  # type: ignore[attr-defined]
            pos._exit_attempts  = _exit_attempts + 1  # type: ignore[attr-defined]
            log.info("[%s] LIMIT EXIT @ $%.2f (bid) attempt=%d | urgency=%s | %s",
                     pos.ticker, _exit_limit, pos._exit_attempts, _urgency, decision.reason)
        elif _mid > 0:
            # Bid unavailable — use mid as fallback
            _exit_limit = round(_mid, 2)
            exit_price  = _exit_limit
            if not getattr(pos, "exit_in_flight", False) or not _exit_submit_ts:
                pos._exit_submit_ts = time.time()  # type: ignore[attr-defined]
            pos._exit_attempts  = _exit_attempts + 1  # type: ignore[attr-defined]
            log.info("[%s] LIMIT EXIT @ $%.2f (mid fallback) attempt=%d | %s",
                     pos.ticker, _exit_limit, pos._exit_attempts, decision.reason)
        else:
            if _urgency == "IMMEDIATE":
                log.critical("[%s] CLOSE BLOCKED — no quote for IMMEDIATE exit | %s", pos.ticker, decision.reason)
                return
            log.warning("[%s] Exit skipped — no quote | will retry | %s", pos.ticker, decision.reason)
            return

        if self.order_state_machine and pos.position_id:
            _price_str = f"${_exit_limit:.2f}" if _exit_limit is not None else "MARKET"
            log.info(
                f"[{pos.ticker}] {'PAPER' if self.paper else 'LIVE'} CLOSE -- "
                f"submitting sell_to_close via OSM @ {_price_str} | {decision.reason}"
            )
            exit_res = self.order_state_machine.submit_exit(
                broker      = self.broker,
                position_id = str(pos.position_id),
                contract    = pos.option_symbol,
                symbol      = pos.ticker,
                direction   = pos.side,
                qty         = pos.quantity_remaining,
                limit_price = _exit_limit,  # None = market order for IMMEDIATE exits
                signal_id   = _sig_id or None,
                order_type  = "market" if _exit_limit is None else "limit",
            )
            if exit_res["ok"]:
                log.info(
                    f"[{pos.ticker}] Exit order submitted | "
                    f"local={exit_res['local_order_id']} broker={exit_res['broker_order_id']} "
                    f"qty={pos.quantity_remaining} @ ${pos.current_option_price:.2f}"
                )
            else:
                log.error(
                    f"[{pos.ticker}] Exit submit failed via OSM | "
                    f"order={exit_res['local_order_id']} error={exit_res['error']}"
                )
                return
        else:
            log.critical(
                f"[{pos.ticker}] CLOSE BLOCKED — OSM or position_id missing; "
                f"cannot submit sell_to_close through production authority | {decision.reason}"
            )
            return

        # Always compute option P&L from option prices — never from pos.entry_price
        # which can be seeded from avg_fill (which sometimes stored underlying price).
        _entry_opt = float(pos.entry_price or 0)
        _exit_opt  = float(exit_price or 0)
        if _entry_opt > 0 and _exit_opt > 0:
            opt_pnl = (_exit_opt - _entry_opt) / _entry_opt * 100  # e.g. 15.3 = 15.3%
        else:
            opt_pnl = 0.0
        # Breakeven band: trades within BREAKEVEN_BAND_PCT of entry count as
        # breakeven wins — not losses. Prevents $1-$2 slippage from showing
        # as a loss when the position was effectively flat. PR-B / FIX-8:
        # reads from the module-level constant, not an inline getenv.
        win  = opt_pnl >= BREAKEVEN_BAND_PCT
        tier = sig.get("tier", "A+")

        if _entry_opt > 0:
            log.info(
                "[%s] P&L CALC | entry=$%.4f exit=$%.4f → option_pnl=%.1f%% win=%s",
                pos.ticker, _entry_opt, _exit_opt, opt_pnl, win,
            )

        # Set 30-min same-direction cooldown on master_control
        try:
            import time as _t
            _ck = f"{pos.ticker.upper()}:{pos.side.upper()}:cooldown"
            if hasattr(self.master_control, "_trade_cooldowns"):
                self.master_control._trade_cooldowns[_ck] = _t.time()
        except Exception:
            pass

        # ── TRADE INTEGRITY CHECKLIST — runs every close, before proof guard ────────
        if not getattr(pos, "_integrity_logged", False):
            pos._integrity_logged = True  # type: ignore[attr-defined]
            _checks = [
                ("has_position_id",  bool(getattr(pos, "position_id", ""))),
                ("exit_px_positive", exit_price > 0),
                ("pnl_recorded",     abs(opt_pnl) > 0.001),
            ]
            _pass = all(v for _, v in _checks)
            _str  = " ".join(f"{k}={'OK' if v else 'FAIL'}" for k, v in _checks)
            log.info(
                "[INTEGRITY] %s | %s | %s | pnl=%+.1f%% exit=$%.2f",
                pos.ticker, "PASS" if _pass else "FAIL",
                _str, opt_pnl * 100, exit_price,
            )

        # Guard: only log once per position — exit_in_flight retries must not re-log
        if getattr(pos, "proof_logged", False):
            log.debug("[%s] proof.log_trade skipped — already logged for this position", pos.ticker)
            return

        # ── FIX 2: STAGE proof data on position — DO NOT write proof yet ─────
        # submit_exit() means the exit order was SUBMITTED, not FILLED.
        # Proof / P&L / feedback must only be written after broker fill
        # is confirmed through the fill_monitor → mark_position_closed path.
        # Stage all the data now (while we have decision/sig context) and
        # finalize it in _finalize_proof() called from mark_position_closed.
        #
        # exit_price here is the limit price we placed (estimated fill).
        # The actual broker fill price overwrites it in _finalize_proof().
        pos._proof_staged = {                                   # type: ignore[attr-defined]
            "ticker":             pos.ticker,
            "pattern":            sig.get("pattern", ""),
            "side":               pos.side,
            "timeframe":          sig.get("timeframe", "1d"),
            "score":              float(sig.get("score", 0) or 0),
            "tier":               tier,
            "context_score":      float((sig.get("score_breakdown") or {}).get("real_time_ctx", 0) or 0),
            "setup_status":       self.feedback.get_setup_status(
                                      pos.ticker, sig.get("pattern", ""),
                                      sig.get("timeframe", "1d"), pos.side),
            "entry_trigger":      pos.underlying_entry,
            "entry_option_price": pos.entry_price,
            "exit_option_price":  exit_price,   # estimated; overwritten at fill
            "underlying_entry":   pos.underlying_entry,
            "underlying_exit":    pos.current_underlying,
            "contracts":          pos.quantity,
            "exit_reason":        decision.reason,
            "opt_pnl":            opt_pnl,       # re-calculated at fill with actual fill price
            "win":                win,
            "spread_pct":         float(sig.get("spread_pct", 0) or 0),
            "chain_grade":        sig.get("chain_grade", ""),
            "opened_at":          pos.opened_at if hasattr(pos, "opened_at") else None,
            "synthetic_entry":    bool(getattr(pos, "synthetic_entry", False)),
            "position_id":        str(getattr(pos, "position_id", "") or ""),
            "local_order_id":     str(getattr(pos, "local_order_id", "") or ""),
            "signal":             sig,
            "paper":              self.paper,
        }
        pos.proof_logged = True  # type: ignore[attr-defined]
        log.info(
            "[EXIT_SUBMITTED_PROOF_STAGED] %s | est_exit=$%.2f pnl=%.1f%% | "
            "proof HELD — will finalize at broker-confirmed fill",
            pos.ticker, exit_price, opt_pnl,
        )
        # All P&L-bearing records (proof, feedback, signal-closed, shadow)
        # are deferred to _finalize_proof() called from mark_position_closed()
        # after broker fill confirmation. Nothing is written here.

        # Log trade to edge intelligence (logger instantiated once in __init__ to avoid resource leaks)
        try:
            _edge_logger = self._edge_logger
            if _edge_logger:
                # PR-B / FIX-7: edge-logger payload uses an explicit
                # allowlist of trade-relevant fields. The earlier
                # pos.__dict__ pattern leaked every ManagedPosition
                # internal (including ghost fields, _submit_generation,
                # pending_exit_*, exit_identity_quarantine, etc.) into
                # the edge intelligence schema, creating an unstable
                # contract that broke on every code release.
                _edge_logger.log_trade(
                    position={
                        "ticker":             pos.ticker,
                        "side":               pos.side,
                        "direction":          pos.side,
                        "entry_price":        getattr(pos, "entry_price", 0.0),
                        "quantity":           getattr(pos, "quantity", 0),
                        "quantity_remaining": getattr(pos, "quantity_remaining", 0),
                        "opened_at":          getattr(pos, "opened_at", None),
                        "position_id":        getattr(pos, "position_id", ""),
                        "client_id":          getattr(pos, "client_id", ""),
                        "signal_id":          getattr(pos, "signal_id", ""),
                        "signal":             sig,
                        "timeframe":          sig.get("timeframe", "1d"),
                        "synthetic_entry":    bool(getattr(pos, "synthetic_entry", False)),
                        # CODEX-1 (PR B follow-up): APTradeLogger.log_trade in
                        # ap_edge_intelligence.py derives contract_symbol from
                        # `option_symbol` or `contract` (line ~110), and uses
                        # underlying_entry / underlying_stop / underlying_target
                        # for r-multiple and risk metrics. These are all
                        # trade-relevant identity/level fields (not internals).
                        # Omitting them was a real regression that broke trade
                        # analytics and the proof_trades ↔ trades_intel join.
                        "option_symbol":      getattr(pos, "option_symbol", ""),
                        "contract":           getattr(pos, "option_symbol", ""),  # alias
                        "underlying_entry":   getattr(pos, "underlying_entry", 0.0),
                        "underlying_stop":    getattr(pos, "underlying_stop", 0.0),
                        "underlying_target":  getattr(pos, "underlying_target", 0.0),
                        # Score / tier come from the signal dict (not pos);
                        # the logger checks both top-level and signal nested.
                        "score":              sig.get("score", 0),
                        "tier":               sig.get("tier", ""),
                        # contracts alias — logger accepts contracts | quantity | qty
                        "contracts":          getattr(pos, "quantity", 0),
                    },
                    exit_info={
                        "exit_price": exit_price,
                        "exit_reason": decision.reason,
                        "exit_ts": datetime.now(timezone.utc).isoformat(),
                        "underlying_exit": pos.current_underlying,
                    },
                    client_id=self.email,
                )
        except Exception as _e:
            log.debug(f"Trade logger error (non-critical): {_e}")

        # PR-B / FIX-6: _record_intel_outcome moved to _finalize_proof()
        # so the intelligence dataset receives the ACTUAL broker fill P/L,
        # not the estimated submit-time P/L. The signal_id is resolved
        # at finalize time from the staged dict.

    # ── CALLBACKS: Expire / Invalidate ────────────────────────────────────────

    def _cleanup_pending_entry_order(self, watched: WatchedSignal, *, action: str, reason: str) -> None:
        """Best-effort OSM cleanup for watcher terminal outcomes.

        The queue creates an ENTRY order before arming the watcher. If the
        watcher later expires or invalidates before broker submission, that
        order must not remain as a ghost CREATED/PENDING_TRIGGER row.
        """
        sig = getattr(watched, "signal", {}) or {}
        local_order_id = str(sig.get("local_order_id") or "").strip()
        if not local_order_id or self.order_state_machine is None:
            return

        try:
            if action == "expire" and hasattr(self.order_state_machine, "expire_pending_entry"):
                ok = self.order_state_machine.expire_pending_entry(local_order_id, reason=reason)
                if not ok:
                    log.warning(
                        "[%s] OSM expire_pending_entry returned false | order=%s reason=%s",
                        watched.ticker, local_order_id, reason,
                    )
                return

            if action == "cancel" and hasattr(self.order_state_machine, "cancel_pending_entry"):
                ok = self.order_state_machine.cancel_pending_entry(local_order_id, reason=reason)
                if not ok:
                    log.warning(
                        "[%s] OSM cancel_pending_entry returned false | order=%s reason=%s",
                        watched.ticker, local_order_id, reason,
                    )
                return

            # Compatibility fallback for older OSM versions that do not expose
            # helper methods yet. Only legal CREATED/PENDING_TRIGGER orders will
            # transition; illegal/terminal states are blocked by OSM.transition().
            fallback_status = "EXPIRED" if action == "expire" else "CANCELED"
            if hasattr(self.order_state_machine, "transition"):
                self.order_state_machine.transition(
                    local_order_id,
                    fallback_status,
                    last_error=reason,
                )
        except Exception as exc:
            log.error(
                "[%s] OSM pending-entry cleanup failed | order=%s action=%s reason=%s error=%s",
                watched.ticker, local_order_id, action, reason, exc, exc_info=True,
            )

    def _on_signal_expire(self, watched: WatchedSignal):
        signal_id = str(watched.signal.get("signal_id", ""))
        if signal_id:
            self.store.update_status(signal_id, "expired", timestamp_flag="expired_at")
        self._cleanup_pending_entry_order(watched, action="expire", reason="watcher_expired")
        funnel.inc("watcher_expired")
        log.info(f"[{watched.ticker}] Signal expired -- no breach")

    def _on_signal_invalidate(self, watched: WatchedSignal):
        signal_id = str(watched.signal.get("signal_id", ""))
        plan = watched.signal.get("plan") or {}
        contract = (
            plan.get("contract_symbol")
            or watched.signal.get("contract_symbol")
            or watched.signal.get("contract")
            or ""
        )

        # ── P1 FIX (2026-05-21): DEFERRED contract guard ───────────────────
        # If the contract starts with 'DEFERRED:', the contract was not yet
        # selected at watcher-arm time. Watcher invalidation on a DEFERRED
        # contract is NOT a true thesis invalidation — it means contract
        # selection at breach time hasn't been attempted yet. Keep the
        # watcher in PENDING_TRIGGER and let breach-time contract selection
        # do its job. Only log the event; do NOT permanently cancel.
        #
        # SAFETY (post-review): by the time _on_signal_invalidate is called,
        # the watcher has ALREADY set self.state = WatchState.INVALIDATED.
        # Returning early without restoring the state would zombie the
        # watcher in INVALIDATED — it would never check() the price again
        # and breach-time contract selection would never run. Restore the
        # state to PENDING explicitly so the next poll re-enters check().
        is_deferred_contract = isinstance(contract, str) and contract.startswith("DEFERRED:")
        if is_deferred_contract:
            try:
                from ap_entry_watcher import WatchState
                _prior_state = getattr(watched, "state", None)
                watched.state = WatchState.PENDING
                # Reset breach counter so a stop touch doesn't immediately
                # re-invalidate on the very next poll.
                if hasattr(watched, "breach_count"):
                    watched.breach_count = 0
                log.info(
                    "[%s] DEFERRED_CONTRACT_INVALIDATED ignored | signal_id=%s contract=%s "
                    "— state restored %s -> PENDING; awaiting breach-time contract selection",
                    watched.ticker, signal_id or "?", contract, _prior_state,
                )
            except Exception as _e:
                # If we can't restore state, the safest thing is to still NOT cancel
                # the order — log the failure so it can be investigated.
                log.error(
                    "[%s] DEFERRED_CONTRACT_INVALIDATED state-restore failed: %s — "
                    "order NOT canceled, but watcher may be stuck in INVALIDATED",
                    watched.ticker, _e,
                )
            funnel.inc("deferred_contract_invalidated")
            return  # Do NOT cancel the order or write 'invalidated' to signal store.

        # ── Full forensic context for legitimate invalidations ─────────────────
        # P1 FIX (2026-05-21): every watcher_invalidated must log:
        # signal_id, plan_id, local_order_id, client_id, symbol, contract,
        # CALL/PUT, trigger, current underlying, option bid/mid/ask, stop, target,
        # setup age, exact invalidation formula, and reason_code.
        try:
            plan_id = (plan.get("plan_id") if isinstance(plan, dict) else None) or "?"
            local_order_id = (
                (plan.get("local_order_id") if isinstance(plan, dict) else None)
                or watched.signal.get("local_order_id") or "?"
            )
            side = (
                (plan.get("direction") if isinstance(plan, dict) else None)
                or watched.signal.get("direction")
                or watched.signal.get("side") or "?"
            )
            trigger = getattr(watched, "trigger_price", None) or watched.signal.get("entry_trigger")
            stop    = (plan.get("stop_level") if isinstance(plan, dict) else None) or watched.signal.get("stop")
            target  = (plan.get("target")     if isinstance(plan, dict) else None) or watched.signal.get("target")
            age_secs = None
            try:
                if getattr(watched, "armed_at", None):
                    import time as _t
                    age_secs = _t.time() - watched.armed_at
            except Exception:
                pass
            # Best-effort current-market snapshot.
            current_underlying = None
            opt_bid = opt_ask = opt_mid = None
            try:
                if hasattr(self, "broker") and hasattr(self.broker, "get_quote"):
                    if watched.ticker:
                        uq = self.broker.get_quote(watched.ticker)
                        if isinstance(uq, dict):
                            current_underlying = uq.get("last") or uq.get("close") or uq.get("price")
                    if contract and not is_deferred_contract:
                        oq = self.broker.get_quote(contract)
                        if isinstance(oq, dict):
                            opt_bid = oq.get("bid")
                            opt_ask = oq.get("ask")
                            if opt_bid and opt_ask:
                                try:
                                    opt_mid = (float(opt_bid) + float(opt_ask)) / 2
                                except Exception:
                                    opt_mid = None
            except Exception:
                pass

            log.warning(
                "[%s] WATCHER_INVALIDATED | signal_id=%s plan_id=%s local_order_id=%s "
                "client_id=%s symbol=%s contract=%s side=%s trigger=%s underlying=%s "
                "opt_bid=%s opt_ask=%s opt_mid=%s stop=%s target=%s age=%s",
                watched.ticker, signal_id or "?", plan_id, local_order_id,
                getattr(self, "client_id", "?"),
                watched.ticker, contract or "?", side,
                trigger if trigger is not None else "?",
                current_underlying if current_underlying is not None else "?",
                opt_bid if opt_bid is not None else "?",
                opt_ask if opt_ask is not None else "?",
                opt_mid if opt_mid is not None else "?",
                stop if stop is not None else "?",
                target if target is not None else "?",
                f"{age_secs:.0f}s" if age_secs is not None else "?",
            )
        except Exception as e:
            log.debug("watcher_invalidated forensic log failed: %s", e)

        if signal_id:
            self.store.update_status(signal_id, "invalidated", timestamp_flag="invalidated_at")
        self._cleanup_pending_entry_order(watched, action="cancel", reason="watcher_invalidated")
        funnel.inc("watcher_invalidated")

    def _finalize_proof(self, pos: "ManagedPosition", actual_fill_price: float = 0.0) -> None:
        """Write proof/P&L/feedback using the ACTUAL broker fill price.

        Called from mark_position_closed via the exit engine's
        on_exit_fill_confirmed callback — the only point where we have
        broker-confirmed fill data. Reads the staged proof dict written
        at submit time in _on_position_close and finalises with real numbers.

        If actual_fill_price is 0 or None (can happen in paper sandbox),
        falls back to the estimated exit_price staged at submission.
        """
        staged = getattr(pos, "_proof_staged", None)
        if not staged:
            return
        if getattr(pos, "_proof_finalized", False):
            log.info(
                "[EXIT_PROOF_FINALIZE_SKIPPED_ALREADY_LOGGED] %s | "
                "position already finalized — skipping duplicate",
                getattr(pos, "ticker", "?"),
            )
            return
        pos._proof_finalized = True  # type: ignore[attr-defined]

        # Use actual fill price; fall back to estimated if broker returns 0/None
        fill = float(actual_fill_price or 0)
        est  = float(staged.get("exit_option_price") or 0)
        final_exit_price = fill if fill > 0 else est

        entry_px = float(staged.get("entry_option_price") or 0)
        if entry_px > 0 and final_exit_price > 0:
            opt_pnl_pct = (final_exit_price - entry_px) / entry_px
        else:
            opt_pnl_pct = staged.get("opt_pnl", 0.0) / 100.0

        # PR-B / FIX-8: single-source-of-truth via module-level constant.
        # BREAKEVEN_BAND_PCT is stored as percent (e.g. -2.0 = -2%);
        # opt_pnl_pct here is decimal, so divide by 100.
        win = opt_pnl_pct >= (BREAKEVEN_BAND_PCT / 100.0)

        slippage_vs_est = round(final_exit_price - est, 4) if est > 0 else None
        if fill > 0 and est > 0:
            log.info(
                "[EXIT_FILL_CONFIRMED_PROOF_FINALIZED] %s | "
                "est_exit=$%.2f actual_fill=$%.2f slip=%.4f pnl=%.1f%%",
                staged["ticker"], est, fill,
                slippage_vs_est if slippage_vs_est is not None else 0.0,
                opt_pnl_pct * 100,
            )
        else:
            log.info(
                "[EXIT_FILL_CONFIRMED_PROOF_FINALIZED] %s | "
                "no broker fill price — using staged estimate $%.2f pnl=%.1f%%",
                staged["ticker"], final_exit_price, opt_pnl_pct * 100,
            )

        underlying_entry = staged.get("underlying_entry") or 0
        underlying_exit  = staged.get("underlying_exit")  or pos.current_underlying or 0
        u_pnl_pct = (
            (underlying_exit - underlying_entry) / underlying_entry * 100
            if underlying_entry else 0
        )

        # proof_trades.option_pnl_pct is stored as PERCENTAGE (e.g. -25.0 = -25%)
        # not decimal (e.g. -0.25). Convert before passing to log_trade.
        opt_pnl_pct_for_proof = round(opt_pnl_pct * 100, 2)

        # CODEX-2 (PR B follow-up): proof.log_trade failure must NOT short-
        # circuit the rest of _finalize_proof. Feedback, shadow, and the
        # intel outcome callback are all independent of proof DB success —
        # they're computational over the staged dict + actual fill price.
        # Dropping them when proof errors creates silent data loss exactly
        # in degraded DB conditions (when intel matters most for diagnosis).
        # Track proof outcome with a flag; do NOT early-return.
        proof_ok = True
        try:
            self.proof.log_trade(
                ticker             = staged["ticker"],
                pattern            = staged.get("pattern", ""),
                side               = staged.get("side", ""),
                timeframe          = staged.get("timeframe", "1d"),
                score              = staged.get("score", 0),
                tier               = staged.get("tier", ""),
                context_score      = staged.get("context_score", 0),
                setup_status       = staged.get("setup_status", ""),
                entry_trigger      = underlying_entry,
                entry_option_price = entry_px,
                exit_option_price  = final_exit_price,
                underlying_entry   = underlying_entry,
                underlying_exit    = underlying_exit,
                contracts          = staged.get("contracts", 1),
                exit_reason        = staged.get("exit_reason", ""),
                option_pnl_pct     = opt_pnl_pct_for_proof,
                underlying_pnl_pct = u_pnl_pct,
                win                = win,
                spread_pct         = staged.get("spread_pct", 0),
                chain_grade        = staged.get("chain_grade", ""),
                opened_at          = staged.get("opened_at"),
                synthetic_entry    = staged.get("synthetic_entry", False),
                position_id        = staged.get("position_id", ""),
                local_order_id     = staged.get("local_order_id", ""),
                # Slippage vs staged estimate
                exit_fill_price    = fill if fill > 0 else None,
                exit_limit_placed  = est if est > 0 else None,
                slippage_vs_bid    = slippage_vs_est,
            )
        except Exception as proof_err:
            proof_ok = False
            log.error(
                "[%s] _finalize_proof: proof.log_trade failed: %s — "
                "continuing to feedback/shadow/intel (proof-independent)",
                staged["ticker"], proof_err,
            )

        # Feedback + shadow safe to finalize here with real P&L
        try:
            sig = staged.get("signal", {})
            paper = staged.get("paper", True)
            self.feedback.record_outcome(
                signal             = sig,
                entry_option_price = entry_px,
                exit_option_price  = final_exit_price,
                exit_reason        = staged.get("exit_reason", ""),
                underlying_entry   = underlying_entry,
                underlying_exit    = underlying_exit,
                contracts          = staged.get("contracts", 1),
                context_notes      = f"mode={'paper' if paper else 'live'} fill_confirmed=True",
                synthetic_entry    = staged.get("synthetic_entry", False),
            )
            # Mark signal closed only after broker-confirmed fill
            signal_id = str(sig.get("signal_id", "") or "")
            if signal_id:
                self.store.update_status(signal_id, "closed", timestamp_flag="closed_at")
        except Exception as fb_err:
            log.error("[%s] _finalize_proof: feedback/store failed: %s", staged["ticker"], fb_err)

        try:
            self.shadow.record_live_outcome(staged.get("tier", ""), opt_pnl_pct)
        except Exception as _e:
            log.warning("shadow_record_live_outcome_failed: %s", _e)

        # PR-B / FIX-6: record_intel_outcome with the ACTUAL broker fill
        # P/L (opt_pnl_pct is decimal here, e.g. 0.18 = 18%). Previously
        # called from _on_position_close with the estimated submit-time
        # P/L — the intelligence dataset received pre-fill estimates.
        if _record_intel_outcome:
            try:
                _intel_sig    = staged.get("signal", {}) or {}
                _intel_sig_id = str(_intel_sig.get("signal_id", "") or "")
                _record_intel_outcome(
                    ticker    = staged.get("ticker", ""),
                    signal_id = _intel_sig_id,
                    pnl_pct   = opt_pnl_pct,  # already decimal; actual fill-based
                )
            except Exception as _alpha_err:
                log.warning("Alpha tracker update failed: %s", _alpha_err)

    def _on_position_scale(self, pos: ManagedPosition, decision):
        log.info(
            f"[{pos.ticker}] SCALE OUT {decision.quantity}x | "
            f"P&L={pos.option_pnl_pct*100:+.1f}% | {decision.reason}"
        )
        _sig_id = str(getattr(pos, "signal", {}).get("signal_id", "") or "")

        if self.order_state_machine and pos.position_id:
            _scale_bid   = getattr(pos, "current_bid", 0) or 0
            _scale_mid   = getattr(pos, "current_option_price", 0) or 0
            if _scale_bid <= 0 and _scale_mid <= 0:
                log.critical("[%s] SCALE BLOCKED — no valid bid or mid for scale-out", pos.ticker)
                return
            _scale_limit = _scale_bid if _scale_bid > 0 else max(round(_scale_mid - 0.01, 2), 0.01)
            scale_res = self.order_state_machine.submit_exit(
                broker      = self.broker,
                position_id = str(pos.position_id),
                contract    = pos.option_symbol,
                symbol      = pos.ticker,
                direction   = pos.side,
                qty         = decision.quantity,
                limit_price = _scale_limit,
                signal_id   = _sig_id or None,
            )
            if scale_res["ok"]:
                log.info(
                    f"[{pos.ticker}] Scale exit submitted | "
                    f"local={scale_res['local_order_id']} broker={scale_res['broker_order_id']} "
                    f"qty={decision.quantity} @ ${_scale_limit:.2f}"
                )
            else:
                log.error(
                    f"[{pos.ticker}] Scale exit failed via OSM | "
                    f"order={scale_res['local_order_id']} error={scale_res['error']}"
                )
                return
        else:
            log.critical(
                f"[{pos.ticker}] SCALE BLOCKED — OSM or position_id missing; "
                "cannot submit scale-out through production authority"
            )
            return

    # ── BROKER HELPERS ────────────────────────────────────────────────────────

    # =========================================================================
    # POSITION SIZING — AGGRESSIVE RISK CURVE
    # =========================================================================
