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

import json
import math
import os
import re
import time
import uuid
import logging
import threading
from datetime import datetime, timezone, timedelta
from typing import Mapping, Optional
from types import MappingProxyType, SimpleNamespace
from zoneinfo import ZoneInfo

from ap_entry_watcher        import APEntryWatcher, WatchedSignal
from ap_exit_engine          import APExitEngine, ManagedPosition
from ap_feedback_loop        import APFeedbackLoop
from ap_tier_engine          import APShadowTracker
from ap_proof_logger         import APProofLogger, funnel
from ap_signal_store         import APSignalStore
from ap_signal_tracker       import APSignalTracker
from ap.broker_submit_identity import canonical_broker_submit_key
from ap.utils                import now_utc_iso

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


def _validate_deferred_selector_result(selection, ticker: str = "") -> tuple[bool, str, float, int]:
    """Return the broker-critical selector values only when all are valid."""
    contract = str(getattr(selection, "contract_symbol", "") or "").strip()
    contract_upper = contract.upper()
    ticker_upper = str(ticker or "").strip().upper()
    is_occ = bool(
        contract
        and not contract_upper.startswith("DEFERRED:")
        and contract_upper != ticker_upper
        and _OCC_CONTRACT_RE.search(contract_upper)
    )
    try:
        price = float(
            getattr(selection, "execution_price_per_share", 0)
            or getattr(selection, "ask", 0)
            or getattr(selection, "mid", 0)
            or 0
        )
    except (TypeError, ValueError):
        price = 0.0
    try:
        qty = int(getattr(selection, "affordable_contracts", 0) or 0)
    except (TypeError, ValueError):
        qty = 0
    return bool(is_occ and price > 0 and qty > 0), contract, price, qty

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
BOT_MODE            = (os.getenv("AP_MODE") or os.getenv("BOT_MODE") or "PAPER").upper()
MAX_POSITIONS       = int(os.getenv("MAX_POSITIONS", "7"))

# ── P0: deferred-breach contract-selector retry taxonomy ─────────────────────
# Reason codes in this set mean the chain provider had a temporary data miss —
# NOT a real contract-quality or risk reject.  On a retryable miss the deferred
# row must NOT be expired/cancelled; instead it is rearmed for retry up to
# MAX_BREACH_SELECTOR_RETRIES total attempts (default 5) before giving up.
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
#   MAX_BREACH_SELECTOR_RETRIES          default 5 total attempts
#   BREACH_SELECTOR_RETRY_DELAY_SECONDS  default 8
#   BREACH_SELECTOR_RETRY_CUTOFF_ET      default 1530 (= 3:30 PM ET; last-entry
#                                        boundary — see _breach_retry_cutoff_hhmm)
# ── Seam 3 (PR #323): canonical retry taxonomy now lives in one place ──────────
# ap/selector_retry_policy.py is the single source of truth for which selector
# reason codes are retryable.  Import the frozenset for back-compat; the module
# also exports is_retryable_selector_reason() and classify_selector_reason()
# for callers that want richer metadata.
from ap.selector_retry_policy import (
    RETRYABLE_BREACH_SELECTOR_REASONS,
    is_retryable_selector_reason as _is_retryable_selector_reason,  # noqa: F401 – re-exported
)
# NOTE: NO_VALID_PLAYBOOK_DTE_CONTRACT is NOT in RETRYABLE_BREACH_SELECTOR_REASONS.
# It is the DTE-ladder aggregation reason and may reflect structural quality
# rejects (OI_TOO_LOW, SPREAD_TOO_WIDE) as well as transient data-miss reasons.
# When the ladder emits it, execution_core inspects the dte_ladder_audit to
# determine whether the exhaustion came from data-miss (retryable) or quality
# rejects (terminal). See _is_ladder_exhaustion_retryable() below.


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
# outcomes. Every triggered deferred row must resolve to EXACTLY ONE of these
# operator-facing outcomes — the acceptance contract for restored trade flow:
#   MATERIALIZED_AND_SUBMITTED     real OCC contract selected AND handed to
#                                  the broker submit path
#   RETRY_LATER_DATA_UNAVAILABLE   transient data-miss (zero quotes / empty
#                                  chain near open); a bounded retry is
#                                  scheduled inside the warmup/entry window
#   RETRY_LATER_SELECTOR_BUDGET    PR #389: the request ran out of direct-
#                                  quote budget before finding a survivor;
#                                  a bounded retry gets a fresh budget
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
    order_row_limit: "float | None" = None,
    order_row_qty: "int | None" = None,
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
        if order_row_limit is not None and abs(
            float(order_row_limit or 0) - float(pre_submit_limit or 0)
        ) > 0.001:
            return False, (
                "order_row_limit_diverges_from_pre_submit:"
                f"row={float(order_row_limit or 0):.4f}:"
                f"pre_submit={float(pre_submit_limit or 0):.4f}"
            )
        if order_row_qty is not None and int(order_row_qty or 0) != int(pre_submit_qty or 0):
            return False, (
                "order_row_qty_diverges_from_pre_submit:"
                f"row={int(order_row_qty or 0)}:pre_submit={int(pre_submit_qty or 0)}"
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
    raw = str(os.getenv("LIVE_CONFIRMATION_REQUIRED", "1")).strip().lower()
    if raw in {"0", "false", "no", "off"}:
        return False
    # Unknown/malformed values fail closed to the LIVE default.
    return True


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
# (default 5 × 8s = ~40s), so this cannot cause open-ended retry loops;
# the cutoff only stops NEW retries from being scheduled into the close.
# Env var name is unchanged so the operational kill-switch muscle memory
# ("set BREACH_SELECTOR_RETRY_CUTOFF_ET=0 to stop all retries") still works.
_BREACH_RETRY_CUTOFF_DEFAULT_HHMM = 1530


def _positive_int_env_config(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        log.warning(
            "DEFERRED_RETRY_ENV_PARSE_ERROR key=%s value=%r default=%s",
            name,
            raw,
            default,
        )
        return int(default)
    if value <= 0:
        log.warning(
            "DEFERRED_RETRY_ENV_PARSE_ERROR key=%s value=%r default=%s",
            name,
            raw,
            default,
        )
        return int(default)
    return value


def _selector_cursor_retry_block_reason(
    *,
    cursor_enabled: bool,
    selector_attempt_number: int,
    cursor_candidate,
    cursor_load_reason: str | None,
) -> str | None:
    """Return the stable fail-closed reason before any retry selector call."""
    if not cursor_enabled or int(selector_attempt_number or 1) <= 1:
        return None
    if cursor_candidate in (None, ""):
        return "MISSING_CURSOR_ON_RETRY"
    return str(cursor_load_reason) if cursor_load_reason else None


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
        and attempt < max_attempts
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
    if _retryable_reason and attempt >= max_attempts:
        return {
            "action": "retry_exhausted",
            "reason_code": _reason_code,
            "retryable_reason": True,
            "terminal_reason": f"BREACH_RETRY_EXHAUSTED:{_reason_code}",
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
    # P0 amendment (fix/deferred-retry-due-execution-p0 §5): preserve BOTH
    # the operational reason (why THIS attempt stopped early) and the last
    # candidate-quality reason the selector was scoring against. Neither is
    # dropped in favour of the other.
    try:
        from ap.selector_retry_policy import classify_retry_reason_taxonomy
        _last_quality = None
        if isinstance(selector_audit, dict):
            _last_quality = (
                selector_audit.get("last_candidate_reject_reason")
                or selector_audit.get("best_candidate_reject_reason")
                or selector_audit.get("last_reject_reason")
            )
        _taxonomy = classify_retry_reason_taxonomy(
            reason_code, last_candidate_quality_reason=_last_quality,
        )
    except Exception:
        _taxonomy = {
            "reason_code": str(reason_code or ""),
            "classification": "UNKNOWN",
            "retry_class": "UNKNOWN",
            "selector_terminal_reason": None,
            "operational_reason": None,
            "may_retry_with_fresh_budget": False,
        }
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
        # P0 amended: canonical materialization outcome stamp — a scheduled
        # retry is either RETRY_LATER_SELECTOR_BUDGET (PR #389, when the
        # request ran out of direct-quote budget) or RETRY_LATER_DATA_UNAVAILABLE
        # (all other retryable data reasons), until the next attempt resolves
        # it to MATERIALIZED_AND_SUBMITTED or TERMINAL_NO_TRADEABLE_CONTRACT.
        "entry_path": _MATERIALIZATION_ENTRY_PATH,
        # PR #389 amendment: SELECTOR_REQUEST_BUDGET_EXHAUSTED gets its own
        # durable outcome so operators can tell "we ran out of direct-quote
        # calls this request, retry with a fresh budget" apart from the
        # generic transient data-miss. All other retryable data reasons keep
        # the existing RETRY_LATER_DATA_UNAVAILABLE contract.
        "materialization_outcome": (
            "RETRY_LATER_SELECTOR_BUDGET"
            if str(reason_code or "").strip() == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
            else "RETRY_LATER_DATA_UNAVAILABLE"
        ),
        "materialization_detail": str(reason_code or ""),
        # P0 §5: honest taxonomy — both operational and candidate-quality
        # dimensions preserved together on the durable row.
        "retry_class": _taxonomy["retry_class"],
        "selector_terminal_reason": _taxonomy["selector_terminal_reason"],
        "operational_reason": _taxonomy["operational_reason"],
        "may_retry_with_fresh_budget": _taxonomy["may_retry_with_fresh_budget"],
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
        # P0 amended: canonical materialization outcome stamp for retry-exhaustion/cutoff/
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


# ── P0 (#301): Fix B — flat selector attempt audit helpers ────────────────────
# Build and persist 16 flat last_deferred_selector_* fields to orders.meta on
# every deferred attempt. Separated into a builder (for atomic merges) and a
# standalone writer (for paths that need a dedicated write).
# ─────────────────────────────────────────────────────────────────────────────

def _build_flat_selector_audit_fields(
    *,
    selector_audit: dict,
    attempt_number: int,
    execution_mode: str,
    is_paper: bool = False,
    broker_base_url: str = "",
) -> dict:
    """
    Return the flat Fix B + Fix C audit field dict WITHOUT calling OSM.
    Used when merging into an existing update_order_meta call atomically.
    Fix C paper domain fields use domain-level classification ('live'/'sandbox'/'unknown'),
    not the raw quote_source string.
    Never raises — returns {} on any error.
    """
    import datetime as _dt
    try:
        _sa   = selector_audit or {}
        _now  = _dt.datetime.now(_dt.timezone.utc).isoformat()
        _patch: dict = {
            "last_deferred_selector_attempt_at":              _now,
            "last_deferred_selector_attempt_number":          int(attempt_number or 0),
            "last_deferred_selector_reason_code":             str(_sa.get("reason_code") or "") or None,
            "last_deferred_selector_stage":                   str(_sa.get("stage") or "") or None,
            "last_deferred_selector_explanation":             str(_sa.get("explanation") or "") or None,
            "last_deferred_selector_chain_rows":              int(_sa.get("chain_rows") or 0),
            "last_deferred_selector_survivor_count":          int(_sa.get("survivor_count") or 0),
            "last_deferred_selector_top_reject_buckets":      _sa.get("top_reject_buckets") or {},
            "last_deferred_selector_best_rejected_candidate": _sa.get("best_rejected_candidate") or None,
            "last_deferred_selector_quote_source":            str(_sa.get("quote_source") or "") or None,
            "last_deferred_selector_tradier_base_url":        str(_sa.get("tradier_base_url") or broker_base_url or "") or None,
            "last_deferred_selector_sandbox_mode":            bool(_sa.get("sandbox_mode", False)),
            "last_deferred_selector_execution_mode":          str(_sa.get("execution_mode") or execution_mode or "") or None,
            "last_dte_ladder_audit":                          _sa.get("last_dte_ladder_audit") or None,
            "dte_ladder_enabled":                             bool(_sa.get("dte_ladder_enabled", False)),
            "ladder_eligible_marker":                         bool(_sa.get("ladder_eligible_marker", False)),
            # P0 PR #302 Fix 3+4: failure classification and chain validity flat fields
            "last_deferred_selector_failure_class":          str(_sa.get("selector_failure_class") or "") or None,
            "last_deferred_selector_nonzero_quote_rows":     int(_sa.get("nonzero_quote_rows") or 0),
            "last_deferred_selector_zero_quote_ratio":       float(_sa.get("zero_quote_ratio") or 0.0),
            "last_deferred_selector_data_failure":           bool(_sa.get("data_failure", False)),
            "last_deferred_selector_quality_failure":        bool(_sa.get("quality_failure", False)),
            # Fix 2: direct quote recovery audit
            "last_deferred_direct_quote_recovery_attempted": bool(_sa.get("direct_quote_recovery_attempted", False)),
            "last_deferred_direct_quote_recovery_selected":  bool(_sa.get("direct_quote_recovery_selected", False)),
        }
        if is_paper:
            try:
                from ap.deferred_breach_underlying_repair import build_paper_domain_fields
                _paper_fields = build_paper_domain_fields(
                    selector_audit=_sa,
                    broker_base_url=broker_base_url,
                )
                _patch.update(_paper_fields)
            except Exception as _pf_exc:
                log.debug("paper domain fields non-critical: %s", _pf_exc)
        return _patch
    except Exception:
        return {}


def _persist_deferred_selector_attempt_audit(
    osm,
    local_order_id: str,
    *,
    selector_audit: dict,
    attempt_number: int,
    execution_mode: str,
    is_paper: bool = False,
    broker_base_url: str = "",
) -> None:
    """
    Write flat Fix B + Fix C audit fields to orders.meta via a standalone
    update_order_meta call. Use only when no existing call is available to
    merge into. Best-effort — never raises.
    """
    try:
        _patch = _build_flat_selector_audit_fields(
            selector_audit=selector_audit,
            attempt_number=attempt_number,
            execution_mode=execution_mode,
            is_paper=is_paper,
            broker_base_url=broker_base_url,
        )
        _update = getattr(osm, "update_order_meta", None)
        if callable(_update) and local_order_id:
            _update(local_order_id, _patch)
    except Exception as _exc:
        log.debug("_persist_deferred_selector_attempt_audit non-critical: %s", _exc)


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
            _order_meta = order.get("meta") or {}
            if isinstance(_order_meta, str):
                try:
                    _order_meta = json.loads(_order_meta)
                except Exception:
                    _order_meta = {}
            if not isinstance(_order_meta, dict):
                _order_meta = {}
            _recovered_mode = str(
                order.get("execution_mode") or _order_meta.get("execution_mode") or ""
            ).strip().lower()
            if _recovered_mode not in ("live", "paper"):
                log.critical(
                    "[%s] Recovered OSM order %s has invalid execution_mode=%r",
                    watched.ticker, local_order_id, _recovered_mode,
                )
                return None
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
                execution_mode=_recovered_mode,
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
                metadata=dict(_order_meta),
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

    def resume_deferred_broker_ready_order(
        self,
        *,
        local_order_id: str,
        plan=None,
    ) -> dict:
        """Resume BROKER_READY through the canonical breach callback.

        The canonical entry-time gate stack (kill switch, exposure
        revalidation, fresh option quote, spread policy, drift policy,
        entry confirmation, account-size cap, submit-intent CAS, broker
        POST) lives inside ``_on_entry_trigger`` and its ~2000 lines of
        supporting infrastructure.  Reproducing that surface here would
        risk drift and duplicate submissions.

        The durable row is fenced before the normal callback is invoked, so
        recovery shares the production policy stack without a second copy.
        It returns an explicit disposition that keeps ownership durable:

          * TERMINAL_DURABLE  — only on a truthful boundary:
                * client identity mismatch
                * wrong kind
                * invalid status (not PENDING_TRIGGER/CREATED)
                * missing trigger evidence
                * malformed trigger timestamp
                * trigger older than DEFERRED_RECOVERY_MAX_TRIGGER_AGE_SECONDS
                * recovery attempt count exhausted
          * KEEP_WATCHER      — inspection could not complete (OSM
                                unavailable, row read raised, row is
                                already-submitted-family) — retain
                                current owner without change.
          * RETRY_WAIT / RECONCILE_PENDING — transient or ambiguous outcome.

        The method never mutates the row directly.  The caller
        (``ap_recovery.APStartupRecovery._recover_deferred_breach_lifecycles``)
        applies the disposition via existing OSM helpers, preserving
        broker_ready, contract, qty, limit, selector evidence, submit
        intent fields, client_id, execution_mode and diagnostics.

        Args:
            local_order_id: the durable row identifier.
            plan: optional prebuilt plan snapshot (ignored by scaffold,
                  reserved for the fully-gated future implementation).

        Returns:
            dict with keys: disposition, reason_code, terminal_status
            (when TERMINAL_DURABLE), next_retry_at (when RETRY_WAIT),
            attempt, max_attempts, owner, generation, local_order_id.
        """
        _now = datetime.now(timezone.utc)
        # Owner label used in returned metadata so downstream code can
        # attribute the recovery pass without additional lookups.
        _owner_label = f"recovery_scheduler:{self.client_id or self.email or ''}"
        _base = {
            "local_order_id": local_order_id,
            "owner": _owner_label,
            "attempt": None,
            "max_attempts": None,
            "configured_max_attempts": None,
            "effective_max_attempts": None,
            "remaining_recovery_window_seconds": None,
            "generation": None,
            "next_retry_at": None,
        }

        def _keep(reason: str) -> dict:
            return {**_base, "disposition": "KEEP_WATCHER", "reason_code": reason}

        def _term(reason: str, status: str = "EXPIRED", **extra) -> dict:
            return {
                **_base, **extra,
                "disposition": "TERMINAL_DURABLE",
                "reason_code": reason,
                "terminal_status": status,
            }

        # ── OSM reachability ────────────────────────────────────────
        osm = getattr(self, "order_state_machine", None) or getattr(self, "osm", None)
        if osm is None:
            return _keep("RECOVERY_OSM_UNAVAILABLE")

        # ── Row read ────────────────────────────────────────────────
        try:
            row = osm.get_order(local_order_id)
        except Exception as exc:
            log.error(
                "[%s] resume_deferred_broker_ready_order row_read_failed "
                "local_order_id=%s exc=%s",
                self.client_id, local_order_id, exc,
            )
            return _keep(f"RECOVERY_ROW_READ_ERROR:{type(exc).__name__}")
        if not isinstance(row, dict):
            return _keep("RECOVERY_ROW_MISSING")

        # ── Identity: client_id ─────────────────────────────────────
        row_client_id = str(row.get("client_id") or "").strip().lower()
        expected_client_id = str(self.client_id or self.email or "").strip().lower()
        if row_client_id and expected_client_id and row_client_id != expected_client_id:
            return _term("RECOVERY_CLIENT_ID_MISMATCH", status="ERROR")

        # ── Identity: kind ──────────────────────────────────────────
        kind = str(row.get("kind") or "").upper()
        if kind != "ENTRY":
            return _term(f"RECOVERY_WRONG_KIND:{kind}", status="ERROR")

        # ── Durable state: status ───────────────────────────────────
        status = str(row.get("status") or "").upper()
        if status not in {"PENDING_TRIGGER", "CREATED"}:
            # Any other status (SUBMITTED family, terminal family) means
            # this row is not eligible for recovery submit — reconciler
            # or order monitor owns it.
            return _keep(f"RECOVERY_STATUS_NOT_ELIGIBLE:{status}")

        # ── Durable state: already submitted (belt-and-suspenders) ──
        broker_order_id = str(row.get("broker_order_id") or "").strip()
        submitted_ts = row.get("submitted_ts")
        if broker_order_id or submitted_ts:
            return _keep("RECOVERY_ALREADY_SUBMITTED")

        # ── Meta hydration ──────────────────────────────────────────
        meta = row.get("meta") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        meta = meta or {}

        # ── Trigger evidence & age ──────────────────────────────────
        # Config with safe defaults.  The chosen defaults intentionally
        # bias to short windows — restart windows are usually well
        # under a minute, and a stale trigger should never become a
        # live submit.
        try:
            _max_trigger_age = int(os.getenv(
                "DEFERRED_RECOVERY_MAX_TRIGGER_AGE_SECONDS", "300"
            ))
        except (TypeError, ValueError):
            _max_trigger_age = 300
        try:
            _configured_max_attempts = int(os.getenv(
                "DEFERRED_RECOVERY_MAX_ATTEMPTS", "20"
            ))
        except (TypeError, ValueError):
            _configured_max_attempts = 20
        try:
            _retry_delay_seconds = int(os.getenv(
                "DEFERRED_RECOVERY_RETRY_DELAY_SECONDS", "30"
            ))
        except (TypeError, ValueError):
            _retry_delay_seconds = 30

        trigger_crossed_at = (
            meta.get("trigger_crossed_at")
            or meta.get("trigger_confirmed_at")
        )
        if not trigger_crossed_at:
            return _term("RECOVERY_MISSING_TRIGGER_EVIDENCE", status="EXPIRED")
        try:
            _crossed = datetime.fromisoformat(str(trigger_crossed_at))
            if _crossed.tzinfo is None:
                _crossed = _crossed.replace(tzinfo=timezone.utc)
            _trigger_age = (_now - _crossed).total_seconds()
        except Exception:
            return _term("RECOVERY_INVALID_TRIGGER_TIMESTAMP", status="EXPIRED")
        if _trigger_age > _max_trigger_age:
            return _term("RECOVERY_TRIGGER_TOO_OLD", status="EXPIRED")
        _remaining_recovery_window = max(0.0, float(_max_trigger_age) - max(0.0, _trigger_age))
        _effective_max_attempts = max(
            1,
            int(math.ceil(_remaining_recovery_window / max(1, _retry_delay_seconds))),
        )
        _max_attempts = max(1, min(_configured_max_attempts, _effective_max_attempts))

        # ── Retry exhaustion ────────────────────────────────────────
        try:
            _prior_attempts = int(meta.get("recovery_attempt_count") or 0)
        except (TypeError, ValueError):
            _prior_attempts = 0
        _attempt = _prior_attempts + 1
        if _attempt > _max_attempts:
            return _term(
                "RECOVERY_RETRY_EXHAUSTED", status="EXPIRED",
                attempt=_attempt, max_attempts=_max_attempts,
            )

        try:
            _generation = int(meta.get("materialization_generation") or 1)
        except (TypeError, ValueError):
            _generation = 1
        _base.update(
            attempt=_attempt,
            max_attempts=_max_attempts,
            configured_max_attempts=_configured_max_attempts,
            effective_max_attempts=_effective_max_attempts,
            remaining_recovery_window_seconds=int(_remaining_recovery_window),
            generation=_generation,
        )

        mode = str(row.get("execution_mode") or meta.get("execution_mode") or "").lower()
        if mode not in {"live", "paper"} or mode != str(self.execution_mode or "").lower():
            return _term("RECOVERY_EXECUTION_MODE_MISMATCH", status="ERROR")
        contract = str(row.get("contract") or "").strip()
        qty = int(row.get("qty") or 0)
        limit_price = float(row.get("limit_price") or 0)
        reserved_cost = float(row.get("reserved_cost") or 0)
        if not self._is_real_occ_contract(contract, str(row.get("symbol") or "")):
            return _term("RECOVERY_INVALID_OCC_CONTRACT", status="ERROR")
        if qty <= 0 or limit_price <= 0 or reserved_cost <= 0:
            return _term("RECOVERY_INVALID_DURABLE_PRICING", status="ERROR")
        selected_contract = str(meta.get("selected_contract") or "").strip()
        if not selected_contract or selected_contract != contract:
            return _term("RECOVERY_SELECTOR_PROOF_MISMATCH", status="ERROR")
        try:
            selected_limit = float(meta.get("selected_limit") or 0)
            selected_qty = int(meta.get("selected_qty") or 0)
        except (TypeError, ValueError):
            return _term("RECOVERY_SELECTOR_PROOF_MISMATCH", status="ERROR")
        if selected_limit <= 0.01 or selected_qty <= 0:
            return _term("RECOVERY_SELECTOR_PROOF_MISMATCH", status="ERROR")

        # The recovery path deliberately enters the same callback used by a
        # live watcher.  That keeps kill switches, exposure, quote/spread/drift,
        # confirmation, final-cap, durable identity and submit_existing_entry
        # policy in one place instead of maintaining a recovery clone.
        claim = getattr(osm, "claim_deferred_broker_ready_submit", None)
        if not callable(claim):
            return {
                **_base, "disposition": "RETRY_WAIT",
                "reason_code": "RECOVERY_SUBMIT_CLAIM_UNAVAILABLE",
                "next_retry_at": (_now + timedelta(seconds=_retry_delay_seconds)).isoformat(),
            }
        _claim_owner = f"recovery_submit:{local_order_id}:{uuid.uuid4().hex[:12]}"
        if not claim(local_order_id, owner=_claim_owner, generation=_generation):
            # A peer, submit intent, status transition, or generation change won
            # the CAS. Re-read on the next scheduler pass; never POST here.
            return {
                **_base, "disposition": "RECONCILE_PENDING",
                "reason_code": "RECOVERY_SUBMIT_CLAIM_NOT_ACQUIRED",
                "owner": _claim_owner,
                "next_retry_at": (_now + timedelta(seconds=_retry_delay_seconds)).isoformat(),
            }

        recovered_plan = plan or SimpleNamespace(
            plan_id=str(row.get("plan_id") or local_order_id),
            signal_id=str(row.get("signal_id") or meta.get("signal_id") or ""),
            client_id=str(row.get("client_id") or ""), execution_mode=mode,
            ticker=str(row.get("symbol") or ""),
            side=str(row.get("direction") or meta.get("side") or "CALL").upper(),
            direction=str(row.get("direction") or meta.get("side") or "CALL").upper(),
            contracts=qty, limit_price=limit_price, max_position_usd=reserved_cost,
            contract_symbol=contract,
            trigger_price=float(meta.get("trigger_price") or row.get("trigger_price") or 0),
            stop_underlying=meta.get("stop_underlying") or meta.get("stop_price"),
            target_underlying=meta.get("target_underlying") or meta.get("target_price"),
            metadata=dict(meta), pattern=str(meta.get("pattern") or ""),
            timeframe=str(meta.get("timeframe") or "1d"), tier=str(meta.get("tier") or "B"),
            score=float(meta.get("score") or 0), trigger_type="breach",
        )
        if not isinstance(getattr(recovered_plan, "metadata", None), dict):
            recovered_plan.metadata = {}
        recovered_plan.metadata.update({
            "contract_deferred": False,
            "recovery_submit_owner": _claim_owner,
            "recovery_submit_generation": _generation,
            "recovery_submit_fenced": True,
        })
        signal = {
            "signal_id": recovered_plan.signal_id, "local_order_id": local_order_id,
            "client_id": recovered_plan.client_id, "execution_mode": mode,
            "ticker": recovered_plan.ticker, "side": recovered_plan.side,
            "entry_price": recovered_plan.trigger_price,
            "stop_price": recovered_plan.stop_underlying,
            "target_price": recovered_plan.target_underlying,
            "contract_symbol": contract, "contracts": qty,
            "limit_price": limit_price, "reserved_cost": reserved_cost,
            "contract_deferred": False, "_approved_plan": recovered_plan,
            "recovery_submit_owner": _claim_owner,
            "recovery_submit_generation": _generation,
        }
        crossed = datetime.fromisoformat(str(trigger_crossed_at))
        if crossed.tzinfo is None:
            crossed = crossed.replace(tzinfo=timezone.utc)
        watched = SimpleNamespace(
            signal=signal, ticker=recovered_plan.ticker, side=recovered_plan.side,
            trigger_price=recovered_plan.trigger_price,
            entry_trigger=recovered_plan.trigger_price,
            stop_level=recovered_plan.stop_underlying,
            target_price=recovered_plan.target_underlying,
            trigger_crossed_at=crossed, triggered_at=crossed,
            breach_price=float(meta.get("observed_underlying_price") or recovered_plan.trigger_price or 0),
        )
        try:
            self._on_entry_trigger(watched)
        except Exception as exc:
            log.exception("[%s] recovered canonical entry callback failed", recovered_plan.ticker)
            return {
                **_base, "disposition": "RETRY_WAIT",
                "reason_code": f"RECOVERY_CANONICAL_GATES_EXCEPTION:{type(exc).__name__}",
                "next_retry_at": (_now + timedelta(seconds=_retry_delay_seconds)).isoformat(),
            }

        after = osm.get_order(local_order_id) or {}
        after_status = str(after.get("status") or "").upper()
        after_meta = after.get("meta") or {}
        if isinstance(after_meta, str):
            try: after_meta = json.loads(after_meta)
            except Exception: after_meta = {}
        if after.get("broker_order_id") and after_status in {"SUBMITTED", "ACK", "ACKNOWLEDGED", "PARTIAL", "PARTIAL_FILL", "FILLED"}:
            return {**_base, "disposition": "SUBMITTED", "reason_code": "RECOVERY_CANONICAL_SUBMIT_ACCEPTED", "broker_order_id": after.get("broker_order_id")}
        if after_meta.get("submit_intent_at"):
            return {**_base, "disposition": "RECONCILE_PENDING", "reason_code": "RECOVERY_SUBMIT_INTENT_REQUIRES_RECONCILIATION"}
        if after_status in {"REJECTED", "EXPIRED", "CANCELED", "ERROR"}:
            return {**_base, "disposition": "TERMINAL_DURABLE", "reason_code": str(after.get("last_error") or "RECOVERY_CANONICAL_GATE_REJECTED"), "terminal_status": after_status}
        return {
            **_base, "disposition": "RETRY_WAIT",
            "reason_code": "RECOVERY_CANONICAL_GATES_NO_DURABLE_OUTCOME",
            "next_retry_at": (_now + timedelta(seconds=_retry_delay_seconds)).isoformat(),
        }

    def resume_deferred_materialization_retry(
        self,
        *,
        local_order_id: str,
        expected_generation: int,
        expected_retry_attempt: int,
        owner: str,
    ) -> dict:
        """Canonical consumer for a due deferred-materialization retry.

        P0 AMENDMENT (fix/deferred-retry-due-execution-p0) — HARDENED
        ---------------------------------------------------------------
        Blocker §1: Fail closed on every required identity field.
          - direction must be CALL or PUT in the durable row; never default to CALL.
          - trigger_crossed_at must be present and parseable; never substitute _now.
          - signal_id, plan_id, client_id must be non-empty from the durable row.
        Blocker §2: Exact identity proof on every field.
          - A missing row field when the expected field is provided FAILS, not passes.
        Blocker §5: Atomic attempt advancement.
          - claim_deferred_materialization receives retry_attempt so the durable
            row always shows the in-flight attempt, even on mid-execution crash.
          - Every RETRY_WAIT return calls schedule_deferred_materialization_retry
            before returning so the durable row is in a verifiable RETRY_WAIT state,
            not MATERIALIZING until lease expires.

        Never defaults direction, client_id, plan_id, or trigger_crossed_at.
        Never calls the broker directly. Never duplicates selector logic.
        """
        _now = datetime.now(timezone.utc)
        _base: dict = {
            "local_order_id": local_order_id,
            "owner": str(owner or ""),
            "attempt": None,
            "max_attempts": None,
            "generation": None,
            "next_retry_at": None,
        }

        def _keep(reason: str) -> dict:
            return {**_base, "disposition": "KEEP_WATCHER", "reason_code": reason}

        def _term(reason: str, status: str = "EXPIRED", **extra) -> dict:
            # P0 FINAL AMENDMENT: return TERMINAL_REQUIRED (not TERMINAL_DURABLE)
            # so recovery uses the fenced terminalize_deferred_retry_if_unchanged()
            # CAS rather than the broad terminalize_deferred_breach(). The expected
            # state fields enable the fenced predicate to refuse writes when a
            # concurrent worker has already advanced generation, attempt, lifecycle,
            # broker_ready, or submit_intent.
            return {
                **_base, **extra,
                "disposition": "TERMINAL_REQUIRED",
                "reason_code": reason,
                "terminal_status": status,
                # Fencing fields: exact state the row must still be in for the
                # terminal CAS to succeed. Use the caller's expectation
                # (_expected_generation, _expected_attempt) since these are
                # locked in at call time and never change within the method.
                "expected_generation": _expected_generation,
                "expected_prior_retry_attempt": _expected_attempt - 1,
                "expected_client_id": (
                    str(getattr(self, "client_id", "") or getattr(self, "email", "") or "").strip().lower()
                ),
                "expected_execution_mode": (
                    str(getattr(self, "execution_mode", "") or getattr(self, "mode", "") or "").strip().lower()
                ),
                "expected_lifecycle_state": "RETRY_WAIT",
                "expected_materialization_status": "RETRY_PENDING",
            }

        def _claim_lost(reason: str) -> dict:
            return {**_base, "disposition": "CLAIM_LOST", "reason_code": reason}

        osm = getattr(self, "order_state_machine", None) or getattr(self, "osm", None)
        if osm is None:
            return _keep("RETRY_OSM_UNAVAILABLE")

        try:
            _expected_generation = int(expected_generation)
            _expected_attempt = int(expected_retry_attempt)
        except (TypeError, ValueError):
            _expected_generation = 0
            _expected_attempt = 0
            return _keep("RETRY_INVALID_EXPECTATIONS")
        if not owner or _expected_generation < 1 or _expected_attempt < 1:
            return _term("RETRY_INVALID_EXPECTATIONS", status="ERROR")

        # ── Row read ─────────────────────────────────────────────────
        try:
            row = osm.get_order(local_order_id)
        except Exception as exc:
            log.error("[%s] resume_deferred_materialization_retry row_read_failed "
                      "local_order_id=%s exc=%s", self.client_id, local_order_id, exc)
            return _keep(f"RETRY_ROW_READ_ERROR:{type(exc).__name__}")
        if not isinstance(row, dict):
            return _keep("RETRY_ROW_MISSING")

        # ── Status gates ────────────────────────────────────────────
        status = str(row.get("status") or "").upper()
        if status != "PENDING_TRIGGER":
            return _keep(f"RETRY_STATUS_NOT_ELIGIBLE:{status}")
        if str(row.get("broker_order_id") or "").strip():
            return _keep("RETRY_ALREADY_SUBMITTED")
        if row.get("submitted_ts"):
            return _keep("RETRY_ALREADY_SUBMITTED")

        # ── BLOCKER §1 + §2: fail-closed identity proof ─────────────
        # client_id: both must be present and must match.
        row_client = str(row.get("client_id") or "").strip().lower()
        expected_client = str(self.client_id or self.email or "").strip().lower()
        if not row_client:
            return _term("RETRY_MISSING_CLIENT_ID", status="ERROR")
        if not expected_client:
            return _term("RETRY_RUNNER_CLIENT_ID_MISSING", status="ERROR")
        if row_client != expected_client:
            return _term("RETRY_CLIENT_ID_MISMATCH", status="ERROR")

        # execution_mode: row must contain a valid mode and must match runner.
        row_mode = str(row.get("execution_mode") or "").strip().lower()
        expected_mode = str(self.execution_mode or self.mode or "").strip().lower()
        if row_mode not in {"live", "paper"}:
            return _term("RETRY_INVALID_EXECUTION_MODE", status="ERROR")
        if expected_mode not in {"live", "paper"}:
            return _term("RETRY_RUNNER_EXECUTION_MODE_INVALID", status="ERROR")
        if row_mode != expected_mode:
            return _term("RETRY_EXECUTION_MODE_MISMATCH", status="ERROR")

        meta = row.get("meta") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        meta = meta or {}

        # signal_id: required — the CAS predicate needs it.
        signal_id = str(row.get("signal_id") or meta.get("signal_id") or "").strip()
        if not signal_id:
            return _term("RETRY_MISSING_SIGNAL_ID", status="ERROR")

        # plan_id: required for plan reconstruction.
        plan_id = str(row.get("plan_id") or meta.get("plan_id") or "").strip()
        if not plan_id:
            return _term("RETRY_MISSING_PLAN_ID", status="ERROR")

        # direction: must be CALL or PUT — never default.
        raw_direction = str(row.get("direction") or meta.get("side") or meta.get("direction") or "").strip().upper()
        if raw_direction not in {"CALL", "PUT"}:
            return _term(f"RETRY_MISSING_OR_INVALID_DIRECTION:got={raw_direction!r}", status="ERROR")
        direction = raw_direction

        # ticker: required.
        ticker = str(row.get("symbol") or meta.get("ticker") or meta.get("symbol") or "").strip().upper()
        if not ticker:
            return _term("RETRY_MISSING_TICKER", status="ERROR")

        # trigger_crossed_at: must be present and parseable — never substitute now.
        trigger_crossed_at_raw = (
            meta.get("trigger_crossed_at") or meta.get("triggered_at")
        )
        if not trigger_crossed_at_raw:
            return _term("RETRY_MISSING_TRIGGER_CROSSED_AT", status="ERROR")
        try:
            trigger_crossed_dt = datetime.fromisoformat(str(trigger_crossed_at_raw))
            if trigger_crossed_dt.tzinfo is None:
                trigger_crossed_dt = trigger_crossed_dt.replace(tzinfo=timezone.utc)
        except Exception:
            return _term("RETRY_INVALID_TRIGGER_CROSSED_AT", status="ERROR")

        # trigger_price: must be positive.
        _trigger_price_raw = row.get("trigger_price") or meta.get("trigger_price")
        if _trigger_price_raw is None:
            return _term("RETRY_MISSING_TRIGGER_PRICE", status="ERROR")
        try:
            trigger_price = float(_trigger_price_raw)
        except (TypeError, ValueError):
            return _term("RETRY_INVALID_TRIGGER_PRICE", status="ERROR")
        if trigger_price <= 0:
            return _term("RETRY_ZERO_TRIGGER_PRICE", status="ERROR")

        # ── Fencing counters ─────────────────────────────────────────
        try:
            durable_generation = int(meta.get("materialization_generation") or 0)
        except (TypeError, ValueError):
            durable_generation = 0
        if durable_generation != _expected_generation:
            return _claim_lost(
                f"RETRY_GENERATION_ADVANCED:durable={durable_generation}:expected={_expected_generation}"
            )
        try:
            durable_prior_attempt = int(meta.get("retry_attempt") or 0)
        except (TypeError, ValueError):
            durable_prior_attempt = 0
        if durable_prior_attempt != _expected_attempt - 1:
            return _claim_lost(
                f"RETRY_ATTEMPT_ADVANCED:durable={durable_prior_attempt}:expected_prior={_expected_attempt - 1}"
            )

        # ── Amendment 1 (rev 2): configured env is the hard ceiling ──────────
        # Policy: MAX_BREACH_SELECTOR_RETRIES is the sole authoritative bound.
        # Durable retry_max_attempts is read only to detect stale rows and is
        # NEVER allowed to raise max_attempts above configured_max.
        #
        # Rationale: "max(configured, durable)" allowed any value in the
        # JSONB metadata — including stale, corrupted, or experimentally high
        # values such as 50, 500, 999999 — to silently override the production
        # policy.  There is no authenticated operator-override path; trusting
        # arbitrary database metadata to define retry capacity creates an
        # unbounded recovery lifecycle.
        #
        # Hard ceiling examples:
        #   durable=3, env=5  → 5   (env raises the floor, as before)
        #   durable=7, env=5  → 5   (durable above env is CLAMPED, not honoured)
        #   durable=500, env=5→ 5   (corrupted/stale value clamped)
        #   durable missing   → 5   (env is sole authority)
        #   malformed durable → 5   (safe default)
        #   negative durable  → 5   (clamped to zero, then env wins)
        try:
            _configured_max = _positive_int_env_config(
                "MAX_BREACH_SELECTOR_RETRIES", 5
            )
        except (TypeError, ValueError):
            _configured_max = 5
        try:
            _durable_raw = int(meta.get("retry_max_attempts") or 0)
        except (TypeError, ValueError):
            _durable_raw = 0
        # Clamp durable to [0, configured_max].  Values above configured_max
        # are NOT honoured — they are artifacts of stale or corrupted metadata.
        _durable_max = max(0, min(_configured_max, _durable_raw))
        # The resolved maximum is always the configured env value.
        # _durable_max is clamped but does not raise above configured_max.
        max_attempts = _configured_max
        if _expected_attempt > max_attempts:
            return _term("RETRY_MAX_ATTEMPTS_EXCEEDED", status="EXPIRED",
                         attempt=_expected_attempt, max_attempts=max_attempts)

        durable_due_at_raw = (
            meta.get("materialization_next_retry_at")
            or meta.get("deferred_retry_next_attempt_at")
            or meta.get("next_retry_at")
        )
        if not durable_due_at_raw:
            return _keep("RETRY_NO_DURABLE_SCHEDULE")
        try:
            durable_due_at = datetime.fromisoformat(str(durable_due_at_raw))
            if durable_due_at.tzinfo is None:
                durable_due_at = durable_due_at.replace(tzinfo=timezone.utc)
        except Exception:
            return _term("RETRY_INVALID_DURABLE_SCHEDULE", status="ERROR")
        if durable_due_at > _now:
            return {**_base, "disposition": "NOT_DUE", "reason_code": "RETRY_NOT_DUE"}

        # ── Blocker §5: enforce absolute_entry_deadline before ANY claim ──
        # An expired deadline must terminalize the row here — before the CAS,
        # before selector work, before _on_entry_trigger. Downstream LIVE
        # gates may provide partial defense, but the lifecycle consumer must
        # honor its own durable deadline. PAPER retries of stale setups must
        # also be blocked.
        _deadline_raw = (
            meta.get("absolute_entry_deadline")
            or meta.get("retry_deadline")
            or meta.get("deferred_retry_deadline")
        )
        if _deadline_raw:
            try:
                _deadline_dt = datetime.fromisoformat(str(_deadline_raw))
                if _deadline_dt.tzinfo is None:
                    _deadline_dt = _deadline_dt.replace(tzinfo=timezone.utc)
                if _now >= _deadline_dt:
                    return _term(
                        "RETRY_DEADLINE_EXHAUSTED",
                        status="EXPIRED",
                        attempt=_expected_attempt,
                        max_attempts=max_attempts,
                    )
            except Exception:
                # Unparseable deadline — fail closed: treat as expired
                # rather than silently allowing a potentially stale retry.
                return _term(
                    "RETRY_INVALID_DEADLINE",
                    status="EXPIRED",
                    attempt=_expected_attempt,
                    max_attempts=max_attempts,
                )

        # ── Policy fields: required from the durable row — no silent defaults ─
        # score, tier, and timeframe affect expiration/playbook selection,
        # sizing gate thresholds, and contract scoring. Manufacturing them
        # inside a broker-capable recovery path is unsafe.
        _score_raw = row.get("score") if row.get("score") is not None else meta.get("score")
        if _score_raw is None:
            return _term("RETRY_MISSING_SCORE", status="ERROR")
        try:
            score = float(_score_raw)
        except (TypeError, ValueError):
            return _term("RETRY_INVALID_SCORE", status="ERROR")

        _tier = str(row.get("tier") or meta.get("tier") or "").strip()
        if not _tier:
            return _term("RETRY_MISSING_TIER", status="ERROR")

        _timeframe = str(row.get("timeframe") or meta.get("timeframe") or "").strip()
        if not _timeframe:
            return _term("RETRY_MISSING_TIMEFRAME", status="ERROR")

        # ── Fenced CAS (blocker §5: also stamps retry_attempt atomically) ─
        _new_generation = _expected_generation + 1
        try:
            _retry_lock_ttl = int(os.getenv("DEFERRED_MATERIALIZATION_LOCK_TTL_SECONDS", "120"))
        except (TypeError, ValueError):
            _retry_lock_ttl = 120
        _lease_until = (_now + timedelta(seconds=_retry_lock_ttl)).isoformat()

        claim = getattr(osm, "claim_deferred_materialization", None)
        if not callable(claim):
            return _keep("RETRY_CLAIM_UNAVAILABLE")
        try:
            claimed = bool(claim(
                local_order_id,
                owner=owner,
                new_generation=_new_generation,
                lease_until=_lease_until,
                trigger_crossed_at=trigger_crossed_at_raw,
                trigger_price=trigger_price,
                observed_underlying_price=float(
                    meta.get("observed_underlying_price")
                    or meta.get("triggered_underlying_price") or 0
                ),
                signal_id=signal_id,
                execution_mode=row_mode,
                retry_attempt=_expected_attempt,
            ))
        except Exception as exc:
            log.error("[%s] resume_deferred_materialization_retry claim_failed "
                      "local_order_id=%s exc=%s", self.client_id, local_order_id, exc)
            return _keep(f"RETRY_CLAIM_EXCEPTION:{type(exc).__name__}")
        if not claimed:
            return _claim_lost("RETRY_CLAIM_NOT_ACQUIRED")

        _base.update(attempt=_expected_attempt, max_attempts=max_attempts, generation=_new_generation)

        # ── Helper: durable RETRY_WAIT schedule (blocker §5) ────────
        # Every RETRY_WAIT return MUST call this so the durable row
        # transitions out of MATERIALIZING before we return — never
        # leave the row stranded at MATERIALIZING until lease expiry.
        try:
            _retry_delay = _positive_int_env_config(
                "BREACH_SELECTOR_RETRY_DELAY_SECONDS", 8
            )
        except (TypeError, ValueError):
            _retry_delay = 20

        def _schedule_retry_wait(reason_code: str, selector_failure: dict | None = None) -> dict:
            """Write a durable RETRY_WAIT row; return truthful disposition.

            P0 blocker §3: RETRY_WAIT is only returned when the durable write
            is confirmed. On write failure, RETRY_SCHEDULE_FAILED is returned
            so recovery can retain ownership and re-attempt on the next pass.
            Recovery must never treat a failed schedule as durably owned.
            """
            _next_retry_at = (_now + timedelta(seconds=_retry_delay)).isoformat()
            _schedule = getattr(osm, "schedule_deferred_materialization_retry", None)
            _ok = False
            if callable(_schedule):
                try:
                    _schedule_meta = _build_deferred_retry_schedule_meta(
                        reason_code=reason_code,
                        selector_audit=selector_failure or {},
                        attempt=_expected_attempt,
                        max_attempts=max_attempts,
                        delay_seconds=_retry_delay,
                        client_id=self.client_id,
                        execution_mode=row_mode,
                        local_order_id=local_order_id,
                        signal_id=signal_id,
                        now=_now,
                    )
                    _ok = bool(_schedule(
                        local_order_id,
                        owner=owner,
                        generation=_new_generation,
                        reason_code=reason_code,
                        attempt=_expected_attempt,
                        max_attempts=max_attempts,
                        next_retry_at=_next_retry_at,
                        selector_failure={
                            **(selector_failure or {}),
                            **_schedule_meta,
                        },
                        signal_id=signal_id,
                        execution_mode=row_mode,
                    ))
                except Exception as _sch_exc:
                    log.critical(
                        "[%s] resume_deferred_materialization_retry "
                        "schedule_retry_wait RAISED local_order_id=%s exc=%s "
                        "— row stranded at MATERIALIZING",
                        self.client_id, local_order_id, _sch_exc,
                    )
            if not _ok:
                log.critical(
                    "[%s] resume_deferred_materialization_retry "
                    "schedule_retry_wait FAILED local_order_id=%s "
                    "— row stranded at MATERIALIZING; recovery must retain ownership",
                    self.client_id, local_order_id,
                )
                # Return RETRY_SCHEDULE_FAILED so recovery calls
                # _retain_recovery_ownership and does NOT claim the row is
                # durably rescheduled. The next health-loop pass will see the
                # row still at MATERIALIZING (or after lease expiry, RETRY_WAIT
                # will be re-attempted).
                return {
                    **_base,
                    "disposition": "RETRY_SCHEDULE_FAILED",
                    "reason_code": f"RETRY_SCHEDULE_WRITE_FAILED:{reason_code}",
                }
            return {
                **_base,
                "disposition": "RETRY_WAIT",
                "reason_code": reason_code,
                "next_retry_at": _next_retry_at,
            }

        # ── Preserve prior attempt diagnostics ───────────────────────
        try:
            prior_history = meta.get("materialization_attempt_history") or []
            if not isinstance(prior_history, list):
                prior_history = []
            prior_diagnostics = {
                "attempt": durable_prior_attempt,
                "generation": durable_generation,
                "started_at": meta.get("materialization_started_at"),
                "completed_at": meta.get("selector_completed_at"),
                "reason_code": (
                    meta.get("retry_reason") or meta.get("materialization_reason")
                    or meta.get("deferred_retry_reason_code")
                ),
                "selector_request_diagnostics": meta.get("materialization_selector_failure") or {},
            }
            new_history = prior_history + [prior_diagnostics]
            update_meta = getattr(osm, "update_order_meta", None)
            if callable(update_meta):
                update_meta(local_order_id, {
                    "materialization_attempt_history": new_history,
                    "selector_request_counters_reset_at": _now.isoformat(),
                    "materialization_current_attempt": _expected_attempt,
                    "materialization_current_generation": _new_generation,
                })
        except Exception as _hist_exc:
            log.warning("[%s] resume_deferred_materialization_retry history_write_failed "
                        "local_order_id=%s exc=%s (non-fatal)",
                        self.client_id, local_order_id, _hist_exc)

        # ── Build plan — all fields from durable row, no defaults ───
        recovered_plan = SimpleNamespace(
            plan_id=plan_id,
            signal_id=signal_id,
            client_id=row_client,
            execution_mode=row_mode,
            ticker=ticker,
            side=direction,
            direction=direction,
            score=score,
            tier=_tier,
            trigger_price=trigger_price,
            stop_underlying=(
                row.get("stop_underlying") if row.get("stop_underlying") is not None
                else meta.get("stop_underlying")
            ),
            target_underlying=(
                row.get("target_underlying") if row.get("target_underlying") is not None
                else meta.get("target_underlying")
            ),
            contract_symbol=str(row.get("contract") or ""),
            pattern=str(row.get("pattern") or meta.get("pattern") or ""),
            timeframe=_timeframe,
            strategy_type=str(meta.get("strategy_type") or ""),
            prior_day_high=meta.get("prior_day_high"),
            prior_day_low=meta.get("prior_day_low"),
            contracts=int(row.get("qty") or 0),
            limit_price=float(row.get("limit_price") or 0),
            max_position_usd=float(row.get("reserved_cost") or 0),
            metadata={
                **dict(meta),
                "contract_deferred": True,
                "materialization_generation": _new_generation,
                "materialization_owner": owner,
                "materialization_retry_owner": owner,
                "materialization_retry_attempt": _expected_attempt,
                "materialization_retry_max_attempts": max_attempts,
                "breach_attempt_count": _expected_attempt - 1,
                "deferred_breach_selection": True,
                "selection_context": "deferred_breach_retry",
                "selector_request_counters_reset_at": _now.isoformat(),
                "selector_request_direct_quote_calls_reset": True,
                "selector_request_chain_calls_reset": True,
                "selector_request_expiration_calls_reset": True,
            },
        )
        signal = {
            "signal_id": signal_id,
            "local_order_id": local_order_id,
            "client_id": row_client,
            "execution_mode": row_mode,
            "ticker": ticker,
            "side": direction,
            "entry_price": trigger_price,
            "stop_price": recovered_plan.stop_underlying,
            "target_price": recovered_plan.target_underlying,
            "contract_symbol": recovered_plan.contract_symbol,
            "contract_deferred": True,
            "_approved_plan": recovered_plan,
            "materialization_retry_owner": owner,
            "materialization_retry_attempt": _expected_attempt,
            "ownership_kind": "materialization_retry",
            "owner": owner,
            "materialization_generation": _new_generation,
            "retry_attempt": _expected_attempt,
            "fenced": True,
            "recovery_submit_fenced": True,
            "recovery_submit_owner": owner,
            "recovery_submit_generation": _new_generation,
        }
        watched = SimpleNamespace(
            signal=signal, ticker=ticker, side=direction,
            trigger_price=trigger_price,
            entry_trigger=trigger_price,
            stop_level=recovered_plan.stop_underlying,
            target_price=recovered_plan.target_underlying,
            trigger_crossed_at=trigger_crossed_dt,
            triggered_at=trigger_crossed_dt,
            breach_price=float(
                meta.get("observed_underlying_price") or trigger_price or 0
            ),
        )

        try:
            # Set verified pre-claim markers so _on_entry_trigger's deferred
            # claim block bypasses its second claim and proceeds directly to
            # the selector + canonical submit continuation. The bypass is
            # verified inside _on_entry_trigger against the durable row —
            # the markers alone are not trusted; the durable row must confirm
            # lifecycle=MATERIALIZING with matching owner/generation/attempt.
            watched.signal.update({
                "_recovery_pre_claimed":             True,
                "_recovery_pre_claimed_owner":       owner,
                "_recovery_pre_claimed_generation":  _new_generation,
                "_recovery_pre_claimed_attempt":     _expected_attempt,
                "_recovery_pre_claimed_client_id":   row_client,
                "_recovery_pre_claimed_mode":        row_mode,
            })
            self._on_entry_trigger(watched)
        except Exception as exc:
            log.exception("[%s] resume_deferred_materialization_retry canonical_callback_failed "
                          "local_order_id=%s", ticker or local_order_id, exc)
            # Blocker §5: schedule a durable RETRY_WAIT before returning
            # so the row does not remain MATERIALIZING until lease expiry.
            return _schedule_retry_wait(
                f"RETRY_CANONICAL_CALLBACK_EXCEPTION:{type(exc).__name__}",
                {"callback_exception": str(exc)[:200]},
            )

        # ── Re-read to determine outcome ─────────────────────────────
        try:
            after = osm.get_order(local_order_id) or {}
        except Exception as exc:
            return _keep(f"RETRY_POST_CALLBACK_READ_ERROR:{type(exc).__name__}")
        after_status = str(after.get("status") or "").upper()
        after_meta = after.get("meta") or {}
        if isinstance(after_meta, str):
            try:
                after_meta = json.loads(after_meta)
            except Exception:
                after_meta = {}

        if after.get("broker_order_id") and after_status in {
            "SUBMITTED", "ACK", "ACKNOWLEDGED", "PARTIAL", "PARTIAL_FILL", "FILLED"
        }:
            return {**_base, "disposition": "SUBMITTED",
                    "reason_code": "RETRY_CANONICAL_SUBMIT_ACCEPTED",
                    "broker_order_id": after.get("broker_order_id")}
        if after_meta.get("broker_ready") is True and after_status == "PENDING_TRIGGER":
            return {**_base, "disposition": "BROKER_READY",
                    "reason_code": "RETRY_CANONICAL_BROKER_READY"}
        after_lifecycle = str(after_meta.get("lifecycle_state") or "").upper()
        after_next_retry = (
            after_meta.get("materialization_next_retry_at")
            or after_meta.get("next_retry_at")
        )
        if after_lifecycle == "RETRY_WAIT" and after_next_retry:
            return {**_base, "disposition": "RETRY_WAIT",
                    "reason_code": str(after_meta.get("retry_reason")
                                       or after_meta.get("materialization_reason")
                                       or "RETRY_RESCHEDULED"),
                    "next_retry_at": str(after_next_retry)}
        # P0 FINAL AMENDMENT: use TERMINAL_ALREADY_DURABLE (not TERMINAL_DURABLE)
        # when the canonical downstream path already wrote a terminal status.
        # Recovery must NOT call terminalize_deferred_retry_if_unchanged() again —
        # the row is already terminal, a second write is unsafe.
        if after_status in {"REJECTED", "EXPIRED", "CANCELED", "ERROR"}:
            return {
                **_base,
                "disposition": "TERMINAL_ALREADY_DURABLE",
                "reason_code": str(after.get("last_error") or after_meta.get("final_reason")
                                   or "RETRY_CANONICAL_TERMINALIZED"),
                "terminal_status": after_status,
                "generation": _new_generation,
                "attempt": _expected_attempt,
            }
        # No durable outcome — schedule retry before returning (blocker §5).
        return _schedule_retry_wait("RETRY_CANONICAL_NO_DURABLE_OUTCOME")
    def reconcile_deferred_broker_intent(
        self,
        *,
        local_order_id: str,
    ) -> dict:
        """Reconcile a submit-intent crash window against broker truth.

        The submit path persists ``submit_intent_at`` and the Tradier
        idempotency tag (``broker_submit_key`` = local_order_id) BEFORE
        any broker bytes leave the process.  If the process crashes after
        the broker accepted the order but before ``broker_order_id`` was
        committed to the row, the durable row shows:

            submit_intent_at present, broker_order_id absent

        while a LIVE order may exist at the broker.  This is the
        double-submit hazard: naively "resuming" such a row (the §2 path)
        could place a second live order for Jason.

        The broker account order list is queried by the durable tag and the
        result is strongly checked against contract, side and quantity before
        an existing order is adopted. Ambiguity remains fail-closed.

          * ALREADY_RECONCILED — broker_order_id already present; the
            order monitor owns the row.  (Defensive; the recovery load
            filter normally excludes these.)
          * NOT_IN_CRASH_WINDOW — no submit_intent_at; the row never
            reached the broker-submit boundary and is safe for the normal
            resume path.
          * RECONCILE_PENDING — broker truth is unavailable or ambiguous.
          * KEEP_WATCHER — inspection could not complete (OSM unavailable,
            row read raised, row missing).

        This method never POSTs. It only reads broker truth and adopts an exact
        match through the existing order state machine.
        """
        _owner_label = f"broker_reconciler:{self.client_id or self.email or ''}"
        _base = {
            "local_order_id": local_order_id,
            "owner": _owner_label,
            "submit_intent_at": None,
            "broker_submit_key": None,
        }

        def _keep(reason: str) -> dict:
            return {**_base, "disposition": "KEEP_WATCHER", "reason_code": reason}

        osm = getattr(self, "order_state_machine", None) or getattr(self, "osm", None)
        if osm is None:
            return _keep("RECONCILE_OSM_UNAVAILABLE")
        try:
            row = osm.get_order(local_order_id)
        except Exception as exc:
            log.error(
                "[%s] reconcile_deferred_broker_intent row_read_failed "
                "local_order_id=%s exc=%s",
                self.client_id, local_order_id, exc,
            )
            return _keep(f"RECONCILE_ROW_READ_ERROR:{type(exc).__name__}")
        if not isinstance(row, dict):
            return _keep("RECONCILE_ROW_MISSING")

        # Identity: client_id (never touch another client's row)
        row_client_id = str(row.get("client_id") or "").strip().lower()
        expected_client_id = str(self.client_id or self.email or "").strip().lower()
        if row_client_id and expected_client_id and row_client_id != expected_client_id:
            return _keep("RECONCILE_CLIENT_ID_MISMATCH")
        row_mode = str(row.get("execution_mode") or "").strip().lower()
        expected_mode = str(getattr(self, "execution_mode", None) or getattr(self, "mode", None) or "").strip().lower()
        if row_mode and expected_mode and row_mode != expected_mode:
            return _keep("RECONCILE_EXECUTION_MODE_MISMATCH")

        broker_order_id = str(row.get("broker_order_id") or "").strip()
        meta = row.get("meta") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        meta = meta or {}
        submit_intent_at = meta.get("submit_intent_at")
        broker_submit_key = meta.get("broker_submit_key")
        _base["submit_intent_at"] = submit_intent_at
        _base["broker_submit_key"] = broker_submit_key

        # ── Already has a broker order → adopted, not our concern ────
        if broker_order_id:
            return {
                **_base,
                "disposition": "ALREADY_RECONCILED",
                "reason_code": "RECONCILE_BROKER_ORDER_PRESENT",
                "broker_order_id": broker_order_id,
            }

        # ── No submit intent → not a crash-window row ────────────────
        if not submit_intent_at:
            return {
                **_base,
                "disposition": "NOT_IN_CRASH_WINDOW",
                "reason_code": "RECONCILE_NO_SUBMIT_INTENT",
            }

        broker = getattr(self, "broker", None)
        list_orders = getattr(broker, "list_orders", None)
        if not callable(list_orders):
            return {**_base, "disposition": "RECONCILE_PENDING", "reason_code": "RECONCILE_BROKER_QUERY_UNAVAILABLE"}
        try:
            broker_orders = list_orders()
        except Exception as exc:
            return {**_base, "disposition": "RECONCILE_PENDING", "reason_code": f"RECONCILE_BROKER_QUERY_FAILED:{type(exc).__name__}"}

        tag = canonical_broker_submit_key(broker_submit_key or local_order_id)
        exact_tag = [o for o in broker_orders if isinstance(o, dict) and str(o.get("tag") or "") == tag]
        expected_contract = str(row.get("contract") or "")
        expected_qty = int(row.get("qty") or 0)
        strong = [
            o for o in exact_tag
            if str(o.get("option_symbol") or o.get("contract") or o.get("symbol") or "") == expected_contract
            and str(o.get("side") or "").lower() == "buy_to_open"
            and int(float(o.get("quantity") or 0)) == expected_qty
        ]
        if len(exact_tag) > 1 or len(strong) > 1:
            return {**_base, "disposition": "RECONCILE_PENDING", "reason_code": "RECONCILE_MULTIPLE_MATCHES", "match_count": len(exact_tag)}
        if exact_tag and not strong:
            return {**_base, "disposition": "RECONCILE_PENDING", "reason_code": "RECONCILE_TAG_IDENTITY_MISMATCH"}
        if not strong:
            # A broker order listing can be delayed, paginated, or incomplete.
            # Once submit intent is durable, one empty listing can never prove
            # that the POST did not land.  Retain the identity fence until exact
            # broker truth or an explicit operator reconciliation resolves it.
            return {
                **_base,
                "disposition": "RECONCILE_PENDING",
                "reason_code": "RECONCILE_BROKER_NO_MATCH_HELD",
            }

        remote = strong[0]
        remote_id = str(remote.get("id") or remote.get("order_id") or "")
        remote_status = str(remote.get("status") or "").lower().replace("-", "_")
        if not remote_id:
            return {**_base, "disposition": "RECONCILE_PENDING", "reason_code": "RECONCILE_MATCH_MISSING_ORDER_ID"}
        status_map = {
            "filled": "FILLED", "partially_filled": "PARTIAL_FILL",
            "partial_filled": "PARTIAL_FILL", "rejected": "REJECTED",
            "canceled": "CANCELED", "cancelled": "CANCELED", "expired": "EXPIRED",
        }
        local_status = status_map.get(remote_status, "SUBMITTED")
        # Establish the accepted boundary first so the existing state machine
        # owns all subsequent fill/terminal transitions.
        if not osm.transition(local_order_id, "SUBMITTED", broker_order_id=remote_id, submitted_ts=now_utc_iso()):
            return {**_base, "disposition": "RECONCILE_PENDING", "reason_code": "RECONCILE_ADOPTION_TRANSITION_FAILED"}
        if local_status != "SUBMITTED":
            osm.transition(
                local_order_id, local_status, broker_order_id=remote_id,
                filled_qty=remote.get("exec_quantity") or remote.get("filled_quantity"),
                fill_price=remote.get("avg_fill_price"),
                last_error=(str(remote.get("reason") or remote.get("message") or "") or None),
            )
        osm.update_order_meta(local_order_id, {
            "reconciled_at": now_utc_iso(), "recovery_classification": "BROKER_ORDER_ADOPTED",
            "broker_reconcile_status": remote_status, "broker_reconcile_response": remote,
            "current_owner": "ORDER_MONITOR", "lifecycle_state": local_status,
        })
        return {**_base, "disposition": "ALREADY_RECONCILED", "reason_code": "BROKER_ORDER_ADOPTED", "broker_order_id": remote_id, "status": local_status}

    def reconcile_exit_broker_intent(
        self,
        *,
        local_order_id: str,
    ) -> dict:
        """Read-only reconciliation for an EXIT submit-intent crash window.

        This method never POSTs or cancels. It only reads broker truth through
        the existing account order listing and adopts an exact EXIT match
        through the order state machine.
        """
        _base = {
            "local_order_id": local_order_id,
            "submit_intent_at": None,
            "broker_submit_key": None,
        }

        osm = getattr(self, "order_state_machine", None) or getattr(self, "osm", None)
        if osm is None:
            return {**_base, "disposition": "RECONCILE_PENDING", "reason_code": "RECONCILE_OSM_UNAVAILABLE"}
        try:
            row = osm.get_order(local_order_id)
        except Exception as exc:
            return {**_base, "disposition": "RECONCILE_PENDING", "reason_code": f"RECONCILE_ROW_READ_ERROR:{type(exc).__name__}"}
        if not isinstance(row, dict):
            return {**_base, "disposition": "RECONCILE_PENDING", "reason_code": "RECONCILE_ROW_MISSING"}

        row_client_id = str(row.get("client_id") or "").strip().lower()
        expected_client_id = str(self.client_id or self.email or "").strip().lower()
        if row_client_id and expected_client_id and row_client_id != expected_client_id:
            return {**_base, "disposition": "RECONCILE_PENDING", "reason_code": "RECONCILE_CLIENT_ID_MISMATCH"}
        row_mode = str(row.get("execution_mode") or "").strip().lower()
        expected_mode = str(getattr(self, "execution_mode", None) or getattr(self, "mode", None) or "").strip().lower()
        if row_mode and expected_mode and row_mode != expected_mode:
            return {**_base, "disposition": "RECONCILE_PENDING", "reason_code": "RECONCILE_EXECUTION_MODE_MISMATCH"}
        if str(row.get("kind") or "").strip().upper() != "EXIT":
            return {**_base, "disposition": "RECONCILE_PENDING", "reason_code": "RECONCILE_KIND_MISMATCH"}

        broker_order_id = str(row.get("broker_order_id") or "").strip()
        meta = row.get("meta") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        meta = meta if isinstance(meta, dict) else {}
        submit_intent_at = meta.get("submit_intent_at")
        broker_submit_key = meta.get("broker_submit_key")
        _base["submit_intent_at"] = submit_intent_at
        _base["broker_submit_key"] = broker_submit_key

        if broker_order_id:
            return {
                **_base,
                "disposition": "ALREADY_RECONCILED",
                "reason_code": "RECONCILE_BROKER_ORDER_PRESENT",
                "broker_order_id": broker_order_id,
                "status": str(row.get("status") or "").strip().upper(),
            }

        if not submit_intent_at:
            return {
                **_base,
                "disposition": "NOT_IN_CRASH_WINDOW",
                "reason_code": "RECONCILE_NO_SUBMIT_INTENT",
            }

        broker = getattr(self, "broker", None)
        list_orders = getattr(broker, "list_orders", None)
        if not callable(list_orders):
            return {**_base, "disposition": "RECONCILE_PENDING", "reason_code": "RECONCILE_BROKER_QUERY_UNAVAILABLE"}
        try:
            broker_orders = list_orders()
        except Exception as exc:
            return {**_base, "disposition": "RECONCILE_PENDING", "reason_code": f"RECONCILE_BROKER_QUERY_FAILED:{type(exc).__name__}"}

        tag = canonical_broker_submit_key(broker_submit_key or local_order_id)
        exact_tag = [o for o in broker_orders if isinstance(o, dict) and str(o.get("tag") or "") == tag]
        expected_contract = str(row.get("contract") or "")
        expected_qty = int(row.get("qty") or 0)
        strong = [
            o for o in exact_tag
            if str(o.get("option_symbol") or o.get("contract") or o.get("symbol") or "") == expected_contract
            and str(o.get("side") or "").lower() == "sell_to_close"
            and int(float(o.get("quantity") or 0)) == expected_qty
        ]
        if len(exact_tag) > 1 or len(strong) > 1:
            return {**_base, "disposition": "RECONCILE_PENDING", "reason_code": "RECONCILE_MULTIPLE_MATCHES", "match_count": len(exact_tag)}
        if exact_tag and not strong:
            return {**_base, "disposition": "RECONCILE_PENDING", "reason_code": "RECONCILE_TAG_IDENTITY_MISMATCH"}
        if not strong:
            return {**_base, "disposition": "RECONCILE_PENDING", "reason_code": "RECONCILE_BROKER_NO_MATCH_HELD"}

        remote = strong[0]
        remote_id = str(remote.get("id") or remote.get("order_id") or "")
        remote_status = str(remote.get("status") or "").lower().replace("-", "_")
        if not remote_id:
            return {**_base, "disposition": "RECONCILE_PENDING", "reason_code": "RECONCILE_MATCH_MISSING_ORDER_ID"}

        status_map = {
            "open": "EXIT_SUBMITTED",
            "submitted": "EXIT_SUBMITTED",
            "pending": "EXIT_SUBMITTED",
            "accepted": "EXIT_SUBMITTED",
            "partially_filled": "EXIT_PARTIAL_FILL",
            "partial_filled": "EXIT_PARTIAL_FILL",
            "filled": "EXIT_FILLED",
            "rejected": "REJECTED",
            "canceled": "CANCELED",
            "cancelled": "CANCELED",
            "expired": "EXPIRED",
        }
        local_status = status_map.get(remote_status, "EXIT_SUBMITTED")

        adopted = osm.transition(
            local_order_id,
            "EXIT_SUBMITTED",
            broker_order_id=remote_id,
            submitted_ts=now_utc_iso(),
        )
        if not adopted:
            current_status = str(row.get("status") or "").strip().upper()
            attach = getattr(osm, "_attach_broker_identity_if_missing", None)
            if current_status in {"EXIT_SUBMITTED", "EXIT_ACKNOWLEDGED", "EXIT_PARTIAL_FILL"} and callable(attach):
                adopted = bool(attach(
                    local_order_id,
                    broker_order_id=remote_id,
                    current_status=current_status,
                    current_execution_mode=str(row.get("execution_mode") or ""),
                ))
            if not adopted:
                return {**_base, "disposition": "RECONCILE_PENDING", "reason_code": "RECONCILE_ADOPTION_TRANSITION_FAILED"}

        if local_status != "EXIT_SUBMITTED":
            osm.transition(
                local_order_id,
                local_status,
                broker_order_id=remote_id,
                filled_qty=remote.get("exec_quantity") or remote.get("filled_quantity"),
                fill_price=remote.get("avg_fill_price"),
                last_error=(str(remote.get("reason") or remote.get("message") or "") or None),
            )

        if callable(getattr(osm, "update_order_meta", None)):
            osm.update_order_meta(local_order_id, {
                "reconciled_at": now_utc_iso(),
                "recovery_classification": "BROKER_ORDER_ADOPTED",
                "broker_reconcile_status": remote_status,
                "broker_reconcile_response": remote,
                "current_owner": "ORDER_MONITOR",
                "lifecycle_state": local_status,
            })
        return {
            **_base,
            "disposition": "ALREADY_RECONCILED",
            "reason_code": "BROKER_ORDER_ADOPTED",
            "broker_order_id": remote_id,
            "status": local_status,
        }

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

        # Recovery ownership must exist before the first policy gate.  Store one
        # immutable context on the callback payload so every cleanup path uses
        # the same exact owner/generation/mode/local-order identity.
        _initial_plan = sig.get("_approved_plan")
        _initial_meta = getattr(_initial_plan, "metadata", None) or {}
        if not isinstance(_initial_meta, dict):
            _initial_meta = {}
        _ownership_kind = str(
            sig.get("ownership_kind")
            or _initial_meta.get("ownership_kind")
            or "broker_ready_recovery"
        ).strip()
        _is_recovered = bool(
            sig.get("recovery_submit_fenced")
            or _initial_meta.get("recovery_submit_fenced")
            or sig.get("fenced")
            or _initial_meta.get("fenced")
        )
        _recovery_owner = str(
            sig.get("recovery_submit_owner")
            or _initial_meta.get("recovery_submit_owner")
            or sig.get("owner")
            or _initial_meta.get("owner")
            or ""
        ).strip()
        _recovery_generation_raw = (
            sig.get("recovery_submit_generation")
            if sig.get("recovery_submit_generation") is not None
            else (
                _initial_meta.get("recovery_submit_generation")
                if _initial_meta.get("recovery_submit_generation") is not None
                else (
                    sig.get("materialization_generation")
                    if sig.get("materialization_generation") is not None
                    else _initial_meta.get("materialization_generation")
                )
            )
        )
        try:
            _recovery_generation = int(_recovery_generation_raw)
        except (TypeError, ValueError):
            _recovery_generation = None
        _recovery_attempt_raw = (
            sig.get("retry_attempt")
            if sig.get("retry_attempt") is not None
            else (
                _initial_meta.get("retry_attempt")
                if _initial_meta.get("retry_attempt") is not None
                else (
                    sig.get("materialization_retry_attempt")
                    if sig.get("materialization_retry_attempt") is not None
                    else _initial_meta.get("materialization_retry_attempt")
                )
            )
        )
        try:
            _recovery_attempt = int(_recovery_attempt_raw)
        except (TypeError, ValueError):
            _recovery_attempt = None
        _callback_mode = str(
            sig.get("execution_mode")
            or getattr(_initial_plan, "execution_mode", None)
            or getattr(self, "execution_mode", None)
            or ""
        ).strip().lower()
        _callback_client_id = str(
            sig.get("client_id")
            or getattr(_initial_plan, "client_id", None)
            or getattr(self, "client_id", None)
            or getattr(self, "email", None)
            or ""
        ).strip().lower()
        _callback_local_order_id = str(sig.get("local_order_id") or "").strip()
        _ownership_context = MappingProxyType({
            "is_recovered": _is_recovered,
            "ownership_kind": _ownership_kind,
            "local_order_id": _callback_local_order_id,
            "client_id": _callback_client_id,
            "fenced": bool(_is_recovered),
            "owner": _recovery_owner,
            "generation": _recovery_generation,
            "retry_attempt": _recovery_attempt,
            "execution_mode": _callback_mode,
        })
        sig["_callback_ownership_context"] = _ownership_context
        if _is_recovered and (
            not _recovery_owner
            or _recovery_generation is None
            or _callback_mode not in {"live", "paper"}
            or not _callback_client_id
            or not _callback_local_order_id
            or _ownership_kind not in {"broker_ready_recovery", "materialization_retry"}
        ):
            log.critical(
                "[%s] RECOVERY_CALLBACK_OWNERSHIP_INVALID order=%s kind=%r "
                "owner=%r generation=%r attempt=%r client=%r mode=%r",
                ticker, _callback_local_order_id, _ownership_kind,
                _recovery_owner, _recovery_generation, _recovery_attempt,
                _callback_client_id, _callback_mode,
            )
            return {
                "disposition": "KEEP_WATCHER",
                "reason_code": "RECOVERY_CALLBACK_OWNERSHIP_INVALID",
            }
        if _is_recovered and _ownership_kind == "materialization_retry":
            if _recovery_attempt is None or _recovery_attempt < 1:
                return {
                    "disposition": "KEEP_WATCHER",
                    "reason_code": "MATERIALIZATION_CALLBACK_OWNERSHIP_INVALID_ATTEMPT",
                }
            if self.order_state_machine is None:
                return {
                    "disposition": "KEEP_WATCHER",
                    "reason_code": "MATERIALIZATION_CALLBACK_OSM_UNAVAILABLE",
                }
            try:
                _owned_row = self.order_state_machine.get_order(_callback_local_order_id)
            except Exception as _own_exc:
                log.critical(
                    "[%s] MATERIALIZATION_CALLBACK_ROW_READ_FAILED order=%s exc=%s",
                    ticker, _callback_local_order_id, _own_exc,
                )
                return {
                    "disposition": "KEEP_WATCHER",
                    "reason_code": "MATERIALIZATION_CALLBACK_ROW_READ_FAILED",
                }
            _owned_meta = {}
            if isinstance(_owned_row, dict):
                _owned_meta = _owned_row.get("meta") or {}
            if isinstance(_owned_meta, str):
                try:
                    _owned_meta = json.loads(_owned_meta)
                except Exception:
                    _owned_meta = {}
            if not isinstance(_owned_meta, dict):
                _owned_meta = {}
            try:
                _owned_generation = int(_owned_meta.get("materialization_generation") or 0)
            except (TypeError, ValueError):
                _owned_generation = 0
            try:
                _owned_attempt = int(_owned_meta.get("retry_attempt") or 0)
            except (TypeError, ValueError):
                _owned_attempt = 0
            _owned_ok = (
                isinstance(_owned_row, dict)
                and str(_owned_row.get("status") or "").upper() == "PENDING_TRIGGER"
                and not str(_owned_row.get("broker_order_id") or "").strip()
                and not _owned_row.get("submitted_ts")
                and str(_owned_row.get("client_id") or "").strip().lower() == _callback_client_id
                and str(_owned_row.get("execution_mode") or "").strip().lower() == _callback_mode
                and str(_owned_meta.get("lifecycle_state") or "").upper() == "MATERIALIZING"
                and str(_owned_meta.get("materialization_status") or "").upper() == "RUNNING"
                and _owned_meta.get("materialization_in_flight") is True
                and str(_owned_meta.get("materialization_owner") or "").strip() == _recovery_owner
                and _owned_generation == _recovery_generation
                and _owned_attempt == _recovery_attempt
            )
            if not _owned_ok:
                log.critical(
                    "[%s] MATERIALIZATION_CALLBACK_OWNERSHIP_VERIFY_FAILED "
                    "order=%s owner=%r generation=%r attempt=%r",
                    ticker, _callback_local_order_id, _recovery_owner,
                    _recovery_generation, _recovery_attempt,
                )
                return {
                    "disposition": "KEEP_WATCHER",
                    "reason_code": "MATERIALIZATION_CALLBACK_OWNERSHIP_VERIFY_FAILED",
                }

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
            # A normal callback return causes watcher ownership to be released.
            # Therefore every breach-risk block must first make the order
            # terminal; logging/decision_status alone is not lifecycle truth.
            _risk_terminalized = self._cleanup_pending_entry_order(
                watched,
                action="expire",
                reason="breach_risk_check_false",
            )
            return {
                "disposition": (
                    "TERMINAL_DURABLE" if _risk_terminalized
                    else str(sig.get("_recovery_cleanup_disposition") or "KEEP_WATCHER")
                ),
                "reason_code": "breach_risk_check_false",
                "retry_after_seconds": 5,
            }

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

        _deferred_claim_context = {"owner": "", "generation": None}

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
            if _ownership_context.get("is_recovered"):
                # Recovery terminal truth belongs to the exact callback owner.
                # Never write generic diagnostics or signal state before its
                # owner/generation/lease CAS succeeds.
                _recovered_ok = self._cleanup_pending_entry_order(
                    watched, action=cleanup_action, reason=reason,
                )
                if _recovered_ok:
                    if funnel_key:
                        funnel.inc(funnel_key)
                    if signal_id:
                        self.store.update_signal_fields(signal_id, {
                            "decision_status": decision_status,
                            "context_notes": context_notes or reason,
                        })
                return
            if funnel_key:
                funnel.inc(funnel_key)
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": decision_status,
                    "context_notes": context_notes or reason,
                })
            _claim_owner = str(_deferred_claim_context.get("owner") or "")
            _claim_generation = _deferred_claim_context.get("generation")
            _atomic_terminalize = getattr(
                self.order_state_machine, "terminalize_deferred_breach", None,
            )
            if _claim_owner and callable(_atomic_terminalize):
                try:
                    if _atomic_terminalize(
                        queue_local_order_id,
                        reason_code=str(reason or "UNKNOWN_BREACH_FAILURE"),
                        terminal_status=("CANCELED" if cleanup_action == "cancel" else "EXPIRED"),
                        owner=_claim_owner,
                        generation=_claim_generation,
                        diagnostics=meta_patch or {},
                    ):
                        return {
                            "disposition": "TERMINAL_DURABLE",
                            "reason_code": str(reason or "UNKNOWN_BREACH_FAILURE"),
                            "terminal_status": ("CANCELED" if cleanup_action == "cancel" else "EXPIRED"),
                        }
                except Exception as _atomic_terminal_exc:
                    log.critical(
                        "[%s] breach atomic terminal write failed order=%s error=%s",
                        ticker, queue_local_order_id, _atomic_terminal_exc,
                    )
            if queue_local_order_id and self.order_state_machine is not None and meta_patch:
                try:
                    update_meta = getattr(self.order_state_machine, "update_order_meta", None)
                    if callable(update_meta):
                        update_meta(queue_local_order_id, meta_patch)
                except Exception as _meta_exc:
                    log.warning("[%s] breach failure meta persist failed: %s", ticker, _meta_exc)
            _cleanup_ok = self._cleanup_pending_entry_order(
                watched, action=cleanup_action, reason=reason,
            )
            if not _cleanup_ok:
                return {
                    "disposition": "KEEP_WATCHER",
                    "reason_code": "BREACH_TERMINAL_WRITE_FAILED",
                    "retry_after_seconds": 5,
                }
            return {
                "disposition": "TERMINAL_DURABLE",
                "reason_code": str(reason or "UNKNOWN_BREACH_FAILURE"),
                "terminal_status": ("CANCELED" if cleanup_action == "cancel" else "EXPIRED"),
            }

        def _terminalize_deferred_breach_failure(reason: str, *, extra_meta: dict | None = None) -> dict:
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
            if _ownership_context.get("is_recovered"):
                return _terminalize_breach_failure(
                    reason,
                    cleanup_action="expire",
                    meta_patch=meta_patch,
                    context_notes=reason,
                )
            _atomic_terminalize = getattr(
                self.order_state_machine, "terminalize_deferred_breach", None,
            )
            if callable(_atomic_terminalize):
                try:
                    if _atomic_terminalize(
                        queue_local_order_id,
                        reason_code=str(reason or "UNKNOWN_DEFERRED_BREACH_FAILURE"),
                        terminal_status="EXPIRED",
                        owner=str(_deferred_claim_context.get("owner") or ""),
                        generation=_deferred_claim_context.get("generation"),
                        diagnostics=meta_patch,
                    ):
                        if signal_id:
                            self.store.update_signal_fields(signal_id, {
                                "decision_status": "blocked_at_breach",
                                "context_notes": reason,
                            })
                        funnel.inc("order_failed")
                        return {
                            "disposition": "TERMINAL_DURABLE",
                            "reason_code": str(reason or "UNKNOWN_DEFERRED_BREACH_FAILURE"),
                            "terminal_status": "EXPIRED",
                        }
                except Exception as _atomic_terminal_exc:
                    log.critical(
                        "[%s] deferred terminal CAS failed order=%s error=%s",
                        ticker, queue_local_order_id, _atomic_terminal_exc,
                    )
            return _terminalize_breach_failure(
                reason,
                cleanup_action="expire",
                meta_patch=meta_patch,
                context_notes=reason,
            )

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
            # canonical materialization outcome onto the order row so the operator can
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

        # ── Intelligence PR 1: dispatch immediately after plan is confirmed ──────
        # Fires before any post-plan terminal return (rejections, expiry, submit).
        # _ensure_intelligence_dispatched is idempotent — retries do not re-dispatch.
        # Disabled by default (INTELLIGENCE_EVIDENCE_ENABLED=0). Never raises.
        try:
            from ap.intelligence_evaluation import _ensure_intelligence_dispatched as _eid
            _eid(
                sig,
                # Req 2: mode resolved inside _ensure_intelligence_dispatched
                # from both sig and approved_plan with mismatch detection.
                # Do NOT pass execution_mode from getattr(approved_plan, ...) alone.
                execution_mode="",   # resolver reads plan directly
                client_id=_breach_client_id,
                local_order_id=str(queue_local_order_id or ""),
                plan=approved_plan,  # Req 1: plan passed for canonical snapshot build
                order_meta_writer=getattr(self.order_state_machine, "update_order_meta", None),
                ticker=ticker,
            )
        except Exception as _eid_exc:
            log.debug("[%s] intelligence early dispatch non-critical: %s", ticker, _eid_exc)
        # ── End early intelligence dispatch ─────────────────────────────────────

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
        _proof_client_id = str(
            getattr(approved_plan, "client_id", None) or _breach_client_id or ""
        )
        _proof_execution_mode = str(
            getattr(approved_plan, "execution_mode", None)
            or sig.get("execution_mode")
            or getattr(self, "execution_mode", None)
            or ""
        )
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
            "order_row_limit":        None,
            "order_row_qty":          None,
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
                return _terminalize_deferred_breach_failure(
                    _reason,
                    extra_meta={"failure_stage": "deferred_contract_selection"},
                )
                log.critical(
                    "[%s] PRODUCTION_ENTRY_BLOCK — contract_deferred=True but no "
                    "contract_selector wired into execution core",
                    ticker,
                )
                return
            # P0 (PR #299): structured start marker — one line per deferred breach
            # attempt. Operators filter on this to confirm the materializer is
            # actually running on the row before looking for SELECTED/FAILED.
            log.info(
                "DEFERRED_MATERIALIZATION_STARTED "
                "order_id=%s client_id=%s execution_mode=%s symbol=%s "
                "direction=%s contract_before=%s limit_before=%.4f",
                str(queue_local_order_id or ""),
                str(_breach_client_id or ""),
                str(getattr(approved_plan, "execution_mode", "") or ""),
                ticker,
                str(getattr(approved_plan, "side", "") or ""),
                str(_contract_sym_raw or ""),
                float(getattr(approved_plan, "limit_price", 0) or 0),
            )
            # ── Pre-claim bypass for resume_deferred_materialization_retry ──
            # resume_deferred_materialization_retry already claimed the row
            # (generation N→N+1, lifecycle→MATERIALIZING) before calling
            # _on_entry_trigger. A second claim here fails because the row is
            # already MATERIALIZING with a live lease, blocking selector + submit.
            #
            # The bypass is ONLY safe when verified against the durable row:
            # lifecycle=MATERIALIZING, materialization_in_flight=true, exact
            # owner, generation, attempt, client_id, execution_mode must all
            # match the pre-claim markers written into sig. Any mismatch fails
            # closed — no selector, no broker path.
            _recovery_pre_claimed = bool(sig.get("_recovery_pre_claimed"))
            _mat_client_id     = str(_breach_client_id or "")
            _mat_exec_mode     = str(getattr(approved_plan, "execution_mode", "") or "")
            _mat_direction     = str(getattr(approved_plan, "side", "") or "")
            _mat_trigger_price = float(getattr(watched, "trigger_price", 0) or 0)

            if _recovery_pre_claimed:
                _pre_owner   = str(sig.get("_recovery_pre_claimed_owner") or "")
                _pre_gen     = int(sig.get("_recovery_pre_claimed_generation") or 0)
                _pre_attempt = int(sig.get("_recovery_pre_claimed_attempt") or 0)
                _pre_client  = str(sig.get("_recovery_pre_claimed_client_id") or "").lower()
                _pre_mode    = str(sig.get("_recovery_pre_claimed_mode") or "").lower()
                _pv_row = None
                try:
                    _pv_row = self.order_state_machine.get_order(queue_local_order_id)
                except Exception:
                    pass
                _pre_verified = False
                if isinstance(_pv_row, dict):
                    _pvm = _pv_row.get("meta") or {}
                    if isinstance(_pvm, str):
                        try:
                            _pvm = json.loads(_pvm)
                        except Exception:
                            _pvm = {}
                    _pvc = str(_pv_row.get("client_id") or "").strip().lower()
                    _pve = str(_pv_row.get("execution_mode") or "").strip().lower()
                    _pvg = int((_pvm or {}).get("materialization_generation") or 0)
                    _pvo = str((_pvm or {}).get("materialization_owner") or "").strip()
                    _pvl = str((_pvm or {}).get("lifecycle_state") or "").upper()
                    _pvif = bool((_pvm or {}).get("materialization_in_flight"))
                    _pva = int((_pvm or {}).get("retry_attempt") or 0)
                    _pre_verified = (
                        _pvl == "MATERIALIZING"
                        and _pvif
                        and _pvg == _pre_gen
                        and _pvo == _pre_owner
                        and _pvc == _pre_client
                        and _pve == _pre_mode
                        and _pva == _pre_attempt
                        and bool(_pre_owner)
                        and _pre_gen > 0
                        and _pre_attempt > 0
                    )
                if not _pre_verified:
                    log.critical(
                        "[%s] MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED order=%s "
                        "pre_gen=%d pre_attempt=%d pre_owner=%r — "
                        "failing closed; no selector or broker path entered",
                        ticker, queue_local_order_id,
                        _pre_gen, _pre_attempt, _pre_owner,
                    )
                    return {
                        "disposition": "KEEP_WATCHER",
                        "reason_code": "MATERIALIZATION_PRE_CLAIM_VERIFY_FAILED",
                        "retry_after_seconds": 5,
                    }
                # Verified — bypass the normal claim and proceed to selector.
                _mat_owner = _pre_owner
                _mat_generation = _pre_gen
                _mat_claimed = True
                _deferred_claim_context.update({
                    "owner": _mat_owner,
                    "generation": _mat_generation,
                })
            else:
                # ── Normal watcher path: claim exactly once ───────────────
                _mat_owner = str(
                    sig.get("watcher_token")
                    or sig.get("materialization_owner")
                    or f"execution-core:{getattr(self, 'client_id', '')}:{queue_local_order_id}"
                )
                _prior_mat_attempt = 0
                try:
                    _meta_for_attempt = getattr(approved_plan, "metadata", None) or {}
                    _prior_mat_attempt = int(_meta_for_attempt.get("materialization_attempts", 0) or 0)
                except Exception:
                    _prior_mat_attempt = 0
                # ── AMENDMENT §3: strictly monotonic generation ──────────
                _persisted_generation = 0
                try:
                    _durable_row = self.order_state_machine.get_order(queue_local_order_id)
                    if isinstance(_durable_row, dict):
                        _dur_meta = _durable_row.get("meta") or {}
                        if isinstance(_dur_meta, str):
                            try:
                                _dur_meta = json.loads(_dur_meta)
                            except Exception:
                                _dur_meta = {}
                        _persisted_generation = int(
                            (_dur_meta or {}).get("materialization_generation") or 0
                        )
                except Exception:
                    _persisted_generation = 0
                _mat_generation = _persisted_generation + 1
                _deferred_claim_context.update({
                    "owner": _mat_owner,
                    "generation": _mat_generation,
                })
                _mat_claim = getattr(
                    self.order_state_machine, "claim_deferred_materialization", None,
                )
                _mat_claimed = False
                if callable(_mat_claim):
                    try:
                        _lease_until = (
                            datetime.now(timezone.utc) + timedelta(seconds=120)
                        ).isoformat()
                        _crossed_at = getattr(watched, "trigger_crossed_at", None)
                        _crossed_at = (
                            _crossed_at.isoformat()
                            if hasattr(_crossed_at, "isoformat")
                            else str(_crossed_at or datetime.now(timezone.utc).isoformat())
                        )
                        _observed_underlying = float(
                            getattr(watched, "last_quote_ask", 0)
                            or getattr(watched, "last_quote_bid", 0)
                            or _mat_trigger_price
                            or 0
                        )
                        _mat_claimed = bool(_mat_claim(
                            str(queue_local_order_id or ""),
                            owner=_mat_owner,
                            generation=_mat_generation,
                            lease_until=_lease_until,
                            trigger_crossed_at=_crossed_at,
                            trigger_price=_mat_trigger_price,
                            observed_underlying_price=_observed_underlying,
                            signal_id=str(getattr(approved_plan, "signal_id", "") or ""),
                            execution_mode=_mat_exec_mode,
                        ))
                    except Exception as _mat_claim_exc:
                        log.critical(
                            "[%s] MATERIALIZATION_STATE_WRITE_FAILED order=%s error=%s",
                            ticker, queue_local_order_id, _mat_claim_exc,
                        )
                if not _mat_claimed:
                    try:
                        _claim_row = self.order_state_machine.get_order(queue_local_order_id)
                    except Exception:
                        _claim_row = None
                    if isinstance(_claim_row, dict):
                        _claim_status = str(_claim_row.get("status") or "").upper()
                        if _claim_status in {"SUBMITTED", "ACKNOWLEDGED", "PARTIAL_FILL", "FILLED"}:
                            return {"disposition": "SUBMITTED"}
                        if _claim_status in {"REJECTED", "EXPIRED", "CANCELED", "ERROR"}:
                            return {"disposition": "TERMINAL_DURABLE"}
                    log.critical(
                        "[%s] MATERIALIZATION_STATE_WRITE_FAILED order=%s — "
                        "selector and broker submission blocked; watcher retains ownership",
                        ticker, queue_local_order_id,
                    )
                    return {
                        "disposition": "KEEP_WATCHER",
                        "reason_code": "MATERIALIZATION_STATE_WRITE_FAILED",
                        "retry_after_seconds": 5,
                    }
            try:
                # PR #401: bind this explicitly-owned deferred attempt to one
                # durable selector cursor.  Ordinary selector calls never
                # receive this request kind or cursor behavior.
                from ap.contract_selector import (
                    SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
                    _new_selector_request_context,
                )
                from ap.selector_retry_policy import (
                    SelectorRecoveryOwnershipLost,
                    load_selector_recovery_cursor,
                    record_selector_recovery_attempt,
                    record_selector_structural_skip,
                )

                _cursor_enabled = str(
                    os.getenv("SELECTOR_DURABLE_RECOVERY_CURSOR_ENABLED", "1")
                ).strip().lower() in {"1", "true", "yes"}
                _cursor_row = self.order_state_machine.get_order(
                    str(queue_local_order_id or "")
                )
                _cursor_meta = (
                    (_cursor_row or {}).get("meta") or {}
                    if isinstance(_cursor_row, dict)
                    else {}
                )
                if isinstance(_cursor_meta, str):
                    try:
                        _cursor_meta = json.loads(_cursor_meta)
                    except Exception:
                        _cursor_meta = {}
                if not isinstance(_cursor_meta, dict):
                    _cursor_meta = {}
                _selector_attempt_number = max(
                    1,
                    int(
                        _cursor_meta.get("retry_attempt")
                        or _cursor_meta.get("materialization_attempts")
                        or (sig.get("_recovery_pre_claimed_attempt") if isinstance(sig, dict) else 0)
                        or 1
                    ),
                )
                _cursor_candidate = (
                    _cursor_meta.get("selector_recovery_cursor_v1")
                    if _cursor_enabled
                    else None
                )
                _selector_recovery_cursor, _cursor_load_reason = (
                    load_selector_recovery_cursor(
                        _cursor_candidate,
                        local_order_id=str(queue_local_order_id or ""),
                        client_id=str(_breach_client_id or ""),
                        execution_mode=_mat_exec_mode,
                        signal_id=str(getattr(approved_plan, "signal_id", "") or ""),
                        materialization_generation=_mat_generation,
                        selector_attempt_count=_selector_attempt_number,
                        allow_previous_generation=bool(_recovery_pre_claimed),
                    )
                )
                _cursor_failure_reason = _selector_cursor_retry_block_reason(
                    cursor_enabled=_cursor_enabled,
                    selector_attempt_number=_selector_attempt_number,
                    cursor_candidate=_cursor_candidate,
                    cursor_load_reason=_cursor_load_reason,
                )
                if _cursor_failure_reason:
                    _terminalize_deferred_breach_failure(
                        f"SELECTOR_RECOVERY_CURSOR_INVALID:{_cursor_failure_reason}",
                        extra_meta={
                            "selector_recovery_cursor_load_reason": (
                                _cursor_failure_reason
                            ),
                            "selector_calls": 0,
                            "direct_quote_calls": 0,
                            "broker_post_count": 0,
                        },
                    )
                    return {
                        "disposition": "TERMINAL_DURABLE",
                        "reason_code": (
                            f"SELECTOR_RECOVERY_CURSOR_INVALID:"
                            f"{_cursor_failure_reason}"
                        ),
                    }

                _cursor_pending_updates = 0

                def _flush_selector_cursor(*, force: bool = False) -> bool:
                    nonlocal _cursor_pending_updates
                    if not _cursor_enabled or _cursor_pending_updates <= 0:
                        return True
                    if not force and _cursor_pending_updates < 5:
                        return True
                    _persist_cursor = getattr(
                        self.order_state_machine,
                        "persist_selector_recovery_cursor",
                        None,
                    )
                    if not callable(_persist_cursor):
                        raise SelectorRecoveryOwnershipLost(
                            "cursor persistence authority unavailable"
                        )
                    persisted = bool(_persist_cursor(
                        str(queue_local_order_id or ""),
                        owner=_mat_owner,
                        generation=_mat_generation,
                        signal_id=str(
                            getattr(approved_plan, "signal_id", "") or ""
                        ),
                        execution_mode=_mat_exec_mode,
                        cursor=_selector_recovery_cursor,
                    ))
                    if not persisted:
                        raise SelectorRecoveryOwnershipLost(
                            "cursor exact-owner CAS missed"
                        )
                    _cursor_pending_updates = 0
                    return True

                def _persist_selector_cursor_progress(
                    *,
                    symbol: str,
                    result_reason: str = "",
                    transient: bool = False,
                    provider_timestamp=None,
                    structural_skip_reason: str = "",
                ) -> None:
                    nonlocal _selector_recovery_cursor, _cursor_pending_updates
                    if not _cursor_enabled:
                        return
                    if structural_skip_reason:
                        _selector_recovery_cursor = record_selector_structural_skip(
                            _selector_recovery_cursor,
                            symbol=symbol,
                            skip_reason=structural_skip_reason,
                        )
                    else:
                        _cursor_expiration = ""
                        try:
                            _cursor_expiration = datetime.strptime(
                                "".join(str(symbol or "").upper().split())[-15:-9],
                                "%y%m%d",
                            ).date().isoformat()
                        except Exception as _cursor_symbol_exc:
                            log.debug(
                                "[%s] selector cursor could not parse OCC expiration "
                                "symbol=%s error=%s",
                                ticker,
                                symbol,
                                _cursor_symbol_exc,
                            )
                        _selector_recovery_cursor = record_selector_recovery_attempt(
                            _selector_recovery_cursor,
                            symbol=symbol,
                            attempt_number=_selector_attempt_number,
                            expiration=_cursor_expiration,
                            result_reason=result_reason,
                            transient=transient,
                            provider_timestamp=provider_timestamp,
                        )
                    try:
                        _selector_request_context.recovery_cursor = (
                            _selector_recovery_cursor
                        )
                    except (NameError, UnboundLocalError) as _cursor_bind_exc:
                        log.debug(
                            "[%s] selector cursor context not bound yet error=%s",
                            ticker,
                            _cursor_bind_exc,
                        )
                    _cursor_pending_updates += 1
                    _flush_selector_cursor()

                _selector_request_context = _new_selector_request_context(
                    ticker,
                    _mat_exec_mode,
                    selector_request_kind=SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
                    recovery_attempt_number=_selector_attempt_number,
                    recovery_cursor=(
                        _selector_recovery_cursor if _cursor_enabled else None
                    ),
                    recovery_cursor_persist=_persist_selector_cursor_progress,
                )

                # Attempts 2..5 must prove the chart is still valid before
                # spending any option-selector or direct-quote capacity.
                if _selector_attempt_number > 1:
                    from ap.live_submit_gates import (
                        MarketTruthAuthority,
                        check_market_validity_gate,
                        classify_market_truth,
                        validate_retry_market_quote_authority,
                    )

                    _truth_bid = _truth_ask = _truth_age = _truth_source = None
                    _truth_fetched_at = None
                    _truth_fetch_failed = True
                    _truth_fetch_error = "approved_market_data_transport_unavailable"
                    try:
                        _truth_transport = getattr(
                            self.contract_selector, "data_broker", None
                        )
                        if _truth_transport is None or not hasattr(
                            _truth_transport, "get_quote"
                        ):
                            raise RuntimeError(
                                "selector data broker has no get_quote"
                            )
                        _truth_quote = _truth_transport.get_quote(ticker)
                        if not isinstance(_truth_quote, dict):
                            raise RuntimeError(
                                f"invalid quote type {type(_truth_quote).__name__}"
                            )
                        _truth_authority_proof = (
                            validate_retry_market_quote_authority(
                                _truth_quote,
                                transport=_truth_transport,
                            )
                        )
                        if not _truth_authority_proof.get("valid"):
                            raise RuntimeError(
                                str(
                                    _truth_authority_proof.get("reason")
                                    or "market quote authority unproven"
                                )
                            )
                        _truth_bid = _truth_quote.get("bid")
                        _truth_ask = _truth_quote.get("ask")
                        _truth_age = _truth_authority_proof["quote_age_ms"]
                        _truth_source = _truth_authority_proof["quote_source"]
                        _truth_fetched_at = _truth_authority_proof[
                            "provider_timestamp"
                        ]
                        _truth_fetch_failed = False
                        _truth_fetch_error = None
                    except Exception as _truth_exc:
                        _truth_fetch_error = (
                            f"{type(_truth_exc).__name__}:{_truth_exc}"
                        )
                    _truth_result = check_market_validity_gate(
                        side=str(getattr(approved_plan, "side", "") or ""),
                        trigger_price=float(
                            getattr(approved_plan, "trigger_price", 0) or 0
                        ),
                        stop_price=(
                            float(
                                getattr(approved_plan, "stop_underlying", 0) or 0
                            )
                            if getattr(approved_plan, "stop_underlying", None)
                            else None
                        ),
                        target_price=(
                            float(
                                getattr(approved_plan, "target_underlying", 0) or 0
                            )
                            if getattr(approved_plan, "target_underlying", None)
                            else None
                        ),
                        current_bid=_truth_bid,
                        current_ask=_truth_ask,
                        quote_age_ms=_truth_age,
                        quote_source=_truth_source,
                        quote_fetched_at=_truth_fetched_at,
                        quote_provenance="provider_timestamp",
                        quote_fetch_failed=_truth_fetch_failed,
                        quote_fetch_error=_truth_fetch_error,
                        # Retry classification must fail closed identically for
                        # PAPER and LIVE; this is market truth, not execution.
                        execution_mode="live",
                    )
                    _truth_authority = classify_market_truth(_truth_result)
                    _selector_recovery_cursor["last_market_truth_outcome"] = (
                        _truth_authority.value
                    )
                    _selector_recovery_cursor["last_market_truth_checked_at"] = (
                        datetime.now(timezone.utc).isoformat()
                    )
                    if _cursor_enabled:
                        _cursor_pending_updates += 1
                        try:
                            _flush_selector_cursor(force=True)
                        except SelectorRecoveryOwnershipLost as _cursor_lost:
                            log.critical(
                                "[%s] SELECTOR_RECOVERY_OWNERSHIP_LOST order=%s "
                                "stage=market_truth error=%s",
                                ticker,
                                queue_local_order_id,
                                _cursor_lost,
                            )
                            return {
                                "disposition": "MATERIALIZATION_OWNERSHIP_LOST",
                                "reason_code": "SELECTOR_RECOVERY_OWNERSHIP_LOST",
                            }
                    if (
                        _truth_authority
                        == MarketTruthAuthority.REARM_DIRECTION_REVERSAL
                    ):
                        _rearm = getattr(
                            self.order_state_machine,
                            "rearm_deferred_materialization_direction_reversal",
                            None,
                        )
                        _rearmed = bool(
                            callable(_rearm)
                            and _rearm(
                                str(queue_local_order_id or ""),
                                owner=_mat_owner,
                                generation=_mat_generation,
                                signal_id=str(
                                    getattr(approved_plan, "signal_id", "") or ""
                                ),
                                execution_mode=_mat_exec_mode,
                                market_truth_audit=_truth_result.audit,
                            )
                        )
                        return {
                            "disposition": (
                                "KEEP_WATCHER"
                                if _rearmed
                                else "MATERIALIZATION_REARM_WRITE_FAILED"
                            ),
                            "reason_code": "REARM_DIRECTION_REVERSAL",
                        }
                    if (
                        _truth_authority
                        == MarketTruthAuthority.TERMINAL_SETUP_COMPLETE
                    ):
                        _terminal_market_reason = str(
                            _truth_result.reason_code
                            or "MARKET_SETUP_INVALIDATED"
                        )
                        _terminalize_deferred_breach_failure(
                            _terminal_market_reason,
                            extra_meta={
                                "final_market_truth": _truth_result.audit,
                                "final_market_truth_reason": _truth_result.reason_code,
                                "selector_calls": 0,
                                "broker_post_count": 0,
                            },
                        )
                        return {
                            "disposition": "TERMINAL_DURABLE",
                            "reason_code": _terminal_market_reason,
                        }
                    if (
                        _truth_authority
                        == MarketTruthAuthority.HOLD_MARKET_TRUTH_UNAVAILABLE
                    ):
                        _max_attempts_truth = _positive_int_env_config(
                            "MAX_BREACH_SELECTOR_RETRIES", 5
                        )
                        if _selector_attempt_number >= _max_attempts_truth:
                            _terminalize_deferred_breach_failure(
                                f"BREACH_RETRY_EXHAUSTED:{_truth_result.reason_code}",
                                extra_meta={
                                    "final_market_truth": _truth_result.audit,
                                    "selector_calls": 0,
                                    "broker_post_count": 0,
                                },
                            )
                            return {
                                "disposition": "TERMINAL_DURABLE",
                                "reason_code": _truth_result.reason_code,
                            }
                        _truth_delay = _positive_int_env_config(
                            "BREACH_SELECTOR_RETRY_DELAY_SECONDS", 8
                        )
                        _truth_next = (
                            datetime.now(timezone.utc)
                            + timedelta(seconds=_truth_delay)
                        ).isoformat()
                        _schedule_truth = getattr(
                            self.order_state_machine,
                            "schedule_deferred_materialization_retry",
                            None,
                        )
                        _truth_failure = _build_deferred_retry_schedule_meta(
                            reason_code=str(_truth_result.reason_code),
                            selector_audit={
                                "market_truth_outcome": _truth_authority.value,
                                "market_truth_reason": _truth_result.reason_code,
                                "market_truth_audit": _truth_result.audit,
                            },
                            attempt=_selector_attempt_number,
                            max_attempts=_max_attempts_truth,
                            delay_seconds=_truth_delay,
                            client_id=str(_breach_client_id or ""),
                            execution_mode=_mat_exec_mode,
                            local_order_id=str(queue_local_order_id or ""),
                            signal_id=str(
                                getattr(approved_plan, "signal_id", "") or ""
                            ),
                        )
                        _truth_scheduled = bool(
                            callable(_schedule_truth)
                            and _schedule_truth(
                                str(queue_local_order_id or ""),
                                owner=_mat_owner,
                                generation=_mat_generation,
                                reason_code=str(_truth_result.reason_code),
                                attempt=_selector_attempt_number,
                                max_attempts=_max_attempts_truth,
                                next_retry_at=_truth_next,
                                selector_failure=_truth_failure,
                                signal_id=str(
                                    getattr(approved_plan, "signal_id", "") or ""
                                ),
                                execution_mode=_mat_exec_mode,
                                selector_recovery_cursor=(
                                    _selector_recovery_cursor
                                    if _cursor_enabled
                                    else None
                                ),
                            )
                        )
                        return {
                            "disposition": (
                                "RETRY_WAIT"
                                if _truth_scheduled
                                else "RETRY_SCHEDULE_FAILED"
                            ),
                            "reason_code": _truth_result.reason_code,
                            "next_retry_at": _truth_next,
                        }

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
                try:
                    _sel = self.contract_selector.select(
                        approved_plan,
                        request_context=_selector_request_context,
                    )
                    _flush_selector_cursor(force=True)
                except SelectorRecoveryOwnershipLost as _cursor_lost:
                    log.critical(
                        "[%s] SELECTOR_RECOVERY_OWNERSHIP_LOST order=%s "
                        "stage=selector error=%s",
                        ticker,
                        queue_local_order_id,
                        _cursor_lost,
                    )
                    return {
                        "disposition": "MATERIALIZATION_OWNERSHIP_LOST",
                        "reason_code": "SELECTOR_RECOVERY_OWNERSHIP_LOST",
                    }
                (
                    _sel_result_valid,
                    _sel_contract,
                    _sel_price_candidate,
                    _sel_qty_candidate,
                ) = _validate_deferred_selector_result(_sel, ticker)
                _live_contract = str(getattr(approved_plan, "contract_symbol", "") or "").strip()
                _sel_is_real = self._is_real_occ_contract(_sel_contract, ticker)

                if _sel is not None and not _sel_result_valid:
                    _invalid_reason = "SELECTOR_RESULT_INVALID"
                    _terminalize_deferred_breach_failure(
                        _invalid_reason,
                        extra_meta={
                            "failure_stage": "selector_result_validation",
                            "selector_contract": _sel_contract,
                            "selector_price": _sel_price_candidate,
                            "selector_qty": _sel_qty_candidate,
                        },
                    )
                    return {"disposition": "TERMINAL_DURABLE"}

                # A deferred breach retry can start from a durable row that
                # already contains an OCC-shaped contract while its executable
                # fields are still placeholders (for example limit_price=0.01,
                # broker_ready=false).  The selector result is the fresh
                # breach-time authority in every deferred attempt.  Restricting
                # copyback to DEFERRED:* plans discards a successful retry and
                # leaves the stale contract/penny limit in place.
                if _sel_is_real:
                    try:
                        approved_plan.contract_symbol = _sel_contract
                        _sel_price = _sel_price_candidate
                        if _sel_price:
                            approved_plan.limit_price = float(_sel_price)
                        _sel_qty = _sel_qty_candidate
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
                        # Write breach last error to trade_queue BEFORE terminalizing
                        # so Supabase shows the cap reason without reading Render logs.
                        try:
                            from ap.queue import write_deferred_breach_last_error
                            _cap_queue_id = (
                                sig.get("queue_id")
                                or sig.get("trade_queue_id")
                                or (getattr(approved_plan, "metadata", None) or {}).get("queue_id")
                            )
                            write_deferred_breach_last_error(
                                _cap_queue_id,
                                reason_code=f"ACCEPTANCE_CAP_MISCONFIGURED:{_cap_error}",
                                explanation=f"acceptance cap misconfigured: {_cap_error}",
                                attempt=1,
                                client_id=_breach_client_id,
                                ticker=ticker,
                            )
                        except Exception as _cap_obs_exc:
                            log.debug("[%s] cap misconfigured queue write non-critical: %s",
                                      ticker, _cap_obs_exc)
                        return _terminalize_deferred_breach_failure(
                            _cap_reason,
                            extra_meta={
                                "failure_stage": "acceptance_ask_cap",
                                "acceptance_cap_error": _cap_error,
                                "selected_contract": _sel_contract,
                            },
                        )
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
                        # Write breach last error to trade_queue BEFORE terminalizing
                        # so Supabase shows the cap block reason without reading Render logs.
                        try:
                            from ap.queue import write_deferred_breach_last_error
                            _cap_queue_id2 = (
                                sig.get("queue_id")
                                or sig.get("trade_queue_id")
                                or (getattr(approved_plan, "metadata", None) or {}).get("queue_id")
                            )
                            write_deferred_breach_last_error(
                                _cap_queue_id2,
                                reason_code="ACCEPTANCE_ASK_CAP_EXCEEDED",
                                explanation=(
                                    f"acceptance ask cap exceeded: ask={_sel_ask:.2f} "
                                    f"cap={_accept_cap:.2f} contract={_sel_contract}"
                                ),
                                attempt=1,
                                client_id=_breach_client_id,
                                ticker=ticker,
                            )
                        except Exception as _cap_obs_exc2:
                            log.debug("[%s] cap exceeded queue write non-critical: %s",
                                      ticker, _cap_obs_exc2)
                        return _terminalize_deferred_breach_failure(
                            _cap_reason,
                            extra_meta={
                                "failure_stage": "acceptance_ask_cap",
                                "selected_contract": _sel_contract,
                                "selected_ask": _sel_ask,
                                "acceptance_cap": _accept_cap,
                            },
                        )
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
                    _sf_dict = _sf if isinstance(_sf, dict) else {}
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
                        "raw_selector_reason": _sf_dict.get("raw_reason"),
                        # P0 PR #302: pass through selector_failure fields that
                        # were previously lost (not included in audit dict).
                        "chain_rows":            int(_sf_dict.get("chain_rows") or 0),
                        "survivor_count":        int(_sf_dict.get("survivor_count") or 0),
                        "top_reject_buckets":    _sf_dict.get("top_reject_buckets") or {},
                        "best_rejected_candidate": _sf_dict.get("best_rejected_candidate"),
                        "quote_source":          _sf_dict.get("quote_source") or "unknown",
                        "tradier_base_url":      _sf_dict.get("tradier_base_url") or "",
                        "sandbox_mode":          bool(_sf_dict.get("sandbox_mode", False)),
                        # Fix 3: failure classification
                        "selector_failure_class": _sf_dict.get("selector_failure_class"),
                        "data_failure":           bool(_sf_dict.get("data_failure", False)),
                        "quality_failure":        bool(_sf_dict.get("quality_failure", False)),
                        # Fix 4: chain quote validity
                        "chain_quote_validity":   _sf_dict.get("selector_chain_quote_validity"),
                        "nonzero_quote_rows":     int(
                            (_sf_dict.get("selector_chain_quote_validity") or {})
                            .get("rows_with_bid_and_ask_gt_zero") or 0
                        ),
                        "zero_quote_ratio":       float(
                            (_sf_dict.get("selector_chain_quote_validity") or {})
                            .get("zero_quote_ratio") or 0.0
                        ),
                        # Fix 2: direct quote recovery
                        "direct_quote_recovery_attempted": bool(
                            _sf_dict.get("direct_quote_recovery_attempted", False)),
                        "direct_quote_recovery_selected":  bool(
                            _sf_dict.get("direct_quote_recovery_selected", False)),
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
                    _MAX_RETRIES_A = _positive_int_env_config(
                        "MAX_BREACH_SELECTOR_RETRIES", 5
                    )
                    _RETRY_DELAY_A = _positive_int_env_config(
                        "BREACH_SELECTOR_RETRY_DELAY_SECONDS", 8
                    )
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
                        # P0 (PR #299): canonical retry marker.
                        log.warning(
                            "DEFERRED_MATERIALIZATION_RETRY "
                            "order_id=%s client_id=%s execution_mode=%s symbol=%s "
                            "direction=%s failure_reason=%s retry_count=%d "
                            "contract_before=%s limit_before=%.4f",
                            str(queue_local_order_id or ""),
                            str(_breach_client_id or ""),
                            str(getattr(approved_plan, "execution_mode", "") or ""),
                            ticker,
                            str(getattr(approved_plan, "side", "") or ""),
                            str(_obs_rc_a or ""),
                            int(_this_attempt_a),
                            str(_contract_sym_raw or ""),
                            float(getattr(approved_plan, "limit_price", 0) or 0),
                        )
                        # Transfer the fenced claim to a durable RETRY_WAIT row.
                        # The watcher remains the runnable owner in this process;
                        # startup recovery consumes the same next_retry_at after a
                        # restart.  There is no thread-only scheduler or sleep.
                        _next_retry_at_a = (
                            datetime.now(timezone.utc)
                            + timedelta(seconds=int(_RETRY_DELAY_A))
                        ).isoformat()
                        _schedule_retry = getattr(
                            self.order_state_machine,
                            "schedule_deferred_materialization_retry",
                            None,
                        )
                        _retry_persisted = False
                        if callable(_schedule_retry):
                            try:
                                _retry_persisted = bool(_schedule_retry(
                                    str(queue_local_order_id or ""),
                                    owner=_mat_owner,
                                    generation=_mat_generation,
                                    reason_code=str(_obs_rc_a or ""),
                                    attempt=int(_this_attempt_a),
                                    max_attempts=int(_MAX_RETRIES_A),
                                    next_retry_at=_next_retry_at_a,
                                    selector_failure={
                                        **(_deferred_selector_audit or {}),
                                        **_build_deferred_retry_schedule_meta(
                                            reason_code=_obs_rc_a,
                                            selector_audit=_deferred_selector_audit or {},
                                            attempt=_this_attempt_a,
                                            max_attempts=_MAX_RETRIES_A,
                                            delay_seconds=_RETRY_DELAY_A,
                                            client_id=_breach_client_id,
                                            execution_mode=str(
                                                getattr(approved_plan, "execution_mode", "") or ""
                                            ),
                                            local_order_id=str(queue_local_order_id or ""),
                                            signal_id=str(
                                                getattr(approved_plan, "signal_id", "") or ""
                                            ),
                                        ),
                                    },
                                    selector_recovery_cursor=(
                                        _selector_recovery_cursor
                                        if _cursor_enabled
                                        else None
                                    ),
                                    signal_id=str(
                                        getattr(approved_plan, "signal_id", "") or ""
                                    ),
                                    execution_mode=_mat_exec_mode,
                                ))
                            except Exception as _schedule_exc:
                                log.critical(
                                    "[%s] MATERIALIZATION_RETRY_SCHEDULE_WRITE_FAILED "
                                    "order=%s error=%s",
                                    ticker, queue_local_order_id, _schedule_exc,
                                )
                        if not _retry_persisted:
                            return {
                                "disposition": "KEEP_WATCHER",
                                "reason_code": "MATERIALIZATION_RETRY_SCHEDULE_WRITE_FAILED",
                                "retry_after_seconds": 5,
                            }

                        try:
                            _ap_meta_a = getattr(approved_plan, "metadata", None)
                            if isinstance(_ap_meta_a, dict):
                                _ap_meta_a["breach_attempt_count"] = _this_attempt_a
                                _ap_meta_a["materialization_generation"] = _mat_generation
                        except Exception:
                            pass
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
                        return {
                            "disposition": "RETRY_WAIT",
                            "reason_code": str(_obs_rc_a or ""),
                            "next_retry_at": _next_retry_at_a,
                            "retry_attempt": int(_this_attempt_a),
                        }

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
                    # P0 (PR #299): canonical failure marker with candidate-scan counts
                    # sourced from the selector audit. Operators filter on this to see
                    # exactly how many contracts were scanned, how many failed each gate,
                    # and what the best rejected candidate looked like.
                    _sel_fail = _deferred_selector_audit or {}
                    log.critical(
                        "DEFERRED_MATERIALIZATION_FAILED "
                        "order_id=%s client_id=%s execution_mode=%s symbol=%s "
                        "direction=%s status=%s failure_reason=%s retry_count=%d "
                        "contract_before=%s limit_before=%.4f "
                        "chain_rows=%d rejected_by_oi=%d rejected_by_spread=%d "
                        "rejected_by_volume=%d rejected_by_zero_bid_ask=%d "
                        "best_candidate=%s best_candidate_rejection=%s",
                        str(queue_local_order_id or ""),
                        str(_breach_client_id or ""),
                        str(getattr(approved_plan, "execution_mode", "") or ""),
                        ticker,
                        str(getattr(approved_plan, "side", "") or ""),
                        _cs_status_a,
                        str(_decision_a.get("terminal_reason") or _reason or ""),
                        int(_this_attempt_a),
                        str(_contract_sym_raw or ""),
                        float(getattr(approved_plan, "limit_price", 0) or 0),
                        int(_sel_fail.get("chain_rows") or 0),
                        int((_sel_fail.get("top_reject_buckets") or {}).get("OI_TOO_LOW", 0)),
                        int((_sel_fail.get("top_reject_buckets") or {}).get("SPREAD_TOO_WIDE", 0)),
                        int((_sel_fail.get("top_reject_buckets") or {}).get("VOLUME_TOO_LOW", 0)),
                        int((_sel_fail.get("top_reject_buckets") or {}).get("CHAIN_ROW_ZERO_BID_ASK", 0))
                        + int((_sel_fail.get("top_reject_buckets") or {}).get("DIRECT_QUOTE_ZERO_BID_ASK", 0)),
                        str((_sel_fail.get("best_rejected_candidate") or {}).get("symbol") or "none"),
                        str((_sel_fail.get("best_rejected_candidate") or {}).get("rejection_reason") or ""),
                    )
                    log.critical(
                        "BREACH_TIME_CONTRACT_SELECTION_FAILED "
                        "client=%s ticker=%s reason=%s",
                        _breach_client_id, ticker, _reason,
                    )
                    # P0 (PR #300): stamp FAILED_TERMINAL lifecycle state.
                    try:
                        from ap.deferred_materializer import stamp_failed_terminal
                        stamp_failed_terminal(
                            self.order_state_machine,
                            str(queue_local_order_id or ""),
                            client_id=str(_breach_client_id or ""),
                            execution_mode=str(getattr(approved_plan, "execution_mode", "") or ""),
                            symbol=ticker,
                            direction=str(getattr(approved_plan, "side", "") or ""),
                            reason_code=str(_decision_a.get("terminal_reason") or _reason or ""),
                            attempt=int(_this_attempt_a),
                            selector_failure=_deferred_selector_audit or {},
                        )
                    except Exception as _ft_exc:
                        log.debug("[%s] stamp_failed_terminal non-critical: %s", ticker, _ft_exc)
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
                    # Fix B: persist flat selector attempt audit on terminal failure (Path A)
                    try:
                        _persist_deferred_selector_attempt_audit(
                            self.order_state_machine,
                            str(queue_local_order_id or ""),
                            selector_audit=_deferred_selector_audit or {},
                            attempt_number=_this_attempt_a,
                            execution_mode=str(getattr(approved_plan, "execution_mode", "") or ""),
                            is_paper=bool(getattr(self, "paper", False)),
                            broker_base_url=str(
                                getattr(getattr(self.broker, "cfg", None), "base_url", "") or ""
                            ),
                        )
                    except Exception as _b_term_exc:
                        log.debug("[%s] Fix B terminal audit non-critical: %s", ticker, _b_term_exc)
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
                    return _terminalize_deferred_breach_failure(
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
                    # Fix B: persist flat selector attempt audit on Path B (DEFERRED unresolved)
                    try:
                        _persist_deferred_selector_attempt_audit(
                            self.order_state_machine,
                            str(queue_local_order_id or ""),
                            selector_audit=_deferred_selector_audit or {},
                            attempt_number=_prior_mat_attempt + 1,
                            execution_mode=str(getattr(approved_plan, "execution_mode", "") or ""),
                            is_paper=bool(getattr(self, "paper", False)),
                            broker_base_url=str(
                                getattr(getattr(self.broker, "cfg", None), "base_url", "") or ""
                            ),
                        )
                    except Exception as _b_path_b_exc:
                        log.debug("[%s] Fix B path-B audit non-critical: %s", ticker, _b_path_b_exc)
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
                    return _terminalize_deferred_breach_failure(
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
                # P0 (PR #299): canonical structured selection marker.
                log.info(
                    "DEFERRED_MATERIALIZATION_SELECTED "
                    "order_id=%s client_id=%s execution_mode=%s symbol=%s "
                    "direction=%s contract_after=%s limit_after=%.4f qty=%d",
                    str(queue_local_order_id or ""),
                    str(_breach_client_id or ""),
                    str(getattr(approved_plan, "execution_mode", "") or ""),
                    ticker,
                    str(getattr(approved_plan, "side", "") or ""),
                    str(_live_contract or ""),
                    float(getattr(approved_plan, "limit_price", 0) or 0),
                    int(getattr(approved_plan, "contracts", 0) or 0),
                )
                # Capture complete selector evidence, but do not mark the row
                # broker-ready yet.  The final refreshed submit price is not
                # known until later; readiness is committed atomically with the
                # contract/price/quantity columns immediately before submit.
                _sel_bid  = float(getattr(_sel, "bid", 0) or 0)
                _sel_ask  = float(getattr(_sel, "ask", 0) or 0)
                _sel_mid  = (_sel_bid + _sel_ask) / 2.0 if _sel_bid and _sel_ask else 0.0
                _sel_qty  = int(getattr(approved_plan, "contracts", 1) or 1)
                _sel_lim  = float(getattr(approved_plan, "limit_price", 0) or 0)
                _sel_cost = round(_sel_qty * _sel_lim * 100, 2)
                _selector_materialization_meta = {
                    "selected_bid": _sel_bid,
                    "selected_ask": _sel_ask,
                    "selected_mid": _sel_mid,
                    "selector_pricing_basis": str(
                        getattr(_sel, "pricing_basis", "")
                        or "execution_price_per_share_or_ask"
                    ),
                    "selector_effective_budget": float(
                        getattr(approved_plan, "max_position_usd", 0) or 0
                    ),
                    "selected_expiration": str(
                        getattr(_sel, "expiration_date", "") or ""
                    ) or None,
                    "selected_strike": float(getattr(_sel, "strike", 0) or 0) or None,
                    "selected_option_type": str(
                        getattr(_sel, "option_type", "")
                        or getattr(approved_plan, "side", "")
                    ),
                    "selected_dte": int(getattr(_sel, "dte", 0) or 0) or None,
                    "selected_delta": float(getattr(_sel, "delta", 0) or 0) or None,
                    "selected_open_interest": int(
                        getattr(_sel, "open_interest", 0) or 0
                    ) or None,
                    "selected_volume": int(getattr(_sel, "volume", 0) or 0) or None,
                    "selector_diagnostics": getattr(_sel, "candidate_audit", None) or {},
                }
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
                return _terminalize_deferred_breach_failure(
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

        if queue_local_order_id and _plan_limit <= 0.01:
            _selector_failure_meta = {}
            try:
                _selector_failure_meta = (
                    (getattr(approved_plan, "metadata", None) or {}).get("selector_failure") or {}
                )
            except Exception:
                _selector_failure_meta = {}
            if not isinstance(_selector_failure_meta, dict) or not _selector_failure_meta:
                try:
                    _selected_meta = getattr(locals().get("_sel"), "metadata", None) or {}
                    if isinstance(_selected_meta, dict):
                        _selector_failure_meta = _selected_meta.get("selector_failure") or {}
                except Exception:
                    _selector_failure_meta = {}
            if not isinstance(_selector_failure_meta, dict) or not _selector_failure_meta:
                try:
                    _last_failure = getattr(self.contract_selector, "get_last_failure", lambda: None)()
                    if isinstance(_last_failure, dict):
                        _selector_failure_meta = _last_failure
                except Exception:
                    _selector_failure_meta = {}
            _deferred_selector_audit = (
                dict(_selector_failure_meta) if isinstance(_selector_failure_meta, dict) else {}
            )
            _limit_reason = str(
                _selector_failure_meta.get("reason_code") or "INVALID_EXECUTABLE_LIMIT"
            ).strip() or "INVALID_EXECUTABLE_LIMIT"
            log.critical(
                "[%s] PRODUCTION_ENTRY_BLOCK — deferred selection produced penny limit "
                "contract=%s limit=%.4f reason=%s",
                ticker,
                str(getattr(approved_plan, "contract_symbol", "") or ""),
                _plan_limit,
                _limit_reason,
            )
            return _terminalize_deferred_breach_failure(
                _limit_reason,
                extra_meta={
                    "failure_stage": "deferred_contract_selection",
                    "selected_contract": str(getattr(approved_plan, "contract_symbol", "") or ""),
                    "selected_limit_price": _plan_limit,
                    "deferred_selector_audit": _deferred_selector_audit,
                    "selector_failure": _selector_failure_meta,
                },
            )

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
            "quote_age_ms":                (
                int(_quote_age_ms) if _quote_age_ms is not None else None
            ),
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

            # ── Identity vars — resolved here, before the PR #295 copyback
            # CAS block and before the PR #294 pre-submit proof block, so
            # both can reference them without risk of UnboundLocalError.
            # Defensive getattr: an unusual runner shape must never NameError
            # either identity field — we fall back to empty string, which the
            # CAS meta and proof log will carry rather than crashing.
            _proof_client_id = str(getattr(self, "client_id", "") or "")
            _proof_execution_mode = str(
                getattr(self, "execution_mode", None)
                or getattr(self, "mode", "")
                or ""
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
                            "persist_deferred_broker_ready",
                            None,
                        ) if self.order_state_machine is not None else None
                        if callable(_rec_fn):
                            _copyback_write_ok = bool(_rec_fn(
                                queue_local_order_id,
                                owner=_mat_owner,
                                generation=_mat_generation,
                                signal_id=str(getattr(approved_plan, "signal_id", "") or ""),
                                execution_mode=str(
                                    getattr(approved_plan, "execution_mode", "") or ""
                                ),
                                contract=_cb_contract,
                                limit_price=_cb_limit,
                                qty=_cb_qty,
                                reserved_cost=_cb_cost,
                                selector_meta={
                                    **(locals().get("_selector_materialization_meta") or {}),
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
                        _ap_meta_sel = getattr(approved_plan, "metadata", None)
                        if isinstance(_ap_meta_sel, dict):
                            _ap_meta_sel["broker_ready"] = True
                            _ap_meta_sel["materialization_status"] = "SELECTED"
                            _ap_meta_sel["lifecycle_state"] = "BROKER_READY"
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
            _plan_meta_mapping = (
                _plan_meta_for_confirm
                if isinstance(_plan_meta_for_confirm, Mapping)
                else {}
            )
            # Runtime-owned execution mode is authoritative for this broker
            # boundary. Plan metadata must never be able to relabel LIVE as PAPER.
            _confirmation_execution_mode = _normalize_execution_mode(
                getattr(self, "execution_mode", None)
            )
            if _confirmation_execution_mode is None:
                _confirmation_execution_mode = _normalize_execution_mode(
                    getattr(self, "mode", None)
                )
            if _confirmation_execution_mode is None and isinstance(
                getattr(self, "paper", None), bool
            ):
                _confirmation_execution_mode = (
                    "paper" if self.paper else "live"
                )
            _is_live_submit = _confirmation_execution_mode == "live"
            _live_confirmation_default = _live_confirmation_required()
            if _is_live_submit:
                if not _live_confirmation_default:
                    log.critical(
                        "[%s] LIVE_CONFIRMATION_BYPASSED_BY_ENV — "
                        "LIVE_CONFIRMATION_REQUIRED=0 is set; live submit will "
                        "proceed under legacy confirmation semantics. This must "
                        "never be set in normal operation.",
                        ticker,
                    )
            _sandbox = bool(
                _plan_meta_mapping.get("sandbox_mode")
                or getattr(self.broker, "sandbox", False)
            )
            _underlying_last = None
            _underlying_quote_age_ms = None
            try:
                # Re-use the latest watcher underlying quote if available
                _ul_ask = getattr(watched, "last_quote_ask", None)
                _ul_bid = getattr(watched, "last_quote_bid", None)
                _underlying_quote_age_ms = getattr(watched, "last_quote_age_ms", None)
                if _ul_ask and _ul_bid and _ul_ask > 0 and _ul_bid > 0:
                    _underlying_last = (_ul_ask + _ul_bid) / 2
                elif _ul_ask and _ul_ask > 0:
                    _underlying_last = _ul_ask
            except Exception:
                pass

            _confirm_result = check_entry_confirmation(
                plan             = approved_plan,
                direction        = str(getattr(approved_plan, "side", "") or "").upper(),
                trigger_price    = float(getattr(approved_plan, "trigger_price", 0) or 0) or None,
                live_bid         = _submit_quote_fields.get("submit_bid"),
                live_ask         = _submit_quote_fields.get("submit_ask"),
                live_quote_age_ms= _quote_age_ms if "_quote_age_ms" in dir() else None,
                underlying_last  = _underlying_last,
                underlying_quote_age_ms = _underlying_quote_age_ms,
                decision_option_price = float(_plan_limit or 0) or None,  # use saved pre-overwrite decision price
                score     = float(_sig_for_confirm.get("score") or 0) or None,
                tier      = str(getattr(approved_plan, "tier", "") or ""),
                timeframe = str(_sig_for_confirm.get("timeframe") or "1d"),
                sandbox_mode = _sandbox,
                execution_mode = _confirmation_execution_mode or "",
                live_default_required = _live_confirmation_default,
            )
            # P0 (PR #262): LIVE must block on an unknown/None result with an
            # explicit reason. (The outer except already fails closed on any
            # raise; this names the None-shape case instead of surfacing an
            # AttributeError.)
            if _is_live_submit and _confirm_result is None:
                raise RuntimeError("live_confirmation_error:none_result")
            _confirmation_passed = getattr(_confirm_result, "passed", None)
            if _is_live_submit and not isinstance(_confirmation_passed, bool):
                raise RuntimeError("live_confirmation_error:malformed_result")
            _confirm_meta = _confirm_result.to_meta(
                started_at   = _confirm_result.metadata.get("live_entry_ts", ""),
                completed_at = __import__("datetime").datetime.now(
                    __import__("datetime").timezone.utc).isoformat(),
            )
            _confirm_meta["client_id"] = str(
                (_sig_for_confirm or {}).get("client_id")
                or getattr(self, "client_id", "")
                or ""
            )
            _confirm_meta["execution_mode"] = _confirmation_execution_mode or ""
            if _confirm_meta.get("confirmation_requirement_conflict"):
                log.critical(
                    "[%s] ENTRY_CONFIRM_REQUIREMENT_CONFLICT — failing closed | "
                    "client_id=%s execution_mode=%s",
                    ticker,
                    _confirm_meta["client_id"],
                    _confirm_meta["execution_mode"],
                )
            elif _confirm_meta.get("confirmation_requirement_malformed"):
                log.warning(
                    "[%s] ENTRY_CONFIRM_REQUIREMENT_MALFORMED | client_id=%s "
                    "execution_mode=%s required=%s",
                    ticker,
                    _confirm_meta["client_id"],
                    _confirm_meta["execution_mode"],
                    _confirm_meta.get("confirmation_required"),
                )
            if _confirmation_passed is not True:
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
            _gate_meta_imp = _gate_meta_imp if isinstance(_gate_meta_imp, Mapping) else {}
            _hcqg_imp = _gate_meta_imp.get("hybrid_client_quality_gate") or {}
            _hcqg_imp = _hcqg_imp if isinstance(_hcqg_imp, Mapping) else {}
            # P0 (PR #262): in LIVE the module is required, full stop —
            # reason live_confirmation_required. The plan-flag path below
            # continues to cover client-gated paper flows.
            _live_needs_confirm_imp = (
                (
                    _normalize_execution_mode(getattr(self, "execution_mode", None))
                    or _normalize_execution_mode(getattr(self, "mode", None))
                    or ("paper" if getattr(self, "paper", None) is True else "live")
                ) == "live"
                and _live_confirmation_required()
            )
            _top_required_imp = str(
                _gate_meta_imp.get("confirmation_required", "")
            ).strip().lower() in {"1", "true", "yes", "on"}
            _nested_required_imp = str(
                _hcqg_imp.get("confirmation_required", "")
            ).strip().lower() in {"1", "true", "yes", "on"}
            if _live_needs_confirm_imp or _top_required_imp or _nested_required_imp:
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

            # ── P0 (PR #300): broker_ready gate — FIRST check.
            # broker_ready=True is set by the OSM atomic deferred copyback CAS
            # when a real OCC contract is materialized with limit>0.01 and qty>=1.
            # If it's not True here, the materialization lifecycle did not complete
            # successfully; block broker POST immediately with a clear reason.
            # Read from plan.metadata (in-memory, synced by stamp_selected above).
            try:
                _plan_meta_for_ready = getattr(approved_plan, "metadata", None) or {}
                _broker_ready = _plan_meta_for_ready.get("broker_ready")
            except Exception:
                _broker_ready = None
            if _broker_ready is not True:
                _br_inv_err = "materialization_incomplete_pre_submit:broker_ready_not_set"
                log.critical(
                    "[%s] MATERIALIZATION_PRE_SUBMIT_INVARIANT_FAILED — "
                    "broker_ready=%r | deferred order not broker-ready; "
                    "broker_order_id=null; blocking broker POST",
                    ticker, _broker_ready,
                )
                log.critical(
                    "MATERIALIZATION_PRE_SUBMIT_INVARIANT_FAILED "
                    "order_id=%s client_id=%s execution_mode=%s symbol=%s "
                    "failure_reason=%s contract_before=%s limit_before=%.4f "
                    "materialization_status=not_selected broker_ready=false",
                    str(queue_local_order_id or ""),
                    str(_proof_client_id or ""),
                    str(_proof_execution_mode or ""),
                    ticker,
                    _br_inv_err,
                    str(_pre_contract or ""),
                    float(_pre_limit),
                )
                _terminalize_breach_failure(_br_inv_err)
                return

            # ── P0 amendment #5+#6 (PR #294 final hardening): fail-closed
            # order-row read. Identity vars (_proof_client_id /
            # _proof_execution_mode) are assigned above in the PR #295
            # copyback block — do not re-assign here. Two distinct failure
            # details, both map to TERMINAL_NO_TRADEABLE_CONTRACT:
            #   READ FAILURE      → MATERIALIZATION_ORDER_ROW_UNREADABLE
            #   READABLE MISMATCH → MATERIALIZATION_COPYBACK_MISMATCH
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
                # ── AMENDMENT: PR #323 Seam 1 — PRE_SUBMIT_PROOF_RETRY ──────
                # Previously this path terminalized immediately on a transient
                # DB read failure. That destroyed a valid selected contract.
                #
                # Required behaviour: durably transition to PRE_SUBMIT_PROOF_RETRY,
                # preserving the real OCC contract (already in the row columns from
                # persist_deferred_broker_ready), and schedule a bounded proof retry
                # that only re-reads the row — it does NOT rerun selector unless
                # the quote becomes stale (Seam 2).  On exhaustion the row is
                # terminalized with the exact read failure reason.
                #
                # Two recovery workers cannot double-submit: the OSM CAS on the
                # proof-retry transition requires exact owner+generation, and the
                # downstream submit-intent CAS from the original BROKER_READY path
                # is still required before any broker POST.

                _proof_retry_fn = getattr(
                    self.order_state_machine, "persist_pre_submit_proof_retry", None
                ) if self.order_state_machine is not None else None

                _proof_retry_max = int(os.getenv("PRE_SUBMIT_PROOF_RETRY_MAX", "3"))
                _proof_retry_delay = int(os.getenv("PRE_SUBMIT_PROOF_RETRY_DELAY_SECONDS", "5"))
                _proof_retry_deadline_s = int(os.getenv("PRE_SUBMIT_PROOF_RETRY_DEADLINE_SECONDS", "60"))
                _now_proof = datetime.now(timezone.utc)
                _proof_next_retry_at = (
                    _now_proof + timedelta(seconds=_proof_retry_delay)
                ).isoformat()
                _proof_deadline = (
                    _now_proof + timedelta(seconds=_proof_retry_deadline_s)
                ).isoformat()

                _proof_persist_ok = False
                if callable(_proof_retry_fn):
                    try:
                        _proof_persist_ok = bool(_proof_retry_fn(
                            queue_local_order_id,
                            owner=str(_deferred_claim_context.get("owner") or ""),
                            generation=int(_deferred_claim_context.get("generation") or 1),
                            retry_attempt=1,
                            max_attempts=_proof_retry_max,
                            next_retry_at=_proof_next_retry_at,
                            retry_deadline=_proof_deadline,
                            read_error=str(_row_read_reason or ""),
                            selected_at=str(_handoff_snapshot.get("selected_at") or _now_proof.isoformat()),
                            selected_quote_at=str(_handoff_snapshot.get("selected_quote_at") or _now_proof.isoformat()),
                        ))
                    except Exception as _pfr_exc:
                        log.critical(
                            "[%s] PRE_SUBMIT_PROOF_RETRY persist raised local_order_id=%s exc=%s",
                            ticker, queue_local_order_id, _pfr_exc,
                        )

                if _proof_persist_ok:
                    # Row is now in PRE_SUBMIT_PROOF_RETRY.  Selected contract,
                    # qty, limit are preserved in the row.  Recovery will retry
                    # the order-row proof via the recovery loop on next startup.
                    # Emit a diagnostic outcome (not a terminal outcome).
                    log.warning(
                        "[%s] PRE_SUBMIT_PROOF_RETRY scheduled local_order_id=%s "
                        "read_error=%s contract=%s owner=%s generation=%s "
                        "next_retry_at=%s deadline=%s",
                        ticker, queue_local_order_id, _row_read_reason,
                        _pre_contract,
                        _deferred_claim_context.get("owner"),
                        _deferred_claim_context.get("generation"),
                        _proof_next_retry_at, _proof_deadline,
                    )
                    _emit_deferred_outcome(
                        "DEFERRED_PRE_SUBMIT_PROOF_RETRY",
                        reason=f"order_row_unreadable_proof_retry_scheduled:{_row_read_reason}",
                        contract=str(_pre_contract or ""),
                        extra={
                            "failure_stage":         "handoff_order_row_read",
                            "proof_retry_scheduled": True,
                            "proof_retry_deadline":  _proof_deadline,
                            "absolute_entry_deadline": _proof_deadline,
                            "original_trigger_crossed_at": str(
                                _handoff_snapshot.get("trigger_crossed_at")
                                or getattr(watched, "trigger_crossed_at", "")
                            ),
                            "order_row_read_error":  _row_read_reason,
                            "pre_submit_contract":   _pre_contract,
                            "pre_submit_limit":      _pre_limit,
                            "pre_submit_qty":        _pre_qty,
                            "local_order_id":        str(queue_local_order_id or ""),
                            "client_id":             _proof_client_id,
                            "execution_mode":        _proof_execution_mode,
                        },
                    )
                    return
                else:
                    # Proof-retry persistence failed (CAS miss, OSM unavailable, etc.).
                    # Fall back to the previous terminal behaviour — better to
                    # terminalize with a clear reason than leave an orphan row.
                    _inv_err = "MATERIALIZATION_ORDER_ROW_UNREADABLE"
                    log.critical(
                        "[%s] PRE_SUBMIT_INVARIANT_FAILED — %s | "
                        "reason=%s local_order_id=%s | "
                        "selector proved real OCC contract but persisted "
                        "order row is unreadable and proof_retry persistence failed — "
                        "broker_order_id=null; terminalizing",
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
                            "proof_retry_persist_failed":      True,
                            "selector_contract":               _handoff_snapshot.get("selector_contract"),
                            "selector_bid":                    _handoff_snapshot.get("selector_bid"),
                            "selector_ask":                    _handoff_snapshot.get("selector_ask"),
                            "selector_mid":                    _handoff_snapshot.get("selector_mid"),
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
                    _handoff_snapshot["order_row_limit"] = float(
                        _row_d.get("limit_price") or 0
                    )
                    _handoff_snapshot["order_row_qty"] = int(_row_d.get("qty") or 0)
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
                order_row_limit=_handoff_snapshot.get("order_row_limit"),
                order_row_qty=_handoff_snapshot.get("order_row_qty"),
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
                log.critical(
                    "MATERIALIZATION_PRE_SUBMIT_INVARIANT_FAILED "
                    "order_id=%s client_id=%s execution_mode=%s symbol=%s "
                    "failure_reason=%s contract_before=%s limit_before=%.4f",
                    str(queue_local_order_id or ""),
                    str(_proof_client_id or ""),
                    str(_proof_execution_mode or ""),
                    ticker,
                    _inv_err,
                    str(_pre_contract or ""),
                    float(_pre_limit),
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
                log.critical(
                    "MATERIALIZATION_PRE_SUBMIT_INVARIANT_FAILED "
                    "order_id=%s client_id=%s execution_mode=%s symbol=%s "
                    "failure_reason=%s contract_before=%s limit_before=%.4f",
                    str(queue_local_order_id or ""),
                    str(_proof_client_id or ""),
                    str(_proof_execution_mode or ""),
                    ticker,
                    _inv_err,
                    str(_pre_contract or ""),
                    float(_pre_limit),
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

        # ── P0 (#301) Fix A: resolve_positive_underlying_for_breach ──────────────
        # Mandatory before submit_existing_entry() for all breach orders.
        # For deferred: missing underlying terminates with a durable audit.
        # For non-deferred: best-effort patch only.
        # Log markers (literal strings — grep-able in Render):
        #   ZERO_UNDERLYING_REPAIRED / ZERO_UNDERLYING_NO_POSITIVE_SOURCE
        # ─────────────────────────────────────────────────────────────────────
        try:
            from ap.deferred_breach_underlying_repair import (
                resolve_positive_underlying_for_breach as _resolve_underlying,
                build_underlying_patch as _build_underlying_patch,
                build_no_source_meta as _build_no_source_meta,
                check_paper_selector_data_domain as _check_paper_domain,
                ZERO_UNDERLYING_TERMINAL_REASON as _ZU_TERMINAL,
                PAPER_SELECTOR_DATA_DOMAIN_BLOCKED as _PAPER_DOMAIN_BLOCKED,
            )
            # Inline log markers — must remain as string literals for Render grep
            _ZU_REPAIRED  = "ZERO_UNDERLYING_REPAIRED"
            _ZU_NO_SOURCE = "ZERO_UNDERLYING_NO_POSITIVE_SOURCE"

            _sel_for_udl       = _sel if _deferred else None              # type: ignore[name-defined]
            # _deferred_selector_audit is only assigned on failure paths — safe access
            _sel_audit_for_udl = (
                locals().get("_deferred_selector_audit") or {}
            ) if _deferred else {}
            _order_row_meta_udl: dict = {}
            try:
                _orm_raw = getattr(self.order_state_machine, "_get_order", lambda x: {})(
                    queue_local_order_id
                ) or {}
                _order_row_meta_udl = dict(_orm_raw.get("meta") or _orm_raw.get("metadata") or {})
            except Exception:
                pass

            _udl_price, _udl_source, _udl_audit = _resolve_underlying(
                approved_plan=approved_plan,
                watched=watched,
                sig=sig,
                selector_audit=_sel_audit_for_udl,
                selector_result=_sel_for_udl,
                order_row_meta=_order_row_meta_udl,
            )

            _is_paper_udl  = bool(getattr(self, "paper", False))
            _broker_url_udl = str(
                getattr(getattr(self.broker, "cfg", None), "base_url", "") or
                getattr(self.broker, "base_url", "") or ""
            )

            if _deferred:
                # ── Fix C: paper data-domain enforcement (before underlying check) ──
                if _is_paper_udl:
                    _paper_block, _paper_block_reason = _check_paper_domain(
                        is_paper=True,
                        selector_audit=_sel_audit_for_udl,
                        broker_base_url=_broker_url_udl,
                    )
                    if _paper_block and _paper_block_reason:
                        log.critical(
                            "%s order_id=%s symbol=%s execution_mode=%s "
                            "failure_reason=%s",
                            "PAPER_SELECTOR_DATA_DOMAIN_BLOCKED",
                            str(queue_local_order_id or ""),
                            ticker,
                            str(_proof_execution_mode or ""),
                            _paper_block_reason,
                        )
                        try:
                            _upd_pb = getattr(self.order_state_machine, "update_order_meta", None)
                            if callable(_upd_pb) and queue_local_order_id:
                                _upd_pb(queue_local_order_id, {
                                    "paper_domain_block_reason": _paper_block_reason,
                                    "paper_domain_block_at": datetime.now(timezone.utc).isoformat(),
                                })
                        except Exception:
                            pass
                        _terminalize_breach_failure(_paper_block_reason)
                        return

                if _udl_price is None or _udl_price <= 0:
                    # Fail closed — persist full candidate audit BEFORE terminalize
                    log.critical(
                        "%s order_id=%s client_id=%s execution_mode=%s symbol=%s "
                        "failure_reason=%s broker_ready=blocked contract=%s limit=%.4f",
                        _ZU_NO_SOURCE,
                        str(queue_local_order_id or ""),
                        str(_proof_client_id or ""),
                        str(_proof_execution_mode or ""),
                        ticker,
                        _ZU_TERMINAL,
                        str(approved_contract or ""),
                        float(submit_limit or 0),
                    )
                    try:
                        _no_src_meta = _build_no_source_meta(
                            _udl_audit,
                            order_id=str(queue_local_order_id or ""),
                            ticker=ticker,
                        )
                        _upd_zu = getattr(self.order_state_machine, "update_order_meta", None)
                        if callable(_upd_zu) and queue_local_order_id:
                            _upd_zu(queue_local_order_id, _no_src_meta)
                    except Exception as _zu_persist_exc:
                        log.debug("[%s] zero_underlying no-source meta persist non-critical: %s",
                                  ticker, _zu_persist_exc)
                    _terminalize_breach_failure(_ZU_TERMINAL)
                    return

                # Underlying resolved — patch in-memory plan metadata
                try:
                    _existing_trigger = {}
                    _ap_meta_udl = getattr(approved_plan, "metadata", None)
                    if isinstance(_ap_meta_udl, dict):
                        _existing_trigger = dict(_ap_meta_udl.get("trigger") or {})
                    _udl_patch = _build_underlying_patch(
                        _udl_price, _udl_source or "unknown", _udl_audit,
                        order_id=str(queue_local_order_id or ""),
                        ticker=ticker,
                        existing_trigger=_existing_trigger,
                    )
                    # Patch flat keys + nested trigger into approved_plan.metadata
                    if isinstance(_ap_meta_udl, dict):
                        _ap_meta_udl["underlying_entry"]         = _udl_price
                        _ap_meta_udl["underlying_price"]         = _udl_price
                        _ap_meta_udl["current_underlying_price"] = _udl_price
                        _ap_meta_udl["trigger_current_price"]    = _udl_price
                        _ap_meta_udl["zero_underlying_repair"]   = _udl_patch["zero_underlying_repair"]
                        # nested trigger — merge, never replace the whole dict
                        if "trigger" not in _ap_meta_udl or not isinstance(_ap_meta_udl.get("trigger"), dict):
                            _ap_meta_udl["trigger"] = {}
                        _ap_meta_udl["trigger"]["current_price"] = _udl_price
                    # Persist to orders.meta (best-effort)
                    try:
                        _upd_udl = getattr(self.order_state_machine, "update_order_meta", None)
                        if callable(_upd_udl) and queue_local_order_id:
                            _upd_udl(queue_local_order_id, _udl_patch)
                    except Exception:
                        pass
                    log.info(
                        "%s order_id=%s symbol=%s source=%s underlying=%.4f",
                        _ZU_REPAIRED,
                        str(queue_local_order_id or ""),
                        ticker,
                        str(_udl_source or ""),
                        float(_udl_price),
                    )
                except Exception as _udl_patch_exc:
                    log.warning("[%s] underlying patch non-critical: %s", ticker, _udl_patch_exc)
            else:
                # Non-deferred: best-effort patch — no terminalize on failure
                if _udl_price and _udl_price > 0:
                    try:
                        _existing_trigger_nd: dict = {}
                        _ap_meta_nd = getattr(approved_plan, "metadata", None)
                        if isinstance(_ap_meta_nd, dict):
                            _existing_trigger_nd = dict(_ap_meta_nd.get("trigger") or {})
                        _udl_patch_nd = _build_underlying_patch(
                            _udl_price, _udl_source or "unknown", _udl_audit,
                            order_id=str(queue_local_order_id or ""),
                            ticker=ticker,
                            existing_trigger=_existing_trigger_nd,
                        )
                        if isinstance(_ap_meta_nd, dict):
                            _ap_meta_nd.update({
                                "underlying_entry":         _udl_price,
                                "underlying_price":         _udl_price,
                                "current_underlying_price": _udl_price,
                                "trigger_current_price":    _udl_price,
                            })
                            if "trigger" not in _ap_meta_nd or not isinstance(_ap_meta_nd.get("trigger"), dict):
                                _ap_meta_nd["trigger"] = {}
                            _ap_meta_nd["trigger"]["current_price"] = _udl_price
                    except Exception:
                        pass

        except ImportError as _udl_import_err:
            log.warning("[%s] deferred_breach_underlying_repair import failed: %s",
                        ticker, _udl_import_err)
        except Exception as _udl_exc:
            log.critical("[%s] UNEXPECTED_UNDERLYING_RESOLVE_ERROR order_id=%s error=%s",
                         ticker, str(queue_local_order_id or ""), _udl_exc)
            if _deferred:
                _terminalize_breach_failure(f"underlying_resolve_error:{_udl_exc}")
                return

        # ── Fix B + Fix C: persist flat selector attempt audit at submit ──────
        if _deferred:
            try:
                _persist_deferred_selector_attempt_audit(
                    self.order_state_machine,
                    str(queue_local_order_id or ""),
                    selector_audit=(locals().get("_deferred_selector_audit") or {}) if _deferred else {},
                    attempt_number=(locals().get("_prior_mat_attempt") or 0) + 1,
                    execution_mode=str(_proof_execution_mode or ""),
                    is_paper=bool(getattr(self, "paper", False)),
                    broker_base_url=_broker_url_udl if "_broker_url_udl" in dir() else "",
                )
            except Exception as _b_submit_exc:
                log.debug("[%s] Fix B submit audit non-critical: %s", ticker, _b_submit_exc)

        # P0 (PR #299): invariant OK marker — emitted only on deferred rows
        # that passed every pre-submit check. Operators filter on this to
        # confirm the broker submit path was actually reached with a real contract.
        if _deferred:
            log.info(
                "MATERIALIZATION_PRE_SUBMIT_INVARIANT_OK "
                "order_id=%s client_id=%s execution_mode=%s symbol=%s "
                "contract_after=%s limit_after=%.4f qty=%d",
                str(queue_local_order_id or ""),
                str(_proof_client_id or ""),
                str(_proof_execution_mode or ""),
                ticker,
                str(approved_contract or ""),
                float(submit_limit or 0),
                int(approved_qty or 0),
            )

        # ── P0 (PR #305): LIVE submit safety gates ─────────────────────────
        # Three brakes between materialization completion and the broker POST:
        #   1. IDENTITY   — client_id + execution_mode must be non-empty,
        #                   canonical, and self-consistent
        #   2. MARKET     — fresh underlying quote must confirm the setup is
        #                   still tradeable (not stale/reversed/target-hit)
        #   3. AGE        — time from first breach to submit must not exceed
        #                   ENTRY_TRIGGER_MAX_AGE_SEC
        #
        # Each gate is a pure classifier from ap.live_submit_gates. On FAIL
        # in LIVE mode: block broker submit, terminalize the row with the
        # Intelligence already dispatched at plan-confirmation seam above.
        # _ensure_intelligence_dispatched is idempotent — this is a no-op.

        # canonical reason_code, stamp orders.meta.live_submit_gate with
        # full audit evidence, and return. PAPER logs failures but proceeds
        # so sandbox flow can be exercised.
        try:
            from ap.live_submit_gates import (
                derive_submit_execution_mode,
                resolve_trigger_timestamps,
                check_identity_gate,
                check_market_validity_gate,
                check_trigger_age_gate,
                GateOutcome,
            )

            # Amendment 4 (hardened): collect all 6 client_id sources and verify
            # they agree before accepting any as canonical. A stale/wrong
            # _proof_client_id wins silently if we just return the first nonblank —
            # that violates client_id attribution for LIVE money.
            def _resolve_gate_client_id() -> tuple:
                """
                Returns (canonical_client_id, mismatch_reason).
                canonical is the agreed value across all nonblank sources.
                mismatch_reason is non-empty if any two sources disagree.
                """
                _sig_local = sig if isinstance(sig, dict) else {}
                _cid_sources: dict = {
                    "_proof_client_id":       str(_proof_client_id or "").strip(),
                    "plan.client_id":         str(getattr(approved_plan, "client_id", "") or "").strip(),
                    "sig.client_id":          str(_sig_local.get("client_id") or "").strip(),
                    "sig.original_client_id": str(_sig_local.get("original_client_id") or "").strip(),
                }
                try:
                    _osm_row = (
                        getattr(self.order_state_machine, "get_order", lambda x: {})(
                            queue_local_order_id
                        ) or {}
                    ) if self.order_state_machine else {}
                    _cid_sources["osm_order.client_id"] = str(_osm_row.get("client_id") or "").strip()
                except Exception:
                    _cid_sources["osm_order.client_id"] = ""
                _cid_sources["osm.client_id"]  = str(getattr(self.order_state_machine, "client_id", "") or "").strip()
                _cid_sources["self.client_id"] = str(getattr(self, "client_id", "") or "").strip()

                _nonblank = {k: v for k, v in _cid_sources.items() if v}
                if not _nonblank:
                    return "", ""  # all blank — gate will fail closed on missing client_id

                _unique = set(_nonblank.values())
                if len(_unique) > 1:
                    # Positive disagreement between sources — fail closed regardless of which wins.
                    _detail = "; ".join(f"{k}={v!r}" for k, v in sorted(_nonblank.items()))
                    return "", f"client_id_source_mismatch [{_detail}]"

                return _unique.pop(), ""

            _gate_client_id, _gate_client_id_mismatch = _resolve_gate_client_id()
            if _gate_client_id_mismatch:
                log.critical(
                    "LIVE_SUBMIT_GATE_BLOCKED gate=client_id_source_mismatch "
                    "order_id=%s detail=%s",
                    str(queue_local_order_id or ""), _gate_client_id_mismatch,
                )
                try:
                    self.order_state_machine.update_order_meta(
                        str(queue_local_order_id or ""),
                        {"live_submit_gate": {
                            "all_passed": False,
                            "failed_gate": "client_id_source_mismatch",
                            "detail": _gate_client_id_mismatch[:400],
                        }},
                    )
                except Exception as _mism_meta_exc:
                    log.warning(
                        "[%s] LIVE_SUBMIT_GATE_AUDIT_WRITE_FAILED gate=client_id_source_mismatch "
                        "order_id=%s error=%s — submit still blocked",
                        ticker, str(queue_local_order_id or ""), _mism_meta_exc,
                    )
                _terminalize_breach_failure("live_submit_gate:CLIENT_ID_SOURCE_MISMATCH")
                return
            _final_market_validity_audit = {
                "gate": "market_validity",
                "checked_at": __import__("datetime").datetime.now(
                    __import__("datetime").timezone.utc
                ).isoformat(),
                "not_run": True,
                "reason": "not_reached",
                "execution_mode": None,
                "symbol": ticker,
            }

            # ── Amendment 1: execution_mode fallback chain ──────────────────
            # _proof_execution_mode is the primary source but may be blank.
            # Derive from 5 sources in priority order so LIVE never silently
            # submits with an unknown mode.
            _gate_exec_mode = derive_submit_execution_mode(
                proof_execution_mode=_proof_execution_mode,
                approved_plan_execution_mode=getattr(approved_plan, "execution_mode", None),
                self_execution_mode=getattr(self, "execution_mode", None),
                self_mode=getattr(self, "mode", None),
                self_paper=getattr(self, "paper", None),
                osm_execution_mode=(
                    getattr(self.order_state_machine, "execution_mode", None)
                    if self.order_state_machine else None
                ),
            )
            _gate_is_live   = _gate_exec_mode == "live"
            _final_market_validity_audit["execution_mode"] = _gate_exec_mode or None

            # ── Amendment 5: DEFERRED contract hard-stop ────────────────────
            # A DEFERRED:* contract must never reach broker POST. If we're
            # still DEFERRED at the gate, block with a specific reason code
            # that is distinct from the materialization invariant check that
            # fires earlier. This is a belt-and-suspenders guard.
            _gate_contract = str(approved_contract or "").strip()
            if _gate_contract.upper().startswith("DEFERRED:") or not _gate_contract:
                _deferred_reason = "LIVE_SUBMIT_CONTRACT_NOT_MATERIALIZED"
                log.critical(
                    "LIVE_SUBMIT_GATE_BLOCKED gate=contract_check reason=%s "
                    "order_id=%s client_id=%s execution_mode=%s symbol=%s "
                    "contract=%r — blocking broker POST on placeholder contract",
                    _deferred_reason,
                    str(queue_local_order_id or ""), _gate_client_id,
                    _gate_exec_mode, ticker, _gate_contract,
                )
                try:
                    self.order_state_machine.update_order_meta(
                        str(queue_local_order_id or ""),
                        {"live_submit_gate": {
                            "failed_gate": "contract_check",
                            "reason_code": _deferred_reason,
                            "contract": _gate_contract,
                        },
                        "final_market_validity": {
                            **_final_market_validity_audit,
                            "reason": _deferred_reason,
                            "contract": _gate_contract,
                        }},
                    )
                except Exception as _def_meta_exc:
                    log.warning(
                        "[%s] LIVE_SUBMIT_GATE_AUDIT_WRITE_FAILED gate=deferred_hard_stop "
                        "order_id=%s error=%s — submit still blocked",
                        ticker, str(queue_local_order_id or ""), _def_meta_exc,
                    )
                _terminalize_breach_failure(f"live_submit_gate:{_deferred_reason}")
                return

            # ── Amendment 2: read trigger timestamps from durable meta ───────
            # WatchedSignal.check() stamps trigger_crossed_at on first breach
            # AND the watcher persists it to orders.meta on callback success
            # (see ap_entry_watcher.py Amendment 2). Read from meta first,
            # then fall back to in-memory watched object (if in scope), then
            # to approved_plan._watched_signal. This eliminates the fragile
            # locals().get("watched") pattern.
            try:
                # Tier 1: durable orders.meta (most reliable)
                _meta_for_ts = {}
                try:
                    _get_order = getattr(self.order_state_machine, "get_order", None)
                    if callable(_get_order) and queue_local_order_id:
                        _order_row = _get_order(str(queue_local_order_id)) or {}
                        _meta_for_ts = _order_row.get("meta") or {}
                        if isinstance(_meta_for_ts, str):
                            import json as _json
                            try:
                                _meta_for_ts = _json.loads(_meta_for_ts)
                            except Exception:
                                _meta_for_ts = {}
                except Exception:
                    _meta_for_ts = {}
                _ta_crossed_at, _ta_confirmed_at = resolve_trigger_timestamps(
                    order_meta=_meta_for_ts,
                    approved_plan_watched_signal=getattr(approved_plan, "_watched_signal", None),
                    watched_signal=watched,
                )
            except Exception as _ts_exc:
                log.debug("[%s] trigger timestamp resolution: %s", ticker, _ts_exc)
                _ta_crossed_at, _ta_confirmed_at = None, None

            # ── Gate 1: identity
            _id_res = check_identity_gate(
                client_id=_gate_client_id,
                execution_mode=_gate_exec_mode,
                osm_client_id=str(getattr(self.order_state_machine, "client_id", "") or ""),
                osm_execution_mode=str(getattr(self.order_state_machine, "execution_mode", "") or ""),
            )
            if not _id_res.passed:
                log.critical(
                    "LIVE_SUBMIT_GATE_BLOCKED gate=identity reason=%s detail=%s "
                    "order_id=%s client_id=%s execution_mode=%s symbol=%s",
                    _id_res.reason_code, _id_res.detail,
                    str(queue_local_order_id or ""), _gate_client_id,
                    _gate_exec_mode, ticker,
                )
                try:
                    self.order_state_machine.update_order_meta(
                        str(queue_local_order_id or ""),
                        {"live_submit_gate": {"failed_gate": "identity", **_id_res.audit},
                         "final_market_validity": {
                             **_final_market_validity_audit,
                             "reason": _id_res.reason_code,
                         }},
                    )
                except Exception as _id_meta_exc:
                    log.warning(
                        "[%s] LIVE_SUBMIT_GATE_AUDIT_WRITE_FAILED gate=identity "
                        "order_id=%s client_id=%s error=%s — submit still blocked",
                        ticker, str(queue_local_order_id or ""), _gate_client_id, _id_meta_exc,
                    )
                _terminalize_breach_failure(f"live_submit_gate:{_id_res.reason_code}")
                return

            # ── Gate 2: market validity — LIVE only fails closed
            # Fetch a fresh underlying quote from the broker. If the fetch
            # itself fails, classify CURRENT_PRICE_FETCH_FAILED (fail closed LIVE).
            # Amendment 6: quote adapter — try the real production quote path.
            # Use the same quote provider shape the watcher and selector use.
            # Supports bid/ask/source/quote_age_ms. Provider age is optional on
            # a synchronous response; explicit fetch provenance below proves
            # freshness without pretending that a cached/unknown quote is new.
            _mv_bid = None
            _mv_ask = None
            _mv_quote_age_ms = None
            _mv_quote_source = None
            _mv_quote_fetched_at = None
            _mv_quote_provenance = None
            _mv_quote_fetch_failed = True
            _mv_quote_fetch_error = "quote_adapter_unavailable"
            try:
                _mv_q: dict = {}
                _mv_raw_q = None
                # Method 1: broker.get_quote() — primary production path
                if hasattr(self.broker, "get_quote"):
                    _mv_raw_q = self.broker.get_quote(ticker)
                    _mv_quote_fetched_at = datetime.now(timezone.utc).isoformat()
                # Method 2: broker.get_bid_ask() — alternate production adapter
                elif hasattr(self.broker, "get_bid_ask"):
                    _ba = self.broker.get_bid_ask(ticker)
                    _mv_quote_fetched_at = datetime.now(timezone.utc).isoformat()
                    if isinstance(_ba, dict):
                        _mv_raw_q = {
                            "bid": _ba.get("bid"),
                            "ask": _ba.get("ask"),
                            "quote_age_ms": _ba.get("quote_age_ms"),
                            "source": _ba.get("source", "get_bid_ask"),
                        }
                    else:
                        _mv_raw_q = _ba
                # Method 3: broker.quote() — older adapter shape
                elif hasattr(self.broker, "quote"):
                    _mv_raw_q = self.broker.quote(ticker)
                    _mv_quote_fetched_at = datetime.now(timezone.utc).isoformat()
                # Method 4: data_broker attr (paper pods may wire data separately)
                elif hasattr(self, "data_broker") and hasattr(self.data_broker, "get_quote"):
                    _mv_raw_q = self.data_broker.get_quote(ticker)
                    _mv_quote_fetched_at = datetime.now(timezone.utc).isoformat()

                if isinstance(_mv_raw_q, dict):
                    _mv_q = _mv_raw_q
                    _mv_quote_fetch_failed = False
                    _mv_quote_fetch_error = None
                    _mv_quote_provenance = "synchronous_submit_fetch"
                    _mv_bid = _mv_q.get("bid")
                    _mv_ask = _mv_q.get("ask")
                    _mv_quote_age_ms = _mv_q.get("quote_age_ms")
                    _mv_quote_source = (
                        _mv_q.get("source")
                        or _mv_q.get("quote_source")
                        or _mv_q.get("provider")
                        or "unknown"
                    )
                elif _mv_quote_fetched_at is not None:
                    _mv_quote_fetch_error = (
                        f"invalid_quote_response_type:{type(_mv_raw_q).__name__}"
                    )
            except Exception as _mv_exc:
                _mv_quote_fetch_failed = True
                _mv_quote_fetch_error = f"{type(_mv_exc).__name__}:{_mv_exc}"
                log.warning(
                    "[%s] LIVE_SUBMIT_GATE market quote fetch failed "
                    "order_id=%s source=%r error=%s",
                    ticker, str(queue_local_order_id or ""), _mv_quote_source, _mv_exc,
                )
            _mv_res = check_market_validity_gate(
                side=str(getattr(approved_plan, "side", "") or ""),
                trigger_price=float(getattr(approved_plan, "trigger_price", 0) or 0),
                stop_price=(
                    float(getattr(approved_plan, "stop_underlying", 0) or 0)
                    if getattr(approved_plan, "stop_underlying", None) else None
                ),
                target_price=(
                    float(getattr(approved_plan, "target_underlying", 0) or 0)
                    if getattr(approved_plan, "target_underlying", None) else None
                ),
                current_bid=_mv_bid,
                current_ask=_mv_ask,
                quote_age_ms=_mv_quote_age_ms,
                quote_source=_mv_quote_source,
                quote_fetched_at=_mv_quote_fetched_at,
                quote_provenance=_mv_quote_provenance,
                quote_fetch_failed=_mv_quote_fetch_failed,
                quote_fetch_error=_mv_quote_fetch_error,
                execution_mode=_gate_exec_mode,
            )
            _final_market_validity_audit = _mv_res.audit
            if not _mv_res.passed:
                log.critical(
                    "LIVE_SUBMIT_GATE_BLOCKED gate=market_validity reason=%s detail=%s "
                    "order_id=%s client_id=%s symbol=%s",
                    _mv_res.reason_code, _mv_res.detail,
                    str(queue_local_order_id or ""), _gate_client_id, ticker,
                )
                try:
                    self.order_state_machine.update_order_meta(
                        str(queue_local_order_id or ""),
                        {
                            "live_submit_gate": {"failed_gate": "market_validity", **_mv_res.audit},
                            "final_market_validity": _mv_res.audit,
                        },
                    )
                except Exception as _mv_meta_exc:
                    log.warning(
                        "[%s] LIVE_SUBMIT_GATE_AUDIT_WRITE_FAILED gate=market_validity "
                        "order_id=%s reason=%s error=%s — submit still blocked",
                        ticker, str(queue_local_order_id or ""), _mv_res.reason_code, _mv_meta_exc,
                    )
                _terminalize_breach_failure(f"live_submit_gate:{_mv_res.reason_code}")
                return
            # Even on PASS in paper we log the mid so audit trails are complete
            elif not _gate_is_live and _mv_res.audit.get("current_mid"):
                log.info(
                    "[%s] LIVE_SUBMIT_GATE_PASS gate=market_validity paper mid=%s",
                    ticker, _mv_res.audit.get("current_mid"),
                )

            # A market-valid quote is the only event allowed to advance the
            # trigger confirmation anchor. Persist it, then reread the exact
            # durable value used by the age gate. The original breach timestamp
            # is never rewritten.
            _confirm_now = datetime.now(timezone.utc)
            _absolute_deadline_raw = _meta_for_ts.get("absolute_entry_deadline")
            if _absolute_deadline_raw:
                try:
                    _absolute_deadline = datetime.fromisoformat(
                        str(_absolute_deadline_raw).replace("Z", "+00:00")
                    )
                    if _absolute_deadline.tzinfo is None:
                        _absolute_deadline = _absolute_deadline.replace(tzinfo=timezone.utc)
                except Exception:
                    _absolute_deadline = None
                if _absolute_deadline is None or _confirm_now >= _absolute_deadline:
                    _terminalize_breach_failure("live_submit_gate:ABSOLUTE_ENTRY_DEADLINE_EXCEEDED")
                    return

            from zoneinfo import ZoneInfo as _ZoneInfo
            _entry_cutoff_hhmm = int(
                _meta_for_ts.get("entry_cutoff_et")
                or os.getenv("ENTRY_CUTOFF_ET_HHMM", "1530")
            )
            _confirm_et = _confirm_now.astimezone(_ZoneInfo("America/New_York"))
            if _gate_is_live and (_confirm_et.hour * 100 + _confirm_et.minute) >= _entry_cutoff_hhmm:
                _terminalize_breach_failure("live_submit_gate:ENTRY_CUTOFF_EXCEEDED")
                return

            _last_confirmed_candidate = _confirm_now.isoformat()
            if not self.order_state_machine.update_order_meta(
                str(queue_local_order_id or ""),
                {
                    "last_confirmed_trigger_at": _last_confirmed_candidate,
                    "original_trigger_crossed_at": (
                        _meta_for_ts.get("original_trigger_crossed_at")
                        or _ta_crossed_at
                    ),
                    "last_trigger_confirmation_quote": _mv_res.audit,
                },
            ):
                _terminalize_breach_failure("live_submit_gate:TRIGGER_CONFIRMATION_PERSIST_FAILED")
                return
            _confirmed_row = self.order_state_machine.get_order(
                str(queue_local_order_id or "")
            ) or {}
            _confirmed_meta = _confirmed_row.get("meta") or {}
            if isinstance(_confirmed_meta, str):
                _confirmed_meta = json.loads(_confirmed_meta)
            _last_confirmed_durable = _confirmed_meta.get("last_confirmed_trigger_at")
            if _last_confirmed_durable != _last_confirmed_candidate:
                _terminalize_breach_failure("live_submit_gate:TRIGGER_CONFIRMATION_REREAD_MISMATCH")
                return

            # ── Gate 3: trigger age (uses the exact durable confirmation)
            _ta_res = check_trigger_age_gate(
                trigger_crossed_at=_ta_crossed_at,
                trigger_confirmed_at=_ta_confirmed_at,
                last_confirmed_trigger_at=_last_confirmed_durable,
                execution_mode=_gate_exec_mode,
            )
            if not _ta_res.passed:
                log.critical(
                    "LIVE_SUBMIT_GATE_BLOCKED gate=trigger_age reason=%s detail=%s "
                    "order_id=%s client_id=%s symbol=%s age_seconds=%s",
                    _ta_res.reason_code, _ta_res.detail,
                    str(queue_local_order_id or ""), _gate_client_id, ticker,
                    _ta_res.audit.get("age_seconds"),
                )
                try:
                    self.order_state_machine.update_order_meta(
                        str(queue_local_order_id or ""),
                        {"live_submit_gate": {"failed_gate": "trigger_age", **_ta_res.audit},
                         "final_market_validity": _final_market_validity_audit},
                    )
                except Exception as _ta_meta_exc:
                    log.warning(
                        "[%s] LIVE_SUBMIT_GATE_AUDIT_WRITE_FAILED gate=trigger_age "
                        "order_id=%s reason=%s error=%s — submit still blocked",
                        ticker, str(queue_local_order_id or ""), _ta_res.reason_code, _ta_meta_exc,
                    )
                _terminalize_breach_failure(f"live_submit_gate:{_ta_res.reason_code}")
                return

            # All three passed — stamp combined audit as evidence.
            log.info(
                "LIVE_SUBMIT_GATES_PASSED "
                "order_id=%s client_id=%s execution_mode=%s symbol=%s "
                "market_mid=%s trigger_age_s=%s",
                str(queue_local_order_id or ""), _gate_client_id,
                _gate_exec_mode, ticker,
                _mv_res.audit.get("current_mid"),
                _ta_res.audit.get("age_seconds"),
            )
            try:
                self.order_state_machine.update_order_meta(
                    str(queue_local_order_id or ""),
                    {"live_submit_gate": {
                        "all_passed": True,
                        "identity_gate": _id_res.audit,
                        "market_validity_gate": _mv_res.audit,
                        "trigger_age_gate": _ta_res.audit,
                    },
                    "final_market_validity": _mv_res.audit,
                    },
                )
            except Exception as _ap_meta_exc:
                log.debug(
                    "[%s] LIVE_SUBMIT_GATE all-passed meta write failed "
                    "order_id=%s error=%s (non-blocking)",
                    ticker, str(queue_local_order_id or ""), _ap_meta_exc,
                )
        except Exception as _gate_exc:
            # If the entire gate module fails, LIVE fails closed. Never let
            # a bug in the safety code allow an unchecked broker submit.
            log.critical(
                "[%s] LIVE_SUBMIT_GATE_MODULE_ERROR — %s — LIVE will block, PAPER will proceed",
                ticker, _gate_exc, exc_info=True,
            )
            _module_error_exec_mode = str(locals().get("_gate_exec_mode") or "").strip().lower()
            if not _module_error_exec_mode:
                for _mode_candidate in (
                    _proof_execution_mode,
                    getattr(approved_plan, "execution_mode", None),
                    getattr(self, "execution_mode", None),
                    getattr(self, "mode", None),
                    "live" if getattr(self, "paper", True) is False else None,
                    getattr(self.order_state_machine, "execution_mode", None)
                    if self.order_state_machine else None,
                ):
                    _candidate = str(_mode_candidate or "").strip().lower()
                    if _candidate in ("live", "paper"):
                        _module_error_exec_mode = _candidate
                        break
            # Amendment 5: fail closed on blank/unknown mode, not only explicit "live".
            # The entire incident class is blank/unknown execution_mode being treated
            # as safe. Unknown cannot be safe — only explicit "paper" may proceed.
            if _module_error_exec_mode != "paper":
                log.critical(
                    "[%s] LIVE_SUBMIT_GATE_MODULE_ERROR_BLOCKING "
                    "mode=%r order_id=%s — blocking submit (unknown mode treated as live)",
                    ticker, _module_error_exec_mode, str(queue_local_order_id or ""),
                )
                try:
                    self.order_state_machine.update_order_meta(
                        str(queue_local_order_id or ""),
                        {"live_submit_gate": {
                            "all_passed": False,
                            "failed_gate": "module_error",
                            "error": str(_gate_exc)[:200],
                            "resolved_mode": _module_error_exec_mode or "unknown",
                        },
                        "final_market_validity": {
                            **(locals().get("_final_market_validity_audit") or {}),
                            "gate": "market_validity",
                            "not_run": True,
                            "reason": "MODULE_ERROR",
                            "execution_mode": _module_error_exec_mode or "unknown",
                        }},
                    )
                except Exception as _me_meta_exc:
                    log.warning(
                        "[%s] LIVE_SUBMIT_GATE_AUDIT_WRITE_FAILED gate=module_error "
                        "order_id=%s error=%s — submit still blocked",
                        ticker, str(queue_local_order_id or ""), _me_meta_exc,
                    )
                _terminalize_breach_failure("live_submit_gate:MODULE_ERROR")
                return

        submit_res = self.order_state_machine.submit_existing_entry(
            local_order_id=queue_local_order_id,
            broker=self.broker,
            plan=approved_plan,
            limit_price=submit_limit,
        )

        if submit_res.get("reconciliation_required") or submit_res.get("split_brain"):
            reason_code = str(
                submit_res.get("error")
                or (
                    "ENTRY_SPLIT_BRAIN_QUARANTINED"
                    if submit_res.get("split_brain")
                    else "ENTRY_BROKER_IDENTITY_UNPROVEN"
                )
            )
            broker_order_id = submit_res.get("broker_order_id")
            log.warning(
                "[%s] Entry submit requires broker reconciliation | local=%s "
                "broker=%s error=%s reconciliation_required=%s split_brain=%s",
                ticker,
                submit_res.get("local_order_id") or queue_local_order_id,
                broker_order_id,
                reason_code,
                submit_res.get("reconciliation_required"),
                submit_res.get("split_brain"),
            )
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "reconcile_pending",
                    "context_notes": f"osm_reconcile_broker_intent={reason_code}",
                })
            return {
                "disposition": "RECONCILE_BROKER_INTENT",
                "reason_code": reason_code,
                "broker_order_id": broker_order_id,
            }

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

        if (
            submit_res.get("error") == "RECOVERY_SUBMIT_INTENT_FENCE_LOST"
            or submit_res.get("error") == "MATERIALIZATION_SUBMIT_OWNERSHIP_TRANSFERRED"
        ):
            # Ownership loss is not an order failure.  A winner may already own
            # the row or have durable submit intent; never cancel/expire it.
            disposition = self._classify_recovered_ownership_loss(queue_local_order_id)
            log.warning(
                "[%s] SUBMIT_OWNERSHIP_TRANSFERRED order=%s disposition=%s error=%s",
                ticker, queue_local_order_id, disposition, submit_res.get("error"),
            )
            return {
                "disposition": disposition,
                "reason_code": str(
                    submit_res.get("error") or "SUBMIT_OWNERSHIP_TRANSFERRED"
                ),
            }

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
                local_order_id=str(getattr(pos, "pending_exit_local_order_id", "") or "") or None,
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

    def _cleanup_pending_entry_order(self, watched: WatchedSignal, *, action: str, reason: str) -> bool:
        """Best-effort OSM cleanup for watcher terminal outcomes.

        The queue creates an ENTRY order before arming the watcher. If the
        watcher later expires or invalidates before broker submission, that
        order must not remain as a ghost CREATED/PENDING_TRIGGER row.
        """
        sig = getattr(watched, "signal", {}) or {}
        local_order_id = str(sig.get("local_order_id") or "").strip()
        if not local_order_id or self.order_state_machine is None:
            return False

        ownership = sig.get("_callback_ownership_context") or {}
        if ownership.get("is_recovered"):
            if ownership.get("ownership_kind") == "materialization_retry":
                terminalize = getattr(
                    self.order_state_machine, "terminalize_materialization_retry", None
                )
                if not callable(terminalize):
                    sig["_recovery_cleanup_disposition"] = "KEEP_WATCHER"
                    return False
                terminal_status = "EXPIRED" if action == "expire" else "CANCELED"
                try:
                    ok = bool(terminalize(
                        local_order_id,
                        owner=str(ownership.get("owner") or ""),
                        generation=ownership.get("generation"),
                        retry_attempt=ownership.get("retry_attempt"),
                        client_id=str(ownership.get("client_id") or ""),
                        execution_mode=str(ownership.get("execution_mode") or ""),
                        terminal_status=terminal_status,
                        reason=reason,
                    ))
                except Exception as exc:
                    log.error("[%s] materialization retry terminal CAS raised: %s", watched.ticker, exc)
                    ok = False
                if not ok:
                    sig["_recovery_cleanup_disposition"] = self._classify_recovered_ownership_loss(
                        local_order_id
                    )
                return ok

            terminalize = getattr(self.order_state_machine, "terminalize_recovered_entry", None)
            if not callable(terminalize):
                sig["_recovery_cleanup_disposition"] = "KEEP_WATCHER"
                return False
            terminal_status = "EXPIRED" if action == "expire" else "CANCELED"
            try:
                ok = bool(terminalize(
                    local_order_id,
                    owner=str(ownership.get("owner") or ""),
                    generation=ownership.get("generation"),
                    execution_mode=str(ownership.get("execution_mode") or ""),
                    terminal_status=terminal_status,
                    reason=reason,
                ))
            except Exception as exc:
                log.error("[%s] recovered terminal CAS raised: %s", watched.ticker, exc)
                ok = False
            if not ok:
                sig["_recovery_cleanup_disposition"] = self._classify_recovered_ownership_loss(
                    local_order_id
                )
            return ok

        # ── Materialization ownership guard (Rule 1 / Rule 3) ──────────────────
        # Production proof (July 15 2026, TMO LIVE): after a deferred entry
        # triggers and schedules a contract-selection retry, the watcher is reset
        # to PENDING (line 4329 ap_entry_watcher.py) so the retry delay is
        # honoured.  On the *next* poll tick, check() sees now >= expire_at and
        # returns EXPIRED.  Without this guard, _cleanup_pending_entry_order
        # calls expire_pending_entry() and the order becomes EXPIRED with
        # last_error="watcher_expired" while materialization_status=RETRY_PENDING
        # — an impossible and terminal lifecycle combination.
        #
        # Rule 1: RETRY_PENDING / RUNNING / QUEUED is an active owner state.
        #   The materializer still holds this order; watcher cleanup may not
        #   terminalize it.
        # Rule 3: Original watcher expiry controls untriggered setups only.
        #   Once contract materialization has begun, use the materializer
        #   lifecycle deadline and entry cutoff — not the watcher's expire_at.
        # Rule 5: Cleanup cannot overwrite newer lifecycle state.
        if action == "expire":
            try:
                _get_order_fn = getattr(self.order_state_machine, "get_order", None)
                if callable(_get_order_fn):
                    _snap = _get_order_fn(local_order_id)
                    if _snap:
                        _snap_meta = _snap.get("meta") or {}
                        if isinstance(_snap_meta, str):
                            try:
                                import json as _jg
                                _snap_meta = _jg.loads(_snap_meta)
                            except Exception:
                                _snap_meta = {}
                        if not isinstance(_snap_meta, dict):
                            _snap_meta = {}
                        _mat_status = str(_snap_meta.get("materialization_status") or "").upper()
                        _lc_state   = str(_snap_meta.get("lifecycle_state") or "").upper()
                        _in_flight  = bool(_snap_meta.get("materialization_in_flight"))
                        _is_active_mat = (
                            _mat_status in {"QUEUED", "RUNNING", "RETRY_PENDING"}
                            or _lc_state  in {"MATERIALIZING", "RETRY_WAIT"}
                            or _in_flight
                        )
                        if _is_active_mat:
                            _sig_id_for_log  = str(_snap.get("signal_id") or "").strip()
                            _client_for_log  = str(_snap.get("client_id") or "").strip()
                            _mode_for_log    = str(_snap.get("execution_mode") or "").strip()
                            _gen_for_log     = _snap_meta.get("materialization_generation")
                            _attempt_for_log = _snap_meta.get("retry_attempt")
                            _token_for_log   = (
                                str(_snap_meta.get("materialization_owner") or "")
                                or str(_snap_meta.get("watcher_token") or "")
                            ).strip()
                            _next_retry      = str(_snap_meta.get("next_retry_at")
                                                   or _snap_meta.get("materialization_next_retry_at")
                                                   or "").strip()
                            _deadline        = str(_snap_meta.get("absolute_entry_deadline")
                                                   or _snap_meta.get("materialization_lifecycle_deadline")
                                                   or "").strip()
                            log.critical(
                                "[%s] WATCHER_EXPIRY_SKIPPED_ACTIVE_MATERIALIZATION | "
                                "order=%s client_id=%s execution_mode=%s signal_id=%s "
                                "materialization_status=%s lifecycle_state=%s "
                                "materialization_generation=%s retry_attempt=%s "
                                "watcher_token=%s next_retry_at=%s lifecycle_deadline=%s "
                                "expire_reason=%s — watcher expiry suppressed; "
                                "materializer retains lifecycle ownership",
                                watched.ticker, local_order_id, _client_for_log,
                                _mode_for_log, _sig_id_for_log,
                                _mat_status, _lc_state,
                                _gen_for_log, _attempt_for_log,
                                _token_for_log, _next_retry, _deadline,
                                reason,
                            )
                            sig["_watcher_expiry_skipped"] = True
                            sig["_watcher_expiry_skip_reason"] = "WATCHER_EXPIRY_SKIPPED_ACTIVE_MATERIALIZATION"
                            return False
            except Exception as _guard_exc:
                # Fail closed: if we cannot verify materialization state, do not
                # expire the order. An order that stays alive is recoverable;
                # an order that is incorrectly terminalized is not.
                log.critical(
                    "[%s] WATCHER_EXPIRY_GUARD_CHECK_FAILED (fail-closed) | "
                    "order=%s expire_reason=%s guard_error=%s — "
                    "suppressing expiry; manual inspection required",
                    watched.ticker, local_order_id, reason, _guard_exc,
                )
                sig["_watcher_expiry_skipped"] = True
                sig["_watcher_expiry_skip_reason"] = "WATCHER_EXPIRY_GUARD_CHECK_FAILED"
                return False

        try:
            if action == "expire" and hasattr(self.order_state_machine, "expire_pending_entry"):
                ok = self.order_state_machine.expire_pending_entry(local_order_id, reason=reason)
                if not ok:
                    log.warning(
                        "[%s] OSM expire_pending_entry returned false | order=%s reason=%s",
                        watched.ticker, local_order_id, reason,
                    )
                return bool(ok)

            if action == "cancel" and hasattr(self.order_state_machine, "cancel_pending_entry"):
                ok = self.order_state_machine.cancel_pending_entry(local_order_id, reason=reason)
                if not ok:
                    log.warning(
                        "[%s] OSM cancel_pending_entry returned false | order=%s reason=%s",
                        watched.ticker, local_order_id, reason,
                    )
                return bool(ok)

            # Compatibility fallback for older OSM versions that do not expose
            # helper methods yet. Only legal CREATED/PENDING_TRIGGER orders will
            # transition; illegal/terminal states are blocked by OSM.transition().
            fallback_status = "EXPIRED" if action == "expire" else "CANCELED"
            if hasattr(self.order_state_machine, "transition"):
                return bool(self.order_state_machine.transition(
                    local_order_id,
                    fallback_status,
                    last_error=reason,
                ))
        except Exception as exc:
            log.error(
                "[%s] OSM pending-entry cleanup failed | order=%s action=%s reason=%s error=%s",
                watched.ticker, local_order_id, action, reason, exc, exc_info=True,
            )
        return False

    def _classify_recovered_ownership_loss(self, local_order_id: str) -> str:
        """Classify a lost recovery CAS without mutating lifecycle state."""
        osm = getattr(self, "order_state_machine", None)
        if osm is None or not hasattr(osm, "get_order"):
            return "KEEP_WATCHER"
        try:
            row = osm.get_order(local_order_id) or {}
        except Exception:
            return "KEEP_WATCHER"
        status = str(row.get("status") or "").upper()
        if status in {"SUBMITTED", "ACK", "ACKNOWLEDGED", "PARTIAL", "PARTIAL_FILL", "FILLED"}:
            return "SUBMITTED"
        if status in {"REJECTED", "EXPIRED", "CANCELED", "ERROR"}:
            return "TERMINAL_DURABLE"
        meta = row.get("meta") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        if not isinstance(meta, dict):
            meta = {}
        if str(meta.get("lifecycle_state") or "").upper() == "SUBMITTING" or meta.get("submit_intent_at"):
            return "RECONCILE_PENDING"
        if str(meta.get("recovery_submit_owner") or "").strip():
            return "OWNERSHIP_TRANSFERRED"
        return "KEEP_WATCHER"

    def _on_signal_expire(self, watched: WatchedSignal):
        signal_id = str(watched.signal.get("signal_id", ""))
        _local_oid_exp = str(
            (getattr(watched, "signal", {}) or {}).get("local_order_id") or ""
        ).strip()

        # Preserve exact expire reason from audit if available.
        _expire_reason = "watcher_expired"
        try:
            _audit_for_exp = getattr(watched, "_pending_audit", None)
            if isinstance(_audit_for_exp, dict):
                _raw_exp = str(_audit_for_exp.get("reason_code") or "").strip()
                if _raw_exp:
                    _expire_reason = _raw_exp
        except Exception:
            pass

        # ── Required fix (PR #342 amendment): check cleanup suppression FIRST ──
        # store.update_status(signal_id, "expired") and the watcher_invalidation_reason
        # meta write must NOT be committed before we know whether
        # _cleanup_pending_entry_order() suppressed the expiry for active
        # deferred materialization.  Writing "expired" state unconditionally and
        # then returning FAILED from the watcher left a contradictory durable row:
        #   ap_signals.decision_status = "expired"   ← wrong
        #   orders.meta.watcher_invalidation_reason = "watcher_expired"  ← wrong
        # while materialization_status = "RETRY_PENDING" was still active.
        _cleanup_ok = self._cleanup_pending_entry_order(
            watched, action="expire", reason=_expire_reason
        )

        # Detect whether cleanup was suppressed by the active-materialization guard.
        _skip_reason = str(
            (getattr(watched, "signal", {}) or {}).get("_watcher_expiry_skip_reason") or ""
        ).strip()
        _expiry_suppressed = _skip_reason in (
            "WATCHER_EXPIRY_SKIPPED_ACTIVE_MATERIALIZATION",
            "WATCHER_EXPIRY_GUARD_CHECK_FAILED",
        )

        if _expiry_suppressed:
            # Active materialization owns this order.  Do NOT write any durable
            # "expired" state — the materializer must be able to retry normally.
            # Do NOT call store.update_status(..., "expired").
            # Do NOT stamp expired_at or watcher_expired reason.
            funnel.inc("watcher_expiry_suppressed_active_materialization")
            log.info(
                "[%s] _on_signal_expire: expiry suppressed — skip_reason=%s | "
                "no expired state written; materialization retains ownership",
                watched.ticker, _skip_reason,
            )
            try:
                from ap.pending_trigger_classifier import (
                    WatcherCompletionResult as _WCR,
                    WatcherCompletionOutcome as _WCO,
                )
                return _WCR(
                    outcome=_WCO.FAILED,
                    reason_code=_skip_reason,
                    local_order_id=_local_oid_exp or None,
                )
            except Exception:
                return None

        # Normal expiry path — cleanup either succeeded or failed for a genuine
        # reason (not an active-materialization guard). Write durable expired state.

        # Persist watcher_invalidation_class for expire outcomes.
        _osm_exp = getattr(self, "order_state_machine", None)
        if _osm_exp is not None and _local_oid_exp:
            _upd_exp = getattr(_osm_exp, "update_order_meta", None)
            if callable(_upd_exp):
                try:
                    from ap.pending_trigger_classifier import classify_watcher_reason
                    _exp_class = classify_watcher_reason(_expire_reason)
                    _upd_exp(_local_oid_exp, {
                        "watcher_invalidation_class":  _exp_class,
                        "watcher_invalidation_reason": _expire_reason,
                        "watcher_invalidation_source": "poll_loop_expire",
                    })
                except Exception:
                    pass

        if signal_id:
            self.store.update_status(signal_id, "expired", timestamp_flag="expired_at")
        funnel.inc("watcher_expired")
        log.info("[%s] Signal expired -- no breach", watched.ticker)

        try:
            from ap.pending_trigger_classifier import (
                WatcherCompletionResult as _WCR,
                WatcherCompletionOutcome as _WCO,
            )
            if _cleanup_ok:
                return _WCR(
                    outcome=_WCO.TERMINALIZED,
                    reason_code=_expire_reason,
                    local_order_id=_local_oid_exp or None,
                )
            return _WCR(
                outcome=_WCO.FAILED,
                reason_code=f"cleanup_returned_false:{_expire_reason}",
                local_order_id=_local_oid_exp or None,
            )
        except Exception:
            return None

    # ── P0 (PR #304): Real underlying invalidation classifier ────────────────
    # Reason codes that mean "the underlying thesis is broken" and must NOT be
    # ignored just because the option contract is still DEFERRED:*. Sourced
    # directly from ap_entry_watcher.py invalidation sites (grep for
    # reason_code= assignments in check() and _revalidate_overnight_at_open).
    #
    # If a reason code is not in this set AND we can positively classify it as
    # benign, keep the DEFERRED watcher alive for breach-time selection. LIVE
    # fails closed on unclassified reasons (see _on_signal_invalidate).
    _REAL_UNDERLYING_INVALIDATION_REASONS: frozenset[str] = frozenset({
        # Watcher intraday stop-touch (WatchedSignal.check)
        "stop_bid_below_call_stop",
        "stop_ask_above_put_stop",
        # Watcher overnight structural / drift / breach
        "overnight_daily_invalidated",
        "overnight_premarket_breached",
        "overnight_too_far_from_trigger",
        "overnight_open_recheck_data_timeout",
        "overnight_daily_validator_error",
        "overnight_live_quote_unavailable",
        "overnight_daily_already_through_trigger",   # PR #304 Bug D
        # Arm-time invalidation (add_signal / _try_rearm)
        "arm_drift",
        "arm_below_stop",
        "arm_already_through_trigger",                # PR #304 Bug C
        # Catch-all watcher decision
        "watcher_invalidated",
        "on_trigger_exhausted_3_attempts",            # PR #304 Bug B exhaustion
    })

    def _is_real_underlying_invalidation(self, reason_code: str) -> bool:
        """PR #324 §4: delegate to canonical classify_watcher_reason().

        True for TERMINAL or ALREADY_BREACHED classifications — these mean
        the underlying thesis is broken and a DEFERRED contract cannot be
        kept alive.  RETRYABLE/REARMABLE are NOT real underlying invalidation.
        """
        try:
            from ap.pending_trigger_classifier import (
                classify_watcher_reason, WatcherInvalidationClass,
            )
            cls = classify_watcher_reason(reason_code)
            return cls in (
                WatcherInvalidationClass.TERMINAL,
                WatcherInvalidationClass.ALREADY_BREACHED,
            )
        except Exception:
            # Fallback: stop_* prefix is always terminal
            rc = str(reason_code or "").strip().lower()
            return rc.startswith("stop_") or rc in self._REAL_UNDERLYING_INVALIDATION_REASONS

    def _on_signal_invalidate(self, watched: WatchedSignal):
        signal_id = str(watched.signal.get("signal_id", ""))
        plan = watched.signal.get("plan") or {}
        contract = (
            plan.get("contract_symbol")
            or watched.signal.get("contract_symbol")
            or watched.signal.get("contract")
            or ""
        )

        # ── P0 (PR #304): DEFERRED contract guard — REAL invalidation must win ──
        # A 'DEFERRED:<SYMBOL>' contract means the OCC contract was not yet
        # selected at watcher-arm time; selection is deferred to breach time.
        # That defers *contract selection* only — it does NOT defer *thesis
        # validity*. A genuine underlying invalidation (stop touched, overnight
        # structural break, arm-time drift/through-trigger) invalidates the
        # SETUP regardless of whether the option contract has been picked yet.
        #
        # THE BUG THIS FIXES (Bug A): the prior guard unconditionally restored
        # state to PENDING and returned for ANY invalidation on a DEFERRED
        # contract — including real stop breaks. That resurrected setups whose
        # underlying thesis was already dead, keeping them in PENDING_TRIGGER
        # until they eventually fired a live entry into a broken setup.
        #
        # CORRECT BEHAVIOR:
        #   • Real underlying invalidation (stop_*, overnight_*, arm_*,
        #     watcher_invalidated, through-trigger) → terminalize the setup
        #     even when DEFERRED. The contract being unpicked is irrelevant;
        #     the trade is off.
        #   • Benign / non-underlying invalidation with NO recognized reason
        #     code → keep the DEFERRED watcher alive in PENDING so breach-time
        #     selection can still run (the original intent of this guard).
        #   • LIVE + unclassified reason → FAIL CLOSED (terminalize). We never
        #     keep a LIVE deferred watcher alive on an invalidation we cannot
        #     positively classify as benign.
        is_deferred_contract = isinstance(contract, str) and contract.startswith("DEFERRED:")
        if is_deferred_contract:
            _inv_reason_code = ""
            try:
                _pending_audit = getattr(watched, "_pending_audit", None)
                if isinstance(_pending_audit, dict):
                    _inv_reason_code = str(_pending_audit.get("reason_code") or "").strip()
            except Exception:
                _inv_reason_code = ""

            _is_real_underlying_invalidation = self._is_real_underlying_invalidation(_inv_reason_code)
            _watcher_mode = str(
                getattr(self, "execution_mode", "")
                or getattr(self, "mode", "")
                or ""
            ).strip().lower()
            _watcher_is_live = _watcher_mode == "live" or (
                not _watcher_mode and getattr(self, "paper", None) is False
            )
            # PR #324 §5 — LIVE unknown reason check uses classify_watcher_reason, not empty-string.
            # NO_WATCHER_OWNER from classifier means the reason is unknown or invariant-violating.
            _inv_class = ""
            try:
                from ap.pending_trigger_classifier import (
                    classify_watcher_reason as _cwrfn,
                    WatcherInvalidationClass as _WIC,
                )
                _inv_class = _cwrfn(_inv_reason_code) if _inv_reason_code else _WIC.NO_WATCHER_OWNER
            except Exception:
                _inv_class = "INVALIDATED_NO_WATCHER_OWNER"

            # Unknown LIVE reason (NO_WATCHER_OWNER) → FAILED quarantine, no terminalize.
            _is_unknown_live_reason = (
                _watcher_is_live
                and _inv_class == "INVALIDATED_NO_WATCHER_OWNER"
                and not _is_real_underlying_invalidation
            )
            # PR #324 final amendment §1: blank/whitespace/malformed/unrecognized LIVE
            # reason must ALSO enter FAILED quarantine. Do NOT require _inv_reason_code
            # to be truthy — an empty reason on a LIVE watcher is precisely the case
            # where falling into the benign RETRY_OWNED path violates the ownership contract.
            if _is_unknown_live_reason:
                _unknown_reason = _inv_reason_code or "blank_live_invalidation_reason"
                log.critical(
                    "[%s] DEFERRED_CONTRACT_UNKNOWN_LIVE_REASON — signal_id=%s "
                    "reason_code=%r class=%s — FAILED quarantine (watcher retained, "
                    "dedup held, exact reason preserved, no cancel).",
                    watched.ticker, signal_id or "?",
                    _unknown_reason, _inv_class,
                )
                funnel.inc("deferred_contract_unknown_live_reason_quarantine")
                try:
                    from ap.pending_trigger_classifier import (
                        WatcherCompletionResult as _UWCR, WatcherCompletionOutcome as _UWCO,
                    )
                    return _UWCR(
                        outcome=_UWCO.FAILED,
                        reason_code=f"unknown_live_reason:{_unknown_reason}",
                        local_order_id=str(
                            (getattr(watched, "signal", {}) or {}).get("local_order_id") or ""
                        ),
                        detail=f"class={_inv_class}",
                    )
                except Exception:
                    return None

            _unclassified_live = False  # handled above

            if _is_real_underlying_invalidation or _unclassified_live:
                # Terminalize — the underlying thesis is invalid; deferral of
                # contract selection does not save it. Fall through to the full
                # forensic invalidation path below (do NOT return early).
                log.warning(
                    "[%s] DEFERRED_CONTRACT_REAL_INVALIDATION — signal_id=%s contract=%s "
                    "reason_code=%s live=%s unclassified_live=%s — terminalizing setup; "
                    "deferred contract selection does NOT override underlying invalidation",
                    watched.ticker, signal_id or "?", contract,
                    _inv_reason_code or "(none)", _watcher_is_live, _unclassified_live,
                )
                funnel.inc("deferred_contract_real_invalidation_terminalized")
                # do NOT return — continue to the full invalidation/cancel path
            else:
                # PR #324 Failure B fix: remove detached PENDING restoration.
                #
                # Previously: watched.state = WatchState.PENDING; return
                # That changed the state of a Python object that was ALREADY REMOVED
                # from APEntryWatcher._pending (the poll loop removed it before calling
                # this callback).  Setting state on a detached object is not registry
                # ownership — the watcher cannot poll, trigger, or retry.
                #
                # Correct behavior for benign/transient deferred invalidation:
                # The PR #324 callback-first ordering (Failure C fix) ensures the watcher
                # is STILL in _pending when we arrive here.  We return RETRY_OWNED so
                # the poll loop keeps the watcher registered with its dedup key held.
                # The watcher remains in WatchState.PENDING (is_active=True on the next
                # poll cycle) so breach-time contract selection can still run.
                #
                # If we are somehow called after removal (legacy path), the result is
                # still RETRY_OWNED — the caller is responsible for verifying registry
                # membership before accepting the result.
                try:
                    from ap_entry_watcher import WatchState
                    _prior_state = getattr(watched, "state", None)
                    # Restore to PENDING only if the watcher is still owned (state was
                    # INVALIDATED from a benign reason, not yet terminalized).
                    if _prior_state == WatchState.INVALIDATED:
                        watched.state = WatchState.PENDING
                    if hasattr(watched, "breach_count"):
                        watched.breach_count = 0
                    log.info(
                        "[%s] DEFERRED_CONTRACT_INVALIDATED_RETRY_OWNED | signal_id=%s "
                        "contract=%s reason_code=%s prior_state=%s — "
                        "watcher retained in registry for breach-time selection",
                        watched.ticker, signal_id or "?", contract,
                        _inv_reason_code or "(none)", _prior_state,
                    )
                except Exception as _e:
                    log.error(
                        "[%s] DEFERRED_CONTRACT_INVALIDATED state-restore failed: %s",
                        watched.ticker, _e,
                    )
                funnel.inc("deferred_contract_invalidated")
                # PR #324 §6 — RETRY_OWNED must persist durable bounded retry metadata.
                _def_oid = str((getattr(watched, "signal", {}) or {}).get("local_order_id") or "")
                _def_now = datetime.now(timezone.utc)
                try:
                    _def_retry_delay = max(5, int(os.getenv(
                        "WATCHER_DEFERRED_RETRY_DELAY_SECONDS", "30"
                    )))
                except (TypeError, ValueError):
                    _def_retry_delay = 30
                try:
                    _def_retry_deadline_secs = max(60, int(os.getenv(
                        "WATCHER_DEFERRED_RETRY_DEADLINE_SECONDS", "300"
                    )))
                except (TypeError, ValueError):
                    _def_retry_deadline_secs = 300

                _def_next_at = (_def_now + timedelta(seconds=_def_retry_delay)).isoformat()
                _def_deadline = (_def_now + timedelta(seconds=_def_retry_deadline_secs)).isoformat()
                _def_owner = f"deferred_retry:{_def_oid}"

                # Persist metadata — failure → FAILED quarantine.
                _def_meta_ok = False
                _def_meta_exc = None
                _def_osm = getattr(self, "order_state_machine", None)
                if _def_osm is not None and _def_oid:
                    _def_upd = getattr(_def_osm, "update_order_meta", None)
                    if callable(_def_upd):
                        try:
                            _def_meta_ok = bool(_def_upd(_def_oid, {
                                "watcher_invalidation_class":  "INVALIDATED_RETRYABLE",
                                "watcher_invalidation_reason": _inv_reason_code or "deferred_contract_benign_invalidation",
                                "watcher_retry_owner":         _def_owner,
                                "watcher_retry_attempt":       1,
                                "watcher_retry_next_at":       _def_next_at,
                                "watcher_retry_deadline":      _def_deadline,
                            }))
                        except Exception as _dme:
                            _def_meta_exc = _dme
                            _def_meta_ok = False

                try:
                    from ap.pending_trigger_classifier import (
                        WatcherCompletionResult as _WCR,
                        WatcherCompletionOutcome as _WCO,
                    )
                    if not _def_meta_ok:
                        log.critical(
                            "[%s] deferred RETRY_OWNED metadata write failed (exc=%s) — "
                            "cannot claim RETRY_OWNED without durable metadata; "
                            "returning FAILED for quarantine.",
                            watched.ticker, _def_meta_exc,
                        )
                        return _WCR(
                            outcome=_WCO.FAILED,
                            reason_code="deferred_retry_metadata_persistence_failed",
                            local_order_id=_def_oid or None,
                            detail=str(_def_meta_exc)[:200] if _def_meta_exc else "write_returned_false",
                        )
                    return _WCR(
                        outcome=_WCO.RETRY_OWNED,
                        reason_code=_inv_reason_code or "deferred_contract_benign_invalidation",
                        local_order_id=_def_oid or None,
                        retry_next_at=_def_next_at,
                        retry_deadline=_def_deadline,
                    )
                except Exception:
                    return None  # fallback — poll loop normalizes None → FAILED

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

        # PR #324: extract and preserve exact invalidation reason before cleanup.
        _exact_inv_reason = "watcher_invalidated"
        try:
            _pending_audit_for_reason = getattr(watched, "_pending_audit", None)
            if isinstance(_pending_audit_for_reason, dict):
                _raw_rc = str(_pending_audit_for_reason.get("reason_code") or "").strip()
                if _raw_rc and _raw_rc != "watcher_invalidated":
                    _exact_inv_reason = _raw_rc
        except Exception:
            pass

        # Persist invalidation taxonomy alongside the exact reason.
        _local_oid_inv = str(
            (getattr(watched, "signal", {}) or {}).get("local_order_id") or ""
        ).strip()
        _osm_inv = getattr(self, "order_state_machine", None)
        if _osm_inv is not None and _local_oid_inv:
            _upd_inv = getattr(_osm_inv, "update_order_meta", None)
            if callable(_upd_inv):
                try:
                    from ap.pending_trigger_classifier import classify_watcher_reason
                    _inv_class = classify_watcher_reason(_exact_inv_reason)
                    _upd_inv(_local_oid_inv, {
                        "watcher_invalidation_class":  _inv_class,
                        "watcher_invalidation_reason": _exact_inv_reason,
                        "watcher_invalidation_source": "poll_loop",
                    })
                except Exception:
                    pass

        if signal_id:
            self.store.update_status(signal_id, "invalidated", timestamp_flag="invalidated_at")
        _cleanup_ok = self._cleanup_pending_entry_order(
            watched, action="cancel", reason=_exact_inv_reason
        )
        funnel.inc("watcher_invalidated")

        # Return explicit WatcherCompletionResult so poll loop can verify.
        try:
            from ap.pending_trigger_classifier import (
                WatcherCompletionResult as _WCR,
                WatcherCompletionOutcome as _WCO,
            )
            if _cleanup_ok:
                return _WCR(
                    outcome=_WCO.TERMINALIZED,
                    reason_code=_exact_inv_reason,
                    local_order_id=_local_oid_inv or None,
                )
            return _WCR(
                outcome=_WCO.FAILED,
                reason_code=f"cleanup_returned_false:{_exact_inv_reason}",
                local_order_id=_local_oid_inv or None,
            )
        except Exception:
            return None

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
