"""ap/selector_retry_policy.py — Canonical selector / materialization retry taxonomy.

AMENDMENT: PR #323 Seam 3

WHY THIS MODULE EXISTS
─────────────────────
Before this module, retry reason classification was duplicated between:

  ap_execution_core.RETRYABLE_BREACH_SELECTOR_REASONS   (frozenset)
  ap/deferred_materializer.RETRYABLE_MATERIALIZATION_REASONS  (frozenset)

Both had to be kept in sync manually and could diverge silently.  A reason
present in one but not the other would behave differently in the two consumers
(execution core vs materializer) with no test catching the drift.  When a new
selector reason code is added the engineer must now update ONE place and the
classification propagates to all consumers automatically.

CLASSIFICATIONS
───────────────
Every reason code emitted by ap/contract_selector.py (and any reason code the
execution core or materializer might surface from the selector result) maps to
exactly one of:

  RETRYABLE_DATA       — transient data-miss: chain warmup, zero quotes,
                         provider 429/timeout; retry makes sense
  TERMINAL_QUALITY     — structural quality reject: OI too low, spread too
                         wide, premium cap, delta OTM; retrying the same
                         chain state produces the same result
  TERMINAL_POLICY      — business-rule veto: earnings lockout, account size,
                         DTE policy, capital; retrying within the same session
                         will not help
  TERMINAL_AUTH        — authentication/authorization failure against the data
                         provider; requires operator action
  TERMINAL_INVARIANT   — internal consistency or configuration error; implies
                         a code or config bug
  UNKNOWN_FAIL_CLOSED  — any reason code not explicitly mapped; fail closed —
                         do NOT retry; emit full diagnostics

POLICY RECORD
─────────────
Each reason maps to a SelectorRetryPolicy named tuple which encodes whether:

  selector_rerun_allowed    — may we call the selector again from scratch?
  retain_existing_contract  — may we keep the previously selected OCC contract
                              without re-running the selector?
  retry_delay_applies       — should a backoff delay be applied between attempts?
  max_attempts_applies      — does the configured max-attempt cap apply?
  final_reason_code         — the durable terminal reason to write on exhaustion
  queue_facing_reason       — the operator-visible outcome bucket

CONSUMERS
─────────
Replace the two frozensets with:

  from ap.selector_retry_policy import is_retryable_selector_reason
  from ap.selector_retry_policy import classify_selector_reason
  from ap.selector_retry_policy import get_policy

Unknown reasons fail closed — they are NOT retryable by default.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import NamedTuple


class SelectorRetryPolicy(NamedTuple):
    classification: str          # one of the six constants below
    selector_rerun_allowed: bool
    retain_existing_contract: bool
    retry_delay_applies: bool
    max_attempts_applies: bool
    final_reason_code: str       # written to orders.meta on exhaustion
    queue_facing_reason: str     # RETRY_LATER_DATA_UNAVAILABLE | TERMINAL_NO_TRADEABLE_CONTRACT | etc.


# ── Classification constants ─────────────────────────────────────────────────

RETRYABLE_DATA     = "RETRYABLE_DATA"
TERMINAL_QUALITY   = "TERMINAL_QUALITY"
TERMINAL_POLICY    = "TERMINAL_POLICY"
TERMINAL_AUTH      = "TERMINAL_AUTH"
TERMINAL_INVARIANT = "TERMINAL_INVARIANT"
UNKNOWN_FAIL_CLOSED = "UNKNOWN_FAIL_CLOSED"

# ── Policy table ─────────────────────────────────────────────────────────────
# Every reason code emitted by ap/contract_selector.py is listed here.
# Reason codes are keyed exactly as the selector emits them (uppercase, no
# prefix).  Aliases (e.g. CHAIN_AUTH_ERROR normalised to NO_CHAIN_DATA in the
# selector's own canon table) are listed at their emitted value so this module
# is independent of the normalisation layer.

_POLICY_TABLE: dict[str, SelectorRetryPolicy] = {

    # ── RETRYABLE_DATA ────────────────────────────────────────────────────────
    # Transient data-miss — chain warmup, provider 429, zero quotes at open.
    "NO_CHAIN_DATA": SelectorRetryPolicy(
        classification=RETRYABLE_DATA,
        selector_rerun_allowed=True, retain_existing_contract=False,
        retry_delay_applies=True, max_attempts_applies=True,
        final_reason_code="NO_CHAIN_DATA",
        queue_facing_reason="RETRY_LATER_DATA_UNAVAILABLE",
    ),
    "CHAIN_PROVIDER_ERROR": SelectorRetryPolicy(
        classification=RETRYABLE_DATA,
        selector_rerun_allowed=True, retain_existing_contract=False,
        retry_delay_applies=True, max_attempts_applies=True,
        final_reason_code="CHAIN_PROVIDER_ERROR",
        queue_facing_reason="RETRY_LATER_DATA_UNAVAILABLE",
    ),
    "CHAIN_PROVIDER_EMPTY_EXPIRATIONS": SelectorRetryPolicy(
        classification=RETRYABLE_DATA,
        selector_rerun_allowed=True, retain_existing_contract=False,
        retry_delay_applies=True, max_attempts_applies=True,
        final_reason_code="CHAIN_PROVIDER_EMPTY_EXPIRATIONS",
        queue_facing_reason="RETRY_LATER_DATA_UNAVAILABLE",
    ),
    "CHAIN_PROVIDER_EMPTY_OPTIONS": SelectorRetryPolicy(
        classification=RETRYABLE_DATA,
        selector_rerun_allowed=True, retain_existing_contract=False,
        retry_delay_applies=True, max_attempts_applies=True,
        final_reason_code="CHAIN_PROVIDER_EMPTY_OPTIONS",
        queue_facing_reason="RETRY_LATER_DATA_UNAVAILABLE",
    ),
    "CHAIN_PARSE_EMPTY": SelectorRetryPolicy(
        classification=RETRYABLE_DATA,
        selector_rerun_allowed=True, retain_existing_contract=False,
        retry_delay_applies=True, max_attempts_applies=True,
        final_reason_code="CHAIN_PARSE_EMPTY",
        queue_facing_reason="RETRY_LATER_DATA_UNAVAILABLE",
    ),
    "CHAIN_EMPTY": SelectorRetryPolicy(
        classification=RETRYABLE_DATA,
        selector_rerun_allowed=True, retain_existing_contract=False,
        retry_delay_applies=True, max_attempts_applies=True,
        final_reason_code="CHAIN_EMPTY",
        queue_facing_reason="RETRY_LATER_DATA_UNAVAILABLE",
    ),
    "CHAIN_FETCH_FAILED": SelectorRetryPolicy(
        classification=RETRYABLE_DATA,
        selector_rerun_allowed=True, retain_existing_contract=False,
        retry_delay_applies=True, max_attempts_applies=True,
        final_reason_code="CHAIN_FETCH_FAILED",
        queue_facing_reason="RETRY_LATER_DATA_UNAVAILABLE",
    ),
    "NO_EXPIRATION_IN_DTE_WINDOW": SelectorRetryPolicy(
        classification=RETRYABLE_DATA,
        selector_rerun_allowed=True, retain_existing_contract=False,
        retry_delay_applies=True, max_attempts_applies=True,
        final_reason_code="NO_EXPIRATION_IN_DTE_WINDOW",
        queue_facing_reason="RETRY_LATER_DATA_UNAVAILABLE",
    ),
    "DIRECT_QUOTE_UNAVAILABLE": SelectorRetryPolicy(
        classification=RETRYABLE_DATA,
        selector_rerun_allowed=True, retain_existing_contract=False,
        retry_delay_applies=True, max_attempts_applies=True,
        final_reason_code="DIRECT_QUOTE_UNAVAILABLE",
        queue_facing_reason="RETRY_LATER_DATA_UNAVAILABLE",
    ),
    "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED": SelectorRetryPolicy(
        classification=RETRYABLE_DATA,
        selector_rerun_allowed=True, retain_existing_contract=False,
        retry_delay_applies=True, max_attempts_applies=True,
        final_reason_code="DUPLICATE_QUOTE_CONFLICT_UNRESOLVED",
        queue_facing_reason="RETRY_LATER_DATA_UNAVAILABLE",
    ),
    "CHAIN_ROW_ZERO_BID_ASK": SelectorRetryPolicy(
        classification=RETRYABLE_DATA,
        selector_rerun_allowed=True, retain_existing_contract=False,
        retry_delay_applies=True, max_attempts_applies=True,
        final_reason_code="CHAIN_ROW_ZERO_BID_ASK",
        queue_facing_reason="RETRY_LATER_DATA_UNAVAILABLE",
    ),
    "DIRECT_QUOTE_ZERO_BID_ASK": SelectorRetryPolicy(
        classification=RETRYABLE_DATA,
        selector_rerun_allowed=True, retain_existing_contract=False,
        retry_delay_applies=True, max_attempts_applies=True,
        final_reason_code="DIRECT_QUOTE_ZERO_BID_ASK",
        queue_facing_reason="RETRY_LATER_DATA_UNAVAILABLE",
    ),
    "QUOTE_FETCH_FAILED": SelectorRetryPolicy(
        classification=RETRYABLE_DATA,
        selector_rerun_allowed=True, retain_existing_contract=False,
        retry_delay_applies=True, max_attempts_applies=True,
        final_reason_code="QUOTE_FETCH_FAILED",
        queue_facing_reason="RETRY_LATER_DATA_UNAVAILABLE",
    ),
    "QUOTE_ZERO_BID_ASK": SelectorRetryPolicy(
        classification=RETRYABLE_DATA,
        selector_rerun_allowed=True, retain_existing_contract=False,
        retry_delay_applies=True, max_attempts_applies=True,
        final_reason_code="QUOTE_ZERO_BID_ASK",
        queue_facing_reason="RETRY_LATER_DATA_UNAVAILABLE",
    ),
    "SELECTOR_REQUEST_BUDGET_EXHAUSTED": SelectorRetryPolicy(
        classification=RETRYABLE_DATA,
        selector_rerun_allowed=True, retain_existing_contract=False,
        retry_delay_applies=True, max_attempts_applies=True,
        final_reason_code="SELECTOR_REQUEST_BUDGET_EXHAUSTED",
        queue_facing_reason="RETRY_LATER_DATA_UNAVAILABLE",
    ),
    "MARKET_DATA_THROTTLE_UNAVAILABLE": SelectorRetryPolicy(
        classification=RETRYABLE_DATA,
        selector_rerun_allowed=True, retain_existing_contract=False,
        retry_delay_applies=True, max_attempts_applies=True,
        final_reason_code="MARKET_DATA_THROTTLE_UNAVAILABLE",
        queue_facing_reason="RETRY_LATER_DATA_UNAVAILABLE",
    ),
    "PROVIDER_RATE_LIMITED": SelectorRetryPolicy(
        classification=RETRYABLE_DATA,
        selector_rerun_allowed=True, retain_existing_contract=False,
        retry_delay_applies=True, max_attempts_applies=True,
        final_reason_code="PROVIDER_RATE_LIMITED",
        queue_facing_reason="RETRY_LATER_DATA_UNAVAILABLE",
    ),
    "PROVIDER_TIMEOUT": SelectorRetryPolicy(
        classification=RETRYABLE_DATA,
        selector_rerun_allowed=True, retain_existing_contract=False,
        retry_delay_applies=True, max_attempts_applies=True,
        final_reason_code="PROVIDER_TIMEOUT",
        queue_facing_reason="RETRY_LATER_DATA_UNAVAILABLE",
    ),
    "REJECT_UNAVAILABLE": SelectorRetryPolicy(
        classification=RETRYABLE_DATA,
        selector_rerun_allowed=True, retain_existing_contract=False,
        retry_delay_applies=True, max_attempts_applies=True,
        final_reason_code="REJECT_UNAVAILABLE",
        queue_facing_reason="RETRY_LATER_DATA_UNAVAILABLE",
    ),

    # ── TERMINAL_QUALITY ──────────────────────────────────────────────────────
    # Structural quality rejects — retrying the same chain produces the same result.
    "OI_TOO_LOW": SelectorRetryPolicy(
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="OI_TOO_LOW",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),
    "VOLUME_TOO_LOW": SelectorRetryPolicy(
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="VOLUME_TOO_LOW",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),
    "LIQUIDITY_BELOW_THRESHOLD": SelectorRetryPolicy(
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="LIQUIDITY_BELOW_THRESHOLD",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),
    "SPREAD_TOO_WIDE": SelectorRetryPolicy(
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="SPREAD_TOO_WIDE",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),
    "FINAL_SPREAD_TOO_WIDE": SelectorRetryPolicy(
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="FINAL_SPREAD_TOO_WIDE",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),
    "CHEAP_CONTRACT_NO_UPGRADE": SelectorRetryPolicy(
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="CHEAP_CONTRACT_NO_UPGRADE",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),
    "BID_BELOW_MIN": SelectorRetryPolicy(
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="BID_BELOW_MIN",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),
    "DEEP_OTM_REJECT": SelectorRetryPolicy(
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="DEEP_OTM_REJECT",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),
    "OTM_TOO_FAR": SelectorRetryPolicy(
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="OTM_TOO_FAR",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),
    "DELTA_OUT_OF_RANGE": SelectorRetryPolicy(
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="DELTA_OUT_OF_RANGE",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),
    "MONEYNESS_OUT_OF_RANGE": SelectorRetryPolicy(
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="MONEYNESS_OUT_OF_RANGE",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),
    "DTE_OUT_OF_RANGE": SelectorRetryPolicy(
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="DTE_OUT_OF_RANGE",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),
    "PREMIUM_TOO_LOW": SelectorRetryPolicy(
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="PREMIUM_TOO_LOW",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),
    "PREMIUM_TOO_HIGH": SelectorRetryPolicy(
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="PREMIUM_TOO_HIGH",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),
    "PREMIUM_CAP_EXCEEDED": SelectorRetryPolicy(
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="PREMIUM_CAP_EXCEEDED",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),
    "NO_CONTRACT_AFTER_FILTERS": SelectorRetryPolicy(
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="NO_CONTRACT_AFTER_FILTERS",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),
    "IV_RANK_TOO_HIGH": SelectorRetryPolicy(
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="IV_RANK_TOO_HIGH",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),
    "IV_ZONE_SCORE_TOO_LOW": SelectorRetryPolicy(
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="IV_ZONE_SCORE_TOO_LOW",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),
    "REJECT_DIRECT_ZERO": SelectorRetryPolicy(
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="REJECT_DIRECT_ZERO",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),
    "DTE_LADDER_ERROR": SelectorRetryPolicy(
        # The DTE ladder itself errored — structural; treated as terminal here.
        # _is_ladder_exhaustion_retryable() in execution_core provides finer
        # granularity when the top-level code is NO_VALID_PLAYBOOK_DTE_CONTRACT.
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="DTE_LADDER_ERROR",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),
    # NO_VALID_PLAYBOOK_DTE_CONTRACT is intentionally NOT in the retryable set
    # here because the execution core inspects the dte_ladder_audit to determine
    # whether the exhaustion was data-miss (retryable) or quality-reject
    # (terminal). Default fail-closed.
    "NO_VALID_PLAYBOOK_DTE_CONTRACT": SelectorRetryPolicy(
        classification=TERMINAL_QUALITY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="NO_VALID_PLAYBOOK_DTE_CONTRACT",
        queue_facing_reason="TERMINAL_NO_TRADEABLE_CONTRACT",
    ),

    # ── TERMINAL_POLICY ───────────────────────────────────────────────────────
    # Business-rule vetoes that cannot be resolved by retrying within the session.
    "EARNINGS_LOCKOUT": SelectorRetryPolicy(
        classification=TERMINAL_POLICY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="EARNINGS_LOCKOUT",
        queue_facing_reason="TERMINAL_POLICY_BLOCK",
    ),
    "EARNINGS_GUARD_ERROR": SelectorRetryPolicy(
        classification=TERMINAL_POLICY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="EARNINGS_GUARD_ERROR",
        queue_facing_reason="TERMINAL_POLICY_BLOCK",
    ),
    "UNTRADEABLE_FOR_ACCOUNT_SIZE": SelectorRetryPolicy(
        classification=TERMINAL_POLICY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="UNTRADEABLE_FOR_ACCOUNT_SIZE",
        queue_facing_reason="TERMINAL_POLICY_BLOCK",
    ),
    "CHEAP_CONTRACT_ONLY_CHOICE": SelectorRetryPolicy(
        classification=TERMINAL_POLICY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="CHEAP_CONTRACT_ONLY_CHOICE",
        queue_facing_reason="TERMINAL_POLICY_BLOCK",
    ),
    "NO_AFFORDABLE_CONTRACT": SelectorRetryPolicy(
        classification=TERMINAL_POLICY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="NO_AFFORDABLE_CONTRACT",
        queue_facing_reason="TERMINAL_POLICY_BLOCK",
    ),
    "FINAL_CONTRACT_UNAFFORDABLE": SelectorRetryPolicy(
        classification=TERMINAL_POLICY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="FINAL_CONTRACT_UNAFFORDABLE",
        queue_facing_reason="TERMINAL_POLICY_BLOCK",
    ),
    "CAPITAL_NO_REMAINING": SelectorRetryPolicy(
        classification=TERMINAL_POLICY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="CAPITAL_NO_REMAINING",
        queue_facing_reason="TERMINAL_POLICY_BLOCK",
    ),
    "INVALID_POSITION_BUDGET": SelectorRetryPolicy(
        classification=TERMINAL_POLICY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="INVALID_POSITION_BUDGET",
        queue_facing_reason="TERMINAL_POLICY_BLOCK",
    ),
    "UNSUPPORTED_INDEX_MAPPING": SelectorRetryPolicy(
        classification=TERMINAL_POLICY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="UNSUPPORTED_INDEX_MAPPING",
        queue_facing_reason="TERMINAL_POLICY_BLOCK",
    ),
    "BLOCKED": SelectorRetryPolicy(
        classification=TERMINAL_POLICY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="BLOCKED",
        queue_facing_reason="TERMINAL_POLICY_BLOCK",
    ),
    "IV_GATE": SelectorRetryPolicy(
        classification=TERMINAL_POLICY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="IV_GATE",
        queue_facing_reason="TERMINAL_POLICY_BLOCK",
    ),
    # IV_FILTER_ERROR: IV computation itself failed — treat as policy block
    # because the guard errored (fail-closed), not data-miss.
    "IV_FILTER_ERROR": SelectorRetryPolicy(
        classification=TERMINAL_POLICY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="IV_FILTER_ERROR",
        queue_facing_reason="TERMINAL_POLICY_BLOCK",
    ),
    # Request-scope structural policy proof is terminal policy truth, not an
    # unknown reason and not terminal quality.  The reducer may emit this only
    # after every known candidate agrees on STRUCTURAL_TERMINAL_POLICY_REJECT.
    "TERMINAL_POLICY_REJECT": SelectorRetryPolicy(
        classification=TERMINAL_POLICY,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="TERMINAL_POLICY_REJECT",
        queue_facing_reason="TERMINAL_POLICY_BLOCK",
    ),

    # ── TERMINAL_AUTH ────────────────────────────────────────────────────────
    # Authentication/authorization failure — requires operator action.
    "CHAIN_AUTH_ERROR": SelectorRetryPolicy(
        classification=TERMINAL_AUTH,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="CHAIN_AUTH_ERROR",
        queue_facing_reason="TERMINAL_PROVIDER_AUTH",
    ),

    # ── TERMINAL_INVARIANT ────────────────────────────────────────────────────
    # Internal consistency / configuration errors.
    "INVALID_PLAN": SelectorRetryPolicy(
        classification=TERMINAL_INVARIANT,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="INVALID_PLAN",
        queue_facing_reason="TERMINAL_INVARIANT_VIOLATION",
    ),
    "INVALID_EXECUTION_MODE": SelectorRetryPolicy(
        classification=TERMINAL_INVARIANT,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="INVALID_EXECUTION_MODE",
        queue_facing_reason="TERMINAL_INVARIANT_VIOLATION",
    ),
    "EXECUTION_MODE_MISMATCH": SelectorRetryPolicy(
        classification=TERMINAL_INVARIANT,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="EXECUTION_MODE_MISMATCH",
        queue_facing_reason="TERMINAL_INVARIANT_VIOLATION",
    ),
    # The deferred request-scope reducer emits this only when it raises or
    # returns an unusable value. Preserve the exact invariant failure through
    # runtime, restart, and materializer consumers; never retry it as data.
    "UNKNOWN_SELECTOR_RECOVERY_FAILURE": SelectorRetryPolicy(
        classification=TERMINAL_INVARIANT,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="UNKNOWN_SELECTOR_RECOVERY_FAILURE",
        queue_facing_reason="TERMINAL_INVARIANT_VIOLATION",
    ),

    # ── UNKNOWN_REJECTION (sentinel) ──────────────────────────────────────────
    # The selector emits UNKNOWN_REJECTION as a catch-all for unexpected rejects.
    # Map it fail-closed.
    "UNKNOWN_REJECTION": SelectorRetryPolicy(
        classification=UNKNOWN_FAIL_CLOSED,
        selector_rerun_allowed=False, retain_existing_contract=False,
        retry_delay_applies=False, max_attempts_applies=False,
        final_reason_code="UNKNOWN_REJECTION",
        queue_facing_reason="TERMINAL_UNKNOWN_FAIL_CLOSED",
    ),
}

# ── Back-compat: build RETRYABLE_BREACH_SELECTOR_REASONS from the policy table
# so existing callers that import the frozenset continue to work without any
# code change while routing through the canonical table.
RETRYABLE_BREACH_SELECTOR_REASONS: frozenset = frozenset(
    code for code, policy in _POLICY_TABLE.items()
    if policy.classification == RETRYABLE_DATA
)

# ── Same for RETRYABLE_MATERIALIZATION_REASONS (previously in deferred_materializer)
RETRYABLE_MATERIALIZATION_REASONS: frozenset = RETRYABLE_BREACH_SELECTOR_REASONS


# ── Public API ────────────────────────────────────────────────────────────────

_FALLBACK_UNKNOWN = SelectorRetryPolicy(
    classification=UNKNOWN_FAIL_CLOSED,
    selector_rerun_allowed=False,
    retain_existing_contract=False,
    retry_delay_applies=False,
    max_attempts_applies=False,
    final_reason_code="UNKNOWN_SELECTOR_REASON",
    queue_facing_reason="TERMINAL_UNKNOWN_FAIL_CLOSED",
)


def get_policy(reason_code: "str | None") -> SelectorRetryPolicy:
    """Return the canonical policy for a selector reason code.

    Unknown reasons fail closed — they are NOT retryable.  Full diagnostics
    are preserved via the UNKNOWN_FAIL_CLOSED classification.
    """
    key = str(reason_code or "").strip()
    return _POLICY_TABLE.get(key, _FALLBACK_UNKNOWN)


def classify_selector_reason(reason_code: "str | None") -> str:
    """Return the classification string for a selector reason code.

    Always returns one of the six classification constants.  Never raises.
    """
    return get_policy(reason_code).classification


def is_retryable_selector_reason(reason_code: "str | None") -> bool:
    """True iff the reason maps to RETRYABLE_DATA.

    Equivalent to `reason_code in RETRYABLE_BREACH_SELECTOR_REASONS` in the
    old frozenset approach but derived from the single canonical table so
    addition of a new reason code in the table is the only required change.
    """
    return get_policy(reason_code).classification == RETRYABLE_DATA


# ── Sub-classification: OPERATIONAL_REQUEST_BUDGET vs candidate-quality ───────
#
# P0 AMENDMENT (fix/deferred-retry-due-execution-p0 §5)
# ------------------------------------------------------
# SELECTOR_REQUEST_BUDGET_EXHAUSTED is classified RETRYABLE_DATA above (a
# subsequent attempt with fresh per-request counters may find a candidate),
# but operationally it is NOT the same as a chain-warmup miss — a per-request
# budget exhaustion means the selector's HTTP quota for THIS attempt was
# consumed before a decisive gate fired. Downstream diagnostics must preserve
# BOTH:
#
#   * the LAST candidate-quality rejection reason the selector scored during
#     the attempt (e.g. OI_TOO_LOW), so operators can see whether attempt N
#     was operationally curtailed or was hitting real quality rejects; and
#   * the operational reason (SELECTOR_REQUEST_BUDGET_EXHAUSTED), so the
#     retry class is correctly attributed to a fresh-budget retry, not to
#     a quality reject that would (by policy §5 in the spec) fail closed.
#
# A retry caused by operational request-budget exhaustion MAY run again on a
# fresh per-request budget. A true terminal quality failure (OI_TOO_LOW,
# SPREAD_TOO_WIDE, PREMIUM_CAP_EXCEEDED, DELTA_OUT_OF_RANGE, DTE_OUT_OF_RANGE,
# NO_CONTRACT_AFTER_FILTERS, etc.) stays terminal and respects existing
# max-attempt policy.

OPERATIONAL_REQUEST_BUDGET_CODES: frozenset = frozenset({
    "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
    "MARKET_DATA_THROTTLE_UNAVAILABLE",
    "PROVIDER_RATE_LIMITED",
    "PROVIDER_TIMEOUT",
})


def is_operational_request_budget_reason(reason_code: "str | None") -> bool:
    """True iff the reason code represents per-request operational budget
    exhaustion (as opposed to structural chain-data unavailability).

    All operational budget codes are also RETRYABLE_DATA — a fresh attempt
    with reset counters can find a candidate. This helper is used purely
    for diagnostic labelling of the retry_class field in
    orders.meta.materialization_selector_failure so post-mortem analysis
    can distinguish operational curtailment from data unavailability.
    """
    return str(reason_code or "").strip() in OPERATIONAL_REQUEST_BUDGET_CODES


def classify_retry_reason_taxonomy(
    reason_code: "str | None",
    *,
    last_candidate_quality_reason: "str | None" = None,
) -> dict:
    """Return the honest, dual-label taxonomy for a retry outcome.

    Preserves BOTH the operational reason (why THIS attempt stopped early)
    and the last candidate-quality reason (what the selector was scoring
    against when it stopped). Neither is dropped in favour of the other.

    Return shape:

        {
            "reason_code":               <as-emitted>,
            "classification":            RETRYABLE_DATA / TERMINAL_QUALITY / ...,
            "retry_class":               "OPERATIONAL_REQUEST_BUDGET" |
                                         "TRANSIENT_DATA" |
                                         "TERMINAL_QUALITY" | ...,
            "selector_terminal_reason":  <last candidate-quality reason if any>,
            "operational_reason":        <same as reason_code when operational>,
            "may_retry_with_fresh_budget": bool,
        }

    Consumers write the full dict into
    ``orders.meta.materialization_selector_failure`` so both dimensions are
    preserved in the durable row and in operator dashboards.
    """
    _code = str(reason_code or "").strip()
    policy = get_policy(_code)
    _operational = is_operational_request_budget_reason(_code)
    if _operational:
        _retry_class = "OPERATIONAL_REQUEST_BUDGET"
    elif policy.classification == RETRYABLE_DATA:
        _retry_class = "TRANSIENT_DATA"
    else:
        _retry_class = policy.classification
    return {
        "reason_code": _code,
        "classification": policy.classification,
        "retry_class": _retry_class,
        "selector_terminal_reason": (
            str(last_candidate_quality_reason).strip()
            if last_candidate_quality_reason else None
        ),
        "operational_reason": _code if _operational else None,
        "may_retry_with_fresh_budget": _operational,
    }


_CURSOR_MAX_SYMBOLS = 200
_CURSOR_MAX_EXPIRATIONS = 10


class SelectorRecoveryOwnershipLost(RuntimeError):
    """The exact order-owner CAS no longer authorizes selector continuation."""


class SelectorRecoveryCursorPersistFailed(RuntimeError):
    """Durable cursor persistence for a deferred-breach retry attempt failed
    after a completed provider call or structural skip -- a genuine write
    failure (DB exception, serialization error, timeout, or any other
    non-ownership error), distinct from SelectorRecoveryOwnershipLost
    (identity/generation CAS miss, where ownership itself is proven lost).
    Callers must stop further selector/provider work immediately rather
    than continuing with process-memory-only progress: continuing would
    let the process spend more request-budget capacity on candidates whose
    prior-attempt outcomes have no durable record, exactly the crash-loss/
    capacity-laundering risk the durable restart contract exists to
    prevent.
    """


class DeferredMaterializationConfigConflict(RuntimeError):
    """MAX_BREACH_SELECTOR_RETRIES and DEFERRED_MATERIALIZATION_MAX_ATTEMPTS
    were both explicitly set in the environment to different values -- or
    either was set to a malformed/non-positive value. There is no correct
    silent choice between two explicitly-conflicting operator instructions
    for the same conceptual retry ceiling."""


_DEFERRED_MATERIALIZATION_MAX_ATTEMPTS_DEFAULT = 5


def resolve_deferred_materialization_max_attempts() -> int:
    """Single canonical resolver for the deferred-materialization retry
    ceiling. Every consumer of this ceiling -- ap_execution_core.py's
    selector retry loop, ap/pending_trigger_restart_recovery.py's restart-
    recovery exhaustion check, and ap/deferred_materializer.py's bucket
    config -- must call this function rather than reading either env var
    independently, so all three resolve to the exact same value under
    every environment configuration, not merely under matching hardcoded
    defaults (which is all the prior fix guaranteed).

    Precedence:
      - neither var set: both default to 5.
      - exactly one set: that value is used.
      - both set and equal: that value is used.
      - both set and unequal: raises DeferredMaterializationConfigConflict.
      - either set to a malformed or non-positive value: raises
        DeferredMaterializationConfigConflict.

    Raises rather than silently picking a value on conflict -- callers
    decide how to fail safe in their own context (this module has no
    opinion on selector/restart/materializer-specific fallback behavior).
    """
    _raw_breach = os.getenv("MAX_BREACH_SELECTOR_RETRIES")
    _raw_deferred = os.getenv("DEFERRED_MATERIALIZATION_MAX_ATTEMPTS")

    def _parse(raw, name):
        if raw is None or str(raw).strip() == "":
            return None
        try:
            value = int(str(raw).strip())
        except (TypeError, ValueError):
            raise DeferredMaterializationConfigConflict(
                f"{name}={raw!r} is not a valid integer"
            )
        if value <= 0:
            raise DeferredMaterializationConfigConflict(
                f"{name}={raw!r} must be a positive integer"
            )
        return value

    breach_value = _parse(_raw_breach, "MAX_BREACH_SELECTOR_RETRIES")
    deferred_value = _parse(_raw_deferred, "DEFERRED_MATERIALIZATION_MAX_ATTEMPTS")

    if breach_value is not None and deferred_value is not None:
        if breach_value != deferred_value:
            raise DeferredMaterializationConfigConflict(
                f"MAX_BREACH_SELECTOR_RETRIES={breach_value} conflicts with "
                f"DEFERRED_MATERIALIZATION_MAX_ATTEMPTS={deferred_value} -- "
                "both env vars govern the same conceptual retry ceiling and "
                "must agree, or only one of them should be set"
            )
        return breach_value
    if breach_value is not None:
        return breach_value
    if deferred_value is not None:
        return deferred_value
    return _DEFERRED_MATERIALIZATION_MAX_ATTEMPTS_DEFAULT


def _utc_iso(now=None) -> str:
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _strict_positive_cursor_int(value) -> int | None:
    """Resolve a cursor integer field to a positive int without bool/float coercion.

    Rules:
      - bool is not int even though isinstance(True, int) is True in Python.
      - float is never acceptable regardless of fractional part.
      - bare int ≥ 1 is accepted.
      - numeric-string produced by a proven production cursor producer is NOT
        accepted here; this helper is used for both trusted expected-authority
        args and for durable cursor fields.  Callers that need decimal-string
        compatibility must add their own documented allowance.
      - Returns None for every rejected value; never raises.
    """
    if isinstance(value, bool) or isinstance(value, float):
        return None
    if isinstance(value, int):
        return value if value >= 1 else None
    return None


def _parse_cursor_aware_timestamp(value) -> datetime | None:
    """Resolve a durable cursor timestamp string to a timezone-aware datetime.

    Never raises.  Requirements:
      - value must be a str
      - nonblank
      - valid ISO-8601 parseable via datetime.fromisoformat
      - timezone-aware (tzinfo must not be None after parsing)

    Trailing Z is accepted because _utc_iso (the canonical producer) emits
    trailing +00:00 on CPython 3.11+ but some upstream test representations
    use Z; both resolve unambiguously to UTC.

    Timezone-naive timestamps are rejected without silent UTC normalization.
    The durable producer (_utc_iso) already emits aware timestamps; this
    loader must not manufacture authority that was never stored.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        # Naive timestamp — producer would not emit this.
        # Reject rather than silently attaching UTC.
        return None
    return dt


def new_selector_recovery_cursor(
    *,
    local_order_id: str,
    client_id: str,
    execution_mode: str,
    signal_id: str,
    materialization_generation: int,
    selector_attempt_count: int = 1,
    now=None,
) -> dict:
    """Build the bounded namespaced cursor stored in orders.meta."""
    return {
        "version": 1,
        "local_order_id": str(local_order_id or ""),
        "client_id": str(client_id or "").strip().lower(),
        "execution_mode": str(execution_mode or "").strip().lower(),
        "signal_id": str(signal_id or ""),
        "materialization_generation": int(materialization_generation or 0),
        "selector_attempt_count": max(1, int(selector_attempt_count or 1)),
        "attempted_symbols": {},
        "structurally_skipped_symbols": {},
        "expirations_probed": [],
        "last_ranked_index_by_expiration": {},
        "last_market_truth_outcome": None,
        "last_market_truth_checked_at": None,
        "updated_at": _utc_iso(now),
    }


def load_selector_recovery_cursor(
    candidate,
    *,
    local_order_id: str,
    client_id: str,
    execution_mode: str,
    signal_id: str,
    materialization_generation: int,
    selector_attempt_count: int,
    allow_previous_generation: bool = False,
    now=None,
) -> tuple[dict, str | None]:
    """Validate identity and bound untrusted JSON cursor input.

    A mismatched or malformed cursor is never reused.  The sole
    previous-generation allowance supports the existing atomic claim
    transition N→N+1: callers may opt in only after proving the canonical row
    is owned by the exact new generation.  Untrusted cursor data never raises;
    every malformed field returns one stable classified reason.

    TRUSTED EXPECTED AUTHORITY
    ──────────────────────────
    materialization_generation and selector_attempt_count are caller-supplied
    expected authority.  Runtime and preflight callers must provide
    already-resolved positive integer authority.  bool and float are not
    accepted.  None is not a positive integer; the function returns a
    classified authority failure.

    DURABLE CURSOR FIELDS
    ─────────────────────
    All numeric fields from the untrusted JSON cursor are validated with
    _strict_positive_cursor_int.  No bool, float, or string coercion.

    COLLECTION SHAPES AND NESTED RECORDS
    ─────────────────────────────────────
    All four cursor collections must be the expected container type before any
    bounded-truncation is applied.  Malformed collections return a stable
    MALFORMED_CURSOR:<field> reason and never silently erase durable evidence.
    Nested attempted-symbol and structural-skip records are validated against
    the actual producer schema before any bounding.  A malformed nested record
    anywhere in the map — including beyond the bounded window — rejects the
    entire cursor.
    """
    # ── 1. Validate trusted expected authority ────────────────────────────────
    expected_generation = _strict_positive_cursor_int(materialization_generation)
    expected_attempt = _strict_positive_cursor_int(selector_attempt_count)
    # Build fresh using safe fallback values so new_selector_recovery_cursor
    # never receives None (it calls int(... or 0) internally).
    safe_generation = expected_generation if expected_generation is not None else 0
    safe_attempt = expected_attempt if expected_attempt is not None else 1
    fresh = new_selector_recovery_cursor(
        local_order_id=local_order_id,
        client_id=client_id,
        execution_mode=execution_mode,
        signal_id=signal_id,
        materialization_generation=safe_generation,
        selector_attempt_count=safe_attempt,
        now=now,
    )
    if expected_generation is None:
        return fresh, "INVALID_EXPECTED_CURSOR_AUTHORITY:materialization_generation"
    if expected_attempt is None:
        return fresh, "INVALID_EXPECTED_CURSOR_AUTHORITY:selector_attempt_count"

    # ── 2. Absent cursor is a fresh start ─────────────────────────────────────
    if candidate in (None, ""):
        return fresh, None
    if not isinstance(candidate, dict):
        return fresh, "MALFORMED_CURSOR"

    # ── 3. Version — exact integer 1 only ────────────────────────────────────
    # type(version) is int explicitly rejects bool because bool is a subclass
    # of int in Python (isinstance(True, int) is True, but type(True) is bool).
    version = candidate.get("version")
    if type(version) is not int or version != 1:  # noqa: E721
        return fresh, "IDENTITY_MISMATCH:version"

    # ── 4. String identity fields ─────────────────────────────────────────────
    string_identity = {
        "local_order_id": str(local_order_id or ""),
        "client_id": str(client_id or "").strip().lower(),
        "execution_mode": str(execution_mode or "").strip().lower(),
        "signal_id": str(signal_id or ""),
    }
    for key, expected_value in string_identity.items():
        actual = candidate.get(key)
        if key in {"client_id", "execution_mode"}:
            actual = str(actual or "").strip().lower()
        if actual != expected_value:
            return fresh, f"IDENTITY_MISMATCH:{key}"

    # ── 5. Cursor generation — strict positive integer, no bool/float ─────────
    actual_generation = _strict_positive_cursor_int(
        candidate.get("materialization_generation")
    )
    if actual_generation is None:
        return fresh, "IDENTITY_MISMATCH:materialization_generation"
    allowed_generations = {expected_generation}
    if allow_previous_generation and expected_generation > 1:
        allowed_generations.add(expected_generation - 1)
    if actual_generation not in allowed_generations:
        return fresh, "IDENTITY_MISMATCH:materialization_generation"

    # ── 6. Cursor attempt count — strict positive integer, no bool/float ──────
    actual_attempt = _strict_positive_cursor_int(
        candidate.get("selector_attempt_count")
    )
    if actual_attempt is None:
        return fresh, "MALFORMED_CURSOR:selector_attempt_count"

    # ── 7. Collection container types — reject before any truncation ──────────
    # A malformed collection must never be silently replaced with {} or [].
    # That would destroy durable provider-call and structural-skip evidence.
    required_shapes = {
        "attempted_symbols": dict,
        "structurally_skipped_symbols": dict,
        "expirations_probed": list,
        "last_ranked_index_by_expiration": dict,
    }
    for field, expected_type in required_shapes.items():
        if not isinstance(candidate.get(field), expected_type):
            return fresh, f"MALFORMED_CURSOR:{field}"

    attempted = candidate["attempted_symbols"]
    skipped = candidate["structurally_skipped_symbols"]
    expirations = candidate["expirations_probed"]
    ranked = candidate["last_ranked_index_by_expiration"]

    # ── 8. Attempted-symbol records: canonical key + nested field validation ──
    #
    # Producer canonical key: "".join(str(symbol).upper().split())
    # Durable keys differing from their canonical form represent laundered
    # corruption (duplicate evidence for the same symbol under different
    # representations). Reject rather than normalizing.
    #
    # Validation covers ALL records BEFORE any bounding. A malformed record
    # anywhere in the map — including beyond the bounded retention window —
    # causes the entire cursor to be rejected.
    #
    # Producer record shape (record_selector_recovery_attempt):
    #   attempt_number    : positive int  (max(1, int(attempt_number or 1)))
    #   expiration        : str           (str(expiration or ""), may be empty)
    #   result_reason     : str           (str(result_reason or ""), may be empty)
    #   attempted_at      : aware ISO str (_utc_iso())
    #   provider_timestamp: any JSON-compatible value including None
    #   transient         : exact bool    (bool(transient))
    for symbol, record in attempted.items():
        canonical_symbol = "".join(str(symbol).upper().split())
        if (
            not isinstance(symbol, str)
            or not canonical_symbol
            or symbol != canonical_symbol
            or not isinstance(record, dict)
        ):
            return fresh, "MALFORMED_CURSOR:attempted_symbols"
        # attempt_number: strict positive int; no bool/float/string coercion
        if _strict_positive_cursor_int(record.get("attempt_number")) is None:
            return fresh, "MALFORMED_CURSOR:attempted_symbols"
        # transient: exact bool; "true"/"false"/1/0/None are not acceptable
        if type(record.get("transient")) is not bool:  # noqa: E721
            return fresh, "MALFORMED_CURSOR:attempted_symbols"
        # attempted_at: nonblank str, valid ISO, timezone-aware
        if _parse_cursor_aware_timestamp(record.get("attempted_at")) is None:
            return fresh, "MALFORMED_CURSOR:attempted_symbols"
        # expiration: str (empty string allowed — producer emits str(expiration or ""))
        if not isinstance(record.get("expiration"), str):
            return fresh, "MALFORMED_CURSOR:attempted_symbols"
        # result_reason: str (empty string allowed — producer emits str(result_reason or ""))
        if not isinstance(record.get("result_reason"), str):
            return fresh, "MALFORMED_CURSOR:attempted_symbols"
        # provider_timestamp: allow any JSON-compatible value including None.
        # The producer stores an opaque provider-side value; no further constraint.

    # ── 9. Structural-skip records: canonical key + nested field validation ───
    #
    # Producer record shape (record_selector_structural_skip):
    #   skip_reason : str           (str(skip_reason or ""), may be empty)
    #   observed_at : aware ISO str (_utc_iso())
    for symbol, record in skipped.items():
        canonical_symbol = "".join(str(symbol).upper().split())
        if (
            not isinstance(symbol, str)
            or not canonical_symbol
            or symbol != canonical_symbol
            or not isinstance(record, dict)
        ):
            return fresh, "MALFORMED_CURSOR:structurally_skipped_symbols"
        # skip_reason: str (empty string allowed — producer emits str(skip_reason or ""))
        if not isinstance(record.get("skip_reason"), str):
            return fresh, "MALFORMED_CURSOR:structurally_skipped_symbols"
        # observed_at: nonblank str, valid ISO, timezone-aware
        if _parse_cursor_aware_timestamp(record.get("observed_at")) is None:
            return fresh, "MALFORMED_CURSOR:structurally_skipped_symbols"

    # ── 10. Expiration list and ranked-index map element shapes ───────────────
    if any(
        not isinstance(entry, str) or not entry.strip()
        for entry in expirations
    ):
        return fresh, "MALFORMED_CURSOR:expirations_probed"

    if any(
        not isinstance(expiration, str)
        or not expiration.strip()
        or type(index) is not int  # noqa: E721 — reject bool
        or index < 0
        for expiration, index in ranked.items()
    ):
        return fresh, "MALFORMED_CURSOR:last_ranked_index_by_expiration"

    # ── 11. All validation passed — build bounded output cursor ───────────────
    cursor = dict(candidate)
    cursor["materialization_generation"] = expected_generation
    # Advance attempt to trusted current authority if cursor trails behind.
    cursor["selector_attempt_count"] = max(actual_attempt, expected_attempt)
    cursor["attempted_symbols"] = dict(
        list(attempted.items())[-_CURSOR_MAX_SYMBOLS:]
    )
    cursor["structurally_skipped_symbols"] = dict(
        list(skipped.items())[-_CURSOR_MAX_SYMBOLS:]
    )
    cursor["expirations_probed"] = list(
        dict.fromkeys(expirations)
    )[-_CURSOR_MAX_EXPIRATIONS:]
    cursor["last_ranked_index_by_expiration"] = dict(
        list(ranked.items())[-_CURSOR_MAX_EXPIRATIONS:]
    )
    cursor["updated_at"] = _utc_iso(now)
    return cursor, None


def selector_symbol_may_retry(
    record,
    *,
    refresh_seconds: int,
    now=None,
) -> bool:
    """Return True if a symbol's most recent attempt may be retried after cooldown.

    Requires exact Boolean retry authority — bool("false") is True in Python
    and therefore str("false") must not reach this helper as retry authority.
    The loader validates transient before a cursor can reach execution; this
    helper enforces the same invariant as defense in depth.

    Returns False for any malformed or missing record field without raising.
    """
    if not isinstance(record, dict):
        return False
    # Exact bool required. "true", "false", 1, 0, None are all rejected.
    if record.get("transient") is not True:
        return False
    attempted_at = _parse_cursor_aware_timestamp(record.get("attempted_at"))
    if attempted_at is None:
        # Malformed, blank, non-string, or timezone-naive timestamp — fail closed.
        return False
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return (current - attempted_at.astimezone(timezone.utc)).total_seconds() >= max(
        5, min(120, int(refresh_seconds or 20))
    )


def record_selector_recovery_attempt(
    cursor: dict,
    *,
    symbol: str,
    attempt_number: int,
    expiration: str | None,
    result_reason: str,
    transient: bool,
    provider_timestamp=None,
    now=None,
) -> dict:
    out = dict(cursor or {})
    records = dict(out.get("attempted_symbols") or {})
    key = "".join(str(symbol or "").upper().split())
    if key:
        records.pop(key, None)
        records[key] = {
            "attempt_number": max(1, int(attempt_number or 1)),
            "expiration": str(expiration or ""),
            "result_reason": str(result_reason or ""),
            "attempted_at": _utc_iso(now),
            "provider_timestamp": provider_timestamp,
            "transient": bool(transient),
        }
    out["attempted_symbols"] = dict(list(records.items())[-_CURSOR_MAX_SYMBOLS:])
    if expiration:
        expirations = list(out.get("expirations_probed") or [])
        if str(expiration) not in expirations:
            expirations.append(str(expiration))
        out["expirations_probed"] = expirations[-_CURSOR_MAX_EXPIRATIONS:]
        ranked = dict(out.get("last_ranked_index_by_expiration") or {})
        ranked[str(expiration)] = int(ranked.get(str(expiration), 0) or 0) + 1
        out["last_ranked_index_by_expiration"] = dict(
            list(ranked.items())[-_CURSOR_MAX_EXPIRATIONS:]
        )
    out["selector_attempt_count"] = max(
        int(out.get("selector_attempt_count") or 0),
        max(1, int(attempt_number or 1)),
    )
    out["updated_at"] = _utc_iso(now)
    return out


def record_selector_structural_skip(
    cursor: dict,
    *,
    symbol: str,
    skip_reason: str,
    now=None,
) -> dict:
    out = dict(cursor or {})
    records = dict(out.get("structurally_skipped_symbols") or {})
    key = "".join(str(symbol or "").upper().split())
    if key:
        records.pop(key, None)
        records[key] = {
            "skip_reason": str(skip_reason or ""),
            "observed_at": _utc_iso(now),
        }
    out["structurally_skipped_symbols"] = dict(
        list(records.items())[-_CURSOR_MAX_SYMBOLS:]
    )
    out["updated_at"] = _utc_iso(now)
    return out


# ── PR #491: request-scope structural terminal truth ─────────────────────────
#
# Canonical mapping from a candidate-level structural skip reason to the
# request-level terminal reason it may prove -- ONLY when the full relevant
# candidate set is exhausted by that single structural condition. Shared by
# _resolve_exhaustive_structural_terminal_reason() below so the resolver and
# the helper can never drift on which structural code maps to which canonical
# request-level reason.
_STRUCTURAL_TO_CANONICAL_REQUEST_REASON: dict[str, str] = {
    "STRUCTURAL_DTE_OUT_OF_RANGE": "DTE_OUT_OF_RANGE",
    "STRUCTURAL_MONEYNESS_OUT_OF_RANGE": "MONEYNESS_OUT_OF_RANGE",
    "STRUCTURAL_DELTA_OUT_OF_RANGE": "DELTA_OUT_OF_RANGE",
    "STRUCTURAL_TERMINAL_POLICY_REJECT": "TERMINAL_POLICY_REJECT",
}

# The set of canonical request-level reasons governed EXCLUSIVELY by
# _resolve_exhaustive_structural_terminal_reason() above. These reasons may
# only become request-level truth via that helper's exhaustive proof -- the
# fallback_selector_reason path (Step 8.5 below) must never be able to
# resurrect one of them merely because the selector's own pre-reducer
# observation happened to be one of these codes. Doing so would silently
# defeat the entire point of PR #491: a candidate-level structural skip
# that failed exhaustive proof must not become request-level terminal
# truth through a side door immediately next to the door that just denied
# it. Derived from the same mapping the helper uses, so the two can never
# drift apart.
_STRUCTURAL_REQUEST_LEVEL_REASONS: frozenset = frozenset(
    _STRUCTURAL_TO_CANONICAL_REQUEST_REASON.values()
)


def _normalize_recovery_symbol(raw) -> str | None:
    """Canonical OCC identity, matching the durable cursor's own normalizer.

    Returns None for anything that is not a nonblank string -- never raises,
    never invents ticker-only identity.
    """
    if not isinstance(raw, str):
        return None
    canonical = "".join(raw.upper().split())
    return canonical or None


def _coerce_structural_skip_records(raw) -> list[tuple[object, object]] | None:
    """Return structural records without discarding duplicate identities."""
    if raw is None:
        return []
    if isinstance(raw, dict):
        return list(raw.items())
    if not isinstance(raw, (list, tuple)):
        return None

    records: list[tuple[object, object]] = []
    for record in raw:
        if isinstance(record, dict):
            records.append((
                record.get("symbol", record.get("contract")),
                record.get("skip_reason", record.get("reason")),
            ))
        elif isinstance(record, (list, tuple)) and len(record) == 2:
            records.append((record[0], record[1]))
        else:
            return None
    return records


def _normalize_structural_skip_reasons(raw) -> dict[str, str] | None:
    """Normalize structural records while preserving conflicting evidence."""
    records = _coerce_structural_skip_records(raw)
    if records is None:
        return None

    normalized: dict[str, str] = {}
    for raw_symbol, raw_reason in records:
        norm = _normalize_recovery_symbol(raw_symbol)
        reason = str(raw_reason or "").strip()
        if norm is None or not reason:
            return None
        if norm in normalized and normalized[norm] != reason:
            # Conflicting duplicate evidence for one OCC must remain
            # untrusted; never let a later record overwrite the first.
            return None
        normalized[norm] = reason
    return normalized


def _normalize_quality_rejection_records(raw) -> dict[str, str] | None:
    """Normalize candidate identities and reasons from quality evidence."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        records = list(raw.items())
    elif isinstance(raw, (list, tuple)):
        records = []
        for record in raw:
            if not isinstance(record, dict):
                return None
            records.append((
                record.get("symbol", record.get("contract")),
                record.get(
                    "reason",
                    record.get("rejection_reason", record.get("result_reason")),
                ),
            ))
    else:
        return None

    normalized: dict[str, str] = {}
    for raw_symbol, raw_reason in records:
        norm = _normalize_recovery_symbol(raw_symbol)
        reason = str(raw_reason or "").strip()
        if norm is None or not reason:
            return None
        if norm in normalized and normalized[norm] != reason:
            # Conflicting quality records for one OCC are just as ambiguous
            # as conflicting structural records and must fail closed.
            return None
        normalized[norm] = reason
    return normalized


def _resolve_exhaustive_structural_terminal_reason(
    skipped: dict | list,
    attempted: dict,
    eligible_raw,
    known_eligible_raw=None,
    quality_rejection_raw=None,
) -> str | None:
    """Return a request-level structural reason ONLY when the full relevant
    candidate set exhaustively proves that single structural condition.

    A candidate-level structural skip (e.g. one far-OTM contract) or governed
    ordinary-quality rejection is valid candidate truth, but it is NOT by
    itself proof that the entire deferred selector request should terminalize
    with that reason -- another candidate may still be eligible, unattempted,
    transiently retryable, or rejected for a different condition. This helper
    is the ONLY place that may promote one of the four governed reasons to
    request-level truth, and it does so only when every known candidate in the
    request is accounted for and agrees on the same canonical reason.

    Pure and side-effect-free. Never raises. Any malformed, conflicting, or
    incomplete evidence returns None (no exhaustive proof) rather than
    strengthening a false claim -- callers fall through to the existing
    truthful reducer precedence below.

    known_eligible_raw (optional): an independent accounting of every
    candidate symbol that reached direct-quote eligibility THIS pass,
    regardless of its eventual fate (structurally skipped, genuinely
    attempted, or otherwise). At the real production call site this is
    request_context.direct_quote_eligible_symbols, which is populated
    unconditionally, in-pass, before either the structural-skip or the
    direct-quote-attempt branch runs (see ap/contract_selector.py). It
    exists to catch a real accounting gap: real historical production
    evidence (see the CRM production replay fixture) proves that a
    genuinely direct-quote-attempted candidate can end up unrepresented in
    the `attempted` evidence actually used for a given resolver
    invocation. Regardless of the specific historical cause, this
    independent in-pass accounting closes the gap directly: it does not
    depend on -- and makes no claim about -- how or when the durable
    recovery cursor gets updated. When provided, this parameter requires
    every named symbol to be accounted for by structural, attempted,
    eligible-unattempted, or per-candidate quality evidence before exhaustive
    proof can succeed; if any is not, exhaustive proof is refused. When
    absent (None), this check is skipped entirely -- existing callers and
    evidence shapes that do not supply it are unaffected.

    quality_rejection_raw (optional): complete per-candidate ordinary-quality
    evidence. Aggregate quality counts are not sufficient to establish the
    candidate universe. Every normalized symbol in this source is included in
    the universe; only a governed canonical reason can participate in the
    homogeneous proof, while OI/spread/volume/premium/affordability and other
    ordinary reasons block that proof. A symbol represented by both structural
    and quality records is contradictory and fails closed.
    """
    # ── Eligible-unattempted container: a real container is authoritative;
    # missing/absent is a legitimate "none eligible" signal; anything else
    # (a malformed non-container value) must not silently become "empty" --
    # that would let malformed input manufacture a false exhaustive claim.
    if eligible_raw is None:
        eligible_items: list = []
    elif isinstance(eligible_raw, (list, tuple, set)):
        eligible_items = list(eligible_raw)
    else:
        return None

    normalized_eligible: set[str] = set()
    for raw_symbol in eligible_items:
        norm = _normalize_recovery_symbol(raw_symbol)
        if norm is None:
            return None
        normalized_eligible.add(norm)

    # Rule 2: zero eligible unattempted candidates.
    if normalized_eligible:
        return None

    # ── Normalize structural-skip evidence: normalized_symbol -> reason.
    # The live deferred caller passes a record list, not a dict, so duplicate
    # OCC rows remain visible until this conflict check.
    normalized_skipped = _normalize_structural_skip_reasons(skipped)
    if normalized_skipped is None:
        return None

    # ── Normalize attempted evidence: normalized_symbol -> result reason.
    normalized_attempted: dict[str, str] = {}
    for raw_symbol, record in (attempted or {}).items():
        norm = _normalize_recovery_symbol(raw_symbol)
        if norm is None:
            return None
        if isinstance(record, dict):
            reason = str(record.get("result_reason") or "").strip()
        else:
            reason = str(record or "").strip()
        if not reason:
            return None
        if norm in normalized_attempted and normalized_attempted[norm] != reason:
            return None
        normalized_attempted[norm] = reason

    # A normalized OCC cannot simultaneously be structurally skipped AND
    # attempted -- that is conflicting evidence for the same candidate, not
    # proof of anything. Fail closed.
    if set(normalized_skipped) & set(normalized_attempted):
        return None

    # Ordinary quality rejects are candidate evidence, not merely aggregate
    # counters. If one shares an OCC with a structural record, the evidence
    # is contradictory and cannot strengthen a structural claim.
    normalized_quality = _normalize_quality_rejection_records(
        quality_rejection_raw
    )
    if normalized_quality is None:
        return None
    if set(normalized_skipped) & set(normalized_quality):
        return None

    # Rule 1: at least one known candidate.
    universe = (
        set(normalized_skipped)
        | set(normalized_attempted)
        | normalized_eligible
        | set(normalized_quality)
    )
    if not universe:
        return None

    # Rule 1.5 (accounting-gap closure): if an independent this-pass
    # eligibility accounting was supplied, every symbol it names must be
    # represented in the universe above. A malformed non-container value
    # (present but not a recognizable collection) must not be silently
    # treated as "no additional accounting" -- that would let malformed
    # input manufacture a false exhaustive claim, same principle as
    # eligible_raw above. Absence (None) is the legitimate "no independent
    # accounting supplied" signal and skips this check entirely.
    if known_eligible_raw is not None:
        if isinstance(known_eligible_raw, (list, tuple, set)):
            known_eligible_items = list(known_eligible_raw)
        else:
            return None
        normalized_known_eligible: set[str] = set()
        for raw_symbol in known_eligible_items:
            norm = _normalize_recovery_symbol(raw_symbol)
            if norm is None:
                return None
            normalized_known_eligible.add(norm)
        if normalized_known_eligible - universe:
            # At least one candidate that reached direct-quote eligibility
            # this pass is unaccounted for by structural/attempted/eligible
            # evidence -- erased from the proof. Cannot claim exhaustive.
            return None

    # Rule 3: zero attempted candidates with retryable/transient truth.
    for reason in normalized_attempted.values():
        if get_policy(reason).classification == RETRYABLE_DATA:
            return None

    # Rules 4 & 5: every candidate in the known universe must be represented
    # by either a structural skip or a governed candidate-level quality
    # rejection. An attempted candidate -- retryable or not -- is not
    # structural proof; its mere presence means the candidate set is not
    # exhaustively governed by one structural condition. Likewise, an
    # ordinary quality rejection (OI, spread, volume, premium, affordability,
    # etc.) is accounted candidate truth but cannot prove one of the four
    # request-level structural reasons.
    represented_by_governed_evidence: dict[str, str] = {}
    for symbol, reason in normalized_skipped.items():
        canonical = _STRUCTURAL_TO_CANONICAL_REQUEST_REASON.get(reason)
        if canonical is None:
            return None
        represented_by_governed_evidence[symbol] = canonical
    for symbol, reason in normalized_quality.items():
        if reason not in _STRUCTURAL_REQUEST_LEVEL_REASONS:
            return None
        represented_by_governed_evidence[symbol] = reason

    if set(represented_by_governed_evidence) != universe:
        return None

    # Rule 6: every governed candidate record must resolve to the SAME
    # canonical request-level reason -- no picking whichever is encountered
    # first, and no shortcut from the aggregate quality histogram.
    canonical_reasons: set[str] = set()
    canonical_reasons.update(represented_by_governed_evidence.values())

    if len(canonical_reasons) != 1:
        return None

    return next(iter(canonical_reasons))


def resolve_selector_recovery_final_reason(evidence: dict) -> str:
    """Resolve the truthful terminal reason without side effects."""
    data = evidence if isinstance(evidence, dict) else {}
    market_outcome = str(data.get("market_truth_outcome") or "").upper()
    market_reason = str(data.get("market_truth_reason") or "").strip()
    if market_outcome == "TERMINAL_SETUP_COMPLETE":
        return market_reason or "MARKET_SETUP_INVALIDATED"
    if market_reason == "MARKET_SETUP_INVALIDATED":
        return market_reason

    quality = data.get("quality_rejections")
    quality = quality if isinstance(quality, dict) else {}
    attempted = data.get("attempted_results")
    attempted = attempted if isinstance(attempted, dict) else {}
    skipped = data.get("structural_skip_results")
    skipped = skipped if isinstance(skipped, dict) else {}
    structural_evidence = data.get("structural_skip_records")
    if structural_evidence is None:
        structural_evidence = skipped

    # This amendment is surgical: it demotes ONLY affordability so that one
    # unaffordable candidate cannot terminalize a request while retryable
    # candidates, budget exhaustion, or non-affordability terminal geometry are
    # the truthful story. Every OTHER precedence relationship (terminal policy,
    # non-affordability structural geometry, terminal quality, budget, transient
    # data, retryable quality) is preserved exactly as it was before PR #401's
    # amendment. Affordability now terminalizes only when the FULL candidate set
    # is accounted for and every accounted candidate is an affordability reason.
    #
    # Affordability reasons are named ONCE here so the terminal-policy and
    # terminal-quality vetoes below can exclude them. NO_AFFORDABLE_CONTRACT is
    # classified TERMINAL_POLICY and PREMIUM_CAP_EXCEEDED is TERMINAL_QUALITY;
    # without this exclusion an affordability reason arriving via
    # quality_rejections would short-circuit at step 2 or step 4 and bypass the
    # full-set accounting entirely — the precise defect this amendment closes.
    no_affordable_reasons = {
        "NO_AFFORDABLE_CONTRACT",
        "STRUCTURAL_CLEARLY_UNAFFORDABLE",
    }
    premium_cap_reasons = {
        "PREMIUM_CAP_EXCEEDED",
        "STRUCTURAL_PREMIUM_CAP_EXCEEDED",
    }
    affordability_reasons = no_affordable_reasons | premium_cap_reasons

    # ── Step 2: terminal policy veto (excluding affordability) ───────────────
    terminal_policy = next(
        (
            reason
            for reason in quality
            if reason not in affordability_reasons
            and reason not in _STRUCTURAL_REQUEST_LEVEL_REASONS
            and get_policy(reason).classification == TERMINAL_POLICY
        ),
        None,
    )
    if terminal_policy:
        return terminal_policy

    # ── Step 3: non-affordability terminal structural geometry ───────────────
    # The affordability structural skips are intentionally EXCLUDED here and
    # resolved by the full-set affordability accounting below. This preserves
    # the pre-amendment position of DTE/moneyness/delta/policy geometry.
    #
    # PR #491: a candidate-level structural skip is valid CANDIDATE truth but
    # is not by itself proof of REQUEST-level terminal truth. One far-OTM
    # candidate must not terminalize a request while another candidate is
    # still eligible, unattempted, or transiently retryable. Only an
    # exhaustive governed proof (every known candidate agrees on the same
    # governed reason, none eligible, none retryable) may promote candidate
    # evidence to request-level truth. See
    # _resolve_exhaustive_structural_terminal_reason() above.
    evidence_overflowed = bool(
        data.get("structural_skip_records_overflowed")
        or data.get("quality_rejection_records_overflowed")
    )
    exhaustive_structural_reason = None
    if not evidence_overflowed:
        exhaustive_structural_reason = _resolve_exhaustive_structural_terminal_reason(
            structural_evidence,
            attempted,
            data.get("eligible_unattempted_symbols"),
            data.get("direct_quote_known_eligible_symbols"),
            data.get("quality_rejection_records"),
        )
    if exhaustive_structural_reason is not None:
        return exhaustive_structural_reason

    # ── Step 4: actual request-budget exhaustion with candidates left ────────
    # Candidate-level terminal-quality observations are not request-level
    # exhaustion truth while another known candidate remains eligible and
    # unattempted. Preserve the request for another selector pass.
    eligible = list(data.get("eligible_unattempted_symbols") or [])
    if (
        bool(data.get("actual_limit_reached"))
        and bool(data.get("budget_exhausted_stage"))
        and eligible
    ):
        return "SELECTOR_REQUEST_BUDGET_EXHAUSTED"

    # ── Step 5: retryable attempted-data failure ─────────────────────────────
    # A genuinely attempted candidate's transient provider truth outranks an
    # aggregate terminal-quality histogram collected from other candidates.
    # Otherwise one SPREAD_TOO_WIDE/OI_TOO_LOW observation can erase a direct
    # quote that is explicitly retryable and prematurely release ownership.
    transient_counts: dict[str, int] = {}
    for record in attempted.values():
        if isinstance(record, dict):
            reason = str(record.get("result_reason") or "")
        else:
            reason = str(record or "")
        if reason and get_policy(reason).classification == RETRYABLE_DATA:
            transient_counts[reason] = transient_counts.get(reason, 0) + 1
    if transient_counts:
        return sorted(transient_counts.items(), key=lambda item: (-item[1], item[0]))[0][0]

    # ── Step 6: retryable per-candidate quality/data failure ─────────────────
    # The aggregate quality histogram is diagnostic evidence only for retry
    # authority. It can include reasons collected for candidates that were
    # structurally skipped and therefore never became genuine retryable
    # observations. Only symbol-bound quality records may authorize another
    # selector pass here.
    normalized_quality_records = _normalize_quality_rejection_records(
        data.get("quality_rejection_records")
    )
    retryable_quality_counts: dict[str, int] = {}
    if normalized_quality_records is not None:
        for reason in normalized_quality_records.values():
            if get_policy(reason).classification == RETRYABLE_DATA:
                retryable_quality_counts[reason] = (
                    retryable_quality_counts.get(reason, 0) + 1
                )
    retryable_quality = list(retryable_quality_counts.items())
    if retryable_quality:
        return sorted(retryable_quality, key=lambda item: (-item[1], item[0]))[0][0]

    # ── Step 7: terminal quality veto (excluding affordability and governed
    # structural request-level reasons) ──────────────────────────────────────
    # This is request-terminal only after exhaustive structural proof failed,
    # no known candidate remains eligible/unattempted under an exhausted
    # request budget, and no attempted or symbol-bound candidate evidence is
    # retryable. DTE/delta/moneyness/policy request truth is never promoted
    # from an aggregate quality histogram; those candidate-level records must
    # first pass the homogeneous proof at Step 3.
    terminal_quality = next(
        (
            reason
            for reason in quality
            if reason not in affordability_reasons
            and reason not in _STRUCTURAL_REQUEST_LEVEL_REASONS
            and get_policy(reason).classification == TERMINAL_QUALITY
        ),
        None,
    )
    if terminal_quality:
        return terminal_quality

    # ── Step 8: affordability terminal — only when the FULL candidate set is
    # accounted for and every accounted candidate is an affordability reason.
    # Do NOT infer the whole set is unaffordable because one candidate is.
    # (no_affordable_reasons / premium_cap_reasons / affordability_reasons are
    # declared once near the top of this function.)
    accounted_reasons: list[str] = []
    accounted_symbols: set[str] = set()
    accounting_valid = not evidence_overflowed

    # The live deferred handoff uses structural_skip_records as the
    # authoritative stream. Do not fall back to the legacy dict here: that
    # dict is intentionally absent/empty on the corrected production path,
    # and using it would erase structural candidates from full-set accounting.
    normalized_structural_accounting = _normalize_structural_skip_reasons(
        structural_evidence
    )
    if normalized_structural_accounting is None:
        accounting_valid = False
    else:
        accounted_symbols.update(normalized_structural_accounting)
        accounted_reasons.extend(normalized_structural_accounting.values())

    for raw_symbol, record in attempted.items():
        norm = _normalize_recovery_symbol(raw_symbol)
        if norm is None:
            accounting_valid = False
            continue
        if isinstance(record, dict):
            reason = str(record.get("result_reason") or "")
        else:
            reason = str(record or "")
        if not reason:
            accounting_valid = False
            continue
        accounted_symbols.add(norm)
        accounted_reasons.append(reason)

    for raw_symbol in eligible:
        norm = _normalize_recovery_symbol(raw_symbol)
        if norm is None:
            accounting_valid = False
            continue
        accounted_symbols.add(norm)

    raw_quality_records = data.get("quality_rejection_records")
    if raw_quality_records is not None:
        normalized_quality_accounting = _normalize_quality_rejection_records(
            raw_quality_records
        )
        if normalized_quality_accounting is None:
            accounting_valid = False
        else:
            accounted_symbols.update(normalized_quality_accounting)
            accounted_reasons.extend(normalized_quality_accounting.values())
    else:
        # Backward-compatible evidence shapes may only have the aggregate
        # histogram. It remains usable for legacy affordability tests, but it
        # carries no candidate identities and therefore cannot satisfy an
        # independent known-eligible completeness check below.
        accounted_reasons.extend(str(reason or "") for reason in quality.keys())

    known_eligible_for_accounting = data.get(
        "direct_quote_known_eligible_symbols"
    )
    if known_eligible_for_accounting is not None:
        if not isinstance(known_eligible_for_accounting, (list, tuple, set)):
            accounting_valid = False
        else:
            normalized_known_eligible = set()
            for raw_symbol in known_eligible_for_accounting:
                norm = _normalize_recovery_symbol(raw_symbol)
                if norm is None:
                    accounting_valid = False
                    continue
                normalized_known_eligible.add(norm)
            if normalized_known_eligible - accounted_symbols:
                accounting_valid = False

    accounted_reasons = [reason for reason in accounted_reasons if reason]

    # Affordability is the terminal reason ONLY when the entire candidate set is
    # accounted for and every accounted candidate is an affordability reason:
    #   * eligible_unattempted_symbols is empty;
    #   * no retryable attempted-data failure remains (returned at step 6);
    #   * no retryable quality failure remains (returned at step 7);
    #   * at least one accounted reason exists;
    #   * every accounted attempted, structural, and quality-record reason is
    #     an affordability reason.
    # There is deliberately NO structural fallback below this block: a partial
    # affordability set (candidates still eligible, or non-affordability reasons
    # present) must never terminalize as affordability. It falls through to the
    # truthful higher-priority reason above or to UNKNOWN.
    if (
        accounting_valid
        and not eligible
        and accounted_reasons
        and all(reason in affordability_reasons for reason in accounted_reasons)
    ):
        if any(reason in no_affordable_reasons for reason in accounted_reasons):
            return "NO_AFFORDABLE_CONTRACT"
        return "PREMIUM_CAP_EXCEEDED"

    # ── Step 8.5 (PR #491): known original selector fallback ─────────────────
    # No stronger exhaustive/aggregate proof was found above. Before falling
    # back to the generic UNKNOWN_SELECTOR_RECOVERY_FAILURE, honor the
    # selector's already-established pre-reducer reason if -- and only if --
    # existing policy classification proves it is a KNOWN reason. This never
    # invents a new allowlist: it reuses get_policy(), the same canonical
    # taxonomy authority every other step in this function already depends
    # on. A blank, malformed, unmapped, or UNKNOWN_FAIL_CLOSED fallback does
    # NOT get preserved -- the resolver still fails closed to UNKNOWN in that
    # case, exactly as before this amendment.
    #
    # AMENDMENT (post-review escape-hatch closure): a fallback must NEVER be
    # allowed to resurrect one of the canonical structural request-level
    # reasons (_STRUCTURAL_REQUEST_LEVEL_REASONS) that
    # _resolve_exhaustive_structural_terminal_reason() governs. By the time
    # execution reaches this line, that helper has already run (at Step 3)
    # and did NOT prove a specific structural reason for this exact
    # evidence -- otherwise the function would have returned already. If
    # the selector's pre-reducer observation (_obs_reason in
    # ap/contract_selector.py) happened to be, say, MONEYNESS_OUT_OF_RANGE
    # because one candidate was structurally invalid, honoring it here
    # would resurrect the exact false request-level claim the exhaustive
    # helper just correctly declined to prove -- reopening the defect this
    # PR exists to close, via a side door next to the one that was just
    # locked. Real production evidence (WDAY, 2026-08-18,
    # jasoncosby1@gmail.com, LIVE) showed exactly this shape: mixed
    # STRUCTURAL_DELTA_OUT_OF_RANGE + STRUCTURAL_MONEYNESS_OUT_OF_RANGE
    # candidates with zero unattempted symbols, non-exhaustive by
    # definition, yet the selector's pre-reducer observation was
    # MONEYNESS_OUT_OF_RANGE. Governed structural reasons therefore may
    # NOT flow through this fallback at all -- only through the exhaustive
    # helper. Non-structural known fallbacks (NO_CONTRACT_AFTER_FILTERS,
    # DIRECT_QUOTE_ZERO_BID_ASK, SELECTOR_REQUEST_BUDGET_EXHAUSTED,
    # OI_TOO_LOW, etc.) are unaffected and continue through unchanged.
    fallback_reason = data.get("fallback_selector_reason")
    if fallback_reason:
        fallback_policy = get_policy(fallback_reason)
        if fallback_policy.classification != UNKNOWN_FAIL_CLOSED:
            _fallback_final = fallback_policy.final_reason_code
            if _fallback_final not in _STRUCTURAL_REQUEST_LEVEL_REASONS:
                return _fallback_final

    # ── Step 9: unknown recovery failure ─────────────────────────────────────
    return "UNKNOWN_SELECTOR_RECOVERY_FAILURE"
