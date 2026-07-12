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
