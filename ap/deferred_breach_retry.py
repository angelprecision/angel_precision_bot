"""Deferred breach-time contract selection retry policy.

PR P0 (deferred-breach multi-exp + retry):
When the watcher correctly detects a breach on an overnight/deferred setup and
the selector is called at breach time, transient quote-quality failures at
market open (wide spreads, missing MM quotes, chain-fetch flakes) should not
immediately terminalize the trade. Retry a small number of times with a short
sleep to give the option chain a chance to settle. Fail closed on structural
reasons that would fail regardless of when we ask.

This module is intentionally small and pure. It contains no I/O, no logging,
no selector invocation — just the classification of reason codes and the sleep
policy. The retry loop itself lives in ap_execution_core, so all side effects
stay in the caller.

Non-goals
- Do NOT loosen quality gates.
- Do NOT retry on structural reasons (illiquidity, size, delta, earnings).
- Do NOT retry forever — attempts are capped by config.
- Do NOT bypass the DTE ladder — the ladder runs FIRST inside select(),
  so by the time we see a retryable reason at the top level, all expirations
  in the ladder have already been tried once.
"""
from __future__ import annotations

import os
from typing import Optional


# Reason codes worth retrying — the failure is a transient quote-quality signal
# rather than a structural rejection. The chain may just need a few seconds to
# refresh, or a missing NBBO may materialize when a market maker requotes.
#
# Every code here is one where a second attempt 5–15 seconds later has a
# reasonable chance of returning a different, valid contract.
RETRYABLE_REASON_CODES: frozenset[str] = frozenset({
    "CHAIN_ROW_ZERO_BID_ASK",
    "DIRECT_QUOTE_ZERO_BID_ASK",
    "QUOTE_FETCH_FAILED",
    "CHAIN_EMPTY",
    "CHAIN_FETCH_FAILED",
    # NO_VALID_PLAYBOOK_DTE_CONTRACT is the ladder's "all buckets exhausted"
    # exit. It's retryable because the constituent bucket failures are usually
    # transient (wide-spread + zero-quote at open), and running the ladder
    # again after a short sleep gives every bucket a fresh chance.
    "NO_VALID_PLAYBOOK_DTE_CONTRACT",
})


# Reason codes that must NEVER be retried. These are structural verdicts on
# the entire setup — retrying would burn tokens/API calls and get the same
# result, and might also mask the true blocker in operator diagnostics.
TERMINAL_REASON_CODES: frozenset[str] = frozenset({
    "UNTRADEABLE_FOR_ACCOUNT_SIZE",
    "OI_TOO_LOW",
    "VOLUME_TOO_LOW",
    "BID_BELOW_MIN",
    "SPREAD_TOO_WIDE",
    "DELTA_OUT_OF_RANGE",
    "PREMIUM_CAP_EXCEEDED",
    "EARNINGS_LOCKOUT",
    "EARNINGS_GUARD_ERROR",
    "INVALID_PLAN",
    "UNSUPPORTED_INDEX_MAPPING",
    "NO_CHAIN_DATA",
    "DEFERRED_UNRESOLVED_AT_BREACH",
})


def is_retryable(reason_code: Optional[str]) -> bool:
    """Return True iff the selector rejection is worth retrying.

    Unknown / missing / empty codes are NOT retried — better to fail visibly
    than to silently loop on a code we can't reason about.
    """
    if not reason_code:
        return False
    return str(reason_code).strip().upper() in RETRYABLE_REASON_CODES


def is_terminal(reason_code: Optional[str]) -> bool:
    """Return True iff the reason is an explicit terminal verdict.

    Used for observability: callers can log a differently-shaped event when
    retry is refused because the reason is *explicitly* terminal vs. simply
    unknown / unclassified.
    """
    if not reason_code:
        return False
    return str(reason_code).strip().upper() in TERMINAL_REASON_CODES


def max_attempts(env: Optional[dict] = None) -> int:
    """Total selector attempts before giving up (includes the initial attempt).

    Default is 3 total = 1 initial + 2 retries. Configurable via env var
    DEFERRED_BREACH_MAX_ATTEMPTS. Clamped to [1, 5] to protect against
    misconfiguration.
    """
    env = env if env is not None else os.environ
    try:
        n = int(env.get("DEFERRED_BREACH_MAX_ATTEMPTS", "3"))
    except (TypeError, ValueError):
        n = 3
    return max(1, min(5, n))


def sleep_seconds_for_attempt(attempt: int, env: Optional[dict] = None) -> float:
    """Seconds to sleep BEFORE the given attempt index (1-based).

    Attempt 1 (initial) never sleeps. Attempt 2 sleeps DEFERRED_BREACH_RETRY_INITIAL_SLEEP_SEC.
    Subsequent attempts add DEFERRED_BREACH_RETRY_STEP_SLEEP_SEC per attempt,
    capped at DEFERRED_BREACH_RETRY_MAX_SLEEP_SEC.

    Defaults: initial=5s, step=5s, max=15s → sleep schedule 0s, 5s, 10s, 15s, 15s.
    """
    if attempt <= 1:
        return 0.0
    env = env if env is not None else os.environ
    try:
        initial = float(env.get("DEFERRED_BREACH_RETRY_INITIAL_SLEEP_SEC", "5"))
    except (TypeError, ValueError):
        initial = 5.0
    try:
        step = float(env.get("DEFERRED_BREACH_RETRY_STEP_SLEEP_SEC", "5"))
    except (TypeError, ValueError):
        step = 5.0
    try:
        cap = float(env.get("DEFERRED_BREACH_RETRY_MAX_SLEEP_SEC", "15"))
    except (TypeError, ValueError):
        cap = 15.0
    # Clamp all values non-negative so a mis-set env var can never turn the
    # sleep into a busy loop or a negative wait.
    initial = max(0.0, initial)
    step    = max(0.0, step)
    cap     = max(0.0, cap)
    computed = initial + step * (attempt - 2)
    return min(computed, cap)


__all__ = [
    "RETRYABLE_REASON_CODES",
    "TERMINAL_REASON_CODES",
    "is_retryable",
    "is_terminal",
    "max_attempts",
    "sleep_seconds_for_attempt",
]
