"""
ap_canonical_signal — Canonical Signal ID helper (P0 client parity)
====================================================================

Single source of truth for converting a (signal_id, signal) pair into the
canonical_signal_id that groups the SAME market opportunity across all
active eligible client accounts.

Why this exists
---------------
On 2026-06-03 multiple winning canonical signals (UNH CALL, VZ PUT,
META CALL, PG CALL) showed broken client parity. One client filled while
the other two were canceled with startup_phantom_clear_pre_submit or
watcher_invalidated. Root cause: the overnight re-evaluator wraps every
fresh per-client emit as ``REEVAL:<original_uuid>:<random_hex>`` so that
master_control dedup treats each per-client re-evaluation as fresh. The
side effect is that the SAME opportunity ends up with N different
signal_ids — one per client — and cannot be grouped for parity audits.

The function below strips the per-client random suffix while preserving
the REEVAL: prefix and the underlying UUID. For non-REEVAL signal_ids it
returns the input unchanged.

Rules (verbatim from the PR acceptance):
  * If signal_id has a random per-order suffix, strip ONLY the suffix.
  * Preserve REEVAL: as the canonical identity.
  * Do not rely on symbol+direction alone for canonical identity.

Examples
--------
    '8d9338d0-5dde-4b7b-81ea-208039999b72'         -> unchanged
    'REEVAL:8d9338d0-...:f4dc44'                   -> 'REEVAL:8d9338d0-...'
    'REEVAL:603bce5b-352e-44e0-b00b-6baa826ca2c9'  -> unchanged (no suffix)
    ''                                              -> ''
    None                                            -> ''

The function is pure, has zero side effects, and is safe to call from any
thread. It is the only place in the codebase that knows about the
REEVAL:<uuid>:<hex> wire format for canonical-id construction.
"""
from __future__ import annotations

import re
from typing import Any, Mapping, Optional

# REEVAL:<uuid>:<hex-suffix>
#   group 1  -> "REEVAL:<uuid>"  (the canonical identity)
#   group 2  -> ":<hex>"         (the per-order/per-client suffix to strip)
#
# We accept any non-empty hex tail >= 1 char so this catches both the
# 6-char form currently used by ap_overnight_reeval and any future variant.
_REEVAL_WITH_SUFFIX_RE = re.compile(
    r"^(REEVAL:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12})(:[0-9a-fA-F]+)$"
)


def build_canonical_signal_id(
    signal_id: Any,
    signal: Optional[Mapping[str, Any]] = None,
) -> str:
    """Return the canonical_signal_id for an opportunity.

    Args:
        signal_id: The signal_id as seen by the order/queue path. May be a
            plain UUID, a REEVAL:<uuid>:<hex> wrapped form, or any string.
        signal: Optional signal payload. Reserved for future use (e.g.
            falling back to a stored canonical_signal_id field if the
            scanner ever emits one directly). Today the function relies
            solely on signal_id, per the spec rule "do not rely on
            symbol+direction alone for canonical identity".

    Returns:
        - For a plain UUID, the input unchanged.
        - For REEVAL:<uuid>:<hex>, the prefix + UUID with the per-order
          suffix stripped: REEVAL:<uuid>.
        - For REEVAL:<uuid> already (no suffix), the input unchanged.
        - For empty / None / non-string, the empty string.
    """
    # 1. Trust an explicit canonical_signal_id on the payload if the
    #    scanner ever provides one. This keeps the function forward
    #    compatible without coupling callers to internal regex details.
    if isinstance(signal, Mapping):
        explicit = signal.get("canonical_signal_id")
        if isinstance(explicit, str) and explicit:
            return explicit

    if not isinstance(signal_id, str) or not signal_id:
        return ""

    m = _REEVAL_WITH_SUFFIX_RE.match(signal_id)
    if m:
        return m.group(1)

    # Plain UUID, REEVAL:<uuid> with no suffix, or anything else: return
    # unchanged. Stripping aggressively here would risk collapsing
    # legitimately distinct signal_ids that happen to share a colon.
    return signal_id


__all__ = ["build_canonical_signal_id"]
