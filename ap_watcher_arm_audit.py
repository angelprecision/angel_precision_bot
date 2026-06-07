# =============================================================================
# ap_watcher_arm_audit.py  —  PR #92 Watcher Arm Price-Readiness Audit
# =============================================================================
# PURE module: NO database, NO broker, NO threading. All side effects live
# in the integration layer (ap/queue.py + ap_watcher_arm_retry.py).
#
# Responsibilities:
#   1. Pin the canonical reason_code / conceptual_bucket / final_decision
#      enums (spec §1-2-7).
#   2. Classify any raw arm-failure string (the legacy
#      _last_reject_reason value, e.g. "stale_price_-0.4pct_from_trigger"
#      or "arm_below_stop_mid_212.50_stop_213.10") into a structured
#      taxonomy.
#   3. Build the dossier-shaped watcher_arm_audit JSON merged into
#      orders.meta. Compatible with future PR93 trade dossier ingestion.
#
# DESIGN RULES from the PR92 spec:
#   * Three conceptual buckets: TRUE_INVALIDATION, QUOTE_READINESS_UNKNOWN,
#     FRESH_PRICE_DRIFT (plus DOMAIN_SAFETY_BLOCK and UNKNOWN).
#   * Never confuse fresh-quote-drift with stale-quote.
#   * NEVER expose tokens, bearer headers, HMAC secrets, account creds.
#   * Audit is additive: it never overwrites existing orders.meta.
#
# Public API:
#   classify_arm_failure(raw_reason, *, fresh_quote=None) -> Classification
#   build_watcher_arm_audit(...)                        -> dict
#   merge_into_order_meta(existing_meta, audit)         -> dict
#   redact(obj)                                          -> dict
# =============================================================================

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger("ap.watcher_arm_audit")


# ---------------------------------------------------------------------------
# Canonical enums (spec §2)
# ---------------------------------------------------------------------------

# Conceptual buckets — three primary + two edge cases
BUCKET_TRUE_INVALIDATION      = "TRUE_INVALIDATION"
BUCKET_QUOTE_READINESS_UNKNOWN = "QUOTE_READINESS_UNKNOWN"
BUCKET_FRESH_PRICE_DRIFT      = "FRESH_PRICE_DRIFT"
BUCKET_DOMAIN_SAFETY_BLOCK    = "DOMAIN_SAFETY_BLOCK"
BUCKET_UNKNOWN                = "UNKNOWN"

CONCEPTUAL_BUCKETS = (
    BUCKET_TRUE_INVALIDATION,
    BUCKET_QUOTE_READINESS_UNKNOWN,
    BUCKET_FRESH_PRICE_DRIFT,
    BUCKET_DOMAIN_SAFETY_BLOCK,
    BUCKET_UNKNOWN,
)

# Reason codes — quote-readiness
REASON_UNDERLYING_QUOTE_MISSING = "UNDERLYING_QUOTE_MISSING"
REASON_UNDERLYING_QUOTE_STALE   = "UNDERLYING_QUOTE_STALE"
REASON_OPTION_QUOTE_MISSING     = "OPTION_QUOTE_MISSING"
REASON_OPTION_QUOTE_STALE       = "OPTION_QUOTE_STALE"
REASON_QUOTE_REFRESH_FAILED     = "QUOTE_REFRESH_FAILED"
REASON_PRICE_NOT_READY          = "PRICE_NOT_READY"
REASON_ARM_TIMEOUT              = "ARM_TIMEOUT"

# Reason codes — true invalidation / geometry
REASON_PRICE_ALREADY_INVALIDATED      = "PRICE_ALREADY_INVALIDATED"
REASON_ARM_BELOW_STOP                 = "ARM_BELOW_STOP"
REASON_STOP_BID_BELOW_CALL_STOP       = "STOP_BID_BELOW_CALL_STOP"
REASON_STOP_ASK_ABOVE_PUT_STOP        = "STOP_ASK_ABOVE_PUT_STOP"
REASON_TRIGGER_MISSING                = "TRIGGER_MISSING"
REASON_STOP_MISSING                   = "STOP_MISSING"
REASON_INVALID_DIRECTION_GEOMETRY     = "INVALID_DIRECTION_GEOMETRY"
REASON_INVALID_TRIGGER_STOP_GEOMETRY  = "INVALID_TRIGGER_STOP_GEOMETRY"

# Reason code — fresh drift
REASON_PRICE_DRIFT_FROM_TRIGGER = "PRICE_DRIFT_FROM_TRIGGER"

# Reason codes — domain / unknown
REASON_QUOTE_DOMAIN_MISMATCH = "QUOTE_DOMAIN_MISMATCH"
REASON_UNKNOWN_ARM_FAILURE   = "UNKNOWN_ARM_FAILURE"

VALID_REASON_CODES = frozenset({
    REASON_UNDERLYING_QUOTE_MISSING,
    REASON_UNDERLYING_QUOTE_STALE,
    REASON_OPTION_QUOTE_MISSING,
    REASON_OPTION_QUOTE_STALE,
    REASON_QUOTE_REFRESH_FAILED,
    REASON_PRICE_NOT_READY,
    REASON_ARM_TIMEOUT,
    REASON_PRICE_ALREADY_INVALIDATED,
    REASON_ARM_BELOW_STOP,
    REASON_STOP_BID_BELOW_CALL_STOP,
    REASON_STOP_ASK_ABOVE_PUT_STOP,
    REASON_TRIGGER_MISSING,
    REASON_STOP_MISSING,
    REASON_INVALID_DIRECTION_GEOMETRY,
    REASON_INVALID_TRIGGER_STOP_GEOMETRY,
    REASON_PRICE_DRIFT_FROM_TRIGGER,
    REASON_QUOTE_DOMAIN_MISMATCH,
    REASON_UNKNOWN_ARM_FAILURE,
})

# Final decisions
DECISION_ARMED                          = "ARMED"
DECISION_ARMED_AFTER_RETRY              = "ARMED_AFTER_RETRY"
DECISION_FAILED_AFTER_RETRY             = "FAILED_AFTER_RETRY"
DECISION_BLOCKED_TRUE_INVALIDATION      = "BLOCKED_TRUE_INVALIDATION"
DECISION_BLOCKED_INVALID_GEOMETRY       = "BLOCKED_INVALID_GEOMETRY"
DECISION_BLOCKED_PRICE_DRIFT            = "BLOCKED_PRICE_DRIFT"
DECISION_BLOCKED_QUOTE_DOMAIN_MISMATCH  = "BLOCKED_QUOTE_DOMAIN_MISMATCH"
DECISION_BLOCKED_UNKNOWN_ARM_FAILURE    = "BLOCKED_UNKNOWN_ARM_FAILURE"
DECISION_RETRY_DISABLED_QUOTE_NOT_READY = "RETRY_DISABLED_QUOTE_NOT_READY"

VALID_FINAL_DECISIONS = frozenset({
    DECISION_ARMED,
    DECISION_ARMED_AFTER_RETRY,
    DECISION_FAILED_AFTER_RETRY,
    DECISION_BLOCKED_TRUE_INVALIDATION,
    DECISION_BLOCKED_INVALID_GEOMETRY,
    DECISION_BLOCKED_PRICE_DRIFT,
    DECISION_BLOCKED_QUOTE_DOMAIN_MISMATCH,
    DECISION_BLOCKED_UNKNOWN_ARM_FAILURE,
    DECISION_RETRY_DISABLED_QUOTE_NOT_READY,
})

# Buckets that are NEVER retried (spec §7).
NON_RETRYABLE_BUCKETS = frozenset({
    BUCKET_TRUE_INVALIDATION,
    BUCKET_FRESH_PRICE_DRIFT,
    BUCKET_DOMAIN_SAFETY_BLOCK,
})

# Dossier shape constants (spec §10).
DOSSIER_SECTION = "watcher_arm"
DOSSIER_VERSION = "watcher_arm_v1"
LIFECYCLE_FAILED    = "WATCHER_ARM_FAILED"
LIFECYCLE_RECOVERED = "WATCHER_ARM_RECOVERED"


# ---------------------------------------------------------------------------
# Secret redaction
# ---------------------------------------------------------------------------
# We never want to surface tokens or credentials in audit JSON or logs. Any
# key whose name matches one of these patterns is redacted. We also redact
# obvious bearer-style values regardless of key name.

_SECRET_KEY_PATTERNS = re.compile(
    r"(token|secret|password|api[_-]?key|bearer|authorization|auth$|hmac|credential|cookie)",
    re.IGNORECASE,
)
_BEARER_VALUE_PATTERN = re.compile(r"^Bearer\s+\S+", re.IGNORECASE)
_LONG_HEX_PATTERN     = re.compile(r"^[A-Za-z0-9_\-]{40,}$")


def _looks_like_secret_value(v: Any) -> bool:
    if not isinstance(v, str):
        return False
    if _BEARER_VALUE_PATTERN.match(v):
        return True
    # Long opaque tokens (JWT, base64-ish). We don't redact short ids.
    if len(v) >= 40 and _LONG_HEX_PATTERN.match(v):
        return True
    return False


def redact(obj: Any) -> Any:
    """Recursively redact secret-shaped keys/values. Returns a new structure.

    Never raises — pathological inputs (cyclic, exotic types) are coerced
    to strings.
    """
    try:
        if isinstance(obj, dict):
            out = {}
            for k, v in obj.items():
                ks = str(k)
                if _SECRET_KEY_PATTERNS.search(ks):
                    out[ks] = "***REDACTED***"
                elif _looks_like_secret_value(v):
                    out[ks] = "***REDACTED***"
                else:
                    out[ks] = redact(v)
            return out
        if isinstance(obj, list):
            return [redact(x) for x in obj]
        if isinstance(obj, tuple):
            return tuple(redact(x) for x in obj)
        if _looks_like_secret_value(obj):
            return "***REDACTED***"
        return obj
    except Exception:
        return "***UNSERIALIZABLE***"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(v: Any) -> Optional[float]:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f or f in (float("inf"), float("-inf")):  # NaN / inf
        return None
    return f


# ---------------------------------------------------------------------------
# Quote freshness helpers
# ---------------------------------------------------------------------------
# A quote is considered "stale" if its age exceeds the configured threshold.
# Default 5s is conservative for live-arm decisions. The caller (queue
# integration) reads ENV to override.

DEFAULT_UNDERLYING_STALE_AFTER_SECS = 5.0
DEFAULT_OPTION_STALE_AFTER_SECS     = 5.0


def quote_is_missing(quote: Optional[dict[str, Any]]) -> bool:
    """A quote is missing if the dict is None/empty OR every price field
    is None/0."""
    if not quote:
        return True
    for k in ("bid", "ask", "mid", "last"):
        v = _safe_float(quote.get(k))
        if v is not None and v > 0:
            return False
    return True


def quote_age_seconds(quote: Optional[dict[str, Any]]) -> Optional[float]:
    """Compute current age in seconds of a quote dict. Looks at any of:
    quote_ts, ts, timestamp. Returns None if no parsable timestamp."""
    if not quote:
        return None
    ts_value = quote.get("quote_ts") or quote.get("ts") or quote.get("timestamp")
    if ts_value is None:
        # If the quote dict provides an explicit age, honor it directly.
        age = quote.get("quote_age_seconds")
        return _safe_float(age)
    try:
        if isinstance(ts_value, (int, float)):
            # Unix epoch seconds
            dt = datetime.fromtimestamp(float(ts_value), tz=timezone.utc)
        elif isinstance(ts_value, str):
            s = ts_value.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        elif isinstance(ts_value, datetime):
            dt = ts_value if ts_value.tzinfo else ts_value.replace(tzinfo=timezone.utc)
        else:
            return None
    except Exception:
        return None
    return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds())


def quote_is_stale(
    quote: Optional[dict[str, Any]],
    *,
    stale_after_secs: float = DEFAULT_UNDERLYING_STALE_AFTER_SECS,
) -> bool:
    age = quote_age_seconds(quote)
    if age is None:
        # A quote with prices but no timestamp is NOT classified as stale —
        # it is classified as "unknown freshness" but still has data. The
        # caller decides via quote_is_missing whether to treat as MISSING.
        return False
    return age > stale_after_secs


# ---------------------------------------------------------------------------
# Geometry validation
# ---------------------------------------------------------------------------

def _geometry_state(
    *,
    direction: Optional[str],
    trigger: Optional[float],
    stop: Optional[float],
) -> dict[str, Any]:
    """Pure geometry classifier. Returns:
        {
          trigger_stop_geometry_valid: bool,
          direction_geometry_valid:    bool,
          reason_code:                 Optional[str],  # geometry-specific
        }
    """
    d = (direction or "").strip().upper()
    t = _safe_float(trigger)
    s = _safe_float(stop)

    if t is None or t <= 0:
        return {
            "trigger_stop_geometry_valid": False,
            "direction_geometry_valid":    None,
            "reason_code":                 REASON_TRIGGER_MISSING,
        }
    if s is None or s <= 0:
        return {
            "trigger_stop_geometry_valid": False,
            "direction_geometry_valid":    None,
            "reason_code":                 REASON_STOP_MISSING,
        }
    if d not in ("CALL", "PUT"):
        return {
            "trigger_stop_geometry_valid": True,
            "direction_geometry_valid":    False,
            "reason_code":                 REASON_INVALID_DIRECTION_GEOMETRY,
        }
    # CALL: stop must be below trigger. PUT: stop must be above trigger.
    if d == "CALL" and s >= t:
        return {
            "trigger_stop_geometry_valid": False,
            "direction_geometry_valid":    True,
            "reason_code":                 REASON_INVALID_TRIGGER_STOP_GEOMETRY,
        }
    if d == "PUT" and s <= t:
        return {
            "trigger_stop_geometry_valid": False,
            "direction_geometry_valid":    True,
            "reason_code":                 REASON_INVALID_TRIGGER_STOP_GEOMETRY,
        }
    return {
        "trigger_stop_geometry_valid": True,
        "direction_geometry_valid":    True,
        "reason_code":                 None,
    }


# ---------------------------------------------------------------------------
# Fresh-quote invalidation check
# ---------------------------------------------------------------------------

def evaluate_fresh_quote(
    *,
    direction: Optional[str],
    trigger: Optional[float],
    stop: Optional[float],
    underlying: Optional[dict[str, Any]],
    drift_threshold_pct: float = 0.40,
) -> dict[str, Any]:
    """Given a FRESH underlying quote, decide whether the setup is still
    armable or has been invalidated / drifted away.

    Returns:
        {
          "bucket":     <CONCEPTUAL_BUCKET>,
          "reason_code": <REASON_CODE or None on OK>,
          "final_decision": <decision or None on OK>,
          "ok":          bool,
          "geometry":    <_geometry_state output>,
        }

    Spec §7: CALL is invalidated when bid/last is below stop. PUT is
    invalidated when ask/last is above stop. Drift is computed from
    mid vs trigger.
    """
    geo = _geometry_state(direction=direction, trigger=trigger, stop=stop)
    if geo["reason_code"] in (
        REASON_TRIGGER_MISSING,
        REASON_STOP_MISSING,
        REASON_INVALID_TRIGGER_STOP_GEOMETRY,
    ):
        return {
            "bucket":         BUCKET_TRUE_INVALIDATION,
            "reason_code":    geo["reason_code"],
            "final_decision": DECISION_BLOCKED_INVALID_GEOMETRY,
            "ok":             False,
            "geometry":       geo,
        }
    if geo["reason_code"] == REASON_INVALID_DIRECTION_GEOMETRY:
        return {
            "bucket":         BUCKET_TRUE_INVALIDATION,
            "reason_code":    REASON_INVALID_DIRECTION_GEOMETRY,
            "final_decision": DECISION_BLOCKED_INVALID_GEOMETRY,
            "ok":             False,
            "geometry":       geo,
        }

    d = (direction or "").strip().upper()
    t = float(trigger)  # safe — geometry passed
    s = float(stop)

    bid  = _safe_float((underlying or {}).get("bid"))
    ask  = _safe_float((underlying or {}).get("ask"))
    last = _safe_float((underlying or {}).get("last"))
    mid: Optional[float] = None
    if bid is not None and ask is not None and bid > 0 and ask > 0:
        mid = (bid + ask) / 2.0
    elif last is not None and last > 0:
        mid = last

    # Stop violation check (spec §5 + §7) — only on fresh quote.
    if d == "CALL":
        # CALL invalid if current bid/last is BELOW stop.
        ref = bid if (bid is not None and bid > 0) else last
        if ref is not None and ref < s:
            return {
                "bucket":         BUCKET_TRUE_INVALIDATION,
                "reason_code":    REASON_STOP_BID_BELOW_CALL_STOP,
                "final_decision": DECISION_BLOCKED_TRUE_INVALIDATION,
                "ok":             False,
                "geometry":       geo,
                "ref_price":      ref,
            }
    else:  # PUT
        ref = ask if (ask is not None and ask > 0) else last
        if ref is not None and ref > s:
            return {
                "bucket":         BUCKET_TRUE_INVALIDATION,
                "reason_code":    REASON_STOP_ASK_ABOVE_PUT_STOP,
                "final_decision": DECISION_BLOCKED_TRUE_INVALIDATION,
                "ok":             False,
                "geometry":       geo,
                "ref_price":      ref,
            }

    # Drift check: quote is fresh, but price moved away from trigger.
    if mid is not None:
        drift_pct = abs(mid - t) / t if t > 0 else 0.0
        if drift_pct > drift_threshold_pct:
            return {
                "bucket":         BUCKET_FRESH_PRICE_DRIFT,
                "reason_code":    REASON_PRICE_DRIFT_FROM_TRIGGER,
                "final_decision": DECISION_BLOCKED_PRICE_DRIFT,
                "ok":             False,
                "geometry":       geo,
                "drift_pct":      drift_pct,
                "mid":            mid,
            }

    return {
        "bucket":         None,
        "reason_code":    None,
        "final_decision": None,
        "ok":             True,
        "geometry":       geo,
        "mid":            mid,
    }


# ---------------------------------------------------------------------------
# Classifier — maps a legacy raw_reason string (or absent fresh_quote)
# into a structured Classification.
# ---------------------------------------------------------------------------

@dataclass
class Classification:
    bucket:        str
    reason_code:   str
    final_decision: Optional[str]   # None when retry is possible
    retryable:     bool
    raw_reason:    str


# Legacy raw_reason -> (reason_code, bucket) lookup. Substring match.
_RAW_TOKEN_MAP: tuple[tuple[str, str, str], ...] = (
    # === Quote readiness (retry candidates) ===
    ("underlying_quote_missing",      REASON_UNDERLYING_QUOTE_MISSING, BUCKET_QUOTE_READINESS_UNKNOWN),
    ("underlying_quote_stale",        REASON_UNDERLYING_QUOTE_STALE,   BUCKET_QUOTE_READINESS_UNKNOWN),
    ("option_quote_missing",          REASON_OPTION_QUOTE_MISSING,     BUCKET_QUOTE_READINESS_UNKNOWN),
    ("option_quote_stale",            REASON_OPTION_QUOTE_STALE,       BUCKET_QUOTE_READINESS_UNKNOWN),
    ("quote_refresh_failed",          REASON_QUOTE_REFRESH_FAILED,     BUCKET_QUOTE_READINESS_UNKNOWN),
    ("price_not_ready",               REASON_PRICE_NOT_READY,          BUCKET_QUOTE_READINESS_UNKNOWN),
    ("chain_not_loaded",              REASON_PRICE_NOT_READY,          BUCKET_QUOTE_READINESS_UNKNOWN),
    ("missing_quote",                 REASON_UNDERLYING_QUOTE_MISSING, BUCKET_QUOTE_READINESS_UNKNOWN),
    ("stale_quote",                   REASON_UNDERLYING_QUOTE_STALE,   BUCKET_QUOTE_READINESS_UNKNOWN),

    # === True invalidation (never retry) ===
    # Order matters: more specific tokens before more general ones.
    ("stop_bid_below_call_stop",      REASON_STOP_BID_BELOW_CALL_STOP, BUCKET_TRUE_INVALIDATION),
    ("stop_ask_above_put_stop",       REASON_STOP_ASK_ABOVE_PUT_STOP,  BUCKET_TRUE_INVALIDATION),
    ("arm_below_stop",                REASON_ARM_BELOW_STOP,           BUCKET_TRUE_INVALIDATION),
    ("price_already_invalidated",     REASON_PRICE_ALREADY_INVALIDATED, BUCKET_TRUE_INVALIDATION),
    ("trigger_missing",               REASON_TRIGGER_MISSING,          BUCKET_TRUE_INVALIDATION),
    ("stop_missing",                  REASON_STOP_MISSING,             BUCKET_TRUE_INVALIDATION),
    ("invalid_direction_geometry",    REASON_INVALID_DIRECTION_GEOMETRY, BUCKET_TRUE_INVALIDATION),
    ("invalid_trigger_stop_geometry", REASON_INVALID_TRIGGER_STOP_GEOMETRY, BUCKET_TRUE_INVALIDATION),
    ("geometry_invalid",              REASON_INVALID_TRIGGER_STOP_GEOMETRY, BUCKET_TRUE_INVALIDATION),

    # === Fresh-quote drift (never retry; distinct from stale) ===
    # "stale_price_X_from_trigger" historically meant "quote was fresh but
    # price moved away from trigger." It is NOT a quote-staleness signal.
    # We map it to FRESH_PRICE_DRIFT so retry never fires.
    ("stale_price_",                  REASON_PRICE_DRIFT_FROM_TRIGGER, BUCKET_FRESH_PRICE_DRIFT),
    ("arm_drift",                     REASON_PRICE_DRIFT_FROM_TRIGGER, BUCKET_FRESH_PRICE_DRIFT),
    ("option_premium_stale_",         REASON_PRICE_DRIFT_FROM_TRIGGER, BUCKET_FRESH_PRICE_DRIFT),
    ("price_drift",                   REASON_PRICE_DRIFT_FROM_TRIGGER, BUCKET_FRESH_PRICE_DRIFT),

    # === Watcher-internal labels (legacy strings emitted by entry_watcher) ===
    # These fire BEFORE quote evaluation, so they are routed to UNKNOWN_ARM_FAILURE
    # rather than a price-state bucket. Operators still get a useful reason_code
    # instead of the bare 'unknown' that the legacy path produced.
    ("osm_validation_failed",         REASON_UNKNOWN_ARM_FAILURE,      BUCKET_UNKNOWN),
    ("dedup_block",                   REASON_UNKNOWN_ARM_FAILURE,      BUCKET_UNKNOWN),
    ("opposite_side_conflict",        REASON_UNKNOWN_ARM_FAILURE,      BUCKET_UNKNOWN),
    ("same_side_block",               REASON_UNKNOWN_ARM_FAILURE,      BUCKET_UNKNOWN),

    # === Domain ===
    ("quote_domain_mismatch",         REASON_QUOTE_DOMAIN_MISMATCH,    BUCKET_DOMAIN_SAFETY_BLOCK),
    ("sandbox_quote_in_live",         REASON_QUOTE_DOMAIN_MISMATCH,    BUCKET_DOMAIN_SAFETY_BLOCK),
    ("paper_quote_in_live",           REASON_QUOTE_DOMAIN_MISMATCH,    BUCKET_DOMAIN_SAFETY_BLOCK),

    # === Retry timeout (set by retry layer) ===
    ("arm_timeout",                   REASON_ARM_TIMEOUT,              BUCKET_QUOTE_READINESS_UNKNOWN),
)


def classify_arm_failure(
    raw_reason: Optional[str],
    *,
    fresh_quote: Optional[dict[str, Any]] = None,
) -> Classification:
    """Translate a legacy raw_reason string into a structured Classification.

    `fresh_quote`, when supplied, lets the caller hint that the quote was
    confirmed fresh at the time of the failure. This is the bridge that
    keeps us from mis-tagging a drift event as quote-staleness:

        * If raw_reason hints at staleness BUT fresh_quote is non-empty +
          well-aged, the classifier emits PRICE_DRIFT_FROM_TRIGGER.
        * Otherwise the raw_reason mapping wins.

    Unknown / empty raw_reason -> UNKNOWN_ARM_FAILURE / UNKNOWN.
    """
    r = (raw_reason or "").strip().lower()
    if not r:
        return Classification(
            bucket=BUCKET_UNKNOWN,
            reason_code=REASON_UNKNOWN_ARM_FAILURE,
            final_decision=DECISION_BLOCKED_UNKNOWN_ARM_FAILURE,
            retryable=False,
            raw_reason=raw_reason or "",
        )

    bucket = None
    code = None
    for token, c, b in _RAW_TOKEN_MAP:
        if token in r:
            code = c
            bucket = b
            break

    if code is None:
        return Classification(
            bucket=BUCKET_UNKNOWN,
            reason_code=REASON_UNKNOWN_ARM_FAILURE,
            final_decision=DECISION_BLOCKED_UNKNOWN_ARM_FAILURE,
            retryable=False,
            raw_reason=raw_reason or "",
        )

    # Disambiguation: legacy "stale_price_X_from_trigger" is *fresh drift*
    # in the original code path even though it contains the word "stale".
    # If the caller also supplied a fresh_quote, this confirms the drift
    # bucket. If raw_reason says "underlying_quote_stale" but fresh_quote
    # is present and not actually stale, demote to drift.
    if (
        fresh_quote is not None
        and bucket == BUCKET_QUOTE_READINESS_UNKNOWN
        and not quote_is_missing(fresh_quote)
        and not quote_is_stale(fresh_quote)
    ):
        # The quote is actually fresh. Whatever the raw_reason said,
        # this is drift territory.
        bucket = BUCKET_FRESH_PRICE_DRIFT
        code   = REASON_PRICE_DRIFT_FROM_TRIGGER

    retryable = bucket == BUCKET_QUOTE_READINESS_UNKNOWN

    if bucket == BUCKET_TRUE_INVALIDATION:
        # Geometry-only failures get the invalid-geometry decision.
        if code in (
            REASON_TRIGGER_MISSING,
            REASON_STOP_MISSING,
            REASON_INVALID_DIRECTION_GEOMETRY,
            REASON_INVALID_TRIGGER_STOP_GEOMETRY,
        ):
            final = DECISION_BLOCKED_INVALID_GEOMETRY
        else:
            final = DECISION_BLOCKED_TRUE_INVALIDATION
    elif bucket == BUCKET_FRESH_PRICE_DRIFT:
        final = DECISION_BLOCKED_PRICE_DRIFT
    elif bucket == BUCKET_DOMAIN_SAFETY_BLOCK:
        final = DECISION_BLOCKED_QUOTE_DOMAIN_MISMATCH
    elif bucket == BUCKET_QUOTE_READINESS_UNKNOWN:
        # No final decision yet — caller decides whether to retry.
        final = None
    else:
        final = DECISION_BLOCKED_UNKNOWN_ARM_FAILURE

    return Classification(
        bucket=bucket,
        reason_code=code,
        final_decision=final,
        retryable=retryable,
        raw_reason=raw_reason or "",
    )


# ---------------------------------------------------------------------------
# Audit object builder (dossier-compatible)
# ---------------------------------------------------------------------------

def build_watcher_arm_audit(
    *,
    classification: Classification,
    final_decision: Optional[str] = None,
    recovered_by_retry: bool = False,
    # Identity
    symbol: Optional[str]              = None,
    direction: Optional[str]           = None,
    timeframe: Optional[str]           = None,
    pattern: Optional[str]             = None,
    signal_id: Optional[str]           = None,
    canonical_signal_id: Optional[str] = None,
    plan_id: Optional[str]             = None,
    client_id: Optional[str]           = None,
    pod_id: Optional[str]              = None,
    local_order_id: Optional[str]      = None,
    # Geometry
    trigger_price: Any = None,
    stop_price:    Any = None,
    target_price:  Any = None,
    # Underlying quote
    underlying: Optional[dict[str, Any]] = None,
    # Option quote
    option_symbol: Optional[str]      = None,
    option:        Optional[dict[str, Any]] = None,
    # Quote source / domain
    quote_source:              Optional[str] = None,
    quote_snapshot_id:         Optional[str] = None,
    broker_base_url:           Optional[str] = None,
    sandbox_mode:              Optional[bool] = None,
    data_broker_mode:          Optional[str] = None,
    price_domain_used_for_arm:    Optional[str] = None,
    price_domain_used_for_submit: Optional[str] = None,
    # Retry / attempts
    arm_attempts:        Optional[list[dict[str, Any]]] = None,
    arm_attempt_number:  int = 1,
    arm_retry_count:     int = 0,
    retry_schedule_seconds: Optional[list[int]] = None,
    retry_started_at:    Optional[str] = None,
    retry_ended_at:      Optional[str] = None,
    # Circuit
    circuit_breaker_open: bool = False,
    # Optional instrumentation (spec §3 - never required)
    quote_request_started_at:   Optional[str] = None,
    quote_request_completed_at: Optional[str] = None,
    quote_request_latency_ms:   Optional[float] = None,
) -> dict[str, Any]:
    """Build the canonical watcher_arm_audit JSON. Output is dossier-shaped
    (spec §10) so PR93 can ingest it directly.

    Output is automatically run through redact() so tokens never leak.
    """
    # Decide final_decision: caller may pass one explicitly (e.g. retry
    # layer sets ARMED_AFTER_RETRY / FAILED_AFTER_RETRY); else use the
    # classifier's default; else fall back per bucket.
    decision = final_decision
    if decision is None:
        decision = classification.final_decision
    if decision is None:
        # Classifier returned None (retry candidate, no final yet) — record
        # an explicit "RETRY_DISABLED_QUOTE_NOT_READY" so audit is never blank.
        decision = DECISION_RETRY_DISABLED_QUOTE_NOT_READY

    if decision not in VALID_FINAL_DECISIONS:
        decision = DECISION_BLOCKED_UNKNOWN_ARM_FAILURE

    geo = _geometry_state(
        direction=direction,
        trigger=_safe_float(trigger_price),
        stop=_safe_float(stop_price),
    )

    underlying_bid  = _safe_float((underlying or {}).get("bid"))
    underlying_ask  = _safe_float((underlying or {}).get("ask"))
    underlying_last = _safe_float((underlying or {}).get("last"))
    underlying_mid  = (
        (underlying_bid + underlying_ask) / 2.0
        if underlying_bid is not None and underlying_ask is not None
        and underlying_bid > 0 and underlying_ask > 0
        else (underlying_last if underlying_last and underlying_last > 0 else None)
    )

    option_bid  = _safe_float((option or {}).get("bid"))
    option_ask  = _safe_float((option or {}).get("ask"))
    option_last = _safe_float((option or {}).get("last"))
    option_mid  = (
        (option_bid + option_ask) / 2.0
        if option_bid is not None and option_ask is not None
        and option_bid > 0 and option_ask > 0
        else (option_last if option_last and option_last > 0 else None)
    )

    tp = _safe_float(trigger_price)
    sp = _safe_float(stop_price)
    distance_to_trigger_pct = (
        (underlying_mid - tp) / tp if (underlying_mid is not None and tp and tp > 0) else None
    )
    distance_to_stop_pct = (
        (underlying_mid - sp) / sp if (underlying_mid is not None and sp and sp > 0) else None
    )

    lifecycle = LIFECYCLE_RECOVERED if recovered_by_retry else LIFECYCLE_FAILED

    audit = {
        # Dossier shape (spec §10)
        "dossier_section":     DOSSIER_SECTION,
        "dossier_version":     DOSSIER_VERSION,
        "lifecycle_event":     lifecycle,
        "created_at":          _now_iso(),
        # Classification
        "block_stage":         "watcher_arm",
        "conceptual_bucket":   classification.bucket,
        "block_reason":        classification.reason_code,
        "reason_code":         classification.reason_code,
        "raw_reason":          classification.raw_reason,
        "final_decision":      decision,
        # Identity
        "symbol":              symbol,
        "direction":           direction,
        "timeframe":           timeframe,
        "pattern":             pattern,
        "signal_id":           signal_id,
        "canonical_signal_id": canonical_signal_id,
        "plan_id":             plan_id,
        "client_id":           client_id,
        "pod_id":              pod_id,
        "local_order_id":      local_order_id,
        # Geometry
        "trigger_price":       tp,
        "stop_price":          sp,
        "target_price":        _safe_float(target_price),
        "trigger_stop_geometry_valid": geo["trigger_stop_geometry_valid"],
        "direction_geometry_valid":    geo["direction_geometry_valid"],
        "distance_to_trigger_pct":     distance_to_trigger_pct,
        "distance_to_stop_pct":        distance_to_stop_pct,
        # Underlying quote
        "underlying_bid":              underlying_bid,
        "underlying_ask":              underlying_ask,
        "underlying_mid":              underlying_mid,
        "underlying_last":             underlying_last,
        "underlying_quote_ts":         (underlying or {}).get("quote_ts")
                                        or (underlying or {}).get("ts"),
        "underlying_quote_age_seconds": quote_age_seconds(underlying),
        # Option quote
        "option_symbol":               option_symbol,
        "option_bid":                  option_bid,
        "option_ask":                  option_ask,
        "option_mid":                  option_mid,
        "option_last":                 option_last,
        "option_quote_ts":             (option or {}).get("quote_ts")
                                        or (option or {}).get("ts"),
        "option_quote_age_seconds":    quote_age_seconds(option),
        # Quote source / domain
        "quote_source":                quote_source,
        "quote_snapshot_id":           quote_snapshot_id,
        "broker_base_url":             broker_base_url,
        "sandbox_mode":                sandbox_mode,
        "data_broker_mode":            data_broker_mode,
        "price_domain_used_for_arm":    price_domain_used_for_arm,
        "price_domain_used_for_submit": price_domain_used_for_submit,
        # Retry sequence (spec §4)
        "arm_attempt_number":  int(arm_attempt_number),
        "arm_retry_count":     int(arm_retry_count),
        "retry_schedule_seconds": list(retry_schedule_seconds or []),
        "retry_started_at":    retry_started_at,
        "retry_ended_at":      retry_ended_at,
        "recovered_by_retry":  bool(recovered_by_retry),
        "arm_attempts":        list(arm_attempts or []),
        # Circuit
        "circuit_breaker_open": bool(circuit_breaker_open),
        # Optional instrumentation
        "quote_request_started_at":   quote_request_started_at,
        "quote_request_completed_at": quote_request_completed_at,
        "quote_request_latency_ms":   _safe_float(quote_request_latency_ms),
    }

    return redact(audit)


def build_attempt_record(
    *,
    attempt_number: int,
    reason_code_before_attempt: Optional[str],
    underlying: Optional[dict[str, Any]] = None,
    option:     Optional[dict[str, Any]] = None,
    conceptual_bucket_after_refresh: Optional[str] = None,
    reason_code_after_refresh:       Optional[str] = None,
    decision_after_attempt:          Optional[str] = None,
) -> dict[str, Any]:
    """Build one entry for watcher_arm_audit.arm_attempts (spec §4).
    Always run through redact()."""
    underlying_bid  = _safe_float((underlying or {}).get("bid"))
    underlying_ask  = _safe_float((underlying or {}).get("ask"))
    underlying_last = _safe_float((underlying or {}).get("last"))
    underlying_mid  = (
        (underlying_bid + underlying_ask) / 2.0
        if underlying_bid is not None and underlying_ask is not None
        and underlying_bid > 0 and underlying_ask > 0
        else (underlying_last if underlying_last and underlying_last > 0 else None)
    )
    option_bid  = _safe_float((option or {}).get("bid"))
    option_ask  = _safe_float((option or {}).get("ask"))
    option_last = _safe_float((option or {}).get("last"))
    option_mid  = (
        (option_bid + option_ask) / 2.0
        if option_bid is not None and option_ask is not None
        and option_bid > 0 and option_ask > 0
        else (option_last if option_last and option_last > 0 else None)
    )
    rec = {
        "attempt_number":               int(attempt_number),
        "attempted_at":                 _now_iso(),
        "reason_code_before_attempt":   reason_code_before_attempt,
        "underlying_bid":               underlying_bid,
        "underlying_ask":               underlying_ask,
        "underlying_mid":               underlying_mid,
        "underlying_last":              underlying_last,
        "underlying_quote_ts":          (underlying or {}).get("quote_ts")
                                          or (underlying or {}).get("ts"),
        "underlying_quote_age_seconds": quote_age_seconds(underlying),
        "option_bid":                   option_bid,
        "option_ask":                   option_ask,
        "option_mid":                   option_mid,
        "option_last":                  option_last,
        "option_quote_ts":              (option or {}).get("quote_ts")
                                          or (option or {}).get("ts"),
        "option_quote_age_seconds":     quote_age_seconds(option),
        "conceptual_bucket_after_refresh": conceptual_bucket_after_refresh,
        "reason_code_after_refresh":       reason_code_after_refresh,
        "decision_after_attempt":          decision_after_attempt,
    }
    return redact(rec)


# ---------------------------------------------------------------------------
# Safe merge into orders.meta
# ---------------------------------------------------------------------------

def merge_into_order_meta(
    existing_meta: Any,
    watcher_arm_audit: dict[str, Any],
) -> dict[str, Any]:
    """Safely merge the audit into orders.meta.

    Rules (spec §3):
      * Do not overwrite existing orders.meta fields.
      * Do not delete existing metadata.
      * If orders.meta.watcher_arm_audit already exists, the new audit
        is appended/replaced sensibly (most recent wins for top-level
        fields, arm_attempts are concatenated).

    Accepts existing_meta in any of: None, dict, JSON string. Returns a
    new dict (never mutates input).
    """
    base: dict[str, Any] = {}
    if isinstance(existing_meta, dict):
        # Deep copy via json round-trip is safest but expensive; shallow
        # copy works because we never mutate nested structures.
        base = dict(existing_meta)
    elif isinstance(existing_meta, str) and existing_meta.strip():
        try:
            import json
            parsed = json.loads(existing_meta)
            if isinstance(parsed, dict):
                base = parsed
        except Exception:
            base = {}

    prior = base.get("watcher_arm_audit")
    if isinstance(prior, dict):
        # Concatenate arm_attempts so the dossier preserves history.
        prior_attempts = prior.get("arm_attempts") or []
        new_attempts = watcher_arm_audit.get("arm_attempts") or []
        merged_attempts = list(prior_attempts) + list(new_attempts)
        # Build new audit blob: new top-level fields win, attempts merged.
        merged = dict(prior)
        merged.update(watcher_arm_audit)
        merged["arm_attempts"] = merged_attempts
        # Bump attempt counters if obviously stale
        merged["arm_retry_count"] = max(
            int(prior.get("arm_retry_count") or 0),
            int(watcher_arm_audit.get("arm_retry_count") or 0),
        )
        base["watcher_arm_audit"] = redact(merged)
    else:
        base["watcher_arm_audit"] = redact(watcher_arm_audit)
    return base


__all__ = [
    # Buckets
    "BUCKET_TRUE_INVALIDATION",
    "BUCKET_QUOTE_READINESS_UNKNOWN",
    "BUCKET_FRESH_PRICE_DRIFT",
    "BUCKET_DOMAIN_SAFETY_BLOCK",
    "BUCKET_UNKNOWN",
    "CONCEPTUAL_BUCKETS",
    "NON_RETRYABLE_BUCKETS",
    # Reason codes
    "REASON_UNDERLYING_QUOTE_MISSING",
    "REASON_UNDERLYING_QUOTE_STALE",
    "REASON_OPTION_QUOTE_MISSING",
    "REASON_OPTION_QUOTE_STALE",
    "REASON_QUOTE_REFRESH_FAILED",
    "REASON_PRICE_NOT_READY",
    "REASON_ARM_TIMEOUT",
    "REASON_PRICE_ALREADY_INVALIDATED",
    "REASON_ARM_BELOW_STOP",
    "REASON_STOP_BID_BELOW_CALL_STOP",
    "REASON_STOP_ASK_ABOVE_PUT_STOP",
    "REASON_TRIGGER_MISSING",
    "REASON_STOP_MISSING",
    "REASON_INVALID_DIRECTION_GEOMETRY",
    "REASON_INVALID_TRIGGER_STOP_GEOMETRY",
    "REASON_PRICE_DRIFT_FROM_TRIGGER",
    "REASON_QUOTE_DOMAIN_MISMATCH",
    "REASON_UNKNOWN_ARM_FAILURE",
    "VALID_REASON_CODES",
    # Decisions
    "DECISION_ARMED",
    "DECISION_ARMED_AFTER_RETRY",
    "DECISION_FAILED_AFTER_RETRY",
    "DECISION_BLOCKED_TRUE_INVALIDATION",
    "DECISION_BLOCKED_INVALID_GEOMETRY",
    "DECISION_BLOCKED_PRICE_DRIFT",
    "DECISION_BLOCKED_QUOTE_DOMAIN_MISMATCH",
    "DECISION_BLOCKED_UNKNOWN_ARM_FAILURE",
    "DECISION_RETRY_DISABLED_QUOTE_NOT_READY",
    "VALID_FINAL_DECISIONS",
    # Dossier
    "DOSSIER_SECTION",
    "DOSSIER_VERSION",
    "LIFECYCLE_FAILED",
    "LIFECYCLE_RECOVERED",
    # Core API
    "Classification",
    "classify_arm_failure",
    "evaluate_fresh_quote",
    "build_watcher_arm_audit",
    "build_attempt_record",
    "merge_into_order_meta",
    "redact",
    # Quote helpers
    "quote_is_missing",
    "quote_is_stale",
    "quote_age_seconds",
]
