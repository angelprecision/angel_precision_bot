"""
PHASE 6: dashboard entry telemetry projection.

The dashboard backend used to read order.meta directly and re-implement the
field-derivation logic in JavaScript. That meant every code change here
forced a dashboard release. Phase 6 ships ONE Python function that produces
the canonical, dashboard-shaped projection of every entry attempt:

    compute_entry_telemetry(order_row, position_row=None, broker_order=None)
       -> dict[str, Any]

The dashboard backend calls this. The bot loop also calls this when it
writes decision_events and audit_log entries. Single source of truth.

Output schema (every key always present; null when unknown)
-----------------------------------------------------------
  client_id              : str
  local_order_id         : str
  broker_order_id        : str | None
  symbol                 : str
  contract               : str
  direction              : 'CALL' | 'PUT'
  status                 : str          (order.status)
  score                  : float | None
  entry_attempt          : int          (0 = initial submit, 1+ = repeg/retry)
  repeg_attempt          : int          (count of re-pegs, from order.meta)
  retry_attempt          : int          (post-cancel retry index, from order.meta)
  reason_bucket          : str          (categorical, see RESULT_BUCKETS below)
  cancel_reason_detail   : str | None
  selector_ask           : float | None
  submit_ask             : float | None
  submit_limit           : float | None
  fill_price             : float | None  (from position_row.avg_fill or broker)
  quote_age_ms           : int | None
  seconds_to_fill        : float | None  (fill_ts - submit_ts; null if unfilled)
  account_equity         : float | None
  position_budget        : float | None
  final_qty              : int | None
  sizing_reason_code     : str | None

reason_bucket categorical values
--------------------------------
  filled                 : status FILLED, position exists
  partial                : status PARTIAL_FILL
  canceled_signal_dead   : cancel reason in NON_RETRYABLE_REASONS (Phase 5)
  canceled_signal_alive  : cancel reason in RETRYABLE_REASONS (Phase 5)
  canceled_other         : cancel without a categorized reason
  rejected               : broker REJECTED
  expired                : ENTRY_MAX_AGE_* timeout reached
  pending                : still working (ACK / SUBMITTED / NEW)
  unknown                : status doesn't fit any bucket

Determinism: same inputs -> same output (no time-of-day calls inside).
The caller passes 'now_ts' if it wants seconds_to_fill from in-flight clock,
but normally fill_ts is on position_row.
"""

from __future__ import annotations

from typing import Any, Optional


# ----------------------------------------------------------------------
# Reason classification (mirrors Phase 5 non/retryable sets)
# ----------------------------------------------------------------------

# Kept inline (not imported from post_cancel_retry) so this module has zero
# import-time side effects and can be loaded by the dashboard backend
# without pulling in random / dataclasses / etc.
_NON_RETRYABLE_REASONS = frozenset({
    "thesis_invalid", "stale_thesis_call", "stale_thesis_put",
    "spread_wide", "runaway_quote", "runaway_quote_at_submit",
    "positions_full", "daily_trade_cap", "daily_trade_cap_post_preempt",
    "lost_handoff", "lost_handoff_systemic", "lost_handoff_systemic_halt",
    "risk_gate_blocked", "kill_switch_active", "read_only_mode",
    "client_inactive",
})

_RETRYABLE_REASONS = frozenset({
    "entry_max_age_normal_reached", "entry_max_age_aplus_reached",
    "stale_entry_timeout", "missed_move", "broker_transient_error",
    "broker_rejected_transient", "unfilled_at_ladder_top",
})

# Canonical bucket names \u2014 dashboard reads these verbatim.
RESULT_BUCKETS = (
    "filled",
    "partial",
    "canceled_signal_dead",
    "canceled_signal_alive",
    "canceled_other",
    "rejected",
    "expired",
    "pending",
    "unknown",
)


# ----------------------------------------------------------------------
# Tiny coercion helpers (None-safe)
# ----------------------------------------------------------------------

def _f(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _i(x: Any) -> Optional[int]:
    if x is None or x == "":
        return None
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return None


def _s(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x)
    return s if s else None


def _norm_reason(raw: Any) -> str:
    if not raw:
        return ""
    s = str(raw).strip().lower()
    for prefix in ("cancel_reason:", "reason:", "stale_entry:"):
        if s.startswith(prefix):
            s = s[len(prefix):].strip()
    return s


# ----------------------------------------------------------------------
# reason_bucket derivation
# ----------------------------------------------------------------------

def derive_reason_bucket(status: Any, cancel_reason: Any) -> str:
    """Categorical bucket for the dashboard.

    Inputs are the order.status and the cancel reason (free text). We return
    one of the values in RESULT_BUCKETS. The dashboard uses this to build
    its funnel chart without re-implementing the classification logic.
    """
    st = (str(status or "")).strip().upper()
    if st in ("FILLED",):
        return "filled"
    if st in ("PARTIAL_FILL", "PARTIAL"):
        return "partial"
    if st in ("REJECTED",):
        return "rejected"
    if st in ("EXPIRED",):
        return "expired"
    if st in ("CANCELED", "CANCELLED"):
        reason = _norm_reason(cancel_reason)
        # Phase 2 timeout tokens -> expired bucket so ops can see ceiling hits.
        if reason in ("entry_max_age_normal_reached", "entry_max_age_aplus_reached"):
            return "expired"
        if reason in _NON_RETRYABLE_REASONS:
            return "canceled_signal_dead"
        if reason in _RETRYABLE_REASONS:
            return "canceled_signal_alive"
        if reason == "":
            return "canceled_other"
        return "canceled_other"
    if st in ("ACK", "ACKED", "ACKNOWLEDGED", "SUBMITTED", "NEW", "PENDING",
              "OPEN", "PENDING_FILL", "PENDING_TRIGGER", "CREATED", "OK", "ACCEPTED"):
        return "pending"
    return "unknown"


# ----------------------------------------------------------------------
# Fill-time helpers
# ----------------------------------------------------------------------

def _seconds_to_fill(order_row: dict, position_row: Optional[dict]) -> Optional[float]:
    """Compute (fill_ts - submit_ts) in seconds, if both are known.

    submit_ts is order.created_ts (ISO 8601 or epoch).
    fill_ts is position.opened_ts (preferred) or order.updated_ts on FILLED.
    """
    submit_raw = order_row.get("created_ts") or order_row.get("submit_ts")
    if position_row:
        fill_raw = (position_row.get("opened_ts")
                    or position_row.get("fill_ts")
                    or position_row.get("created_ts"))
    else:
        # Fall back to order.updated_ts when status==FILLED.
        fill_raw = order_row.get("updated_ts") if (
            str(order_row.get("status") or "").upper() == "FILLED"
        ) else None

    if not submit_raw or not fill_raw:
        return None

    try:
        return float(_to_epoch(fill_raw) - _to_epoch(submit_raw))
    except Exception:
        return None


def _to_epoch(ts: Any) -> float:
    if isinstance(ts, (int, float)):
        return float(ts)
    s = str(ts)
    # ISO 8601 (with optional Z)
    from datetime import datetime, timezone
    try:
        # support "...Z" and "+00:00"
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except ValueError:
        return float("nan")


def _fill_price(order_row: dict, position_row: Optional[dict], broker_order: Optional[dict]) -> Optional[float]:
    """Resolve the actual fill price.

    Preference order:
      1. position_row.avg_fill (canonical when present)
      2. broker_order.avg_fill_price / average_fill_price / fill_price /
         filled_avg_price / price
      3. None
    """
    if position_row:
        for k in ("avg_fill", "avg_fill_price", "fill_price"):
            v = position_row.get(k)
            if v not in (None, "", 0):
                fp = _f(v)
                if fp is not None:
                    return fp
    if broker_order:
        for k in ("avg_fill_price", "average_fill_price", "fill_price",
                  "filled_avg_price", "price"):
            v = broker_order.get(k)
            if v not in (None, "", 0):
                fp = _f(v)
                if fp is not None:
                    return fp
    return None


# ----------------------------------------------------------------------
# Main projection
# ----------------------------------------------------------------------

def compute_entry_telemetry(
    order_row: dict,
    position_row: Optional[dict] = None,
    broker_order: Optional[dict] = None,
) -> dict[str, Any]:
    """Return the dashboard-shaped telemetry dict for one order.

    None-safe in every field. Always returns the full 23-key schema (every
    key always present) so the dashboard does not need to .get() with
    defaults.
    """
    if not isinstance(order_row, dict):
        order_row = {}
    meta = order_row.get("meta") or {}
    if not isinstance(meta, dict):
        meta = {}

    status = order_row.get("status")
    cancel_reason = (order_row.get("last_error")
                     or meta.get("cancel_reason_detail")
                     or meta.get("last_repeg_reason"))
    reason_bucket = derive_reason_bucket(status, cancel_reason)

    return {
        # Identity
        "client_id":            _s(order_row.get("client_id")),
        "local_order_id":       _s(order_row.get("local_order_id")),
        "broker_order_id":      _s(order_row.get("broker_order_id")),
        "symbol":               _s(order_row.get("symbol")) or _s(meta.get("ticker")),
        "contract":             _s(order_row.get("contract")),
        "direction":            _s(order_row.get("direction")) or _s(meta.get("direction")),
        "status":               _s(status),
        "score":                _f(meta.get("score")),

        # Attempt counters (Phase 2/3/5)
        "entry_attempt":        _i(meta.get("entry_attempt")) or 0,
        "repeg_attempt":        _i(meta.get("repeg_attempts")) or 0,
        "retry_attempt":        _i(meta.get("retry_attempts")) or 0,

        # Categorical
        "reason_bucket":        reason_bucket,
        "cancel_reason_detail": _s(cancel_reason),

        # Submit-time pricing (Phase 3)
        "selector_ask":         _f(meta.get("selector_ask")),
        "submit_ask":           _f(meta.get("submit_ask")),
        "submit_limit":         _f(meta.get("submit_limit")) or _f(order_row.get("limit_price")),

        # Fill (from position_row or broker echo)
        "fill_price":           _fill_price(order_row, position_row, broker_order),

        # Timing
        "quote_age_ms":         _i(meta.get("quote_age_ms")),
        "seconds_to_fill":      _seconds_to_fill(order_row, position_row),

        # Sizing (Phase 4)
        "account_equity":       _f(meta.get("account_equity")),
        "position_budget":      _f(meta.get("position_budget")),
        "final_qty":            _i(meta.get("final_qty")) or _i(order_row.get("qty")),
        "sizing_reason_code":   _s(meta.get("sizing_reason_code")),
    }
