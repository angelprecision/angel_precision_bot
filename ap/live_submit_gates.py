"""
ap/live_submit_gates.py

LIVE pre-submit safety gates — PR #305.

These are the last three brakes between the materialization pipeline and the
broker POST. Each is a pure, stateless classifier that returns a GateResult:
either PASS (submit may proceed) or FAIL (block submit, stamp durable audit,
terminalize the row).

WHY THIS MODULE EXISTS
──────────────────────
The META/VZ incident postmortem exposed three failure classes that could still
reach the broker even after PR #300 (broker_ready lifecycle) and PR #304
(watcher/order lifecycle integrity):

    1. IDENTITY DRIFT — a plan whose execution_mode was blank/unknown, or
       whose client_id was missing, or where the plan and OSM disagreed on
       which mode was active. Result: live money could be committed under
       an incorrect account.

    2. STALE MARKET VALIDITY — a trigger confirmed at 9:31 ET, held in the
       materialization retry queue until 9:47 ET, submitted after the move
       had already run its course. The contract was materialized correctly,
       but the underlying opportunity was gone.

    3. STALE TRIGGER AGE — same category but from a different angle: a
       genuine trigger breach from 30-45 minutes ago must not fire a live
       order regardless of intermediate lifecycle validity.

DESIGN CONTRACT
───────────────
• Pure classification — no DB writes, no side effects on watcher/OSM state.
• Never raises — client-money code; a classifier bug must never crash submit.
• LIVE fails closed on every ambiguous condition.
• PAPER fails closed on identity (mode drift is a genuine bug), but market-
  validity and trigger-age are LIVE-only gates (paper is for testing, we
  want stale-quote signals to still exercise the flow).
• Every FAIL returns a canonical reason_code from a documented set — the
  operator dashboard filters on these strings.
• Every FAIL returns an audit dict ready for orders.meta.

USAGE
─────
    from ap.live_submit_gates import (
        GateResult, GateOutcome,
        check_identity_gate,
        check_market_validity_gate,
        check_trigger_age_gate,
    )

    # At the exact call site of submit_existing_entry:
    id_result = check_identity_gate(
        client_id=plan_client_id,
        execution_mode=plan_execution_mode,
        watcher_client_id=watcher_client_id,
        osm_client_id=osm_client_id,
    )
    if not id_result.passed:
        # Stamp audit, terminalize, DO NOT SUBMIT
        _terminalize(id_result.reason_code)
        return

    # ... likewise for market_validity and trigger_age ...
"""

from __future__ import annotations

import os
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from urllib.parse import urlparse

from ap.logger import get_logger

log = get_logger("ap.live_submit_gates")


# ─────────────────────────────────────────────────────────────────────────────
# Canonical reason codes — must stay in sync with operator runbook filters
# ─────────────────────────────────────────────────────────────────────────────

class GateOutcome:
    """Canonical block reasons — orders.meta.live_submit_gate.reason_code."""

    # Identity gate (Gate 1)
    LIVE_SUBMIT_IDENTITY_INVALID           = "LIVE_SUBMIT_IDENTITY_INVALID"
    LIVE_SUBMIT_EXECUTION_MODE_UNKNOWN     = "LIVE_SUBMIT_EXECUTION_MODE_UNKNOWN"
    LIVE_SUBMIT_CLIENT_ID_MISSING          = "LIVE_SUBMIT_CLIENT_ID_MISSING"
    LIVE_SUBMIT_CLIENT_ID_MISMATCH         = "LIVE_SUBMIT_CLIENT_ID_MISMATCH"
    LIVE_SUBMIT_EXECUTION_MODE_MISMATCH    = "LIVE_SUBMIT_EXECUTION_MODE_MISMATCH"

    # Market validity gate (Gate 2)
    CURRENT_PRICE_AGE_UNKNOWN              = "CURRENT_PRICE_AGE_UNKNOWN"
    CURRENT_PRICE_FETCH_FAILED             = "CURRENT_PRICE_FETCH_FAILED"
    CURRENT_PRICE_INVALID                  = "CURRENT_PRICE_INVALID"
    CURRENT_PRICE_FRESH_SYNC_FETCH         = "CURRENT_PRICE_FRESH_SYNC_FETCH"
    CURRENT_PRICE_STALE                    = "CURRENT_PRICE_STALE"
    CURRENT_PRICE_MISSING                  = "CURRENT_PRICE_MISSING"
    CURRENT_PRICE_ZERO                     = "CURRENT_PRICE_ZERO"
    CURRENT_OPTION_SIDE_INVALID            = "CURRENT_OPTION_SIDE_INVALID"
    TARGET_ALREADY_INVALID                 = "TARGET_ALREADY_INVALID"
    REMAINING_OPPORTUNITY_TOO_SMALL        = "REMAINING_OPPORTUNITY_TOO_SMALL"
    CALL_NO_LONGER_ABOVE_TRIGGER           = "CALL_NO_LONGER_ABOVE_TRIGGER"
    CALL_STOP_ALREADY_BROKEN               = "CALL_STOP_ALREADY_BROKEN"
    PUT_NO_LONGER_BELOW_TRIGGER            = "PUT_NO_LONGER_BELOW_TRIGGER"
    PUT_STOP_ALREADY_BROKEN                = "PUT_STOP_ALREADY_BROKEN"

    # Trigger age gate (Gate 3)
    STALE_TRIGGER_BREACH                   = "STALE_TRIGGER_BREACH"

    # Sentinel
    PASS                                   = "PASS"


@dataclass
class GateResult:
    """Uniform return type for every gate.

    Fields:
        passed: True → submit may proceed to the next gate.
                False → block submit; caller must terminalize + audit.
        reason_code: canonical code from GateOutcome (PASS on success).
        detail: short human-readable explanation for logs / audit.
        audit: dict ready to be stamped into orders.meta under the gate's key.
               Always non-empty on FAIL, populated with all data used for
               the decision so an operator can reproduce the logic.
    """
    passed: bool
    reason_code: str = GateOutcome.PASS
    detail: str = ""
    audit: dict = field(default_factory=dict)


class MarketTruthAuthority(str, Enum):
    SUBMIT_VALID = "SUBMIT_VALID"
    REARM_DIRECTION_REVERSAL = "REARM_DIRECTION_REVERSAL"
    HOLD_MARKET_TRUTH_UNAVAILABLE = "HOLD_MARKET_TRUTH_UNAVAILABLE"
    TERMINAL_SETUP_COMPLETE = "TERMINAL_SETUP_COMPLETE"


def validate_retry_market_quote_authority(
    quote: object,
    *,
    transport: object,
    now: Optional[datetime] = None,
    max_age_ms: int = 5000,
    max_future_skew_ms: int = 1000,
) -> dict:
    """Prove an approved LIVE Tradier observation and provider timestamp."""
    if not isinstance(quote, dict):
        return {"valid": False, "reason": "MARKET_QUOTE_INVALID_PAYLOAD"}
    cfg = getattr(transport, "cfg", None)
    base_url = str(
        getattr(cfg, "base_url", None)
        or getattr(transport, "base_url", None)
        or ""
    ).strip()
    parsed = urlparse(base_url)
    if parsed.scheme.lower() != "https" or parsed.hostname != "api.tradier.com":
        return {
            "valid": False,
            "reason": "MARKET_QUOTE_UNAPPROVED_TRANSPORT",
            "transport_url": base_url or None,
        }
    raw_source = str(
        quote.get("source")
        or quote.get("quote_source")
        or quote.get("provider")
        or ""
    ).strip().lower()
    if raw_source and raw_source not in {"tradier", "tradier_live", "api.tradier.com"}:
        return {
            "valid": False,
            "reason": "MARKET_QUOTE_SOURCE_UNPROVEN",
            "quote_source": raw_source or None,
        }
    # Tradier production payloads omit provider metadata. The exact approved
    # HTTPS transport is authoritative for a source-less quote; contradictory
    # explicit metadata remains rejected above.
    resolved_source = raw_source or "tradier_live"

    # Validate the timestamp of EVERY price leg downstream market truth may use.
    # The retry market gate reads the bid for PUT trigger truth, the ask for
    # CALL trigger truth, and bid+ask for midpoint truth. A single-timestamp
    # "first nonblank wins" selection could accept a stale bid while reading a
    # fresh ask (or vice versa); the oldest USED leg must control freshness.
    #
    # Common/provider-level timestamp candidates. bid_date/ask_date are
    # leg-specific and are deliberately excluded from this common chain.
    common_timestamp_raw = (
        quote.get("provider_timestamp")
        or quote.get("quote_timestamp")
        or quote.get("timestamp")
        or quote.get("trade_date")
    )

    def _finite_positive(value) -> bool:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return False
        return math.isfinite(numeric) and numeric > 0.0

    def _parse_provider_timestamp(raw) -> Optional[datetime]:
        try:
            if isinstance(raw, datetime):
                observed = raw
            elif isinstance(raw, (int, float)):
                numeric = float(raw)
                if numeric > 10_000_000_000:
                    numeric /= 1000.0
                observed = datetime.fromtimestamp(numeric, tz=timezone.utc)
            else:
                observed = datetime.fromisoformat(
                    str(raw).strip().replace("Z", "+00:00")
                )
            if observed.tzinfo is None:
                # Naive datetime / timezone-less ISO string is not accepted.
                return None
            return observed.astimezone(timezone.utc)
        except (TypeError, ValueError, OverflowError, OSError):
            return None

    bid_used = _finite_positive(quote.get("bid"))
    ask_used = _finite_positive(quote.get("ask"))

    # Each required leg is (diagnostic_field, raw_timestamp_source).
    required_legs: list = []
    if bid_used:
        required_legs.append(
            ("bid", quote.get("bid_date") or common_timestamp_raw)
        )
    if ask_used:
        required_legs.append(
            ("ask", quote.get("ask_date") or common_timestamp_raw)
        )
    if not required_legs:
        # No positive bid or ask: preserve existing authority behavior by
        # validating the common timestamp. Missing/zero prices remain the
        # responsibility of the downstream market-validity gate.
        required_legs.append(("common", common_timestamp_raw))

    parsed_legs: list = []
    for leg_name, raw in required_legs:
        observed = _parse_provider_timestamp(raw)
        if observed is None:
            return {
                "valid": False,
                "reason": "MARKET_QUOTE_TIMESTAMP_UNPROVEN",
            }
        parsed_legs.append((leg_name, observed))

    current = (now or _now_utc()).astimezone(timezone.utc)

    # Age is calculated independently for every required timestamp.
    aged_legs: list = []
    for leg_name, observed in parsed_legs:
        age_ms = (current - observed).total_seconds() * 1000.0
        if not math.isfinite(age_ms) or age_ms < -float(max_future_skew_ms):
            return {
                "valid": False,
                "reason": "MARKET_QUOTE_TIMESTAMP_FUTURE",
                "provider_timestamp": observed.isoformat(),
                "timestamp_field": leg_name,
            }
        aged_legs.append((age_ms, leg_name, observed))
    for age_ms, leg_name, observed in aged_legs:
        if age_ms > float(max_age_ms):
            return {
                "valid": False,
                "reason": "MARKET_QUOTE_STALE",
                "provider_timestamp": observed.isoformat(),
                "quote_age_ms": age_ms,
                "timestamp_field": leg_name,
            }

    # The oldest used price leg controls freshness. Do not average or pick the
    # newest timestamp.
    oldest_age_ms, oldest_leg, oldest_observed = max(
        aged_legs, key=lambda item: item[0]
    )
    result = {
        "valid": True,
        "reason": "MARKET_QUOTE_AUTHORITY_PROVEN",
        "provider_timestamp": oldest_observed.isoformat(),
        "quote_age_ms": max(0.0, oldest_age_ms),
        "quote_source": resolved_source,
        "transport_url": base_url,
    }
    for leg_name, observed in parsed_legs:
        if leg_name == "bid":
            result["bid_provider_timestamp"] = observed.isoformat()
        elif leg_name == "ask":
            result["ask_provider_timestamp"] = observed.isoformat()
    return result


def classify_market_truth(result: GateResult) -> MarketTruthAuthority:
    """Map the pure market gate into the deferred-retry authority contract."""
    reason = str(getattr(result, "reason_code", "") or "")
    if bool(getattr(result, "passed", False)) and reason == GateOutcome.PASS:
        return MarketTruthAuthority.SUBMIT_VALID
    if reason in {
        GateOutcome.CALL_NO_LONGER_ABOVE_TRIGGER,
        GateOutcome.PUT_NO_LONGER_BELOW_TRIGGER,
    }:
        return MarketTruthAuthority.REARM_DIRECTION_REVERSAL
    if reason in {
        GateOutcome.CALL_STOP_ALREADY_BROKEN,
        GateOutcome.PUT_STOP_ALREADY_BROKEN,
        GateOutcome.TARGET_ALREADY_INVALID,
        GateOutcome.REMAINING_OPPORTUNITY_TOO_SMALL,
    }:
        return MarketTruthAuthority.TERMINAL_SETUP_COMPLETE
    return MarketTruthAuthority.HOLD_MARKET_TRUTH_UNAVAILABLE


# ─────────────────────────────────────────────────────────────────────────────
# Config helpers (env-tunable, safe defaults)
# ─────────────────────────────────────────────────────────────────────────────

def _int_env(name: str, default: int) -> int:
    try:
        return max(1, int(os.getenv(name, str(default)).strip()))
    except (TypeError, ValueError):
        return default


def _float_env(name: str, default: float) -> float:
    try:
        return max(0.0, float(os.getenv(name, str(default)).strip()))
    except (TypeError, ValueError):
        return default


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        s = str(dt_str).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        d = datetime.fromisoformat(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d
    except Exception:
        return None


def derive_submit_execution_mode(
    *,
    proof_execution_mode: Optional[str] = None,
    approved_plan_execution_mode: Optional[str] = None,
    self_execution_mode: Optional[str] = None,
    self_mode: Optional[str] = None,
    self_paper: Optional[bool] = None,
    osm_execution_mode: Optional[str] = None,
) -> str:
    """Resolve the mode used by the final submit gates.

    Blank/unknown values intentionally resolve to an empty string so the
    identity gate fails closed with LIVE_SUBMIT_EXECUTION_MODE_UNKNOWN.
    """
    for candidate in (
        proof_execution_mode,
        approved_plan_execution_mode,
        self_execution_mode,
        self_mode,
        "live" if self_paper is False else None,
        osm_execution_mode,
    ):
        c = str(candidate or "").strip().lower()
        if c in {"live", "paper"}:
            return c
    return ""


def resolve_trigger_timestamps(
    *,
    order_meta: Optional[dict] = None,
    approved_plan_watched_signal=None,
    watched_signal=None,
) -> tuple[Optional[str], Optional[str]]:
    """Resolve trigger timestamps with durable order meta as the source of truth."""
    meta = order_meta if isinstance(order_meta, dict) else {}
    crossed_at = meta.get("trigger_crossed_at")
    confirmed_at = meta.get("trigger_confirmed_at")

    if not crossed_at:
        w = approved_plan_watched_signal or watched_signal
        if w is not None:
            tc = getattr(w, "trigger_crossed_at", None)
            tf = getattr(w, "triggered_at", None)
            if tc is not None:
                crossed_at = tc.isoformat() if hasattr(tc, "isoformat") else str(tc)
            if tf is not None and not confirmed_at:
                confirmed_at = tf.isoformat() if hasattr(tf, "isoformat") else str(tf)

    return (
        str(crossed_at) if crossed_at else None,
        str(confirmed_at) if confirmed_at else None,
    )


# ─────────────────────────────────────────────────────────────────────────────
# GATE 1 — IDENTITY
# ─────────────────────────────────────────────────────────────────────────────

_VALID_EXECUTION_MODES: frozenset[str] = frozenset({"live", "paper"})


def check_identity_gate(
    *,
    client_id: Optional[str],
    execution_mode: Optional[str],
    watcher_client_id: Optional[str] = None,
    osm_client_id: Optional[str] = None,
    watcher_execution_mode: Optional[str] = None,
    osm_execution_mode: Optional[str] = None,
) -> GateResult:
    """
    Prove submit identity before any broker POST.

    Rules:
        1. client_id must be non-empty
        2. execution_mode must be exactly "live" or "paper" (case-insensitive)
        3. LIVE submit requires execution_mode == "live"
        4. If any of the caller-supplied cross-check IDs are provided and
           non-empty, they must match the primary. Missing cross-checks are
           acceptable (paper handoff paths may not carry all four) — this
           gate blocks on POSITIVE disagreement only, never absence.

    LIVE and PAPER both fail closed on blank/unknown execution_mode and
    missing client_id. Identity is not a mode-conditional concept.
    """
    cid = str(client_id or "").strip()
    mode = str(execution_mode or "").strip().lower()

    audit_common = {
        "gate":            "identity",
        "checked_at":      _now_utc().isoformat(),
        "client_id":       cid or None,
        "execution_mode":  mode or None,
        "watcher_client_id": (str(watcher_client_id or "").strip() or None),
        "osm_client_id":     (str(osm_client_id or "").strip() or None),
        "watcher_execution_mode": (str(watcher_execution_mode or "").strip().lower() or None),
        "osm_execution_mode":     (str(osm_execution_mode or "").strip().lower() or None),
    }

    # Rule 1: client_id required
    if not cid:
        return GateResult(
            passed=False,
            reason_code=GateOutcome.LIVE_SUBMIT_CLIENT_ID_MISSING,
            detail="client_id is blank/None — cannot attribute broker submit",
            audit={**audit_common, "passed": False,
                   "reason_code": GateOutcome.LIVE_SUBMIT_CLIENT_ID_MISSING},
        )

    # Rule 2: execution_mode must be a known value
    if mode not in _VALID_EXECUTION_MODES:
        return GateResult(
            passed=False,
            reason_code=GateOutcome.LIVE_SUBMIT_EXECUTION_MODE_UNKNOWN,
            detail=f"execution_mode={mode!r} not in {sorted(_VALID_EXECUTION_MODES)}",
            audit={**audit_common, "passed": False,
                   "reason_code": GateOutcome.LIVE_SUBMIT_EXECUTION_MODE_UNKNOWN},
        )

    # Rule 4: cross-check client_id mismatch (positive disagreement only)
    for cross_cid, source_name in (
        (str(watcher_client_id or "").strip(), "watcher"),
        (str(osm_client_id or "").strip(), "osm"),
    ):
        if cross_cid and cross_cid != cid:
            return GateResult(
                passed=False,
                reason_code=GateOutcome.LIVE_SUBMIT_CLIENT_ID_MISMATCH,
                detail=(
                    f"plan.client_id={cid!r} disagrees with "
                    f"{source_name}.client_id={cross_cid!r}"
                ),
                audit={**audit_common, "passed": False,
                       "reason_code": GateOutcome.LIVE_SUBMIT_CLIENT_ID_MISMATCH,
                       "mismatch_source": source_name},
            )

    # Rule 4: cross-check execution_mode mismatch
    for cross_mode, source_name in (
        (str(watcher_execution_mode or "").strip().lower(), "watcher"),
        (str(osm_execution_mode or "").strip().lower(), "osm"),
    ):
        if cross_mode and cross_mode != mode:
            return GateResult(
                passed=False,
                reason_code=GateOutcome.LIVE_SUBMIT_EXECUTION_MODE_MISMATCH,
                detail=(
                    f"plan.execution_mode={mode!r} disagrees with "
                    f"{source_name}.execution_mode={cross_mode!r}"
                ),
                audit={**audit_common, "passed": False,
                       "reason_code": GateOutcome.LIVE_SUBMIT_EXECUTION_MODE_MISMATCH,
                       "mismatch_source": source_name},
            )

    return GateResult(
        passed=True,
        reason_code=GateOutcome.PASS,
        detail=f"identity verified: client_id={cid} execution_mode={mode}",
        audit={**audit_common, "passed": True, "reason_code": GateOutcome.PASS},
    )


# ─────────────────────────────────────────────────────────────────────────────
# GATE 2 — FINAL MARKET VALIDITY
# ─────────────────────────────────────────────────────────────────────────────

def _remaining_opportunity_pct(
    side: str,
    current_price: float,
    trigger_price: float,
    target_price: Optional[float],
) -> Optional[float]:
    """
    How much of the intended move is still available from current_price to target.

    For a CALL: (target - current) / (target - trigger)
    For a PUT:  (current - target) / (trigger - target)

    Returns None when the geometry is undefined (missing target, degenerate
    move sizing). Returns 0.0 or negative when the target has already been
    touched or overshot.
    """
    if target_price is None:
        return None
    try:
        cp, tg, tr = float(current_price), float(target_price), float(trigger_price)
    except (TypeError, ValueError):
        return None
    if not all(math.isfinite(v) for v in (cp, tg, tr)):
        return None

    _side = str(side or "").strip().upper()
    if _side == "CALL":
        denom = tg - tr
        if denom <= 0:
            return None
        return (tg - cp) / denom
    if _side == "PUT":
        denom = tr - tg
        if denom <= 0:
            return None
        return (cp - tg) / denom
    return None


def check_market_validity_gate(
    *,
    side: str,
    trigger_price: float,
    stop_price: Optional[float],
    target_price: Optional[float],
    current_bid: Optional[float],
    current_ask: Optional[float],
    quote_age_ms: Optional[float] = None,
    quote_source: Optional[str] = None,
    quote_fetched_at: Optional[object] = None,
    quote_provenance: Optional[str] = None,
    quote_fetch_failed: bool = False,
    quote_fetch_error: Optional[str] = None,
    execution_mode: str = "live",
    max_quote_age_ms: Optional[int] = None,
    min_remaining_opportunity_pct: Optional[float] = None,
) -> GateResult:
    """
    Final pre-submit sanity check on the underlying market.

    Called immediately before broker POST. Fails closed on LIVE for any
    stale/missing quote or invalidated setup geometry. PAPER passes with a
    warning so testing can continue past stale-quote sandbox artifacts.

    Rules (LIVE):
      • current bid and ask must both exist and be > 0
      • quote age must not exceed max_quote_age_ms (env: LIVE_SUBMIT_MAX_QUOTE_AGE_MS,
        default 5000)
      • CALL: current mid ≥ trigger (still in breach direction),
              current mid < target (target not touched),
              remaining opportunity ≥ min_remaining_pct (default 0.10 = 10%),
              current mid > stop (stop not broken)
      • PUT: mirror of CALL

    Rules (PAPER):
      • Same checks but log-only. Result.passed always True in paper.
    """
    max_age_ms = max_quote_age_ms if max_quote_age_ms is not None else _int_env("LIVE_SUBMIT_MAX_QUOTE_AGE_MS", 5000)
    max_future_skew_ms = _int_env("LIVE_SYNC_QUOTE_MAX_FUTURE_SKEW_MS", 1000)
    min_rem_pct = min_remaining_opportunity_pct if min_remaining_opportunity_pct is not None else _float_env("LIVE_SUBMIT_MIN_REMAINING_OPPORTUNITY_PCT", 0.10)
    _mode = str(execution_mode or "").strip().lower()
    _live = _mode == "live"

    def _to_float(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    def _finite_float(v):
        f = _to_float(v)
        return f if f is not None and math.isfinite(f) else None

    def _quote_value_was_supplied(v) -> bool:
        if v is None:
            return False
        if isinstance(v, str) and not v.strip():
            return False
        return True

    bid = _to_float(current_bid)
    ask = _to_float(current_ask)
    bid_supplied = _quote_value_was_supplied(current_bid)
    ask_supplied = _quote_value_was_supplied(current_ask)
    bid_malformed = bid is None and bid_supplied
    ask_malformed = ask is None and ask_supplied
    provider_age_ms = _to_float(quote_age_ms)
    provider_age_invalid = provider_age_ms is not None and not math.isfinite(provider_age_ms)
    provenance = str(quote_provenance or "").strip().lower()
    fetched_at_iso = None
    fetched_age_ms = None
    fetched_at_invalid = False
    fetched_at_future_ms = None
    now = _now_utc()
    if quote_fetched_at is not None:
        try:
            if isinstance(quote_fetched_at, datetime):
                fetched_at = quote_fetched_at
            else:
                fetched_at = datetime.fromisoformat(
                    str(quote_fetched_at).strip().replace("Z", "+00:00")
                )
            if fetched_at.tzinfo is None:
                raise ValueError("quote_fetched_at must include timezone")
            fetched_at = fetched_at.astimezone(timezone.utc)
            fetched_at_iso = fetched_at.isoformat()
            raw_fetched_age_ms = (now - fetched_at).total_seconds() * 1000.0
            if not math.isfinite(raw_fetched_age_ms):
                fetched_at_invalid = True
            elif raw_fetched_age_ms < -float(max_future_skew_ms):
                fetched_at_future_ms = abs(raw_fetched_age_ms)
                fetched_at_invalid = True
            else:
                fetched_age_ms = max(0.0, raw_fetched_age_ms)
        except (TypeError, ValueError, OverflowError):
            fetched_at_iso = None
            fetched_age_ms = None
            fetched_at_invalid = True

    effective_age_ms = provider_age_ms
    freshness_code = None
    if provider_age_invalid:
        effective_age_ms = None
    if effective_age_ms is None and provenance == "synchronous_submit_fetch" and not fetched_at_invalid:
        effective_age_ms = fetched_age_ms
        if effective_age_ms is not None:
            freshness_code = GateOutcome.CURRENT_PRICE_FRESH_SYNC_FETCH
    mid = None
    if (
        bid is not None and ask is not None
        and math.isfinite(bid) and math.isfinite(ask)
        and bid > 0 and ask > 0 and ask >= bid
    ):
        mid = (bid + ask) / 2.0

    audit = {
        "gate":                       "market_validity",
        "checked_at":                 _now_utc().isoformat(),
        "execution_mode":             _mode,
        "side":                       str(side or "").upper(),
        "trigger_price":              _to_float(trigger_price),
        "stop_price":                 _to_float(stop_price),
        "target_price":               _to_float(target_price),
        "current_bid":                bid,
        "current_ask":                ask,
        "current_mid":                mid,
        "quote_age_ms":               effective_age_ms,
        "provider_quote_age_ms":      provider_age_ms,
        "quote_source":               str(quote_source or "").strip() or None,
        "quote_fetched_at":           fetched_at_iso,
        "quote_fetched_at_invalid":   fetched_at_invalid,
        "quote_fetched_at_future_ms": fetched_at_future_ms,
        "quote_provenance":           provenance or None,
        "quote_fetch_failed":         bool(quote_fetch_failed),
        "quote_fetch_error":          str(quote_fetch_error or "").strip() or None,
        "quote_freshness_code":       freshness_code,
        "max_quote_age_ms":           int(max_age_ms),
        "max_future_skew_ms":         int(max_future_skew_ms),
        "min_remaining_opportunity_pct": min_rem_pct,
    }

    def _fail(reason_code: str, detail: str) -> GateResult:
        # In paper mode we log the fail but still return passed=True so the
        # sandbox flow can be exercised end-to-end. LIVE always blocks.
        blocked = _live
        _audit_out = {**audit, "passed": not blocked, "reason_code": reason_code}
        if blocked:
            _audit_out["blocked"] = True
        return GateResult(
            passed=not blocked,
            reason_code=reason_code if blocked else GateOutcome.PASS,
            detail=("BLOCKED " if blocked else "warn ") + detail,
            audit=_audit_out,
        )

    # Rule: fetch transport completed successfully.
    if quote_fetch_failed:
        return _fail(
            GateOutcome.CURRENT_PRICE_FETCH_FAILED,
            f"submit-time quote fetch failed: {quote_fetch_error or 'unknown'}",
        )

    # Rule: quote presence and numeric validity.
    if bid is None and ask is None:
        return _fail(GateOutcome.CURRENT_PRICE_MISSING, "no bid/ask received")
    if bid_malformed or ask_malformed:
        return _fail(GateOutcome.CURRENT_PRICE_INVALID, f"bid={current_bid!r} ask={current_ask!r}")
    if bid is None or ask is None:
        return _fail(GateOutcome.CURRENT_PRICE_MISSING, f"bid={bid} ask={ask}")
    if not math.isfinite(bid) or not math.isfinite(ask):
        return _fail(GateOutcome.CURRENT_PRICE_INVALID, f"bid={bid} ask={ask}")
    if bid <= 0 or ask <= 0:
        return _fail(GateOutcome.CURRENT_PRICE_ZERO, f"bid={bid} ask={ask}")
    if ask < bid:
        return _fail(GateOutcome.CURRENT_PRICE_INVALID, f"inverted bid/ask bid={bid} ask={ask}")
    if provider_age_invalid:
        return _fail(GateOutcome.CURRENT_PRICE_INVALID, f"quote_age_ms={provider_age_ms}")
    if fetched_at_invalid and provenance == "synchronous_submit_fetch" and provider_age_ms is None:
        return _fail(
            GateOutcome.CURRENT_PRICE_AGE_UNKNOWN,
            "invalid synchronous quote fetch timestamp",
        )

    # Rule: quote freshness
    if effective_age_ms is None and _live:
        return _fail(
            GateOutcome.CURRENT_PRICE_AGE_UNKNOWN,
            "quote age unknown — LIVE requires provider age or explicit "
            "synchronous_submit_fetch provenance with a valid UTC fetch timestamp",
        )
    if effective_age_ms is not None and effective_age_ms < 0:
        return _fail(GateOutcome.CURRENT_PRICE_AGE_UNKNOWN, "quote age is negative")
    if effective_age_ms is not None and effective_age_ms > max_age_ms:
        return _fail(
            GateOutcome.CURRENT_PRICE_STALE,
            f"quote_age_ms={effective_age_ms:.0f} > max={max_age_ms}",
        )

    _side = str(side or "").strip().upper()
    if _side not in {"CALL", "PUT"}:
        return _fail(
            GateOutcome.CURRENT_OPTION_SIDE_INVALID,
            f"option side must be CALL or PUT, got {side!r}",
        )
    tr = _finite_float(trigger_price)
    tg = _finite_float(target_price)
    st = _finite_float(stop_price)
    if tr is None:
        return _fail(GateOutcome.CURRENT_PRICE_INVALID, f"trigger_price={trigger_price!r}")
    if target_price is not None and tg is None:
        return _fail(GateOutcome.CURRENT_PRICE_INVALID, f"target_price={target_price!r}")
    if stop_price is not None and st is None:
        return _fail(GateOutcome.CURRENT_PRICE_INVALID, f"stop_price={stop_price!r}")

    # Rule: direction-specific geometry
    # Amendment 3 (PR #305): use ASK (CALL) / BID (PUT) as primary trigger-
    # lane check — matching WatchedSignal.check breach semantics exactly.
    # Mid is only used as fallback when bid/ask missing, and for stop/target
    # geometry (which is symmetric on both sides).
    if _side == "CALL":
        _trigger_check = ask if ask and ask > 0 else mid
        if st is not None and st > 0 and mid is not None and mid <= st:
            return _fail(
                GateOutcome.CALL_STOP_ALREADY_BROKEN,
                f"mid={mid:.4f} <= stop={st:.4f}",
            )
        if tg is not None and tg > 0 and mid is not None and mid >= tg:
            return _fail(
                GateOutcome.TARGET_ALREADY_INVALID,
                f"CALL mid={mid:.4f} >= target={tg:.4f} — move complete",
            )
        if _trigger_check is not None and _trigger_check < tr:
            return _fail(
                GateOutcome.CALL_NO_LONGER_ABOVE_TRIGGER,
                f"ask={ask:.4f} mid={(mid or 0.0):.4f} < trigger={tr:.4f} — breach reversed",
            )
    elif _side == "PUT":
        _trigger_check = bid if bid and bid > 0 else mid
        if st is not None and st > 0 and mid is not None and mid >= st:
            return _fail(
                GateOutcome.PUT_STOP_ALREADY_BROKEN,
                f"mid={mid:.4f} >= stop={st:.4f}",
            )
        if tg is not None and tg > 0 and mid is not None and mid <= tg:
            return _fail(
                GateOutcome.TARGET_ALREADY_INVALID,
                f"PUT mid={mid:.4f} <= target={tg:.4f} — move complete",
            )
        if _trigger_check is not None and _trigger_check > tr:
            return _fail(
                GateOutcome.PUT_NO_LONGER_BELOW_TRIGGER,
                f"bid={bid:.4f} mid={(mid or 0.0):.4f} > trigger={tr:.4f} — breach reversed",
            )
    # Rule: remaining opportunity
    rem_pct = _remaining_opportunity_pct(_side, mid, tr, tg)
    if rem_pct is not None:
        audit["remaining_opportunity_pct"] = round(rem_pct, 6)
        if rem_pct < min_rem_pct:
            return _fail(
                GateOutcome.REMAINING_OPPORTUNITY_TOO_SMALL,
                f"remaining={rem_pct:.2%} < min={min_rem_pct:.2%}",
            )

    return GateResult(
        passed=True,
        reason_code=GateOutcome.PASS,
        detail=f"market valid: mid={mid:.4f} side={_side}",
        audit={
            **audit,
            "passed": True,
            "reason_code": GateOutcome.PASS,
            "quote_freshness_code": freshness_code,
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# GATE 3 — TRIGGER AGE
# ─────────────────────────────────────────────────────────────────────────────

def check_trigger_age_gate(
    *,
    trigger_crossed_at: Optional[str],
    trigger_confirmed_at: Optional[str] = None,
    last_confirmed_trigger_at: Optional[str] = None,
    submit_attempt_at: Optional[datetime] = None,
    execution_mode: str = "live",
    max_age_seconds: Optional[int] = None,
) -> GateResult:
    """
    Enforce a maximum age from FIRST breach (or most recent fresh confirmation)
    to broker POST.

    AMENDMENT: PR #323 Seam 2 — timing-boundary fix

    Problem:  The gate previously computed age solely from trigger_crossed_at
    (the original breach time).  During deferred materialization with transient
    quote failures, the recovery path can span 20–300 seconds.  A valid
    reconfirmation obtained during recovery (fresh underlying quote passing the
    market_validity gate) does not help if the final trigger_age gate counts the
    entire elapsed time since the original breach.  This causes legitimate
    recoveries to be rejected purely due to inconsistent timing semantics, not
    because the thesis has reversed.

    Resolution:  When last_confirmed_trigger_at is provided (a fresh underlying
    quote confirmed the setup is still on the valid trigger side, obtained via
    the market_validity gate BEFORE the trigger_age gate is reached), the gate
    applies the age limit to the most recent confirmation rather than the
    original breach.  original_trigger_crossed_at is preserved as a diagnostic
    field and is NEVER the age reference when fresh confirmation is available.

    The max_age limit remains unchanged (ENTRY_TRIGGER_MAX_AGE_SEC).  This is
    not a blanket increase of the window — it is correct application of the same
    limit to the semantically correct anchor timestamp.

    Key invariants:
      * original trigger_crossed_at is always preserved for diagnostics.
      * last_confirmed_trigger_at MUST be no earlier than trigger_crossed_at
        (checked below); if it is, the original breach time is used.
      * If last_confirmed_trigger_at is in the future (clock skew): fail
        closed on LIVE — a future confirmation is not a valid anchor.
      * If both timestamps are absent: fail closed on LIVE.

    Timing fields written to orders.meta (Seam 2):
      * original_trigger_crossed_at  — immutable diagnostic (= trigger_crossed_at)
      * last_confirmed_trigger_at    — fresh underlying confirmation timestamp
      * operational_recovery_started_at — when the current recovery attempt began
      * operational_recovery_elapsed_ms — elapsed ms for this recovery pass
      * selected_quote_at            — when the option quote used for the limit was captured
      * absolute_entry_deadline      — hard lifecycle deadline (e.g. now + max_recovery_window)
      * entry_cutoff_et              — 15:30 ET entry cutoff (configurable)
    """
    _mode = str(execution_mode or "").strip().lower()
    _live = _mode == "live"
    max_age = max_age_seconds if max_age_seconds is not None else _int_env("ENTRY_TRIGGER_MAX_AGE_SEC", 120)

    now = submit_attempt_at or _now_utc()
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    crossed = _parse_iso(trigger_crossed_at)
    confirmed = _parse_iso(trigger_confirmed_at)
    last_confirmed = _parse_iso(last_confirmed_trigger_at)

    # ── Choose the effective anchor ──────────────────────────────────────────
    # Use last_confirmed_trigger_at when ALL of:
    #   1. trigger_crossed_at is present and parseable (we MUST have the original)
    #   2. last_confirmed is provided and parseable
    #   3. last_confirmed is not in the future (clock skew guard)
    #   4. last_confirmed >= trigger_crossed_at (regression guard — confirmation
    #      cannot pre-date the original breach)
    # Without the original trigger_crossed_at, the gate fails closed on LIVE
    # regardless of whether last_confirmed_trigger_at is present — a fresh
    # confirmation cannot substitute for a missing original breach proof.
    _effective_anchor = crossed
    _anchor_label = "trigger_crossed_at"
    if crossed is not None and last_confirmed is not None:
        _lcf_age = (now - last_confirmed).total_seconds()
        _valid_last_confirmed = (
            _lcf_age >= -1.0                # not in the future
            and last_confirmed >= crossed   # not earlier than original breach
        )
        if _valid_last_confirmed:
            _effective_anchor = last_confirmed
            _anchor_label = "last_confirmed_trigger_at"

    audit = {
        "gate":                        "trigger_age",
        "trigger_crossed_at":          trigger_crossed_at,
        "trigger_confirmed_at":        trigger_confirmed_at,
        "last_confirmed_trigger_at":   last_confirmed_trigger_at,
        "effective_anchor":            _anchor_label,
        "submit_attempt_at":           now.isoformat(),
        "max_age_seconds":             int(max_age),
        "execution_mode":              _mode,
    }

    if _effective_anchor is None:
        # No anchor — LIVE fails closed.
        return GateResult(
            passed=not _live,
            reason_code=GateOutcome.STALE_TRIGGER_BREACH if _live else GateOutcome.PASS,
            detail="trigger_crossed_at missing — cannot verify age" + (" [LIVE BLOCKED]" if _live else " [paper warn]"),
            audit={**audit, "passed": not _live,
                   "reason_code": GateOutcome.STALE_TRIGGER_BREACH if _live else GateOutcome.PASS,
                   "age_seconds": None,
                   "missing_trigger_crossed_at": True},
        )

    age_seconds = (now - _effective_anchor).total_seconds()
    audit["age_seconds"] = round(age_seconds, 3)
    # Always record original breach age for diagnostics even when fresh
    # confirmation is the effective anchor.
    if crossed is not None and _anchor_label == "last_confirmed_trigger_at":
        audit["original_breach_age_seconds"] = round((now - crossed).total_seconds(), 3)

    # Clock skew: anchor timestamp is in the future
    if age_seconds < -1.0:
        return GateResult(
            passed=not _live,
            reason_code=GateOutcome.STALE_TRIGGER_BREACH if _live else GateOutcome.PASS,
            detail=f"{_anchor_label} is {-age_seconds:.1f}s in the future — clock skew",
            audit={**audit, "passed": not _live, "clock_skew": True},
        )

    if age_seconds > max_age:
        return GateResult(
            passed=not _live,
            reason_code=GateOutcome.STALE_TRIGGER_BREACH if _live else GateOutcome.PASS,
            detail=f"age={age_seconds:.1f}s > max={max_age}s (anchor={_anchor_label})",
            audit={**audit, "passed": not _live,
                   "reason_code": GateOutcome.STALE_TRIGGER_BREACH if _live else GateOutcome.PASS,
                   "blocked": _live},
        )

    return GateResult(
        passed=True,
        reason_code=GateOutcome.PASS,
        detail=f"trigger age {age_seconds:.1f}s within {max_age}s limit (anchor={_anchor_label})",
        audit={**audit, "passed": True, "reason_code": GateOutcome.PASS},
    )


# ─────────────────────────────────────────────────────────────────────────────
# Convenience: run all three gates in order
# ─────────────────────────────────────────────────────────────────────────────

def run_all_live_submit_gates(
    *,
    # Identity
    client_id: Optional[str],
    execution_mode: Optional[str],
    watcher_client_id: Optional[str] = None,
    osm_client_id: Optional[str] = None,
    watcher_execution_mode: Optional[str] = None,
    osm_execution_mode: Optional[str] = None,
    # Market validity
    side: str = "",
    trigger_price: float = 0.0,
    stop_price: Optional[float] = None,
    target_price: Optional[float] = None,
    current_bid: Optional[float] = None,
    current_ask: Optional[float] = None,
    quote_age_ms: Optional[float] = None,
    quote_source: Optional[str] = None,
    quote_fetched_at: Optional[object] = None,
    quote_provenance: Optional[str] = None,
    quote_fetch_failed: bool = False,
    quote_fetch_error: Optional[str] = None,
    # Trigger age
    trigger_crossed_at: Optional[str] = None,
    trigger_confirmed_at: Optional[str] = None,
    submit_attempt_at: Optional[datetime] = None,
) -> tuple[GateResult, dict]:
    """
    Run identity → market_validity → trigger_age in order.
    Returns (first_failing_result_or_passing, combined_audit).

    Short-circuits on first failure — subsequent gates are not called.
    Combined audit dict has three top-level keys:
        identity_gate, market_validity_gate, trigger_age_gate
    """
    combined_audit = {}

    id_res = check_identity_gate(
        client_id=client_id,
        execution_mode=execution_mode,
        watcher_client_id=watcher_client_id,
        osm_client_id=osm_client_id,
        watcher_execution_mode=watcher_execution_mode,
        osm_execution_mode=osm_execution_mode,
    )
    combined_audit["identity_gate"] = id_res.audit
    if not id_res.passed:
        return id_res, combined_audit

    mv_res = check_market_validity_gate(
        side=side,
        trigger_price=trigger_price,
        stop_price=stop_price,
        target_price=target_price,
        current_bid=current_bid,
        current_ask=current_ask,
        quote_age_ms=quote_age_ms,
        quote_source=quote_source,
        quote_fetched_at=quote_fetched_at,
        quote_provenance=quote_provenance,
        quote_fetch_failed=quote_fetch_failed,
        quote_fetch_error=quote_fetch_error,
        execution_mode=str(execution_mode or "").strip().lower(),
    )
    combined_audit["market_validity_gate"] = mv_res.audit
    if not mv_res.passed:
        return mv_res, combined_audit

    ta_res = check_trigger_age_gate(
        trigger_crossed_at=trigger_crossed_at,
        trigger_confirmed_at=trigger_confirmed_at,
        submit_attempt_at=submit_attempt_at,
        execution_mode=str(execution_mode or "").strip().lower(),
    )
    combined_audit["trigger_age_gate"] = ta_res.audit
    if not ta_res.passed:
        return ta_res, combined_audit

    return GateResult(
        passed=True,
        reason_code=GateOutcome.PASS,
        detail="all three LIVE submit gates passed",
        audit=combined_audit,
    ), combined_audit
