"""Bounded entry-efficiency recheck policy.

This module is deliberately pure: it evaluates a caller-supplied snapshot and
does not read the broker, write the database, or schedule a worker.  The
watcher and execution core own those side effects so the policy can be replayed
without creating a second submit path.
"""
from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo


ET = ZoneInfo("America/New_York")
ENTRY_EFFICIENCY_OBSERVE_ONLY = "observe_only"
ENTRY_EFFICIENCY_PAPER_AUTHORITATIVE = "paper_authoritative"
ENTRY_EFFICIENCY_MODES = frozenset({
    ENTRY_EFFICIENCY_OBSERVE_ONLY,
    ENTRY_EFFICIENCY_PAPER_AUTHORITATIVE,
})

ENTRY_EFFICIENCY_CONFIG_UNSET = "CONFIG_UNSET"
ENTRY_EFFICIENCY_CONFIG_VALID = "CONFIG_VALID"
ENTRY_EFFICIENCY_CONFIG_INVALID = "CONFIG_INVALID"
ENTRY_EFFICIENCY_CONFIG_CONFLICT = "CONFIG_CONFLICT"

READY_NOW = "READY_NOW"
WAIT_CONFIRMATION = "WAIT_CONFIRMATION"
REARM_FOR_REBREACH = "REARM_FOR_REBREACH"
TERMINAL_INVALID = "TERMINAL_INVALID"

ENTRY_EFFICIENCY_IDENTITY_FIELDS = (
    "local_order_id",
    "signal_id",
    "canonical_signal_id",
    "client_id",
    "execution_mode",
)

_DAILY_TIMEFRAMES = frozenset({"1d", "d", "day", "daily"})
_UNTRUSTED_INTELLIGENCE_KEYS = frozenset({
    "breach_profile",
    "breach_intelligence",
    "intelligence_breach_profile",
    "entry_efficiency_profile",
    "decision_class",
    "classification",
    "entry_efficiency_class",
    "recommendation",
    "decision",
    "continuation_confirmed",
    "fresh_continuation",
    "genuine_rebreach",
    "reset_confirmed",
})

_ENTRY_EFFICIENCY_GENERATION_RE = re.compile(r"(?:0|[1-9][0-9]*)\Z")
_ENTRY_EFFICIENCY_GENERATION_MISSING = object()


def _safe_float(value: Any) -> float | None:
    # bool is an int subclass; accepting it would turn False into a valid zero
    # quote and True into a valid one-dollar quote.
    if isinstance(value, bool) or value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif value is None or str(value).strip() == "":
        return None
    else:
        try:
            parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    # A naive timestamp cannot establish an exchange-session boundary or a
    # bounded elapsed-time window.  Reject it; never guess UTC.
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _normalize_entry_efficiency_mode(raw: Any) -> str | None:
    normalized = str(raw or "").strip().lower().replace("-", "_")
    return normalized if normalized in ENTRY_EFFICIENCY_MODES else None


def resolve_entry_efficiency_mode_with_reason(raw: Any = None) -> tuple[str, str]:
    """Resolve rollout mode and configuration provenance fail-closed.

    The targeted PAPER 2-3-2 policy is opt-in through an explicit
    ``paper_authoritative`` mode.  Unset, ``observe_only``, malformed, or
    unsupported configuration fails closed to observation.  LIVE has no
    authoritative mode in this PR.  When environment configuration is used,
    both supported aliases are parsed independently; a malformed or
    contradictory alias never gets hidden by first-value-wins resolution.
    """
    if raw is not None:
        normalized = _normalize_entry_efficiency_mode(raw)
        if normalized is None:
            return ENTRY_EFFICIENCY_OBSERVE_ONLY, ENTRY_EFFICIENCY_CONFIG_INVALID
        return normalized, ENTRY_EFFICIENCY_CONFIG_VALID

    configured: list[str] = []
    for variable_name in ("AP_ENTRY_EFFICIENCY_MODE", "ENTRY_EFFICIENCY_MODE"):
        if variable_name not in os.environ:
            continue
        candidate = os.environ.get(variable_name)
        if not str(candidate or "").strip():
            return ENTRY_EFFICIENCY_OBSERVE_ONLY, ENTRY_EFFICIENCY_CONFIG_INVALID
        normalized = _normalize_entry_efficiency_mode(candidate)
        if normalized is None:
            return ENTRY_EFFICIENCY_OBSERVE_ONLY, ENTRY_EFFICIENCY_CONFIG_INVALID
        configured.append(normalized)

    if not configured:
        return ENTRY_EFFICIENCY_OBSERVE_ONLY, ENTRY_EFFICIENCY_CONFIG_UNSET
    if len(set(configured)) != 1:
        return ENTRY_EFFICIENCY_OBSERVE_ONLY, ENTRY_EFFICIENCY_CONFIG_CONFLICT
    return configured[0], ENTRY_EFFICIENCY_CONFIG_VALID


def resolve_entry_efficiency_mode(raw: Any = None) -> str:
    """Resolve the rollout mode only, preserving fail-closed semantics."""
    return resolve_entry_efficiency_mode_with_reason(raw)[0]


def parse_entry_efficiency_generation(
    raw: Any = _ENTRY_EFFICIENCY_GENERATION_MISSING,
    *,
    state: Any = "",
) -> int | None:
    """Parse one durable lifecycle generation without reconstructing truth.

    The only implicit value permitted is the initial generation ``0`` when no
    lifecycle state exists and the generation field is absent.  Once a state
    exists, a generation must be explicitly present and canonical.  Boolean,
    float, signed, zero-padded, blank, whitespace-padded, and malformed values
    are rejected rather than coerced into an initial generation.
    """
    state_present = bool(str(state or "").strip())
    if raw is _ENTRY_EFFICIENCY_GENERATION_MISSING:
        return 0 if not state_present else None
    if raw is None or isinstance(raw, bool):
        return None

    if isinstance(raw, int):
        generation = raw
    elif isinstance(raw, str):
        if not _ENTRY_EFFICIENCY_GENERATION_RE.fullmatch(raw):
            return None
        try:
            generation = int(raw)
        except (TypeError, ValueError, OverflowError):
            return None
    else:
        return None

    # Generation zero belongs only to the empty initial lifecycle.  Once a
    # durable state exists, the first legitimate transition has already
    # advanced the generation to one; accepting stateful zero would let a
    # corrupt row re-enter the CAS lifecycle as if it were authoritative.
    if state_present:
        return generation if generation >= 1 else None
    return 0 if generation == 0 else None


def _normalize_entry_efficiency_execution_mode(raw: Any) -> str | None:
    normalized = str(raw or "").strip().lower()
    return normalized if normalized in {"paper", "live"} else None


def entry_efficiency_identity_is_proven(
    signal: Mapping[str, Any] | None,
    *,
    metadata: Mapping[str, Any] | None = None,
    runtime_execution_mode: Any = None,
    runtime_paper: Any = None,
    state: Any = None,
    generation: Any = _ENTRY_EFFICIENCY_GENERATION_MISSING,
) -> bool:
    """Prove persisted efficiency identity before honoring a lifecycle state.

    A non-empty lifecycle must be bound to the reconstructed opportunity and
    to the current PAPER runtime.  The empty initial lifecycle at generation
    zero is intentionally allowed to establish those durable identity fields
    on its first authoritative CAS transition.
    """
    if not isinstance(signal, Mapping):
        return False
    persisted_metadata = metadata if metadata is not None else signal.get("metadata")
    if not isinstance(persisted_metadata, Mapping):
        return False

    signal_mode = _normalize_entry_efficiency_execution_mode(
        signal.get("execution_mode")
    )
    runtime_mode = _normalize_entry_efficiency_execution_mode(
        runtime_execution_mode
    )
    if signal_mode != "paper" or runtime_mode != "paper" or runtime_paper is not True:
        return False

    persisted_state = str(
        persisted_metadata.get("entry_efficiency_state") or ""
    ).strip().upper()
    effective_state = (
        persisted_state if state is None else str(state or "").strip().upper()
    )
    if state is not None and effective_state != persisted_state:
        return False

    persisted_generation = parse_entry_efficiency_generation(
        persisted_metadata.get(
            "entry_efficiency_generation", _ENTRY_EFFICIENCY_GENERATION_MISSING
        ),
        state=effective_state,
    )
    if persisted_generation is None:
        return False
    observed_generation = (
        persisted_generation
        if generation is _ENTRY_EFFICIENCY_GENERATION_MISSING
        else parse_entry_efficiency_generation(generation, state=effective_state)
    )
    if observed_generation is None or observed_generation != persisted_generation:
        return False

    # No lifecycle state exists yet; generation zero is the only valid initial
    # state and the first authoritative transition may establish identity.
    if not effective_state:
        return persisted_generation == 0 and observed_generation == 0

    expected = {
        "local_order_id": str(signal.get("local_order_id") or "").strip(),
        "signal_id": str(signal.get("signal_id") or "").strip(),
        "canonical_signal_id": str(
            signal.get("canonical_signal_id")
            or persisted_metadata.get("canonical_signal_id")
            or ""
        ).strip(),
        "client_id": str(
            signal.get("client_id")
            or signal.get("client_email")
            or persisted_metadata.get("client_id")
            or persisted_metadata.get("client_email")
            or ""
        ).strip().lower(),
        "execution_mode": signal_mode,
    }
    persisted = {
        "local_order_id": str(
            persisted_metadata.get("entry_efficiency_local_order_id") or ""
        ).strip(),
        "signal_id": str(
            persisted_metadata.get("entry_efficiency_signal_id") or ""
        ).strip(),
        "canonical_signal_id": str(
            persisted_metadata.get("entry_efficiency_canonical_signal_id") or ""
        ).strip(),
        "client_id": str(
            persisted_metadata.get("entry_efficiency_client_id") or ""
        ).strip().lower(),
        "execution_mode": _normalize_entry_efficiency_execution_mode(
            persisted_metadata.get("entry_efficiency_execution_mode")
        ),
    }
    if any(not expected[field] for field in ENTRY_EFFICIENCY_IDENTITY_FIELDS):
        return False
    if any(not persisted[field] for field in ENTRY_EFFICIENCY_IDENTITY_FIELDS):
        return False
    return all(
        persisted[field] == expected[field]
        for field in ENTRY_EFFICIENCY_IDENTITY_FIELDS
    )


def normalize_strategy_pattern(pattern: Any, timeframe: Any) -> tuple[str, bool]:
    """Return the canonical daily 2-3-2 family and whether it applies."""
    raw = str(pattern or "").strip().strip('"\'').lower()
    raw = raw.replace("–", "-").replace("—", "-")
    raw = re.sub(r"\s+", "", raw)
    timeframe_key = str(timeframe or "").strip().lower()
    if timeframe_key in _DAILY_TIMEFRAMES and raw in {"2-3", "2-3-2"}:
        return "2-3-2", True
    return raw, False


def _authoritative_for_mode(mode: str, execution_mode: Any) -> bool:
    runtime_mode = str(execution_mode or "").strip().lower()
    return mode == ENTRY_EFFICIENCY_PAPER_AUTHORITATIVE and runtime_mode == "paper"


def _rth_minutes(now: datetime) -> tuple[float | None, bool]:
    now_et = now.astimezone(ET)
    open_et = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    minutes = (now_et - open_et).total_seconds() / 60.0
    return minutes, now_et.date() == open_et.date()


def _opening_breach(first_breach_at: datetime | None) -> bool:
    if first_breach_at is None:
        return False
    breach_et = first_breach_at.astimezone(ET)
    open_et = breach_et.replace(hour=9, minute=30, second=0, microsecond=0)
    minutes = (breach_et - open_et).total_seconds() / 60.0
    return breach_et.date() == open_et.date() and 0 <= minutes < 30


@dataclass(frozen=True)
class EntryEfficiencyResult:
    decision: str
    mode: str
    authoritative: bool
    reason_code: str
    detail: str
    pattern_raw: str
    canonical_pattern: str
    opening_window: bool
    breach_was_opening: bool
    minutes_since_rth_open: float | None
    first_breach_at: str | None
    rearm_pending: bool
    next_evaluation_at: str | None
    deadline_at: str | None
    evidence: Mapping[str, Any] = field(default_factory=dict)

    @property
    def should_hold(self) -> bool:
        return self.authoritative and self.decision in {
            WAIT_CONFIRMATION,
            REARM_FOR_REBREACH,
        }

    def to_meta(self) -> dict[str, Any]:
        return {
            "entry_efficiency_state": self.decision,
            "entry_efficiency_mode": self.mode,
            "entry_efficiency_authoritative": self.authoritative,
            "entry_efficiency_reason_code": self.reason_code,
            "entry_efficiency_detail": self.detail,
            "entry_efficiency_pattern_raw": self.pattern_raw,
            "entry_efficiency_canonical_pattern": self.canonical_pattern,
            "entry_efficiency_opening_window": self.opening_window,
            "entry_efficiency_breach_was_opening": self.breach_was_opening,
            "entry_efficiency_minutes_since_rth_open": self.minutes_since_rth_open,
            "entry_efficiency_first_breach_at": self.first_breach_at,
            "entry_efficiency_rearm_pending": self.rearm_pending,
            "entry_efficiency_next_eval_at": self.next_evaluation_at,
            "entry_efficiency_deadline_at": self.deadline_at,
            "entry_efficiency_evidence": dict(self.evidence),
        }


def evaluate_entry_efficiency(
    *,
    ticker: Any,
    side: Any,
    pattern: Any,
    timeframe: Any,
    metadata: Mapping[str, Any] | None,
    execution_mode: Any,
    trigger_price: Any,
    stop_price: Any,
    target_price: Any,
    bid: Any,
    ask: Any,
    quote_age_ms: Any,
    first_breach_at: Any,
    prior_state: Any = "",
    rearm_pending: bool = False,
    prior_deadline_at: Any = None,
    now: datetime | None = None,
    mode: Any = None,
) -> EntryEfficiencyResult:
    """Evaluate one fresh breach snapshot.

    The decision is intentionally conservative.  If the authoritative mode
    cannot prove fresh quote truth or a permitted continuation, it returns a
    durable wait/rearm state; it never converts missing truth into READY_NOW.
    """
    now_utc = _parse_datetime(now) or datetime.now(timezone.utc)
    metadata_map = dict(metadata or {}) if isinstance(metadata, Mapping) else {}
    configured_mode, config_reason = resolve_entry_efficiency_mode_with_reason(mode)
    canonical_pattern, applies = normalize_strategy_pattern(pattern, timeframe)
    # Authority is deliberately limited to the explicitly targeted policy;
    # unrelated setups remain telemetry-only even when PAPER rollout is on.
    authoritative = _authoritative_for_mode(configured_mode, execution_mode) and applies
    pattern_raw = str(pattern or "").strip()
    side_key = str(side or "").strip().upper()
    first_breach = _parse_datetime(first_breach_at)
    first_breach_text = first_breach.isoformat() if first_breach else None
    minutes_since_open, same_session = _rth_minutes(now_utc)
    opening_window = bool(same_session and minutes_since_open is not None and 0 <= minutes_since_open < 30)
    breach_was_opening = _opening_breach(first_breach)
    prior = str(prior_state or "").strip().upper()
    untrusted_intelligence_keys = tuple(
        sorted(key for key in metadata_map if key in _UNTRUSTED_INTELLIGENCE_KEYS)
    )

    try:
        recheck_seconds = max(1, int(float(os.getenv("ENTRY_EFFICIENCY_RECHECK_SECONDS", "30"))))
    except (TypeError, ValueError):
        recheck_seconds = 30
    try:
        max_wait_seconds = max(recheck_seconds, int(float(os.getenv("ENTRY_EFFICIENCY_MAX_WAIT_SECONDS", "1800"))))
    except (TypeError, ValueError):
        max_wait_seconds = 1800

    durable_deadline = _parse_datetime(prior_deadline_at)
    deadline = durable_deadline
    if deadline is None and first_breach is not None:
        deadline = first_breach + timedelta(seconds=max_wait_seconds)
    elif deadline is None:
        # A confirmed watcher breach should normally carry the durable first
        # breach timestamp.  If that evidence is unavailable, fail closed but
        # still keep the continuation bounded from this evaluation rather than
        # creating an unbounded in-memory wait.
        deadline = now_utc + timedelta(seconds=max_wait_seconds)
    next_eval = now_utc + timedelta(seconds=recheck_seconds)
    if deadline is not None and next_eval > deadline:
        next_eval = deadline

    evidence: dict[str, Any] = {
        "ticker": str(ticker or "").upper().strip(),
        "side": side_key,
        "trigger_price": _safe_float(trigger_price),
        "bid": _safe_float(bid),
        "ask": _safe_float(ask),
        "quote_age_ms": quote_age_ms,
        "untrusted_intelligence_keys": untrusted_intelligence_keys,
        "intelligence_metadata_authority": "IGNORED",
        "timing_authority_basis": "DIRECT_MARKET_EVIDENCE",
        "rollout_config_reason": config_reason,
    }

    def result(decision: str, reason: str, detail: str) -> EntryEfficiencyResult:
        return EntryEfficiencyResult(
            decision=decision,
            mode=configured_mode,
            authoritative=authoritative,
            reason_code=reason,
            detail=detail,
            pattern_raw=pattern_raw,
            canonical_pattern=canonical_pattern,
            opening_window=opening_window,
            breach_was_opening=breach_was_opening,
            minutes_since_rth_open=minutes_since_open,
            first_breach_at=first_breach_text,
            rearm_pending=(bool(rearm_pending) if decision != READY_NOW else False),
            next_evaluation_at=next_eval.isoformat(),
            deadline_at=deadline.isoformat() if deadline else None,
            evidence=evidence,
        )

    if prior in {TERMINAL_INVALID, "EXPIRED"}:
        return result(
            TERMINAL_INVALID,
            "ENTRY_EFFICIENCY_TERMINAL_STATE",
            "durable terminal efficiency state cannot be reopened",
        )

    # Non-target families are intentionally observational.  The PR must not
    # change existing entry behavior for unrelated strategy patterns.
    if not applies:
        return result(READY_NOW, "PATTERN_NOT_IN_SCOPE", "entry-efficiency family not applicable")

    if not authoritative:
        if configured_mode != ENTRY_EFFICIENCY_OBSERVE_ONLY and not _authoritative_for_mode(
            configured_mode, execution_mode
        ):
            return result(READY_NOW, "MODE_EXECUTION_MISMATCH_OBSERVE_ONLY", "configured mode does not match order mode")
        return result(
            WAIT_CONFIRMATION if opening_window or breach_was_opening else READY_NOW,
            "OBSERVE_ONLY",
            "classification recorded without changing submit behavior",
        )

    trigger = _safe_float(trigger_price)
    bid_value = _safe_float(bid)
    ask_value = _safe_float(ask)
    stop = _safe_float(stop_price)
    target = _safe_float(target_price)
    if side_key not in {"CALL", "PUT"} or trigger is None or trigger <= 0:
        return result(TERMINAL_INVALID, "ENTRY_EFFICIENCY_INVALID_TRIGGER", "side or trigger truth is malformed")
    if stop is None or target is None:
        return result(
            WAIT_CONFIRMATION,
            "ENTRY_EFFICIENCY_SETUP_TRUTH_UNAVAILABLE",
            "stop and target truth are required before an authoritative release",
        )

    # A fresh executable quote is mandatory in authoritative mode.  Age is
    # supplied by the existing quote path; missing age is not proof of truth.
    try:
        quote_age = int(quote_age_ms) if quote_age_ms is not None and not isinstance(quote_age_ms, bool) else None
    except (TypeError, ValueError, OverflowError):
        quote_age = None
    try:
        max_quote_age = max(1, int(float(os.getenv("ENTRY_EFFICIENCY_MAX_QUOTE_AGE_MS", "30000"))))
    except (TypeError, ValueError):
        max_quote_age = 30000
    if (
        bid_value is None
        or ask_value is None
        or bid_value <= 0
        or ask_value <= 0
        or quote_age is None
        or quote_age < 0
        or quote_age > max_quote_age
    ):
        return result(WAIT_CONFIRMATION, "ENTRY_EFFICIENCY_QUOTE_UNAVAILABLE", "fresh bid/ask truth is required")

    if side_key == "CALL":
        relation_holds = ask_value >= trigger
        stop_broken = stop is not None and bid_value <= stop
        target_complete = target is not None and ask_value >= target
    else:
        relation_holds = bid_value <= trigger
        stop_broken = stop is not None and ask_value >= stop
        target_complete = target is not None and bid_value <= target
    evidence.update({
        "relation_holds": relation_holds,
        "stop_broken": stop_broken,
        "target_complete": target_complete,
    })

    if stop_broken:
        return result(TERMINAL_INVALID, "ENTRY_EFFICIENCY_STOP_INVALIDATED", "stop-side truth is already broken")
    if target_complete:
        return result(TERMINAL_INVALID, "ENTRY_EFFICIENCY_TARGET_COMPLETE", "target-side truth leaves no entry opportunity")
    if deadline is not None and now_utc >= deadline:
        return result(TERMINAL_INVALID, "ENTRY_EFFICIENCY_WAIT_DEADLINE_EXPIRED", "bounded recheck window expired")
    if first_breach is None:
        return result(
            WAIT_CONFIRMATION,
            "ENTRY_EFFICIENCY_FIRST_BREACH_UNAVAILABLE",
            "durable first-breach evidence is required before release",
        )
    if not relation_holds:
        return result(REARM_FOR_REBREACH, "ENTRY_EFFICIENCY_REARM_REQUIRED", "price reclaimed the pre-breach side")
    if prior == REARM_FOR_REBREACH and not rearm_pending:
        return result(
            WAIT_CONFIRMATION,
            "ENTRY_EFFICIENCY_REARM_STATE_UNPROVEN",
            "re-arm state is not durably paired with a fresh re-breach",
        )
    if prior == READY_NOW:
        # A crash/restart after the durable READY promotion must resume the
        # existing pre-submit handoff rather than downgrade the state merely
        # because the original breach occurred during the opening window.
        # Stop/target/relation/deadline truth above still revalidates the
        # opportunity before selector and final submit gates run.
        return result(READY_NOW, "ENTRY_EFFICIENCY_READY_RETRY", "durable READY handoff is being resumed")

    # The first opening-window breach is always held.  The later clock tick is
    # not a release signal; only a genuine reset/re-breach recorded by the
    # durable watcher may release it.  Generic intelligence metadata is never
    # a timing authority.
    if opening_window or breach_was_opening:
        if rearm_pending:
            return result(READY_NOW, "ENTRY_EFFICIENCY_GENUINE_REBREACH", "fresh reset/re-breach is proven")
        return result(WAIT_CONFIRMATION, "ENTRY_EFFICIENCY_OPENING_BREACH", "first 30-minute breach requires continuation or reset/re-breach")

    if prior in {WAIT_CONFIRMATION, REARM_FOR_REBREACH} and not rearm_pending:
        return result(WAIT_CONFIRMATION, "ENTRY_EFFICIENCY_CLOCK_NOT_SUFFICIENT", "clock alone cannot release the prior breach")
    return result(READY_NOW, "ENTRY_EFFICIENCY_READY", "fresh post-window entry truth is proven")


__all__ = [
    "EntryEfficiencyResult",
    "ENTRY_EFFICIENCY_MODES",
    "ENTRY_EFFICIENCY_IDENTITY_FIELDS",
    "ENTRY_EFFICIENCY_OBSERVE_ONLY",
    "ENTRY_EFFICIENCY_PAPER_AUTHORITATIVE",
    "READY_NOW",
    "REARM_FOR_REBREACH",
    "TERMINAL_INVALID",
    "WAIT_CONFIRMATION",
    "evaluate_entry_efficiency",
    "entry_efficiency_identity_is_proven",
    "normalize_strategy_pattern",
    "resolve_entry_efficiency_mode",
]
