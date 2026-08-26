# ap_entry_watcher.py — Angel Precision Real-Time Entry Watcher
# =============================================================================
# OSM / queue / execution-core compatible watcher.
#
# Responsibilities:
#   1) Hold pending plan-based entry signals until the underlying breaches trigger.
#   2) Confirm breach momentum over multiple polls before firing on_trigger(watched).
#   3) Hold post-session and pre-market setups overnight, then revalidate at open.
#   4) Protect OSM from duplicate, conflicting, stale, or chaotic open-trigger events.
#
# Non-responsibilities:
#   - This file does NOT submit broker orders.
#   - This file does NOT transition OSM order states.
#   - ExecutionCore must wire on_trigger -> _on_entry_trigger -> OSM.submit_existing_entry(...).
#
# Production notes:
#   - Based on the newer OSM-integrated watcher, with production-safe pieces kept
#     from the older simpler watcher: debug visibility, explicit watcher ref for
#     dedup cleanup, clearer comments, and safer fallback behavior.
#   - Daily overnight signals use ap.overnight_daily_validator when available.
#   - If the validator import is unavailable, the watcher still imports/runs, but
#     daily-specific validation is disabled and a warning is emitted.
# =============================================================================

from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
import types
import uuid
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional
from zoneinfo import ZoneInfo

from ap_canonical_signal import build_canonical_signal_id

# ── Lifecycle + health wiring (defensive — watcher runs standalone if missing) ──
try:
    from ap_lifecycle import (
        LEDGER as _EW_LEDGER,
        SignalState as _EW_SS,
        LifecycleOwner as _EW_LO,
        signal_watching,
        signal_invalidated,
        signal_cancelled,
        signal_expired,
        signal_triggered,
    )
    _EW_LIFECYCLE_OK = True
except Exception:
    _EW_LIFECYCLE_OK = False

def _ew_record(signal_id: str, ticker: str, to_state_name: str, reason: str, **meta) -> None:
    """Safe lifecycle wrapper — never raises, never blocks watcher logic."""
    if not _EW_LIFECYCLE_OK:
        return
    try:
        fn_map = {
            "WATCHING":     signal_watching,
            "INVALIDATED":  signal_invalidated,
            "CANCELLED":    signal_cancelled,
            "EXPIRED":      signal_expired,
            "TRIGGER_READY": signal_triggered,
        }
        fn = fn_map.get(to_state_name)
        if fn:
            fn(signal_id, ticker, _EW_LO.WATCHER, reason, **meta)
    except Exception:
        pass

log = logging.getLogger("ap.entry_watcher")


def _valid_positive_finite_quote(value) -> Optional[float]:
    """Return a usable canonical quote, or ``None`` when unavailable.

    Trigger evidence is stricter than display data: booleans, malformed,
    non-finite, zero, and negative values are all unavailable and must never
    become breach evidence.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or parsed <= 0:
        return None
    return parsed


def _is_crossed_quote_pair(bid: Optional[float], ask: Optional[float]) -> bool:
    """Return whether both sides are present but internally inconsistent."""
    return bid is not None and ask is not None and bid > ask

# Module-level ET zoneinfo: declared BEFORE any helper that uses it.
ET = ZoneInfo("America/New_York")

# Stable recovery refusal reason.  Recovery callers use this exact marker to
# leave the durable order untouched when confirmed-trigger provenance cannot be
# bound to the lifecycle being restored.
RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN = (
    "RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN"
)


def _parse_trigger_crossed_at(raw) -> Optional[datetime]:
    """Parse durable first-breach evidence without treating bad data as proof.

    PR #407 tightening: naive datetimes and ISO strings without timezone
    information are rejected. Silent coercion to UTC would fabricate
    confirmed-breach evidence out of ambiguous input, which is exactly what
    the pre-breach stop-activation invariant forbids. Returns None for any
    input the caller must treat as absence-of-proof.
    """
    if raw is None:
        return None
    # datetime instances: accept iff tz-aware.
    if isinstance(raw, datetime):
        return raw if raw.tzinfo is not None else None
    # Anything else must be a non-empty ISO-8601 string with tz info.
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        value = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None
    if value.tzinfo is None:
        return None
    return value


def _coerce_materialization_generation(raw) -> Optional[int]:
    try:
        if raw is None or raw == "":
            return None
        return int(raw)
    except (TypeError, ValueError):
        return None


def _build_trigger_crossed_at_provenance(
    signal: dict,
    local_order_id: Optional[str],
) -> dict:
    """Build the durable identity for a newly confirmed trigger timestamp.

    ``materialization_generation`` is deliberately not part of this contract.
    The ordinary queue path owns no durable generation; deferred materialization
    owns one for its retry CAS separately.  Mixing the two contracts would make
    ordinary queue-created watchers either fabricate a generation or fail
    recovery for a lifecycle that never had one.
    """
    metadata = signal.get("metadata") or {}
    if not isinstance(metadata, dict):
        metadata = {}
    raw_signal_id = str(
        signal.get("signal_id") or metadata.get("signal_id") or ""
    ).strip()
    canonical_signal_id = str(
        signal.get("canonical_signal_id")
        or metadata.get("canonical_signal_id")
        or ""
    ).strip()
    if not canonical_signal_id:
        # Ordinary queue and deferred-rescue plans do not carry a separate
        # canonical field.  Use the same authority as OSM instead of storing
        # a REEVAL:<uuid>:<suffix> as durable lifecycle evidence.
        canonical_signal_id = build_canonical_signal_id(raw_signal_id)
    return {
        "canonical_signal_id": canonical_signal_id,
        "client_id": str(
            signal.get("client_id")
            or signal.get("client_email")
            or metadata.get("client_id")
            or metadata.get("client_email")
            or ""
        ).strip().lower(),
        "execution_mode": str(
            signal.get("execution_mode") or metadata.get("execution_mode") or ""
        ).strip().lower(),
        "local_order_id": str(
            local_order_id or signal.get("local_order_id") or ""
        ).strip(),
    }


def _trigger_crossed_at_provenance_matches(
    provenance,
    signal: dict,
    local_order_id: Optional[str],
) -> bool:
    """Return True only for complete, exact lifecycle provenance."""
    if not isinstance(provenance, dict):
        return False
    expected = _build_trigger_crossed_at_provenance(signal, local_order_id)
    actual = {
        "canonical_signal_id": str(provenance.get("canonical_signal_id") or "").strip(),
        "client_id": str(provenance.get("client_id") or "").strip().lower(),
        "execution_mode": str(provenance.get("execution_mode") or "").strip().lower(),
        "local_order_id": str(provenance.get("local_order_id") or "").strip(),
    }
    if (
        not expected["canonical_signal_id"]
        or not expected["client_id"]
        or not expected["execution_mode"]
        or not expected["local_order_id"]
    ):
        return False
    if (
        not actual["canonical_signal_id"]
        or not actual["client_id"]
        or not actual["execution_mode"]
        or not actual["local_order_id"]
    ):
        return False
    return actual == expected


def recovery_trigger_evidence_identity_is_proven(
    signal_or_row,
    local_order_id: Optional[str] = None,
) -> bool:
    """Return whether durable confirmed-trigger evidence is safe to reuse.

    A lifecycle with no durable ``trigger_crossed_at`` has no confirmed
    evidence to validate and remains eligible for an ordinary pre-breach
    rearm.  Once a timestamp is present, the timestamp itself must be valid
    and its four-field identity must match exactly.  This helper is shared by
    recovery callers so they can refuse before quote, selector, watcher, or
    order-side effects occur.
    """
    def _coerce_metadata(raw_metadata):
        if raw_metadata is None:
            return {}, True
        if isinstance(raw_metadata, dict):
            return dict(raw_metadata), True
        if isinstance(raw_metadata, str):
            if not raw_metadata.strip():
                return {}, True
            try:
                parsed = json.loads(raw_metadata)
            except Exception:
                return {}, False
            return (dict(parsed), True) if isinstance(parsed, dict) else ({}, False)
        return {}, False

    def _metadata_source(primary, fallback):
        if primary is None:
            return fallback
        if isinstance(primary, str) and not primary.strip():
            return fallback
        if isinstance(primary, (dict, list, tuple, set)) and not primary:
            return fallback
        return primary

    if isinstance(signal_or_row, dict):
        signal = dict(signal_or_row)
        raw_metadata = _metadata_source(
            signal.get("metadata"), signal.get("meta")
        )
        metadata, metadata_is_valid = _coerce_metadata(raw_metadata)
        if not metadata_is_valid:
            log.critical(
                "%s | malformed recovery metadata; refusing rearm",
                RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN,
            )
            return False
        signal["metadata"] = metadata
        # Persisted orders commonly keep canonical identity in JSONB metadata
        # while client/mode remain columns. Hydrate the same production
        # identity shape used by plan objects before comparing provenance.
        signal["canonical_signal_id"] = (
            signal.get("canonical_signal_id")
            or metadata.get("canonical_signal_id")
        )
        signal["client_id"] = (
            signal.get("client_id")
            or metadata.get("client_id")
            or metadata.get("client_email")
        )
        signal["execution_mode"] = (
            signal.get("execution_mode")
            or metadata.get("execution_mode")
        )
        raw_crossed_at = signal.get("trigger_crossed_at")
        if raw_crossed_at is None:
            raw_crossed_at = metadata.get("trigger_crossed_at")
        provenance = metadata.get("trigger_crossed_at_provenance")
        resolved_local_order_id = (
            local_order_id
            or signal.get("local_order_id")
        )
    else:
        raw_metadata = _metadata_source(
            getattr(signal_or_row, "metadata", None),
            getattr(signal_or_row, "meta", None),
        )
        metadata, metadata_is_valid = _coerce_metadata(raw_metadata)
        if not metadata_is_valid:
            log.critical(
                "%s | malformed recovery metadata; refusing rearm",
                RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN,
            )
            return False
        signal = {
            "signal_id": getattr(signal_or_row, "signal_id", ""),
            "canonical_signal_id": (
                getattr(signal_or_row, "canonical_signal_id", "")
                or metadata.get("canonical_signal_id")
            ),
            "client_id": (
                getattr(signal_or_row, "client_id", "")
                or metadata.get("client_id")
                or metadata.get("client_email")
            ),
            "execution_mode": (
                getattr(signal_or_row, "execution_mode", "")
                or metadata.get("execution_mode")
            ),
            "local_order_id": getattr(signal_or_row, "local_order_id", ""),
            "metadata": metadata,
        }
        raw_crossed_at = getattr(signal_or_row, "trigger_crossed_at", None)
        if raw_crossed_at is None:
            raw_crossed_at = metadata.get("trigger_crossed_at")
        provenance = metadata.get("trigger_crossed_at_provenance")
        resolved_local_order_id = local_order_id or signal.get("local_order_id")

    if raw_crossed_at is None:
        return True
    if _parse_trigger_crossed_at(raw_crossed_at) is None:
        return False
    return _trigger_crossed_at_provenance_matches(
        provenance, signal, resolved_local_order_id
    )

# FUNNEL FIX (2026-05-20, hardened 2026-05-21) + PR-C / BUG-EW-1:
# Pre-open helper for the stop-touch invalidation guard. Returns True from
# midnight ET through 9:30 ET (pre-market) AND through the 5-min open-protect
# window (9:30–9:35 ET) — spreads remain wide during the open protect window,
# transient bid-below-stop ticks should not invalidate overnight/daily setups.
#
# PR-C / BUG-EW-1: removed lazy `from datetime import` and
# `from zoneinfo import` calls inside the function body. They added a
# sys.modules lookup on every poll tick (this helper is called for
# every overnight/daily signal on every 15s cycle) AND constructed a
# new ZoneInfo("America/New_York") object every call, shadowing the
# module-level ET constant. Now we use the module-level ET directly.
def _is_pre_market_now() -> bool:
    try:
        et = datetime.now(ET)
        # Pre-9:30 → pre-market
        if et.hour < 9 or (et.hour == 9 and et.minute < 30):
            return True
        # 9:30–9:35 → open-protect window: still wide spreads, treat as pre-open
        if et.hour == 9 and 30 <= et.minute < 35:
            return True
        return False
    except Exception:
        return False

POLL_INTERVAL_SEC = 15
MAX_WATCH_MINUTES = 4320  # 72 hours — covers weekend holds

# PR-C / Section 1 documentation note:
# EOD_CUTOFF (15:30 ET) intentionally fires BEFORE the exit engine's
# EOD_HARD_CLOSE (15:50 ET) in ap_exit_engine.py. This 20-minute gap
# ensures no new entries are opened while the exit engine is in its
# final forced-close window. Do not align these times — the gap is a
# deliberate safety margin.
EOD_CUTOFF_HOUR = 15
EOD_CUTOFF_MIN = 30
WRONG_DIR_BUFFER_PCT = 0.001
OVERNIGHT_THRESHOLD_HOUR = 15
OVERNIGHT_THRESHOLD_MIN = 30

MAX_INTRADAY_WATCH_MIN = int(os.getenv("MAX_INTRADAY_WATCH_MIN", "45"))
# Default 45 min: watcher stays armed for 45 minutes after breach.
# Was hardcoded 5 — too short for slow-moving setups. Env-tunable.

# PR-C: MAX_INTRADAY_DRIFT_PCT is now env-tunable for consistency with
# every other arm/watch threshold. Default 0.015 (1.5%) preserved.
MAX_INTRADAY_DRIFT_PCT = float(os.getenv("MAX_INTRADAY_DRIFT_PCT", "0.015"))

# PR-C: MAX_OPTION_PREMIUM_DRIFT_PCT elevated from inline os.getenv()
# in watch() to a module-level constant. Default 0.25 (25%) preserved.
# If the option bid is already this far above the signal's reference
# entry_option_price at arm time, the move has likely happened without us
# and entering would be catching the reversal. Module-level so the dep
# is visible to static analysis and the read isn't repeated per arm.
MAX_OPTION_PREMIUM_DRIFT_PCT = float(os.getenv("MAX_OPTION_PREMIUM_DRIFT_PCT", "0.25"))

# ============================================================
# FUNNEL FIX (2026-05-20, hardened 2026-05-21) — dedicated arm-time tolerance.
#
# Production observed 12/29 orders today rejected with
# 'watch_arm_failed:stale_price_-0.X%_from_trigger' at deltas of 0.0–0.4%.
# Two root causes possible:
#   (a) MAX_INTRADAY_DRIFT_PCT is hard-coded 1.5% — NOT env tunable.
#   (b) The 'stop guard' below conflates 'price below stop' with stale, and the
#       reject reason string only captures pct_from_trigger — making it look
#       like a drift reject even when it's actually a too-close stop reject.
#
# Hardened logic:
#   - WATCH_ARM_STALE_TOLERANCE_PCT (default 0.010 = 1.0%) is env-tunable.
#   - Env actually controls the threshold (replaces the previous max() floor
#     which prevented the env from ever loosening below 1.5%).
#   - Safety bounds: minimum 0.001 (0.1%), maximum 0.05 (5%) — protects
#     against fat-finger env values that would either kill all signals (too
#     tight) or admit garbage (too loose).
#   - Reject reason now distinguishes 'arm_drift_X%' from 'arm_below_stop'
#     so we can tell which gate fired in post-mortem analysis.
# ============================================================
try:
    _raw_tol = float(os.getenv("WATCH_ARM_STALE_TOLERANCE_PCT", "0.010"))
    # Safety bounds — env can loosen but cannot become absurd.
    WATCH_ARM_STALE_TOLERANCE_PCT = max(0.001, min(0.05, _raw_tol))
    if _raw_tol != WATCH_ARM_STALE_TOLERANCE_PCT:
        # Will be visible at startup so operator knows env was clamped.
        _clamp_warn = True
    else:
        _clamp_warn = False
except (TypeError, ValueError):
    WATCH_ARM_STALE_TOLERANCE_PCT = 0.010
    _clamp_warn = False

# Effective threshold used at arm-time. The env value IS the threshold.
# (Previously this was max(env, MAX_INTRADAY_DRIFT_PCT) which prevented env
# from loosening below 1.5% — defeating the purpose of the env knob.)
WATCH_ARM_EFFECTIVE_THRESHOLD_PCT = WATCH_ARM_STALE_TOLERANCE_PCT

if _clamp_warn:
    log.warning(
        "[entry-watcher] WATCH_ARM_STALE_TOLERANCE_PCT was clamped to safety "
        "bounds [0.001, 0.05]; effective=%.4f",
        WATCH_ARM_EFFECTIVE_THRESHOLD_PCT,
    )
log.info(
    "[entry-watcher] thresholds loaded: "
    "MAX_INTRADAY_DRIFT_PCT=%.4f WATCH_ARM_STALE_TOLERANCE_PCT=%.4f effective=%.4f",
    MAX_INTRADAY_DRIFT_PCT,
    WATCH_ARM_STALE_TOLERANCE_PCT,
    WATCH_ARM_EFFECTIVE_THRESHOLD_PCT,
)
OVERNIGHT_MAX_DRIFT_PCT = 0.020  # generic/non-daily overnight drift guard

OPEN_PROTECT_MINUTES = 5
MAX_OPEN_TRIGGERS = 1

# ── P0-W2: Strong-signal re-arm after temporary wrong-side-of-stop ───────────
#
# When arm is rejected because price is temporarily on the wrong side of stop
# (below_stop gate, NOT drift_stale), high-conviction signals enter a
# DISARMED_WAITING_FOR_RECLAIM hold instead of being permanently killed.
# If price reclaims the valid side within WATCHER_REARM_WINDOW_SEC, the signal
# arms normally with the same signal_id/plan_id. If it does not, it expires.
#
# Only applies to: score >= WATCHER_REARM_MIN_SCORE OR tier A, AND
# (if WATCHER_REARM_ONLY_DAILY_OR_OVERNIGHT) the signal must be daily/overnight.
# Drift-stale rejects are always permanent — those represent a decisive miss.
#
# WATCHER_REARM_TOLERANCE_PCT: the underlying must clear past the stop level
# by this fraction before re-arming. Prevents oscillation at the stop line.
# Default 0.001 = 0.1% of stop price. For NVDA stop=212.71, reclaim fires when
# mid <= 212.71 * 0.999 = 212.497 (PUT) or mid >= stop * 1.001 (CALL).
#
# WATCHER_REARM_MAX_ATTEMPTS: after this many rearm cycles (disarm → reclaim),
# the next disarm is permanent. Default 1 — allows one recovery per signal.
#
# Overnight rearm expiry is market-open-aware: if the signal enters rearm mode
# pre-market, the window starts at 9:30 ET open, not at arm time, so the signal
# is not silently expired before quotes are even available.
# ─────────────────────────────────────────────────────────────────────────────
WATCHER_REARM_ENABLED: bool = (
    os.getenv("WATCHER_REARM_ENABLED", "0").strip().lower() not in {"0", "false", "no"}
)
WATCHER_REARM_MIN_SCORE: float = float(os.getenv("WATCHER_REARM_MIN_SCORE", "75"))
_raw_rearm_window = int(os.getenv("WATCHER_REARM_WINDOW_SEC", "600"))
WATCHER_REARM_WINDOW_SEC: int = max(60, min(3600, _raw_rearm_window))  # clamp 1 min – 60 min
WATCHER_REARM_ONLY_DAILY_OR_OVERNIGHT: bool = (
    os.getenv("WATCHER_REARM_ONLY_DAILY_OR_OVERNIGHT", "1").strip().lower() not in {"0", "false", "no"}
)
_raw_rearm_tol = float(os.getenv("WATCHER_REARM_TOLERANCE_PCT", "0.001"))
WATCHER_REARM_TOLERANCE_PCT: float = max(0.0, min(0.02, _raw_rearm_tol))  # clamp 0 – 2%
WATCHER_REARM_MAX_ATTEMPTS: int = max(1, int(os.getenv("WATCHER_REARM_MAX_ATTEMPTS", "1")))

# Opposite-side conflict smart-eligibility (P0 PR):
# Friday's audit: 419/871 (~48%) of canceled entries were
# watcher_block:opposite_side_conflict. Many were against stale or already-
# canceled opposite watchers. New rule set below; env-overridable.
#
# Decision matrix:
#   1. If opposite watcher is STALE (older than OPPOSITE_CONFLICT_MAX_AGE_SEC)
#      AND has lower-or-equal score, ignore it. New signal admits.
#   2. If opposite watcher score > new score AND fresh, BLOCK new (current
#      behavior preserved — protect the better setup).
#   3. If new score > opposite score, FLIP (cancel opposite, admit new —
#      current behavior preserved).
#   4. Equal scores within OPPOSITE_CONFLICT_SCORE_TIE_PCT of each other,
#      prefer the higher-tier timeframe (1d/4h beats 1h/15m). Daily beats
#      intraday on tie.
#   5. New signal can override if it is non-rearm/active + opposite is rearm-only
#      (rearm signals are weaker by design).
OPPOSITE_CONFLICT_MAX_AGE_SEC      = int(os.getenv("OPPOSITE_CONFLICT_MAX_AGE_SEC", "600"))   # 10 min default
OPPOSITE_CONFLICT_SCORE_TIE_PCT    = float(os.getenv("OPPOSITE_CONFLICT_SCORE_TIE_PCT", "0.03"))  # within 3pts = tie

# Timeframe tier preference (higher wins on ties). Daily/4h/overnight
# structurally stronger than intraday for failed-directional setups.
_OPPOSITE_TF_TIER = {
    "1d": 5, "d": 5, "daily": 5,
    "4h": 4, "240m": 4, "4hour": 4,
    "1h": 3, "60m": 3, "hourly": 3,
    "30m": 2,
    "15m": 1, "5m": 1, "1m": 1,
}

def _opposite_tf_rank(tf: str) -> int:
    return _OPPOSITE_TF_TIER.get(str(tf or "").lower().strip(), 0)

log.info(
    "[entry-watcher] rearm config: enabled=%s min_score=%.0f window=%ds "
    "daily_only=%s tolerance=%.4f max_attempts=%d",
    WATCHER_REARM_ENABLED,
    WATCHER_REARM_MIN_SCORE,
    WATCHER_REARM_WINDOW_SEC,
    WATCHER_REARM_ONLY_DAILY_OR_OVERNIGHT,
    WATCHER_REARM_TOLERANCE_PCT,
    WATCHER_REARM_MAX_ATTEMPTS,
)


# ── Optional daily overnight validator integration ───────────────────────────
try:
    from ap.overnight_daily_validator import (  # type: ignore
        OvernightWatchState,
        _is_daily_signal as _validator_is_daily_signal,
        recheck_overnight_daily as _validator_recheck_overnight_daily,
    )
    _DAILY_VALIDATOR_AVAILABLE = True
except Exception as _import_err:  # pragma: no cover - defensive import fallback
    _DAILY_VALIDATOR_AVAILABLE = False
    log.warning(
        "ap.overnight_daily_validator unavailable; daily overnight signals will "
        "use generic overnight validation only: %s",
        _import_err,
    )

    class OvernightWatchState:  # type: ignore[no-redef]
        OVERNIGHT_QUEUED = "OVERNIGHT_QUEUED"
        OPEN_RECHECK_PENDING = "OPEN_RECHECK_PENDING"
        VALID_AWAITING_BREACH = "VALID_AWAITING_BREACH"
        INVALIDATED = "INVALIDATED"

    def _validator_is_daily_signal(_obj) -> bool:
        return False

    def _validator_recheck_overnight_daily(_obj, _broker):
        class _Result:
            valid = True
            reason_code = "VALIDATOR_UNAVAILABLE"
            reason_text = "Daily validator unavailable; generic watcher fallback used."

        return _Result()


def _safe_is_daily_signal(obj) -> bool:
    """Safely identify daily signals whether validator expects object or dict."""
    try:
        return bool(_validator_is_daily_signal(obj))
    except Exception:
        try:
            return bool(_validator_is_daily_signal(getattr(obj, "signal", obj)))
        except Exception as exc:
            log.debug("daily-signal classification failed; treating as non-daily: %s", exc)
            return False


class WatchState:
    PENDING = "PENDING"
    TRIGGERED = "TRIGGERED"
    EXPIRED = "EXPIRED"
    INVALIDATED = "INVALIDATED"
    CANCELLED = "CANCELLED"


# Fixed safety invariant: pre-confirmation breach continuity may survive
# missing canonical observations only for this bounded interval, measured from
# the most recent valid breach observation. This is intentionally not an
# environment-tunable frequency setting.
WATCHER_BREACH_CONTINUITY_MAX_GAP_SEC = 45


class WatchedSignal:
    MOMENTUM_POLLS_REQUIRED = 2

    def __init__(self, signal: dict, overnight: bool = False):
        self.signal = signal
        self.ticker = str(signal.get("ticker") or "").upper().strip()
        self.side = str(signal.get("side") or "CALL").upper().strip()
        self.overnight = bool(overnight)

        if not self.ticker:
            raise ValueError("WatchedSignal requires ticker")
        if self.side not in {"CALL", "PUT"}:
            raise ValueError(f"[{self.ticker}] invalid side={self.side!r}; expected CALL or PUT")

        # Normalize payload so downstream execution core sees clean fields.
        self.signal["ticker"] = self.ticker
        self.signal["side"] = self.side

        trigger = signal.get("trigger") or {}
        entry = signal.get("entry_price") or trigger.get("entry")
        stop = signal.get("stop_price") or trigger.get("stop")
        target = signal.get("target_price") or trigger.get("pt1") or trigger.get("pt2")

        # PR-C / BUG-EW-2: explicit None-or-empty check on entry.
        # Previously \`float(entry or 0) or None\` silently coerced a literal
        # 0.0 to None, leaving the subsequent guard to raise ValueError
        # anyway. The pattern was confusing and conflated "missing" with
        # "zero-valued" — raise loudly for either case.
        def _coerce_or_none(v):
            if v is None or v == "" or v == 0 or v == 0.0:
                return None
            try:
                f = float(v)
            except (TypeError, ValueError):
                return None
            return f if f != 0.0 else None

        self.entry_trigger = _coerce_or_none(entry)
        self.stop_level    = _coerce_or_none(stop)
        self.target_price  = _coerce_or_none(target)

        if not self.entry_trigger:
            raise ValueError(
                f"[{self.ticker}] entry_price/trigger is None or zero — "
                f"signal payload incomplete: entry={entry} stop={stop} target={target}"
            )

        self.score = float(signal.get("score") or 0)
        self.grade = signal.get("grade", "B")
        self.signal_id = str(signal.get("signal_id") or uuid.uuid4())
        self.signal["signal_id"] = self.signal_id

        # PR #421 watcher rollback fix: a per-instance identity token,
        # unique for the life of the process. Never reused, unlike
        # id(self) — CPython immediately reuses a garbage-collected
        # object's memory address for the next allocation, so id() alone
        # cannot safely distinguish "the exact registration a recovery
        # actor created" from "a different registration that happens to
        # have been allocated at the same freed address after the first
        # one was evicted and dereferenced." Recovery rollback fencing
        # (ap_recovery.py _capture_just_registered_watcher_id /
        # _evict_just_registered_watcher) keys off this token, not id().
        self._registration_token = uuid.uuid4().hex

        self.state = WatchState.PENDING
        self.created_at = datetime.now(timezone.utc)
        self.triggered_at: Optional[datetime] = None
        # PR #305: FIRST breach moment (distinct from triggered_at which is
        # when the confirmed poll fires after MOMENTUM_POLLS_REQUIRED breaches).
        # Used by ap.live_submit_gates.check_trigger_age_gate to enforce
        # ENTRY_TRIGGER_MAX_AGE_SEC (default 120s).
        _trigger_crossed_raw = signal.get("trigger_crossed_at")
        if _trigger_crossed_raw is None:
            _signal_meta = signal.get("metadata") or {}
            if isinstance(_signal_meta, dict):
                _trigger_crossed_raw = _signal_meta.get("trigger_crossed_at")
        # Existing order metadata is the durable lifecycle evidence used to
        # keep the scanner stop active after a restart/reattachment.  Invalid
        # values are not proof and therefore leave the stop dormant until a
        # fresh canonical breach is observed.
        self.trigger_crossed_at: Optional[datetime] = _parse_trigger_crossed_at(
            _trigger_crossed_raw
        )
        # PR #407: pending (unconfirmed) first-breach timestamp. Populated on
        # the first breach poll of a streak, promoted into trigger_crossed_at
        # only after MOMENTUM_POLLS_REQUIRED breaches confirm. Never persisted.
        self._pending_first_breach_at: Optional[datetime] = None
        self.first_breach_bid: float = 0.0
        self.first_breach_ask: float = 0.0
        self.trigger_price: Optional[float] = None
        self.expire_at = self.created_at + timedelta(minutes=MAX_WATCH_MINUTES)

        self.breach_count = 0
        self.breach_price = 0.0
        # Process-local and intentionally non-durable. A restart must not
        # fabricate continuity for an unconfirmed trigger lifecycle.
        self._last_valid_breach_observation_at: Optional[datetime] = None
        self.last_quote_bid = 0.0
        self.last_quote_ask = 0.0
        self.last_quote_bid_raw = None
        self.last_quote_ask_raw = None
        self.last_trigger_evidence_reason: Optional[str] = None
        self.last_quote_age_ms: Optional[int] = None
        self._watcher_ref = None
        self._pending_audit: Optional[dict] = None  # audit payload staged inside check(), persisted by poll loop

        # P0-W2: re-arm state — set by add_signal() when watch() marks the signal rearm-eligible.
        self.rearm_mode: bool = False                   # True while waiting for price reclaim
        self.rearm_expires_at: Optional[datetime] = None  # deadline for reclaim (None = no window set)
        self.rearm_reason: str = ""                     # original arm_below_stop raw_reason
        self.rearm_count: int = 0                       # how many disarm→reclaim cycles completed

        # PR #324 — ownership quarantine state (FAILED completion result).
        # When a cleanup callback fails or cannot be verified, the watcher
        # enters quarantine: stays in _pending, dedup held, cannot trigger,
        # retries cleanup on a bounded schedule.
        self._ownership_quarantine: bool = False
        self._quarantine_reason: str = ""
        self.cleanup_retry_attempt: int = 0
        self.cleanup_retry_next_at: Optional[datetime] = None
        self.cleanup_retry_deadline: Optional[datetime] = None
        # Final amendment §5: exposed in watcher status diagnostics.
        self.quarantine_metadata_persist_failed: bool = False

        # PR #324 — overnight LIVE quote retry state (Failure A fix).
        # overnight_live_quote_unavailable becomes bounded retry, not inert INVALIDATED.
        self._overnight_quote_retry_attempt: int = 0
        self._overnight_quote_retry_first_failed_at: Optional[datetime] = None
        self._overnight_quote_retry_last_failed_at: Optional[datetime] = None
        self._overnight_quote_retry_deadline: Optional[datetime] = None
        self._overnight_quote_retry_next_at: Optional[datetime] = None  # PR #324 §7: schedule guard

        # PR #324 — trigger/stop collision on the same poll (Failure D fix).
        # Set in check() when both trigger AND stop conditions are simultaneously true.
        self._trigger_stop_collision: bool = False

        # PR #388 Block-2 — late-attachment continuation / reset state.
        # None: ordinary arm; check() runs the normal breach path.
        # LATE_ATTACHMENT_WITHIN_CONTINUATION: attached inside the continuation
        # window; requires 2 confirming polls before the normal breach path
        # is allowed to fire.
        # MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET: attached past the
        # continuation window but structurally valid; waits for 2 reset polls
        # then re-arms via the ordinary breach path (never submits itself).
        self.late_attachment_state: Optional[str] = None
        self.late_confirm_polls: int = 0
        self.late_reset_polls: int = 0
        self.late_attachment_generation: int = 0
        self.late_attachment_first_seen_at: Optional[datetime] = None
        # Diagnostics only — carried into audit payloads.
        self.late_attachment_last_quote: Optional[float] = None
        self.late_attachment_last_source: Optional[str] = None

        # Seed from the arm-time classifier (set by APEntryWatcher.watch()
        # when the arm-time canonical quote landed inside the continuation
        # zone or past it into WAITING_RESET). Consumed on first read so
        # restart-recovery cannot double-seed.
        _seed = signal.pop("_late_attachment_seed", None)
        if isinstance(_seed, dict):
            self.late_attachment_state = str(_seed.get("state") or "") or None
            self.late_attachment_generation += 1
            self.late_attachment_last_quote = _seed.get("quote")
            self.late_attachment_last_source = _seed.get("quote_source")
            try:
                self.late_attachment_first_seen_at = datetime.fromisoformat(
                    str(_seed.get("seen_at") or "").replace("Z", "+00:00")
                )
            except Exception:
                self.late_attachment_first_seen_at = datetime.now(timezone.utc)

        if self.overnight and _safe_is_daily_signal(self):
            self.signal["queue_status"] = OvernightWatchState.OVERNIGHT_QUEUED

        if self.overnight:
            log.info(
                "[%s] OVERNIGHT signal queued | %s | trigger=$%s | stop=$%s | "
                "target=$%s | activates next session open",
                self.ticker,
                self.side,
                self.entry_trigger,
                self.stop_level,
                self.target_price,
            )
        else:
            log.info(
                "[%s] Watching %s | trigger=$%s | stop=$%s | target=$%s | expires %s",
                self.ticker,
                self.side,
                self.entry_trigger,
                self.stop_level,
                self.target_price,
                self.expire_at.strftime("%H:%M UTC"),
            )

    @property
    def is_active(self) -> bool:
        # rearm_mode signals are PENDING but NOT active — they are waiting for
        # price to reclaim the valid side of stop and must not enter normal
        # breach/trigger/stale-drift poll logic until reclaimed.
        # PR #324: quarantined watchers (FAILED cleanup) must also not trigger
        # or submit; they stay in _pending for ownership but cannot fire.
        return (
            self.state == WatchState.PENDING
            and not self.rearm_mode
            and not self._ownership_quarantine
        )

    @property
    def minutes_watching(self) -> float:
        return (datetime.now(timezone.utc) - self.created_at).total_seconds() / 60.0

    def _release_dedup_key(self) -> None:
        try:
            watcher = getattr(self, "_watcher_ref", None)
            dedup_set = getattr(watcher, "_dedup_set", None) if watcher else None
            if dedup_set is not None and self.signal_id:
                dedup_set.discard(str(self.signal_id))
                log.debug("[%s] Dedup key released | signal=%s", self.ticker, self.signal_id)
        except Exception:
            pass

    def _reset_pending_breach_continuity(self) -> None:
        """Discard only stale pre-confirmation breach evidence."""
        self.breach_count = 0
        self._pending_first_breach_at = None
        self.breach_price = 0.0
        self.first_breach_bid = 0.0
        self.first_breach_ask = 0.0
        self.trigger_price = None
        self._last_valid_breach_observation_at = None

    def _get_watcher_now(self) -> datetime:
        """Narrow clock seam used by bounded-continuity comparisons."""
        return datetime.now(timezone.utc)

    def check(self, bid: float, ask: float, quote_age_ms: Optional[int] = None) -> str:
        # PR-C precedence note: when ask >= trigger AND bid <= stop on the
        # SAME poll tick, the breach check runs FIRST (may set
        # state=TRIGGERED), then the stop check runs and OVERWRITES with
        # state=INVALIDATED. This is "last-write wins" and the chosen
        # behavior is safer-by-design: a single tick where the underlying
        # is whipsawing both directions should NOT fire an entry. Do not
        # add an early-return after TRIGGERED — the current precedence is
        # intentional. See tests/test_entry_watcher_audit.py
        # TestPrecedenceTriggerVsStop for the structural guarantee.
        now = self._get_watcher_now()
        _raw_bid = bid
        _raw_ask = ask
        _bid_quote = _valid_positive_finite_quote(bid)
        _ask_quote = _valid_positive_finite_quote(ask)
        _crossed_quote_pair = _is_crossed_quote_pair(_bid_quote, _ask_quote)
        if _crossed_quote_pair:
            # A crossed pair is not contradictory market truth that can be
            # used for a decision; it is an internally inconsistent quote.
            # Treat both sides as unavailable so neither entry nor the newly
            # active stop can authorize a mutation from this poll.
            _bid_quote = None
            _ask_quote = None
        _entry_trigger = _valid_positive_finite_quote(self.entry_trigger)
        bid = _bid_quote if _bid_quote is not None else 0.0
        ask = _ask_quote if _ask_quote is not None else 0.0
        self.last_quote_bid_raw = _raw_bid
        self.last_quote_ask_raw = _raw_ask
        self.last_quote_bid = bid
        self.last_quote_ask = ask
        if quote_age_ms is not None:
            self.last_quote_age_ms = quote_age_ms

        if now >= self.expire_at:
            self.state = WatchState.EXPIRED
            log.info("[%s] EXPIRED — no breach in %smin", self.ticker, MAX_WATCH_MINUTES)
            return self.state

        # Missing/unusable required-side evidence suppresses entry evidence,
        # but does not represent contradictory market truth. Before a trigger
        # is confirmed, hold a fresh partial streak only within the fixed
        # continuity window. Once confirmed, this timer has no authority.
        _required_quote = _ask_quote if self.side == "CALL" else _bid_quote
        _entry_trigger_evidence_unavailable = (
            _entry_trigger is None or _required_quote is None
        )
        _suppress_entry_breach_evidence = False
        if _entry_trigger_evidence_unavailable:
            if _crossed_quote_pair:
                self.last_trigger_evidence_reason = (
                    "TRIGGER_EVIDENCE_UNAVAILABLE_CROSSED_BID_ASK"
                )
            else:
                _required_side = "ASK" if self.side == "CALL" else "BID"
                self.last_trigger_evidence_reason = (
                    f"TRIGGER_EVIDENCE_UNAVAILABLE_{_required_side}"
                )
            _suppress_entry_breach_evidence = True
            if self.trigger_crossed_at is None:
                if self.breach_count == 0:
                    self._last_valid_breach_observation_at = None
                else:
                    _elapsed = None
                    if self._last_valid_breach_observation_at is not None:
                        _elapsed = (
                            now - self._last_valid_breach_observation_at
                        ).total_seconds()
                    _fresh = (
                        _elapsed is not None
                        and 0.0 <= _elapsed <= WATCHER_BREACH_CONTINUITY_MAX_GAP_SEC
                    )
                    if not _fresh:
                        log.info(
                            "[%s] WATCHER_BREACH_CONTINUITY_EXPIRED — side=%s "
                            "raw_bid=%r raw_ask=%r continuity_age_sec=%s "
                            "continuity_max_gap_sec=%d stale_breach_count=%d",
                            self.ticker,
                            self.side,
                            _raw_bid,
                            _raw_ask,
                            f"{_elapsed:.1f}" if _elapsed is not None else "unknown",
                            WATCHER_BREACH_CONTINUITY_MAX_GAP_SEC,
                            self.breach_count,
                        )
                        self._reset_pending_breach_continuity()
            # Otherwise HOLD unchanged; no count, timestamp, or trigger price
            # is fabricated from an unusable observation.
        else:
            self.last_trigger_evidence_reason = None

        # A confirmed watcher may return to PENDING for callback retry. If its
        # entry side is valid but the independently required stop side is not,
        # do not re-fire the entry callback from incomplete safety truth.
        _stop_side_quote = _bid_quote if self.side == "CALL" else _ask_quote
        _active_stop_truth_unavailable = (
            self.trigger_crossed_at is not None
            and bool(self.stop_level)
            and _stop_side_quote is None
        )
        if _active_stop_truth_unavailable and not _suppress_entry_breach_evidence:
            _stop_side = "BID" if self.side == "CALL" else "ASK"
            self.last_trigger_evidence_reason = (
                f"ACTIVE_STOP_TRUTH_UNAVAILABLE_{_stop_side}"
            )
            _suppress_entry_breach_evidence = True

        # Intraday stale-move invalidation. Daily overnight signals get their
        # own structural validator, not generic drift logic.
        if (
            not self.overnight
            and not _safe_is_daily_signal(self)
            and self.minutes_watching >= MAX_INTRADAY_WATCH_MIN
        ):
            # PR-C / BUG-EW-3: explicit zero-quote guard BEFORE drift
            # calculation. Previously a quote outage (bid=0, ask=0)
            # produced drift = (0 - trigger) / trigger = -1.0, which for
            # a PUT signal was below the negative threshold and silently
            # expired the signal. Now: if both bid and ask are zero we
            # never enter the drift branch — we wait for a real quote.
            if bid <= 0 and ask <= 0:
                mid = 0.0  # explicit: no quote available, skip drift check
            else:
                mid = (bid + ask) / 2 if bid > 0 and ask > 0 else max(bid, ask)
            if mid > 0 and self.entry_trigger:
                drift = (mid - self.entry_trigger) / self.entry_trigger
                stale = (self.side == "CALL" and drift > MAX_INTRADAY_DRIFT_PCT) or (
                    self.side == "PUT" and drift < -MAX_INTRADAY_DRIFT_PCT
                )
                if stale:
                    self.state = WatchState.EXPIRED
                    log.info(
                        "[%s] STALE ENTRY — watching %.1fmin, price drifted %.2f%% "
                        "from trigger $%.2f. Move missed — expiring.",
                        self.ticker,
                        self.minutes_watching,
                        drift * 100.0,
                        self.entry_trigger,
                    )
                    return self.state

        # PR #388 Block-2 — late-attachment gate.
        # If this watcher was armed inside the continuation window (or past
        # it, awaiting reset), consult the canonical classifier on every
        # fresh poll before letting the normal breach path run.
        if self.late_attachment_state:
            try:
                from ap.pending_trigger_classifier import (
                    classify_late_attachment as _pt_classify_late,
                    is_reset_confirmed as _pt_is_reset,
                    LATE_ATTACHMENT_AWAITING_FIRST_TRUTH as _PT_AWAITING,
                    LATE_ATTACHMENT_WITHIN_CONTINUATION as _PT_WITHIN,
                    MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET as _PT_WAITING,
                    STOP_ALREADY_BROKEN_TERMINAL as _PT_STOP_BROKEN,
                    TARGET_ALREADY_COMPLETE_TERMINAL as _PT_TARGET_COMPLETE,
                    LATE_ATTACHMENT_MOVE_MISSED_TERMINAL as _PT_MOVE_MISSED,
                    TRIGGER_TRUTH_UNAVAILABLE_RETRY as _PT_TRUTH_RETRY,
                )
            except Exception as _pt_import_exc:
                # PR #388 Blocker #3: FAIL CLOSED on classifier import failure.
                # The prior amendment let execution fall through to the
                # ordinary CALL/PUT breach path, silently bypassing
                # WITHIN_CONTINUATION / WAITING_RESET / stop-truth /
                # target-truth / missed-move classification whenever the
                # shared classifier import broke. A safety state machine
                # cannot evaporate because its own import failed.
                log.critical(
                    "[%s] LATE_ATTACHMENT_CLASSIFIER_IMPORT_FAILED — "
                    "preserving state=%s and blocking ordinary breach path "
                    "for this poll: %s",
                    self.ticker, self.late_attachment_state, _pt_import_exc,
                )
                return self.state

            if _pt_classify_late is None:
                # Defensive: import returned None (shouldn't happen but
                # never fall through to ordinary breach if it does).
                log.critical(
                    "[%s] LATE_ATTACHMENT_CLASSIFIER_UNAVAILABLE — "
                    "preserving state=%s and blocking ordinary breach path",
                    self.ticker, self.late_attachment_state,
                )
                return self.state

            if _pt_classify_late is not None:
                # PR #388 P0-7: compute target_complete from canonical side.
                # CALL complete when ask >= target; PUT complete when bid <= target.
                _tgt_complete = False
                try:
                    _tgt = float(self.target_price or 0)
                    if _tgt > 0:
                        if self.side == "CALL" and ask > 0 and ask >= _tgt:
                            _tgt_complete = True
                        elif self.side == "PUT" and bid > 0 and bid <= _tgt:
                            _tgt_complete = True
                except (TypeError, ValueError):
                    _tgt_complete = False

                # PR #388 Blocker #4: wire decisive drift from the existing
                # authoritative MAX_INTRADAY_DRIFT_PCT threshold (1.5% by
                # default). Canonical quote (CALL=ask, PUT=bid) beyond the
                # trigger by more than this pct → the move is decisively
                # past; classify_late_attachment must terminalize as
                # LATE_ATTACHMENT_MOVE_MISSED_TERMINAL rather than
                # transition to WAITING_RESET.
                _decisive_drift = False
                try:
                    _t_poll = float(self.entry_trigger or 0)
                    if _t_poll > 0:
                        if self.side == "CALL" and ask > 0:
                            if ask > _t_poll * (1.0 + MAX_INTRADAY_DRIFT_PCT):
                                _decisive_drift = True
                        elif self.side == "PUT" and bid > 0:
                            if bid < _t_poll * (1.0 - MAX_INTRADAY_DRIFT_PCT):
                                _decisive_drift = True
                except (TypeError, ValueError):
                    _decisive_drift = False

                _late_dec = _pt_classify_late(
                    side=self.side,
                    trigger_price=self.entry_trigger,
                    bid=bid,
                    ask=ask,
                    stop=self.stop_level,
                    target_complete=_tgt_complete,
                    decisive_drift_exceeded=_decisive_drift,
                    trigger_previously_breached=(
                        getattr(self, "trigger_crossed_at", None) is not None
                    ),
                )
                self.late_attachment_last_quote = (
                    float(_late_dec.quote) if _late_dec.quote is not None else None
                )
                self.late_attachment_last_source = _late_dec.quote_source

                if _late_dec.classification == _PT_TRUTH_RETRY and _late_dec.quote is None:
                    # No fresh canonical quote; do not touch state or fire.
                    # (A TRUTH_RETRY with a non-None quote just means the price
                    # is on the ordinary-breach side of trigger — that must
                    # still be evaluated by the WAITING_RESET branch below.)
                    return self.state

                if _late_dec.classification in (_PT_STOP_BROKEN, _PT_TARGET_COMPLETE, _PT_MOVE_MISSED):
                    self.state = WatchState.INVALIDATED
                    self.breach_count = 0
                    _reason_map = {
                        _PT_STOP_BROKEN:     "stop_already_broken_terminal",
                        _PT_TARGET_COMPLETE: "target_already_complete_terminal",
                        _PT_MOVE_MISSED:     "late_attachment_move_missed_terminal",
                    }
                    _reason = _reason_map[_late_dec.classification]
                    _wref = getattr(self, "_watcher_ref", None)
                    if _wref is not None:
                        try:
                            self._pending_audit = _wref._build_watcher_audit_payload(
                                self,
                                trigger_type="late_attachment_gate",
                                current_bid=bid,
                                current_ask=ask,
                                current_mid=(bid + ask) / 2.0 if bid > 0 and ask > 0 else max(bid, ask),
                                arm_condition=f"trigger_{self.entry_trigger:.4f}",
                                reason_code=_reason,
                                raw_reason=f"late_attachment_gate:{_late_dec.detail}",
                                extra={
                                    "late_attachment_state": self.late_attachment_state,
                                    "late_confirm_polls": self.late_confirm_polls,
                                    "late_reset_polls": self.late_reset_polls,
                                    "late_attachment_generation": self.late_attachment_generation,
                                },
                            )
                        except Exception:
                            pass
                    log.warning(
                        "[%s] LATE_ATTACHMENT_GATE_TERMINAL — %s state=%s reason=%s",
                        self.ticker, self.side, self.late_attachment_state, _reason,
                    )
                    return self.state

                # State transitions.
                if self.late_attachment_state == _PT_AWAITING:
                    # No canonical quote at arm time. Quote is now available
                    # (TRUTH_RETRY-with-None was returned above). Reclassify
                    # this first truthful poll and transition; never let the
                    # ordinary breach path run from AWAITING without the
                    # continuation policy applied.
                    if _late_dec.classification == _PT_WITHIN:
                        log.info(
                            "[%s] LATE_ATTACHMENT_FIRST_TRUTH_WITHIN — "
                            "%s canonical_quote=%s (transitioning "
                            "AWAITING_FIRST_TRUTH → WITHIN_CONTINUATION; "
                            "requires 2 confirming polls)",
                            self.ticker, self.side, _late_dec.quote,
                        )
                        self.late_attachment_state = _PT_WITHIN
                        self.late_confirm_polls = 0
                        return self.state
                    if _late_dec.classification == _PT_WAITING:
                        log.info(
                            "[%s] LATE_ATTACHMENT_FIRST_TRUTH_PAST_ZONE — "
                            "%s canonical_quote=%s (transitioning "
                            "AWAITING_FIRST_TRUTH → WAITING_RESET; "
                            "requires 2 reset polls then new ordinary breach)",
                            self.ticker, self.side, _late_dec.quote,
                        )
                        self.late_attachment_state = _PT_WAITING
                        self.late_reset_polls = 0
                        return self.state
                    if _late_dec.classification == _PT_TRUTH_RETRY:
                        # Quote available but on the pre-trigger side of the
                        # canonical lane (CALL: quote<trigger; PUT: quote>trigger).
                        # Setup has NOT breached yet — release the gate and
                        # let the ordinary breach path own the future.
                        log.info(
                            "[%s] LATE_ATTACHMENT_FIRST_TRUTH_PRE_TRIGGER — "
                            "%s canonical_quote=%s (clearing AWAITING gate; "
                            "ordinary future breach permitted)",
                            self.ticker, self.side, _late_dec.quote,
                        )
                        self.late_attachment_state = None
                        return self.state
                    # Terminal classifications are handled by the block
                    # above and never reach this switch. Defensive: preserve
                    # state rather than risk an unclassified fire.
                    return self.state

                if self.late_attachment_state == _PT_WITHIN:
                    if _late_dec.classification == _PT_WITHIN:
                        self.late_confirm_polls += 1
                        if self.late_confirm_polls >= self.MOMENTUM_POLLS_REQUIRED:
                            # Confirmed continuation. Clear late state and
                            # let the normal breach path run — with the
                            # canonical quote at/above trigger it will fire
                            # exactly once via the ordinary confirmation
                            # code below.
                            log.info(
                                "[%s] LATE_CONTINUATION_CONFIRMED — %s "
                                "polls=%d quote=%s (releasing gate; ordinary "
                                "breach path takes over)",
                                self.ticker, self.side,
                                self.late_confirm_polls, _late_dec.quote,
                            )
                            self.late_attachment_state = None
                        else:
                            log.debug(
                                "[%s] LATE_ATTACHMENT_WITHIN_CONTINUATION — "
                                "confirm poll %d/%d",
                                self.ticker,
                                self.late_confirm_polls,
                                self.MOMENTUM_POLLS_REQUIRED,
                            )
                            return self.state
                    elif _late_dec.classification == _PT_WAITING:
                        # Price crossed out of the zone — transition.
                        log.info(
                            "[%s] LATE_ATTACHMENT_ZONE_EXITED — transitioning "
                            "WITHIN_CONTINUATION → WAITING_RESET",
                            self.ticker,
                        )
                        self.late_attachment_state = _PT_WAITING
                        self.late_confirm_polls = 0
                        self.late_reset_polls = 0
                        return self.state
                    else:
                        return self.state

                elif self.late_attachment_state == _PT_WAITING:
                    if _pt_is_reset is not None and _pt_is_reset(
                        side=self.side,
                        trigger_price=self.entry_trigger,
                        canonical_quote=_late_dec.quote,
                    ):
                        self.late_reset_polls += 1
                        if self.late_reset_polls >= self.MOMENTUM_POLLS_REQUIRED:
                            log.info(
                                "[%s] LATE_ATTACHMENT_RESET_CONFIRMED — "
                                "%s reset_polls=%d quote=%s (clearing gate; "
                                "requires new ordinary breach to submit)",
                                self.ticker, self.side,
                                self.late_reset_polls, _late_dec.quote,
                            )
                            self.late_attachment_state = None
                            self.late_reset_polls = 0
                            self.breach_count = 0
                            return self.state
                        log.debug(
                            "[%s] MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET "
                            "— reset poll %d/%d",
                            self.ticker,
                            self.late_reset_polls,
                            self.MOMENTUM_POLLS_REQUIRED,
                        )
                        return self.state
                    else:
                        # Not reset yet; keep waiting (never terminalize).
                        self.late_reset_polls = 0
                        return self.state

        if self.side == "CALL":
            if not _suppress_entry_breach_evidence:
                if _ask_quote is not None and ask >= _entry_trigger:
                    # A valid observation may continue a partial streak only
                    # when its anchor is fresh. This backstop also covers a
                    # delayed valid observation with no intervening missing
                    # poll having been processed.
                    if self.trigger_crossed_at is None and self.breach_count > 0:
                        _elapsed = None
                        if self._last_valid_breach_observation_at is not None:
                            _elapsed = (
                                now - self._last_valid_breach_observation_at
                            ).total_seconds()
                        if (
                            _elapsed is None
                            or _elapsed < 0.0
                            or _elapsed > WATCHER_BREACH_CONTINUITY_MAX_GAP_SEC
                        ):
                            self._reset_pending_breach_continuity()
                    if self.breach_count == 0:
                        self.breach_price = ask
                        # PR #407: durable trigger_crossed_at proof is issued ONLY
                        # after MOMENTUM_POLLS_REQUIRED breaches confirm. Until
                        # then, retain the first-breach poll timestamp in a private
                        # pending slot and record the observed first-breach quote.
                        self._pending_first_breach_at = now
                        self.first_breach_bid = bid
                        self.first_breach_ask = ask
                        log.debug(
                            "[%s] CALL breach candidate — ask=$%.2f >= trigger=$%.2f",
                            self.ticker,
                            ask,
                            self.entry_trigger,
                        )
                    self.breach_count += 1
                    self._last_valid_breach_observation_at = now
                    if self.breach_count >= self.MOMENTUM_POLLS_REQUIRED:
                        # PR #407: confirmation promotes the pending first-breach
                        # timestamp into the durable trigger_crossed_at proof.
                        if self.trigger_crossed_at is None:
                            confirmed_at = self._pending_first_breach_at
                            if confirmed_at is None:
                                confirmed_at = now
                            self.trigger_crossed_at = confirmed_at
                        self._pending_first_breach_at = None
                        self.state = WatchState.TRIGGERED
                        self.triggered_at = now
                        self.trigger_price = ask
                        log.info(
                            "[%s] CALL CONFIRMED — ask=$%.2f held above $%.2f for %d polls",
                            self.ticker,
                            ask,
                            self.entry_trigger,
                            self.breach_count,
                        )
                else:
                    if self.breach_count > 0:
                        log.debug("[%s] CALL breach reset — ask=$%.2f pulled back", self.ticker, ask)
                    self.breach_count = 0
                    self._last_valid_breach_observation_at = None

            # FUNNEL FIX (2026-05-20): for overnight + daily setups, do NOT
            # invalidate on a pre-market stop touch. Pre-open spreads are wide
            # and thinly-traded extended-hours quotes can spike below stop
            # transiently without representing a real thesis break.
            # The validated overnight revalidation at 9:30 ET (which runs the
            # full structural daily validator) will catch genuine breaks.
            #
            # HOTFIX (2026-08-05): this protection is for PRE-breach signals
            # only. Once trigger_crossed_at is confirmed (PR #407), the stop
            # must stay live for the remainder of the pending-entry lifecycle,
            # including through the overnight/pre-market window — that is the
            # explicit invariant PR #407 established ("the stop becomes
            # active for the remainder of the same durable lifecycle").
            # Without this exclusion, a confirmed-breach pending-entry
            # watcher's stop went dormant every night from close until
            # 9:35 ET regardless of confirmed evidence, leaving the setup
            # eligible for later recovery, retry, contract selection, or
            # materialization when it should already be invalidated. This
            # affects the pending-entry watcher only, before broker submit —
            # it does not touch filled positions, which are protected by the
            # order monitor / exit engine.
            _pre_open_skip = (
                (self.overnight or _safe_is_daily_signal(self))
                and _is_pre_market_now()
                and getattr(self, "trigger_crossed_at", None) is None
            )
            # PR #407: scanner-stop protection is dormant until a CONFIRMED
            # entry-direction breach exists (trigger_crossed_at is set only
            # after MOMENTUM_POLLS_REQUIRED breaches). Same-poll trigger/stop
            # collision remains fail-closed because the confirmation branch
            # above assigns trigger_crossed_at within this same check() call.
            if (
                self.stop_level
                and getattr(self, "trigger_crossed_at", None) is not None
                and _bid_quote is not None
                and bid <= self.stop_level * (1 - WRONG_DIR_BUFFER_PCT)
                and not _pre_open_skip
            ):
                _call_stop_mid = (bid + ask) / 2.0 if (bid and ask) else max(bid, ask)
                _call_wref = getattr(self, "_watcher_ref", None)
                # PR #324 — Failure D fix: detect trigger/stop same-poll collision.
                # If the CALL trigger was already confirmed on this same tick
                # (state == TRIGGERED), the stop check must NOT silently overwrite
                # it.  Preserve trigger evidence; set a dedicated collision outcome.
                if self.state == WatchState.TRIGGERED:
                    self._trigger_stop_collision = True
                    _collision_mid = _call_stop_mid
                    if _call_wref is not None:
                        self._pending_audit = _call_wref._build_watcher_audit_payload(
                            self,
                            trigger_type="intraday_check",
                            current_bid=bid,
                            current_ask=ask,
                            current_mid=_collision_mid,
                            arm_condition=f"trigger_{self.entry_trigger:.4f}",
                            stop_condition=f"bid_{bid:.4f}_le_call_stop_{self.stop_level:.4f}",
                            reason_code="trigger_stop_same_poll_collision",
                            raw_reason=(
                                f"call_trigger_confirmed_and_bid_{bid:.4f}"
                                f"_broke_call_stop_{self.stop_level:.4f}_same_poll"
                            ),
                            extra={
                                "trigger_crossed_at": (
                                    self.trigger_crossed_at.isoformat()
                                    if self.trigger_crossed_at else None
                                ),
                                "trigger_confirmed_at": now.isoformat(),
                                "trigger_price": self.trigger_price,
                                "first_breach_bid": self.first_breach_bid,
                                "first_breach_ask": self.first_breach_ask,
                                "stop_level": self.stop_level,
                                "collision": True,
                                "overnight": self.overnight,
                            },
                        )
                    self.state = WatchState.INVALIDATED
                    self.breach_count = 0
                    log.warning(
                        "[%s] TRIGGER_STOP_SAME_POLL_COLLISION — CALL trigger confirmed "
                        "but bid=$%.2f also broke stop=$%.2f on same poll. "
                        "classification=INVALIDATED_ALREADY_BREACHED "
                        "reason=trigger_stop_same_poll_collision — NOT submitting.",
                        self.ticker, bid, self.stop_level,
                    )
                else:
                    if _call_wref is not None:
                        self._pending_audit = _call_wref._build_watcher_audit_payload(
                            self,
                            trigger_type="intraday_check",
                            current_bid=bid,
                            current_ask=ask,
                            current_mid=_call_stop_mid,
                            arm_condition=f"trigger_{self.entry_trigger:.4f}",
                            stop_condition=f"bid_{bid:.4f}_le_call_stop_{self.stop_level:.4f}",
                            reason_code="stop_bid_below_call_stop",
                            raw_reason=f"bid_{bid:.4f}_broke_call_stop_{self.stop_level:.4f}",
                            extra={
                                "overnight": self.overnight,
                                "is_daily": _safe_is_daily_signal(self),
                                "pre_open_skip": _pre_open_skip,
                            },
                        )
                    self.state = WatchState.INVALIDATED
                    self.breach_count = 0
                    log.info(
                        "[%s] INVALIDATED — bid=$%.2f broke stop=$%.2f before trigger",
                        self.ticker,
                        bid,
                        self.stop_level,
                    )
            elif _pre_open_skip and self.stop_level and bid <= self.stop_level * (1 - WRONG_DIR_BUFFER_PCT):
                log.debug(
                    "[%s] pre-open stop touch ignored for daily/overnight setup "
                    "(bid=$%.2f stop=$%.2f) — will revalidate at 9:30 ET",
                    self.ticker, bid, self.stop_level,
                )

        else:  # PUT
            if not _suppress_entry_breach_evidence:
                if _bid_quote is not None and bid <= _entry_trigger:
                    if self.trigger_crossed_at is None and self.breach_count > 0:
                        _elapsed = None
                        if self._last_valid_breach_observation_at is not None:
                            _elapsed = (
                                now - self._last_valid_breach_observation_at
                            ).total_seconds()
                        if (
                            _elapsed is None
                            or _elapsed < 0.0
                            or _elapsed > WATCHER_BREACH_CONTINUITY_MAX_GAP_SEC
                        ):
                            self._reset_pending_breach_continuity()
                    if self.breach_count == 0:
                        self.breach_price = bid
                        # PR #407: see CALL branch — pending until confirmed.
                        self._pending_first_breach_at = now
                        self.first_breach_bid = bid
                        self.first_breach_ask = ask
                        log.debug(
                            "[%s] PUT breach candidate — bid=$%.2f <= trigger=$%.2f",
                            self.ticker,
                            bid,
                            self.entry_trigger,
                        )
                    self.breach_count += 1
                    self._last_valid_breach_observation_at = now
                    if self.breach_count >= self.MOMENTUM_POLLS_REQUIRED:
                        # PR #407: confirmation promotes pending timestamp.
                        if self.trigger_crossed_at is None:
                            confirmed_at = self._pending_first_breach_at
                            if confirmed_at is None:
                                confirmed_at = now
                            self.trigger_crossed_at = confirmed_at
                        self._pending_first_breach_at = None
                        self.state = WatchState.TRIGGERED
                        self.triggered_at = now
                        self.trigger_price = bid
                        log.info(
                            "[%s] PUT CONFIRMED — bid=$%.2f held below $%.2f for %d polls",
                            self.ticker,
                            bid,
                            self.entry_trigger,
                            self.breach_count,
                        )
                else:
                    if self.breach_count > 0:
                        log.debug("[%s] PUT breach reset — bid=$%.2f pulled back", self.ticker, bid)
                    self.breach_count = 0
                    self._last_valid_breach_observation_at = None

            # FUNNEL FIX (2026-05-20): same pre-open guard for PUT setups.
            # HOTFIX (2026-08-05): see CALL branch — dormant only pre-breach;
            # affects the pending-entry watcher only, not a filled position.
            _pre_open_skip = (
                (self.overnight or _safe_is_daily_signal(self))
                and _is_pre_market_now()
                and getattr(self, "trigger_crossed_at", None) is None
            )
            # PR #407: PUT scanner stop is dormant until confirmed breach.
            # Symmetric to CALL; a pre-trigger ask touch is inert. Same-poll
            # collision fail-closed via confirmation-branch assignment above.
            if (
                self.stop_level
                and getattr(self, "trigger_crossed_at", None) is not None
                and _ask_quote is not None
                and ask >= self.stop_level * (1 + WRONG_DIR_BUFFER_PCT)
                and not _pre_open_skip
            ):
                _put_stop_mid = (bid + ask) / 2.0 if (bid and ask) else max(bid, ask)
                _put_wref = getattr(self, "_watcher_ref", None)
                # PR #324 — Failure D fix: PUT trigger/stop collision.
                if self.state == WatchState.TRIGGERED:
                    self._trigger_stop_collision = True
                    if _put_wref is not None:
                        self._pending_audit = _put_wref._build_watcher_audit_payload(
                            self,
                            trigger_type="intraday_check",
                            current_bid=bid,
                            current_ask=ask,
                            current_mid=_put_stop_mid,
                            arm_condition=f"trigger_{self.entry_trigger:.4f}",
                            stop_condition=f"ask_{ask:.4f}_ge_put_stop_{self.stop_level:.4f}",
                            reason_code="trigger_stop_same_poll_collision",
                            raw_reason=(
                                f"put_trigger_confirmed_and_ask_{ask:.4f}"
                                f"_broke_put_stop_{self.stop_level:.4f}_same_poll"
                            ),
                            extra={
                                "trigger_crossed_at": (
                                    self.trigger_crossed_at.isoformat()
                                    if self.trigger_crossed_at else None
                                ),
                                "trigger_confirmed_at": now.isoformat(),
                                "trigger_price": self.trigger_price,
                                "first_breach_bid": self.first_breach_bid,
                                "first_breach_ask": self.first_breach_ask,
                                "stop_level": self.stop_level,
                                "collision": True,
                                "overnight": self.overnight,
                            },
                        )
                    self.state = WatchState.INVALIDATED
                    self.breach_count = 0
                    log.warning(
                        "[%s] TRIGGER_STOP_SAME_POLL_COLLISION — PUT trigger confirmed "
                        "but ask=$%.2f also broke stop=$%.2f on same poll. "
                        "classification=INVALIDATED_ALREADY_BREACHED "
                        "reason=trigger_stop_same_poll_collision — NOT submitting.",
                        self.ticker, ask, self.stop_level,
                    )
                else:
                    if _put_wref is not None:
                        self._pending_audit = _put_wref._build_watcher_audit_payload(
                            self,
                            trigger_type="intraday_check",
                            current_bid=bid,
                            current_ask=ask,
                            current_mid=_put_stop_mid,
                            arm_condition=f"trigger_{self.entry_trigger:.4f}",
                            stop_condition=f"ask_{ask:.4f}_ge_put_stop_{self.stop_level:.4f}",
                            reason_code="stop_ask_above_put_stop",
                            raw_reason=f"ask_{ask:.4f}_broke_put_stop_{self.stop_level:.4f}",
                            extra={
                                "overnight": self.overnight,
                                "is_daily": _safe_is_daily_signal(self),
                                "pre_open_skip": _pre_open_skip,
                            },
                        )
                    self.state = WatchState.INVALIDATED
                    self.breach_count = 0
                    log.info(
                        "[%s] INVALIDATED — ask=$%.2f broke stop=$%.2f before trigger",
                        self.ticker,
                        ask,
                        self.stop_level,
                    )

        # A trigger may become confirmed inside this very poll.  The
        # pre-confirmation stop-truth guard above cannot see that transition,
        # so apply the same fail-closed rule once the ordinary breach path has
        # completed.  Preserve the confirmed trigger evidence, but do not
        # return TRIGGERED to the dispatcher while the newly active scanner
        # stop has unknown truth.
        if (
            self.state == WatchState.TRIGGERED
            and self.trigger_crossed_at is not None
            and bool(self.stop_level)
            and _stop_side_quote is None
        ):
            _stop_side = "BID" if self.side == "CALL" else "ASK"
            self.state = WatchState.PENDING
            if _crossed_quote_pair:
                self.last_trigger_evidence_reason = (
                    "TRIGGER_EVIDENCE_UNAVAILABLE_CROSSED_BID_ASK"
                )
            else:
                self.last_trigger_evidence_reason = (
                    f"ACTIVE_STOP_TRUTH_UNAVAILABLE_{_stop_side}"
                )
            log.warning(
                "[%s] SAME_POLL_ACTIVE_STOP_TRUTH_UNAVAILABLE — "
                "preserving trigger_crossed_at=%s, holding watcher PENDING, "
                "and suppressing trigger callback | side=%s stop_side=%s",
                self.ticker,
                self.trigger_crossed_at,
                self.side,
                _stop_side,
            )

        return self.state


class APEntryWatcher:
    """Background watcher for queue-created entry plans."""

    def __init__(self, broker, order_state_machine=None,
                 require_on_trigger: Optional[bool] = None,
                 mode: str = "PAPER"):
        self.broker = broker
        self.order_state_machine = order_state_machine
        if require_on_trigger is None:
            require_on_trigger = os.getenv("AP_WATCHER_REQUIRE_ON_TRIGGER", "1").strip().lower() not in {"0", "false", "no"}
        self.require_on_trigger = bool(require_on_trigger)

        # PR-C / BUG-EW-5: canonical mode wired from APExecutionCore at
        # construct time. Previously self.mode was never set anywhere,
        # so getattr(self, "mode", "PAPER") in _revalidate_overnight_at_open
        # always returned "PAPER" — silently arming live overnight setups
        # on pre-market quote outages instead of invalidating them. Default
        # is PAPER for back-compat with any callers that omit the kwarg.
        self.mode = (mode or "PAPER").upper()
        self._pending: list[WatchedSignal] = []
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self.owner_token = f"watcher:{uuid.uuid4()}"

        self.on_trigger: Optional[Callable] = None
        self.on_expire: Optional[Callable] = None
        self.on_invalidate: Optional[Callable] = None

        self._open_trigger_count = 0
        self._open_protect_date = None
        self._open_trigger_tickers: set = set()   # per-ticker open protection
        self._open_trigger_keys: set = set()      # arbitration identity keys
        self._open_trigger_owners: dict = {}      # key -> exact retained watcher

        # Real watcher-level duplicate barrier. Cleanup alone is not enough;
        # the key must be initialized and enforced before a signal is armed.
        self._dedup_set: set[str] = set()
        self._last_reject_reason: Optional[str] = None

    @staticmethod
    def _is_deferred_signal(signal: dict) -> bool:
        contract = str((signal or {}).get("contract_symbol") or "").strip().upper()
        return bool((signal or {}).get("contract_deferred")) or contract.startswith("DEFERRED:")

    def _resolve_trigger_callback_disposition(self, watched, result) -> tuple[str, str | None]:
        """Prove the durable owner/state before a triggered watcher is removed.

        AMENDMENT §4 (durable verification of callback dispositions)
        -----------------------------------------------------------
        A trigger callback returns a *claimed* outcome — it is a request,
        not proof.  A callback's own DB write may have silently failed
        while it still returned ``disposition=SUBMITTED``.  Removing the
        watcher on that word alone leaves the row PENDING_TRIGGER with
        no owner.

        For deferred signals we therefore ALWAYS re-read the order row
        and verify the claim against the durable state before allowing
        watcher removal.  An unverified or unknowable claim collapses to
        ``KEEP_WATCHER`` so the invariant "no ownerless row" holds.

        For non-deferred signals the existing behaviour is preserved
        (the callback dict is trusted verbatim) — those paths already
        have their own durable-write guarantees and rewiring them here
        would risk duplicate submissions.
        """
        signal = getattr(watched, "signal", {}) or {}
        is_deferred = self._is_deferred_signal(signal)

        # ── Extract the CLAIM (may be absent / malformed) ────────────
        claimed_disposition: str | None = None
        claimed_next_retry: str | None = None
        if isinstance(result, dict):
            _raw = str(result.get("disposition") or "").strip().upper()
            if _raw in {
                "RETRY_WAIT", "KEEP_WATCHER", "TERMINAL_DURABLE",
                "SUBMITTED", "OWNERSHIP_TRANSFERRED",
                "RECONCILE_BROKER_INTENT",
            }:
                claimed_disposition = _raw
                claimed_next_retry = result.get("next_retry_at")

        # ── Non-deferred: preserve prior behaviour (trust the claim) ──
        if not is_deferred:
            if claimed_disposition:
                return claimed_disposition, claimed_next_retry
            return "OWNERSHIP_TRANSFERRED", None

        # ── Deferred: always re-read the row ─────────────────────────
        local_order_id = str(signal.get("local_order_id") or "").strip()
        get_order = getattr(self.order_state_machine, "get_order", None)
        if not local_order_id or not callable(get_order):
            return "UNKNOWN", None
        try:
            row = get_order(local_order_id)
        except Exception:
            return "UNKNOWN", None
        if not isinstance(row, dict):
            return "UNKNOWN", None

        status = str(row.get("status") or "").upper()
        broker_order_id = str(row.get("broker_order_id") or "").strip()
        submitted_ts = row.get("submitted_ts")
        meta = row.get("meta") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        meta = meta or {}
        lifecycle = str(meta.get("lifecycle_state") or "").upper()
        mstatus = str(meta.get("materialization_status") or "").upper()

        def _submitted_family() -> bool:
            return status in {"SUBMITTED", "ACKNOWLEDGED", "PARTIAL_FILL", "FILLED"}

        def _terminal_family() -> bool:
            return status in {"REJECTED", "EXPIRED", "CANCELED", "ERROR"}

        def _terminal_reason_present() -> bool:
            return bool(
                meta.get("reason_code")
                or meta.get("final_reason")
                or meta.get("materialization_reason")
            )

        def _verify_submitted() -> tuple[str, str | None]:
            """Return the verified disposition for a SUBMITTED claim."""
            if not _submitted_family():
                return "KEEP_WATCHER", None
            if broker_order_id:
                return "SUBMITTED", None
            # No broker id yet.  If a durable submit-intent exists the
            # order is in the crash-window and belongs to reconciliation.
            # Per amendment §6 (still to land) the reconciler will own
            # the row; until then we keep the watcher so the row is
            # never ownerless.  RECONCILE_BROKER_INTENT is emitted for
            # diagnostic clarity and is treated as "retain" by the
            # consumer.
            if meta.get("submit_intent_at"):
                return "RECONCILE_BROKER_INTENT", None
            # Status says submitted but no broker id and no submit intent
            # — inconsistent durable state.  Retain the watcher.
            return "KEEP_WATCHER", None

        def _verify_terminal() -> tuple[str, str | None]:
            if _terminal_family() and _terminal_reason_present():
                return "TERMINAL_DURABLE", None
            return "KEEP_WATCHER", None

        def _verify_retry() -> tuple[str, str | None]:
            if status not in {"PENDING_TRIGGER", "CREATED"}:
                return "KEEP_WATCHER", None
            if lifecycle != "RETRY_WAIT" or mstatus != "RETRY_PENDING":
                return "KEEP_WATCHER", None
            next_retry_at = (
                meta.get("next_retry_at")
                or meta.get("materialization_next_retry_at")
            )
            if not next_retry_at:
                return "KEEP_WATCHER", None
            if meta.get("retry_attempt") is None:
                return "KEEP_WATCHER", None
            if meta.get("retry_max_attempts") is None:
                return "KEEP_WATCHER", None
            if meta.get("broker_ready") is True:
                return "KEEP_WATCHER", None
            if broker_order_id:
                return "KEEP_WATCHER", None
            if submitted_ts is not None:
                return "KEEP_WATCHER", None
            return "RETRY_WAIT", str(next_retry_at)

        def _verify_ownership_transferred() -> tuple[str, str | None]:
            current_owner = str(
                meta.get("current_owner")
                or meta.get("materialization_owner")
                or ""
            ).strip()
            owner_token = str(
                meta.get("owner_token")
                or meta.get("materialization_owner_token")
                or ""
            ).strip()
            owner_generation = (
                meta.get("owner_generation")
                if meta.get("owner_generation") is not None
                else meta.get("materialization_generation")
            )
            owner_lease_until = (
                meta.get("owner_lease_until")
                or meta.get("materialization_lease_until")
            )
            if not (current_owner and owner_token
                    and owner_generation is not None and owner_lease_until):
                return "KEEP_WATCHER", None
            try:
                lease = datetime.fromisoformat(str(owner_lease_until))
                if lease.tzinfo is None:
                    lease = lease.replace(tzinfo=timezone.utc)
            except Exception:
                return "KEEP_WATCHER", None
            if lease <= datetime.now(timezone.utc):
                return "KEEP_WATCHER", None
            return "OWNERSHIP_TRANSFERRED", None

        # ── Route the claim through its verifier ──────────────────────
        if claimed_disposition == "SUBMITTED":
            return _verify_submitted()
        if claimed_disposition == "TERMINAL_DURABLE":
            return _verify_terminal()
        if claimed_disposition == "RETRY_WAIT":
            return _verify_retry()
        if claimed_disposition == "OWNERSHIP_TRANSFERRED":
            return _verify_ownership_transferred()
        if claimed_disposition == "KEEP_WATCHER":
            return "KEEP_WATCHER", claimed_next_retry
        if claimed_disposition == "RECONCILE_BROKER_INTENT":
            # Trust the claim only if row actually shows a durable intent.
            if meta.get("submit_intent_at"):
                return "RECONCILE_BROKER_INTENT", None
            return "KEEP_WATCHER", None

        # ── No claim (None/malformed) — infer from durable row ────────
        # Prior behaviour, preserved as fallback.  Every branch below
        # ends in either a verified terminal state, an in-flight
        # KEEP_WATCHER, or UNKNOWN (which the consumer converts into
        # watcher retention via RuntimeError).
        submitted, _ = _verify_submitted()
        if submitted in {"SUBMITTED", "RECONCILE_BROKER_INTENT"}:
            return submitted, None
        terminal, _ = _verify_terminal()
        if terminal == "TERMINAL_DURABLE":
            return terminal, None
        if lifecycle == "RETRY_WAIT":
            next_retry = (
                meta.get("next_retry_at")
                or meta.get("materialization_next_retry_at")
            )
            if next_retry:
                return "RETRY_WAIT", str(next_retry)
        if lifecycle == "MATERIALIZING":
            return "KEEP_WATCHER", meta.get("materialization_lease_until")
        return "UNKNOWN", None

    def _dedup_key_for_signal(self, signal: dict) -> str:
        return str(signal.get("signal_id") or "").strip()

    def has_order(self, local_order_id: Optional[str]) -> bool:
        """Return True when this watcher currently owns the local ENTRY order.

        PENDING_TRIGGER is the valid steady-state for an armed watcher-held
        order. OrderMonitor uses this proof to distinguish live watcher-owned
        rows from true orphaned ghosts after a restart or arm failure.
        """
        local_order_id = str(local_order_id or "").strip()
        if not local_order_id:
            return False

        with self._lock:
            for watched in self._pending:
                _sig = getattr(watched, "signal", {}) or {}
                _owned_id = str(_sig.get("local_order_id") or "").strip()
                if _owned_id != local_order_id:
                    continue
                if watched.is_active or getattr(watched, "rearm_mode", False):
                    return True
        return False

    def prove_materialization_retry_owner(
        self,
        local_order_id: Optional[str],
        *,
        expected_client_id: Optional[str] = None,
        expected_execution_mode: Optional[str] = None,
        expected_watcher_token: Optional[str] = None,
        expected_generation: Optional[int] = None,
        durable_next_retry_at: Optional[str] = None,
        durable_retry_deadline: Optional[str] = None,
    ) -> dict:
        """Return a structured proof of executable retry ownership.

        P0 AMENDMENT (fix/deferred-retry-due-execution-p0):
        Registry presence alone is NOT proof that a watcher will execute a
        due materialization retry. This method inspects the actual registered
        ``WatchedSignal`` and verifies every condition required for it to
        execute the next attempt:

          * exact local_order_id, client_id, execution_mode match
          * watcher_token matches durable row (fenced ownership)
          * trigger_generation matches durable materialization_generation
          * watcher is not terminal, quarantined, expired, or inactive
          * the durable next_retry_at is either absent (already due & waiting
            for poll) or matches the in-memory ``deferred_retry_not_before``
            within a small tolerance — proving the poll loop will actually
            consume the due retry rather than continuing to sleep past it
          * retry deadline (absolute_entry_deadline) has not expired

        Returns a structured result:

            {
                "proven": bool,
                "reason_code": str,
                "watcher_token": str | None,
                "generation": int | None,
                "retry_due_at": str | None,   # in-memory deferred_retry_not_before
                "retry_deadline": str | None, # from durable row echoed back
            }

        A bare ``has_order()`` cannot express this; it returns True for a
        registered-but-inert watcher whose deferred_retry_not_before has been
        cleared or is None but that has no executable path back into the
        selector callback. Recovery MUST use this method for due retries and
        MUST fenced-CAS reclaim when ``proven=False`` — never fall through
        to "registered means owned."
        """
        local_order_id = str(local_order_id or "").strip()
        base: dict = {
            "proven": False,
            "reason_code": "",
            "watcher_token": None,
            "generation": None,
            "retry_due_at": None,
            "retry_deadline": durable_retry_deadline,
        }
        if not local_order_id:
            base["reason_code"] = "PROOF_MISSING_LOCAL_ORDER_ID"
            return base

        expected_client_id_norm = str(expected_client_id or "").strip().lower() or None
        expected_mode_norm = str(expected_execution_mode or "").strip().lower() or None
        expected_token_norm = str(expected_watcher_token or "").strip() or None
        try:
            expected_generation_int = (
                int(expected_generation) if expected_generation is not None else None
            )
        except (TypeError, ValueError):
            base["reason_code"] = "PROOF_INVALID_EXPECTED_GENERATION"
            return base

        durable_due_at_dt: Optional[datetime] = None
        if durable_next_retry_at:
            try:
                durable_due_at_dt = datetime.fromisoformat(str(durable_next_retry_at))
                if durable_due_at_dt.tzinfo is None:
                    durable_due_at_dt = durable_due_at_dt.replace(tzinfo=timezone.utc)
            except Exception:
                base["reason_code"] = "PROOF_INVALID_DURABLE_NEXT_RETRY_AT"
                return base

        durable_deadline_dt: Optional[datetime] = None
        if durable_retry_deadline:
            try:
                durable_deadline_dt = datetime.fromisoformat(str(durable_retry_deadline))
                if durable_deadline_dt.tzinfo is None:
                    durable_deadline_dt = durable_deadline_dt.replace(tzinfo=timezone.utc)
            except Exception:
                base["reason_code"] = "PROOF_INVALID_DURABLE_RETRY_DEADLINE"
                return base

        now = datetime.now(timezone.utc)
        if durable_deadline_dt is not None and now >= durable_deadline_dt:
            base["reason_code"] = "PROOF_RETRY_DEADLINE_EXPIRED"
            return base

        with self._lock:
            candidate = None
            for watched in self._pending:
                _sig = getattr(watched, "signal", {}) or {}
                if str(_sig.get("local_order_id") or "").strip() != local_order_id:
                    continue
                candidate = watched
                break

            if candidate is None:
                base["reason_code"] = "PROOF_WATCHER_NOT_REGISTERED"
                return base

            _sig = getattr(candidate, "signal", {}) or {}
            row_client = str(_sig.get("client_id") or "").strip().lower()
            row_mode = str(_sig.get("execution_mode") or "").strip().lower()
            row_token = str(_sig.get("watcher_token") or "").strip()
            try:
                row_generation = int(_sig.get("trigger_generation") or 0)
            except (TypeError, ValueError):
                row_generation = 0

            base["watcher_token"] = row_token or None
            base["generation"] = row_generation or None
            _retry_dt = getattr(candidate, "deferred_retry_not_before", None)
            if isinstance(_retry_dt, datetime):
                base["retry_due_at"] = _retry_dt.isoformat()
            elif _retry_dt is not None:
                base["retry_due_at"] = str(_retry_dt)

            # ── BLOCKER §2: exact identity — absence fails like a mismatch ──
            # When an expected identity field is supplied, an absent watcher
            # field must fail proof exactly as a mismatch would. This closes
            # the fail-open window where a watcher with no stored client_id,
            # watcher_token, or generation could still pass because one side
            # of the comparison was empty.
            if expected_client_id_norm:
                if expected_client_id_norm != row_client:
                    base["reason_code"] = "PROOF_CLIENT_ID_MISMATCH"
                    return base
            if expected_mode_norm:
                if expected_mode_norm != row_mode:
                    base["reason_code"] = "PROOF_EXECUTION_MODE_MISMATCH"
                    return base
            if expected_token_norm:
                if expected_token_norm != row_token:
                    base["reason_code"] = "PROOF_WATCHER_TOKEN_MISMATCH"
                    return base
            if expected_generation_int is not None:
                if expected_generation_int != row_generation:
                    base["reason_code"] = "PROOF_GENERATION_MISMATCH"
                    return base

            # Must be in a state that can actually execute the poll callback.
            # Quarantined, rearm-only, or expired watchers cannot.
            _state = getattr(candidate, "state", None)
            _state_name = getattr(_state, "name", str(_state)) if _state is not None else ""
            if str(_state_name).upper() not in {"PENDING", ""}:
                base["reason_code"] = f"PROOF_WATCHER_STATE_NOT_PENDING:{_state_name}"
                return base
            if getattr(candidate, "_ownership_quarantine", False):
                base["reason_code"] = "PROOF_WATCHER_QUARANTINED"
                return base
            if getattr(candidate, "rearm_mode", False):
                base["reason_code"] = "PROOF_WATCHER_REARM_ONLY"
                return base
            if not getattr(candidate, "is_active", False):
                base["reason_code"] = "PROOF_WATCHER_NOT_ACTIVE"
                return base

            _expire_at = getattr(candidate, "expire_at", None)
            if isinstance(_expire_at, datetime):
                if _expire_at.tzinfo is None:
                    _expire_at = _expire_at.replace(tzinfo=timezone.utc)
                if now >= _expire_at:
                    base["reason_code"] = "PROOF_WATCHER_EXPIRED"
                    return base

            # ── Schedule alignment proof (applies to ALL retries) ───────────
            # P0 blocker §1 (second round): previously only validated
            # deferred_retry_not_before when the durable timestamp was already
            # due. For future retries, a watcher with deferred_retry_not_before
            # = None proceeds immediately to quote evaluation on every poll
            # cycle — it never waits for the durable retry time. A watcher
            # whose in-memory schedule is malformed is also cleared and proceeds
            # immediately. Both must FAIL proof for any durable timestamp,
            # whether past or future.
            #
            # Tolerance: 1 second. The clock delta between the durable write
            # and the recovery pass creates a small natural drift; a 1-second
            # tolerance is tight enough to catch None/malformed/wildly mismatched
            # schedules and wide enough to avoid false negatives from sub-second
            # timing differences.
            _SCHEDULE_TOLERANCE_SECONDS = 1

            if durable_due_at_dt is not None:
                # Parse the in-memory schedule.
                _mem_due_dt: Optional[datetime] = None
                if isinstance(_retry_dt, datetime):
                    _mem_due_dt = _retry_dt
                elif isinstance(_retry_dt, str) and _retry_dt.strip():
                    try:
                        _mem_due_dt = datetime.fromisoformat(_retry_dt)
                    except Exception:
                        _mem_due_dt = None

                if _mem_due_dt is None:
                    # No in-memory schedule — watcher is inert regardless of
                    # whether the durable retry is due or future.
                    base["reason_code"] = "PROOF_NO_MEMORY_RETRY_SCHEDULE"
                    return base

                if _mem_due_dt.tzinfo is None:
                    _mem_due_dt = _mem_due_dt.replace(tzinfo=timezone.utc)

                _delta = abs((_mem_due_dt - durable_due_at_dt).total_seconds())
                if _delta > _SCHEDULE_TOLERANCE_SECONDS:
                    # In-memory schedule diverges materially from the durable
                    # schedule — the poll loop will fire at the wrong time (or
                    # not at all). Fail proof with the exact delta so forensics
                    # can see how far out of alignment the watcher is.
                    base["reason_code"] = (
                        f"PROOF_SCHEDULE_MISMATCH:delta={_delta:.1f}s:"
                        f"mem={_mem_due_dt.isoformat()}:"
                        f"durable={durable_due_at_dt.isoformat()}"
                    )
                    return base

            base["proven"] = True
            base["reason_code"] = "PROOF_OK"
            return base

    def _validate_local_order_id(self, local_order_id: Optional[str]) -> bool:
        """Best-effort OSM pre-arm validation.

        If an OSM instance is supplied, prove the queue-created local order exists
        before arming. If no OSM is supplied, stay backward compatible and let
        ExecutionCore perform breach-time recovery.
        """
        if not local_order_id:
            return True
        osm = getattr(self, "order_state_machine", None)
        if osm is None:
            return True

        probes = ("has_order", "exists", "contains", "get_order", "get", "get_order_by_local_id")
        for name in probes:
            fn = getattr(osm, name, None)
            if not callable(fn):
                continue
            try:
                result = fn(local_order_id)
                if isinstance(result, bool):
                    return result
                if result is not None:
                    return True
            except TypeError:
                continue
            except Exception as exc:
                log.warning("OSM local_order_id validation failed via %s(%s): %s", name, local_order_id, exc)
                return False

        for attr in ("orders", "_orders", "pending_orders", "_pending_orders", "local_orders", "_local_orders"):
            obj = getattr(osm, attr, None)
            try:
                if isinstance(obj, dict):
                    return local_order_id in obj
                if obj is not None and local_order_id in obj:
                    return True
            except Exception:
                continue

        log.warning(
            "[WATCHER_VALIDATION_FALLBACK] OSM object supplied but no recognized "
            "local-order lookup contract exists; allowing watcher arm for "
            "local_order_id=%s and relying on ExecutionCore recovery. "
            "A misconfigured or mock OSM will silently validate every signal. "
            "Set AP_WATCHER_STRICT_OSM_VALIDATION=1 to make this path reject instead.",
            local_order_id,
        )
        return True

    def _load_order_row_for_recovery_rearm(self, local_order_id: Optional[str]) -> dict:
        """Best-effort OSM row load for recovery rearm classification."""
        oid = str(local_order_id or "").strip()
        if not oid:
            return {}
        osm = getattr(self, "order_state_machine", None)
        if osm is None:
            return {}
        for name in ("get_order", "get", "get_order_by_local_id"):
            fn = getattr(osm, name, None)
            if not callable(fn):
                continue
            try:
                row = fn(oid)
                return row if isinstance(row, dict) else {}
            except TypeError:
                continue
            except Exception as exc:
                log.warning("[%s] recovery rearm order-row load failed via %s: %s", oid, name, exc)
                return {}
        for attr in ("orders", "_orders", "pending_orders", "_pending_orders", "local_orders", "_local_orders"):
            obj = getattr(osm, attr, None)
            try:
                if isinstance(obj, dict):
                    row = obj.get(oid) or {}
                    return row if isinstance(row, dict) else {}
            except Exception:
                continue
        return {}

    def _is_past_entry_cutoff_now(self) -> bool:
        try:
            now_et = datetime.now(ET)
            return (
                now_et.hour > EOD_CUTOFF_HOUR
                or (now_et.hour == EOD_CUTOFF_HOUR and now_et.minute >= EOD_CUTOFF_MIN)
            )
        except Exception:
            return False

    def _is_live_runtime(self) -> bool:
        """
        True when this watcher instance is running in LIVE mode.
        Checks three independent attributes so the detection is robust even
        when one is blank (common during restart / recovery paths):
          1. self.execution_mode == "live"
          2. self.mode == "LIVE"
          3. self.paper is explicitly False
        Only returns True if at least one confirms LIVE and none confirm PAPER.
        """
        _exec_mode = str(getattr(self, "execution_mode", "") or "").strip().lower()
        _mode_str  = str(getattr(self, "mode",           "") or "").strip().upper()
        _paper_flag = getattr(self, "paper", None)
        is_live  = (
            _exec_mode  == "live"
            or _mode_str == "LIVE"
            or _paper_flag is False
        )
        is_paper = (
            _exec_mode  == "paper"
            or _mode_str == "PAPER"
            or _paper_flag is True
        )
        # If contradictory, trust the explicit paper flag first (safer default)
        if is_paper:
            return False
        return is_live

    def _is_regular_session_now(self) -> bool:
        try:
            now_et = datetime.now(ET)
            if now_et.hour < 9 or (now_et.hour == 9 and now_et.minute < 30):
                return False
            if (
                now_et.hour > OVERNIGHT_THRESHOLD_HOUR
                or (now_et.hour == OVERNIGHT_THRESHOLD_HOUR and now_et.minute >= OVERNIGHT_THRESHOLD_MIN)
            ):
                return False
            return True
        except Exception:
            return False

    def _terminalize_recovery_rearm_candidate(
        self,
        local_order_id: Optional[str],
        *,
        ticker: str,
        classification: str,
        watcher_owned: bool,
        already_through: Optional[bool],
    ) -> None:
        """Durably block unsafe recovery candidates without broker submit."""
        oid = str(local_order_id or "").strip()
        audit = {
            "reason_code": "recovery_rearm_blocked",
            "classification": str(classification or ""),
            "watcher_owned": bool(watcher_owned),
            "live_quote_already_through_trigger": already_through,
            "trigger_type": "recovery_rearm_classifier",
        }
        try:
            self._persist_watcher_audit(oid, audit)
        except Exception:
            pass
        osm = getattr(self, "order_state_machine", None)
        cancel_fn = getattr(osm, "cancel_pending_entry", None) if osm is not None else None
        if callable(cancel_fn) and oid:
            try:
                cancel_fn(oid, reason=f"pending_trigger_classifier:{classification}")
                log.warning(
                    "[%s] RECOVERY_REARM_BLOCKED classification=%s local_order_id=%s "
                    "watcher_owned=%s already_through=%s terminalized=true",
                    ticker, classification, oid, watcher_owned, already_through,
                )
            except Exception as exc:
                log.error("[%s] recovery rearm terminalize failed for %s: %s", ticker, oid, exc)

    # ── Watcher Audit Helpers ────────────────────────────────────────────────
    # _build_watcher_audit_payload: pure dict construction — safe to call inside
    #   any lock or poll tick. No I/O.
    # _persist_watcher_audit: best-effort OSM meta merge. Never raises.
    #   Logs with persisted=false when local_order_id is absent or order not found.

    def _resolve_watcher_quote_transport(self) -> dict:
        """Resolve the watcher quote transport without leaking credentials."""
        import os as _os
        _LIVE_QUOTE_URL = "https://api.tradier.com"
        base_url = str(
            _os.getenv("TRADIER_MARKET_DATA_BASE_URL")
            or _os.getenv("TRADIER_DATA_BASE_URL")
            or _LIVE_QUOTE_URL
        ).rstrip("/")
        if "sandbox.tradier.com" in base_url.lower():
            log.error(
                "[watcher_quotes] WATCHER_QUOTE_URL_SANDBOX_GUARD_TRIGGERED "
                "resolved_url=%s — sandbox URL must not be used for watcher "
                "quote/trigger/stop/invalidation decisions. "
                "Forcing https://api.tradier.com. "
                "Set TRADIER_MARKET_DATA_BASE_URL=https://api.tradier.com to silence.",
                base_url,
            )
            base_url = _LIVE_QUOTE_URL

        token = None
        token_source_name = "missing"
        if _os.getenv("TRADIER_MARKET_DATA_TOKEN"):
            token = _os.getenv("TRADIER_MARKET_DATA_TOKEN")
            token_source_name = "TRADIER_MARKET_DATA_TOKEN"
        elif _os.getenv("TRADIER_DATA_TOKEN"):
            token = _os.getenv("TRADIER_DATA_TOKEN")
            token_source_name = "TRADIER_DATA_TOKEN"
        elif getattr(self.broker, "live_access_token", None):
            token = getattr(self.broker, "live_access_token", None)
            token_source_name = "broker.live_access_token"
        elif getattr(getattr(self.broker, "cfg", None), "live_access_token", None):
            token = getattr(getattr(self.broker, "cfg", None), "live_access_token", None)
            token_source_name = "broker.cfg.live_access_token"

        return {
            "watcher_quote_source": "tradier_live",
            "watcher_quote_base_url": base_url,
            "watcher_sandbox_mode": False,
            "watcher_quote_token_source": token_source_name,
            "token": token or None,
        }

    def _resolve_watcher_quote_url(self) -> tuple:
        """
        Resolve (base_url, token) for watcher QUOTE fetches.

        Execution mode must NOT determine the quote URL:
          - Paper execution uses sandbox for orders, but watcher quote/trigger/
            stop/invalidation must evaluate against live market data.
          - Live execution: live for both orders and quotes.

        URL priority:
          1. TRADIER_MARKET_DATA_BASE_URL env var
          2. TRADIER_DATA_BASE_URL env var
          3. https://api.tradier.com  (hardcoded live default — NEVER sandbox)

        Token priority:
          1. TRADIER_MARKET_DATA_TOKEN env var
          2. TRADIER_DATA_TOKEN env var
          3. broker.live_access_token attribute
          4. broker.cfg.live_access_token attribute
          5. None  (caller falls back to broker.session)

        Never returns sandbox.tradier.com as the quote URL.
        """
        transport = self._resolve_watcher_quote_transport()
        return transport["watcher_quote_base_url"], transport["token"]

    def _set_last_quote_fetch_proof(self, **proof) -> dict:
        current = dict(getattr(self, "_last_quote_fetch_proof", {}) or {})
        current.update(proof)
        self._last_quote_fetch_proof = current
        return current

    def _current_watcher_quote_proof(self) -> dict:
        transport = self._resolve_watcher_quote_transport()
        proof = dict(getattr(self, "_last_quote_fetch_proof", {}) or {})
        return {
            "watcher_quote_source": proof.get("watcher_quote_source", transport["watcher_quote_source"]),
            "watcher_quote_base_url": proof.get("watcher_quote_base_url", transport["watcher_quote_base_url"]),
            "watcher_sandbox_mode": bool(
                proof.get("watcher_sandbox_mode", transport["watcher_sandbox_mode"])
            ),
            "watcher_quote_token_source": proof.get(
                "watcher_quote_token_source",
                transport["watcher_quote_token_source"],
            ),
            "quote_fetch_status": proof.get("quote_fetch_status", "not_fetched"),
        }

    def _watcher_quote_identity(self) -> dict:
        """Return the ACTUAL quote source/base_url/sandbox flag the watcher uses.

        Now reads from _resolve_watcher_quote_url() — the same function that
        _fetch_quotes() uses — so watcher_audit rows always match reality.
        Paper execution_mode no longer implies sandbox quotes.
        """
        return self._current_watcher_quote_proof()

    def _coerce_quote_age_ms(self, value) -> Optional[int]:
        try:
            return max(0, int(round(float(value))))
        except (TypeError, ValueError):
            return None

    def _validate_market_data_preflight(self) -> None:
        transport = self._resolve_watcher_quote_transport()
        base_url = transport["watcher_quote_base_url"]
        md_token = transport["token"]
        if str(getattr(self, "mode", "PAPER")).upper() != "PAPER" or md_token:
            return

        failure_reason = (
            "PAPER_WATCHER_NO_MARKET_DATA_TOKEN mode=PAPER "
            f"base_url={base_url} "
            "watcher quotes require live market data; "
            "set TRADIER_MARKET_DATA_TOKEN or TRADIER_DATA_TOKEN."
        )
        try:
            from ap_health_registry import (
                HEALTH as _WH,
                Criticality as _WC,
                HealthStatus as _WS,
            )
            _WH.ensure_registered("ap_entry_watcher", _WC.HIGH, stale_after_s=45.0)
            _WH.set_status(
                "ap_entry_watcher",
                _WS.FAILED,
                reason=failure_reason,
                metrics={
                    "error_code": "PAPER_WATCHER_NO_MARKET_DATA_TOKEN",
                    "execution_mode": "PAPER",
                    "watcher_quote_base_url": base_url,
                    "watcher_quote_source": "tradier_live",
                    "watcher_sandbox_mode": False,
                    "watcher_quote_token_source": transport["watcher_quote_token_source"],
                },
            )
        except Exception:
            pass

        log.critical("[watcher_quotes] %s", failure_reason)
        raise RuntimeError(failure_reason)

    def _build_watcher_audit_payload(
        self,
        w=None,
        *,
        symbol: str = "",
        score: float = 0.0,
        tier: str = "",
        direction: str = "",
        timeframe: str = "",
        pattern: str = "",
        signal_id: str = "",
        plan_id: str = "",
        trigger_type: str = "",
        signal_entry_price: Optional[float] = None,
        trigger_price: Optional[float] = None,
        stop_price: Optional[float] = None,
        current_bid: float = 0.0,
        current_ask: float = 0.0,
        current_mid: float = 0.0,
        quote_age_ms: Optional[int] = None,
        arm_price: Optional[float] = None,
        arm_condition: str = "",
        stop_condition: str = "",
        reason_code: str = "",
        raw_reason: str = "",
        extra: Optional[dict] = None,
    ) -> dict:
        """Build a structured watcher_audit dict for any block/expire/invalidate path.
        If a WatchedSignal (w) is provided, missing keyword fields are pulled from it.
        Explicit keyword args always override w-derived values.
        Pure dict construction — no I/O, safe to call inside any lock."""
        if w is not None:
            _sig = getattr(w, "signal", {}) or {}
            symbol      = symbol      or getattr(w, "ticker", "")
            score       = score       or float(getattr(w, "score", 0) or 0)
            tier        = tier        or str(getattr(w, "grade", "") or "")
            direction   = direction   or getattr(w, "side", "")
            timeframe   = timeframe   or str(_sig.get("timeframe") or "")
            pattern     = pattern     or str(_sig.get("pattern") or "")
            signal_id   = signal_id   or str(getattr(w, "signal_id", "") or "")
            plan_id     = plan_id     or str(_sig.get("plan_id") or "")
            if signal_entry_price is None:
                signal_entry_price = getattr(w, "entry_trigger", None)
            if stop_price is None:
                stop_price = getattr(w, "stop_level", None)
            if quote_age_ms is None:
                quote_age_ms = self._coerce_quote_age_ms(
                    getattr(w, "last_quote_age_ms", None)
                )
            if not current_bid and not current_ask:
                current_bid = float(getattr(w, "last_quote_bid", 0) or 0)
                current_ask = float(getattr(w, "last_quote_ask", 0) or 0)

        if not current_mid and (current_bid or current_ask):
            current_mid = (
                (current_bid + current_ask) / 2.0
                if (current_bid and current_ask)
                else max(current_bid, current_ask)
            )

        entry_ref = signal_entry_price if signal_entry_price is not None else trigger_price
        dist_trigger_pct: Optional[float] = None
        if entry_ref and current_mid:
            try:
                dist_trigger_pct = round((current_mid - entry_ref) / entry_ref * 100.0, 4)
            except ZeroDivisionError:
                pass
        dist_stop_pct: Optional[float] = None
        if stop_price and current_mid:
            try:
                dist_stop_pct = round((current_mid - stop_price) / stop_price * 100.0, 4)
            except ZeroDivisionError:
                pass

        payload: dict = {
            "symbol":              symbol,
            "score":               score,
            "tier":                tier,
            "direction":           direction,
            "timeframe":           timeframe,
            "pattern":             pattern,
            "signal_id":           signal_id,
            "plan_id":             plan_id,
            "trigger_type":        trigger_type,
            "signal_entry_price":  signal_entry_price,
            "trigger_price":       trigger_price if trigger_price is not None else signal_entry_price,
            "stop_price":          stop_price,
            "current_underlying":  current_mid,
            "current_bid":         current_bid,
            "current_ask":         current_ask,
            "current_mid":         current_mid,
            "quote_age_ms":        quote_age_ms,
            "arm_price":           arm_price,
            "arm_condition":       arm_condition,
            "stop_condition":      stop_condition,
            "distance_to_trigger_pct": dist_trigger_pct,
            "distance_to_stop_pct":    dist_stop_pct,
            "reason_code":         reason_code,
            "raw_reason":          raw_reason,
            "evaluated_at":        datetime.now(timezone.utc).isoformat(),
            # QUOTE-DOMAIN AUDIT: record the ACTUAL quote source the watcher
            # used (read off self.broker, never assumed). This lets us prove
            # whether the watcher evaluated against sandbox or live quotes —
            # the core question behind the paper no-fill investigation.
            **self._current_watcher_quote_proof(),
        }
        if extra:
            # Never allow extra to overwrite protected order fields
            _protected = {"retry_status", "retry_payload"}
            for k, v in extra.items():
                if k not in _protected:
                    payload[k] = v
        return payload

    def _insert_watcher_audit_row(
        self, payload: dict, local_order_id: Optional[str] = None
    ) -> None:
        """Best-effort INSERT into public.watcher_decision_audit for analytics.

        Writes to the NEW watcher_decision_audit table (text client_id, text
        local_order_id) — NOT the legacy public.watcher_audit (UUID fields).
        The legacy table stays untouched and unmigrated.
        orders.meta.watcher_audit remains the primary per-order embedded proof
        and is written by _persist_watcher_audit (unchanged).
        """
        import json as _json_local  # local import keeps watcher importable without ap.db
        try:
            from ap.db import conn as _ap_conn, run_with_retry as _ap_retry  # type: ignore
        except ImportError:
            return

        # Rearm config snapshot at evaluation time
        _rearm_enabled  = WATCHER_REARM_ENABLED
        _rearm_daily    = WATCHER_REARM_ONLY_DAILY_OR_OVERNIGHT
        _rearm_window   = WATCHER_REARM_WINDOW_SEC
        _rearm_min_sc   = WATCHER_REARM_MIN_SCORE
        _rearm_tol      = WATCHER_REARM_TOLERANCE_PCT
        _rearm_max      = WATCHER_REARM_MAX_ATTEMPTS

        # Eligibility breakdown — use payload fields, not method call, to avoid
        # re-evaluating against current env if env changed mid-session.
        _score       = float(payload.get("score") or 0)
        _tier        = str(payload.get("tier") or "").upper()
        _score_ok    = _score >= _rearm_min_sc
        _tier_ok     = _tier == "A"
        # is_rearm_eligible is already in payload for arm_time_rearm_queued events;
        # fall back to evaluating it for other trigger types.
        _eligible    = bool(payload.get("rearm_eligible") or (
            _rearm_enabled and (_score_ok or _tier_ok)
        ))

        # Lifecycle boolean derivation from reason_code
        _reason      = str(payload.get("reason_code") or "")
        _rearmed     = _reason == "rearm_reclaimed"
        _expired     = _reason in {
            "rearm_window_expired",
            "rearm_max_attempts_expired",
            "overnight_too_far_from_trigger",
            "overnight_premarket_breached",
            "overnight_live_quote_unavailable",
        }
        _perm_reject = _reason in {
            "arm_drift",
            "arm_below_stop",          # only when not rearm-eligible
            "osm_validation_failed",
            "dedup_block",
            "opposite_side_conflict",
            "same_side_block",
        } and not _eligible

        # Fetch client_id from OSM if available — not in payload
        _client_id = getattr(
            getattr(self, "order_state_machine", None), "client_id", None
        )

        # Quote identity fields from payload (set by _build_watcher_audit_payload)
        _wq_source  = payload.get("watcher_quote_source")
        _wq_sandbox = payload.get("watcher_sandbox_mode")
        _wq_url     = payload.get("watcher_quote_base_url")

        _sql = """
            INSERT INTO public.watcher_decision_audit (
                local_order_id, signal_id, canonical_signal_id, plan_id, client_id,
                execution_mode, symbol, contract, direction, pattern, timeframe, tier, score,
                decision, reason_code, raw_reason,
                trigger_price, stop_price, target_price,
                current_underlying, current_bid, current_ask, current_mid,
                watcher_quote_source, watcher_sandbox_mode, watcher_quote_base_url,
                quote_age_ms,
                payload
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s,
                %s::jsonb
            )
        """
        _params = (
            local_order_id or None,
            payload.get("signal_id") or None,
            payload.get("canonical_signal_id") or None,
            payload.get("plan_id") or None,
            str(_client_id) if _client_id is not None else None,
            # execution context
            payload.get("execution_mode") or getattr(self, "mode", None),
            payload.get("symbol"),
            payload.get("contract") or payload.get("symbol"),
            payload.get("direction"),
            payload.get("pattern"),
            payload.get("timeframe"),
            payload.get("tier"),
            _score,
            # watcher decision
            payload.get("trigger_type") or payload.get("decision"),
            payload.get("reason_code"),
            payload.get("raw_reason"),
            # prices
            payload.get("trigger_price") or payload.get("signal_entry_price"),
            payload.get("stop_price"),
            payload.get("target_price"),
            payload.get("current_underlying") or payload.get("current_mid"),
            payload.get("current_bid"),
            payload.get("current_ask"),
            payload.get("current_mid"),
            # quote source proof (api.tradier.com after P0 fix)
            _wq_source,
            _wq_sandbox,
            _wq_url,
            # quote freshness: populated from payload if _build_watcher_audit_payload set it
            payload.get("quote_age_ms"),
            # full payload JSONB
            # order_row_id (orders.id bigint) is left NULL here — watcher context
            # only has local_order_id (text).  order_row_id may be backfilled by
            # reconciler or analytics query: SELECT id FROM orders WHERE local_order_id=...
            _json_local.dumps(payload, default=str),
        )

        try:
            def _write():
                with _ap_conn() as _c:
                    _cur = _c.execute(_sql, _params)
                    return getattr(_cur, "rowcount", getattr(_c, "rowcount", None))
            _ap_retry(_write)
        except Exception as _exc:
            log.warning("[watcher_decision_audit] insert failed (non-critical): %s", _exc)

    def _persist_watcher_audit(
        self, local_order_id: Optional[str], payload: dict
    ) -> None:
        """Best-effort merge of watcher_audit into orders.meta.
        Logs with persisted=false when local_order_id is absent or order row not found.
        Does NOT overwrite retry_status, retry_payload, or any existing order meta key
        other than watcher_audit / watcher_audit_history.
        Never raises — audit must never disrupt watcher flow."""
        try:
            # Always attempt a table row — works even without local_order_id.
            # Fails silently if the migration hasn't run yet or ap.db is unavailable.
            self._insert_watcher_audit_row(payload, local_order_id=local_order_id)

            if not local_order_id:
                log.info(
                    "[watcher_audit] no local_order_id — orders.meta skipped | reason=%s | %s",
                    payload.get("reason_code"),
                    json.dumps({**payload, "persisted": False}, default=str),
                )
                return

            osm = getattr(self, "order_state_machine", None)
            if osm is None:
                log.info(
                    "[watcher_audit] no OSM — orders.meta skipped | local_order_id=%s | reason=%s | %s",
                    local_order_id,
                    payload.get("reason_code"),
                    json.dumps({**payload, "persisted": False}, default=str),
                )
                return

            # Retrieve order — try every plausible OSM accessor
            order = None
            for _mname in ("get_order", "get_order_by_local_id", "get", "get_by_local_id"):
                _fn = getattr(osm, _mname, None)
                if callable(_fn):
                    try:
                        order = _fn(local_order_id)
                        if order is not None:
                            break
                    except Exception:
                        continue

            if order is None:
                log.info(
                    "[watcher_audit] order not found in OSM — orders.meta skipped | "
                    "local_order_id=%s | reason=%s",
                    local_order_id,
                    payload.get("reason_code"),
                )
                return

            # Build the patch — ONLY watcher_audit and watcher_audit_history.
            # Never read the full existing meta and pass it back into DB.
            # That pattern risks overwriting concurrent fields (retry_status,
            # retry_payload, submit_refresh evidence) with a stale snapshot.
            if isinstance(order, dict):
                _existing_meta = order.get("meta") or {}
            else:
                _existing_meta = getattr(order, "meta", None) or {}
            _existing_meta = _existing_meta if isinstance(_existing_meta, dict) else {}
            _history = list(_existing_meta.get("watcher_audit_history") or [])
            _history.append(payload)
            _patch = {
                "watcher_audit":         payload,
                "watcher_audit_history": _history[-5:],
            }

            # Try every plausible OSM meta-update method (patch dict only)
            _persisted = False
            for _mname in ("update_order_meta", "patch_meta", "set_meta", "update_meta"):
                _fn = getattr(osm, _mname, None)
                if callable(_fn):
                    try:
                        _result = _fn(local_order_id, _patch)
                        # update_order_meta returns bool; older shims may return None
                        _persisted = bool(_result) if _result is not None else True
                        break
                    except Exception as _me:
                        log.debug(
                            "[watcher_audit] %s(%s) failed: %s", _mname, local_order_id, _me
                        )

            # Direct SQL fallback — for deployments where the OSM predates
            # update_order_meta.  Conditional import keeps ap_entry_watcher
            # importable in test / CI environments without ap.db.
            # Writes only _patch (not full meta). Uses COALESCE so NULL meta
            # rows are handled safely.  Rowcount is read from the cursor, not
            # from a fallback that would turn 0 into 1.
            if not _persisted:
                try:
                    from ap.db import conn as _ap_conn, run_with_retry as _ap_retry  # type: ignore
                    _client_id = getattr(osm, "client_id", "")
                    _patch_json = json.dumps(_patch, default=str)
                    if _client_id:
                        _fb_sql = (
                            "UPDATE orders "
                            "SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb, "
                            "    updated_ts = NOW() "
                            "WHERE local_order_id = %s AND client_id = %s"
                        )
                        _fb_params = (_patch_json, local_order_id, _client_id)
                    else:
                        _fb_sql = (
                            "UPDATE orders "
                            "SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb, "
                            "    updated_ts = NOW() "
                            "WHERE local_order_id = %s"
                        )
                        _fb_params = (_patch_json, local_order_id)

                    def _update_fn():
                        with _ap_conn() as _c:
                            _cur = _c.execute(_fb_sql, _fb_params)
                            return getattr(_cur, "rowcount", getattr(_c, "rowcount", None))

                    _rowcount = _ap_retry(_update_fn)
                    _persisted = bool(_rowcount and _rowcount > 0)
                except ImportError:
                    pass  # ap.db not available (test env / CI) — fall through to log
                except Exception as _sql_exc:
                    log.debug("[watcher_audit] direct SQL fallback failed: %s", _sql_exc)

            if _persisted:
                log.debug(
                    "[watcher_audit] persisted | local_order_id=%s reason=%s",
                    local_order_id,
                    payload.get("reason_code"),
                )
            else:
                log.info(
                    "[watcher_audit] OSM meta update not available — logging payload | "
                    "local_order_id=%s | %s",
                    local_order_id,
                    json.dumps({**payload, "persisted": False}, default=str),
                )
        except Exception as _exc:
            log.warning("[watcher_audit] persist failed (non-critical): %s", _exc)

    # ── P0-W2: Re-arm eligibility + reclaim poll ─────────────────────────────

    def _is_rearm_eligible(
        self,
        score: float,
        tier: str,
        is_overnight_or_daily: bool,
    ) -> bool:
        """Return True when a wrong-side-of-stop arm failure should enter DISARMED
        wait mode instead of permanently rejecting the signal.

        Eligibility requires ALL of:
          - WATCHER_REARM_ENABLED=1 (env default: off — must be explicitly enabled)
          - score >= WATCHER_REARM_MIN_SCORE OR tier is 'A'
          - if WATCHER_REARM_ONLY_DAILY_OR_OVERNIGHT: signal must be overnight or daily

        Drift-stale rejects are NEVER eligible — those mean price has moved
        decisively away from trigger, not temporarily beyond the stop.
        """
        if not WATCHER_REARM_ENABLED:
            return False
        score_ok = score >= WATCHER_REARM_MIN_SCORE
        tier_ok = str(tier or "").upper() == "A"
        if not (score_ok or tier_ok):
            return False
        if WATCHER_REARM_ONLY_DAILY_OR_OVERNIGHT and not is_overnight_or_daily:
            return False
        return True

    def _check_rearm_signals(self) -> None:
        """Poll all DISARMED_WAITING_FOR_RECLAIM signals. Called from _check_all()
        during market hours (after the pre-market gate, so never before 9:30 ET).

        For each rearm signal:
          - Timeout elapsed → expire, fire on_expire, clean from pending.
          - Price reclaimed valid side + tolerance → flip rearm_mode=False so normal
            poll picks it up next cycle. Log + audit the reclaim.
          - Otherwise → log debug, continue waiting.

        All state changes are written to watcher_audit via _persist_watcher_audit."""
        with self._lock:
            rearm_signals = [
                w for w in self._pending
                if getattr(w, "rearm_mode", False) and w.state == WatchState.PENDING
            ]
        if not rearm_signals:
            return

        tickers = list({w.ticker for w in rearm_signals})
        try:
            quotes = self._fetch_quotes(tickers)
        except Exception as exc:
            log.warning("[rearm] Quote fetch failed: %s", exc)
            return

        now = datetime.now(timezone.utc)
        to_expire: list = []
        to_rearm: list = []

        for w in rearm_signals:
            # ── Timeout check ──────────────────────────────────────────────
            if w.rearm_expires_at and now >= w.rearm_expires_at:
                to_expire.append(w)
                continue

            quote = quotes.get(w.ticker)
            if not quote:
                continue

            bid = float(quote.get("bid", 0) or 0)
            ask = float(quote.get("ask", 0) or 0)
            w.last_quote_age_ms = self._coerce_quote_age_ms(quote.get("quote_age_ms"))
            if bid == 0 and ask == 0:
                last = float(quote.get("last", 0) or 0)
                bid = ask = last
            mid = (bid + ask) / 2.0 if (bid and ask) else max(bid, ask)
            if mid <= 0:
                continue

            # ── Direction-aware reclaim check ──────────────────────────────
            # CALL: was disarmed because mid < call_stop (dropped below support).
            #   Reclaim: mid rises back above stop + tolerance buffer.
            # PUT:  was disarmed because mid > put_stop (rallied above resistance).
            #   Reclaim: mid falls back below stop - tolerance buffer.
            reclaimed = False
            reclaim_threshold = 0.0
            if w.stop_level:
                if w.side == "CALL":
                    reclaim_threshold = w.stop_level * (1.0 + WATCHER_REARM_TOLERANCE_PCT)
                    reclaimed = mid >= reclaim_threshold
                else:  # PUT
                    reclaim_threshold = w.stop_level * (1.0 - WATCHER_REARM_TOLERANCE_PCT)
                    reclaimed = mid <= reclaim_threshold

            if reclaimed:
                to_rearm.append((w, bid, ask, mid, reclaim_threshold))
            else:
                remaining = (
                    max(0.0, (w.rearm_expires_at - now).total_seconds())
                    if w.rearm_expires_at else 0.0
                )
                log.debug(
                    "[%s] REARM_WAIT | side=%s mid=%.4f stop=%.4f "
                    "reclaim_threshold=%.4f window_remaining=%.0fs",
                    w.ticker, w.side, mid,
                    w.stop_level or 0.0,
                    reclaim_threshold,
                    remaining,
                )

        # ── Process expirations — state change inside lock, I/O outside ───
        expired_signals: list = []
        with self._lock:
            for w in to_expire:
                if getattr(w, "rearm_mode", False) and w.state == WatchState.PENDING:
                    w.rearm_mode = False
                    w.state = WatchState.EXPIRED
                    expired_signals.append(w)

        for w in expired_signals:
            _exp_audit = self._build_watcher_audit_payload(
                w,
                trigger_type="rearm_check",
                reason_code="rearm_window_expired",
                raw_reason=(
                    f"rearm_window_{WATCHER_REARM_WINDOW_SEC}s_elapsed_no_reclaim"
                    f"_side_{w.side}_stop_{w.stop_level:.4f}"
                    if w.stop_level else f"rearm_window_{WATCHER_REARM_WINDOW_SEC}s_elapsed"
                ),
                extra={
                    "rearm_count":       w.rearm_count,
                    "rearm_reason":      w.rearm_reason,
                    "rearm_window_sec":  WATCHER_REARM_WINDOW_SEC,
                },
            )
            self._persist_watcher_audit(w.signal.get("local_order_id"), _exp_audit)
            _sig_id = str(w.signal.get("signal_id", ""))
            if _sig_id and w.ticker:
                _ew_record(_sig_id, w.ticker, "EXPIRED", "rearm_window_expired")
            log.info(
                "[%s] REARM_EXPIRED — no reclaim within %ds | side=%s stop=%s | expiring",
                w.ticker, WATCHER_REARM_WINDOW_SEC, w.side,
                f"{w.stop_level:.4f}" if w.stop_level else "none",
            )
            self._dispatch_completion(
                w, self.on_expire or (lambda _w: None),
                pre_computed_audit=_exp_audit,
            )

        # ── Process reclaims — flip inside lock, audit+log outside ────────
        rearmed_signals: list = []
        _max_attempt_expired: list = []  # signals that hit limit during reclaim evaluation
        with self._lock:
            for w, bid, ask, mid, reclaim_threshold in to_rearm:
                if getattr(w, "rearm_mode", False) and w.state == WatchState.PENDING:
                    # Guard: if already at max rearm attempts, expire instead of re-arming
                    if w.rearm_count >= WATCHER_REARM_MAX_ATTEMPTS:
                        w.rearm_mode = False
                        w.state = WatchState.EXPIRED
                        _max_attempt_expired.append(w)   # separate list — cleanup runs below
                        log.info(
                            "[%s] REARM_MAX_ATTEMPTS reached (%d) — expiring on reclaim",
                            w.ticker, WATCHER_REARM_MAX_ATTEMPTS,
                        )
                        continue
                    w.rearm_mode = False
                    w.rearm_count += 1
                    w.last_quote_bid = bid
                    w.last_quote_ask = ask
                    w.last_quote_age_ms = self._coerce_quote_age_ms(quote.get("quote_age_ms"))
                    rearmed_signals.append((w, bid, ask, mid, reclaim_threshold))

        # Max-attempts expiry: callbacks + pending cleanup.
        # Must run after the lock is released; separate from the timeout expiry path
        # because expired_signals cleanup already ran above.
        for w in _max_attempt_expired:
            _max_audit = self._build_watcher_audit_payload(
                w,
                trigger_type="rearm_check",
                reason_code="rearm_max_attempts_expired",
                raw_reason=(
                    f"rearm_count_{w.rearm_count}_reached_max_{WATCHER_REARM_MAX_ATTEMPTS}"
                    f"_on_reclaim_side_{w.side}"
                ),
                extra={
                    "rearm_count":        w.rearm_count,
                    "rearm_max_attempts": WATCHER_REARM_MAX_ATTEMPTS,
                    "rearm_reason":       w.rearm_reason,
                },
            )
            self._persist_watcher_audit(w.signal.get("local_order_id"), _max_audit)
            _sig_id = str(w.signal.get("signal_id", ""))
            if _sig_id and w.ticker:
                _ew_record(_sig_id, w.ticker, "EXPIRED", "rearm_max_attempts_expired")
            log.info(
                "[%s] REARM_MAX_ATTEMPTS_EXPIRED | side=%s stop=%s | dispatching expiry",
                w.ticker, w.side,
                f"{w.stop_level:.4f}" if w.stop_level else "none",
            )
            self._dispatch_completion(
                w, self.on_expire or (lambda _w: None),
                pre_computed_audit=_max_audit,
            )

        for w, bid, ask, mid, reclaim_threshold in rearmed_signals:
            _reclaim_dir = "above" if w.side == "CALL" else "below"
            _rearm_audit = self._build_watcher_audit_payload(
                w,
                trigger_type="rearm_check",
                current_bid=bid,
                current_ask=ask,
                current_mid=mid,
                arm_condition=f"trigger_{w.entry_trigger:.4f}" if w.entry_trigger else "",
                stop_condition=(
                    f"mid_{mid:.4f}_{_reclaim_dir}_reclaim_threshold_{reclaim_threshold:.4f}"
                ),
                reason_code="rearm_reclaimed",
                raw_reason=(
                    f"mid_{mid:.4f}_reclaimed_{_reclaim_dir}"
                    f"_stop_{w.stop_level:.4f}_tol_{WATCHER_REARM_TOLERANCE_PCT:.4f}"
                    if w.stop_level else f"mid_{mid:.4f}_reclaimed"
                ),
                extra={
                    "rearm_count":       w.rearm_count,
                    "rearm_reason":      w.rearm_reason,
                    "reclaim_mid":       mid,
                    "reclaim_threshold": reclaim_threshold,
                    "stop_level":        w.stop_level,
                    "rearm_tolerance":   WATCHER_REARM_TOLERANCE_PCT,
                },
            )
            self._persist_watcher_audit(w.signal.get("local_order_id"), _rearm_audit)
            log.info(
                "[%s] REARM_RECLAIMED — price returned to valid side | side=%s "
                "mid=%.4f stop=%.4f threshold=%.4f rearm_count=%d | "
                "arming for breach detection",
                w.ticker, w.side, mid,
                w.stop_level or 0.0,
                reclaim_threshold,
                w.rearm_count,
            )

    def _opposite_conflict_applies(self, watched, opposite) -> bool:
        """Return whether legacy opposite-direction arbitration applies.

        The package watcher overrides this seam for exact client/mode ownership
        and healthy pre-breach co-arming.  The legacy default preserves the
        existing ticker-level behavior for callers that do not opt into that
        hardening shim.
        """
        return True

    def _same_side_conflict_applies(self, watched, same_side_watcher) -> bool:
        """Return whether legacy same-side arbitration applies."""
        return True

    def add_signal(
        self, signal: dict, *, registration_provenance_out: Optional[dict] = None,
    ) -> bool:
        # PR #421 final amendment (P0-1): reset the caller's per-call
        # provenance output BEFORE any possible return path, including
        # every rejection below. Provenance is call-local — never shared
        # mutable self state, which a second concurrent caller could
        # overwrite between this caller's registration and its read of
        # the result.
        if registration_provenance_out is not None:
            registration_provenance_out["created_by_this_call"] = False
            registration_provenance_out["registration_token"] = None

        now_et = datetime.now(ET)
        post_session = (
            now_et.hour > OVERNIGHT_THRESHOLD_HOUR
            or (now_et.hour == OVERNIGHT_THRESHOLD_HOUR and now_et.minute >= OVERNIGHT_THRESHOLD_MIN)
        )
        pre_market = now_et.hour < 9 or (now_et.hour == 9 and now_et.minute < 30)
        overnight = post_session or pre_market

        if overnight:
            session_label = "Pre-market" if pre_market else "Post-session"
            log.info(
                "[%s] %s signal accepted — holding for next regular session open. trigger=$%s",
                signal.get("ticker"),
                session_label,
                signal.get("entry_price") or (signal.get("trigger") or {}).get("entry", "?"),
            )

        watched = WatchedSignal(signal, overnight=overnight)
        watched._watcher_ref = self
        watched.deferred_retry_not_before = signal.get("deferred_retry_not_before")

        local_order_id = watched.signal.get("local_order_id")
        if not self._validate_local_order_id(local_order_id):
            log.error(
                "[%s] WATCH_ARM_BLOCKED — local_order_id=%s does not exist in OSM",
                watched.ticker,
                local_order_id,
            )
            _osm_audit = self._build_watcher_audit_payload(
                watched,
                trigger_type="add_signal_block",
                reason_code="osm_validation_failed",
                raw_reason=f"local_order_id={local_order_id}_not_in_osm",
            )
            self._persist_watcher_audit(local_order_id, _osm_audit)
            # PR-C: every self._last_reject_reason write goes under the
            # watcher lock so concurrent add_signal() callers cannot race
            # on the diagnostic field.
            with self._lock:
                self._last_reject_reason = "osm_validation_failed"
            return False

        dedup_key = self._dedup_key_for_signal(watched.signal)
        if not dedup_key:
            log.warning(
                "[%s] add_signal: signal_id missing — dedup disabled for this signal. "
                "Duplicate arms possible.", watched.ticker
            )

        with self._lock:
            if dedup_key and dedup_key in self._dedup_set:
                log.info(
                    "[%s] DEDUP_BLOCK — signal_id=%s is already armed in watcher",
                    watched.ticker,
                    dedup_key,
                )
                _dedup_audit = self._build_watcher_audit_payload(
                    watched,
                    trigger_type="add_signal_block",
                    reason_code="dedup_block",
                    raw_reason=f"signal_id_{dedup_key}_already_armed",
                    extra={"dedup_key": dedup_key, "persisted": False},
                )
                log.info(
                    "[watcher_audit] dedup_block | local_order_id=%s | %s",
                    watched.signal.get("local_order_id"),
                    json.dumps(_dedup_audit, default=str),
                )
                self._last_reject_reason = "dedup_block"
                return False

            # Conflict detection must include rearm_mode signals.
            # A DISARMED_WAITING_FOR_RECLAIM signal is still a live position attempt
            # with an open OSM order. Treating it as invisible (is_active=False)
            # would allow a new opposite-side signal to arm alongside it, and if
            # the disarmed signal later reclaims, both would be active simultaneously.
            # Using (w.is_active or w.rearm_mode) ensures rearm signals participate
            # in the same scoring and cancellation logic as normal signals.
            same_side = [
                w
                for w in self._pending
                if (w.is_active or getattr(w, "rearm_mode", False))
                and w.ticker == watched.ticker
                and w.side == watched.side
                and self._same_side_conflict_applies(watched, w)
            ]
            opposite_side = [
                w
                for w in self._pending
                if (w.is_active or getattr(w, "rearm_mode", False))
                and w.ticker == watched.ticker
                and w.side != watched.side
                and self._opposite_conflict_applies(watched, w)
            ]

            # Never keep both CALL and PUT armed for the same ticker. Stronger
            # score wins. Equal/lower score gets blocked to avoid OSM conflict.
            #
            # P0 PR smart-eligibility: prune the opposite_side candidate set
            # before the conflict decision. A stale or non-active opposite
            # should NOT block a fresh signal. We keep the legacy stronger-
            # score-wins / flip semantics; we only refine what counts as a
            # valid opposite to compare against.
            if opposite_side:
                # Same-symbol opposite filter — already guaranteed by the
                # ticker check above, kept as a documented invariant.
                _now_dt = datetime.now(timezone.utc)
                _fresh_opps = []
                _stale_opps_ignored = []
                for _opp in opposite_side:
                    _opp_age_sec = max(0.0, (_now_dt - _opp.created_at).total_seconds())
                    _opp_is_terminal = _opp.state in (
                        WatchState.CANCELLED,
                        WatchState.EXPIRED,
                        WatchState.INVALIDATED,
                    )
                    _opp_is_rearm_only = (
                        getattr(_opp, "rearm_mode", False) and not _opp.is_active
                    )
                    if _opp_is_terminal:
                        _stale_opps_ignored.append(
                            (_opp, "opp_in_terminal_state", _opp_age_sec)
                        )
                        continue
                    if (_opp_age_sec > OPPOSITE_CONFLICT_MAX_AGE_SEC
                            and _opp.score <= watched.score):
                        # Stale opposite that is not stronger — ignore.
                        _stale_opps_ignored.append(
                            (_opp, "opp_stale_not_stronger", _opp_age_sec)
                        )
                        continue
                    if (_opp_is_rearm_only and not getattr(watched, "rearm_mode", False)
                            and watched.score >= _opp.score):
                        # Fresh active beats rearm-only at equal/higher score.
                        _stale_opps_ignored.append(
                            (_opp, "opp_rearm_only_weaker", _opp_age_sec)
                        )
                        continue
                    _fresh_opps.append(_opp)

                if _stale_opps_ignored:
                    # P1 (Codex review): a pruned-as-stale opposite watcher
                    # MUST be cancelled and removed from _pending before we
                    # admit the new signal; otherwise both CALL and PUT can
                    # remain armed for the same ticker, leaving OSM rows
                    # stuck in PENDING_TRIGGER. The pruning was the
                    # admission decision; this loop enacts the bookkeeping.
                    _opps_to_drop = []
                    for _opp, _why, _age in _stale_opps_ignored:
                        _was_terminal = _opp.state in (
                            WatchState.CANCELLED,
                            WatchState.EXPIRED,
                            WatchState.INVALIDATED,
                        )
                        if _was_terminal:
                            # Already in a terminal state — no OSM call
                            # needed; just make sure it is not still in
                            # _pending (defensive: poll-loop usually purges
                            # terminal watchers but we cannot rely on timing).
                            _action = "ignored_terminal"
                            _opps_to_drop.append(_opp)
                        else:
                            # Still PENDING / WATCHING etc. — actively
                            # cancel so OSM PENDING_TRIGGER is released.
                            _opp.state = WatchState.CANCELLED
                            _opp._release_dedup_key()
                            _opps_to_drop.append(_opp)
                            _local_oid = (_opp.signal or {}).get("local_order_id")
                            _osm = self.order_state_machine
                            _skip_cancel_recovery = bool(signal.get("__recovery_rearm"))
                            if _local_oid and _osm and hasattr(_osm, "cancel_pending_entry") and not _skip_cancel_recovery:
                                try:
                                    _cancel_ok = _osm.cancel_pending_entry(
                                        _local_oid,
                                        reason="opposite_side_replaced_stale_or_weaker",
                                    )
                                    if not _cancel_ok:
                                        log.error(
                                            "[%s] opposite_side_replaced_stale_or_weaker: "
                                            "OSM cancel_pending_entry returned False for "
                                            "local_order_id=%s reason=%s opp_side=%s "
                                            "opp_score=%.1f — OSM row may be stuck in "
                                            "PENDING_TRIGGER.",
                                            watched.ticker, _local_oid, _why,
                                            _opp.side, _opp.score,
                                        )
                                except Exception as _exc:
                                    log.warning(
                                        "[%s] OSM cancel raised during "
                                        "opposite_side_replaced_stale_or_weaker for "
                                        "local_order_id=%s: %s",
                                        watched.ticker, _local_oid, _exc,
                                    )
                            _action = (
                                "cancelled_rearm_weaker"
                                if _why == "opp_rearm_only_weaker"
                                else "cancelled_stale_active"
                            )

                        # Structured log line per ignored opposite (audit).
                        log.info(
                            "[%s] OPPOSITE_IGNORED %s | action=%s opp_side=%s "
                            "opp_score=%.1f opp_age=%.0fs opp_state=%s new_side=%s "
                            "new_score=%.1f",
                            watched.ticker, _why, _action, _opp.side, _opp.score,
                            _age, str(_opp.state), watched.side, watched.score,
                        )

                        # Best-effort audit stamp onto the cancelled opposite's
                        # order row so the post-mortem can reconstruct WHY a
                        # stale row was cancelled by an opposite signal.
                        if _action != "ignored_terminal":
                            try:
                                _opp_audit_replaced = self._build_watcher_audit_payload(
                                    _opp,
                                    trigger_type="opposite_side_replaced",
                                    reason_code="opposite_side_replaced_stale_or_weaker",
                                    raw_reason=(
                                        f"{_why}_replaced_by_{watched.side}_{watched.score:.1f}"
                                    ),
                                    extra={
                                        "block_stage":              "watcher",
                                        "block_reason":             "opposite_side_replaced_stale_or_weaker",
                                        "symbol":                   watched.ticker,
                                        "replacing_local_order_id": (watched.signal or {}).get("local_order_id"),
                                        "replacing_direction":      watched.side,
                                        "replacing_score":          watched.score,
                                        "replacing_signal_id":      watched.signal_id,
                                        "replaced_local_order_id":  (_opp.signal or {}).get("local_order_id"),
                                        "replaced_direction":       _opp.side,
                                        "replaced_score":           _opp.score,
                                        "replaced_signal_id":       _opp.signal_id,
                                        "replaced_age_seconds":     _age,
                                        "replaced_state":           str(_opp.state),
                                        "decision_rule":            _why,
                                        "ignored_opposite_action":  _action,
                                    },
                                )
                                _replaced_oid = (_opp.signal or {}).get("local_order_id")
                                if _replaced_oid:
                                    try:
                                        self._persist_watcher_audit(_replaced_oid, _opp_audit_replaced)
                                    except Exception as _persist_exc:
                                        log.debug(
                                            "[%s] persist audit on replaced opposite "
                                            "failed (best-effort): %s",
                                            watched.ticker, _persist_exc,
                                        )
                            except Exception as _audit_exc:
                                log.debug(
                                    "[%s] build audit on replaced opposite "
                                    "failed (best-effort): %s",
                                    watched.ticker, _audit_exc,
                                )

                    # Remove all dropped opposites from _pending (under the
                    # outer self._lock that already wraps this whole block).
                    if _opps_to_drop:
                        self._pending = [
                            w for w in self._pending if w not in _opps_to_drop
                        ]

                # If pruning eliminated all opposites, admit normally.
                if not _fresh_opps:
                    opposite_side = []

            if opposite_side:
                best_opp = max(_fresh_opps if '_fresh_opps' in dir() and _fresh_opps else opposite_side,
                               key=lambda w: w.score)
                # Equal-score tie-break: prefer higher-tier timeframe.
                _opp_score = best_opp.score
                _new_score = watched.score
                _scores_tied = abs(_new_score - _opp_score) <= OPPOSITE_CONFLICT_SCORE_TIE_PCT * max(1.0, _opp_score)
                if _scores_tied:
                    _opp_tf = (best_opp.signal or {}).get("timeframe", "")
                    _new_tf = (watched.signal or {}).get("timeframe", "")
                    _opp_tier = _opposite_tf_rank(_opp_tf)
                    _new_tier = _opposite_tf_rank(_new_tf)
                    # If new has strictly higher tier, treat as winner.
                    if _new_tier > _opp_tier:
                        _new_score = _opp_score + 0.01  # tip the scales for the > check below
                if _new_score > best_opp.score:
                    for w in opposite_side:
                        w.state = WatchState.CANCELLED
                        w._release_dedup_key()
                        log.info(
                            "[%s] SAFE_MODE_DIRECTION_FLIP — cancelling %s score=%.1f "
                            "for stronger %s score=%.1f",
                            watched.ticker,
                            w.side,
                            w.score,
                            watched.side,
                            watched.score,
                        )
                        _local_oid = w.signal.get("local_order_id")
                        _skip_cancel_dir_flip = bool(signal.get("__recovery_rearm"))
                        if _local_oid and self.order_state_machine and hasattr(self.order_state_machine, "cancel_pending_entry") and not _skip_cancel_dir_flip:
                            try:
                                _cancel_ok = self.order_state_machine.cancel_pending_entry(
                                    _local_oid, reason="direction_flip_watcher_cancel"
                                )
                                if not _cancel_ok:
                                    log.error(
                                        "[%s] direction_flip: cancel_pending_entry returned False "
                                        "for local_order_id=%s — OSM row may be stuck in "
                                        "PENDING_TRIGGER. Investigate immediately.",
                                        w.ticker, _local_oid,
                                    )
                            except Exception as _exc:
                                log.warning("[%s] OSM cancel failed for direction_flip: %s", w.ticker, _exc)
                    self._pending = [w for w in self._pending if w not in opposite_side]
                else:
                    # P0 PR — structured opposite_side_conflict audit.
                    # The block is the SAFE choice; we preserve the legacy
                    # "weaker setup loses to stronger opposite" semantic but
                    # add the conflicting-signal fields so the post-mortem
                    # can answer: was this correct? was the opposite stale?
                    # was it a tie that timeframe-tier should have flipped?
                    _opp_age_sec = max(
                        0.0,
                        (datetime.now(timezone.utc) - best_opp.created_at).total_seconds(),
                    )
                    _opp_signal = best_opp.signal or {}
                    _new_signal = watched.signal or {}
                    _decision_rule = (
                        "opp_stronger_fresh" if best_opp.score > watched.score
                        else (
                            "opp_higher_tf_tier_on_tie"
                            if _scores_tied
                            else "opp_equal_or_higher_score_blocks"
                        )
                    )
                    _opp_audit = self._build_watcher_audit_payload(
                        watched,
                        trigger_type="add_signal_block",
                        reason_code="opposite_side_conflict",
                        raw_reason=(
                            f"blocked_{watched.side}_{watched.score:.1f}"
                            f"_existing_{best_opp.side}_{best_opp.score:.1f}"
                        ),
                        extra={
                            # Required structured fields (P0 PR spec):
                            "block_stage":              "watcher",
                            "block_reason":             "opposite_side_conflict",
                            "symbol":                   watched.ticker,
                            "current_local_order_id":   _new_signal.get("local_order_id"),
                            "current_direction":        watched.side,
                            "current_score":            watched.score,
                            "current_timeframe":        _new_signal.get("timeframe"),
                            "current_pattern":          _new_signal.get("pattern"),
                            "current_signal_id":        watched.signal_id,
                            "conflicting_local_order_id": _opp_signal.get("local_order_id"),
                            "conflicting_direction":    best_opp.side,
                            "conflicting_score":        best_opp.score,
                            "conflicting_timeframe":    _opp_signal.get("timeframe"),
                            "conflicting_pattern":      _opp_signal.get("pattern"),
                            "conflicting_signal_id":    best_opp.signal_id,
                            "conflicting_state":        str(best_opp.state),
                            "conflict_age_seconds":     _opp_age_sec,
                            "conflict_scope":           "same_symbol_opposite_direction",
                            "decision_rule":            _decision_rule,
                            "score_tie_window_pct":     OPPOSITE_CONFLICT_SCORE_TIE_PCT,
                            "max_age_threshold_sec":    OPPOSITE_CONFLICT_MAX_AGE_SEC,
                            "stale_opps_ignored_count": len(_stale_opps_ignored) if '_stale_opps_ignored' in dir() else 0,
                            # Back-compat keys (old dashboard / replay scripts):
                            "blocked_side":    watched.side,
                            "blocked_score":   watched.score,
                            "existing_side":   best_opp.side,
                            "existing_score":  best_opp.score,
                            "persisted":       False,
                        },
                    )
                    log.info(
                        "[watcher_audit] opposite_side_conflict | local_order_id=%s | %s",
                        watched.signal.get("local_order_id"),
                        json.dumps(_opp_audit, default=str),
                    )
                    self._last_reject_reason = "opposite_side_conflict"
                    log.info(
                        "[%s] SAFE_MODE_BLOCK_OPPOSITE — keeping existing %s score=%.1f, "
                        "blocking new %s score=%.1f",
                        watched.ticker,
                        best_opp.side,
                        best_opp.score,
                        watched.side,
                        watched.score,
                    )
                    return False

            # Same-side dedup/replacement by score.
            if same_side:
                best_same = max(same_side, key=lambda w: w.score)
                if watched.score > best_same.score:
                    for w in same_side:
                        w.state = WatchState.CANCELLED
                        w._release_dedup_key()
                        log.info(
                            "[%s] SAME_SIDE_REPLACE — cancelling %s score=%.1f "
                            "for stronger same-side score=%.1f",
                            watched.ticker,
                            w.side,
                            w.score,
                            watched.score,
                        )
                        _local_oid = w.signal.get("local_order_id")
                        if _local_oid and self.order_state_machine and hasattr(self.order_state_machine, "cancel_pending_entry") and not bool(signal.get("__recovery_rearm")):
                            try:
                                _cancel_ok = self.order_state_machine.cancel_pending_entry(
                                    _local_oid, reason="same_side_replace_watcher_cancel"
                                )
                                if not _cancel_ok:
                                    log.error(
                                        "[%s] same_side_replace: cancel_pending_entry returned False "
                                        "for local_order_id=%s — OSM row may be stuck in "
                                        "PENDING_TRIGGER. Investigate immediately.",
                                        w.ticker, _local_oid,
                                    )
                            except Exception as _exc:
                                log.warning("[%s] OSM cancel failed for same_side_replace: %s", w.ticker, _exc)
                    self._pending = [w for w in self._pending if w not in same_side]
                else:
                    _ss_audit = self._build_watcher_audit_payload(
                        watched,
                        trigger_type="add_signal_block",
                        reason_code="same_side_block",
                        raw_reason=(
                            f"blocked_{watched.side}_{watched.score:.1f}"
                            f"_existing_same_side_{best_same.score:.1f}"
                        ),
                        extra={
                            "blocked_score":   watched.score,
                            "existing_score":  best_same.score,
                            "side":            watched.side,
                            "persisted":       False,
                        },
                    )
                    log.info(
                        "[watcher_audit] same_side_block | local_order_id=%s | %s",
                        watched.signal.get("local_order_id"),
                        json.dumps(_ss_audit, default=str),
                    )
                    self._last_reject_reason = "same_side_block"
                    log.info(
                        "[%s] SAME_SIDE_BLOCK — keeping %s score=%.1f, "
                        "blocking weaker same-side score=%.1f",
                        watched.ticker,
                        watched.side,
                        best_same.score,
                        watched.score,
                    )
                    return False

            if dedup_key:
                self._dedup_set.add(dedup_key)

            self._pending.append(watched)
            # PR #421 final amendment (P0-1): this is the exact, sole
            # point a new WatchedSignal registration is committed to the
            # registry. Provenance must be set here, from the object this
            # call itself just created and inserted — never rediscovered
            # afterward by scanning _pending for a logical-identity match,
            # which cannot distinguish "I created this" from "I merely
            # observed this."
            if registration_provenance_out is not None:
                registration_provenance_out["created_by_this_call"] = True
                registration_provenance_out["registration_token"] = (
                    watched._registration_token
                )

            # P0-W2: consume rearm marker placed by watch() arm-time path.
            # The marker is a private key in watched.signal (which IS signal_dict
            # by reference). Pop it so it never propagates downstream.
            _rearm_at_arm = bool(watched.signal.pop("__watcher_rearm_pending", False))
            _rearm_arm_reason = str(watched.signal.pop("__watcher_rearm_reason", "") or "")
            if _rearm_at_arm:
                watched.rearm_mode = True
                watched.rearm_reason = _rearm_arm_reason
                watched.signal["queue_status"] = "DISARMED_WAITING_FOR_RECLAIM"
                # Overnight-aware expiry: if signal is overnight/daily and we are
                # pre-market, start the window from 9:30 ET open, not right now,
                # so the signal is not silently expired before quotes are available.
                _rearm_window = timedelta(seconds=WATCHER_REARM_WINDOW_SEC)
                _now_utc = datetime.now(timezone.utc)
                if watched.overnight or _safe_is_daily_signal(watched):
                    try:
                        _market_open_et = datetime.now(ET).replace(
                            hour=9, minute=30, second=0, microsecond=0
                        ).astimezone(timezone.utc)
                        if _now_utc < _market_open_et:
                            watched.rearm_expires_at = _market_open_et + _rearm_window
                        else:
                            watched.rearm_expires_at = _now_utc + _rearm_window
                    except Exception:
                        watched.rearm_expires_at = _now_utc + _rearm_window
                else:
                    watched.rearm_expires_at = _now_utc + _rearm_window
                log.info(
                    "[%s] DISARMED_WAITING_FOR_RECLAIM | side=%s score=%.1f tier=%s | "
                    "reason=%s | rearm_window=%ds | expires=%s UTC",
                    watched.ticker,
                    watched.side,
                    watched.score,
                    watched.grade,
                    _rearm_arm_reason,
                    WATCHER_REARM_WINDOW_SEC,
                    watched.rearm_expires_at.strftime("%H:%M:%S") if watched.rearm_expires_at else "?",
                )

            overnight_count = sum(1 for w in self._pending if w.overnight and w.is_active)
            same_day_count = sum(1 for w in self._pending if not w.overnight and w.is_active)
            active_total = sum(1 for w in self._pending if w.is_active)
            rearm_total = sum(1 for w in self._pending if getattr(w, "rearm_mode", False))

        log.info(
            "[%s] Added to watch queue — %d same-day + %d overnight = %d active | %d rearm-wait",
            watched.ticker,
            same_day_count,
            overnight_count,
            active_total,
            rearm_total,
        )
        return True

    def watch(
        self,
        plan,
        local_order_id: str,
        *,
        recovery_rearm: bool = False,
        no_cancel_on_reject: bool = False,
        materialization_resume: bool = False,
        registration_provenance_out: Optional[dict] = None,
    ) -> bool:
        """Plan-aware entrypoint called by queue/execution orchestration.

        recovery_rearm=True / no_cancel_on_reject=True — safe recovery mode:
          Used by the morning handoff audit to re-arm watcher ownership for
          DB rows that survived a process restart. In this mode:
            - Staleness / drift / below-stop rejections are skipped entirely
              (the row's contract, qty, limit_price, status are NOT changed)
            - add_signal() cancel_pending_entry calls are suppressed
            - The watch() cancel_pending_entry call at the end is suppressed
            - Only watcher audit metadata is written

        registration_provenance_out — PR #421 final amendment (P0-1):
          optional per-call, caller-owned dict. If supplied, is reset to
          {"created_by_this_call": False, "registration_token": None}
          immediately, before any early return. Several paths in this
          method can return True because a watcher matching this row
          ALREADY exists (e.g. the recovery_rearm "left alone" path below)
          WITHOUT this call ever creating a new registration — watch()
          returning True is proof ownership exists, never proof this
          invocation created it. Only the actual add_signal() call at the
          bottom of this method can flip created_by_this_call to True, and
          only by reporting the exact object it itself just inserted. Must
          be a value the caller owns exclusively for this one call — never
          shared self state, which a second concurrent watch() call could
          overwrite before the first caller reads its own result.
          This guarantees that a row with a valid trigger can be re-owned by
          the watcher without any risk of DB mutation or OSM state change.
        """
        if registration_provenance_out is not None:
            registration_provenance_out["created_by_this_call"] = False
            registration_provenance_out["registration_token"] = None

        if plan is None:
            log.warning("watch() called with None plan -- skipping")
            return False

        # Propagate recovery mode flag into signal_dict so add_signal()
        # can suppress its own cancel_pending_entry calls.
        _recovery_rearm    = bool(recovery_rearm)
        _materialization_resume = bool(materialization_resume)
        _no_cancel_on_reject = bool(no_cancel_on_reject or recovery_rearm)
        _plan_metadata = getattr(plan, "metadata", None) or {}
        if not isinstance(_plan_metadata, dict):
            _plan_metadata = {}
        _plan_signal_id = str(getattr(plan, "signal_id", "") or "").strip()
        _plan_canonical_signal_id = str(
            getattr(plan, "canonical_signal_id", "")
            or _plan_metadata.get("canonical_signal_id")
            or ""
        ).strip()
        if not _plan_canonical_signal_id:
            _plan_canonical_signal_id = build_canonical_signal_id(
                _plan_signal_id, _plan_metadata
            )
        _plan_materialization_generation = getattr(
            plan, "materialization_generation", None
        )
        if _plan_materialization_generation is None:
            _plan_materialization_generation = _plan_metadata.get(
                "materialization_generation"
            )

        signal_dict = {
            "signal_id": _plan_signal_id or str(uuid.uuid4()),
            "canonical_signal_id": _plan_canonical_signal_id,
            "ticker": getattr(plan, "ticker", ""),
            "side": getattr(plan, "side", "CALL"),
            "score": getattr(plan, "score", 65.0),
            "grade": getattr(plan, "tier", "B"),
            "entry_price": getattr(plan, "trigger_price", None),
            "entry_trigger": getattr(
                plan, "entry_trigger", getattr(plan, "trigger_price", None)
            ),
            "stop_price": getattr(plan, "stop_underlying", None),
            "target_price": getattr(plan, "target_underlying", None),
            # Preserve existing durable first-breach evidence for the
            # classifier and the reconstructed WatchedSignal.  No new truth
            # source is introduced; this is only the order/plan metadata that
            # already survives watcher recovery.
            "trigger_crossed_at": (
                getattr(plan, "trigger_crossed_at", None)
                or (_plan_metadata.get("trigger_crossed_at") if isinstance(_plan_metadata, dict) else None)
            ),
            "plan_id": getattr(plan, "plan_id", ""),
            "local_order_id": local_order_id,
            "metadata": dict(_plan_metadata),
            "materialization_generation": _plan_materialization_generation,
            "client_id": str(
                getattr(plan, "client_id", "")
                or _plan_metadata.get("client_id")
                or ""
            ),
            "execution_mode": str(
                getattr(plan, "execution_mode", "")
                or _plan_metadata.get("execution_mode")
                or self.mode
            ).lower(),
            "watcher_token": self.owner_token,
            "trigger_generation": int(
                _plan_materialization_generation or 1
            ),
            "deferred_retry_not_before": (
                _plan_metadata.get("next_retry_at")
                or _plan_metadata.get("materialization_next_retry_at")
            ),
            # PR #182: carry trade_queue.id through to breach time so
            # write_deferred_breach_last_error() can find the queue row.
            # Populated by queue.py _dispatch() onto plan.metadata before watch() is called.
            "queue_id": _plan_metadata.get("queue_id"),
            "trade_queue_id": _plan_metadata.get("trade_queue_id"),
            # PR #388 late-attachment policy provenance flag. True ONLY for
            # plans built by the PR#388 seams (run_overnight_reeval new
            # watchers, REATTACH_WATCHER reconstructed plans, and the open-
            # revalidation seam that operates directly on WatchedSignal
            # without going through watch()). Ordinary intraday/direct
            # arms keep committed-main's strict anti-chase invariant.
            "late_attachment_policy_eligible": bool(
                getattr(plan, "late_attachment_policy_eligible", False)
                or _plan_metadata.get("late_attachment_policy_eligible", False)
            ),
            "contract_symbol": getattr(plan, "contract_symbol", ""),
            "pattern": getattr(plan, "pattern", ""),
            "prior_day_high": getattr(plan, "prior_day_high", None),
            "prior_day_low": getattr(plan, "prior_day_low", None),
            "timeframe": getattr(plan, "timeframe", "1d"),
            "strategy_type": getattr(plan, "strategy_type", ""),
            "contract_deferred": bool(
                _plan_metadata.get("contract_deferred")
                or str(getattr(plan, "contract_symbol", "") or "").upper().startswith("DEFERRED:")
            ),
            "trigger": {
                "entry": getattr(plan, "trigger_price", None),
                "stop": getattr(plan, "stop_underlying", None),
                "pt1": getattr(plan, "target_underlying", None),
            },
        }

        if _recovery_rearm or _materialization_resume:
            if not recovery_trigger_evidence_identity_is_proven(
                signal_dict, local_order_id
            ):
                self._last_reject_reason = (
                    RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN
                )
                log.critical(
                    "[%s] %s local_order_id=%s — refusing recovery rearm; "
                    "durable order and trigger evidence remain unchanged",
                    signal_dict.get("ticker") or "?",
                    RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN,
                    local_order_id or "?",
                )
                return False

        now_et = datetime.now(ET)
        post_session = (
            now_et.hour > OVERNIGHT_THRESHOLD_HOUR
            or (now_et.hour == OVERNIGHT_THRESHOLD_HOUR and now_et.minute >= OVERNIGHT_THRESHOLD_MIN)
        )
        pre_market = now_et.hour < 9 or (now_et.hour == 9 and now_et.minute < 30)
        trigger = signal_dict.get("entry_price")
        side = str(signal_dict.get("side", "CALL")).upper()
        ticker = str(signal_dict.get("ticker", "")).upper()
        stop = signal_dict.get("stop_price")
        # PR #388 P0-7: needed by the arm-time late-attachment gate.
        target = signal_dict.get("target_price")

        # Stamp the recovery flag so add_signal() suppresses cancel_pending_entry.
        if _recovery_rearm:
            signal_dict["__recovery_rearm"] = True
        if _materialization_resume:
            signal_dict["__materialization_resume"] = True
        if _recovery_rearm and _materialization_resume:
            _plan_meta_for_adopt = getattr(plan, "metadata", None) or {}
            _adopt_fn = getattr(
                getattr(self, "order_state_machine", None),
                "adopt_deferred_retry_watcher",
                None,
            )
            _durable_next_retry = (
                _plan_meta_for_adopt.get("materialization_next_retry_at")
                or _plan_meta_for_adopt.get("next_retry_at")
            )
            if not callable(_adopt_fn):
                log.critical(
                    "[%s] RECOVERY_REARM_WATCHER_ADOPT_UNAVAILABLE local_order_id=%s",
                    ticker, local_order_id,
                )
                return False
            try:
                _adopt_ok = bool(_adopt_fn(
                    local_order_id,
                    watcher_token=self.owner_token,
                    generation=int(_plan_meta_for_adopt.get("materialization_generation") or 1),
                    retry_attempt=int(_plan_meta_for_adopt.get("retry_attempt") or 0),
                    next_retry_at=str(_durable_next_retry or ""),
                    execution_mode=str(signal_dict.get("execution_mode") or ""),
                ))
            except Exception as _adopt_exc:
                log.critical(
                    "[%s] RECOVERY_REARM_WATCHER_ADOPT_RAISED local_order_id=%s error=%s",
                    ticker, local_order_id, _adopt_exc,
                )
                return False
            if not _adopt_ok:
                log.critical(
                    "[%s] RECOVERY_REARM_WATCHER_ADOPT_CAS_MISS local_order_id=%s "
                    "generation=%s attempt=%s",
                    ticker, local_order_id,
                    _plan_meta_for_adopt.get("materialization_generation"),
                    _plan_meta_for_adopt.get("retry_attempt"),
                )
                return False

        if _recovery_rearm and not _materialization_resume:
            try:
                from ap.pending_trigger_classifier import (
                    PendingTriggerClassification,
                    classify_pending_trigger_row,
                    is_safe_to_recovery_rearm,
                )
                _recovery_row = self._load_order_row_for_recovery_rearm(local_order_id)
                _watcher_owned = self.has_order(local_order_id)
                _past_entry_cutoff = self._is_past_entry_cutoff_now()
                _already_through = None
                if self._is_regular_session_now() and trigger and float(trigger or 0) > 0:
                    try:
                        _recovery_quote = self._get_quote(ticker) or {}
                    except Exception:
                        _recovery_quote = {}
                    _recovery_bid = float((_recovery_quote or {}).get("bid") or 0)
                    _recovery_ask = float((_recovery_quote or {}).get("ask") or 0)

                    # Amendment 3: LIVE + regular session + quote unavailable = fail closed.
                    # A missing quote during regular session means we cannot verify the
                    # underlying has not already blown through the trigger. Never rearm blind.
                    _is_live_watcher_rr = self._is_live_runtime()
                    if _is_live_watcher_rr and _recovery_bid == 0 and _recovery_ask == 0:
                        log.critical(
                            "[%s] RECOVERY_REARM_QUOTE_UNAVAILABLE — LIVE mode, regular session, "
                            "quote returned bid=0 ask=0. Blocking recovery_rearm for "
                            "local_order_id=%s to prevent late entry without price verification.",
                            ticker, local_order_id,
                        )
                        try:
                            self._persist_watcher_audit(local_order_id, {
                                "reason_code":     "RECOVERY_REARM_QUOTE_UNAVAILABLE",
                                "trigger_type":    "recovery_rearm_classifier",
                                "classification":  "RECOVERY_REARM_QUOTE_UNAVAILABLE",
                                "watcher_owned":   _watcher_owned,
                                "mode":            "LIVE",
                                "regular_session": True,
                                "quote_available": False,
                                "no_cancel_on_reject": _no_cancel_on_reject,
                            })
                        except Exception as _rr_audit_exc:
                            log.warning(
                                "[%s] RECOVERY_REARM_QUOTE_UNAVAILABLE audit write failed "
                                "local_order_id=%s error=%s",
                                ticker, local_order_id, _rr_audit_exc,
                            )
                        # PR #388 Blocker 2: the REATTACH_WATCHER contract in
                        # ap_overnight_reeval calls watch(recovery_rearm=True,
                        # no_cancel_on_reject=True) precisely so a temporary
                        # LIVE quote outage cannot destroy the exact
                        # PENDING_TRIGGER order the amendment was designed to
                        # recover. Honor the no-cancel flag here — before
                        # _terminalize_recovery_rearm_candidate (which calls
                        # cancel_pending_entry unconditionally). Older
                        # recovery callers that did NOT set no_cancel_on_reject
                        # still get the pre-existing terminalization behavior.
                        if _no_cancel_on_reject:
                            log.warning(
                                "[%s] RECOVERY_REARM_QUOTE_UNAVAILABLE + "
                                "no_cancel_on_reject=True — preserving exact "
                                "PENDING_TRIGGER order local_order_id=%s; "
                                "returning False without cancel_pending_entry",
                                ticker, local_order_id,
                            )
                            return False
                        self._terminalize_recovery_rearm_candidate(
                            local_order_id,
                            ticker=ticker,
                            classification="RECOVERY_REARM_QUOTE_UNAVAILABLE",
                            watcher_owned=_watcher_owned,
                            already_through=None,
                        )
                        return False

                    if _recovery_bid > 0 or _recovery_ask > 0:
                        _already_through = self._is_already_through_trigger(
                            side, float(trigger), _recovery_bid, _recovery_ask,
                        )

                # Restart recovery needs to re-own clean rows whose watcher died
                # with the process. Explicit ORPHAN_NO_WATCHER evidence in meta
                # is still classified unsafe by the shared classifier.
                _recovery_classification = classify_pending_trigger_row(
                    _recovery_row,
                    watcher_owned=True,
                    is_past_eod=_past_entry_cutoff,
                    live_quote_already_through_trigger=_already_through,
                )
                try:
                    self._persist_watcher_audit(local_order_id, {
                        "reason_code": "recovery_rearm_classified",
                        "classification": _recovery_classification,
                        "watcher_owned": _watcher_owned,
                        "is_past_eod": _past_entry_cutoff,
                        "live_quote_already_through_trigger": _already_through,
                        "trigger_type": "recovery_rearm_classifier",
                    })
                except Exception:
                    pass

                if _watcher_owned and _recovery_classification in (
                    PendingTriggerClassification.WAITING_VALID,
                    PendingTriggerClassification.WAITING_RETRYABLE,
                ):
                    log.info(
                        "[%s] RECOVERY_REARM_LEFT_ALONE classification=%s "
                        "local_order_id=%s watcher_owned=true",
                        ticker, _recovery_classification, local_order_id,
                    )
                    return True

                if not is_safe_to_recovery_rearm(_recovery_classification):
                    # PR #388 provenance-gated bypass: when the plan is
                    # explicitly late_attachment_policy_eligible AND the
                    # ONLY reason the old recovery classifier refuses is
                    # UNSAFE_ALREADY_THROUGH_TRIGGER, do NOT terminalize
                    # here. Route onward to the canonical late-attachment
                    # classifier in the normal watch-flow so it can
                    # distinguish WITHIN_CONTINUATION / WAITING_RESET /
                    # LATE_ATTACHMENT_MOVE_MISSED_TERMINAL / STOP_BROKEN /
                    # TARGET_COMPLETE. Other terminal signals (trigger-
                    # ready residue, real prior invalidation, terminal
                    # materialization, past-EOD stale, orphan no-watcher)
                    # remain authoritative here.
                    _late_eligible_bypass = (
                        _no_cancel_on_reject
                        and bool(signal_dict.get("late_attachment_policy_eligible"))
                        and _recovery_classification == (
                            PendingTriggerClassification.UNSAFE_ALREADY_THROUGH_TRIGGER
                        )
                    )
                    if _late_eligible_bypass:
                        try:
                            self._persist_watcher_audit(local_order_id, {
                                "reason_code":     "recovery_late_attachment_bypass",
                                "trigger_type":    "recovery_rearm_classifier",
                                "classification":  _recovery_classification,
                                "watcher_owned":   _watcher_owned,
                                "already_through": _already_through,
                                "late_attachment_policy_eligible": True,
                                "no_cancel_on_reject":              True,
                            })
                        except Exception:
                            pass
                        log.info(
                            "[%s] RECOVERY_REARM_LATE_ATTACHMENT_BYPASS — "
                            "old classifier flagged UNSAFE_ALREADY_THROUGH_TRIGGER "
                            "but plan is late_attachment_policy_eligible with "
                            "no_cancel_on_reject; skipping early terminalization "
                            "and routing to the canonical late-attachment classifier. "
                            "local_order_id=%s",
                            ticker, local_order_id,
                        )
                        # Fall through — the ordinary arm-time gate below
                        # will invoke classify_late_attachment() on this
                        # exact plan (which carries the provenance flag),
                        # and it will decide WITHIN / WAITING / terminal.
                    else:
                        self._terminalize_recovery_rearm_candidate(
                            local_order_id,
                            ticker=ticker,
                            classification=_recovery_classification,
                            watcher_owned=_watcher_owned,
                            already_through=_already_through,
                        )
                        return False
            except Exception as _recovery_cls_exc:
                log.error(
                    "[%s] RECOVERY_REARM_CLASSIFIER_ERROR local_order_id=%s error=%s",
                    ticker, local_order_id, _recovery_cls_exc,
                    exc_info=True,
                )
                self._terminalize_recovery_rearm_candidate(
                    local_order_id,
                    ticker=ticker,
                    classification="RECOVERY_REARM_CLASSIFIER_ERROR",
                    watcher_owned=False,
                    already_through=None,
                )
                return False

        # Queue-time staleness check is skipped for outside-session setups.
        # Those are revalidated at the regular-session open instead.
        # Also skip for overnight/daily signals — their trigger is a prior-day
        # level, not a same-day intraday price. A 1.5% move from a prior-day
        # high/low is normal and should not invalidate the signal at arm time.
        _is_overnight_signal = bool(
            signal_dict.get("prior_day_high") or
            signal_dict.get("prior_day_low") or
            str(signal_dict.get("timeframe", "")).lower() in ("1d", "daily", "overnight")
        )

        # ── OPTION PREMIUM STALENESS CHECK ────────────────────────────────
        # Separate from the underlying check. Even if the underlying is at
        # the trigger level, the option premium may have already moved 20-40%
        # because other participants front-ran the entry. Entering now means
        # overpaying on premium that already priced in the move — catching the
        # reversal instead of the setup.
        #
        # Only runs when we have a reference option price from the signal and
        # live option quotes are available. If quotes unavailable, pass through
        # (fail open — a missed quote check is better than a missed trade).
        #
        # MAX_OPTION_PREMIUM_DRIFT: if option bid is already >25% above the
        # signal's entry_option_price, the move happened without us.
        # PR-C: read from the module-level constant; was previously read
        # via os.getenv() inline on every signal arm.
        _MAX_OPTION_PREMIUM_DRIFT = MAX_OPTION_PREMIUM_DRIFT_PCT
        _signal_option_price = float(
            getattr(plan, "entry_option_price", 0)
            or signal_dict.get("entry_option_price", 0)
            or 0
        )
        if _signal_option_price > 0 and not post_session and not pre_market:
            try:
                _opt_quote = self._get_option_quote(
                    str(signal_dict.get("contract_symbol", "")
                    or getattr(plan, "contract_symbol", ""))
                )
                _opt_bid = float((_opt_quote or {}).get("bid", 0) or 0)
                if _opt_bid > 0:
                    _opt_drift = (_opt_bid - _signal_option_price) / _signal_option_price
                    if _opt_drift > _MAX_OPTION_PREMIUM_DRIFT:
                        log.warning(
                            "[%s] OPTION_PREMIUM_STALE | signal_price=$%.2f "
                            "current_bid=$%.2f drift=+%.1f%% > %.0f%% max | "
                            "move already happened — rejecting late entry",
                            ticker, _signal_option_price, _opt_bid,
                            _opt_drift * 100, _MAX_OPTION_PREMIUM_DRIFT * 100,
                        )
                        self._last_reject_reason = (
                            f"option_premium_stale_{_opt_drift*100:+.1f}pct_above_signal"
                        )
                        if _recovery_rearm:
                            log.info(
                                "[%s] recovery_rearm: suppressing option_premium_stale "
                                "rejection — row preserved, no DB mutation",
                                ticker,
                            )
                        else:
                            return False
                    log.debug(
                        "[%s] Option premium OK | signal=$%.2f bid=$%.2f drift=%.1f%%",
                        ticker, _signal_option_price, _opt_bid, _opt_drift * 100,
                    )
            except Exception as _oq_exc:
                log.debug(
                    "[%s] Option premium check unavailable (non-blocking): %s",
                    ticker, _oq_exc,
                )

        if post_session or pre_market or _is_overnight_signal:
            log.info(
                "[%s] Outside-session queue — skipping underlying staleness check "
                "(trigger=$%.2f side=%s)",
                ticker,
                float(trigger or 0),
                side,
            )
        elif trigger and trigger > 0:
            try:
                quote = self._get_quote(ticker)
                quote_age_ms = self._coerce_quote_age_ms(quote.get("quote_age_ms"))
                bid = float(quote.get("bid") or 0)
                ask = float(quote.get("ask") or 0)
                mid = (bid + ask) / 2 if bid > 0 and ask > 0 else max(bid, ask)
                # Amendment 3b: LIVE + regular session + quote unavailable = block arm.
                # If mid=0, we have no current price to verify the underlying has not
                # already blown through the trigger. Never arm LIVE without verification.
                _is_live_arm = self._is_live_runtime()
                _regular_session_arm = not post_session and not pre_market
                if mid == 0 and _is_live_arm and _regular_session_arm:
                    _watcher_arm_reason = "WATCHER_ARM_QUOTE_UNAVAILABLE"
                    log.critical(
                        "[%s] WATCHER_ARM_QUOTE_UNAVAILABLE — LIVE mode, regular session, "
                        "quote returned bid=0 ask=0 mid=0 for trigger=$%.4f. "
                        "Blocking arm to prevent entry without price verification.",
                        ticker, float(trigger or 0),
                    )
                    _no_quote_audit = self._build_watcher_audit_payload(
                        None,
                        symbol=ticker,
                        score=float(signal_dict.get("score") or 0),
                        tier=str(signal_dict.get("grade") or ""),
                        direction=side,
                        timeframe=str(signal_dict.get("timeframe") or ""),
                        pattern=str(signal_dict.get("pattern") or ""),
                        signal_id=str(signal_dict.get("signal_id") or ""),
                        plan_id=str(signal_dict.get("plan_id") or ""),
                        trigger_type="arm_time",
                        signal_entry_price=trigger,
                        trigger_price=trigger,
                        stop_price=stop,
                        current_bid=0.0,
                        current_ask=0.0,
                        current_mid=0.0,
                        reason_code=_watcher_arm_reason,
                        raw_reason="live_regular_session_arm_quote_zero",
                    )
                    try:
                        self._persist_watcher_audit(local_order_id, _no_quote_audit)
                    except Exception as _arm_audit_exc:
                        log.warning(
                            "[%s] WATCHER_ARM_QUOTE_UNAVAILABLE audit write failed "
                            "local_order_id=%s error=%s",
                            ticker, local_order_id, _arm_audit_exc,
                        )
                    return False

                if mid > 0:
                    pct_from_trigger = (mid - trigger) / trigger
                    # FUNNEL FIX (2026-05-20):
                    # 1) Use WATCH_ARM_EFFECTIVE_THRESHOLD_PCT (env-tunable, env
                    #    can only loosen, default = 1.0% or hard-coded 1.5%
                    #    whichever is larger).
                    # 2) Distinguish drift-stale from below-stop-stale so the
                    #    reject reason actually tells operators which gate fired.
                    drift_stale = (
                        (side == "CALL" and pct_from_trigger >  WATCH_ARM_EFFECTIVE_THRESHOLD_PCT) or
                        (side == "PUT"  and pct_from_trigger < -WATCH_ARM_EFFECTIVE_THRESHOLD_PCT)
                    )
                    below_stop = False
                    if stop and stop > 0:
                        if side == "CALL" and mid < stop:
                            below_stop = True
                        elif side == "PUT" and mid > stop:
                            below_stop = True
                    if drift_stale:
                        # Drift stale: price has moved decisively away from trigger.
                        # Structurally missed — no re-arm opportunity.
                        reason_code = (
                            f"arm_drift_{pct_from_trigger*100:+.2f}pct"
                            f"_thr_{WATCH_ARM_EFFECTIVE_THRESHOLD_PCT*100:.2f}pct"
                        )
                        log.warning(
                            "[%s] STALE_ARM_REJECT gate=drift mid=$%.2f trigger=$%.2f "
                            "drift=%.3f%% effective_threshold=%.3f%% side=%s",
                            ticker, mid, trigger,
                            pct_from_trigger * 100.0,
                            WATCH_ARM_EFFECTIVE_THRESHOLD_PCT * 100.0,
                            side,
                        )
                        _drift_audit = self._build_watcher_audit_payload(
                            None,
                            symbol=ticker,
                            score=float(signal_dict.get("score") or 0),
                            tier=str(signal_dict.get("grade") or ""),
                            direction=side,
                            timeframe=str(signal_dict.get("timeframe") or ""),
                            pattern=str(signal_dict.get("pattern") or ""),
                            signal_id=str(signal_dict.get("signal_id") or ""),
                            plan_id=str(signal_dict.get("plan_id") or ""),
                            trigger_type="arm_time",
                            signal_entry_price=trigger,
                            trigger_price=trigger,
                            stop_price=stop,
                            current_bid=bid,
                            current_ask=ask,
                            current_mid=mid,
                            quote_age_ms=quote_age_ms,
                            arm_price=mid,
                            arm_condition=(
                                f"drift_{pct_from_trigger*100:+.2f}pct"
                                f"_vs_threshold_{WATCH_ARM_EFFECTIVE_THRESHOLD_PCT*100:.2f}pct"
                            ),
                            stop_condition=f"stop_{stop:.4f}" if stop and stop > 0 else "",
                            reason_code="arm_drift",
                            raw_reason=reason_code,
                        )
                        self._persist_watcher_audit(local_order_id, _drift_audit)
                        self._last_reject_reason = reason_code
                        if _recovery_rearm:
                            log.info(
                                "[%s] recovery_rearm: suppressing arm_drift rejection "
                                "— row preserved, no DB mutation",
                                ticker,
                            )
                        else:
                            return False

                    elif below_stop:
                        # Wrong-side-of-stop: price is temporarily on the wrong side.
                        # For strong daily/overnight signals this may be transient —
                        # check rearm eligibility before permanently rejecting.
                        reason_code = f"arm_below_stop_mid_{mid:.2f}_stop_{stop:.2f}"
                        log.warning(
                            "[%s] STALE_ARM_REJECT gate=below_stop mid=$%.2f trigger=$%.2f "
                            "drift=%.3f%% side=%s stop=%.2f",
                            ticker, mid, trigger,
                            pct_from_trigger * 100.0,
                            side, stop,
                        )
                        _score_val = float(signal_dict.get("score") or 0)
                        _tier_val = str(signal_dict.get("grade") or "")
                        _is_daily_or_overnight = post_session or pre_market or _is_overnight_signal
                        _stop_audit = self._build_watcher_audit_payload(
                            None,
                            symbol=ticker,
                            score=_score_val,
                            tier=_tier_val,
                            direction=side,
                            timeframe=str(signal_dict.get("timeframe") or ""),
                            pattern=str(signal_dict.get("pattern") or ""),
                            signal_id=str(signal_dict.get("signal_id") or ""),
                            plan_id=str(signal_dict.get("plan_id") or ""),
                            trigger_type="arm_time",
                            signal_entry_price=trigger,
                            trigger_price=trigger,
                            stop_price=stop,
                            current_bid=bid,
                            current_ask=ask,
                            current_mid=mid,
                            quote_age_ms=quote_age_ms,
                            arm_price=mid,
                            arm_condition=f"mid_{mid:.4f}_wrong_side_of_stop_{stop:.4f}",
                            stop_condition=f"stop_{stop:.4f}" if stop and stop > 0 else "",
                            reason_code="arm_below_stop",
                            raw_reason=reason_code,
                        )
                        if self._is_rearm_eligible(_score_val, _tier_val, _is_daily_or_overnight):
                            # Signal accepted into watcher in DISARMED state.
                            # add_signal() will flip rearm_mode=True via __watcher_rearm_pending.
                            # OSM order stays alive — on_expire fires on timeout if no reclaim.
                            _stop_audit["trigger_type"] = "arm_time_rearm_queued"
                            _stop_audit["rearm_eligible"] = True
                            _stop_audit["rearm_window_sec"] = WATCHER_REARM_WINDOW_SEC
                            _stop_audit["is_daily_or_overnight"] = _is_daily_or_overnight
                            log.info(
                                "[%s] ARM_BELOW_STOP → REARM_ELIGIBLE | "
                                "score=%.1f tier=%s daily_overnight=%s | "
                                "entering disarm window=%ds",
                                ticker, _score_val, _tier_val,
                                _is_daily_or_overnight,
                                WATCHER_REARM_WINDOW_SEC,
                            )
                            self._persist_watcher_audit(local_order_id, _stop_audit)
                            # Mark for rearm mode — consumed and cleaned by add_signal()
                            signal_dict["__watcher_rearm_pending"] = True
                            signal_dict["__watcher_rearm_reason"] = reason_code
                            # Do NOT set _last_reject_reason — this is not a rejection
                            # Do NOT return False — fall through to add_signal()
                        else:
                            # Low-score / intraday / not eligible — permanent reject
                            # In recovery_rearm mode, suppress the reject so the row
                            # is re-owned by the watcher without any DB mutation.
                            self._persist_watcher_audit(local_order_id, _stop_audit)
                            self._last_reject_reason = reason_code
                            if _recovery_rearm:
                                log.info(
                                    "[%s] recovery_rearm: suppressing arm_below_stop "
                                    "rejection — row preserved, no DB mutation",
                                    ticker,
                                )
                            else:
                                return False
                    log.debug(
                        "[%s] Price check OK — $%.2f vs trigger $%.2f (%.1f%%)",
                        ticker,
                        mid,
                        trigger,
                        pct_from_trigger * 100.0,
                    )
            except Exception as exc:
                log.debug("[%s] Price staleness check failed; continuing: %s", ticker, exc)

        log.info(
            "[%s] watch() | plan=%s order=%s trigger=$%s side=%s",
            ticker,
            getattr(plan, "plan_id", ""),
            local_order_id,
            signal_dict["entry_price"],
            signal_dict["side"],
        )
        # ── P0 (PR #304) Bug C: arm-time already-through-trigger gate ──────
        # Before add_signal(), during regular session, verify the underlying
        # has not already crossed the trigger. If it has, a watcher armed now
        # would fire on a move that already happened — the exact "late entry"
        # failure the audit calls out.
        #
        # Runs even when:
        #   - _is_overnight_signal is True (daily/overnight rows that got
        #     late-handed off to a regular-session arm)
        #   - recovery_rearm is True (morning handoff / restart recovery —
        #     recovery mode may skip drift/staleness rejections, but MUST NOT
        #     bypass this safety gate)
        #
        # Skipped when:
        #   - pre_market or post_session (no live regular quote to trust)
        #   - trigger is missing/invalid
        #   - quote fetch fails (fail-safe: let existing paths handle it)
        _regular_session_now = not pre_market and not post_session
        if (
            _regular_session_now
            and trigger
            and float(trigger or 0) > 0
            and not _materialization_resume
        ):
            try:
                _bug_c_quote = self._get_quote(ticker) or {}
            except Exception:
                _bug_c_quote = {}
            _bug_c_bid = float(_bug_c_quote.get("bid") or 0)
            _bug_c_ask = float(_bug_c_quote.get("ask") or 0)

            # PR #388 Blocker 1: continuation/reset classifier applies ONLY
            # to plans that explicitly opted in via the provenance flag
            # (overnight_reeval new watchers, REATTACH_WATCHER reconstructed
            # plans, open-revalidation seams). Ordinary intraday/direct
            # arms keep committed-main's strict arm_already_through_trigger
            # anti-chase behavior — never seed WITHIN/WAITING/AWAITING for
            # those callers.
            _late_policy_eligible = bool(signal_dict.get("late_attachment_policy_eligible"))
            if not _late_policy_eligible:
                if (_bug_c_bid > 0 or _bug_c_ask > 0) and self._is_already_through_trigger(
                    side, float(trigger), _bug_c_bid, _bug_c_ask,
                ):
                    _bug_c_mid = (_bug_c_bid + _bug_c_ask) / 2.0 if (_bug_c_bid and _bug_c_ask) else max(_bug_c_bid, _bug_c_ask)
                    _bug_c_full_audit = {
                        "reason_code":         "arm_already_through_trigger",
                        "raw_reason": (
                            f"side_{side}_ask_{_bug_c_ask:.4f}_bid_{_bug_c_bid:.4f}"
                            f"_already_through_trigger_{float(trigger):.4f}"
                            f"_at_arm_time recovery_rearm={_recovery_rearm} "
                            f"late_policy_eligible=false"
                        ),
                        "trigger_type":        "arm_check",
                        "current_bid":         _bug_c_bid,
                        "current_ask":         _bug_c_ask,
                        "current_mid":         _bug_c_mid,
                        "arm_condition":       f"trigger_{float(trigger):.4f}",
                        "recovery_rearm":      _recovery_rearm,
                        "regular_session":     True,
                        "trigger_price":       float(trigger),
                        "side":                side,
                        "late_attachment_policy_eligible": False,
                    }
                    try:
                        self._persist_watcher_audit(local_order_id, _bug_c_full_audit)
                    except Exception:
                        pass
                    log.warning(
                        "[%s] WATCHER_ARM_REJECTED_ALREADY_THROUGH_TRIGGER — "
                        "%s ask=%.4f bid=%.4f already crossed trigger=%.4f at arm-time "
                        "(recovery_rearm=%s late_policy_eligible=false). "
                        "Refusing to arm; setup missed the move.",
                        ticker, side, _bug_c_ask, _bug_c_bid, float(trigger),
                        _recovery_rearm,
                    )
                    if not _recovery_rearm and self.on_invalidate is not None:
                        try:
                            _bug_c_shim = types.SimpleNamespace(
                                signal=signal_dict,
                                ticker=ticker,
                                _pending_audit=_bug_c_full_audit,
                                state=WatchState.INVALIDATED,
                            )
                            self.on_invalidate(_bug_c_shim)
                        except Exception as _bc_exc:
                            log.error(
                                "[%s] arm_already_through_trigger cleanup callback failed: %s",
                                ticker, _bc_exc,
                            )
                    return False
                # Ineligible + not already through trigger → fall through to
                # normal add_signal path; the PR#388 classifier is skipped.

            elif _late_policy_eligible:
                # PR #388 Block-2: canonical late-attachment classifier.
                # (ONLY reached when late_attachment_policy_eligible=True.)
                # Consult the shared classifier for the arm-time decision.
                # A small continuation zone around the trigger is not a
                # missed move; only terminalize when the setup is
                # structurally dead.
                from ap.pending_trigger_classifier import (
                    classify_late_attachment as _pt_classify_late,
                    LATE_ATTACHMENT_WITHIN_CONTINUATION as _PT_WITHIN,
                    MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET as _PT_WAITING,
                    LATE_ATTACHMENT_MOVE_MISSED_TERMINAL as _PT_MOVE_MISSED,
                    STOP_ALREADY_BROKEN_TERMINAL as _PT_STOP_BROKEN,
                    TARGET_ALREADY_COMPLETE_TERMINAL as _PT_TARGET_COMPLETE,
                    TRIGGER_TRUTH_UNAVAILABLE_RETRY as _PT_TRUTH_RETRY,
                )
                # PR #388 P0-7: compute target_complete from canonical side.
                _arm_tgt_complete = False
                try:
                    _tgt = float(target or 0)
                    if _tgt > 0:
                        if side == "CALL" and _bug_c_ask > 0 and _bug_c_ask >= _tgt:
                            _arm_tgt_complete = True
                        elif side == "PUT" and _bug_c_bid > 0 and _bug_c_bid <= _tgt:
                            _arm_tgt_complete = True
                except (TypeError, ValueError):
                    _arm_tgt_complete = False
                # PR #388 Blocker #4: wire decisive drift at arm-time.
                _arm_decisive_drift = False
                try:
                    _t_arm = float(trigger or 0)
                    if _t_arm > 0:
                        if side == "CALL" and _bug_c_ask > 0:
                            if _bug_c_ask > _t_arm * (1.0 + MAX_INTRADAY_DRIFT_PCT):
                                _arm_decisive_drift = True
                        elif side == "PUT" and _bug_c_bid > 0:
                            if _bug_c_bid < _t_arm * (1.0 - MAX_INTRADAY_DRIFT_PCT):
                                _arm_decisive_drift = True
                except (TypeError, ValueError):
                    _arm_decisive_drift = False

                _late_decision = _pt_classify_late(
                    side=side,
                    trigger_price=float(trigger),
                    bid=_bug_c_bid,
                    ask=_bug_c_ask,
                    stop=stop,
                    target_complete=_arm_tgt_complete,
                    decisive_drift_exceeded=_arm_decisive_drift,
                    trigger_previously_breached=(
                        _parse_trigger_crossed_at(signal_dict.get("trigger_crossed_at"))
                        is not None
                    ),
                )
                _late_cls = _late_decision.classification

                if _late_cls in (_PT_WITHIN, _PT_WAITING):
                    # Attach the watcher, seed the late-attachment state so
                    # check() enforces the confirmation / reset requirement
                    # before allowing the normal breach path to fire.
                    signal_dict.setdefault("_late_attachment_seed", {
                        "state": _late_cls,
                        "seen_at": datetime.now(timezone.utc).isoformat(),
                        "quote": float(_late_decision.quote or 0),
                        "quote_source": _late_decision.quote_source,
                        "allowed_continuation": (
                            float(_late_decision.allowed_continuation)
                            if _late_decision.allowed_continuation is not None else None
                        ),
                        "raw_bid": _bug_c_bid,
                        "raw_ask": _bug_c_ask,
                    })
                    log.info(
                        "[%s] LATE_ATTACHMENT_%s — %s canonical_quote=%s trigger=%.4f "
                        "allowed_continuation=%s recovery_rearm=%s "
                        "(watcher armed; requires poll-confirmation before submit)",
                        ticker,
                        "WITHIN_CONTINUATION" if _late_cls == _PT_WITHIN else "WAITING_RESET",
                        side,
                        _late_decision.quote,
                        float(trigger),
                        _late_decision.allowed_continuation,
                        _recovery_rearm,
                    )
                    # Fall through to normal add_signal / watcher arm path.
                elif _late_cls == _PT_TRUTH_RETRY and _late_decision.quote is None:
                    # Missing canonical truth at arm-time (canonical side has no
                    # quote — CALL: ask missing; PUT: bid missing; or both zero).
                    # Seed AWAITING_FIRST_TRUTH so the poll loop's gate stays
                    # engaged; the ordinary breach path must not fire on the
                    # first available quote without first classifying it against
                    # the continuation window. TRUTH_RETRY with a valid quote is
                    # handled by the ordinary-arm branch below — that case is
                    # the normal pre-trigger arm, not a late attachment.
                    from ap.pending_trigger_classifier import (
                        LATE_ATTACHMENT_AWAITING_FIRST_TRUTH as _PT_AWAITING,
                    )
                    signal_dict.setdefault("_late_attachment_seed", {
                        "state": _PT_AWAITING,
                        "seen_at": datetime.now(timezone.utc).isoformat(),
                        "quote": None,
                        "quote_source": _late_decision.quote_source,
                        "allowed_continuation": (
                            float(_late_decision.allowed_continuation)
                            if _late_decision.allowed_continuation is not None else None
                        ),
                        "raw_bid": _bug_c_bid,
                        "raw_ask": _bug_c_ask,
                        "detail": _late_decision.detail,
                    })
                    log.info(
                        "[%s] LATE_ATTACHMENT_AWAITING_FIRST_TRUTH — "
                        "canonical trigger quote unavailable at arm time "
                        "(detail=%s recovery_rearm=%s); watcher armed with "
                        "gate engaged. Ordinary breach path is blocked until "
                        "the first truthful canonical quote arrives.",
                        ticker, _late_decision.detail, _recovery_rearm,
                    )
                    # Fall through to normal add_signal / watcher arm path.
                elif _late_cls == _PT_TRUTH_RETRY:
                    # TRUTH_RETRY with a valid canonical quote — the quote is on
                    # the ordinary pre-trigger side of the canonical lane (CALL:
                    # quote<trigger; PUT: quote>trigger). This is the ordinary
                    # normal arm before the trigger has crossed; the setup has
                    # NOT breached and NOT missed its move. Do NOT seed any
                    # late-attachment state — the ordinary breach path owns it.
                    log.debug(
                        "[%s] arm-time canonical quote=%s is pre-trigger "
                        "(detail=%s) — arming normally; ordinary breach path owns "
                        "the future.",
                        ticker, _late_decision.quote, _late_decision.detail,
                    )
                    # Fall through to normal add_signal / watcher arm path.
                elif _late_cls in (_PT_STOP_BROKEN, _PT_TARGET_COMPLETE, _PT_MOVE_MISSED):
                    # Terminal at arm-time: STOP_BROKEN / TARGET_COMPLETE /
                    # LATE_ATTACHMENT_MOVE_MISSED_TERMINAL. Preserve the
                    # existing terminalization path, but tag the reason code
                    # with the canonical classifier output.
                    _terminal_reason_map = {
                        _PT_STOP_BROKEN:      "stop_already_broken_terminal",
                        _PT_TARGET_COMPLETE:  "target_already_complete_terminal",
                        _PT_MOVE_MISSED:      "late_attachment_move_missed_terminal",
                    }
                    _terminal_reason = _terminal_reason_map.get(
                        _late_cls, "late_attachment_move_missed_terminal",
                    )
                    _bug_c_mid = (_bug_c_bid + _bug_c_ask) / 2.0 if (_bug_c_bid and _bug_c_ask) else max(_bug_c_bid, _bug_c_ask)
                    _bug_c_full_audit = {
                        "reason_code":     _terminal_reason,
                        "raw_reason": (
                            f"side_{side}_ask_{_bug_c_ask:.4f}_bid_{_bug_c_bid:.4f}"
                            f"_late_attachment_{_late_cls}_{float(trigger):.4f}"
                            f"_at_arm_time recovery_rearm={_recovery_rearm}"
                        ),
                        "trigger_type":    "arm_check",
                        "current_bid":     _bug_c_bid,
                        "current_ask":     _bug_c_ask,
                        "current_mid":     _bug_c_mid,
                        "arm_condition":   f"trigger_{float(trigger):.4f}",
                        "recovery_rearm":  _recovery_rearm,
                        "regular_session": True,
                        "trigger_price":   float(trigger),
                        "side":            side,
                        "late_attachment_detail": _late_decision.detail,
                    }
                    try:
                        self._persist_watcher_audit(local_order_id, _bug_c_full_audit)
                    except Exception:
                        pass
                    if _recovery_rearm:
                        self._terminalize_recovery_rearm_candidate(
                            local_order_id,
                            ticker=ticker,
                            classification=_terminal_reason,
                            watcher_owned=bool(_watcher_owned),
                            already_through=True,
                        )
                    # Preserve legacy log markers + reason code so downstream
                    # structural checks (PR #304 Bug C tests, invalidation
                    # taxonomy) continue to recognise this terminalization.
                    # arm_already_through_trigger stays as the canonical
                    # legacy reason string; the new specific code above is
                    # the Block-2 refinement.
                    log.warning(
                        "[%s] WATCHER_ARM_REJECTED_ALREADY_THROUGH_TRIGGER — "
                        "%s ask=%.4f bid=%.4f trigger=%.4f "
                        "new_reason=%s detail=%s (recovery_rearm=%s). "
                        "reason_code=arm_already_through_trigger",
                        ticker, side, _bug_c_ask, _bug_c_bid, float(trigger),
                        _terminal_reason, _late_decision.detail, _recovery_rearm,
                    )
                    if not _recovery_rearm and self.on_invalidate is not None:
                        try:
                            _bug_c_shim = types.SimpleNamespace(
                                signal=signal_dict,
                                ticker=ticker,
                                _pending_audit=_bug_c_full_audit,
                                state=WatchState.INVALIDATED,
                            )
                            self.on_invalidate(_bug_c_shim)
                        except Exception as _bc_exc:
                            log.error(
                                "[%s] %s cleanup callback failed: %s",
                                ticker, _terminal_reason, _bc_exc,
                            )
                    return False

        # PR-C / BUG-EW-4: when add_signal blocks (dedup, opposite-side
        # weaker score, etc.), the OSM entry order created earlier by the
        # queue/worker stays as a ghost CREATED row with no watcher
        # attached. Same failure mode as the watcher_expired /
        # watcher_invalidated cases that execution_core already cleans up
        # via _cleanup_pending_entry_order. Here we proactively cancel
        # the pending entry order through the existing OSM helper.
        try:
            ok = self.add_signal(
                signal_dict,
                registration_provenance_out=registration_provenance_out,
            )
        finally:
            signal_dict.pop("__watcher_rearm_pending", None)
            signal_dict.pop("__watcher_rearm_reason", None)
            signal_dict.pop("__recovery_rearm", None)
        if not ok:
            # add_signal already logged the audit for locked-path blocks (dedup/opposite/same-side).
            # Attempt a best-effort DB persist here using the full signal context available in watch().
            _blk_reject = getattr(self, "_last_reject_reason", "") or "watcher_add_signal_blocked"
            _blk_audit = self._build_watcher_audit_payload(
                None,
                symbol=ticker,
                score=float(signal_dict.get("score") or 0),
                tier=str(signal_dict.get("grade") or ""),
                direction=side,
                timeframe=str(signal_dict.get("timeframe") or ""),
                pattern=str(signal_dict.get("pattern") or ""),
                signal_id=str(signal_dict.get("signal_id") or ""),
                plan_id=str(signal_dict.get("plan_id") or ""),
                trigger_type="add_signal_block",
                signal_entry_price=trigger,
                trigger_price=trigger,
                stop_price=stop,
                reason_code=_blk_reject,
                raw_reason=_blk_reject,
            )
            self._persist_watcher_audit(local_order_id, _blk_audit)
        if not ok and local_order_id and self.order_state_machine is not None and not _no_cancel_on_reject:
            # recovery_rearm / no_cancel_on_reject: suppress OSM cancel so the DB row
            # is never mutated during a handoff audit re-arm. The caller owns recovery.
            cancel_fn = getattr(self.order_state_machine, "cancel_pending_entry", None)
            if callable(cancel_fn):
                _reject = getattr(self, "_last_reject_reason", "") or "watcher_add_signal_blocked"
                try:
                    cancel_fn(local_order_id, reason=f"watcher_block:{_reject}")
                    log.info(
                        "[%s] OSM cancel_pending_entry called after watcher block | "
                        "local_order_id=%s reason=%s",
                        ticker, local_order_id, _reject,
                    )
                except Exception as _osm_exc:
                    log.error(
                        "[%s] OSM cancel_pending_entry failed after watcher block | "
                        "local_order_id=%s error=%s",
                        ticker, local_order_id, _osm_exc,
                    )
        elif not ok and _no_cancel_on_reject:
            log.info(
                "[%s] recovery_rearm/no_cancel_on_reject: suppressing cancel_pending_entry "
                "after watcher block | local_order_id=%s reason=%s",
                ticker, local_order_id,
                getattr(self, "_last_reject_reason", "") or "watcher_add_signal_blocked",
            )
        return ok

    def start(self):
        if self._running:
            return
        if not self.on_trigger:
            msg = (
                "APEntryWatcher cannot start without on_trigger callback; "
                "triggered entries would not submit to execution core/OSM."
            )
            if self.require_on_trigger:
                raise RuntimeError(msg)
            log.warning(msg)
        self._validate_market_data_preflight()
        self._running = True
        self._thread = threading.Thread(
            target=self._poll_loop,
            daemon=True,
            name="ap-entry-watcher",
        )
        self._thread.start()
        log.info("APEntryWatcher started")

    def stop(self):
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        log.info("APEntryWatcher stopped")

    def status(self) -> list[dict]:
        with self._lock:
            return [
                {
                    "ticker": w.ticker,
                    "side": w.side,
                    "trigger": w.entry_trigger,
                    "target": w.target_price,
                    "stop": w.stop_level,
                    "state": w.state,
                    "mins_watching": round(w.minutes_watching, 1),
                    "score": w.score,
                    "grade": w.grade,
                    "overnight": w.overnight,
                    "queue_status": w.signal.get("queue_status", ""),
                    "strategy_type": w.signal.get("strategy_type", ""),
                    "timeframe": w.signal.get("timeframe", ""),
                    "plan_id": w.signal.get("plan_id", ""),
                    "local_order_id": w.signal.get("local_order_id", ""),
                    "contract_symbol": w.signal.get("contract_symbol", ""),
                    "last_bid": w.last_quote_bid,
                    "last_ask": w.last_quote_ask,
                    "breach_count": w.breach_count,
                    "rearm_mode": getattr(w, "rearm_mode", False),
                    "rearm_count": getattr(w, "rearm_count", 0),
                    "rearm_expires_at": (
                        w.rearm_expires_at.isoformat()
                        if getattr(w, "rearm_expires_at", None) else None
                    ),
                    "rearm_reason": getattr(w, "rearm_reason", ""),
                    # Final amendment §5: quarantine ownership diagnostics.
                    "ownership_quarantine": getattr(w, "_ownership_quarantine", False),
                    "quarantine_reason": getattr(w, "_quarantine_reason", ""),
                    "quarantine_metadata_persist_failed": getattr(
                        w, "quarantine_metadata_persist_failed", False
                    ),
                    "cleanup_retry_attempt": getattr(w, "cleanup_retry_attempt", 0),
                }
                for w in self._pending
            ]

    def _poll_loop(self):
        # Register with health registry once at thread start.
        try:
            from ap_health_registry import HEALTH as _WH, Criticality as _WC
            _WH.ensure_registered("ap_entry_watcher", _WC.HIGH, stale_after_s=45.0)
            _watcher_health_ok = True
        except Exception:
            _watcher_health_ok = False

        while self._running:
            try:
                self._check_all()
                if _watcher_health_ok:
                    try:
                        # PR-C: _WH was already captured at thread start;
                        # the inner `from ap_health_registry import HEALTH`
                        # was redundant and added a sys.modules lookup per
                        # heartbeat. Removed.
                        with self._lock:
                            _watching_count = sum(1 for w in self._pending if w.is_active)
                        _WH.heartbeat(
                            "ap_entry_watcher",
                            metrics={"watching": _watching_count},
                        )
                    except Exception:
                        pass
            except Exception as exc:
                log.error("Watcher poll error: %s", exc, exc_info=True)
            time.sleep(POLL_INTERVAL_SEC)

    def _check_all(self):
        now_et = datetime.now(ET)

        today_et = now_et.date()
        if self._open_protect_date != today_et:
            self._open_protect_date = today_et
            self._open_trigger_count = 0
            self._open_trigger_tickers = set()
            self._open_trigger_keys = set()
            self._open_trigger_owners = {}

        open_protect_active = (
            now_et.hour == 9 and 30 <= now_et.minute < 30 + OPEN_PROTECT_MINUTES
        )

        # EOD force-expire same-day signals only. Overnight setups survive.
        if now_et.hour > EOD_CUTOFF_HOUR or (now_et.hour == EOD_CUTOFF_HOUR and now_et.minute >= EOD_CUTOFF_MIN):
            with self._lock:
                expired = []
                surviving = []
                for w in self._pending:
                    # Expire both actively-watching and rearm-wait same-day signals.
                    # rearm_mode signals have is_active=False so they were previously
                    # invisible to this gate and would ghost as PENDING overnight.
                    _is_eod_target = (
                        (w.is_active and not w.overnight)
                        or (getattr(w, "rearm_mode", False) and not w.overnight)
                    )
                    if _is_eod_target:
                        if getattr(w, "rearm_mode", False):
                            w.rearm_mode = False  # clear before state change
                        w.state = WatchState.EXPIRED
                        # BUG TRAP: WATCHING → EXPIRED logged here.
                        # If signals vanish before market open, this log reveals if
                        # the EOD cron is incorrectly expiring overnight signals.
                        _ew_record(
                            str(w.signal.get("signal_id", "")),
                            w.ticker, "EXPIRED",
                            "eod_force_expire_same_day_signal",
                            overnight=False,
                        )
                        expired.append(w)
                        log.info("[%s] Force-expired at market close (same-day signal)", w.ticker)
                    else:
                        surviving.append(w)
                self._pending = surviving
                if expired:
                    log.info(
                        "EOD force-expire: %d same-day signals expired, %d overnight signals held",
                        len(expired),
                        len(surviving),
                    )
            for w in expired:
                if self.on_expire:
                    try:
                        self.on_expire(w)
                    except Exception as _exc:
                        log.error("[%s] on_expire callback failed on EOD expire: %s", w.ticker, _exc)
            return

        # Pre-market hold — no regular trigger polling before 9:30 ET.
        if now_et.hour < 9 or (now_et.hour == 9 and now_et.minute < 30):
            return

        self._revalidate_overnight_at_open()
        self._poll_active_signals(open_protect_active=open_protect_active)
        self._check_rearm_signals()
        self._retry_quarantined_cleanup()  # PR #324: retry FAILED cleanup owners

    def _revalidate_overnight_at_open(self) -> None:
        with self._lock:
            overnight_active = [w for w in self._pending if w.is_active and w.overnight]

        if not overnight_active:
            return

        to_remove = []
        for w in overnight_active:
            if _safe_is_daily_signal(w) and _DAILY_VALIDATOR_AVAILABLE:
                w.signal["queue_status"] = OvernightWatchState.OPEN_RECHECK_PENDING
                try:
                    result = _validator_recheck_overnight_daily(w, self.broker)
                except Exception as exc:
                    _ov_exc_audit = self._build_watcher_audit_payload(
                        w,
                        trigger_type="overnight_revalidation",
                        reason_code="overnight_daily_validator_error",
                        raw_reason=f"validator_exception_{type(exc).__name__}",
                        extra={
                            "queue_status": str(w.signal.get("queue_status") or ""),
                            "is_daily": True,
                        },
                    )
                    self._persist_watcher_audit(w.signal.get("local_order_id"), _ov_exc_audit)
                    w.state = WatchState.INVALIDATED
                    w.signal["queue_status"] = OvernightWatchState.INVALIDATED
                    log.error("[%s] OVERNIGHT_DAILY_VALIDATOR_ERROR — %s", w.ticker, exc, exc_info=True)
                    to_remove.append(w)
                    continue

                if not getattr(result, "valid", False):
                    # ── PR — RETRY_LATER vs structural INVALIDATE split ────────
                    # Reason codes that mean "data unavailable right now" must
                    # NOT permanently invalidate the watcher. Missing bars /
                    # snapshot at 9:30 ET is normal — Tradier timesales lag
                    # 1-3 minutes after the bell. We retry until 9:40 ET then
                    # expire honestly. Only genuine structural failures
                    # (prior-day boundary breached, both sides breached, invalid
                    # side) warrant permanent invalidation.
                    _DATA_UNAVAILABLE_CODES = frozenset({
                        "INVALIDATED_SNAPSHOT_UNAVAILABLE",
                        "INVALIDATED_MISSING_PRIOR_LEVELS",
                        # Legacy strings emitted before the enum was standardised:
                        "SNAPSHOT_UNAVAILABLE",
                        "MISSING_PRIOR_LEVELS",
                    })
                    _result_rc = str(getattr(result, "reason_code", "") or "")
                    _is_data_unavailable = _result_rc in _DATA_UNAVAILABLE_CODES

                    if _is_data_unavailable:
                        # ── Data-unavailable path: RETRY_LATER or timeout ─────
                        _now_et_rv = datetime.now(ET)
                        _et_minutes_rv = _now_et_rv.hour * 60 + _now_et_rv.minute
                        _RECHECK_DEADLINE = 9 * 60 + 40  # 09:40 ET

                        if _et_minutes_rv < _RECHECK_DEADLINE:
                            # Still within retry window — keep watching.
                            # Do NOT set INVALIDATED. Do NOT release dedup key.
                            # Do NOT call on_invalidate. Do NOT add to to_remove.
                            w.signal["queue_status"] = OvernightWatchState.OPEN_RECHECK_PENDING
                            _ov_retry_audit = self._build_watcher_audit_payload(
                                w,
                                trigger_type="overnight_revalidation",
                                reason_code="overnight_open_data_unavailable_retry_later",
                                raw_reason=_result_rc,
                                extra={
                                    "queue_status":   OvernightWatchState.OPEN_RECHECK_PENDING,
                                    "et_now":         _now_et_rv.strftime("%H:%M:%S"),
                                    "deadline_et":    "09:40",
                                    "is_daily":       True,
                                },
                            )
                            self._persist_watcher_audit(
                                w.signal.get("local_order_id"), _ov_retry_audit,
                            )
                            log.warning(
                                "[%s] MORNING_REEVAL_PREOPEN_RETRY_LATER | side=%s | "
                                "reason=%s | et=%s | will retry until 09:40 ET",
                                w.ticker, w.side, _result_rc,
                                _now_et_rv.strftime("%H:%M:%S"),
                            )
                            # Keep watcher alive — do not add to to_remove.
                            continue

                        else:
                            # Past 09:40 ET deadline — data never arrived.
                            # Expire with an honest, durable reason.
                            _ov_timeout_audit = self._build_watcher_audit_payload(
                                w,
                                trigger_type="overnight_revalidation",
                                reason_code="overnight_open_recheck_data_timeout",
                                raw_reason=(
                                    f"data_unavailable_past_0940_et "
                                    f"last_code={_result_rc} "
                                    f"et={_now_et_rv.strftime('%H:%M:%S')}"
                                ),
                                extra={
                                    "queue_status": str(w.signal.get("queue_status") or ""),
                                    "et_now":       _now_et_rv.strftime("%H:%M:%S"),
                                    "deadline_et":  "09:40",
                                    "is_daily":     True,
                                },
                            )
                            self._persist_watcher_audit(
                                w.signal.get("local_order_id"), _ov_timeout_audit,
                            )
                            w.state = WatchState.EXPIRED
                            w.signal["queue_status"] = OvernightWatchState.INVALIDATED
                            log.warning(
                                "[%s] MORNING_REEVAL_REJECTED_DATA_UNAVAILABLE_AFTER_OPEN | "
                                "side=%s | reason=overnight_open_recheck_data_timeout | "
                                "et=%s — data never arrived before 09:40 ET deadline",
                                w.ticker, w.side, _now_et_rv.strftime("%H:%M:%S"),
                            )
                            to_remove.append(w)
                            continue

                    # ── Structural invalidation path (unchanged) ──────────────
                    # PRIOR_HIGH_BREACHED, PRIOR_LOW_BREACHED, BOTH_SIDES_BREACHED,
                    # INVALID_SIDE, EXPIRED_NO_TRIGGER — these are real failures
                    # that mean the setup is no longer valid regardless of data.
                    _ov_inv_audit = self._build_watcher_audit_payload(
                        w,
                        trigger_type="overnight_revalidation",
                        reason_code="overnight_daily_invalidated",
                        raw_reason=(
                            f"{getattr(result, 'reason_code', 'UNKNOWN')}"
                            f"_{getattr(result, 'reason_text', '')}"
                        ),
                        extra={
                            "queue_status":     str(w.signal.get("queue_status") or ""),
                            "validator_reason": getattr(result, "reason_code", "UNKNOWN"),
                            "validator_text":   getattr(result, "reason_text", ""),
                            "is_daily":         True,
                        },
                    )
                    self._persist_watcher_audit(w.signal.get("local_order_id"), _ov_inv_audit)
                    w.state = WatchState.INVALIDATED
                    w.signal["queue_status"] = OvernightWatchState.INVALIDATED
                    log.info(
                        "[%s] MORNING_REEVAL_STUCK_ROW_PREVENTED structural INVALIDATE | "
                        "side=%s | %s | %s",
                        w.ticker,
                        w.side,
                        getattr(result, "reason_code", "UNKNOWN"),
                        getattr(result, "reason_text", ""),
                    )
                    to_remove.append(w)
                else:
                    # ── PR #388 Block-2 fix (P0-6) ────────────────────────
                    # The daily validator confirmed structural validity. Fetch
                    # a live quote and consult the canonical late-attachment
                    # classifier — the SAME one used at regular-session arm
                    # time. Never re-implement the trigger continuation policy
                    # here. Six outcomes drive one of five actions:
                    #   * pre-trigger (TRUTH_RETRY + valid quote): ordinary arm
                    #   * WITHIN_CONTINUATION: seed WITHIN
                    #   * WAITING_RESET: seed WAITING
                    #   * TRUTH_RETRY + quote is None (missing canonical or
                    #     both bid/ask zero): seed AWAITING_FIRST_TRUTH
                    #   * STOP_BROKEN/TARGET_COMPLETE/MOVE_MISSED: terminal
                    # In every non-terminal path we set overnight=False and
                    # hand control to the normal poll loop.
                    _bug_d_quote = self._get_quote(w.ticker) or {}
                    _bug_d_bid = float(_bug_d_quote.get("bid") or 0)
                    _bug_d_ask = float(_bug_d_quote.get("ask") or 0)

                    # PR #388 P0-7 wiring: target complete uses the canonical
                    # underlying side. CALL: ask >= target. PUT: bid <= target.
                    _bug_d_target_complete = False
                    try:
                        _tgt = float(w.target_price or 0)
                        if _tgt > 0:
                            if w.side == "CALL" and _bug_d_ask > 0 and _bug_d_ask >= _tgt:
                                _bug_d_target_complete = True
                            elif w.side == "PUT" and _bug_d_bid > 0 and _bug_d_bid <= _tgt:
                                _bug_d_target_complete = True
                    except (TypeError, ValueError):
                        _bug_d_target_complete = False

                    try:
                        from ap.pending_trigger_classifier import (
                            classify_late_attachment as _pt_classify_late_open,
                            LATE_ATTACHMENT_AWAITING_FIRST_TRUTH as _PT_OPEN_AWAITING,
                            LATE_ATTACHMENT_WITHIN_CONTINUATION as _PT_OPEN_WITHIN,
                            MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET as _PT_OPEN_WAITING,
                            LATE_ATTACHMENT_MOVE_MISSED_TERMINAL as _PT_OPEN_MISSED,
                            STOP_ALREADY_BROKEN_TERMINAL as _PT_OPEN_STOP,
                            TARGET_ALREADY_COMPLETE_TERMINAL as _PT_OPEN_TARGET,
                            TRIGGER_TRUTH_UNAVAILABLE_RETRY as _PT_OPEN_RETRY,
                        )
                    except Exception as _pt_open_import_exc:
                        _pt_classify_late_open = None
                        _PT_OPEN_AWAITING = _PT_OPEN_WITHIN = _PT_OPEN_WAITING = None
                        _PT_OPEN_MISSED = _PT_OPEN_STOP = _PT_OPEN_TARGET = _PT_OPEN_RETRY = None
                        _pt_open_import_err = _pt_open_import_exc
                    else:
                        _pt_open_import_err = None

                    if _pt_classify_late_open is None:
                        # PR #388 Blocker #3: FAIL CLOSED on classifier import
                        # failure at open. The prior amendment armed the
                        # watcher normally, bypassing stop / target /
                        # continuation / waiting-reset / awaiting-truth
                        # classification precisely when the shared classifier
                        # was unavailable. Retain overnight=True + the pending
                        # queue state so the next reeval poll tries again;
                        # never terminalize, never arm normally.
                        log.critical(
                            "[%s] OPEN_REVALIDATION_CLASSIFIER_IMPORT_FAILED — "
                            "retaining overnight=True and queue_status=%s; "
                            "reeval will retry next poll: %s",
                            w.ticker,
                            OvernightWatchState.OPEN_RECHECK_PENDING,
                            _pt_open_import_err,
                        )
                        w.overnight = True
                        w.signal["queue_status"] = OvernightWatchState.OPEN_RECHECK_PENDING
                        continue

                    # PR #388 Blocker #4: wire decisive drift at open reval.
                    _open_decisive_drift = False
                    try:
                        _t_o = float(w.entry_trigger or 0)
                        if _t_o > 0:
                            if w.side == "CALL" and _bug_d_ask > 0:
                                if _bug_d_ask > _t_o * (1.0 + MAX_INTRADAY_DRIFT_PCT):
                                    _open_decisive_drift = True
                            elif w.side == "PUT" and _bug_d_bid > 0:
                                if _bug_d_bid < _t_o * (1.0 - MAX_INTRADAY_DRIFT_PCT):
                                    _open_decisive_drift = True
                    except (TypeError, ValueError):
                        _open_decisive_drift = False

                    _open_decision = _pt_classify_late_open(
                        side=w.side,
                        trigger_price=w.entry_trigger,
                        bid=_bug_d_bid,
                        ask=_bug_d_ask,
                        stop=w.stop_level,
                        target_complete=_bug_d_target_complete,
                        decisive_drift_exceeded=_open_decisive_drift,
                        trigger_previously_breached=(
                            getattr(w, "trigger_crossed_at", None) is not None
                        ),
                    )
                    _open_cls = _open_decision.classification

                    if _open_cls in (_PT_OPEN_STOP, _PT_OPEN_TARGET, _PT_OPEN_MISSED):
                        _open_reason_map = {
                            _PT_OPEN_STOP:   "stop_already_broken_terminal",
                            _PT_OPEN_TARGET: "target_already_complete_terminal",
                            _PT_OPEN_MISSED: "late_attachment_move_missed_terminal",
                        }
                        _open_reason = _open_reason_map[_open_cls]
                        _open_mid = (_bug_d_bid + _bug_d_ask) / 2.0 if (_bug_d_bid and _bug_d_ask) else max(_bug_d_bid, _bug_d_ask)
                        _open_audit = self._build_watcher_audit_payload(
                            w,
                            trigger_type="overnight_revalidation_late_attachment",
                            current_bid=_bug_d_bid,
                            current_ask=_bug_d_ask,
                            current_mid=_open_mid,
                            arm_condition=f"trigger_{w.entry_trigger:.4f}",
                            reason_code=_open_reason,
                            raw_reason=(
                                f"open_revalidation:{_open_cls}:"
                                f"{_open_decision.detail}"
                            ),
                            extra={
                                "is_daily": True, "validator_valid": True,
                                "open_late_attachment_class": _open_cls,
                            },
                        )
                        self._persist_watcher_audit(
                            w.signal.get("local_order_id"), _open_audit,
                        )
                        w.state = WatchState.EXPIRED
                        w.signal["queue_status"] = OvernightWatchState.INVALIDATED
                        log.warning(
                            "[%s] OVERNIGHT_DAILY_%s — %s trigger=%.4f "
                            "bid=%.4f ask=%.4f detail=%s",
                            w.ticker, _open_reason.upper(),
                            w.side, w.entry_trigger,
                            _bug_d_bid, _bug_d_ask, _open_decision.detail,
                        )
                        to_remove.append(w)
                    else:
                        # Non-terminal: seed the appropriate late-attachment
                        # state so the poll loop's gate owns confirmation,
                        # and hand off to the normal poll loop.
                        if _open_cls == _PT_OPEN_WITHIN:
                            w.late_attachment_state = _PT_OPEN_WITHIN
                            w.late_confirm_polls = 0
                            w.late_attachment_generation += 1
                            log.info(
                                "[%s] OVERNIGHT_DAILY_ARMED_WITHIN_CONTINUATION — "
                                "%s trigger=%.4f quote=%s (poll loop owns confirmation)",
                                w.ticker, w.side, w.entry_trigger, _open_decision.quote,
                            )
                        elif _open_cls == _PT_OPEN_WAITING:
                            w.late_attachment_state = _PT_OPEN_WAITING
                            w.late_reset_polls = 0
                            w.late_attachment_generation += 1
                            log.info(
                                "[%s] OVERNIGHT_DAILY_ARMED_WAITING_RESET — "
                                "%s trigger=%.4f quote=%s (poll loop owns reset+rebreach)",
                                w.ticker, w.side, w.entry_trigger, _open_decision.quote,
                            )
                        elif _open_cls == _PT_OPEN_RETRY and _open_decision.quote is None:
                            w.late_attachment_state = _PT_OPEN_AWAITING
                            w.late_attachment_generation += 1
                            log.info(
                                "[%s] OVERNIGHT_DAILY_ARMED_AWAITING_FIRST_TRUTH — "
                                "%s trigger=%.4f (canonical quote unavailable; "
                                "poll loop stays gated until first truthful quote)",
                                w.ticker, w.side, w.entry_trigger,
                            )
                        # else: pre-trigger (TRUTH_RETRY with valid quote) —
                        # ordinary arm; no gate seeded.
                        w.overnight = False
                        w.signal["queue_status"] = OvernightWatchState.VALID_AWAITING_BREACH
                        log.info(
                            "[%s] OVERNIGHT_DAILY_ARMED | side=%s | queue_status=%s | %s | "
                            "late_state=%s",
                            w.ticker,
                            w.side,
                            OvernightWatchState.VALID_AWAITING_BREACH,
                            getattr(result, "reason_text", "valid"),
                            w.late_attachment_state,
                        )
                continue

            # Generic/non-daily overnight revalidation from older stable watcher.
            try:
                quote = self._get_quote(w.ticker)
            except Exception as exc:
                log.warning("[%s] Overnight quote fetch failed: %s", w.ticker, exc)
                quote = {}

            bid = float(quote.get("bid") or 0)
            ask = float(quote.get("ask") or 0)
            w.last_quote_age_ms = self._coerce_quote_age_ms(quote.get("quote_age_ms"))
            if bid == 0 and ask == 0:
                last = float(quote.get("last") or 0)
                bid = ask = last

            if not (bid or ask) or not w.entry_trigger:
                # Mode-aware failure policy:
                # LIVE: PR #324 Failure A fix — bounded retry, NOT inert INVALIDATED.
                #       Quote outage at open is transient; hold ownership until deadline.
                # PAPER: fail open (arm watcher) — sandbox, not money protection.
                _is_live_watcher = self._is_live_runtime()
                if _is_live_watcher:
                    _now_retry = datetime.now(timezone.utc)
                    # Read env-controlled bounds; clamp to safe ranges.
                    try:
                        _retry_delay = max(5, int(os.getenv(
                            "WATCHER_OVERNIGHT_QUOTE_RETRY_DELAY_SECONDS", "30"
                        )))
                    except (TypeError, ValueError):
                        _retry_delay = 30
                    try:
                        _retry_deadline_secs = max(60, int(os.getenv(
                            "WATCHER_OVERNIGHT_QUOTE_RETRY_DEADLINE_SECONDS", "180"
                        )))
                    except (TypeError, ValueError):
                        _retry_deadline_secs = 180
                    try:
                        _retry_max = max(1, int(os.getenv(
                            "WATCHER_OVERNIGHT_QUOTE_RETRY_MAX_ATTEMPTS", "6"
                        )))
                    except (TypeError, ValueError):
                        _retry_max = 6

                    # PR #324 §7 — next_at guard: skip retry if too early.
                    if (
                        w._overnight_quote_retry_next_at is not None
                        and _now_retry < w._overnight_quote_retry_next_at
                    ):
                        # Not yet due — retain without incrementing attempt.
                        continue

                    # Initialise deadline on first failure.
                    if w._overnight_quote_retry_first_failed_at is None:
                        w._overnight_quote_retry_first_failed_at = _now_retry
                        w._overnight_quote_retry_deadline = (
                            _now_retry + timedelta(seconds=_retry_deadline_secs)
                        )
                    w._overnight_quote_retry_attempt += 1
                    w._overnight_quote_retry_last_failed_at = _now_retry

                    _deadline = w._overnight_quote_retry_deadline
                    _attempts = w._overnight_quote_retry_attempt
                    _deadline_expired = (
                        _deadline is not None and _now_retry >= _deadline
                    ) or _attempts > _retry_max

                    if not _deadline_expired:
                        # Within bounds — persist retry metadata then RETRY_OWNED.
                        _next_at_ts = _now_retry + timedelta(seconds=_retry_delay)
                        _ov_retry_meta = {
                            "watcher_invalidation_class":    "INVALIDATED_RETRYABLE",
                            "watcher_invalidation_reason":   "overnight_live_quote_unavailable",
                            "watcher_retry_owner":           f"overnight_quote_retry:{w.signal.get('local_order_id', '')}",
                            "watcher_retry_attempt":         _attempts,
                            "watcher_retry_first_failed_at": w._overnight_quote_retry_first_failed_at.isoformat(),
                            "watcher_retry_last_failed_at":  _now_retry.isoformat(),
                            "watcher_retry_next_at":         _next_at_ts.isoformat(),
                            "watcher_retry_deadline":        (_deadline.isoformat() if _deadline else None),
                            "mode": "LIVE",
                        }
                        # PR #324 §7 — metadata write must succeed; failure → FAILED quarantine.
                        _meta_write_ok = False
                        _meta_write_exc = None
                        _osm = self.order_state_machine
                        _local_oid = str(w.signal.get("local_order_id") or "").strip()
                        if _osm is not None and _local_oid:
                            _upd = getattr(_osm, "update_order_meta", None)
                            if callable(_upd):
                                try:
                                    _meta_write_ok = bool(_upd(_local_oid, _ov_retry_meta))
                                except Exception as _mwe:
                                    _meta_write_exc = _mwe
                                    _meta_write_ok = False
                            else:
                                _meta_write_ok = False  # helper missing → FAILED
                        else:
                            _meta_write_ok = False  # OSM or oid missing → FAILED

                        if not _meta_write_ok:
                            log.critical(
                                "[%s] overnight_quote_retry meta write FAILED (exc=%s) — "
                                "cannot claim RETRY_OWNED without durable metadata. "
                                "Entering quarantine (watcher retained, dedup held).",
                                w.ticker, _meta_write_exc,
                            )
                            from ap.pending_trigger_classifier import (
                                WatcherCompletionResult as _OVWCR,
                                WatcherCompletionOutcome as _OVWCO,
                            )
                            _fail_r = _OVWCR(
                                outcome=_OVWCO.FAILED,
                                reason_code="overnight_retry_metadata_persistence_failed",
                                local_order_id=_local_oid or None,
                                detail=str(_meta_write_exc)[:200] if _meta_write_exc else "write_returned_false",
                            )
                            self._enter_ownership_quarantine(w, _fail_r)
                            continue

                        # Persist in-memory next_at after confirmed meta write.
                        w._overnight_quote_retry_next_at = _next_at_ts

                        _ov_quot_audit = self._build_watcher_audit_payload(
                            w,
                            trigger_type="overnight_revalidation",
                            current_bid=0.0, current_ask=0.0, current_mid=0.0,
                            reason_code="overnight_live_quote_unavailable",
                            raw_reason="live_overnight_recheck_quote_zero_retry",
                            extra={
                                "mode": "LIVE",
                                "quote_available": False,
                                "retry_attempt": _attempts,
                                "retry_deadline": _deadline.isoformat() if _deadline else None,
                                "retry_next_at": _next_at_ts.isoformat(),
                            },
                        )
                        self._persist_watcher_audit(w.signal.get("local_order_id"), _ov_quot_audit)
                        log.warning(
                            "[%s] LIVE overnight recheck: quote unavailable — "
                            "RETRY_OWNED (attempt %d, deadline %s, next_at %s). "
                            "Watcher retained; dedup held.",
                            w.ticker, _attempts,
                            _deadline.isoformat() if _deadline else "none",
                            _next_at_ts.isoformat(),
                        )
                        # Watcher stays PENDING (is_active=True), dedup held.
                        continue

                    else:
                        # Deadline/attempts exhausted — terminalize via _dispatch_completion.
                        _timeout_reason = "overnight_live_quote_unavailable_timeout"
                        _ov_timeout_audit = self._build_watcher_audit_payload(
                            w,
                            trigger_type="overnight_revalidation",
                            current_bid=0.0, current_ask=0.0, current_mid=0.0,
                            reason_code=_timeout_reason,
                            raw_reason=(
                                f"live_overnight_quote_unavailable_after_{_attempts}_attempts"
                                f"_deadline_{_deadline.isoformat() if _deadline else 'none'}"
                            ),
                            extra={
                                "mode": "LIVE",
                                "retry_attempts": _attempts,
                                "retry_deadline": _deadline.isoformat() if _deadline else None,
                                "watcher_invalidation_class": "INVALIDATED_TERMINAL",
                                "watcher_invalidation_reason": _timeout_reason,
                            },
                        )
                        w.state = WatchState.EXPIRED
                        w.signal["queue_status"] = OvernightWatchState.INVALIDATED
                        log.warning(
                            "[%s] LIVE overnight recheck: quote unavailable after %d attempts — "
                            "terminalizing via dispatcher reason=%s",
                            w.ticker, _attempts, _timeout_reason,
                        )
                        # Universal dispatcher — handles verification, removal, dedup release, quarantine.
                        _ov_to_expire_cb = self.on_expire
                        self._dispatch_completion(
                            w, _ov_to_expire_cb or (lambda _w: None),
                            pre_computed_audit=_ov_timeout_audit,
                        )
                        continue
                else:
                    w.overnight = False
                    log.warning("[%s] PAPER overnight recheck: quote unavailable — arming fail-open", w.ticker)
                continue

            mid = (bid + ask) / 2.0 if bid and ask else max(bid, ask)
            if not mid:
                w.overnight = False
                continue

            drift = (mid - w.entry_trigger) / w.entry_trigger
            premarket_breached = (w.side == "CALL" and mid >= w.entry_trigger * 1.005) or (
                w.side == "PUT" and mid <= w.entry_trigger * 0.995
            )
            too_far = (w.side == "CALL" and drift > OVERNIGHT_MAX_DRIFT_PCT) or (
                w.side == "PUT" and drift < -OVERNIGHT_MAX_DRIFT_PCT
            )

            if premarket_breached:
                _ov_pre_audit = self._build_watcher_audit_payload(
                    w,
                    trigger_type="overnight_revalidation",
                    current_bid=bid,
                    current_ask=ask,
                    current_mid=mid,
                    arm_condition=f"trigger_{w.entry_trigger:.4f}",
                    stop_condition=f"stop_{w.stop_level:.4f}" if w.stop_level else "",
                    reason_code="overnight_premarket_breached",
                    raw_reason=f"mid_{mid:.4f}_already_through_trigger_{w.entry_trigger:.4f}",
                    extra={"drift_pct": round(drift * 100.0, 4)},
                )
                self._persist_watcher_audit(w.signal.get("local_order_id"), _ov_pre_audit)
                w.state = WatchState.EXPIRED
                log.info(
                    "[%s] OVERNIGHT INVALIDATED — pre-market breach detected. "
                    "Price $%.2f already through trigger $%.2f. Move done; expiring.",
                    w.ticker,
                    mid,
                    w.entry_trigger,
                )
                to_remove.append(w)
            elif too_far:
                _ov_drift_audit = self._build_watcher_audit_payload(
                    w,
                    trigger_type="overnight_revalidation",
                    current_bid=bid,
                    current_ask=ask,
                    current_mid=mid,
                    arm_condition=f"trigger_{w.entry_trigger:.4f}",
                    stop_condition=f"stop_{w.stop_level:.4f}" if w.stop_level else "",
                    reason_code="overnight_too_far_from_trigger",
                    raw_reason=(
                        f"mid_{mid:.4f}_drifted_{drift*100.0:+.2f}pct"
                        f"_from_trigger_{w.entry_trigger:.4f}"
                        f"_max_{OVERNIGHT_MAX_DRIFT_PCT*100:.1f}pct"
                    ),
                    extra={"drift_pct": round(drift * 100.0, 4)},
                )
                self._persist_watcher_audit(w.signal.get("local_order_id"), _ov_drift_audit)
                w.state = WatchState.EXPIRED
                log.info(
                    "[%s] OVERNIGHT INVALIDATED — price $%.2f drifted %.2f%% "
                    "from trigger $%.2f overnight. Expiring stale setup.",
                    w.ticker,
                    mid,
                    drift * 100.0,
                    w.entry_trigger,
                )
                to_remove.append(w)
            else:
                w.overnight = False
                log.info(
                    "[%s] OVERNIGHT VALIDATED at open — price $%.2f within %.2f%% "
                    "of trigger $%.2f. Arming for breach detection.",
                    w.ticker,
                    mid,
                    drift * 100.0,
                    w.entry_trigger,
                )

        # PR #324 §2 — universal dispatcher replaces the old to_remove fire-and-forget loop.
        # Every watcher that needs terminalization goes through _dispatch_completion which:
        #   callback → verify (identity + durable row) → remove + dedup | quarantine
        # Dedup release and _pending removal happen ONLY after TERMINALIZED is verified.
        for w in to_remove:
            _sig_id = str(w.signal.get("signal_id", ""))
            _ticker = str(w.ticker or "")
            if _sig_id and _ticker:
                _reason = (
                    "overnight_revalidation_invalidated"
                    if w.state == WatchState.INVALIDATED
                    else "overnight_revalidation_expired"
                )
                _ew_record(
                    _sig_id, _ticker,
                    "INVALIDATED" if w.state == WatchState.INVALIDATED else "EXPIRED",
                    _reason,
                    queue_status=str(w.signal.get("queue_status", "")),
                )
            _ov_cb = (
                self.on_invalidate if w.state == WatchState.INVALIDATED else self.on_expire
            )
            _ov_pending_audit = (
                getattr(w, "_pending_audit", None)
                if w.state == WatchState.INVALIDATED else None
            )
            self._dispatch_completion(
                w, _ov_cb or (lambda _w: None),
                pre_computed_audit=_ov_pending_audit,
            )

        if to_remove:
            log.info("[WATCHER] Overnight revalidation: %d watchers dispatched", len(to_remove))

    def _before_trigger_dispatch(self, completed):
        """Hook for a watcher implementation to arbitrate trigger batches."""
        return completed

    def _open_protection_key(self, watched):
        """Return the identity used by the base market-open duplicate barrier."""
        return str(getattr(watched, "ticker", "") or "").strip().upper()

    @staticmethod
    def _open_protection_owner(watched):
        """Identify the exact watcher allowed to retry after open admission."""
        registration_token = str(
            getattr(watched, "_registration_token", "") or ""
        ).strip()
        if registration_token:
            return ("registration", registration_token)
        signal = getattr(watched, "signal", {}) or {}
        return (
            "legacy",
            str(signal.get("local_order_id") or "").strip(),
            str(signal.get("signal_id") or getattr(watched, "signal_id", "") or "").strip(),
        )

    def _apply_open_protection(self, completed, open_protect_active: bool):
        """Apply market-open protection after any trigger arbitration hook.

        Arbitration must see every watcher that confirmed in this poll.  Applying
        ticker protection while the watcher list is still being classified can
        discard a candidate before a more specific implementation can choose a
        durable winner.
        """
        if not open_protect_active:
            return completed

        protected = []
        for action, watched in completed:
            if action != "trigger":
                protected.append((action, watched))
                continue

            key = self._open_protection_key(watched)
            ticker = str(getattr(watched, "ticker", "") or "").strip().upper()
            owner = self._open_protection_owner(watched)
            already_triggered = key in self._open_trigger_keys
            # Preserve compatibility with callers/tests that seed the historical
            # ticker-only set directly before the first protected poll.
            if not already_triggered and not self._open_trigger_keys:
                already_triggered = ticker in self._open_trigger_tickers

            if already_triggered:
                if self._open_trigger_owners.get(key) == owner:
                    # A transient callback failure/RETRY_WAIT leaves this exact
                    # watcher pending. Preserve its retry ownership while still
                    # blocking every different watcher for the same key.
                    protected.append(("trigger", watched))
                    continue
                watched.state = WatchState.EXPIRED
                protected.append(("done", watched))
                log.info(
                    "[%s] OPEN_PROTECTION_BLOCK — identity already triggered at open",
                    watched.ticker,
                )
                continue

            self._open_trigger_count += 1
            self._open_trigger_keys.add(key)
            self._open_trigger_owners[key] = owner
            if ticker:
                self._open_trigger_tickers.add(ticker)
            protected.append(("trigger", watched))

        return protected

    def _persist_trigger_confirmation_authority(
        self, watched, *, require_pending_row: bool = False
    ) -> bool:
        """Persist trigger authority before any downstream destructive action.

        Direction claims may cancel an opposite pending lifecycle immediately
        after this write.  Those claims therefore use the optional
        ``require_pending_row`` fence so the metadata write is conditional on
        the durable row still being ``PENDING_TRIGGER``.  Ordinary callback
        persistence keeps the historical two-argument merge semantics.
        """
        if getattr(watched, "_trigger_authority_persisted", False):
            return True

        signal = getattr(watched, "signal", {}) or {}
        local_order_id = str(signal.get("local_order_id") or "").strip()
        trigger_crossed_at = getattr(watched, "trigger_crossed_at", None)
        update_order_meta = getattr(self.order_state_machine, "update_order_meta", None)
        if not local_order_id or trigger_crossed_at is None or not callable(update_order_meta):
            return False

        patch = {
            "trigger_crossed_at": (
                trigger_crossed_at.isoformat()
                if hasattr(trigger_crossed_at, "isoformat")
                else str(trigger_crossed_at)
            ),
            "trigger_crossed_at_provenance": _build_trigger_crossed_at_provenance(
                signal, local_order_id
            ),
            "trigger_confirmed_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            first_bid = float(getattr(watched, "first_breach_bid", 0) or 0)
            first_ask = float(getattr(watched, "first_breach_ask", 0) or 0)
        except (TypeError, ValueError):
            first_bid = first_ask = 0.0
        if first_bid:
            patch["first_breach_bid"] = first_bid
        if first_ask:
            patch["first_breach_ask"] = first_ask

        try:
            if require_pending_row:
                persisted = bool(
                    update_order_meta(
                        local_order_id,
                        patch,
                        expected_status="PENDING_TRIGGER",
                    )
                )
            else:
                persisted = bool(update_order_meta(local_order_id, patch))
            if not persisted:
                return False
        except Exception:
            log.exception(
                "WATCHER_TRIGGER_AUTHORITY_PERSIST_FAILED local_order_id=%s",
                local_order_id,
            )
            return False

        watched._trigger_authority_persisted = True
        log.info(
            "WATCHER_TRIGGER_TIMESTAMPS_PERSISTED "
            "WATCHER_TRIGGER_AUTHORITY_PERSISTED local_order_id=%s "
            "trigger_crossed_at=%s trigger_confirmed_at=%s",
            local_order_id,
            patch["trigger_crossed_at"],
            patch["trigger_confirmed_at"],
        )
        return True

    def _poll_active_signals(self, open_protect_active: bool) -> None:
        with self._lock:
            # PR 158 P1 — RETRY_LATER watchers must not be trigger-polled.
            # w.overnight=True means the overnight open-revalidation has NOT
            # yet passed for this watcher. _revalidate_overnight_at_open() sets
            # w.overnight=False only when the validator returns valid. Until
            # that happens, the watcher must stay alive but cannot trigger —
            # triggering against unvalidated overnight structure is incorrect.
            # Excluding w.overnight=True here is the single gate that enforces
            # this: no other code path in _poll_active_signals can trigger an
            # overnight watcher whose revalidation is still pending.
            active = [w for w in self._pending if w.is_active and not w.overnight]

        if not active:
            return

        tickers = list({w.ticker for w in active})
        try:
            quotes = self._fetch_quotes(tickers)
        except Exception as exc:
            log.warning("Quote fetch failed: %s", exc)
            return

        completed = []
        with self._lock:
            for w in active:
                _retry_not_before = getattr(w, "deferred_retry_not_before", None)
                if _retry_not_before is not None:
                    try:
                        if isinstance(_retry_not_before, str):
                            _retry_not_before = datetime.fromisoformat(_retry_not_before)
                        if _retry_not_before.tzinfo is None:
                            _retry_not_before = _retry_not_before.replace(tzinfo=timezone.utc)
                        if datetime.now(timezone.utc) < _retry_not_before:
                            continue
                        w.deferred_retry_not_before = None
                    except Exception:
                        # Invalid retry timestamps never create an indefinite wait;
                        # clear and let the normal breach check re-prove the trigger.
                        w.deferred_retry_not_before = None
                quote = quotes.get(w.ticker)
                if not isinstance(quote, dict):
                    quote = {}

                # Keep raw canonical side values intact for WatchedSignal.check().
                # The watcher owns type/finite/positive validation and must see
                # (None, None) for an absent quote. LAST is never promoted to
                # trigger authority; the exported shim may turn LAST-only
                # observations into an empty quote, which must still reach this
                # check() call so bounded continuity is evaluated immediately.
                bid = quote.get("bid")
                ask = quote.get("ask")
                _quote_age_ms = self._coerce_quote_age_ms(quote.get("quote_age_ms"))
                w.last_quote_age_ms = _quote_age_ms

                new_state = w.check(bid, ask, quote_age_ms=_quote_age_ms)
                if new_state == WatchState.TRIGGERED:
                    completed.append(("trigger", w))
                elif new_state in (WatchState.EXPIRED, WatchState.INVALIDATED):
                    completed.append(("done", w))

            # ── P0 (PR #304) Bug B fix: DO NOT remove triggered watchers here.
            # ── PR #324: DO NOT remove EXPIRED/INVALIDATED watchers here either.
            #
            # Triggered watchers stay in _pending and are removed below ONLY on
            # on_trigger success or exhaustion (PR #304 Bug B fix, unchanged).
            #
            # EXPIRED/INVALIDATED watchers must also stay in _pending until their
            # cleanup callback is verified against the durable row (PR #324 Failure C
            # fix).  Upfront removal made the callback fire-and-forget: if on_expire
            # or on_invalidate raised or persisted nothing, the watcher was already
            # gone and the row zombied as ownerless PENDING_TRIGGER.
            #
            # New ordering (see dispatch loop below):
            #   check() → classify → callback → verify result → then remove + dedup release
            #   FAILED: enter quarantine (stay in _pending, dedup held, not active)
            pass  # removal now handled per-watcher after callback verification

        # The package watcher uses this post-check/pre-callback seam to make a
        # batch-level confirmed-breach direction claim.  It may remove proven
        # losers or convert an ambiguous batch to a fail-closed hold.
        completed = self._before_trigger_dispatch(completed)
        completed = self._apply_open_protection(completed, open_protect_active)

        for action, w in completed:
            _sig_id = str(w.signal.get("signal_id", ""))
            _ticker = str(w.ticker or "")
            if action == "trigger":
                # Signal breached — record TRIGGER_READY before firing callback.
                _trigger_audit = self._build_watcher_audit_payload(
                    w,
                    trigger_type="trigger",
                    current_bid=float(getattr(w, "last_quote_bid", 0) or 0),
                    current_ask=float(getattr(w, "last_quote_ask", 0) or 0),
                    reason_code="trigger_ready",
                    raw_reason=(
                        f"{str(getattr(w, 'side', '')).lower()}_breach_confirmed"
                        f"_after_{int(getattr(w, 'breach_count', 0) or 0)}_polls"
                    ),
                    extra={
                        "breach_count": int(getattr(w, "breach_count", 0) or 0),
                        "queue_status": str((getattr(w, "signal", {}) or {}).get("queue_status") or ""),
                    },
                )
                self._persist_watcher_audit(
                    (getattr(w, "signal", {}) or {}).get("local_order_id"),
                    _trigger_audit,
                )
                if _sig_id and _ticker:
                    _ew_record(_sig_id, _ticker, "TRIGGER_READY",
                               "trigger_breached_entry_submitted",
                               contract=str(w.signal.get("contract_symbol", "")),
                               entry_trigger=str(w.entry_trigger or ""))
                if self.on_trigger:
                    # Retry on_trigger up to 3 times before expiring.
                    # A transient Tradier timeout or DB hiccup at breach time
                    # must not permanently kill a valid setup. On all 3 failures
                    # the signal is expired with a clear reason — not silently lost.
                    _trigger_attempts = getattr(w, "_trigger_attempts", 0)
                    log.info(
                        "WATCHER_TRIGGER_CALLBACK_ATTEMPT "
                        "ticker=%s signal_id=%s attempt=%d/3 kept_in_pending=true",
                        w.ticker, _sig_id or "?", _trigger_attempts + 1,
                    )
                    # ── Final amendment (PR #306): persist trigger timestamps
                    # BEFORE calling on_trigger.
                    #
                    # WHY: on_trigger is synchronous and immediately enters the
                    # execution/materialization/submit path. The LIVE submit gate
                    # reads trigger_crossed_at from orders.meta to compute trigger
                    # age. If we persist AFTER on_trigger returns, the gate runs
                    # against an empty orders.meta and either passes (stale-miss)
                    # or relies on an in-memory fallback that may not be in scope.
                    # Durable timestamp must exist BEFORE execution can reach the gate.
                    _ts_pre_local_oid = str(
                        ((getattr(w, "signal", {}) or {}).get("local_order_id") or "")
                    ).strip()
                    _ts_pre_write_ok = self._persist_trigger_confirmation_authority(w)
                    if not _ts_pre_write_ok:
                        _is_live_ts = self._is_live_runtime()
                        if _is_live_ts:
                            log.critical(
                                "[%s] WATCHER_TRIGGER_TIMESTAMP_PERSIST_FAILED — "
                                "LIVE mode, trigger timestamps could not be written "
                                "to orders.meta before on_trigger; callback is "
                                "skipped and persistence will be retried. "
                                "local_order_id=%s",
                                w.ticker, _ts_pre_local_oid or "?",
                            )
                        else:
                            log.debug(
                                "[%s] trigger timestamp pre-persist non-critical "
                                "local_order_id=%s",
                                w.ticker, _ts_pre_local_oid or "?",
                            )

                    if (
                        self._is_live_runtime()
                        and not _ts_pre_write_ok
                    ):
                        # Database truth is unavailable.  Keep the watcher as the
                        # active owner and never enter selector/broker work.
                        with self._lock:
                            w.state = WatchState.PENDING
                            w.deferred_retry_not_before = (
                                datetime.now(timezone.utc) + timedelta(seconds=5)
                            )
                        log.critical(
                            "WATCHER_TRIGGER_PERSISTENCE_RETRY "
                            "ticker=%s local_order_id=%s kept_in_pending=true",
                            w.ticker, _ts_pre_local_oid or "?",
                        )
                        continue

                    # ── Call the trigger callback — timestamps are now durable ──
                    try:
                        _callback_result = self.on_trigger(w)
                        _callback_disposition, _callback_next_retry = (
                            self._resolve_trigger_callback_disposition(w, _callback_result)
                        )
                        if _callback_disposition in {"RETRY_WAIT", "KEEP_WATCHER", "RECONCILE_BROKER_INTENT"}:
                            with self._lock:
                                w.state = WatchState.PENDING
                                if _callback_next_retry:
                                    try:
                                        w.deferred_retry_not_before = datetime.fromisoformat(
                                            str(_callback_next_retry)
                                        )
                                    except Exception:
                                        w.deferred_retry_not_before = (
                                            datetime.now(timezone.utc) + timedelta(seconds=5)
                                        )
                                else:
                                    _retry_after = 5
                                    if isinstance(_callback_result, dict):
                                        try:
                                            _retry_after = max(
                                                1, int(_callback_result.get("retry_after_seconds") or 5)
                                            )
                                        except Exception:
                                            _retry_after = 5
                                    w.deferred_retry_not_before = (
                                        datetime.now(timezone.utc)
                                        + timedelta(seconds=_retry_after)
                                    )
                            log.warning(
                                "WATCHER_TRIGGER_OWNERSHIP_RETAINED "
                                "ticker=%s signal_id=%s disposition=%s next_retry_at=%s",
                                w.ticker, _sig_id or "?", _callback_disposition,
                                getattr(w, "deferred_retry_not_before", None),
                            )
                            continue
                        if _callback_disposition == "UNKNOWN":
                            raise RuntimeError(
                                "deferred_trigger_callback_returned_without_durable_outcome"
                            )
                        w._trigger_attempts = 0   # reset on success
                        # ── P0 (PR #304) Bug B: explicit removal on SUCCESS.
                        # Since Bug B fix stopped removing triggered watchers
                        # upfront (they used to zombie on retry), success now
                        # needs an explicit removal so the watcher doesn't
                        # re-fire on the next poll cycle. Uses id-based match
                        # to avoid mutating _pending during callback iteration.
                        with self._lock:
                            _wid = id(w)
                            self._pending = [_p for _p in self._pending if id(_p) != _wid]
                        try:
                            w._release_dedup_key()
                        except Exception:
                            pass
                        log.info(
                            "WATCHER_TRIGGER_CALLBACK_OK ticker=%s signal_id=%s "
                            "removed_from_pending=true dedup_released=true "
                            "timestamps_pre_persisted=%s",
                            w.ticker, _sig_id or "?", _ts_pre_write_ok,
                        )
                        # Post-success: log marker only — timestamps already durable.
                        log.debug(
                            "WATCHER_TRIGGER_TIMESTAMPS_CONFIRMED "
                            "local_order_id=%s pre_write_ok=%s",
                            _ts_pre_local_oid or "?", _ts_pre_write_ok,
                        )
                        # Diagnostic-only — proves the callback ran cleanly.
                        # Pair with BREACH_RISK_CHECK_BLOCKED / ENTRY_TRIGGER_BLOCKED_RETURN
                        # to determine whether a watcher trigger was consumed but blocked.
                        try:
                            _sig = getattr(w, "signal", {}) or {}
                            log.info(
                                "WATCHER_ON_TRIGGER_RETURNED "
                                "client_id=%s local_order_id=%s signal_id=%s "
                                "symbol=%s contract=%s callback_wired=true "
                                "attempt=%d outcome=returned",
                                _sig.get("client_email") or "n/a",
                                _sig.get("local_order_id") or "n/a",
                                _sig.get("signal_id") or "n/a",
                                w.ticker,
                                (
                                    (_sig.get("plan") or {}).get("contract_symbol")
                                    or _sig.get("contract_symbol")
                                    or _sig.get("contract")
                                    or "n/a"
                                ),
                                _trigger_attempts + 1,
                            )
                        except Exception:
                            pass
                    except Exception as exc:
                        _trigger_attempts += 1
                        w._trigger_attempts = _trigger_attempts
                        log.error(
                            "[%s] on_trigger callback failed (attempt %d/3): %s",
                            w.ticker, _trigger_attempts, exc, exc_info=True,
                        )
                        # Diagnostic-only — structured pairing line for the exception.
                        try:
                            _sig = getattr(w, "signal", {}) or {}
                            log.error(
                                "WATCHER_ON_TRIGGER_EXCEPTION "
                                "client_id=%s local_order_id=%s signal_id=%s "
                                "symbol=%s contract=%s callback_wired=true "
                                "attempt=%d outcome=exception "
                                "exception_type=%s exception_message=%s",
                                _sig.get("client_email") or "n/a",
                                _sig.get("local_order_id") or "n/a",
                                _sig.get("signal_id") or "n/a",
                                w.ticker,
                                (
                                    (_sig.get("plan") or {}).get("contract_symbol")
                                    or _sig.get("contract_symbol")
                                    or _sig.get("contract")
                                    or "n/a"
                                ),
                                _trigger_attempts,
                                type(exc).__name__,
                                str(exc)[:200],
                            )
                        except Exception:
                            pass
                        if _trigger_attempts < 3:
                            # ── P0 (PR #304) Bug B: HONEST retry.
                            # Previously this claimed "will retry" but the
                            # watcher had already been removed from _pending
                            # by the done_ids filter above — no retry ever
                            # happened. Now:
                            #   • watcher stayed in _pending (Bug B fix above)
                            #   • state must be reset to PENDING so
                            #     _poll_active_signals() sees it as active
                            #     and check() re-runs on the next poll
                            #   • dedup key stays held
                            # This is the ONLY code path in the loop where
                            # a triggered watcher survives to the next tick.
                            with self._lock:
                                w.state = WatchState.PENDING
                                # Preserve breach_count so momentum polls don't
                                # have to restart from zero if the retry fires
                                # right away; check() will re-verify breach on
                                # the next tick anyway.
                            log.warning(
                                "WATCHER_TRIGGER_CALLBACK_RETRY_SCHEDULED "
                                "ticker=%s signal_id=%s attempt=%d/3 "
                                "state_reset_to_pending=true dedup_held=true "
                                "kept_in_pending=true",
                                w.ticker, _sig_id or "?", _trigger_attempts,
                            )
                            continue  # stay in poll loop, retry on next tick
                        else:
                            # PR #324 §8 — trigger exhaustion: quarantine on failure, verify on success.
                            # Do NOT reset _trigger_attempts to 0 on failure.
                            # Do NOT release dedup in finally — only released after verified TERMINALIZED.
                            log.error(
                                "WATCHER_TRIGGER_CALLBACK_EXHAUSTED_EXPIRED "
                                "ticker=%s signal_id=%s attempts=3 last_error=%s",
                                w.ticker, _sig_id or "?", exc,
                            )
                            _exh_oid = str((getattr(w, "signal", {}) or {}).get("local_order_id") or "")
                            _exh_audit = None
                            try:
                                _exh_audit = self._build_watcher_audit_payload(
                                    w,
                                    trigger_type="trigger",
                                    reason_code="on_trigger_exhausted_3_attempts",
                                    raw_reason=f"on_trigger_failed_3_times_last_error={type(exc).__name__}",
                                    extra={"exception_type": type(exc).__name__,
                                           "exception_message": str(exc)[:200]},
                                )
                            except Exception:
                                pass

                            _terminalize_fn = getattr(
                                self.order_state_machine, "terminalize_deferred_breach", None
                            )
                            _exh_terminalized = False
                            _exh_term_exc = None
                            if callable(_terminalize_fn):
                                try:
                                    _exh_terminalized = bool(_terminalize_fn(
                                        _exh_oid,
                                        reason_code="on_trigger_exhausted_3_attempts",
                                        terminal_status="ERROR",
                                        diagnostics={
                                            "exception_class": type(exc).__name__,
                                            "safe_error_text": str(exc)[:200],
                                            "failure_stage": "trigger_callback",
                                            "diagnostic_id": str(uuid.uuid4()),
                                        },
                                    ))
                                except Exception as _te:
                                    _exh_terminalized = False
                                    _exh_term_exc = _te

                            w.state = WatchState.EXPIRED
                            if _sig_id and _ticker:
                                _ew_record(_sig_id, _ticker, "EXPIRED", "on_trigger_exhausted_3_attempts")

                            if _exh_terminalized:
                                # Terminalization succeeded — verify via dispatcher
                                from ap.pending_trigger_classifier import (
                                    WatcherCompletionResult as _TEXH_WCR,
                                    WatcherCompletionOutcome as _TEXH_WCO,
                                )
                                _pre_result = _TEXH_WCR(
                                    outcome=_TEXH_WCO.TERMINALIZED,
                                    reason_code="on_trigger_exhausted_3_attempts",
                                    local_order_id=_exh_oid or None,
                                )
                                self._dispatch_completion(
                                    w, _pre_result, pre_computed_audit=_exh_audit
                                )
                            else:
                                # Terminalization failed or raised — quarantine.
                                # Do NOT reset _trigger_attempts. Do NOT release dedup.
                                _fail_reason = (
                                    f"trigger_exhaustion_terminal_write_failed:"
                                    f"{type(_exh_term_exc).__name__ if _exh_term_exc else 'returned_false'}"
                                )
                                log.critical(
                                    "WATCHER_TRIGGER_CALLBACK_EXHAUSTED_TERMINAL_WRITE_FAILED "
                                    "ticker=%s signal_id=%s local_order_id=%s "
                                    "terminalize_raised=%s → ownership quarantine",
                                    w.ticker, _sig_id or "?", _exh_oid or "?",
                                    _exh_term_exc,
                                )
                                from ap.pending_trigger_classifier import (
                                    WatcherCompletionResult as _TEXH_WCR,
                                    WatcherCompletionOutcome as _TEXH_WCO,
                                )
                                _fail_result = _TEXH_WCR(
                                    outcome=_TEXH_WCO.FAILED,
                                    reason_code=_fail_reason,
                                    local_order_id=_exh_oid or None,
                                )
                            if _exh_terminalized:
                                continue
                            # Quarantine path: enter quarantine, do NOT continue
                            with self._lock:
                                w.state = WatchState.PENDING
                            self._enter_ownership_quarantine(w, _fail_result)
                            continue
                else:
                    # No on_trigger callback wired — must still go through dispatcher.
                    # PR #324 §8: "no callback" path must produce verified terminal or quarantine.
                    log.error("[%s] TRIGGERED but no on_trigger callback is wired", w.ticker)
                    _nowcb_oid = str((getattr(w, "signal", {}) or {}).get("local_order_id") or "")
                    w.state = WatchState.EXPIRED
                    if _sig_id and _ticker:
                        _ew_record(_sig_id, _ticker, "EXPIRED", "no_on_trigger_callback_wired")
                    _nocb_expire = self.on_expire
                    self._dispatch_completion(w, _nocb_expire or (lambda _w: None))
            elif w.state in (WatchState.EXPIRED, WatchState.INVALIDATED):
                # PR #324 §2 — universal dispatcher handles all verification + removal.
                _is_invalidated = w.state == WatchState.INVALIDATED
                _state_name = "INVALIDATED" if _is_invalidated else "EXPIRED"

                if _sig_id and _ticker:
                    _ew_record(
                        _sig_id, _ticker, _state_name,
                        "signal_invalidated_in_poll_loop" if _is_invalidated
                        else "signal_expired_in_poll_loop",
                        minutes_watching=str(getattr(w, "minutes_watching", "?")),
                    )

                _poll_cb = self.on_invalidate if _is_invalidated else self.on_expire
                _poll_audit = getattr(w, "_pending_audit", None) if _is_invalidated else None
                self._dispatch_completion(
                    w, _poll_cb or (lambda _w: None),
                    pre_computed_audit=_poll_audit,
                )

    # ── PR #324: universal completion dispatcher + verification helpers ────────

    def _dispatch_completion(
        self,
        w: "WatchedSignal",
        callback,
        *,
        pre_computed_audit: Optional[dict] = None,
    ) -> "WatcherCompletionResult":
        """Universal watcher completion dispatcher — single path for ALL outcomes.

        PR #324 §2 — replaces scattered fire-and-forget remove+release patterns.

        Required sequence:
          set decision state + exact audit reason (done by caller before dispatch)
          → invoke callback (or use pre-computed result)
          → normalize typed acknowledgment
          → verify registry + durable database state
          → TERMINALIZED: remove from _pending + release dedup
          → RETRY_OWNED / REARMED: retain in _pending (no removal)
          → FAILED: _enter_ownership_quarantine (retained, not active)
        """
        # Persist pending audit before callback so cleanup can read the row.
        if pre_computed_audit:
            try:
                w._pending_audit = pre_computed_audit
                _sig_for_audit = getattr(w, "signal", {}) or {}
                self._persist_watcher_audit(
                    _sig_for_audit.get("local_order_id"), pre_computed_audit
                )
            except Exception:
                pass

        _cb_result = None
        _cb_exc = None
        try:
            if callable(callback):
                _cb_result = callback(w)
            else:
                _cb_result = callback  # pre-computed WatcherCompletionResult
        except Exception as exc:
            _cb_exc = exc
            log.error(
                "[%s] _dispatch_completion callback raised: %s", w.ticker, exc, exc_info=True
            )

        ack = self._normalize_and_verify_completion(w, _cb_result, _cb_exc)

        _local_oid = str((getattr(w, "signal", {}) or {}).get("local_order_id") or "").strip()
        _state_name = "INVALIDATED" if w.state == WatchState.INVALIDATED else "EXPIRED"

        if ack.outcome == "TERMINALIZED":
            with self._lock:
                _wid = id(w)
                self._pending = [_p for _p in self._pending if id(_p) != _wid]
            try:
                w._release_dedup_key()
            except Exception:
                pass
            log.info(
                "[%s] WATCHER_DISPATCH_TERMINALIZED %s local_order_id=%s "
                "removed_from_pending=true dedup_released=true",
                w.ticker, _state_name, _local_oid or "?",
            )
        elif ack.outcome == "FAILED":
            self._enter_ownership_quarantine(w, ack)
            # When cleanup was intentionally suppressed because the order has
            # active deferred contract selection, this is expected behaviour —
            # not a P0 invariant violation. Log at WARNING, not CRITICAL.
            _is_mat_skip = str(getattr(ack, "reason_code", "") or "").startswith(
                "WATCHER_EXPIRY_SKIPPED_ACTIVE_MATERIALIZATION"
            ) or str(getattr(ack, "reason_code", "") or "") == "WATCHER_EXPIRY_GUARD_CHECK_FAILED"
            if _is_mat_skip:
                log.warning(
                    "[%s] WATCHER_DISPATCH_FAILED %s local_order_id=%s reason=%s "
                    "→ quarantine retained; materializer owns retry lifecycle",
                    w.ticker, _state_name, _local_oid or "?", ack.reason_code,
                )
            else:
                log.critical(
                    "[%s] WATCHER_DISPATCH_FAILED %s local_order_id=%s reason=%s "
                    "→ ownership quarantine. P0 invariant violation.",
                    w.ticker, _state_name, _local_oid or "?", ack.reason_code,
                )
        else:
            # RETRY_OWNED or REARMED — watcher stays in _pending as-is.
            log.info(
                "[%s] WATCHER_DISPATCH_%s %s local_order_id=%s reason=%s "
                "watcher retained in _pending",
                w.ticker, ack.outcome, _state_name, _local_oid or "?", ack.reason_code,
            )
        return ack

    def _normalize_and_verify_completion(
        self,
        w: "WatchedSignal",
        cb_result,
        cb_exc: Optional[Exception] = None,
    ) -> "WatcherCompletionResult":
        """Normalize and strictly verify a watcher completion acknowledgment.

        PR #324 §3 — only WatcherCompletionResult is acceptable.
        None / dict / arbitrary object / exception / unknown outcome → FAILED.

        TERMINALIZED: verified via OSM reread with full identity check.
        RETRY_OWNED: physical registry + durable retry metadata required.
        REARMED: physical registry + explicit rearm state required.
        """
        try:
            from ap.pending_trigger_classifier import (
                WatcherCompletionResult as _WCR,
                WatcherCompletionOutcome as _WCO,
            )
        except Exception as _ie:
            log.error("Cannot import WatcherCompletionResult: %s", _ie)
            return type("_FallbackResult", (), {
                "outcome": "FAILED", "reason_code": "import_error",
                "local_order_id": None, "retry_next_at": None,
                "retry_deadline": None, "detail": str(_ie)[:200],
            })()

        _sig = getattr(w, "signal", {}) or {}
        _local_oid = str(_sig.get("local_order_id") or "").strip()
        _watcher_client = str(_sig.get("client_id") or _sig.get("client_email") or "").strip().lower()
        _watcher_mode = str(_sig.get("execution_mode") or "").strip().lower()
        _TERMINAL_STATUSES = frozenset({"CANCELED", "EXPIRED", "REJECTED", "ERROR"})

        def _failed(reason: str, detail: str = "") -> _WCR:
            return _WCR(
                outcome=_WCO.FAILED,
                reason_code=reason,
                local_order_id=_local_oid or None,
                detail=detail[:200] if detail else None,
            )

        # ── Step 1: only WatcherCompletionResult accepted ─────────────────
        if cb_exc is not None:
            return _failed("callback_raised", str(cb_exc))

        # Guard against module-reload class-identity mismatch: in combined
        # test runs, ap.pending_trigger_classifier may be loaded into two
        # different module instances, making isinstance() fail even when the
        # object is a genuine WatcherCompletionResult.  Check by type name
        # AND required structural attributes so the contract is enforced
        # regardless of which module instance created the object.
        _cb_type_name = type(cb_result).__name__ if cb_result is not None else "NoneType"
        _is_wcr = (
            isinstance(cb_result, _WCR)
            or (
                _cb_type_name == "WatcherCompletionResult"
                and hasattr(cb_result, "outcome")
                and hasattr(cb_result, "reason_code")
                and hasattr(cb_result, "local_order_id")
            )
        )
        if not _is_wcr:
            return _failed("callback_result_not_watcher_completion_result", f"got {_cb_type_name}")

        _valid = {_WCO.TERMINALIZED, _WCO.RETRY_OWNED, _WCO.REARMED, _WCO.FAILED}
        if cb_result.outcome not in _valid:
            return _failed("unknown_outcome_value", str(cb_result.outcome)[:100])

        if cb_result.outcome == _WCO.FAILED:
            return cb_result  # propagate as-is

        if not cb_result.local_order_id:
            return _failed("missing_local_order_id")

        def _extract_meta(row: dict) -> dict:
            _m = row.get("meta") or {}
            if isinstance(_m, str):
                try:
                    import json as _jj; _m = _jj.loads(_m)
                except Exception:
                    _m = {}
            return _m if isinstance(_m, dict) else {}

        def _resolve_signal_id(_w) -> str:
            # Final amendment §3: canonical signal_id resolution.
            return str(
                getattr(_w, "signal_id", None)
                or (getattr(_w, "signal", {}) or {}).get("signal_id")
                or ""
            ).strip()

        # ── Step 2: TERMINALIZED — complete identity + durable-reason match ─
        if cb_result.outcome == _WCO.TERMINALIZED:
            # Final amendment §1: ALL identity values must be present and equal.
            _cb_oid = str(cb_result.local_order_id or "").strip()
            if not _cb_oid:
                return _failed("terminalized_missing_callback_local_order_id")
            if not _local_oid:
                return _failed("terminalized_missing_watcher_local_order_id")
            if _cb_oid != _local_oid:
                return _failed(f"terminalized_callback_watcher_oid_mismatch:{_cb_oid}!={_local_oid}")
            if not _watcher_client:
                return _failed("terminalized_missing_watcher_client_id")
            if not _watcher_mode:
                return _failed("terminalized_missing_watcher_execution_mode")

            osm = getattr(self, "order_state_machine", None)
            if osm is None:
                return _failed("terminalized_osm_unavailable")
            _get_fn = getattr(osm, "get_order", None)
            if not callable(_get_fn):
                return _failed("terminalized_get_order_unavailable")
            try:
                row = _get_fn(_local_oid)
            except Exception as _re:
                log.critical("[%s] TERMINALIZED reread raised: %s", w.ticker, _re)
                return _failed("terminalized_reread_raised", str(_re))
            if row is None:
                return _failed("terminalized_get_order_returned_none")

            row_status = str(row.get("status") or "").strip().upper()
            row_oid = str(row.get("local_order_id") or "").strip()
            row_client = str(row.get("client_id") or row.get("client_email") or "").strip().lower()
            row_mode = str(row.get("execution_mode") or "").strip().lower()

            if not row_oid:
                return _failed("terminalized_row_missing_local_order_id")
            if row_oid != _local_oid:
                return _failed(f"terminalized_identity_mismatch_local_order_id:{row_oid}!={_local_oid}")
            if not row_client:
                return _failed("terminalized_row_missing_client_id")
            if row_client != _watcher_client:
                return _failed("terminalized_identity_mismatch_client_id")
            if not row_mode:
                return _failed("terminalized_row_missing_execution_mode")
            if row_mode != _watcher_mode:
                return _failed(f"terminalized_identity_mismatch_execution_mode:{row_mode}!={_watcher_mode}")

            if row_status == "PENDING_TRIGGER":
                log.critical(
                    "[%s] TERMINALIZED claimed but row still PENDING_TRIGGER local_order_id=%s",
                    w.ticker, _local_oid,
                )
                return _failed("terminalized_claimed_but_row_still_pending_trigger")
            if row_status not in _TERMINAL_STATUSES:
                log.critical(
                    "[%s] TERMINALIZED claimed but status='%s' not in %s local_order_id=%s",
                    w.ticker, row_status, sorted(_TERMINAL_STATUSES), _local_oid,
                )
                return _failed(f"terminalized_nonterminal_status:{row_status}")

            row_meta = _extract_meta(row)
            if str(row_meta.get("submit_intent_owner") or "").strip() or \
               str(row_meta.get("recovery_submit_owner") or "").strip():
                return _failed("terminalized_active_submit_or_recovery_owner_present")

            # Final amendment §2: callback exact reason must match a durable reason.
            _cb_reason = str(cb_result.reason_code or "").strip()
            if _cb_reason:
                _wa = row_meta.get("watcher_audit")
                _wa_reason = str((_wa or {}).get("reason_code") or "").strip() if isinstance(_wa, dict) else ""
                _durable_candidates = {
                    str(row.get("last_error") or "").strip(),
                    str(row_meta.get("watcher_invalidation_reason") or "").strip(),
                    str(row_meta.get("terminal_reason") or "").strip(),
                    _wa_reason,
                }
                _durable_candidates.discard("")
                if not _durable_candidates:
                    return _failed(
                        f"terminalized_no_durable_reason_for_callback_reason:{_cb_reason}"
                    )
                if _cb_reason not in _durable_candidates:
                    log.critical(
                        "[%s] TERMINALIZED reason '%s' not in durable %s local_order_id=%s",
                        w.ticker, _cb_reason, sorted(_durable_candidates), _local_oid,
                    )
                    return _failed(
                        f"terminalized_reason_mismatch:{_cb_reason}",
                        f"durable={sorted(_durable_candidates)}",
                    )
            return cb_result

        # ── Step 3: RETRY_OWNED — registry + reread + durable metadata agreement ─
        if cb_result.outcome == _WCO.RETRY_OWNED:
            with self._lock:
                _in_pending = any(id(p) == id(w) for p in self._pending)
            if not _in_pending:
                return _failed("retry_owned_watcher_not_in_pending")
            _sid = _resolve_signal_id(w)
            if not _sid:
                return _failed("retry_owned_missing_signal_id")
            if _sid not in self._dedup_set:
                return _failed("retry_owned_dedup_key_not_held")
            if not cb_result.retry_next_at:
                return _failed("retry_owned_retry_next_at_not_durable")
            if not cb_result.retry_deadline:
                return _failed("retry_owned_retry_deadline_not_durable")

            osm = getattr(self, "order_state_machine", None)
            if osm is None:
                return _failed("retry_owned_osm_unavailable")
            _gf = getattr(osm, "get_order", None)
            if not callable(_gf):
                return _failed("retry_owned_get_order_unavailable")
            try:
                _r = _gf(_local_oid)
            except Exception as _rre:
                return _failed("retry_owned_reread_raised", str(_rre))
            if _r is None:
                return _failed("retry_owned_reread_returned_none")

            _rs = str(_r.get("status") or "").strip().upper()
            if _rs != "PENDING_TRIGGER":
                return _failed(f"retry_owned_order_not_pending_trigger:{_rs}")
            _r_oid = str(_r.get("local_order_id") or "").strip()
            _r_client = str(_r.get("client_id") or _r.get("client_email") or "").strip().lower()
            _r_mode = str(_r.get("execution_mode") or "").strip().lower()
            if not _r_oid or _r_oid != _local_oid:
                return _failed("retry_owned_local_order_id_mismatch")
            if _watcher_client and (not _r_client or _r_client != _watcher_client):
                return _failed("retry_owned_client_id_mismatch")
            if _watcher_mode and (not _r_mode or _r_mode != _watcher_mode):
                return _failed("retry_owned_execution_mode_mismatch")

            _rmeta = _extract_meta(_r)
            _d_owner = str(_rmeta.get("watcher_retry_owner") or "").strip()
            _d_reason = str(_rmeta.get("watcher_invalidation_reason") or "").strip()
            _d_attempt = _rmeta.get("watcher_retry_attempt")
            _d_next = str(_rmeta.get("watcher_retry_next_at") or "").strip()
            _d_deadline = str(_rmeta.get("watcher_retry_deadline") or "").strip()
            if not _d_owner:
                return _failed("retry_owned_durable_owner_missing")
            if not _d_reason:
                return _failed("retry_owned_durable_reason_missing")
            if _d_attempt is None:
                return _failed("retry_owned_durable_attempt_missing")
            if not _d_next:
                return _failed("retry_owned_durable_next_at_missing")
            if not _d_deadline:
                return _failed("retry_owned_durable_deadline_missing")
            if str(cb_result.retry_next_at).strip() != _d_next:
                return _failed("retry_owned_next_at_disagrees_with_durable")
            if str(cb_result.retry_deadline).strip() != _d_deadline:
                return _failed("retry_owned_deadline_disagrees_with_durable")
            return cb_result

        # ── Step 4: REARMED — registry + reread + durable rearm metadata ──
        if cb_result.outcome == _WCO.REARMED:
            with self._lock:
                _in_pending = any(id(p) == id(w) for p in self._pending)
            if not _in_pending:
                return _failed("rearmed_watcher_not_in_pending")
            _sid = _resolve_signal_id(w)
            if not _sid:
                return _failed("rearmed_missing_signal_id")
            if _sid not in self._dedup_set:
                return _failed("rearmed_dedup_key_not_held")
            if not getattr(w, "rearm_mode", False):
                return _failed("rearmed_watcher_not_in_rearm_state")

            osm = getattr(self, "order_state_machine", None)
            if osm is None:
                return _failed("rearmed_osm_unavailable")
            _gf = getattr(osm, "get_order", None)
            if not callable(_gf):
                return _failed("rearmed_get_order_unavailable")
            try:
                _r = _gf(_local_oid)
            except Exception as _rre:
                return _failed("rearmed_reread_raised", str(_rre))
            if _r is None:
                return _failed("rearmed_reread_returned_none")
            _rs = str(_r.get("status") or "").strip().upper()
            if _rs != "PENDING_TRIGGER":
                return _failed(f"rearmed_order_not_pending_trigger:{_rs}")
            _r_oid = str(_r.get("local_order_id") or "").strip()
            _r_client = str(_r.get("client_id") or _r.get("client_email") or "").strip().lower()
            _r_mode = str(_r.get("execution_mode") or "").strip().lower()
            if not _r_oid or _r_oid != _local_oid:
                return _failed("rearmed_local_order_id_mismatch")
            if _watcher_client and (not _r_client or _r_client != _watcher_client):
                return _failed("rearmed_client_id_mismatch")
            if _watcher_mode and (not _r_mode or _r_mode != _watcher_mode):
                return _failed("rearmed_execution_mode_mismatch")

            _rmeta = _extract_meta(_r)
            _rr_reason = str(_rmeta.get("rearm_reason") or _rmeta.get("watcher_rearm_reason") or "").strip()
            _rr_attempt = _rmeta.get("rearm_attempt", _rmeta.get("watcher_rearm_attempt"))
            _rr_deadline = str(_rmeta.get("rearm_deadline") or _rmeta.get("watcher_rearm_deadline") or "").strip()
            if not _rr_reason:
                return _failed("rearmed_durable_reason_missing")
            if _rr_attempt is None:
                return _failed("rearmed_durable_attempt_missing")
            if not _rr_deadline:
                return _failed("rearmed_durable_deadline_missing")
            return cb_result

        return _failed("unhandled_outcome", str(cb_result.outcome))

    def _enter_ownership_quarantine(
        self, w: "WatchedSignal", ack: "WatcherCompletionResult"
    ) -> None:
        """Put a watcher into cleanup-retry quarantine after a FAILED completion.

        The watcher stays in _pending, is NOT active (cannot trigger/submit),
        and its dedup key remains held.  Cleanup is retried on a bounded
        schedule.  Deadline exhaustion escalates diagnostics but never releases
        ownership.
        """
        try:
            _retry_delay = max(10, int(os.getenv(
                "WATCHER_CLEANUP_RETRY_DELAY_SECONDS", "30"
            )))
        except (TypeError, ValueError):
            _retry_delay = 30
        try:
            _deadline_secs = max(120, int(os.getenv(
                "WATCHER_CLEANUP_RETRY_DEADLINE_SECONDS", "600"
            )))
        except (TypeError, ValueError):
            _deadline_secs = 600

        now = datetime.now(timezone.utc)
        w._ownership_quarantine = True
        w._quarantine_reason = str(getattr(ack, "reason_code", "unknown"))
        if w.cleanup_retry_attempt == 0:
            # First entry into quarantine — set deadline.
            w.cleanup_retry_deadline = now + timedelta(seconds=_deadline_secs)
        w.cleanup_retry_attempt += 1
        w.cleanup_retry_next_at = now + timedelta(seconds=_retry_delay)

        _local_oid = str(w.signal.get("local_order_id") or "").strip()
        # Final amendment §5: check the boolean return of update_order_meta.
        # Success → durable diagnostic confirmed. False/raise → watcher remains
        # quarantined, dedup held, CRITICAL emitted, in-memory marker set.
        w.quarantine_metadata_persist_failed = False
        osm = getattr(self, "order_state_machine", None)
        if osm is not None and _local_oid:
            _upd = getattr(osm, "update_order_meta", None)
            if callable(_upd):
                _persist_ok = False
                try:
                    _persist_ok = bool(_upd(_local_oid, {
                        "watcher_invalidation_class": "INVALIDATED_NO_WATCHER_OWNER",
                        "watcher_quarantine_reason": w._quarantine_reason,
                        "cleanup_retry_attempt": w.cleanup_retry_attempt,
                        "cleanup_retry_next_at": w.cleanup_retry_next_at.isoformat(),
                        "cleanup_retry_deadline": (
                            w.cleanup_retry_deadline.isoformat()
                            if w.cleanup_retry_deadline else None
                        ),
                    }))
                except Exception as _meta_exc:
                    _persist_ok = False
                    log.critical(
                        "[%s] QUARANTINE_METADATA_PERSIST_FAILED (raised) local_order_id=%s "
                        "err=%s — watcher remains quarantined, dedup held.",
                        w.ticker, _local_oid, _meta_exc,
                    )
                if not _persist_ok:
                    w.quarantine_metadata_persist_failed = True
                    log.critical(
                        "[%s] QUARANTINE_METADATA_PERSIST_FAILED local_order_id=%s — "
                        "durable diagnostic NOT confirmed; watcher remains quarantined, "
                        "dedup held, marker set.",
                        w.ticker, _local_oid,
                    )
            else:
                w.quarantine_metadata_persist_failed = True
                log.critical(
                    "[%s] QUARANTINE_METADATA_PERSIST_FAILED — update_order_meta unavailable "
                    "local_order_id=%s; watcher remains quarantined.",
                    w.ticker, _local_oid,
                )
        else:
            w.quarantine_metadata_persist_failed = True
            log.critical(
                "[%s] QUARANTINE_METADATA_PERSIST_FAILED — OSM or local_order_id missing; "
                "watcher remains quarantined.",
                w.ticker,
            )

    def _retry_quarantined_cleanup(self) -> None:
        """Attempt to retry cleanup for quarantined watchers.

        Called from the poll loop.  Quarantined watchers whose cleanup_retry_next_at
        is due are processed.  Deadline exhaustion escalates diagnostics but does NOT
        release ownership — the watcher remains quarantined forever until cleanup succeeds.
        """
        now = datetime.now(timezone.utc)
        with self._lock:
            quarantined = [
                w for w in self._pending
                if getattr(w, "_ownership_quarantine", False)
                and w.cleanup_retry_next_at is not None
                and now >= w.cleanup_retry_next_at
            ]
        if not quarantined:
            return

        for w in quarantined:
            _local_oid = str(w.signal.get("local_order_id") or "").strip()
            _deadline = w.cleanup_retry_deadline
            _deadline_expired = (_deadline is not None and now > _deadline)

            if _deadline_expired:
                log.critical(
                    "[%s] WATCHER_QUARANTINE_DEADLINE_EXHAUSTED local_order_id=%s "
                    "attempt=%d — escalating diagnostics; ownership retained.",
                    w.ticker, _local_oid or "?", w.cleanup_retry_attempt,
                )
                osm = getattr(self, "order_state_machine", None)
                if osm is not None and _local_oid:
                    _upd = getattr(osm, "update_order_meta", None)
                    if callable(_upd):
                        try:
                            _upd(_local_oid, {
                                "watcher_quarantine_deadline_exhausted": True,
                                "watcher_quarantine_escalated_at": now.isoformat(),
                            })
                        except Exception:
                            pass
                # Do NOT release — bump next_at so we log again later.
                w.cleanup_retry_next_at = now + timedelta(seconds=300)
                continue

            # Retry: re-invoke the appropriate cleanup callback.
            _state_name = "INVALIDATED" if w.state == WatchState.INVALIDATED else "EXPIRED"
            _cb = self.on_invalidate if w.state == WatchState.INVALIDATED else self.on_expire
            _cb_result = None
            _cb_exc = None
            if _cb:
                try:
                    _cb_result = _cb(w)
                except Exception as _exc:
                    _cb_exc = _exc
                    log.warning(
                        "[%s] quarantine cleanup retry raised: %s", w.ticker, _exc
                    )
            else:
                # No callback — try direct OSM terminalization.
                # PR #324 §9: must produce a real WatcherCompletionResult.
                from ap.pending_trigger_classifier import (
                    WatcherCompletionResult as _QWCR,
                    WatcherCompletionOutcome as _QWCO,
                )
                osm = getattr(self, "order_state_machine", None)
                if osm is not None and _local_oid:
                    _cancel = getattr(osm, "cancel_pending_entry", None)
                    if callable(_cancel):
                        try:
                            if _cancel(_local_oid, reason="watcher_quarantine_cleanup_retry"):
                                _cb_result = _QWCR(
                                    outcome=_QWCO.TERMINALIZED,
                                    reason_code="direct_osm_cancel_ok",
                                    local_order_id=_local_oid,
                                )
                            else:
                                _cb_result = _QWCR(
                                    outcome=_QWCO.FAILED,
                                    reason_code="quarantine_direct_cancel_returned_false",
                                    local_order_id=_local_oid,
                                )
                        except Exception as _oexc:
                            _cb_exc = _oexc
                    else:
                        _cb_result = _QWCR(
                            outcome=_QWCO.FAILED,
                            reason_code="quarantine_cancel_helper_not_available",
                            local_order_id=_local_oid,
                        )

            # PR #324 §9 — run through the same strict verifier.
            _ack = self._normalize_and_verify_completion(w, _cb_result, _cb_exc)
            if _ack.outcome == "TERMINALIZED":
                with self._lock:
                    _wid = id(w)
                    self._pending = [_p for _p in self._pending if id(_p) != _wid]
                try:
                    w._release_dedup_key()
                except Exception:
                    pass
                log.info(
                    "[%s] WATCHER_QUARANTINE_CLEANUP_SUCCEEDED %s local_order_id=%s "
                    "attempt=%d — removed from _pending, dedup released.",
                    w.ticker, _state_name, _local_oid or "?", w.cleanup_retry_attempt,
                )
            else:
                # Still failing — update retry state and keep quarantined.
                try:
                    _retry_delay = max(10, int(os.getenv(
                        "WATCHER_CLEANUP_RETRY_DELAY_SECONDS", "30"
                    )))
                except (TypeError, ValueError):
                    _retry_delay = 30
                w.cleanup_retry_attempt += 1
                w.cleanup_retry_next_at = now + timedelta(seconds=_retry_delay)
                # PR #324 §9 — metadata write must check boolean return.
                _qmeta_ok = False
                _qosm = getattr(self, "order_state_machine", None)
                if _qosm is not None and _local_oid:
                    _qupd = getattr(_qosm, "update_order_meta", None)
                    if callable(_qupd):
                        try:
                            _qmeta_ok = bool(_qupd(_local_oid, {
                                "cleanup_retry_attempt": w.cleanup_retry_attempt,
                                "cleanup_retry_next_at": w.cleanup_retry_next_at.isoformat(),
                            }))
                        except Exception:
                            _qmeta_ok = False
                if not _qmeta_ok:
                    log.warning(
                        "[%s] quarantine metadata write failed for retry state update",
                        w.ticker,
                    )  # ownership retained regardless — do not release
                log.warning(
                    "[%s] WATCHER_QUARANTINE_CLEANUP_STILL_FAILING %s "
                    "local_order_id=%s attempt=%d reason=%s",
                    w.ticker, _state_name, _local_oid or "?",
                    w.cleanup_retry_attempt, _ack.reason_code,
                )

    # ── P0 (PR #304) Bugs C & D: arm-time already-through-trigger safety ────
    @staticmethod
    def _is_already_through_trigger(
        side: str,
        trigger: float,
        bid: float,
        ask: float,
        mid: float = 0.0,
        last: float = 0.0,
    ) -> bool:
        """
        True when the underlying quote proves price has already crossed the
        entry trigger — meaning any watcher armed now would fire on a move
        that already happened.

        Rule (matches WatchedSignal.check breach logic exactly):
            CALL breached when ask >= trigger
            PUT  breached when bid <= trigger

        Falls back to mid/last only when bid/ask are missing entirely.
        Zero quote data returns False (unknown — cannot claim already-through).
        """
        try:
            trigger = float(trigger or 0)
            bid = float(bid or 0)
            ask = float(ask or 0)
        except (TypeError, ValueError):
            return False
        if trigger <= 0:
            return False

        _side = str(side or "").strip().upper()
        # Primary: bid/ask
        if bid > 0 or ask > 0:
            if _side == "CALL":
                return ask > 0 and ask >= trigger
            if _side == "PUT":
                return bid > 0 and bid <= trigger
            return False

        # Fallback: mid / last (used only when bid/ask both missing)
        try:
            fallback = float(mid or 0) or float(last or 0)
        except (TypeError, ValueError):
            fallback = 0.0
        if fallback <= 0:
            return False
        if _side == "CALL":
            return fallback >= trigger
        if _side == "PUT":
            return fallback <= trigger
        return False

    def _get_quote(self, ticker: str) -> dict:
        try:
            quotes = self._fetch_quotes([ticker])
            return quotes.get(str(ticker).upper(), {})
        except Exception:
            return {}

    def _get_option_quote(self, option_symbol: str) -> dict:
        """Fetch a single option contract quote (bid/ask/last).
        Uses the same Tradier quotes endpoint as _get_quote.
        Returns empty dict on any failure — caller treats as unavailable."""
        if not option_symbol or not option_symbol.strip():
            return {}
        try:
            quotes = self._fetch_quotes([option_symbol.strip().upper()])
            return quotes.get(option_symbol.strip().upper(), {})
        except Exception:
            return {}

    def _fetch_quotes(self, tickers: list[str]) -> dict:
        clean_tickers = [str(t).upper().strip() for t in tickers if str(t).strip()]
        if not clean_tickers:
            return {}

        symbols = ",".join(sorted(set(clean_tickers)))
        try:
            # PR P0-WATCHER-QUOTES: always use live market-data URL/token.
            # Paper execution uses sandbox for orders but watcher quote
            # decisions must evaluate against real-time market prices.
            transport = self._resolve_watcher_quote_transport()
            base_url = transport["watcher_quote_base_url"]
            md_token = transport["token"]
            token_source_name = transport["watcher_quote_token_source"]
            self._set_last_quote_fetch_proof(
                watcher_quote_source=transport["watcher_quote_source"],
                watcher_quote_base_url=base_url,
                watcher_sandbox_mode=transport["watcher_sandbox_mode"],
                watcher_quote_token_source=token_source_name,
                quote_fetch_status="started",
            )
            _quote_started = time.perf_counter()
            if md_token:
                import requests as _req
                resp = _req.get(
                    f"{base_url}/v1/markets/quotes",
                    params={"symbols": symbols, "greeks": "false"},
                    headers={
                        "Authorization": f"Bearer {md_token}",
                        "Accept": "application/json",
                    },
                    timeout=5,
                )
                self._set_last_quote_fetch_proof(
                    watcher_quote_token_source=token_source_name,
                    quote_fetch_status="requested_via_direct_market_data_token",
                )
            else:
                _is_paper = str(getattr(self, "mode", "PAPER")).upper() == "PAPER"
                if _is_paper:
                    # PAPER + no live market-data token: hard fail.
                    # broker.session holds sandbox execution credentials;
                    # using them against api.tradier.com would fail auth.
                    msg = (
                        "[watcher_quotes] PAPER_WATCHER_NO_MARKET_DATA_TOKEN "
                        f"mode=PAPER base_url={base_url} token_source={token_source_name} "
                        "— watcher quotes cannot run without live market-data credentials. "
                        "Set TRADIER_MARKET_DATA_TOKEN or TRADIER_DATA_TOKEN."
                    )
                    self._set_last_quote_fetch_proof(
                        watcher_quote_token_source=token_source_name,
                        quote_fetch_status="market_data_token_missing",
                    )
                    log.critical("%s", msg)
                    raise RuntimeError(msg)
                # LIVE clients: execution token typically has market-data access.
                # Use broker.session with the live URL (safe for live mode only).
                token_source_name = "broker.session_live_execution_token"
                log.warning(
                    "[watcher_quotes] No TRADIER_MARKET_DATA_TOKEN configured — "
                    "falling back to live broker session with %s (LIVE mode only).",
                    base_url,
                )
                self._set_last_quote_fetch_proof(
                    watcher_quote_token_source=token_source_name,
                    quote_fetch_status="requested_via_live_broker_session",
                )
                resp = self.broker.session.get(
                    f"{base_url}/v1/markets/quotes",
                    params={"symbols": symbols, "greeks": "false"},
                    headers={"Accept": "application/json"},
                    timeout=5,
                )
            if hasattr(resp, "raise_for_status"):
                resp.raise_for_status()
            quote_age_ms = max(0, int(round((time.perf_counter() - _quote_started) * 1000.0)))
            data = resp.json()
            quotes_raw = data.get("quotes", {}).get("quote", [])
            if isinstance(quotes_raw, dict):
                quotes_raw = [quotes_raw]
            self._set_last_quote_fetch_proof(
                watcher_quote_token_source=token_source_name,
                quote_fetch_status="success",
            )
            normalized_quotes = {}
            for q in quotes_raw:
                if not q.get("symbol"):
                    continue
                _quote = dict(q)
                _quote["quote_age_ms"] = quote_age_ms
                _quote["quote_fetch_status"] = "success"
                normalized_quotes[str(q.get("symbol", "")).upper()] = _quote
            return normalized_quotes
        except Exception as exc:
            _existing_fetch_status = str(
                (getattr(self, "_last_quote_fetch_proof", {}) or {}).get("quote_fetch_status") or ""
            )
            if _existing_fetch_status not in {"market_data_token_missing"}:
                self._set_last_quote_fetch_proof(
                    quote_fetch_status=f"error:{type(exc).__name__}",
                )
            log.warning("Tradier quote fetch failed: %s", exc)
            if isinstance(exc, RuntimeError) and "PAPER_WATCHER_NO_MARKET_DATA_TOKEN" in str(exc):
                raise
            return {}
