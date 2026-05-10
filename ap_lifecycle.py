"""
ap_lifecycle.py
===============
Canonical, immutable signal lifecycle + rejection ledger for Angel Precision Intelligence.

RULE: If it is not in this ledger, it did not happen.

This module records:
- every signal state transition
- every explicit trade rejection
- every illegal transition attempt
- every suspicious/invariant-breaking movement

It is designed to make debugging breezy:
- grep [SIGNAL_TRACE] for lifecycle movement
- grep [TRADE_REJECTED] for blocked trades
- open logs/signal_ledger.jsonl for lifecycle JSONL records
- open logs/rejection_ledger.jsonl for rejection JSONL records

This module is import-safe anywhere. It starts no threads.
It only writes when you call LEDGER.transition(...) or LEDGER.reject(...).

Typical usage:

    from ap_lifecycle import (
        LEDGER,
        SignalState,
        LifecycleOwner,
        signal_rejected,
        RejectionCategory,
        RejectionSeverity,
    )

    LEDGER.transition(
        signal_id=sig_id,
        ticker=ticker,
        to_state=SignalState.WATCHING,
        owner=LifecycleOwner.OVERNIGHT_EVAL,
        reason="morning_reeval_passed",
    )

    signal_rejected(
        signal_id=sig_id,
        ticker=ticker,
        owner=LifecycleOwner.EXECUTION_CORE,
        category=RejectionCategory.HEALTH,
        reason_code="SYSTEM_UNHEALTHY",
        reason="critical organ unhealthy; entry blocked",
        severity=RejectionSeverity.CRITICAL,
        health_snapshot=HEALTH.snapshot(),
    )
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

log = logging.getLogger("ap.lifecycle")


# ---------------------------------------------------------------------------
# State & Owner Enums
# ---------------------------------------------------------------------------

class SignalState(str, Enum):
    CREATED             = "CREATED"
    PERSISTED           = "PERSISTED"
    LOADED_BY_OSM       = "LOADED_BY_OSM"
    EVALUATING          = "EVALUATING"
    REGISTERED          = "REGISTERED_IN_WATCHER"
    WATCHING            = "WATCHING"
    REVALIDATING        = "REVALIDATING"
    TRIGGER_READY       = "TRIGGER_READY"
    ENTRY_SUBMITTED     = "ENTRY_SUBMITTED"
    POSITION_OPENED     = "POSITION_OPENED"
    INVALIDATED         = "INVALIDATED"
    REJECTED            = "REJECTED"
    EXPIRED             = "EXPIRED"
    CANCELLED           = "CANCELLED"
    REMOVED             = "REMOVED"
    ERROR               = "ERROR"


class LifecycleOwner(str, Enum):
    SCANNER         = "SCANNER"
    SIGNAL_STORE    = "SIGNAL_STORE"
    OVERNIGHT_EVAL  = "OVERNIGHT_EVAL"
    OSM             = "OSM"
    WATCHER         = "WATCHER"
    MORNING_CRON    = "MORNING_CRON"
    EXECUTION_CORE  = "EXECUTION_CORE"
    ENTRY_GATE      = "ENTRY_GATE"
    CLIENT_RUNNER   = "CLIENT_RUNNER"
    QUOTE_MONITOR   = "QUOTE_MONITOR"
    EXIT_ENGINE     = "EXIT_ENGINE"
    FILL_MONITOR    = "FILL_MONITOR"
    BROKER          = "BROKER"
    RECONCILER      = "RECONCILER"
    RECOVERY        = "RECOVERY"
    MASTER_CONTROL  = "MASTER_CONTROL"
    SYSTEM          = "SYSTEM"
    UNKNOWN         = "UNKNOWN"


class RejectionCategory(str, Enum):
    INTELLIGENCE = "INTELLIGENCE"
    RISK = "RISK"
    HEALTH = "HEALTH"
    RUNNER = "RUNNER"
    BROKER = "BROKER"
    QUOTE = "QUOTE"
    EXECUTION = "EXECUTION"
    CONFIG = "CONFIG"
    KILL_SWITCH = "KILL_SWITCH"
    VALIDATION = "VALIDATION"
    DATA = "DATA"
    TIME_WINDOW = "TIME_WINDOW"
    DUPLICATE = "DUPLICATE"
    UNKNOWN = "UNKNOWN"


class RejectionSeverity(str, Enum):
    INFO = "INFO"          # normal rejection, not a bug
    WARNING = "WARNING"    # suspicious, needs review
    CRITICAL = "CRITICAL"  # major system issue / should likely block trading


OwnerLike = Union[str, LifecycleOwner]


# ---------------------------------------------------------------------------
# Legal transition map — anything outside this is a bug
# ---------------------------------------------------------------------------

LEGAL_TRANSITIONS: Dict[Optional[SignalState], set] = {
    None: {SignalState.CREATED},
    SignalState.CREATED: {
        SignalState.PERSISTED, SignalState.REJECTED, SignalState.ERROR,
    },
    SignalState.PERSISTED: {
        SignalState.LOADED_BY_OSM, SignalState.WATCHING,
        SignalState.EXPIRED, SignalState.REJECTED, SignalState.ERROR,
    },
    SignalState.LOADED_BY_OSM: {
        SignalState.EVALUATING, SignalState.REGISTERED,
        SignalState.WATCHING, SignalState.INVALIDATED,
        SignalState.REJECTED, SignalState.ERROR,
    },
    SignalState.EVALUATING: {
        SignalState.WATCHING, SignalState.INVALIDATED,
        SignalState.REJECTED, SignalState.EXPIRED, SignalState.ERROR,
    },
    SignalState.REGISTERED: {
        SignalState.WATCHING, SignalState.REJECTED, SignalState.ERROR,
    },
    SignalState.WATCHING: {
        SignalState.REVALIDATING, SignalState.TRIGGER_READY,
        SignalState.INVALIDATED, SignalState.REJECTED,
        SignalState.EXPIRED, SignalState.CANCELLED,
        SignalState.REMOVED, SignalState.ERROR,
    },
    SignalState.REVALIDATING: {
        SignalState.WATCHING, SignalState.INVALIDATED,
        SignalState.REJECTED, SignalState.EXPIRED, SignalState.ERROR,
    },
    SignalState.TRIGGER_READY: {
        SignalState.ENTRY_SUBMITTED, SignalState.WATCHING,
        SignalState.INVALIDATED, SignalState.REJECTED, SignalState.ERROR,
    },
    SignalState.ENTRY_SUBMITTED: {
        SignalState.POSITION_OPENED, SignalState.REJECTED, SignalState.ERROR,
    },
    # Terminal-ish states
    SignalState.POSITION_OPENED: set(),
    SignalState.INVALIDATED:     {SignalState.REMOVED},
    SignalState.REJECTED:        {SignalState.REMOVED},
    SignalState.CANCELLED:       {SignalState.REMOVED},
    SignalState.EXPIRED:         {SignalState.REMOVED},
    SignalState.REMOVED:         set(),
    SignalState.ERROR:           {SignalState.REMOVED, SignalState.INVALIDATED},
}

TERMINAL_STATES = {
    SignalState.POSITION_OPENED,
    SignalState.REMOVED,
}

SUSPICIOUS_TRANSITIONS = {
    (SignalState.WATCHING, SignalState.REMOVED),
    (SignalState.WATCHING, SignalState.EXPIRED),
}


# ---------------------------------------------------------------------------
# Immutable records
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LedgerEntry:
    entry_id:        str
    correlation_id:  str
    signal_id:       str
    ticker:          str
    from_state:      Optional[str]
    to_state:        str
    owner:           str
    reason:          str
    timestamp_iso:   str
    timestamp_epoch: float
    metadata:        Dict[str, Any]

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


@dataclass(frozen=True)
class RejectionRecord:
    rejection_id:    str
    correlation_id:  str
    signal_id:       str
    ticker:          str
    owner:           str
    category:        str
    severity:        str
    reason_code:     str
    human_reason:    str
    timestamp_iso:   str
    timestamp_epoch: float
    metadata:        Dict[str, Any]

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


# ---------------------------------------------------------------------------
# Singleton Ledger
# ---------------------------------------------------------------------------

class SignalLifecycleLedger:
    """
    Thread-safe, append-only signal lifecycle + rejection ledger.
    Singleton — import LEDGER, do not instantiate directly.
    """

    _instance: Optional["SignalLifecycleLedger"] = None
    _class_lock = threading.Lock()

    def __new__(cls) -> "SignalLifecycleLedger":
        with cls._class_lock:
            if cls._instance is None:
                inst = super().__new__(cls)
                inst._entries: List[LedgerEntry] = []
                inst._rejections: List[RejectionRecord] = []
                inst._current_state: Dict[str, SignalState] = {}
                inst._correlation_ids: Dict[str, str] = {}
                inst._entry_lock = threading.RLock()
                inst._db_writer: Optional[Callable[[dict], None]] = None
                inst._log_path = os.getenv(
                    "AP_LEDGER_PATH", "logs/signal_ledger.jsonl"
                )
                inst._rejection_log_path = os.getenv(
                    "AP_REJECTION_LOG_PATH", "logs/rejection_ledger.jsonl"
                )
                os.makedirs(os.path.dirname(inst._log_path) or "logs", exist_ok=True)
                os.makedirs(os.path.dirname(inst._rejection_log_path) or "logs", exist_ok=True)
                cls._instance = inst
            return cls._instance

    # ------------------------------------------------------------------
    # Wiring
    # ------------------------------------------------------------------

    def wire_db_writer(self, fn: Callable[[dict], None]) -> None:
        """
        Optionally wire a DB persistence callback.

        The callback receives a dict for both lifecycle and rejection records.
        Rejection payloads include payload["_table"] = "trade_rejections".
        Lifecycle payloads include payload["_table"] = "signal_lifecycle".
        Your writer can route based on that field.
        """
        self._db_writer = fn

    # ------------------------------------------------------------------
    # State API
    # ------------------------------------------------------------------

    def current_state(self, signal_id: str) -> Optional[SignalState]:
        with self._entry_lock:
            return self._current_state.get(str(signal_id))

    def transition(
        self,
        signal_id: str,
        ticker: str,
        to_state: SignalState,
        owner: OwnerLike,
        reason: str,
        from_state: Optional[SignalState] = None,
        metadata: Optional[Dict[str, Any]] = None,
        strict: bool = False,
    ) -> LedgerEntry:
        """
        Record a state transition.

        strict=True raises RuntimeError on illegal transitions.
        In production strict=False records ERROR but does not halt the process.
        """
        with self._entry_lock:
            signal_id = str(signal_id)
            ticker = str(ticker).upper()
            owner_value = _owner_value(owner)
            actual_from = self._current_state.get(signal_id)
            resolved_from = from_state if from_state is not None else actual_from

            legal = LEGAL_TRANSITIONS.get(resolved_from, set())
            is_legal = (to_state in legal) or (resolved_from == to_state)

            if not is_legal:
                msg = (
                    f"[LIFECYCLE] ILLEGAL_TRANSITION "
                    f"signal={signal_id} ticker={ticker} "
                    f"{resolved_from.value if resolved_from else 'NONE'}->{to_state.value} "
                    f"owner={owner_value} reason={reason}"
                )
                log.error(msg)
                self._record(
                    signal_id, ticker, resolved_from, SignalState.ERROR,
                    owner_value, f"ILLEGAL_TRANSITION: {reason}",
                    metadata or {},
                )
                if strict:
                    raise RuntimeError(msg)

            entry = self._record(
                signal_id, ticker, resolved_from, to_state,
                owner_value, reason, metadata or {},
            )

            if is_legal:
                self._current_state[signal_id] = to_state

            if (resolved_from, to_state) in SUSPICIOUS_TRANSITIONS:
                log.error(
                    "[LIFECYCLE] INVARIANT_VIOLATION signal=%s ticker=%s "
                    "%s->%s owner=%s reason=%s — signal removed/expired without "
                    "clear terminal explanation. THIS IS THE BUG.",
                    signal_id,
                    ticker,
                    resolved_from.value if resolved_from else "NONE",
                    to_state.value,
                    owner_value,
                    reason,
                )

            return entry

    # ------------------------------------------------------------------
    # Rejection API
    # ------------------------------------------------------------------

    def reject(
        self,
        signal_id: str,
        ticker: str,
        owner: OwnerLike,
        category: RejectionCategory,
        reason_code: str,
        human_reason: str,
        severity: RejectionSeverity = RejectionSeverity.INFO,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> RejectionRecord:
        """
        Record an explicit rejection reason and transition signal to REJECTED.

        Call this anytime a trade is blocked, skipped, denied, or refused.

        Examples:
        - intelligence score too low
        - volume below threshold
        - kill switch active
        - quote monitor stale
        - entries_allowed false
        - no active runner
        - broker auth missing
        - bad/missing option quote
        """
        with self._entry_lock:
            signal_id = str(signal_id)
            ticker = str(ticker).upper()
            owner_value = _owner_value(owner)
            metadata = metadata or {}

            record = RejectionRecord(
                rejection_id=uuid.uuid4().hex,
                correlation_id=self._correlation_id(signal_id),
                signal_id=signal_id,
                ticker=ticker,
                owner=owner_value,
                category=category.value,
                severity=severity.value,
                reason_code=str(reason_code),
                human_reason=str(human_reason),
                timestamp_iso=datetime.now(timezone.utc).isoformat(),
                timestamp_epoch=time.time(),
                metadata=metadata,
            )

            self._rejections.append(record)

            log.warning(
                "[TRADE_REJECTED] corr=%s id=%s ticker=%s owner=%s "
                "category=%s severity=%s code=%s reason=%s",
                record.correlation_id,
                record.signal_id,
                record.ticker,
                record.owner,
                record.category,
                record.severity,
                record.reason_code,
                record.human_reason,
            )

            try:
                os.makedirs(os.path.dirname(self._rejection_log_path) or "logs", exist_ok=True)
                with open(self._rejection_log_path, "a") as f:
                    f.write(record.to_json() + "\n")
            except Exception as e:
                log.error("[REJECTION] JSONL write failed: %s", e)

            if self._db_writer:
                try:
                    payload = asdict(record)
                    payload["_table"] = "trade_rejections"
                    self._db_writer(payload)
                except Exception as e:
                    log.error("[REJECTION] DB write failed: %s", e)

            self.transition(
                signal_id=signal_id,
                ticker=ticker,
                to_state=SignalState.REJECTED,
                owner=owner_value,
                reason=f"{reason_code}: {human_reason}",
                metadata={
                    "rejection_category": category.value,
                    "rejection_severity": severity.value,
                    "rejection_reason_code": reason_code,
                    **metadata,
                },
            )

            return record

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def history(self, signal_id: str) -> List[LedgerEntry]:
        with self._entry_lock:
            return [e for e in self._entries if e.signal_id == str(signal_id)]

    def rejection_history(self, signal_id: str) -> List[RejectionRecord]:
        with self._entry_lock:
            return [r for r in self._rejections if r.signal_id == str(signal_id)]

    def trace_dump(self, signal_id: str) -> str:
        entries = self.history(signal_id)
        rejections = self.rejection_history(signal_id)
        if not entries and not rejections:
            return f"NO TRACE for signal_id={signal_id}"

        lines = [f"=== LIFECYCLE TRACE signal_id={signal_id} ==="]
        for e in entries:
            lines.append(
                f"  {e.timestamp_iso}  "
                f"{e.from_state or 'NONE':>25} -> {e.to_state:<30}  "
                f"owner={e.owner}  reason={e.reason}"
            )

        if rejections:
            lines.append(f"=== REJECTIONS signal_id={signal_id} ===")
            for r in rejections:
                lines.append(
                    f"  {r.timestamp_iso}  category={r.category:<14} "
                    f"severity={r.severity:<8} owner={r.owner:<18} "
                    f"code={r.reason_code} reason={r.human_reason}"
                )
        return "\n".join(lines)

    def all_in_state(self, state: SignalState) -> List[str]:
        with self._entry_lock:
            return [sid for sid, s in self._current_state.items() if s == state]

    def all_rejections(self) -> List[RejectionRecord]:
        with self._entry_lock:
            return list(self._rejections)

    def snapshot(self) -> Dict[str, Any]:
        with self._entry_lock:
            state_counts: Dict[str, int] = {}
            for s in self._current_state.values():
                state_counts[s.value] = state_counts.get(s.value, 0) + 1
            rejection_counts: Dict[str, int] = {}
            for r in self._rejections:
                rejection_counts[r.reason_code] = rejection_counts.get(r.reason_code, 0) + 1
            return {
                "entries_count": len(self._entries),
                "rejections_count": len(self._rejections),
                "state_counts": state_counts,
                "rejection_counts_by_code": rejection_counts,
                "ledger_path": self._log_path,
                "rejection_log_path": self._rejection_log_path,
            }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _correlation_id(self, signal_id: str) -> str:
        if signal_id not in self._correlation_ids:
            self._correlation_ids[signal_id] = uuid.uuid4().hex[:12]
        return self._correlation_ids[signal_id]

    def _record(
        self,
        signal_id: str,
        ticker: str,
        from_state: Optional[SignalState],
        to_state: SignalState,
        owner: str,
        reason: str,
        metadata: Dict[str, Any],
    ) -> LedgerEntry:
        now = time.time()
        entry = LedgerEntry(
            entry_id        = uuid.uuid4().hex,
            correlation_id  = self._correlation_id(str(signal_id)),
            signal_id       = str(signal_id),
            ticker          = str(ticker).upper(),
            from_state      = from_state.value if from_state else None,
            to_state        = to_state.value,
            owner           = str(owner),
            reason          = str(reason),
            timestamp_iso   = datetime.now(timezone.utc).isoformat(),
            timestamp_epoch = now,
            metadata        = metadata,
        )

        self._entries.append(entry)

        log.warning(
            "[SIGNAL_TRACE] corr=%s id=%s ticker=%s %s->%s owner=%s reason=%s",
            entry.correlation_id,
            entry.signal_id,
            entry.ticker,
            entry.from_state or "NONE",
            entry.to_state,
            entry.owner,
            entry.reason,
        )

        try:
            os.makedirs(os.path.dirname(self._log_path) or "logs", exist_ok=True)
            with open(self._log_path, "a") as f:
                f.write(entry.to_json() + "\n")
        except Exception as e:
            log.error("[LIFECYCLE] JSONL write failed: %s", e)

        if self._db_writer:
            try:
                payload = asdict(entry)
                payload["_table"] = "signal_lifecycle"
                self._db_writer(payload)
            except Exception as e:
                log.error("[LIFECYCLE] DB write failed: %s", e)

        return entry


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _owner_value(owner: OwnerLike) -> str:
    if isinstance(owner, LifecycleOwner):
        return owner.value
    return str(owner)


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
LEDGER = SignalLifecycleLedger()


# ---------------------------------------------------------------------------
# Convenience wrappers — reduce boilerplate at call sites
# ---------------------------------------------------------------------------

def signal_created(signal_id: str, ticker: str, owner: OwnerLike = LifecycleOwner.SCANNER, reason: str = "signal_created", **meta) -> LedgerEntry:
    return LEDGER.transition(signal_id, ticker, SignalState.CREATED, owner, reason, metadata=meta or None)


def signal_persisted(signal_id: str, ticker: str, owner: OwnerLike = LifecycleOwner.SIGNAL_STORE, reason: str = "signal_persisted", **meta) -> LedgerEntry:
    return LEDGER.transition(signal_id, ticker, SignalState.PERSISTED, owner, reason, metadata=meta or None)


def signal_loaded(signal_id: str, ticker: str, owner: OwnerLike = LifecycleOwner.OSM, reason: str = "loaded_by_osm", **meta) -> LedgerEntry:
    return LEDGER.transition(signal_id, ticker, SignalState.LOADED_BY_OSM, owner, reason, metadata=meta or None)


def signal_evaluating(signal_id: str, ticker: str, owner: OwnerLike, reason: str = "evaluating", **meta) -> LedgerEntry:
    return LEDGER.transition(signal_id, ticker, SignalState.EVALUATING, owner, reason, metadata=meta or None)


def signal_armed(signal_id: str, ticker: str, owner: OwnerLike, reason: str = "armed_in_watcher", **meta) -> LedgerEntry:
    return LEDGER.transition(signal_id, ticker, SignalState.REGISTERED, owner, reason, metadata=meta or None)


def signal_watching(signal_id: str, ticker: str, owner: OwnerLike, reason: str = "watching", **meta) -> LedgerEntry:
    return LEDGER.transition(signal_id, ticker, SignalState.WATCHING, owner, reason, metadata=meta or None)


def signal_revalidating(signal_id: str, ticker: str, owner: OwnerLike, reason: str = "revalidating", **meta) -> LedgerEntry:
    return LEDGER.transition(signal_id, ticker, SignalState.REVALIDATING, owner, reason, metadata=meta or None)


def signal_triggered(signal_id: str, ticker: str, owner: OwnerLike, reason: str = "trigger_breached", **meta) -> LedgerEntry:
    return LEDGER.transition(signal_id, ticker, SignalState.TRIGGER_READY, owner, reason, metadata=meta or None)


def signal_entry_submitted(signal_id: str, ticker: str, owner: OwnerLike = LifecycleOwner.EXECUTION_CORE, reason: str = "entry_submitted", **meta) -> LedgerEntry:
    return LEDGER.transition(signal_id, ticker, SignalState.ENTRY_SUBMITTED, owner, reason, metadata=meta or None)


def signal_position_opened(signal_id: str, ticker: str, owner: OwnerLike = LifecycleOwner.RECONCILER, reason: str = "position_opened", **meta) -> LedgerEntry:
    return LEDGER.transition(signal_id, ticker, SignalState.POSITION_OPENED, owner, reason, metadata=meta or None)


def signal_invalidated(signal_id: str, ticker: str, owner: OwnerLike, reason: str, **meta) -> LedgerEntry:
    return LEDGER.transition(signal_id, ticker, SignalState.INVALIDATED, owner, reason, metadata=meta or None)


def signal_cancelled(signal_id: str, ticker: str, owner: OwnerLike, reason: str, **meta) -> LedgerEntry:
    return LEDGER.transition(signal_id, ticker, SignalState.CANCELLED, owner, reason, metadata=meta or None)


def signal_expired(signal_id: str, ticker: str, owner: OwnerLike, reason: str, **meta) -> LedgerEntry:
    return LEDGER.transition(signal_id, ticker, SignalState.EXPIRED, owner, reason, metadata=meta or None)


def signal_removed(signal_id: str, ticker: str, owner: OwnerLike, reason: str, **meta) -> LedgerEntry:
    return LEDGER.transition(signal_id, ticker, SignalState.REMOVED, owner, reason, metadata=meta or None)


def signal_error(signal_id: str, ticker: str, owner: OwnerLike, reason: str, **meta) -> LedgerEntry:
    return LEDGER.transition(signal_id, ticker, SignalState.ERROR, owner, reason, metadata=meta or None)


def signal_rejected(
    signal_id: str,
    ticker: str,
    owner: OwnerLike,
    reason: str,
    category: RejectionCategory = RejectionCategory.UNKNOWN,
    reason_code: str = "REJECTED",
    severity: RejectionSeverity = RejectionSeverity.INFO,
    **meta,
) -> RejectionRecord:
    return LEDGER.reject(
        signal_id=signal_id,
        ticker=ticker,
        owner=owner,
        category=category,
        reason_code=reason_code,
        human_reason=reason,
        severity=severity,
        metadata=meta or None,
    )


# ---------------------------------------------------------------------------
# Common rejection helpers — optional, but useful at call sites
# ---------------------------------------------------------------------------

def reject_intelligence(signal_id: str, ticker: str, owner: OwnerLike, reason_code: str, reason: str, **meta) -> RejectionRecord:
    return signal_rejected(signal_id, ticker, owner, reason, RejectionCategory.INTELLIGENCE, reason_code, RejectionSeverity.INFO, **meta)


def reject_risk(signal_id: str, ticker: str, owner: OwnerLike, reason_code: str, reason: str, **meta) -> RejectionRecord:
    return signal_rejected(signal_id, ticker, owner, reason, RejectionCategory.RISK, reason_code, RejectionSeverity.INFO, **meta)


def reject_health(signal_id: str, ticker: str, owner: OwnerLike, reason_code: str, reason: str, **meta) -> RejectionRecord:
    return signal_rejected(signal_id, ticker, owner, reason, RejectionCategory.HEALTH, reason_code, RejectionSeverity.CRITICAL, **meta)


def reject_runner(signal_id: str, ticker: str, owner: OwnerLike, reason_code: str, reason: str, **meta) -> RejectionRecord:
    return signal_rejected(signal_id, ticker, owner, reason, RejectionCategory.RUNNER, reason_code, RejectionSeverity.WARNING, **meta)


def reject_broker(signal_id: str, ticker: str, owner: OwnerLike, reason_code: str, reason: str, **meta) -> RejectionRecord:
    return signal_rejected(signal_id, ticker, owner, reason, RejectionCategory.BROKER, reason_code, RejectionSeverity.WARNING, **meta)


def reject_quote(signal_id: str, ticker: str, owner: OwnerLike, reason_code: str, reason: str, **meta) -> RejectionRecord:
    return signal_rejected(signal_id, ticker, owner, reason, RejectionCategory.QUOTE, reason_code, RejectionSeverity.WARNING, **meta)
