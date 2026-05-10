"""
ap_lifecycle.py
===============
Canonical, immutable signal lifecycle ledger.
Every state transition gets recorded here.
If it's not logged here, it didn't happen.

Deploy TONIGHT before the overnight cron runs.
"""
from __future__ import annotations

import os
import json
import time
import uuid
import threading
import logging
from enum import Enum
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

log = logging.getLogger("ap.lifecycle")


class SignalState(str, Enum):
    CREATED = "CREATED"
    PERSISTED = "PERSISTED"
    LOADED_BY_OSM = "LOADED_BY_OSM"
    EVALUATING = "EVALUATING"
    REGISTERED_IN_WATCHER = "REGISTERED_IN_WATCHER"
    WATCHING = "WATCHING"
    REVALIDATING = "REVALIDATING"
    TRIGGER_READY = "TRIGGER_READY"
    ENTRY_SUBMITTED = "ENTRY_SUBMITTED"
    POSITION_OPENED = "POSITION_OPENED"
    INVALIDATED = "INVALIDATED"
    EXPIRED = "EXPIRED"
    REMOVED = "REMOVED"
    ERROR = "ERROR"


class LifecycleOwner(str, Enum):
    SCANNER = "SCANNER"
    SIGNAL_STORE = "SIGNAL_STORE"
    OVERNIGHT_EVAL = "OVERNIGHT_EVAL"
    OSM = "OSM"
    WATCHER = "WATCHER"
    MORNING_CRON = "MORNING_CRON"
    EXECUTION_CORE = "EXECUTION_CORE"
    RECONCILER = "RECONCILER"
    RECOVERY = "RECOVERY"
    UNKNOWN = "UNKNOWN"


# Legal transitions — anything else is a bug
LEGAL_TRANSITIONS: Dict[Optional[SignalState], set] = {
    None: {SignalState.CREATED},
    SignalState.CREATED: {SignalState.PERSISTED, SignalState.ERROR},
    SignalState.PERSISTED: {SignalState.LOADED_BY_OSM, SignalState.EXPIRED, SignalState.ERROR},
    SignalState.LOADED_BY_OSM: {SignalState.EVALUATING, SignalState.REGISTERED_IN_WATCHER, SignalState.WATCHING, SignalState.INVALIDATED, SignalState.ERROR},
    SignalState.EVALUATING: {SignalState.WATCHING, SignalState.INVALIDATED, SignalState.EXPIRED, SignalState.ERROR},
    SignalState.REGISTERED_IN_WATCHER: {SignalState.WATCHING, SignalState.ERROR},
    SignalState.WATCHING: {SignalState.REVALIDATING, SignalState.TRIGGER_READY, SignalState.INVALIDATED, SignalState.EXPIRED, SignalState.REMOVED, SignalState.ERROR},
    SignalState.REVALIDATING: {SignalState.WATCHING, SignalState.INVALIDATED, SignalState.EXPIRED, SignalState.ERROR},
    SignalState.TRIGGER_READY: {SignalState.ENTRY_SUBMITTED, SignalState.WATCHING, SignalState.INVALIDATED, SignalState.ERROR},
    SignalState.ENTRY_SUBMITTED: {SignalState.POSITION_OPENED, SignalState.ERROR},
    SignalState.POSITION_OPENED: set(),  # terminal for signal lifecycle
    SignalState.INVALIDATED: {SignalState.REMOVED},
    SignalState.EXPIRED: {SignalState.REMOVED},
    SignalState.REMOVED: set(),
    SignalState.ERROR: {SignalState.REMOVED, SignalState.INVALIDATED},
}


@dataclass(frozen=True)
class LedgerEntry:
    entry_id: str
    correlation_id: str
    signal_id: str
    ticker: str
    from_state: Optional[str]
    to_state: str
    owner: str
    reason: str
    timestamp_iso: str
    timestamp_epoch: float
    metadata: Dict[str, Any]

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


class SignalLifecycleLedger:
    """Singleton. Append-only. Immutable records."""

    _instance: Optional["SignalLifecycleLedger"] = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                inst = super().__new__(cls)
                inst._entries: List[LedgerEntry] = []
                inst._current_state: Dict[str, SignalState] = {}
                inst._correlation_ids: Dict[str, str] = {}
                inst._entry_lock = threading.RLock()
                inst._log_path = os.getenv("AP_LEDGER_PATH", "logs/signal_ledger.jsonl")
                inst._db_writer = None
                os.makedirs(os.path.dirname(inst._log_path) or "logs", exist_ok=True)
                cls._instance = inst
            return cls._instance

    def wire_db_writer(self, fn) -> None:
        self._db_writer = fn

    def _correlation_id(self, signal_id: str) -> str:
        with self._entry_lock:
            if signal_id not in self._correlation_ids:
                self._correlation_ids[signal_id] = uuid.uuid4().hex[:12]
            return self._correlation_ids[signal_id]

    def current_state(self, signal_id: str) -> Optional[SignalState]:
        return self._current_state.get(str(signal_id))

    def transition(
        self,
        signal_id: str,
        ticker: str,
        to_state: SignalState,
        owner: str,
        reason: str,
        from_state: Optional[SignalState] = None,
        metadata: Optional[Dict] = None,
        strict: bool = False,
    ) -> LedgerEntry:
        with self._entry_lock:
            actual_from = self._current_state.get(str(signal_id))
            if from_state is None:
                from_state = actual_from

            # Validate legality
            legal = LEGAL_TRANSITIONS.get(from_state, set())
            if to_state not in legal and from_state != to_state:
                violation_msg = (
                    f"ILLEGAL TRANSITION signal={signal_id} ticker={ticker} "
                    f"{from_state.value if from_state else 'NONE'}->{to_state.value} "
                    f"by {owner} reason={reason}"
                )
                log.error("[LEDGER] %s", violation_msg)

                # Always record the attempt for forensics
                self._record(signal_id, ticker, from_state, SignalState.ERROR,
                             owner, f"ILLEGAL_TRANSITION: {reason}", metadata or {})

                if strict:
                    raise RuntimeError(violation_msg)

            entry = self._record(
                signal_id, ticker, from_state, to_state,
                owner, reason, metadata or {}
            )
            self._current_state[str(signal_id)] = to_state
            return entry

    def _record(self, signal_id, ticker, from_state, to_state, owner, reason, metadata) -> LedgerEntry:
        now = time.time()
        entry = LedgerEntry(
            entry_id=uuid.uuid4().hex,
            correlation_id=self._correlation_id(str(signal_id)),
            signal_id=str(signal_id),
            ticker=str(ticker).upper(),
            from_state=from_state.value if from_state else None,
            to_state=to_state.value,
            owner=str(owner),
            reason=str(reason),
            timestamp_iso=datetime.now(timezone.utc).isoformat(),
            timestamp_epoch=now,
            metadata=metadata,
        )

        self._entries.append(entry)

        # STRUCTURED LOG — grep for [SIGNAL_TRACE]
        log.warning(
            "[SIGNAL_TRACE] corr=%s id=%s ticker=%s %s->%s owner=%s reason=%s",
            entry.correlation_id, entry.signal_id, entry.ticker,
            entry.from_state or "NONE", entry.to_state,
            entry.owner, entry.reason,
        )

        # Append-only file
        try:
            with open(self._log_path, "a") as f:
                f.write(entry.to_json() + "\n")
        except Exception as e:
            log.error("[LEDGER] file write failed: %s", e)

        # DB persistence
        if self._db_writer:
            try:
                self._db_writer(asdict(entry))
            except Exception as e:
                log.error("[LEDGER] db write failed: %s", e)

        # INVARIANT CHECK: WATCHING -> REMOVED without terminal is a bug
        if (from_state == SignalState.WATCHING and
                to_state == SignalState.REMOVED):
            log.error(
                "[INVARIANT_VIOLATION] Signal %s (%s) went WATCHING->REMOVED "
                "without terminal state! owner=%s reason=%s",
                signal_id, ticker, owner, reason,
            )

        return entry

    def history(self, signal_id: str) -> List[LedgerEntry]:
        with self._entry_lock:
            return [e for e in self._entries if e.signal_id == str(signal_id)]

    def trace_dump(self, signal_id: str) -> str:
        entries = self.history(signal_id)
        if not entries:
            return f"NO TRACE for signal_id={signal_id}"
        lines = [f"=== LIFECYCLE TRACE signal_id={signal_id} ==="]
        for e in entries:
            lines.append(
                f"  {e.timestamp_iso} | {e.from_state or 'NONE'} -> {e.to_state} "
                f"| owner={e.owner} | reason={e.reason}"
            )
        return "\n".join(lines)


# Singleton
LEDGER = SignalLifecycleLedger()
