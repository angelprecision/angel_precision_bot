# ap_signal_store.py — Angel Precision Signal Intelligence Store
# =============================================================================
# Non-blocking Supabase writes for the 4 signal intelligence tables.
#
# All writes go through a single background worker thread fed by a Queue.
# This eliminates unbounded thread creation and keeps execution non-blocking.
#
# Pass supabase_client=None to run without persistence (local/paper testing).
# =============================================================================

from __future__ import annotations

import logging
import queue
import threading
from datetime import datetime, timezone
from typing import Any, Callable, Optional

log = logging.getLogger("ap.signal_store")


class APSignalStore:
    """
    Non-blocking Supabase writes for signal-intelligence tables.

    Architecture:
        - Every write is a lambda pushed onto a Queue
        - One daemon worker thread drains the queue
        - No unbounded thread creation
        - Execution is never blocked
    """

    def __init__(self, supabase_client=None, client_email: str = "", system_version: str = "v2"):
        self.sb             = supabase_client
        self.client_email   = client_email
        self.system_version = system_version

        # Single write queue + worker — replaces per-write thread creation
        self._q: queue.Queue[tuple[str, str, Callable]] = queue.Queue()
        self._worker = threading.Thread(target=self._drain, daemon=True, name="ap-signal-store-worker")
        self._worker.start()

    # ── BACKGROUND WORKER ─────────────────────────────────────────────────────

    def _drain(self):
        """Single background thread that drains all pending writes."""
        while True:
            try:
                signal_id, label, fn = self._q.get()
                try:
                    fn()
                except Exception as exc:
                    log.warning(
                        "SignalStore write failed | signal_id=%s label=%s error=%s",
                        signal_id, label, exc,
                    )
                finally:
                    self._q.task_done()
            except Exception as exc:
                log.error("SignalStore worker error: %s", exc)

    def _enqueue(self, signal_id: str, label: str, fn: Callable):
        """Push a write onto the queue. Never blocks the caller."""
        if self.sb is None:
            return
        self._q.put((signal_id, label, fn))

    # ── PUBLIC API ────────────────────────────────────────────────────────────

    def insert_signal(self, signal_id: str, signal: dict, decision_status: str = "received"):
        """
        Upsert a new row into ap_signals.
        Uses upsert (not insert) so retries and duplicate event delivery are safe.
        """
        payload = {
            "signal_id":            signal_id,
            "client_email":         self.client_email or None,
            "system_version":       self.system_version,
            "ticker":               signal.get("ticker"),
            "pattern":              signal.get("pattern"),
            "timeframe":            signal.get("timeframe"),
            "side":                 signal.get("side"),
            "score":                signal.get("score"),
            "context_score":        (signal.get("score_breakdown") or {}).get("real_time_ctx"),
            "tier":                 signal.get("tier") or signal.get("grade"),
            "decision_status":      decision_status,
            "entry_trigger":        signal.get("entry_trigger", signal.get("entry_price")),
            "stop_price":           signal.get("stop_price"),
            "target_price":         signal.get("target_price"),
            "underlying_at_signal": signal.get("underlying_price", signal.get("price")),
            "signal_payload":       signal,
            "score_breakdown":      signal.get("score_breakdown"),
            "context_notes":        signal.get("context_notes"),
        }
        # HIGH-015: bind payload by default parameter to avoid lambda closure bug
        self._enqueue(
            signal_id, "insert_signal",
            lambda p=payload: self.sb.table("ap_signals").upsert(p).execute(),
        )

    def update_status(
        self,
        signal_id:      str,
        status:         str,
        timestamp_flag: Optional[str] = None,
        context_notes:  Optional[str] = None,
    ):
        """
        Update decision_status + optional lifecycle timestamp + optional context_notes.

        timestamp_flag: column name to stamp with now(), e.g. "queued_at", "triggered_at"
        context_notes:  use None to leave unchanged, "" to explicitly clear the field
        """
        patch: dict[str, Any] = {"decision_status": status}
        if timestamp_flag:
            patch[timestamp_flag] = datetime.now(timezone.utc).isoformat()
        # FIX: use `is not None` so an empty string explicitly clears the field
        if context_notes is not None:
            patch["context_notes"] = context_notes
        # HIGH-015: bind by default parameter to avoid lambda closure bug
        self._enqueue(
            signal_id, f"status_{status}",
            lambda p=patch, sid=signal_id: self.sb.table("ap_signals").update(p).eq("signal_id", sid).execute(),
        )

    def update_signal_fields(
        self,
        signal_id:      str,
        updates:        dict[str, Any],
        timestamp_flag: Optional[str] = None,
    ):
        """
        Freeform field update. Used for drop reasons (sector_cap,
        context_recheck_fail, requeued_after_trigger) and any other
        ad-hoc patches that don't need a dedicated method.

        timestamp_flag: optional column to stamp with now() alongside the patch.
        """
        if not updates:
            return
        patch = dict(updates)
        if timestamp_flag:
            patch[timestamp_flag] = datetime.now(timezone.utc).isoformat()
        # HIGH-015: bind by default parameter to avoid lambda closure bug
        self._enqueue(
            signal_id, "update_signal_fields",
            lambda p=patch, sid=signal_id: self.sb.table("ap_signals").update(p).eq("signal_id", sid).execute(),
        )

    def insert_option_outcome(self, signal_id: str, outcome: dict[str, Any]):
        """Upsert a row into ap_signal_option_outcomes."""
        row = {"signal_id": signal_id, **outcome}
        # HIGH-015: bind by default parameter to avoid lambda closure bug
        self._enqueue(
            signal_id, "option_outcome",
            lambda r=row: self.sb.table("ap_signal_option_outcomes").upsert(r).execute(),
        )

    def upsert_underlying_outcome(self, signal_id: str, outcome: dict[str, Any]):
        """Upsert a row into ap_signal_underlying_outcomes."""
        row = {"signal_id": signal_id, **outcome}
        # HIGH-015: bind by default parameter to avoid lambda closure bug
        self._enqueue(
            signal_id, "underlying_outcome",
            lambda r=row: self.sb.table("ap_signal_underlying_outcomes").upsert(r).execute(),
        )
