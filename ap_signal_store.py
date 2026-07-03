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
import re
import threading
from datetime import datetime, timezone
from typing import Any, Callable, Optional

log = logging.getLogger("ap.signal_store")


# ─────────────────────────────────────────────────────────────────────────────
# REEVAL: signal_id resolution
# ─────────────────────────────────────────────────────────────────────────────
# overnight_reeval.py wraps the original signal_id with REEVAL:<uuid>:<hex6>
# so master_control dedup treats each per-client re-evaluation as fresh. But
# the ap_signals.signal_id column is type UUID — writing the wrapped form
# raises Postgres 22P02 ("invalid input syntax for type uuid").
#
# All persistence into ap_signals must use the ORIGINAL UUID (the row that
# was originally inserted by the scanner). The REEVAL wrapping is only an
# in-memory dedup key, never a database column value.
#
# Pattern:  REEVAL:<uuid>:<6-hex-chars>
# Strip to: <uuid>
_REEVAL_PREFIX_RE = re.compile(
    r"^REEVAL:([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})(?::[0-9a-fA-F]+)?$"
)


def canonical_signal_id(signal_id: str) -> str:
    """Return the underlying UUID for ap_signals writes.

    For a plain UUID, returns it unchanged. For a REEVAL:<uuid>:<hex>
    wrapped ID, returns just the UUID part.

    Examples:
        '8d9338d0-5dde-4b7b-81ea-208039999b72'                  -> unchanged
        'REEVAL:8d9338d0-...:f4dc44'                            -> '8d9338d0-...'
    """
    if not isinstance(signal_id, str):
        return signal_id
    m = _REEVAL_PREFIX_RE.match(signal_id)
    if m:
        return m.group(1)
    return signal_id


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
        # P0 FIX (2026-05-21): canonicalize signal_id for the DB write.
        # REEVAL:<uuid>:<hex> wrapped IDs would fail with Postgres 22P02 because
        # the ap_signals.signal_id column is type UUID. The REEVAL wrapping is
        # an in-memory dedup key only; the row in ap_signals is keyed by the
        # original UUID.
        db_signal_id = canonical_signal_id(signal_id)
        payload = {
            "signal_id":            db_signal_id,
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
        # P0 (PR #260): per-client identity. ap_signals is keyed by
        # (signal_id, client_email) after migration
        # 2026_07_02_ap_signals_per_client_key.sql — the same scanner signal
        # fans out to ONE ROW PER CLIENT instead of last-writer-wins row
        # theft. client_email is never NULL: scanner/ownerless writes use
        # the '__shared__' sentinel the migration canonicalizes to.
        # Legacy fallback: if the migration is not yet applied, the
        # composite on_conflict raises (42P10 no matching unique constraint)
        # and we retry with the legacy single-key upsert — fail-open to
        # today's exact semantics, never worse, and log loudly so the
        # missing migration is visible.
        payload["client_email"] = payload.get("client_email") or "__shared__"

        def _upsert_per_client(p=payload):
            try:
                return (
                    self.sb.table("ap_signals")
                    .upsert(p, on_conflict="signal_id,client_email")
                    .execute()
                )
            except Exception as exc:
                log.error(
                    "AP_SIGNALS_PER_CLIENT_KEY_MISSING falling back to legacy "
                    "single-key upsert — apply migration "
                    "2026_07_02_ap_signals_per_client_key.sql | signal_id=%s "
                    "client=%s error=%s",
                    p.get("signal_id"), p.get("client_email"), exc,
                )
                return self.sb.table("ap_signals").upsert(p).execute()

        # HIGH-015: bind payload by default parameter to avoid lambda closure bug
        self._enqueue(signal_id, "insert_signal", _upsert_per_client)

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
        # P0 FIX (2026-05-21): canonicalize signal_id for the .eq filter so
        # REEVAL: wrapped IDs target the underlying ap_signals row (UUID column).
        db_signal_id = canonical_signal_id(signal_id)
        # HIGH-015: bind by default parameter to avoid lambda closure bug
        # P0 (PR #260): scope lifecycle updates to THIS client's row so one
        # client's queued/executed transition can never suppress the setup
        # for other clients. Ownerless stores mutate only '__shared__' rows.
        _own_email = self.client_email or "__shared__"
        self._enqueue(
            signal_id, f"status_{status}",
            lambda p=patch, sid=db_signal_id, ce=_own_email: (
                self.sb.table("ap_signals").update(p)
                .eq("signal_id", sid).eq("client_email", ce).execute()
            ),
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
        # P0 FIX (2026-05-21): canonicalize signal_id for the .eq filter.
        db_signal_id = canonical_signal_id(signal_id)
        # HIGH-015: bind by default parameter to avoid lambda closure bug
        # P0 (PR #260): client-scoped — see update_status.
        _own_email = self.client_email or "__shared__"
        self._enqueue(
            signal_id, "update_signal_fields",
            lambda p=patch, sid=db_signal_id, ce=_own_email: (
                self.sb.table("ap_signals").update(p)
                .eq("signal_id", sid).eq("client_email", ce).execute()
            ),
        )

    def insert_option_outcome(self, signal_id: str, outcome: dict[str, Any]):
        """Upsert a row into ap_signal_option_outcomes."""
        # P0 FIX (2026-05-21): canonicalize signal_id so foreign-key
        # references point at the underlying ap_signals UUID.
        db_signal_id = canonical_signal_id(signal_id)
        row = {"signal_id": db_signal_id, **outcome}
        # HIGH-015: bind by default parameter to avoid lambda closure bug
        self._enqueue(
            signal_id, "option_outcome",
            lambda r=row: self.sb.table("ap_signal_option_outcomes").upsert(r).execute(),
        )

    def upsert_underlying_outcome(self, signal_id: str, outcome: dict[str, Any]):
        """Upsert a row into ap_signal_underlying_outcomes."""
        # P0 FIX (2026-05-21): canonicalize signal_id so foreign-key
        # references point at the underlying ap_signals UUID.
        db_signal_id = canonical_signal_id(signal_id)
        row = {"signal_id": db_signal_id, **outcome}
        # HIGH-015: bind by default parameter to avoid lambda closure bug
        self._enqueue(
            signal_id, "underlying_outcome",
            lambda r=row: self.sb.table("ap_signal_underlying_outcomes").upsert(r).execute(),
        )
