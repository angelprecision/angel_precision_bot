# ap_signal_store.py — Angel Precision Signal Intelligence Store
# =============================================================================
# Non-blocking Supabase writes for the 4 signal intelligence tables.
# Every write is fire-and-forget in a daemon thread — execution is never blocked.
# Pass supabase_client=None to run without persistence (local/paper testing).
# =============================================================================

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger("ap.signal_store")


class APSignalStore:
    """Non-blocking Supabase writes for signal-intelligence tables."""

    def __init__(self, supabase_client=None, client_email: str = "", system_version: str = "v2"):
        self.sb             = supabase_client
        self.client_email   = client_email
        self.system_version = system_version

    def _fire(self, fn, label: str):
        if self.sb is None:
            return

        def _runner():
            try:
                fn()
            except Exception as exc:
                log.warning("SignalStore %s failed: %s", label, exc)

        threading.Thread(target=_runner, daemon=True, name=f"ap-signal-store-{label}").start()

    def insert_signal(self, signal_id: str, signal: dict, decision_status: str = "received"):
        payload = {
            "signal_id":          signal_id,
            "client_email":       self.client_email or None,
            "system_version":     self.system_version,
            "ticker":             signal.get("ticker"),
            "pattern":            signal.get("pattern"),
            "timeframe":          signal.get("timeframe"),
            "side":               signal.get("side"),
            "score":              signal.get("score"),
            "context_score":      (signal.get("score_breakdown") or {}).get("real_time_ctx"),
            "tier":               signal.get("tier") or signal.get("grade"),
            "decision_status":    decision_status,
            "entry_trigger":      signal.get("entry_trigger", signal.get("entry_price")),
            "stop_price":         signal.get("stop_price"),
            "target_price":       signal.get("target_price"),
            "underlying_at_signal": signal.get("underlying_price", signal.get("price")),
            "signal_payload":     signal,
            "score_breakdown":    signal.get("score_breakdown"),
            "context_notes":      signal.get("context_notes"),
        }
        self._fire(
            lambda: self.sb.table("ap_signals").insert(payload).execute(),
            "insert_signal",
        )

    def update_status(self, signal_id: str, status: str, timestamp_flag: Optional[str] = None):
        patch: dict[str, Any] = {"decision_status": status}
        if timestamp_flag:
            patch[timestamp_flag] = datetime.now(timezone.utc).isoformat()
        self._fire(
            lambda: self.sb.table("ap_signals").update(patch).eq("signal_id", signal_id).execute(),
            f"status_{status}",
        )

    def update_signal_fields(self, signal_id: str, updates: dict[str, Any]):
        if not updates:
            return
        self._fire(
            lambda: self.sb.table("ap_signals").update(updates).eq("signal_id", signal_id).execute(),
            "update_signal_fields",
        )

    def insert_option_outcome(self, signal_id: str, outcome: dict[str, Any]):
        row = {"signal_id": signal_id, **outcome}
        self._fire(
            lambda: self.sb.table("ap_signal_option_outcomes").upsert(row).execute(),
            "option_outcome",
        )

    def upsert_underlying_outcome(self, signal_id: str, outcome: dict[str, Any]):
        row = {"signal_id": signal_id, **outcome}
        self._fire(
            lambda: self.sb.table("ap_signal_underlying_outcomes").upsert(row).execute(),
            "underlying_outcome",
        )
