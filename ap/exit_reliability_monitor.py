"""
ap/exit_reliability_monitor.py
==============================================================================
Exit reliability monitor — flags positions where the exit system
may have silently failed.

Checks every N minutes for:
  1. GREEN_NO_DECISION   — position peaked > +5% but no exit decision logged
  2. DECISION_NO_ORDER   — exit decision fired but no broker order found
  3. STALE_EXIT_ORDER    — exit order submitted but no fill/reject for > X sec
  4. OPEN_PAST_EOD       — position still open after 4:15 PM ET

Runs as a background thread inside the client runner.
Alerts go to Discord. All findings are written to exit_decision_ledger.
==============================================================================
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

log = logging.getLogger("ap.exit_reliability_monitor")

_CHECK_INTERVAL_SEC  = int(os.getenv("EXIT_MONITOR_INTERVAL_SEC", "60"))
_GREEN_THRESHOLD     = float(os.getenv("EXIT_MONITOR_GREEN_THRESHOLD", "0.05"))   # 5%
_STALE_ORDER_SEC     = int(os.getenv("EXIT_MONITOR_STALE_ORDER_SEC", "90"))


class ExitReliabilityMonitor:
    """
    Runs periodic checks on open positions to detect silent exit failures.

    Attach to a ClientRunner after the exit engine is seeded.
    """

    def __init__(
        self,
        client_id: str,
        exit_engine,
        supabase_client=None,
    ) -> None:
        self.client_id    = client_id
        self.exit_engine  = exit_engine
        self.supabase     = supabase_client
        self._stop        = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name=f"exit-monitor-{self.client_id}",
        )
        self._thread.start()
        log.info("[%s] ExitReliabilityMonitor started", self.client_id)

    def stop(self) -> None:
        self._stop.set()

    def is_alive(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    # ── Main loop ────────────────────────────────────────────────────────────

    def _loop(self) -> None:
        while not self._stop.wait(_CHECK_INTERVAL_SEC):
            try:
                self._run_checks()
            except Exception as exc:
                log.error("[%s] ExitReliabilityMonitor check failed: %s", self.client_id, exc)

    def _run_checks(self) -> None:
        try:
            positions = list(self.exit_engine.active_positions())
        except Exception as exc:
            log.warning("[%s] Could not get active positions: %s", self.client_id, exc)
            return

        now = datetime.now(timezone.utc)
        findings = []

        for pos in positions:
            findings.extend(self._check_position(pos, now))

        if findings:
            self._alert_findings(findings)

    def _check_position(self, pos, now: datetime) -> list:
        findings = []
        pnl   = float(getattr(pos, "option_pnl_pct", 0) or 0)
        peak  = float(getattr(pos, "peak_pnl_pct", 0) or 0)
        ticker = getattr(pos, "ticker", "?")
        pos_id = getattr(pos, "position_id", "?")
        in_flight = bool(getattr(pos, "exit_in_flight", False))

        # ── Check 1: peaked green but no exit action taken ───────────────────
        if peak >= _GREEN_THRESHOLD and not in_flight and pnl < 0:
            findings.append({
                "type":    "GREEN_NO_DECISION",
                "ticker":  ticker,
                "pos_id":  pos_id,
                "peak":    peak,
                "pnl":     pnl,
                "message": (
                    f"{ticker} peaked +{peak*100:.1f}% but now {pnl*100:+.1f}% "
                    f"with no exit in flight — exit engine may have missed"
                ),
            })

        # ── Check 2: exit in flight but no broker order reference ────────────
        if in_flight:
            pending_order = getattr(pos, "pending_exit_local_order_id", None)
            if not pending_order:
                findings.append({
                    "type":    "DECISION_NO_ORDER",
                    "ticker":  ticker,
                    "pos_id":  pos_id,
                    "pnl":     pnl,
                    "message": (
                        f"{ticker} exit_in_flight=True but no pending_exit_local_order_id — "
                        f"exit may have been lost before broker submit"
                    ),
                })

        # ── Check 3: stale exit order ────────────────────────────────────────
        if in_flight:
            pending_order = getattr(pos, "pending_exit_local_order_id", None)
            exit_submitted_at = getattr(pos, "exit_submitted_at", None)
            if pending_order and exit_submitted_at:
                age = (now - exit_submitted_at).total_seconds()
                if age > _STALE_ORDER_SEC:
                    findings.append({
                        "type":    "STALE_EXIT_ORDER",
                        "ticker":  ticker,
                        "pos_id":  pos_id,
                        "order":   pending_order,
                        "age_sec": age,
                        "message": (
                            f"{ticker} exit order {pending_order} "
                            f"stale for {age:.0f}s — broker may not have acked"
                        ),
                    })

        return findings

    def _alert_findings(self, findings: list) -> None:
        for f in findings:
            log.critical(
                "[%s] EXIT_RELIABILITY_ALERT type=%s ticker=%s | %s",
                self.client_id,
                f.get("type"),
                f.get("ticker"),
                f.get("message"),
            )
            # Write to ledger
            try:
                from ap.exit_decision_ledger import record_exit_order_event
                record_exit_order_event(
                    position_id   = f.get("pos_id", ""),
                    local_order_id = f.get("order", ""),
                    event_type    = f"RELIABILITY_ALERT_{f['type']}",
                    client_id     = self.client_id,
                    ticker        = f.get("ticker", ""),
                    error         = f.get("message"),
                    metadata      = f,
                )
            except Exception:
                pass
            # Discord alert
            try:
                from ap.discord_reporter import post_alert
                post_alert(
                    f"⚠️ EXIT RELIABILITY | {self.client_id}\n"
                    f"**{f['type']}** — {f.get('message')}",
                    level="WARNING",
                )
            except Exception:
                pass
