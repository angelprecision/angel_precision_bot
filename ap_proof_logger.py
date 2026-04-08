# =============================================================================
# PATCH: ap_proof_logger.py — FunnelCounter additions
# =============================================================================
# Replace the existing FunnelCounter class with this version.
# Adds the missing fields that the flow audit needs to be complete.
# Everything else in ap_proof_logger.py stays unchanged.
# =============================================================================

class FunnelCounter:
    """
    Tracks signal funnel stats throughout the trading day.
    Reset automatically at start of each new ET trading day.
    Shared across all modules — increment when gates fire.

    Import and use anywhere:
        from ap_proof_logger import funnel
        funnel.inc("signals_received")
        funnel.inc("context_blocked")
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._day  = None
        self.reset()

    def reset(self):
        with self._lock:
            self._day                  = datetime.now(ET).date()
            # ── intake ────────────────────────────────────────────────
            self.signals_received      = 0
            # ── score gate ────────────────────────────────────────────
            self.rejected_score        = 0    # NEW
            self.passed_score          = 0
            # ── context gate ──────────────────────────────────────────
            self.context_blocked       = 0
            self.passed_context        = 0
            # ── tier classify ─────────────────────────────────────────
            self.shadow_tracked        = 0
            # ── ranking queue ─────────────────────────────────────────
            self.sector_capped         = 0
            self.queue_expired         = 0
            # ── watcher ───────────────────────────────────────────────
            self.watcher_sent          = 0    # NEW
            self.watcher_expired       = 0    # NEW
            self.watcher_invalidated   = 0    # NEW
            self.watcher_triggered     = 0    # NEW
            # ── options gate ──────────────────────────────────────────
            self.options_rejected      = 0
            # ── execution ─────────────────────────────────────────────
            self.order_failed          = 0    # NEW
            self.trades_executed       = 0    # incremented at position OPEN
            # ── market context ────────────────────────────────────────
            self.regime                = "unknown"
            self.was_trend_day         = False

    def _check_day_reset(self):
        today = datetime.now(ET).date()
        if self._day and today != self._day:
            log.info(f"[FUNNEL] New trading day {today} — auto-resetting funnel counter")
            self.reset()

    def inc(self, field: str, by: int = 1):
        self._check_day_reset()
        with self._lock:
            current = getattr(self, field, 0)
            setattr(self, field, current + by)

    def set_regime(self, regime: str, trend_day: bool = False):
        with self._lock:
            self.regime        = regime
            self.was_trend_day = trend_day

    def snapshot(self) -> dict:
        self._check_day_reset()
        with self._lock:
            return {
                "signals_received":      self.signals_received,
                "rejected_score":        self.rejected_score,
                "passed_score_filter":   self.passed_score,
                "context_blocked":       self.context_blocked,
                "passed_context_filter": self.passed_context,
                "shadow_tracked":        self.shadow_tracked,
                "sector_capped":         self.sector_capped,
                "queue_expired":         self.queue_expired,
                "watcher_sent":          self.watcher_sent,
                "watcher_expired":       self.watcher_expired,
                "watcher_invalidated":   self.watcher_invalidated,
                "watcher_triggered":     self.watcher_triggered,
                "options_rejected":      self.options_rejected,
                "order_failed":          self.order_failed,
                "trades_executed":       self.trades_executed,
                "regime":                self.regime,
                "was_trend_day":         self.was_trend_day,
            }


# =============================================================================
# WHERE TO ADD funnel.inc() IN ap_execution_core.py
# =============================================================================
# These are the ONLY lines you need to add to ap_execution_core.py.
# Grep for the comment anchor, add the funnel.inc() right after.
# =============================================================================

# ── ANCHOR 1: after score gate rejection return ────────────────────────────
# Find:
#     funnel.inc("rejected_score")
#     self.store.update_status(signal_id, "rejected", ...)
# ADD before the existing funnel.inc line (or replace it — "rejected_score" already exists):
#     funnel.inc("rejected_score")     ← already there, nothing to change

# ── ANCHOR 2: in _start_ranking_processor, after watcher.add_signal succeeds ──
# Find:
#     added = self.watcher.add_signal(sig)
#     if added:
#         self.store.update_status(...)
# ADD inside the "if added:" block:
#         funnel.inc("watcher_sent")

# ── ANCHOR 3: in _on_signal_expire ────────────────────────────────────────────
# Find:
#     def _on_signal_expire(self, watched: WatchedSignal):
# ADD after the store.update_status line:
#         funnel.inc("watcher_expired")

# ── ANCHOR 4: in _on_signal_invalidate ────────────────────────────────────────
# Find:
#     def _on_signal_invalidate(self, watched: WatchedSignal):
# ADD after the store.update_status line:
#         funnel.inc("watcher_invalidated")

# ── ANCHOR 5: in _on_entry_trigger, after triggered status update ─────────────
# Find:
#     if signal_id:
#         self.store.update_status(signal_id, "triggered", timestamp_flag="triggered_at")
# ADD after it:
#         funnel.inc("watcher_triggered")

# ── ANCHOR 6: in _on_entry_trigger, after order failure ──────────────────────
# Find:
#     if not fill_price:
#         log.error(f"[{ticker}] Order failed — no fill price returned")
#         return
# ADD before the return:
#         funnel.inc("order_failed")
