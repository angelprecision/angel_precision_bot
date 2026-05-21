"""
ap/reason_codes.py — Single source of truth for reason codes.

Background
==========
88+ reason codes were scattered as magic strings across the bot (entry gate,
exit engine, reconciler, order monitor, kill switch, etc). Drift between
modules was inevitable and made operator post-mortems painful.

This module:
  1. Defines every reason code as a typed constant.
  2. Groups them by category (RISK_BLOCK, BROKER_ERR, EXIT, LIFECYCLE, …).
  3. Provides a registry so the admin dashboard can render human-readable
     descriptions instead of raw codes.
  4. Exposes validate(code) for CI/runtime checking that callsites use a
     known code.

Usage in code (gradual migration — no big-bang refactor required):
    from ap.reason_codes import REASON

    decision = ExitDecision(
        action="CLOSE_ALL",
        reason_code=REASON.SENTINEL_FORCED_EXIT,
        reason="Daily loss limit breached",
        urgency="IMMEDIATE",
    )

Adding a new reason code:
  1. Add the constant under the appropriate category below.
  2. Add a one-line description in REASON_DESCRIPTIONS.
  3. If the code represents a block/reject, add it to BLOCK_REASONS so the
     dashboard categorizes correctly.

Migration policy:
  - New code MUST use REASON.* constants, never raw strings.
  - Existing magic strings can stay; CI check warns but does not fail.
  - As callsites are touched for other reasons, migrate them.
  - Eventually CI check is upgraded from warn to fail.

Reviewed and audited 2026-05-21.
"""
from __future__ import annotations
from typing import Final


class REASON:
    """All canonical reason codes. Access as REASON.SENTINEL_FORCED_EXIT etc."""

    # ── Entry gate: risk blocks ─────────────────────────────────────────────
    BLOCKED_RISK_MAX_POSITIONS: Final[str] = "BLOCKED_RISK_MAX_POSITIONS"
    BLOCKED_RISK_MAX_PENDING:   Final[str] = "BLOCKED_RISK_MAX_PENDING"
    BLOCKED_RISK_CAPITAL:       Final[str] = "BLOCKED_RISK_CAPITAL"
    BLOCKED_RISK_SECTOR_CAP:    Final[str] = "BLOCKED_RISK_SECTOR_CAP"
    BLOCKED_RISK_TICKER_CAP:    Final[str] = "BLOCKED_RISK_TICKER_CAP"
    BLOCKED_RISK_MAX_CALLS:     Final[str] = "BLOCKED_RISK_MAX_CALLS"
    BLOCKED_RISK_MAX_PUTS:      Final[str] = "BLOCKED_RISK_MAX_PUTS"
    BLOCKED_RISK_MAX_TRADES:    Final[str] = "BLOCKED_RISK_MAX_TRADES"
    BLOCKED_RISK_DAILY_LOSS:    Final[str] = "BLOCKED_RISK_DAILY_LOSS"
    BLOCKED_RISK_TICKER_ACTIVE: Final[str] = "BLOCKED_RISK_TICKER_ACTIVE"
    BLOCKED_RISK_COOLDOWN:      Final[str] = "BLOCKED_RISK_COOLDOWN"
    BLOCKED_RISK_PENDING_EXISTS: Final[str] = "BLOCKED_RISK_PENDING_EXISTS"

    # ── Entry gate: score blocks ────────────────────────────────────────────
    BLOCKED_SCORE_PRIORITY: Final[str] = "BLOCKED_SCORE_PRIORITY"
    BLOCKED_SCORE_FLOOR:    Final[str] = "BLOCKED_SCORE_FLOOR"
    BLOCKED_SCORE_CONTEXT:  Final[str] = "BLOCKED_SCORE_CONTEXT"
    BLOCKED_SCORE_TIER:     Final[str] = "BLOCKED_SCORE_TIER"

    # ── Entry gate: system blocks ───────────────────────────────────────────
    BLOCKED_SYSTEM_EXIT_DOWN:      Final[str] = "BLOCKED_SYSTEM_EXIT_DOWN"
    BLOCKED_SYSTEM_KILL_SWITCH:    Final[str] = "BLOCKED_SYSTEM_KILL_SWITCH"
    BLOCKED_SYSTEM_MODE_READ_ONLY: Final[str] = "BLOCKED_SYSTEM_MODE_READ_ONLY"
    BLOCKED_SYSTEM_DUPLICATE_SIG:  Final[str] = "BLOCKED_SYSTEM_DUPLICATE_SIG"
    BLOCKED_SYSTEM_DUPLICATE_SETUP:Final[str] = "BLOCKED_SYSTEM_DUPLICATE_SETUP"
    BLOCKED_SYSTEM_LIVE_REQUIRES_EV: Final[str] = "BLOCKED_SYSTEM_LIVE_REQUIRES_EV"

    # ── Exit engine ─────────────────────────────────────────────────────────
    EXIT_TARGET_HIT:        Final[str] = "EXIT_TARGET_HIT"
    EXIT_HARD_STOP:         Final[str] = "HARD_STOP"
    EXIT_DEEP_LOSS_STOP:    Final[str] = "DEEP_LOSS_STOP"
    EXIT_THESIS_FAIL_STOP:  Final[str] = "THESIS_FAIL_SOFT_STOP"
    EXIT_NEVER_GREEN_STOP:  Final[str] = "NEVER_GREEN_STOP"
    EXIT_PROFIT_LOCK:       Final[str] = "PROFIT_LOCK"
    EXIT_TOUCHED_PROFIT:    Final[str] = "TOUCHED_PROFIT_STOP"
    EXIT_IMMEDIATE_TP:      Final[str] = "IMMEDIATE_TP"
    EXIT_EOD_FORCE_CLOSE:   Final[str] = "EOD_FORCE_CLOSE"
    EXIT_SENTINEL_FORCED:   Final[str] = "SENTINEL_FORCED_EXIT"
    EXIT_THETA_STOP:        Final[str] = "THETA_STOP"
    EXIT_TIME_STOP:         Final[str] = "TIME_STOP"
    EXIT_UNDERLYING_STOP:   Final[str] = "STOP"
    EXIT_SCALE_OUT:         Final[str] = "SCALE_OUT"
    EXIT_RUNNER_TRAIL:      Final[str] = "RUNNER_TRAIL"

    # ── Lifecycle / order monitor ───────────────────────────────────────────
    LOST_HANDOFF:                  Final[str] = "LOST_HANDOFF"
    MISSED_MOVE_ENTRY_CANCEL:      Final[str] = "MISSED_MOVE_ENTRY_CANCEL"
    STALE_ENTRY_CANCEL:            Final[str] = "STALE_ENTRY_CANCEL"
    ENTRY_ACK_TIMEOUT_CANCEL:      Final[str] = "ENTRY_ACK_TIMEOUT_CANCEL"
    CREATED_NO_BROKER_ID_WATCHDOG: Final[str] = "CREATED_NO_BROKER_ID_WATCHDOG"
    EXPIRED_CONTRACT_LOCAL_CLEANUP: Final[str] = "EXPIRED_CONTRACT_LOCAL_CLEANUP"

    # ── Watcher / signal lifecycle ──────────────────────────────────────────
    WATCH_ARM_FAILED_DRIFT:       Final[str] = "WATCH_ARM_FAILED_DRIFT"
    WATCH_ARM_FAILED_BELOW_STOP:  Final[str] = "WATCH_ARM_FAILED_BELOW_STOP"
    WATCH_ARM_FAILED_OSM_VALIDATION: Final[str] = "WATCH_ARM_FAILED_OSM_VALIDATION"
    WATCH_DEDUP_BLOCK:            Final[str] = "WATCH_DEDUP_BLOCK"
    WATCHER_INVALIDATED:          Final[str] = "WATCHER_INVALIDATED"
    WATCHER_EXPIRED:              Final[str] = "WATCHER_EXPIRED"

    # ── Broker errors ───────────────────────────────────────────────────────
    BROKER_HTTP_4XX:                Final[str] = "BROKER_HTTP_4XX"
    BROKER_HTTP_5XX_EXHAUSTED:      Final[str] = "BROKER_HTTP_5XX_EXHAUSTED"
    BROKER_CONN_ERROR:              Final[str] = "BROKER_CONN_ERROR"
    BROKER_READ_TIMEOUT_AMBIGUOUS:  Final[str] = "BROKER_READ_TIMEOUT_AMBIGUOUS"
    BROKER_REJECTED_ENTRY:          Final[str] = "BROKER_REJECTED_ENTRY"
    BROKER_REJECTED_EXIT:           Final[str] = "BROKER_REJECTED_EXIT"
    BROKER_OVERFILL_CLAMPED:        Final[str] = "BROKER_OVERFILL_CLAMPED"
    BROKER_FILL_ANOMALY:            Final[str] = "BROKER_FILL_ANOMALY"
    BROKER_CONFIRMED_CLOSED:        Final[str] = "BROKER_CONFIRMED_CLOSED"
    BROKER_POSITION_IMPORT_FAILED:  Final[str] = "BROKER_POSITION_IMPORT_FAILED"
    BROKER_POSITION_MISSING_THREE_PASS_CONFIRM: Final[str] = "BROKER_POSITION_MISSING_THREE_PASS_CONFIRM"
    BROKER_STATUS_UNKNOWN:          Final[str] = "BROKER_STATUS_UNKNOWN"
    BROKER_FILL_CHECK_ERROR:        Final[str] = "BROKER_FILL_CHECK_ERROR"

    # ── Reconciler / state integrity ────────────────────────────────────────
    RECON_GHOST_POSITION_CLEARED:   Final[str] = "RECON_GHOST_POSITION_CLEARED"
    RECON_DB_HEAL_FROM_BROKER:      Final[str] = "RECON_DB_HEAL_FROM_BROKER"
    RECON_DUP_POSITION_MERGED:      Final[str] = "RECON_DUP_POSITION_MERGED"
    RECON_EXIT_FILLED_FROM_ORDERS:  Final[str] = "RECON_EXIT_FILLED_FROM_ORDERS"
    RECON_STALE_ACK_RESOLVED:       Final[str] = "RECON_STALE_ACK_RESOLVED"
    DB_UPDATE_MISSED:               Final[str] = "DB_UPDATE_MISSED"
    DB_ROWCOUNT_UNCONFIRMED:        Final[str] = "DB_ROWCOUNT_UNCONFIRMED"
    DEDUP_PERSIST_FAILED:           Final[str] = "DEDUP_PERSIST_FAILED"
    DUPLICATE_EXIT_FILL_IGNORED:    Final[str] = "DUPLICATE_EXIT_FILL_IGNORED"
    FILL_QTY_REGRESSION:            Final[str] = "FILL_QTY_REGRESSION"

    # ── Contract selection ──────────────────────────────────────────────────
    BUDGET_CLIPPED_BY_SELECTOR:     Final[str] = "BUDGET_CLIPPED_BY_SELECTOR"
    CAPITAL_UTIL_BLOCK:             Final[str] = "CAPITAL_UTIL_BLOCK"
    CHAIN_HEALTH_FAILED:            Final[str] = "CHAIN_HEALTH_FAILED"
    CHEAP_CONTRACT_NO_UPGRADE:      Final[str] = "CHEAP_CONTRACT_NO_UPGRADE"
    CHEAP_CONTRACT_UPGRADED:        Final[str] = "CHEAP_CONTRACT_UPGRADED"
    CHEAP_CONTRACT_ONLY_CHOICE:     Final[str] = "CHEAP_CONTRACT_ONLY_CHOICE"
    DELTA_OUT_OF_RANGE:             Final[str] = "DELTA_OUT_OF_RANGE"
    EARNINGS_LOCKOUT:               Final[str] = "EARNINGS_LOCKOUT"
    EARNINGS_GUARD_ERROR:           Final[str] = "EARNINGS_GUARD_ERROR"
    FORCED_1_FOR_PAPER:             Final[str] = "FORCED_1_FOR_PAPER"

    # ── Client / runner ──────────────────────────────────────────────────────
    CLIENT_ENTRIES_PAUSED:          Final[str] = "CLIENT_ENTRIES_PAUSED"
    RUNNER_STARTUP_SKIPPED:         Final[str] = "RUNNER_STARTUP_SKIPPED"
    DEFERRED_CONTRACT_BLOCKED:      Final[str] = "DEFERRED_CONTRACT_BLOCKED"
    FILL_MONITOR_NOT_CONFIRMED:     Final[str] = "FILL_MONITOR_NOT_CONFIRMED"

    # ── Force-close / circuit breaker ───────────────────────────────────────
    FORCE_CLOSE_ALL_REQUESTED:      Final[str] = "FORCE_CLOSE_ALL_REQUESTED"
    DAILY_LOSS_LIMIT_BREACHED:      Final[str] = "DAILY_LOSS_LIMIT_BREACHED"
    DAILY_LOSS_LIMIT_TICK_DETECTED: Final[str] = "DAILY_LOSS_LIMIT_TICK_DETECTED"
    DAILY_LOSS_LIMIT_RECONCILER_DETECTED: Final[str] = "DAILY_LOSS_LIMIT_RECONCILER_DETECTED"
    KILL_SWITCH_ACTIVATED:          Final[str] = "KILL_SWITCH_ACTIVATED"
    KILL_SWITCH_CLEARED:            Final[str] = "KILL_SWITCH_CLEARED"
    GLOBAL_FLATTEN_REQUESTED:       Final[str] = "GLOBAL_FLATTEN_REQUESTED"
    ENTRIES_PAUSED_REQUESTED:       Final[str] = "ENTRIES_PAUSED_REQUESTED"
    ENTRIES_RESUMED:                Final[str] = "ENTRIES_RESUMED"


# Human-readable descriptions for the admin dashboard.
# Keep these short and operator-facing.
REASON_DESCRIPTIONS: dict[str, str] = {
    # Risk
    REASON.BLOCKED_RISK_MAX_POSITIONS:  "Already at max concurrent positions",
    REASON.BLOCKED_RISK_MAX_PENDING:    "Pending entries would exceed position cap",
    REASON.BLOCKED_RISK_CAPITAL:        "Projected capital exposure exceeds limit",
    REASON.BLOCKED_RISK_SECTOR_CAP:     "Sector exposure cap reached",
    REASON.BLOCKED_RISK_TICKER_CAP:     "Per-ticker exposure cap reached",
    REASON.BLOCKED_RISK_MAX_CALLS:      "Open calls at limit",
    REASON.BLOCKED_RISK_MAX_PUTS:       "Open puts at limit",
    REASON.BLOCKED_RISK_MAX_TRADES:     "Daily trade count cap reached",
    REASON.BLOCKED_RISK_DAILY_LOSS:     "Daily realized P&L below loss limit",
    REASON.BLOCKED_RISK_TICKER_ACTIVE:  "Ticker already has open position",
    REASON.BLOCKED_RISK_COOLDOWN:       "Same setup is in cooldown window",
    REASON.BLOCKED_RISK_PENDING_EXISTS: "Pending entry already exists for ticker",
    # Score
    REASON.BLOCKED_SCORE_PRIORITY: "Score below priority-tier floor",
    REASON.BLOCKED_SCORE_FLOOR:    "Score below mode-effective floor",
    REASON.BLOCKED_SCORE_CONTEXT:  "Context score below floor",
    REASON.BLOCKED_SCORE_TIER:     "Tier engine rejected the score",
    # System
    REASON.BLOCKED_SYSTEM_EXIT_DOWN:      "Exit engine offline — entries blocked for safety",
    REASON.BLOCKED_SYSTEM_KILL_SWITCH:    "Kill switch is active",
    REASON.BLOCKED_SYSTEM_MODE_READ_ONLY: "Mode is read-only / no-trade",
    REASON.BLOCKED_SYSTEM_DUPLICATE_SIG:  "Signal ID already seen this session",
    REASON.BLOCKED_SYSTEM_DUPLICATE_SETUP:"Same ticker+side+timeframe already armed",
    REASON.BLOCKED_SYSTEM_LIVE_REQUIRES_EV: "LIVE mode requires positive EV score",
    # Exit
    REASON.EXIT_TARGET_HIT:       "Underlying hit profit target",
    REASON.EXIT_HARD_STOP:        "Option PnL exceeded -33% hard stop",
    REASON.EXIT_DEEP_LOSS_STOP:   "Option PnL exceeded -20% deep loss floor",
    REASON.EXIT_THESIS_FAIL_STOP: "Soft stop fired — thesis no longer confirming",
    REASON.EXIT_NEVER_GREEN_STOP: "Trade never went green within hold window",
    REASON.EXIT_PROFIT_LOCK:      "Peak profit retraced — locking gains",
    REASON.EXIT_TOUCHED_PROFIT:   "Profit target was touched then gave back",
    REASON.EXIT_IMMEDIATE_TP:     "Immediate take-profit threshold hit",
    REASON.EXIT_EOD_FORCE_CLOSE:  "End-of-day forced close (day orders expiring)",
    REASON.EXIT_SENTINEL_FORCED:  "Sentinel / kill switch forced exit",
    REASON.EXIT_THETA_STOP:       "Theta decay stop fired",
    REASON.EXIT_TIME_STOP:        "Time-based exit fired",
    REASON.EXIT_UNDERLYING_STOP:  "Underlying price hit stop level",
    REASON.EXIT_SCALE_OUT:        "Scale-out partial close",
    REASON.EXIT_RUNNER_TRAIL:     "Runner emergency trail triggered",
    # Lifecycle
    REASON.LOST_HANDOFF:          "Entry stuck CREATED — watcher handoff failed",
    REASON.MISSED_MOVE_ENTRY_CANCEL: "Entry canceled — move ran past limit before fill",
    REASON.STALE_ENTRY_CANCEL:    "Entry canceled — stale scalp setup",
    REASON.ENTRY_ACK_TIMEOUT_CANCEL: "Entry acknowledged but not filled within timeout",
    REASON.CREATED_NO_BROKER_ID_WATCHDOG: "Phantom CREATED order cleaned up by watchdog",
    REASON.EXPIRED_CONTRACT_LOCAL_CLEANUP: "Contract expired — local-only cleanup",
    # Watcher
    REASON.WATCH_ARM_FAILED_DRIFT: "Arm rejected — price drifted past tolerance",
    REASON.WATCH_ARM_FAILED_BELOW_STOP: "Arm rejected — current price below stop level",
    REASON.WATCH_ARM_FAILED_OSM_VALIDATION: "Arm rejected — OSM validation failed",
    REASON.WATCH_DEDUP_BLOCK:     "Watcher dedup — signal already armed",
    REASON.WATCHER_INVALIDATED:   "Watcher invalidated — thesis broken",
    REASON.WATCHER_EXPIRED:       "Watcher expired without breach",
    # Broker
    REASON.BROKER_HTTP_4XX:               "Broker rejected with 4xx (permanent)",
    REASON.BROKER_HTTP_5XX_EXHAUSTED:     "Broker 5xx errors exhausted retries",
    REASON.BROKER_CONN_ERROR:             "Broker connection error",
    REASON.BROKER_READ_TIMEOUT_AMBIGUOUS: "Broker read timeout (order may or may not have landed)",
    REASON.BROKER_REJECTED_ENTRY:         "Broker rejected entry order",
    REASON.BROKER_REJECTED_EXIT:          "Broker rejected exit order",
    REASON.BROKER_OVERFILL_CLAMPED:       "Broker reported overfill — clamped to expected qty",
    REASON.BROKER_FILL_ANOMALY:           "Broker fill data inconsistent",
    REASON.BROKER_CONFIRMED_CLOSED:       "Broker confirmed position is closed",
    REASON.BROKER_POSITION_IMPORT_FAILED: "Failed to import position from broker",
    REASON.BROKER_POSITION_MISSING_THREE_PASS_CONFIRM: "Position missing 3 reconciler passes — likely closed",
    REASON.BROKER_STATUS_UNKNOWN:         "Broker returned unknown order status",
    REASON.BROKER_FILL_CHECK_ERROR:       "Error checking broker fill status",
    # Recon
    REASON.RECON_GHOST_POSITION_CLEARED:  "Reconciler cleared ghost position from DB",
    REASON.RECON_DB_HEAL_FROM_BROKER:     "DB row healed from broker truth",
    REASON.RECON_DUP_POSITION_MERGED:     "Duplicate position rows merged",
    REASON.RECON_EXIT_FILLED_FROM_ORDERS: "Position finalized from EXIT_FILLED order",
    REASON.RECON_STALE_ACK_RESOLVED:      "Stale acknowledged exit resolved by reconciler",
    REASON.DB_UPDATE_MISSED:              "DB UPDATE did not affect expected rows",
    REASON.DB_ROWCOUNT_UNCONFIRMED:       "DB rowcount could not be verified",
    REASON.DEDUP_PERSIST_FAILED:          "Dedup record failed to persist",
    REASON.DUPLICATE_EXIT_FILL_IGNORED:   "Duplicate exit fill event ignored",
    REASON.FILL_QTY_REGRESSION:           "Broker fill quantity decreased — anomaly",
    # Selector
    REASON.BUDGET_CLIPPED_BY_SELECTOR: "Contract budget clipped to client max",
    REASON.CAPITAL_UTIL_BLOCK:         "Capital utilization gate blocked",
    REASON.CHAIN_HEALTH_FAILED:        "Option chain quote health check failed",
    REASON.CHEAP_CONTRACT_NO_UPGRADE:  "Cheap-contract policy denied upgrade",
    REASON.CHEAP_CONTRACT_UPGRADED:    "Cheap contract upgraded to wider strike",
    REASON.CHEAP_CONTRACT_ONLY_CHOICE: "Only cheap contract available — accepted",
    REASON.DELTA_OUT_OF_RANGE:         "Selected contract delta outside allowed band",
    REASON.EARNINGS_LOCKOUT:           "Earnings within lockout window",
    REASON.EARNINGS_GUARD_ERROR:       "Earnings guard check failed",
    REASON.FORCED_1_FOR_PAPER:         "Paper mode forced to 1 contract",
    # Client / runner
    REASON.CLIENT_ENTRIES_PAUSED:    "Client entries are paused",
    REASON.RUNNER_STARTUP_SKIPPED:   "Runner startup skipped due to error",
    REASON.DEFERRED_CONTRACT_BLOCKED:"Submission blocked — contract symbol unresolved",
    REASON.FILL_MONITOR_NOT_CONFIRMED:"Fill monitor not running for this client",
    # Force-close / breaker
    REASON.FORCE_CLOSE_ALL_REQUESTED:           "Force-close-all breaker has been tripped",
    REASON.DAILY_LOSS_LIMIT_BREACHED:           "Daily loss limit breached (entry-gate detected)",
    REASON.DAILY_LOSS_LIMIT_TICK_DETECTED:      "Daily loss limit breached (exit-engine tick detected)",
    REASON.DAILY_LOSS_LIMIT_RECONCILER_DETECTED:"Daily loss limit breached (reconciler tick detected)",
    REASON.KILL_SWITCH_ACTIVATED:               "Kill switch activated",
    REASON.KILL_SWITCH_CLEARED:                 "Kill switch cleared",
    REASON.GLOBAL_FLATTEN_REQUESTED:            "Global flatten-all requested",
    REASON.ENTRIES_PAUSED_REQUESTED:            "Block-entries requested",
    REASON.ENTRIES_RESUMED:                     "Entries resumed",
}


# Categorization for the dashboard. Any block reason goes here.
BLOCK_REASONS: set[str] = {
    r for r in REASON_DESCRIPTIONS.keys() if r.startswith("BLOCKED_")
}
EXIT_REASONS: set[str] = {
    REASON.EXIT_TARGET_HIT, REASON.EXIT_HARD_STOP, REASON.EXIT_DEEP_LOSS_STOP,
    REASON.EXIT_THESIS_FAIL_STOP, REASON.EXIT_NEVER_GREEN_STOP, REASON.EXIT_PROFIT_LOCK,
    REASON.EXIT_TOUCHED_PROFIT, REASON.EXIT_IMMEDIATE_TP, REASON.EXIT_EOD_FORCE_CLOSE,
    REASON.EXIT_SENTINEL_FORCED, REASON.EXIT_THETA_STOP, REASON.EXIT_TIME_STOP,
    REASON.EXIT_UNDERLYING_STOP, REASON.EXIT_SCALE_OUT, REASON.EXIT_RUNNER_TRAIL,
}
BROKER_ERROR_REASONS: set[str] = {
    r for r in REASON_DESCRIPTIONS.keys() if r.startswith("BROKER_")
}
LIFECYCLE_REASONS: set[str] = {
    REASON.LOST_HANDOFF,
    REASON.MISSED_MOVE_ENTRY_CANCEL,
    REASON.STALE_ENTRY_CANCEL,
    REASON.ENTRY_ACK_TIMEOUT_CANCEL,
    REASON.CREATED_NO_BROKER_ID_WATCHDOG,
    REASON.EXPIRED_CONTRACT_LOCAL_CLEANUP,
    REASON.WATCH_ARM_FAILED_DRIFT,
    REASON.WATCH_ARM_FAILED_BELOW_STOP,
    REASON.WATCH_ARM_FAILED_OSM_VALIDATION,
    REASON.WATCH_DEDUP_BLOCK,
    REASON.WATCHER_INVALIDATED,
    REASON.WATCHER_EXPIRED,
}
RECON_REASONS: set[str] = {
    r for r in REASON_DESCRIPTIONS.keys() if r.startswith("RECON_") or r in {
        REASON.DB_UPDATE_MISSED, REASON.DB_ROWCOUNT_UNCONFIRMED,
        REASON.DEDUP_PERSIST_FAILED, REASON.DUPLICATE_EXIT_FILL_IGNORED,
        REASON.FILL_QTY_REGRESSION,
    }
}
FORCE_CLOSE_REASONS: set[str] = {
    REASON.FORCE_CLOSE_ALL_REQUESTED,
    REASON.DAILY_LOSS_LIMIT_BREACHED,
    REASON.DAILY_LOSS_LIMIT_TICK_DETECTED,
    REASON.DAILY_LOSS_LIMIT_RECONCILER_DETECTED,
    REASON.KILL_SWITCH_ACTIVATED,
    REASON.GLOBAL_FLATTEN_REQUESTED,
}


def is_known(code: str) -> bool:
    """Return True if the code is in the registry."""
    return code in REASON_DESCRIPTIONS


def describe(code: str) -> str:
    """Return human-readable description, or the code itself if unknown."""
    return REASON_DESCRIPTIONS.get(code, code)


def category(code: str) -> str:
    """Return category bucket: 'block', 'exit', 'broker', 'lifecycle',
    'recon', 'force_close', or 'unknown'."""
    if code in BLOCK_REASONS:        return "block"
    if code in EXIT_REASONS:         return "exit"
    if code in BROKER_ERROR_REASONS: return "broker"
    if code in LIFECYCLE_REASONS:    return "lifecycle"
    if code in RECON_REASONS:        return "recon"
    if code in FORCE_CLOSE_REASONS:  return "force_close"
    return "unknown"


# Self-check on import: every constant on REASON must have a description.
def _self_check() -> None:
    declared = {
        v for k, v in vars(REASON).items()
        if not k.startswith("_") and isinstance(v, str)
    }
    missing_desc = declared - set(REASON_DESCRIPTIONS.keys())
    if missing_desc:
        # We do NOT raise — this would brick the bot on a typo. We log a warning
        # via stderr so it's visible at startup.
        import sys
        sys.stderr.write(
            f"[ap.reason_codes] WARNING: codes missing description: "
            f"{sorted(missing_desc)}\n"
        )


_self_check()
