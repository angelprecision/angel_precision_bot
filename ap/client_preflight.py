"""
ap/client_preflight.py
======================
PR2 — Client State Preflight

Captures the exact reason every client passed or failed before order
creation. Called from _dispatch in ap/queue.py after master_control.evaluate()
approves a signal and before order_state_machine.create_entry_order().

Does NOT change routing logic. Only reads state and returns a verdict.
All reads are fail-safe — never blocks execution.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger("ap.client_preflight")

# ── Miss reason codes ─────────────────────────────────────────────────────────
KILL_SWITCH_ON                 = "kill_switch_on"
ENTRIES_PAUSED                 = "entries_paused"
CLIENT_NOT_APPROVED            = "client_not_approved"
SUBSCRIPTION_INACTIVE          = "subscription_inactive"
MISSING_BROKER_CREDENTIALS     = "missing_broker_credentials"
BROKER_MODE_MISMATCH           = "broker_mode_mismatch"
INSUFFICIENT_BUYING_POWER      = "insufficient_buying_power"
DAILY_CAP_REACHED              = "daily_cap_reached"
LANE_CAP_REACHED               = "lane_cap_reached"
INTRADAY_CAP_REACHED           = "intraday_cap_reached"
SAME_SYMBOL_CAP_REACHED        = "same_symbol_cap_reached"
MAX_OPEN_POSITIONS_REACHED     = "max_open_positions_reached"
MAX_PENDING_ENTRIES_REACHED    = "max_pending_entries_reached"
ESTIMATED_COST_EXCEEDS_LIMIT   = "estimated_cost_exceeds_limit"
UNKNOWN_PREFLIGHT_BLOCK        = "unknown_preflight_block"


@dataclass
class ClientTradePreflight:
    client_id:               str
    client_active:           bool
    approved:                bool
    subscription_active:     bool
    kill_switch:             bool
    entries_paused:          bool
    tradier_active_mode:     str
    expected_mode:           str
    has_account_id:          bool
    has_access_token:        bool
    broker_credentials_present: bool
    buying_power:            float
    estimated_cost:          float
    max_trade_cost:          float
    daily_trade_count:       int
    daily_lane_count:        int
    intraday_lane_count:     int
    same_symbol_count:       int
    open_positions_count:    int
    pending_entries_count:   int
    max_daily_trades:        int
    max_lane_trades:         int
    max_intraday_trades:     int
    max_same_symbol_trades:  int
    max_open_positions:      int
    max_pending_entries:     int
    eligible:                bool
    block_reason:            Optional[str]
    snapshot_ts:             str
    metadata:                dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "client_id":                self.client_id,
            "client_active":            self.client_active,
            "approved":                 self.approved,
            "subscription_active":      self.subscription_active,
            "kill_switch":              self.kill_switch,
            "entries_paused":           self.entries_paused,
            "tradier_active_mode":      self.tradier_active_mode,
            "expected_mode":            self.expected_mode,
            "has_account_id":           self.has_account_id,
            "has_access_token":         self.has_access_token,
            "broker_credentials_present": self.broker_credentials_present,
            "buying_power":             self.buying_power,
            "estimated_cost":           self.estimated_cost,
            "max_trade_cost":           self.max_trade_cost,
            "daily_trade_count":        self.daily_trade_count,
            "daily_lane_count":         self.daily_lane_count,
            "intraday_lane_count":      self.intraday_lane_count,
            "same_symbol_count":        self.same_symbol_count,
            "open_positions_count":     self.open_positions_count,
            "pending_entries_count":    self.pending_entries_count,
            "max_daily_trades":         self.max_daily_trades,
            "max_lane_trades":          self.max_lane_trades,
            "max_intraday_trades":      self.max_intraday_trades,
            "max_same_symbol_trades":   self.max_same_symbol_trades,
            "max_open_positions":       self.max_open_positions,
            "max_pending_entries":      self.max_pending_entries,
            "eligible":                 self.eligible,
            "block_reason":             self.block_reason,
            "snapshot_ts":              self.snapshot_ts,
        }


def _ef(key: str, default: int) -> int:
    try: return int(os.getenv(key, str(default)))
    except: return default


def build_client_trade_preflight(
    client_id: str,
    signal: dict,
    plan,
    *,
    runner=None,
    sb=None,
) -> ClientTradePreflight:
    """
    Read all per-client state and return a preflight verdict.

    Args:
        client_id: The client email
        signal:    Signal payload dict
        plan:      APTradePlan (from master_control.evaluate())
        runner:    Optional ClientRunner instance for live state fields
        sb:        Optional Supabase client override
    """
    now = datetime.now(timezone.utc).isoformat()
    ticker = str(signal.get("ticker") or signal.get("symbol") or "")

    # ── Read limits from env ──────────────────────────────────────────────────
    max_daily_trades    = _ef("MAX_CLIENT_TRADES_PER_DAY", 5)
    max_lane_trades     = _ef("MAX_CLIENT_DAILY_TRADES",   3)
    max_intraday_trades = _ef("MAX_CLIENT_INTRADAY_TRADES", 2)
    max_same_symbol     = _ef("MAX_CLIENT_SYMBOL_TRADES_PER_DAY", 1)
    max_open_positions  = _ef("MAX_CLIENT_OPEN_POSITIONS", 10)
    max_pending_entries = _ef("MAX_CLIENT_PENDING_ENTRIES", 3)
    max_trade_cost      = float(os.getenv("MAX_CLIENT_TRADE_COST_USD", "500"))

    # ── Defaults (safe values — fail-closed direction) ────────────────────────
    approved              = False
    subscription_active   = False
    kill_switch           = True   # default: assume blocked
    entries_paused        = True
    tradier_mode          = "paper"
    expected_mode         = "paper"
    has_account_id        = False
    has_access_token      = False
    buying_power          = 0.0
    estimated_cost        = 0.0
    daily_count           = 0
    daily_lane_count      = 0
    intraday_lane_count   = 0
    same_symbol_count     = 0
    open_positions_count  = 0
    pending_entries_count = 0

    # ── Read from runner (fastest — in-memory) ────────────────────────────────
    if runner is not None:
        try:
            kill_switch    = getattr(runner, "kill_switch_on",    kill_switch)
        except Exception: pass
        try:
            entries_paused = not runner.entries_allowed.is_set()
        except Exception: pass
        try:
            buying_power   = float(getattr(runner, "account_equity", 0) or 0)
        except Exception: pass
        try:
            approved       = bool(getattr(runner, "approved", False))
        except Exception: pass
        try:
            subscription_active = bool(getattr(runner, "subscription_active", False))
        except Exception: pass
        try:
            tradier_mode   = str(getattr(runner, "mode", "paper") or "paper").lower()
        except Exception: pass

    # ── Read from Supabase if runner not available or fields missing ──────────
    _sb = sb
    if _sb is None:
        try:
            from ap.queue import _get_sb_client
            _sb = _get_sb_client()
        except Exception: pass

    if _sb:
        try:
            m_rows = (
                _sb.table("members")
                .select("approved,subscription_active,killswitch,entriespaused,"
                        "tradier_active_mode,tradier_account_id,tradier_access_token,"
                        "tradier_live_account_id,tradier_live_access_token")
                .eq("email", client_id).limit(1).execute().data or []
            )
            if m_rows:
                m = m_rows[0]
                approved            = bool(m.get("approved", False))
                subscription_active = bool(m.get("subscription_active", False))
                kill_switch         = bool(m.get("killswitch", True))
                entries_paused      = bool(m.get("entriespaused", False))
                tradier_mode        = str(m.get("tradier_active_mode") or "paper").lower()
                if tradier_mode == "live":
                    has_account_id   = bool(m.get("tradier_live_account_id"))
                    has_access_token = bool(m.get("tradier_live_access_token"))
                else:
                    has_account_id   = bool(m.get("tradier_account_id"))
                    has_access_token = bool(m.get("tradier_access_token"))
        except Exception as e:
            log.warning("preflight members read failed for %s: %s", client_id, e)

        # ── Read today's trade counts ─────────────────────────────────────────
        try:
            from datetime import date
            today = date.today().isoformat()
            count_rows = (
                _sb.table("orders")
                .select("kind,status")
                .eq("client_id", client_id)
                .in_("status", ["FILLED", "EXIT_FILLED", "BROKER_SUBMITTED", "ACKNOWLEDGED"])
                .gte("created_ts", today)
                .execute().data or []
            )
            daily_count          = len(count_rows)
            daily_lane_count     = sum(1 for r in count_rows if r.get("kind") == "ENTRY")
            intraday_lane_count  = 0  # requires timeframe metadata; conservative default
        except Exception as e:
            log.debug("preflight trade count failed for %s: %s", client_id, e)

        # ── Read same-symbol count ────────────────────────────────────────────
        if ticker:
            try:
                sym_rows = (
                    _sb.table("orders")
                    .select("id")
                    .eq("client_id", client_id)
                    .eq("symbol", ticker)
                    .in_("status", ["FILLED", "BROKER_SUBMITTED", "ACKNOWLEDGED", "PENDING_TRIGGER"])
                    .gte("created_ts", date.today().isoformat())
                    .execute().data or []
                )
                same_symbol_count = len(sym_rows)
            except Exception as e:
                log.debug("preflight symbol count failed for %s: %s", client_id, e)

        # ── Read open positions and pending entries ───────────────────────────
        try:
            open_rows = (
                _sb.table("positions")
                .select("id")
                .eq("client_id", client_id)
                .eq("status", "OPEN")
                .execute().data or []
            )
            open_positions_count = len(open_rows)
        except Exception as e:
            log.debug("preflight open positions failed for %s: %s", client_id, e)

        try:
            pending_rows = (
                _sb.table("orders")
                .select("id")
                .eq("client_id", client_id)
                .eq("kind", "ENTRY")
                .in_("status", ["PENDING_TRIGGER", "BROKER_SUBMITTED", "ACKNOWLEDGED"])
                .execute().data or []
            )
            pending_entries_count = len(pending_rows)
        except Exception as e:
            log.debug("preflight pending entries failed for %s: %s", client_id, e)

    # ── Estimate cost ─────────────────────────────────────────────────────────
    try:
        estimated_cost = float(getattr(plan, "max_position_usd", 0) or 0)
    except Exception: pass

    # ── Expected mode from master_control env ─────────────────────────────────
    expected_mode = str(os.getenv("BOT_MODE", "PAPER")).lower()

    broker_credentials_present = has_account_id and has_access_token

    # ── Evaluate eligibility in priority order ────────────────────────────────
    eligible     = True
    block_reason = None

    if not approved:
        eligible = False; block_reason = CLIENT_NOT_APPROVED
    elif not subscription_active:
        eligible = False; block_reason = SUBSCRIPTION_INACTIVE
    elif kill_switch:
        eligible = False; block_reason = KILL_SWITCH_ON
    elif entries_paused:
        eligible = False; block_reason = ENTRIES_PAUSED
    elif not broker_credentials_present:
        eligible = False; block_reason = MISSING_BROKER_CREDENTIALS
    elif tradier_mode != expected_mode and expected_mode in ("live", "paper"):
        eligible = False; block_reason = BROKER_MODE_MISMATCH
    elif buying_power > 0 and estimated_cost > 0 and estimated_cost > buying_power:
        eligible = False; block_reason = INSUFFICIENT_BUYING_POWER
    elif estimated_cost > 0 and max_trade_cost > 0 and estimated_cost > max_trade_cost:
        eligible = False; block_reason = ESTIMATED_COST_EXCEEDS_LIMIT
    elif daily_count >= max_daily_trades:
        eligible = False; block_reason = DAILY_CAP_REACHED
    elif daily_lane_count >= max_lane_trades:
        eligible = False; block_reason = LANE_CAP_REACHED
    elif intraday_lane_count >= max_intraday_trades:
        eligible = False; block_reason = INTRADAY_CAP_REACHED
    elif same_symbol_count >= max_same_symbol:
        eligible = False; block_reason = SAME_SYMBOL_CAP_REACHED
    elif open_positions_count >= max_open_positions:
        eligible = False; block_reason = MAX_OPEN_POSITIONS_REACHED
    elif pending_entries_count >= max_pending_entries:
        eligible = False; block_reason = MAX_PENDING_ENTRIES_REACHED

    return ClientTradePreflight(
        client_id               = client_id,
        client_active           = approved,
        approved                = approved,
        subscription_active     = subscription_active,
        kill_switch             = kill_switch,
        entries_paused          = entries_paused,
        tradier_active_mode     = tradier_mode,
        expected_mode           = expected_mode,
        has_account_id          = has_account_id,
        has_access_token        = has_access_token,
        broker_credentials_present = broker_credentials_present,
        buying_power            = buying_power,
        estimated_cost          = estimated_cost,
        max_trade_cost          = max_trade_cost,
        daily_trade_count       = daily_count,
        daily_lane_count        = daily_lane_count,
        intraday_lane_count     = intraday_lane_count,
        same_symbol_count       = same_symbol_count,
        open_positions_count    = open_positions_count,
        pending_entries_count   = pending_entries_count,
        max_daily_trades        = max_daily_trades,
        max_lane_trades         = max_lane_trades,
        max_intraday_trades     = max_intraday_trades,
        max_same_symbol_trades  = max_same_symbol,
        max_open_positions      = max_open_positions,
        max_pending_entries     = max_pending_entries,
        eligible                = eligible,
        block_reason            = block_reason,
        snapshot_ts             = now,
    )
