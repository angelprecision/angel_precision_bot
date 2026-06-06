"""
ap/client_preflight.py
======================
PR2 — Client State Preflight
PR81 FINAL AMENDMENT §5 + §6 — preflight reads the AUTHORITATIVE schema.

Captures the exact reason every client passed or failed before order creation.
Called from _dispatch in ap/queue.py after master_control.evaluate() approves
a signal and before order_state_machine.create_entry_order().

Source of truth: ap/entry_gate.py (the existing per-client gate). The
preflight reads the SAME columns entry_gate reads, so audit results cannot
diverge from the live execution gate.

Real members table columns used here (lowercase, no underscores — matching
ap/entry_gate.py):
  killswitch, killswitchreason, entriespaused, maintenancemode,
  scannerroutingenabled, approved, subscriptionactive,
  tradier_account_mode, tradier_paper_account_id, tradier_paper_access_token,
  tradier_account_id, tradier_access_token,
  tradier_live_account_id, tradier_live_access_token,
  execution_pod, allow_live_trading.

Unknown values are recorded EXPLICITLY as 'unavailable' strings in the
to_dict() snapshot, never as a fabricated zero/false.

Does NOT change routing logic. Only reads state and returns a verdict.
All reads are fail-safe — never blocks execution.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Optional

log = logging.getLogger("ap.client_preflight")

# ── Miss reason codes ─────────────────────────────────────────────────────────
KILL_SWITCH_ON                 = "kill_switch_on"
ENTRIES_PAUSED                 = "entries_paused"
MAINTENANCE_MODE               = "maintenance_mode"
SCANNER_DISABLED               = "scanner_disabled"
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

# Sentinel — Amendment §5: unknown states must be explicit, never a
# fabricated zero. -1 internally; rendered as "<field>_unavailable" externally.
UNKNOWN: int = -1


@dataclass
class ClientTradePreflight:
    client_id:               str
    client_active:           bool
    approved:                bool
    subscription_active:     bool
    kill_switch:             bool
    entries_paused:          bool
    maintenance_mode:        bool
    scanner_routing_enabled: bool
    tradier_active_mode:     str
    expected_mode:           str
    has_account_id:          bool
    has_access_token:        bool
    broker_credentials_present: bool
    buying_power:            float            # 0.0 means UNKNOWN (rendered as "_unavailable")
    estimated_cost:          float
    max_trade_cost:          float
    # Cap counters — UNKNOWN (-1) means "data not available", not zero.
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

    @staticmethod
    def _render_count(value: int, label: str) -> Any:
        """UNKNOWN → '<label>_unavailable', else the integer."""
        return f"{label}_unavailable" if value == UNKNOWN else value

    def to_dict(self, *, preflight_enforced: bool = False,
                execution_continued: bool = True) -> dict:
        """Return a serialisable snapshot of the preflight result.

        Amendment §5+§6: unknown/unavailable states are recorded as
        descriptive strings rather than fabricated zeros that could be
        mistaken for confirmed truth. Includes preflight_enforced and
        execution_continued so the ledger row carries the full truth
        about what happened.
        """
        return {
            "client_id":                self.client_id,
            "client_active":            self.client_active,
            "approved":                 self.approved,
            "subscription_active":      self.subscription_active,
            "kill_switch":              self.kill_switch,
            "entries_paused":           self.entries_paused,
            "maintenance_mode":         self.maintenance_mode,
            "scanner_routing_enabled":  self.scanner_routing_enabled,
            "tradier_active_mode":      self.tradier_active_mode or "unavailable",
            "expected_mode":            self.expected_mode or "unavailable",
            "has_account_id":           self.has_account_id,
            "has_access_token":         self.has_access_token,
            "broker_credentials_present": self.broker_credentials_present,
            "buying_power":             self.buying_power
                                        if self.buying_power > 0 else "buying_power_unavailable",
            "estimated_cost":           self.estimated_cost,
            "max_trade_cost":           self.max_trade_cost,
            "daily_trade_count":        self._render_count(self.daily_trade_count,   "daily_trade_count"),
            "daily_lane_count":         self._render_count(self.daily_lane_count,    "daily_lane_count"),
            "intraday_lane_count":      self._render_count(self.intraday_lane_count, "intraday_lane_count"),
            "same_symbol_count":        self._render_count(self.same_symbol_count,   "same_symbol_count"),
            "open_positions_count":     self._render_count(self.open_positions_count, "open_positions_count"),
            "pending_entries_count":    self._render_count(self.pending_entries_count, "pending_entries_count"),
            "max_daily_trades":         self.max_daily_trades,
            "max_lane_trades":          self.max_lane_trades,
            "max_intraday_trades":      self.max_intraday_trades,
            "max_same_symbol_trades":   self.max_same_symbol_trades,
            "max_open_positions":       self.max_open_positions,
            "max_pending_entries":      self.max_pending_entries,
            "eligible":                 self.eligible,
            "block_reason":             self.block_reason,
            "snapshot_ts":              self.snapshot_ts,
            # Amendment §2+§6: enforcement context
            "preflight_enforced":       preflight_enforced,
            "execution_continued":      execution_continued,
        }


def _ef(key: str, default: int) -> int:
    try: return int(os.getenv(key, str(default)))
    except: return default


def preflight_enforce() -> bool:
    """
    CLIENT_PREFLIGHT_ENFORCE=false (default) — run preflight, write audit,
    but DO NOT block execution. Decision is made in queue.py.
    CLIENT_PREFLIGHT_ENFORCE=true — enforce: block on failure before order create.
    """
    return os.getenv("CLIENT_PREFLIGHT_ENFORCE", "false").strip().lower() in ("true", "1")


def _resolve_broker_credentials(member: dict, mode: str) -> tuple[bool, bool]:
    """
    Detect broker credential presence using the SAME priority client_runner.py
    uses (Amendment §5). Returns (has_account_id, has_access_token).

    PAPER priority: tradier_paper_* > generic tradier_*
    LIVE  priority: tradier_live_* only (live must never silently fall back)

    Tokens are Fernet-encrypted in storage. Presence = non-empty string.
    Do NOT attempt to decrypt here; a non-empty value is meaningful even if
    encrypted (the runner handles decryption later).
    """
    m = member or {}
    if (mode or "paper").lower() == "live":
        return (
            bool(m.get("tradier_live_account_id")),
            bool(m.get("tradier_live_access_token")),
        )
    # paper / default
    return (
        bool(m.get("tradier_paper_account_id") or m.get("tradier_account_id")),
        bool(m.get("tradier_paper_access_token") or m.get("tradier_access_token")),
    )


def _resolve_expected_mode(runner, member: dict) -> str:
    """
    Amendment §5: expected_mode must be CLIENT/POD-aware, not forced to a
    single global env var.

    Priority:
      1. runner.expected_mode / runner.mode (live-resolved per-client)
      2. members.tradier_account_mode (the column entry_gate / order paths use)
      3. BOT_MODE env (last-resort global default)
    """
    if runner is not None:
        for attr in ("expected_mode", "mode"):
            try:
                v = getattr(runner, attr, None)
                if v:
                    return str(v).strip().lower()
            except Exception:
                pass
    m = member or {}
    v = (m.get("tradier_account_mode") or "").strip().lower()
    if v:
        return v
    return str(os.getenv("BOT_MODE", "PAPER")).strip().lower()


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

    # ── Defaults ──────────────────────────────────────────────────────────────
    # Booleans default fail-CLOSED (block) so a read failure produces a
    # truthful block reason instead of a silent pass.
    # Counters default UNKNOWN so the snapshot says so explicitly.
    approved              = False
    subscription_active   = False
    kill_switch           = True
    entries_paused        = True
    maintenance_mode      = False
    scanner_routing_enabled = True
    tradier_mode          = ""
    buying_power          = 0.0   # 0.0 means "not read"; rendered as _unavailable
    estimated_cost        = 0.0
    daily_count           = UNKNOWN
    daily_lane_count      = UNKNOWN
    intraday_lane_count   = UNKNOWN
    same_symbol_count     = UNKNOWN
    open_positions_count  = UNKNOWN
    pending_entries_count = UNKNOWN

    member_row: dict = {}

    # ── Read from runner (fastest — in-memory, authoritative for live state) ──
    if runner is not None:
        try:
            kill_switch    = bool(getattr(runner, "kill_switch_active", kill_switch))
        except Exception: pass
        try:
            if hasattr(runner, "entries_allowed"):
                entries_paused = not runner.entries_allowed.is_set()
        except Exception: pass
        try:
            # Amendment §5: do NOT populate buying_power from account_equity.
            # Prefer an explicit buying_power attribute; equity is intentionally
            # ignored here so the snapshot can record "unavailable" honestly.
            _bp = getattr(runner, "buying_power", None)
            if _bp is not None:
                buying_power = float(_bp or 0)
        except Exception: pass
        # Pick up the member row the runner already loaded if possible.
        try:
            _m = getattr(runner, "member", None)
            if isinstance(_m, dict):
                member_row = dict(_m)
        except Exception: pass

    # ── Read from Supabase if runner not available or fields missing ──────────
    _sb = sb
    if _sb is None:
        try:
            from ap.queue import _get_sb_client
            _sb = _get_sb_client()
        except Exception: pass

    if _sb and not member_row:
        try:
            # Amendment §5: use the EXACT columns ap/entry_gate.py reads.
            m_rows = (
                _sb.table("members")
                .select(
                    "email,approved,subscriptionactive,killswitch,killswitchreason,"
                    "entriespaused,maintenancemode,scannerroutingenabled,"
                    "tradier_account_mode,"
                    "tradier_paper_account_id,tradier_paper_access_token,"
                    "tradier_account_id,tradier_access_token,"
                    "tradier_live_account_id,tradier_live_access_token"
                )
                .eq("email", client_id).limit(1).execute().data or []
            )
            if m_rows:
                member_row = m_rows[0] or {}
        except Exception as e:
            log.warning("preflight members read failed for %s: %s", client_id, e)

    # Apply member row over runner defaults.
    if member_row:
        m = member_row
        approved              = bool(m.get("approved", approved))
        subscription_active   = bool(m.get("subscriptionactive", subscription_active))
        kill_switch           = bool(m.get("killswitch", kill_switch))
        entries_paused        = bool(m.get("entriespaused", entries_paused))
        maintenance_mode      = bool(m.get("maintenancemode", False))
        scanner_routing_enabled = (m.get("scannerroutingenabled") is not False)
        tradier_mode          = (str(m.get("tradier_account_mode") or "").strip().lower()
                                  or tradier_mode)

    # ── Credentials (Amendment §5: same priority as client_runner.py) ─────────
    has_account_id, has_access_token = _resolve_broker_credentials(
        member_row, tradier_mode or "paper"
    )

    # ── Expected mode is client/pod-aware (Amendment §5) ─────────────────────
    expected_mode = _resolve_expected_mode(runner, member_row)
    # If tradier_mode is unknown but we have an explicit expected_mode from
    # runner/member, treat tradier_mode as expected for the comparison check
    # (we'll record both fields honestly in the snapshot).
    tradier_mode_effective = tradier_mode or expected_mode

    # ── Read today's trade counts ────────────────────────────────────────────
    if _sb:
        today = date.today().isoformat()
        # Amendment §5: count ONLY entry rows, not ENTRY+EXIT together.
        try:
            entry_rows = (
                _sb.table("orders")
                .select("kind,status,symbol,timeframe")
                .eq("client_id", client_id)
                .eq("kind", "ENTRY")
                .in_("status", ["FILLED", "BROKER_SUBMITTED", "ACKNOWLEDGED"])
                .gte("created_ts", today)
                .execute().data or []
            )
            daily_count       = len(entry_rows)
            daily_lane_count  = daily_count   # all rows are ENTRY by construction
            # Intraday lane = entries on intraday timeframes (1m..60m).
            _intraday_tfs = {"1m", "5m", "15m", "30m", "60m", "1h"}
            _intraday = [r for r in entry_rows
                         if str(r.get("timeframe", "")).lower() in _intraday_tfs]
            # If no timeframe metadata is present anywhere, leave as UNKNOWN
            # rather than fabricating zero (Amendment §5).
            if any(r.get("timeframe") for r in entry_rows):
                intraday_lane_count = len(_intraday)
            # else: stays UNKNOWN
        except Exception as e:
            log.debug("preflight trade count failed for %s: %s", client_id, e)

        # Same-symbol count
        if ticker:
            try:
                sym_rows = (
                    _sb.table("orders")
                    .select("id")
                    .eq("client_id", client_id)
                    .eq("symbol", ticker)
                    .eq("kind", "ENTRY")
                    .in_("status", ["FILLED", "BROKER_SUBMITTED", "ACKNOWLEDGED", "PENDING_TRIGGER"])
                    .gte("created_ts", today)
                    .execute().data or []
                )
                same_symbol_count = len(sym_rows)
            except Exception as e:
                log.debug("preflight symbol count failed for %s: %s", client_id, e)

        # Open positions
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

        # Pending entries
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

    broker_credentials_present = has_account_id and has_access_token

    # ── Evaluate eligibility ─────────────────────────────────────────────────
    # When CLIENT_PREFLIGHT_ENFORCE=false (default), all blocks are captured
    # for audit but do NOT gate execution — that decision is made in queue.py.
    # When enforce=true, the caller (queue.py) respects eligible=False.
    #
    # Hard blocks (definitively known) always reported accurately.
    # Soft blocks (data may be unknown) only block when data is confirmed.
    _enforce  = preflight_enforce()
    eligible  = True
    block_reason = None

    # Hard blocks
    if not approved:
        eligible = False; block_reason = CLIENT_NOT_APPROVED
    elif not subscription_active:
        eligible = False; block_reason = SUBSCRIPTION_INACTIVE
    elif kill_switch:
        eligible = False; block_reason = KILL_SWITCH_ON
    elif entries_paused:
        eligible = False; block_reason = ENTRIES_PAUSED
    elif maintenance_mode:
        eligible = False; block_reason = MAINTENANCE_MODE
    elif not scanner_routing_enabled:
        eligible = False; block_reason = SCANNER_DISABLED
    elif not broker_credentials_present:
        eligible = False; block_reason = MISSING_BROKER_CREDENTIALS

    # Mode mismatch — only when BOTH sides are confirmed known.
    elif (tradier_mode and expected_mode
          and tradier_mode != expected_mode):
        eligible = False; block_reason = BROKER_MODE_MISMATCH

    # Soft caps — UNKNOWN counters never trigger a block.
    elif buying_power > 0 and estimated_cost > 0 and estimated_cost > buying_power:
        eligible = False; block_reason = INSUFFICIENT_BUYING_POWER
    elif estimated_cost > 0 and max_trade_cost > 0 and estimated_cost > max_trade_cost:
        eligible = False; block_reason = ESTIMATED_COST_EXCEEDS_LIMIT
    elif daily_count != UNKNOWN and daily_count >= max_daily_trades:
        eligible = False; block_reason = DAILY_CAP_REACHED
    elif daily_lane_count != UNKNOWN and daily_lane_count >= max_lane_trades:
        eligible = False; block_reason = LANE_CAP_REACHED
    elif intraday_lane_count != UNKNOWN and intraday_lane_count >= max_intraday_trades:
        eligible = False; block_reason = INTRADAY_CAP_REACHED
    elif same_symbol_count != UNKNOWN and same_symbol_count >= max_same_symbol:
        eligible = False; block_reason = SAME_SYMBOL_CAP_REACHED
    elif open_positions_count != UNKNOWN and open_positions_count >= max_open_positions:
        eligible = False; block_reason = MAX_OPEN_POSITIONS_REACHED
    elif pending_entries_count != UNKNOWN and pending_entries_count >= max_pending_entries:
        eligible = False; block_reason = MAX_PENDING_ENTRIES_REACHED

    return ClientTradePreflight(
        client_id               = client_id,
        client_active           = approved,
        approved                = approved,
        subscription_active     = subscription_active,
        kill_switch             = kill_switch,
        entries_paused          = entries_paused,
        maintenance_mode        = maintenance_mode,
        scanner_routing_enabled = scanner_routing_enabled,
        tradier_active_mode     = tradier_mode_effective,
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
