# ap/execution.py - PRODUCTION (SIMPLE + SAFE)
# =====================================================================
# Creates ORDERS only. fill_monitor creates POSITIONS after true fills.
#
# Core Safety:
# - symbol lock (prevents duplicate symbol trades)
# - reserve exact cost (prevents over-allocation bursts)
# - daily loss stop (only stops on losses)
# - hard per-trade $ cap
# - pre-submit kill-switch recheck
#
# Signal Quality Gates (added):
# - Trend alignment: CALL blocked in BEAR, PUT blocked in BULL
# - Time gate: individual stocks blocked before 10:00 AM ET
#   (indices SPY/QQQ/IWM execute immediately — quick TP handled in exit)
#
# Debuggability:
# - minimal branching
# - consistent audit events
# - order row stores: direction + reserved_cost
#
# FIX: MAX_DAILY_LOSS_PCT → DAILY_MAX_LOSS_PCT (matches config.py)
# =====================================================================

from __future__ import annotations

import os
import time
from ap.contract_quote_revalidator import (
    final_quote_check_before_submit as _final_quote_check,
)
from datetime import datetime
from zoneinfo import ZoneInfo

import yfinance as yf

from ap.logger import get_logger
from ap.config import Config
from ap.db import (
    conn,
    run_with_retry,
    get_client,
    get_client_state,
    update_client_state,
    insert_order,
    new_local_order_id,
    update_order,
)
from ap.utils import json_dumps, now_utc_iso
from ap.contract_selection import pick_expiration, resolve_contract_symbol
# TODO: migrate pick_expiration + resolve_contract_symbol to ap/contract_selector.py
# (functions don't exist there yet — kept here for now)
from ap.state import (
    reserve_equity_if_available,
    release_equity,
    acquire_symbol_lock,
    release_symbol_lock,
)

log = get_logger("ap.execution")
cfg = Config()

OPT_MULTIPLIER = 100
NY = ZoneInfo("America/New_York")

# FIX: raised MAX from $2.50 — SPY/QQQ ATM 0DTE options are $5-10/share
# Now reads from config so it's tunable via env var
MIN_PREMIUM_PER_SHARE = float(os.getenv("MIN_PREMIUM_PER_SHARE", "0.50"))
MAX_PREMIUM_PER_SHARE = float(os.getenv("MAX_PREMIUM_PER_SHARE", "10.00"))

MAX_BROKER_RETRIES = 3
BROKER_RETRY_DELAY = 1.0

# ─── PHASE 3: submit-time ask refresh + chase-band guard ─────────────────────
# After contract selection picks the ask (selector_ask) we may sit in the
# admission/insert path for a few hundred ms before the broker.place_order
# call. If the option ran away during that gap we want to KILL the submit
# instead of paying a runaway premium. We re-fetch the ask immediately
# before submit, compute the gap vs the selector_ask, and:
#   - if gap > SUBMIT_CHASE_BAND_PCT: cancel with RUNAWAY_QUOTE_AT_SUBMIT,
#       release equity + symbol lock, emit decision_event, return.
#   - otherwise: use submit_ask as the broker limit (it's the fresher quote)
#       and persist selector_ask, submit_ask, submit_limit, quote_age_ms,
#       entry_attempt=0 in order.meta for the dashboard.
#
# Default chase band matches REPEG_PROXIMITY_PCT (0.08) used downstream by
# the re-peg engine. If the refresh fails (broker quote unavailable) we fall
# through with selector_ask — no chase block — so a quote-feed hiccup does
# not kill all entries.
SUBMIT_CHASE_BAND_PCT      = float(os.getenv("SUBMIT_CHASE_BAND_PCT", "0.08"))
SUBMIT_QUOTE_MAX_AGE_MS    = int(os.getenv("SUBMIT_QUOTE_MAX_AGE_MS", "5000"))

# PR H — quote-refresh fail-safe at first submit.
#
# Previously: when _refresh_ask_at_submit failed (broker quote API hiccup,
# no_quote, invalid_ask, broker_error) the bot fell through to selector_ask
# silently — submitting at a potentially stale price with no audit signal.
#
# New default behavior:
#   QUOTE_REFRESH_FAIL_OPEN=0  (default)  → reject with
#                                          QUOTE_REFRESH_FAILED_AT_SUBMIT,
#                                          release equity + symbol lock,
#                                          retry engine can re-arm later.
#   QUOTE_REFRESH_FAIL_OPEN=1            → legacy fall-through with
#                                          selector_ask (current behavior).
#
# The retry path already gets the same protection because retry calls
# process_signal() which runs through this exact check.
QUOTE_REFRESH_FAIL_OPEN    = os.getenv("QUOTE_REFRESH_FAIL_OPEN", "0").strip() in ("1", "true", "True", "yes")

# ─── QUOTE-DOMAIN AUDIT + PAPER/LIVE EXECUTION SEPARATION ────────────────────
# Problem: Tradier sandbox (paper) appears to judge fills against delayed quotes
# while our selector reads quotes from a LIVE data broker (TRADIER_DATA_BASE_URL
# defaults to https://api.tradier.com). That quote-domain mismatch makes tight
# live-like limits fail to fill in paper even when the contract later goes green.
#
# This block adds:
#   1. PAPER_ENTRY_FILL_MODE — paper-only submission policy (marketable_limit|market)
#   2. Paper cushion knobs — make paper limits more marketable WITHOUT touching live
#   3. A helper to detect/record the quote-domain mismatch per order
#
# HARD INVARIANT: none of these knobs can affect LIVE mode. Market orders are
# impossible in LIVE from this patch. Live always uses the normal marketable
# limit logic (submit_ask within chase band).

# Paper-only fill mode. Ignored entirely when mode is LIVE.
#   marketable_limit (default) — submit a more marketable limit than live:
#                                 submit_ask + paper cushion (capped).
#   market                     — submit a true market order (paper ONLY).
PAPER_ENTRY_FILL_MODE         = os.getenv("PAPER_ENTRY_FILL_MODE", "marketable_limit").strip().lower()

# Paper cushion: how much above submit_ask the paper marketable_limit sits.
# Cushion = min(submit_ask * PAPER_ENTRY_SLIPPAGE_CUSHION_PCT,
#               PAPER_ENTRY_MAX_CUSHION_DOLLARS)
# Both default conservative. LIVE never uses these.
PAPER_ENTRY_SLIPPAGE_CUSHION_PCT = float(os.getenv("PAPER_ENTRY_SLIPPAGE_CUSHION_PCT", "0.10"))  # 10%
PAPER_ENTRY_MAX_CUSHION_DOLLARS  = float(os.getenv("PAPER_ENTRY_MAX_CUSHION_DOLLARS", "0.20"))    # $0.20/share


def _is_sandbox_base_url(base_url) -> bool:
    """True if the base_url points at Tradier's sandbox (delayed) environment."""
    return "sandbox" in str(base_url or "").lower()


def _broker_quote_identity(brk) -> dict:
    """Return (quote_source, quote_base_url, sandbox_mode) for a broker object.

    quote_source is a coarse label: 'tradier_sandbox', 'tradier_live', or
    'unknown'. We read the ACTUAL base_url off the broker/cfg — never assume.
    """
    base_url = (
        getattr(brk, "base_url", None)
        or getattr(getattr(brk, "cfg", None), "base_url", None)
        or ""
    )
    base_url = str(base_url)
    sandbox = _is_sandbox_base_url(base_url)
    if not base_url:
        source = "unknown"
    elif sandbox:
        source = "tradier_sandbox"
    else:
        source = "tradier_live"
    return {
        "quote_source":   source,
        "quote_base_url": base_url,
        "sandbox_mode":   bool(sandbox),
    }


def _compute_paper_marketable_limit(submit_ask: float) -> tuple[float, float]:
    """Paper-only: return (paper_limit, cushion_applied) above submit_ask.

    cushion = min(submit_ask * PAPER_ENTRY_SLIPPAGE_CUSHION_PCT,
                  PAPER_ENTRY_MAX_CUSHION_DOLLARS), floored at $0.01 so we
    always move at least one tick more marketable. Rounded to the penny.
    NEVER called in LIVE mode.
    """
    if submit_ask <= 0:
        return submit_ask, 0.0
    cushion = min(
        submit_ask * PAPER_ENTRY_SLIPPAGE_CUSHION_PCT,
        PAPER_ENTRY_MAX_CUSHION_DOLLARS,
    )
    if cushion < 0.01:
        cushion = 0.01
    paper_limit = round(submit_ask + cushion, 2)
    return paper_limit, round(cushion, 2)


# ─── PHASE 4: account-equity-based sizing ─────────────────────────────────
# Prior sizing was BASE_POSITION_PCT (0.02 = 2%) capped by MAX_POSITION_COST
# = $1000. With a $100K account that gave 1 contract on a $3 premium because
# $1000 / ($3 * 100) = 3.33 → 3 contracts … except the position_pct cap of
# 2% gave only $2000, then MAX_POSITION_COST capped it to $1000. End result
# was perpetual undersizing.
#
# Phase 4 sizes from account equity directly:
#   position_budget = account_equity * POSITION_RISK_PCT      # default 10%
#   qty             = floor(position_budget / (premium * 100))
#
# MAX_TRADE_USD remains as an ABSOLUTE outer safety cap (default $50K so it
# does not bite normal sizing). The legacy MAX_POSITION_COST value is read
# as the floor of MAX_TRADE_USD for back-compat — existing deployments that
# set MAX_POSITION_COST=$1000 will see no behavior change unless they also
# raise MAX_TRADE_USD.
#
# Per-client overrides: client.base_position_pct continues to win when set.
# Operators who want the new 10% sizing for an existing client can either
# (a) set the client column to 0.10 or (b) clear it and rely on the env
# default POSITION_RISK_PCT.
#
# Acceptance examples (premium = $3.08, default POSITION_RISK_PCT = 0.10):
#   $10K  account → budget $1000  → 1000/308   = 3.24  → 3 contracts
#   $30K  account → budget $3000  → 3000/308   = 9.74  → 9 contracts
#   $100K account → budget $10000 → 10000/308  = 32.46 → capped by MAX_CONTRACTS (15)
#
# PR #30 (2026-05-23): MAX_CONTRACTS default is the OPERATIONAL CAP, not
# a force. The qty math is still:
#     qty = floor(position_budget / (submit_ask * 100))
#     qty = min(qty, MAX_CONTRACTS)
# Default lowered from 50 → 15 to match ops policy for proof week. Set the
# env var MAX_CONTRACTS=<int> on Render to override per-deployment.
POSITION_RISK_PCT          = float(os.getenv("POSITION_RISK_PCT", "0.10"))
MAX_TRADE_USD              = float(os.getenv("MAX_TRADE_USD", "50000"))
MAX_CONTRACTS              = int(os.getenv("MAX_CONTRACTS", "15"))

# Indices that bypass both the pre-10AM time gate AND the SPY-trend
# execution gate. These ETFs ARE the broad-market regime, so blocking
# a DIA PUT because "SPY is BULL" is circular. Must stay in sync with
# INDEX_TICKERS in ap_intelligence-3/agents/ap_risk_manager.py.
_INDICES = {"SPY", "QQQ", "IWM", "DIA"}


def audit(client_id: str, level: str, event: str, payload: dict):
    try:
        with conn() as c:
            run_with_retry(lambda: c.execute(
                "INSERT INTO audit_log (ts, level, event, payload, client_id) VALUES (%s,%s,%s,%s,%s)",
                (now_utc_iso(), level, event, json_dumps(payload), client_id),
            ))
    except Exception as e:
        log.error(f"Audit logging failed: {e}")


def _ny_day_key() -> str:
    return datetime.now(NY).strftime("%Y-%m-%d")


# ── SIGNAL QUALITY GATES ──────────────────────────────────────────────────────

def _get_spy_trend_live() -> str:
    try:
        df = yf.download("SPY", period="30d", interval="1d", progress=False, auto_adjust=True)
        if df is None or df.empty or len(df) < 20:
            return "UNKNOWN"
        if hasattr(df.columns, "get_level_values"):
            try:
                df.columns = df.columns.get_level_values(0)
            except Exception:
                pass
        closes = df["Close"].dropna().values.astype(float)
        if len(closes) < 20 or closes[-1] == 0:
            return "UNKNOWN"
        ma20 = float(closes[-20:].mean())
        return "BULL" if closes[-1] > ma20 else "BEAR"
    except Exception as e:
        log.warning(f"SPY trend fetch failed at execution: {e}")
        return "UNKNOWN"


def _trend_allows_execution(symbol: str, direction: str, spy_trend: str) -> tuple[bool, str]:
    if symbol.upper() in _INDICES:
        return True, ""
    if spy_trend == "UNKNOWN":
        return True, ""
    if spy_trend == "BEAR" and direction == "CALL":
        return False, f"trend_blocked:BEAR_market_no_CALL:{symbol}"
    if spy_trend == "BULL" and direction == "PUT":
        return False, f"trend_blocked:BULL_market_no_PUT:{symbol}"
    return True, ""


def _time_gate_allows_execution(symbol: str) -> tuple[bool, str]:
    if symbol.upper() in _INDICES:
        return True, ""

    now_et = datetime.now(NY)
    if now_et.hour > 10 or (now_et.hour == 10 and now_et.minute >= 0):
        return True, ""

    if now_et.hour == 9 and now_et.minute >= 30:
        return False, f"time_gate_blocked:first_30min:{symbol}:{now_et.strftime('%H:%M')}ET"

    return True, ""


# ── EXISTING HELPERS ──────────────────────────────────────────────────────────

def _get_equity_for_client(broker, st: dict, client_cfg: dict) -> float:
    try:
        return float(broker.get_account_equity())
    except Exception as e:
        log.warning(f"broker equity failed: {e}")
        for _, v in [
            ("current_equity", st.get("current_equity")),
            ("starting_equity_today", st.get("starting_equity_today")),
            ("initial_equity", client_cfg.get("initial_equity")),
            ("hard_default", 100000.0),
        ]:
            if v and float(v) > 0:
                return float(v)
        return 100000.0


def _maybe_reset_daily_state(broker, client_id: str, client_cfg: dict, st: dict) -> dict:
    today = _ny_day_key()
    prev = st.get("day_key")
    equity = _get_equity_for_client(broker, st, client_cfg)

    if prev != today:
        patch = {
            "day_key": today,
            "trades_taken_today": 0,
            "realized_pnl_today": 0.0,
            "daily_stop_hit": 0,
            "starting_equity_today": equity,
            "current_equity": equity,
        }
        update_client_state(client_id, patch)
        st.update(patch)
        audit(client_id, "INFO", "DAILY_RESET", {"day_key": today, "starting_equity": equity})
    else:
        update_client_state(client_id, {"current_equity": equity})
        st["current_equity"] = equity

    return st


def _count_open_positions(client_id: str) -> int:
    with conn() as c:
        row = run_with_retry(lambda: c.execute(
            """
            SELECT COUNT(*) AS n
            FROM positions
            WHERE client_id=%s
              AND status IN ('OPEN','CLOSING')
            """,
            (client_id,),
        ).fetchone())
        return int(row["n"] or 0)


# ============================================================
# AUDIT PHASE-2 — slot accounting bug fix.
# Previous behavior: client_state.trades_taken_today is incremented on broker
# ACK and NEVER decremented when the order later cancels, rejects, or expires.
# Result: every canceled order permanently burns a slot for the rest of the day.
# With cap=5 and 5 canceled MSFT/UBER/NFLX/AMZN orders, the bot rejects all
# subsequent signals (including overnight setups) as 'daily_trade_cap'.
#
# Fix: compute the count at READ time from the orders table, counting only
# orders that actually consume capital — active or filled. Canceled / rejected
# / expired orders auto-free their slot. No increment side-effect, no race,
# no decrement bookkeeping. Idempotent by construction.
#
# Statuses considered "slot-consuming":
#   ACK, PARTIAL_FILL, FILLED        (live or already entered)
# Statuses considered "slot-free":
#   CANCELED, REJECTED, EXPIRED, ERROR   (terminal non-fill)
# Special-case: orders with kind='EXIT' do NOT consume an entry slot —
#   they are closing existing positions.
# ============================================================
_SLOT_CONSUMING_STATUSES = (
    "ACK", "ACKNOWLEDGED", "SUBMITTED", "PARTIAL_FILL", "FILLED",
)

def _count_active_entry_orders_today(client_id: str) -> int:
    """Count of ENTRY orders submitted today (UTC) whose status still consumes
    a slot (live or already filled). Canceled/rejected/expired do not count.

    This is the authoritative "trades_today" for the daily cap gate. It
    replaces the broken counter that lived in client_state.trades_taken_today.
    """
    with conn() as c:
        row = run_with_retry(lambda: c.execute(
            """
            SELECT COUNT(*) AS n
            FROM orders
            WHERE client_id = %s
              AND COALESCE(kind, 'ENTRY') = 'ENTRY'
              AND created_ts >= date_trunc('day', NOW() AT TIME ZONE 'UTC')
              AND status IN %s
            """,
            (client_id, _SLOT_CONSUMING_STATUSES),
        ).fetchone())
        return int(row["n"] or 0)


# ============================================================
# AUDIT PHASE-2 — score-based preemption (conservative).
# When the daily cap is full but some slots are held by PRE-SUBMITTED entries
# (status=CREATED or PENDING_TRIGGER, i.e. waiting for a price trigger that
# hasn't hit), a higher-scored incoming signal may preempt the lowest-scored
# pre-submitted entry IF the score delta is meaningful.
#
# Why this is safe:
#   - CREATED / PENDING_TRIGGER orders have NOT been submitted to the broker.
#     Canceling is a local DB transition only — no broker round-trip, no risk
#     of canceling an order that filled in the last millisecond.
#   - We leave SUBMITTED / ACK / PARTIAL_FILL orders alone. Those are live
#     at the broker and racing a cancel against a fill is how you double-pay.
#   - PREEMPT_SCORE_DELTA gate: only preempt if new signal beats the lowest
#     pre-submitted by at least this many points (default 10). Prevents
#     thrashing on near-tied scores.
#   - Gated by PREEMPT_PRE_SUBMITTED_ORDERS env flag. Default OFF for the
#     first rollout day; flip on once observed safe.
# ============================================================
_PREEMPT_PRE_SUBMITTED = (os.getenv("PREEMPT_PRE_SUBMITTED_ORDERS", "0").strip().lower()
                         in ("1", "true", "yes", "on"))
_PREEMPT_SCORE_DELTA = float(os.getenv("PREEMPT_SCORE_DELTA", "10"))

def _find_preemptible_pre_submitted(client_id: str, min_score_to_beat: float) -> dict | None:
    """Return the lowest-scored CREATED/PENDING_TRIGGER entry order for this
    client whose score is at least PREEMPT_SCORE_DELTA below min_score_to_beat.
    Returns the row dict, or None if no candidate qualifies.

    BLOCKER-1 FIX (post-review): regex-guard the meta->>'score' cast so a
    malformed score string ('A+', 'high', '') cannot crash this query.
    Same defensive pattern as the queue claim in ap/queue.py.
    """
    threshold = float(min_score_to_beat) - _PREEMPT_SCORE_DELTA
    with conn() as c:
        row = run_with_retry(lambda: c.execute(
            r"""
            WITH scored AS (
                SELECT id AS local_order_id,
                       broker_order_id,
                       status,
                       created_ts,
                       CASE
                           WHEN meta ? 'score'
                            AND meta->>'score' ~ '^-?[0-9]+(\.[0-9]+)?$'
                           THEN (meta->>'score')::numeric
                           ELSE 65
                       END AS score
                FROM   orders
                WHERE  client_id = %s
                  AND  COALESCE(kind, 'ENTRY') = 'ENTRY'
                  AND  status IN ('CREATED', 'PENDING_TRIGGER')
                  AND  created_ts >= date_trunc('day', NOW() AT TIME ZONE 'UTC')
            )
            SELECT local_order_id, broker_order_id, status, score
            FROM   scored
            WHERE  score <= %s
            ORDER  BY score ASC, created_ts ASC
            LIMIT  1
            """,
            (client_id, threshold),
        ).fetchone())
    return dict(row) if row else None


def _try_preempt_for_higher_score(client_id: str, incoming_score: float) -> dict:
    """Attempt to free one slot for a higher-scored incoming signal by canceling
    the lowest-scored pre-submitted entry. Returns:
      {'ok': True, 'preempted': {...}}  on success
      {'ok': False, 'reason': '...'}    when nothing was preempted
    Caller should re-check the cap gate after success.
    """
    if not _PREEMPT_PRE_SUBMITTED:
        return {"ok": False, "reason": "preemption_disabled"}

    candidate = _find_preemptible_pre_submitted(client_id, incoming_score)
    if not candidate:
        return {"ok": False, "reason": "no_eligible_pre_submitted",
                "incoming_score": incoming_score,
                "delta_required": _PREEMPT_SCORE_DELTA}

    # Cancel via OSM — single source of truth for status transitions.
    try:
        from ap.order_state_machine import APOrderStateMachine
        osm = APOrderStateMachine(client_id)
        ok = osm.cancel_pending_entry(
            candidate["local_order_id"],
            reason=f"preempted_by_score score={incoming_score} ousted={candidate['score']}",
        )
        if not ok:
            return {"ok": False, "reason": "osm_cancel_refused", "candidate": candidate}
        audit(client_id, "INFO", "SLOT_PREEMPTED", {
            "ousted_order_id":     candidate["local_order_id"],
            "ousted_score":        float(candidate["score"]),
            "incoming_score":      float(incoming_score),
            "score_delta":         float(incoming_score) - float(candidate["score"]),
        })
        return {"ok": True, "preempted": candidate}
    except Exception as e:
        log.warning("[%s] preemption failed: %s", client_id, e)
        return {"ok": False, "reason": "preempt_exception", "error": str(e)}


def _check_daily_loss_stop(client_id: str, st: dict) -> dict:
    starting = float(st.get("starting_equity_today") or 0.0) or 100000.0
    realized = float(st.get("realized_pnl_today") or 0.0)

    loss = -min(0.0, realized)
    loss_pct = loss / starting if starting > 0 else 0.0

    # FIX: was cfg.MAX_DAILY_LOSS_PCT — renamed to DAILY_MAX_LOSS_PCT in config.py
    if loss_pct >= float(cfg.DAILY_MAX_LOSS_PCT):
        update_client_state(client_id, {"kill_switch": 1, "mode": "READ_ONLY", "daily_stop_hit": 1})
        audit(client_id, "CRITICAL", "DAILY_LOSS_STOP_HIT", {
            "loss_pct": loss_pct,
            "loss": loss,
            "realized_pnl_today": realized,
            "starting_equity_today": starting,
            "threshold": float(cfg.DAILY_MAX_LOSS_PCT),  # FIX: was MAX_DAILY_LOSS_PCT
        })
        return {"ok": False, "error": "daily_loss_stop", "loss_pct": loss_pct, "loss": loss}

    return {"ok": True}


def _validate_premium(premium: float, mode: str) -> tuple[bool, str]:
    premium = float(premium)
    if mode in ("PAPER", "SIM") and premium == 1.00:
        return True, ""
    if premium < MIN_PREMIUM_PER_SHARE:
        return False, f"premium_too_low:{premium:.2f}"
    if premium > MAX_PREMIUM_PER_SHARE:
        return False, f"premium_too_high:{premium:.2f}"
    return True, ""


def _calc_qty(max_cost: float, premium: float) -> int:
    cost_per_contract = float(premium) * OPT_MULTIPLIER
    if cost_per_contract <= 0:
        return 0
    return int(float(max_cost) // cost_per_contract)


def _size_position(account_equity: float, premium: float, client_cfg: dict | None = None
                   ) -> tuple[int, float, str]:
    """PHASE 4: size an entry from account equity.

    Returns (qty, position_budget, sizing_reason_code).

    sizing_reason_code is one of:
      ACCOUNT_EQUITY_PCT       — sized by POSITION_RISK_PCT (or client override)
      MAX_TRADE_USD_CAP        — budget clamped to MAX_TRADE_USD
      MAX_CONTRACTS_CAP        — qty clamped to MAX_CONTRACTS
      INSUFFICIENT_BUDGET      — qty=0, budget < one contract
      INVALID_INPUTS           — qty=0, equity<=0 or premium<=0

    Logic:
      1. risk_pct = client.base_position_pct OR POSITION_RISK_PCT (env, 0.10)
      2. position_budget = account_equity * risk_pct
      3. clamp position_budget to MAX_TRADE_USD (safety cap)
      4. qty = floor(position_budget / (premium * OPT_MULTIPLIER))
      5. clamp qty to MAX_CONTRACTS
      6. min qty is 0 (caller decides what to do with that)

    There is NO LIVE forced-1 here. If equity * risk_pct cannot afford one
    contract, return qty=0. The caller will reject with position_too_small.
    """
    try:
        account_equity = float(account_equity)
        premium        = float(premium)
    except (TypeError, ValueError):
        return 0, 0.0, "INVALID_INPUTS"

    if account_equity <= 0 or premium <= 0:
        return 0, 0.0, "INVALID_INPUTS"

    # client.base_position_pct continues to win when set (back-compat).
    # The client column was historically named base_position_pct = 0.02; it
    # has the same shape as POSITION_RISK_PCT and we treat them identically.
    try:
        risk_pct = float((client_cfg or {}).get("base_position_pct") or 0) or POSITION_RISK_PCT
    except (TypeError, ValueError):
        risk_pct = POSITION_RISK_PCT

    position_budget = account_equity * risk_pct
    reason = "ACCOUNT_EQUITY_PCT"

    if position_budget > MAX_TRADE_USD:
        position_budget = MAX_TRADE_USD
        reason = "MAX_TRADE_USD_CAP"

    cost_per_contract = premium * OPT_MULTIPLIER
    qty = int(position_budget // cost_per_contract)

    if qty <= 0:
        return 0, float(position_budget), "INSUFFICIENT_BUDGET"

    if qty > MAX_CONTRACTS:
        qty = MAX_CONTRACTS
        reason = "MAX_CONTRACTS_CAP"

    return int(qty), float(position_budget), reason


def _resolve_option_contract(broker, client_id: str, symbol: str, strike: float, direction: str, mode: str, exp_hint: str) -> tuple[str, float]:
    direction = (direction or "").upper()
    if direction not in ("CALL", "PUT"):
        raise ValueError(f"invalid_direction:{direction}")

    expirations = broker.get_option_expirations(symbol)
    if not expirations:
        raise ValueError(f"no_expirations:{symbol}")

    expiration = pick_expiration(expirations, hint=exp_hint)
    chain = broker.get_option_chain(symbol, expiration)

    if not chain:
        if mode in ("PAPER", "SIM"):
            contract = f"{symbol}_{expiration}_{int(strike)}_{direction}"
            premium = 1.00
            audit(client_id, "WARNING", "SYNTHETIC_CONTRACT", {
                "symbol": symbol,
                "expiration": expiration,
                "strike": strike,
                "direction": direction,
                "contract": contract,
                "premium": premium,
                "mode": mode,
                "reason": "no_chain_data",
            })
            return contract, premium
        raise ValueError(f"no_chain:{symbol}:{expiration}")

    # FIX: fetch current underlying price so contract_selection picks ATM strike
    # not the entry trigger (breach level) which caused deep OTM selection ($653 for SPY)
    underlying_price = None
    try:
        if hasattr(broker, "get_quote"):
            q = broker.get_quote(symbol)
            if isinstance(q, dict):
                last = q.get("last") or q.get("lastPrice") or q.get("mark")
                if last and float(last) > 0:
                    underlying_price = float(last)
        if underlying_price is None and hasattr(broker, "get_last_price"):
            p = broker.get_last_price(symbol)
            if p and float(p) > 0:
                underlying_price = float(p)
    except Exception as e:
        log.warning(f"[{symbol}] Could not fetch underlying price for ATM selection: {e}")

    contract = resolve_contract_symbol(chain, strike, direction, underlying_price=underlying_price)

    from ap.contract_pricing import get_contract_price
    premium = float(get_contract_price(broker, contract, side="BUY"))

    if premium <= 0:
        if mode in ("PAPER", "SIM"):
            premium = 1.00
        else:
            raise ValueError(f"invalid_premium:{contract}:{premium}")

    return contract, premium


def _refresh_ask_at_submit(broker, contract: str) -> tuple[float, int, bool, str, dict]:
    """PHASE 3 + PR A: re-fetch the ask immediately before broker submit.

    Returns (submit_ask, quote_age_ms, ok, reason, quote_fields):
      - submit_ask:    fresh ask price (0.0 on failure)
      - quote_age_ms:  age of the quote we just fetched (always small on success)
      - ok:            True if we got a usable ask, False if we should fall through
      - reason:        machine-readable reason when ok is False (e.g. 'no_quote',
                       'broker_error', 'invalid_ask'). Empty string when ok=True.
      - quote_fields:  dict with submit_bid, submit_ask, submit_last, submit_mid,
                       spread_pct — forensics for orders.meta. Always returned
                       (possibly with all None) so callers can persist it.

    Failure is non-fatal: callers should fall through with the selector_ask
    (no chase-band guard) so a quote-feed hiccup does not kill all entries.

    PR A scope: only the new `quote_fields` return slot is forensics. The
    submit_ask / ok / reason behavior is unchanged.
    """
    t0 = time.time()
    _empty_qf = {
        "submit_bid":  None,
        "submit_ask":  None,
        "submit_last": None,
        "submit_mid":  None,
        "spread_pct":  None,
    }
    try:
        quote = broker.get_quote(contract) or {}
    except Exception as e:
        log.warning("_refresh_ask_at_submit: broker.get_quote raised contract=%s err=%s", contract, e)
        return 0.0, 0, False, "broker_error", dict(_empty_qf)

    try:
        ask_raw = quote.get("ask")
        ask = float(ask_raw) if ask_raw is not None else 0.0
    except (TypeError, ValueError):
        return 0.0, 0, False, "invalid_ask", dict(_empty_qf)

    # PR A: extract bid/last from the SAME quote we just fetched so the
    # submit-time evidence in orders.meta is internally consistent. Any of
    # bid / last may be None on illiquid contracts — that's not a failure
    # condition, just an evidence gap to record honestly.
    def _safe_float(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None
    bid = _safe_float(quote.get("bid"))
    last = _safe_float(quote.get("last"))
    submit_mid = (
        (bid + ask) / 2.0
        if (bid is not None and bid > 0 and ask > 0)
        else None
    )
    spread_pct = (
        (ask - bid) / submit_mid
        if (submit_mid is not None and submit_mid > 0 and bid is not None)
        else None
    )

    quote_fields = {
        "submit_bid":  bid,
        "submit_ask":  ask if ask > 0 else None,
        "submit_last": last,
        "submit_mid":  submit_mid,
        "spread_pct":  spread_pct,
    }

    if ask <= 0:
        return 0.0, 0, False, "no_quote", quote_fields

    quote_age_ms = int((time.time() - t0) * 1000)
    return ask, quote_age_ms, True, "", quote_fields


def _submit_order_with_retry(broker, symbol: str, contract: str, qty: int, premium: float,
                             order_type: str = "limit") -> tuple[bool, str | None, str | None]:
    last_error = None

    # PAPER market-order support: when order_type == "market" we pass
    # limit_price=None to place_order (TradierBroker treats None as a market
    # order). The CALLER is responsible for guaranteeing this only happens in
    # paper mode — see the hard guard at the call site. This function does not
    # know the mode, so it defends only by requiring an explicit opt-in arg.
    _limit_arg = None if order_type == "market" else premium

    for attempt in range(1, MAX_BROKER_RETRIES + 1):
        try:
            resp = broker.place_order(
                symbol=symbol,
                contract=contract,
                qty=qty,
                limit_price=_limit_arg,
                side="buy_to_open",
            )

            if isinstance(resp, dict):
                broker_order_id = resp.get("broker_order_id") or resp.get("order_id") or resp.get("id")
                status = (resp.get("status") or "UNKNOWN")
                error = resp.get("error")
            else:
                broker_order_id = getattr(resp, "broker_order_id", None) or getattr(resp, "order_id", None)
                status = getattr(resp, "status", "UNKNOWN")
                error = getattr(resp, "error", None)

            if error:
                return False, broker_order_id, str(error)

            if str(status).upper() in ("ACK", "ACKED", "FILLED", "SUBMITTED", "OK", "ACCEPTED", "PENDING", "OPEN"):
                return True, broker_order_id, None

            last_error = f"unexpected_status:{status}"

        except Exception as e:
            last_error = str(e)
            if attempt < MAX_BROKER_RETRIES:
                time.sleep(BROKER_RETRY_DELAY * (2 ** (attempt - 1)))
                continue

    return False, None, last_error


# ── MAIN ENTRY POINT ──────────────────────────────────────────────────────────

def process_signal(broker, client_id: str, signal_payload: dict) -> dict:
    t0 = time.time()

    symbol = None
    locked = False
    reserved = False
    reserved_cost = 0.0

    try:
        client = get_client(client_id)
        if (client.get("status") or "").upper() != "ACTIVE":
            return {"ok": False, "error": "client_inactive"}

        st = get_client_state(client_id)
        mode = (st.get("mode") or "PAPER").upper()
        if st.get("kill_switch"):
            return {"ok": False, "error": "kill_switch_active"}
        if mode == "READ_ONLY":
            return {"ok": False, "error": "read_only_mode"}

        # P1 ENTRY FIX (2026-05-21): systemic LOST_HANDOFF halt.
        # If the order monitor detected 3+ LOST_HANDOFF_30S events within 5 min
        # for this client, it sets client_state.lost_handoff_systemic_halt=True.
        # That indicates a structural handoff failure (watcher loop crashed,
        # OSM submit-path broken). Continuing to arm new entries will produce
        # the same outcome. Block until an operator clears the flag.
        if st.get("lost_handoff_systemic_halt"):
            log.warning(
                "[%s] ENTRY_BLOCKED: lost_handoff_systemic_halt set — operator must clear",
                client_id,
            )
            return {
                "ok": False,
                "error": "lost_handoff_systemic_halt",
                "hint": "3+ LOST_HANDOFF_30S in 5min; investigate watcher/OSM and clear flag in client_state",
            }

        # PR #30 LIVE-SAFETY: broker-error circuit breaker.
        # If broker submit/cancel/status errors have exceeded the threshold
        # in the rolling window, refuse NEW entries for this client. Exits,
        # force-exits, close-all, and reconciliation paths do NOT call
        # process_signal, so they remain unaffected.
        try:
            from ap import safety_circuit as _sc
            if _sc.is_open(client_id):
                snap = _sc.snapshot(client_id)
                log.warning(
                    "[%s] BROKER_ERROR_CIRCUIT_OPEN broker_error_count=%s "
                    "window_secs=%s sample_error=%s — blocking new entries",
                    client_id, snap.get("broker_error_count"),
                    snap.get("window_secs"), snap.get("last_error_sample"),
                )
                audit(client_id, "CRITICAL", "BROKER_ERROR_CIRCUIT_OPEN", snap)
                return {
                    "ok": False,
                    "error": "broker_error_circuit_open",
                    "hint": (
                        "Broker errors exceeded threshold in rolling window; "
                        "investigate broker/network. Exits, force-exits, and "
                        "reconciliation continue."
                    ),
                    **snap,
                }
        except Exception as _sce:
            # Breaker subsystem failure must NEVER block entries by itself.
            # Log and continue.
            log.debug("[%s] safety_circuit check failed (non-fatal): %s", client_id, _sce)

        st = _maybe_reset_daily_state(broker, client_id, client, st)

        loss_check = _check_daily_loss_stop(client_id, st)
        if not loss_check.get("ok"):
            return loss_check

        max_trades = int(client.get("max_trades_per_day") or cfg.MAX_TRADES_PER_DAY)
        # AUDIT PHASE-2: authoritative count is computed from the orders table
        # (slot-consuming statuses only). client_state.trades_taken_today is
        # kept in sync for backward-compat readers but is no longer the gate.
        trades_today = _count_active_entry_orders_today(client_id)
        if trades_today >= max_trades:
            # AUDIT PHASE-2: try to free a slot by preempting a lower-scored
            # PRE-SUBMITTED entry (gated by PREEMPT_PRE_SUBMITTED_ORDERS env).
            incoming_score = float(signal_payload.get("score") or signal_payload.get("ev_score") or 0)
            preempt = _try_preempt_for_higher_score(client_id, incoming_score)
            if preempt.get("ok"):
                # Re-count after preemption — the canceled order is no longer slot-consuming.
                trades_today = _count_active_entry_orders_today(client_id)
                if trades_today >= max_trades:
                    # Race: another thread admitted between cancel and re-count. Bail cleanly.
                    return {"ok": False, "error": "daily_trade_cap_post_preempt",
                            "trades_today": trades_today, "max_trades": max_trades,
                            "preempted": preempt["preempted"]}
                # else: slot is free, fall through to continue admission.
            else:
                return {"ok": False, "error": "daily_trade_cap",
                        "trades_today": trades_today, "max_trades": max_trades,
                        "preempt_attempt": preempt}

        max_open = int(client.get("max_concurrent_positions") or cfg.MAX_CONCURRENT_POSITIONS)
        open_positions = _count_open_positions(client_id)
        if open_positions >= max_open:
            return {"ok": False, "error": "max_open_positions", "open_positions": open_positions, "max_open": max_open}

        symbol = (signal_payload.get("symbol") or "").strip().upper()
        direction = (signal_payload.get("direction") or "").strip().upper()
        trigger = signal_payload.get("trigger") or {}
        strike = trigger.get("strike")

        if not symbol:
            return {"ok": False, "error": "missing_symbol"}
        if direction not in ("CALL", "PUT"):
            return {"ok": False, "error": "invalid_direction", "direction": direction}
        if strike is None:
            return {"ok": False, "error": "missing_strike"}

        strike = float(strike)

        time_ok, time_reason = _time_gate_allows_execution(symbol)
        if not time_ok:
            audit(client_id, "INFO", "TIME_GATE_BLOCKED", {
                "symbol": symbol, "direction": direction, "reason": time_reason,
            })
            log.info(f"⏳ Time gate blocked {symbol} {direction}: {time_reason}")
            return {"ok": False, "error": "time_gate", "details": time_reason}

        spy_trend = _get_spy_trend_live()
        trend_ok, trend_reason = _trend_allows_execution(symbol, direction, spy_trend)
        if not trend_ok:
            audit(client_id, "INFO", "TREND_GATE_BLOCKED", {
                "symbol": symbol, "direction": direction,
                "spy_trend": spy_trend, "reason": trend_reason,
            })
            log.info(f"⛔ Trend gate blocked {symbol} {direction}: {trend_reason} (SPY={spy_trend})")
            return {"ok": False, "error": "trend_gate", "details": trend_reason, "spy_trend": spy_trend}

        if not acquire_symbol_lock(client_id, symbol, ttl_seconds=90):
            audit(client_id, "WARNING", "SYMBOL_LOCKED", {"symbol": symbol})
            return {"ok": False, "error": "symbol_locked", "symbol": symbol}
        locked = True

        # PR #30 LIVE-SAFETY: exposure gate.
        # Block same-symbol stacking and same-sector overload BEFORE we
        # do equity reserve or contract resolution. Pure entry gate;
        # exits and reconciler do NOT pass through here.
        try:
            from ap import exposure_gate as _eg
            exp_check = _eg.check_exposure(client_id, symbol)
        except Exception as _exge:
            log.debug("[%s] exposure_gate failed (non-fatal): %s", client_id, _exge)
            exp_check = {"ok": True, "error": None, "reason_code": None,
                         "symbol": symbol, "sector": None}

        if not exp_check.get("ok"):
            release_symbol_lock(client_id, symbol)
            locked = False
            audit(client_id, "WARNING", exp_check.get("reason_code", "EXPOSURE_LIMIT"),
                  exp_check)
            log.warning(
                "[%s] %s symbol=%s sector=%s open_same_symbol=%s open_same_sector=%s",
                client_id, exp_check.get("reason_code"),
                exp_check.get("symbol"), exp_check.get("sector"),
                exp_check.get("open_same_symbol"),
                exp_check.get("open_same_sector"),
            )
            return {"ok": False, **exp_check}
        elif exp_check.get("sector") is None:
            # Unmapped ticker: same-symbol cap was applied, sector skipped.
            log.info(
                "[%s] sector_unknown symbol=%s — enforcing same-symbol cap only",
                client_id, exp_check.get("symbol"),
            )

        # PHASE 4: account-equity sizing.
        # account_equity is the LIVE broker equity reading (falls back to
        # current_equity / starting_equity_today on broker failure). It is
        # the canonical input for sizing under Phase 4.
        account_equity = _get_equity_for_client(broker, st, client)
        # `equity` retained for back-compat with downstream reserve/audit code.
        equity = account_equity

        exp_hint = (
            trigger.get("expiry_hint")
            or signal_payload.get("exp_hint")
            or signal_payload.get("dte")
            or "DAILY"
        ).strip().upper()
        contract, premium = _resolve_option_contract(
            broker, client_id, symbol, strike, direction, mode=mode, exp_hint=exp_hint
        )

        ok_p, prem_err = _validate_premium(premium, mode)
        if not ok_p:
            release_symbol_lock(client_id, symbol)
            locked = False
            return {"ok": False, "error": "premium_out_of_range", "details": prem_err, "premium": float(premium)}

        # PHASE 4: size from account equity directly. Returns the canonical
        # (qty, position_budget, sizing_reason_code) tuple used downstream by
        # the dashboard. No LIVE forced-1; if qty == 0 we reject cleanly.
        qty, position_budget, sizing_reason_code = _size_position(
            account_equity, premium, client_cfg=client
        )
        if qty < 1:
            release_symbol_lock(client_id, symbol)
            locked = False
            log.warning(
                "[%s] POSITION_TOO_SMALL symbol=%s premium=%.2f equity=%.2f "
                "position_budget=%.2f reason=%s",
                client_id, symbol, premium, account_equity, position_budget,
                sizing_reason_code,
            )
            return {
                "ok": False,
                "error": "position_too_small",
                "premium": float(premium),
                "account_equity": float(account_equity),
                "position_budget": float(position_budget),
                "sizing_reason_code": sizing_reason_code,
            }

        total_cost = float(qty) * float(premium) * OPT_MULTIPLIER

        if not reserve_equity_if_available(client_id, total_cost, equity):
            release_symbol_lock(client_id, symbol)
            locked = False
            audit(client_id, "WARNING", "RESERVE_EQUITY_FAILED", {
                "symbol": symbol, "cost": float(total_cost), "equity": float(equity)
            })
            return {"ok": False, "error": "insufficient_available_equity",
                    "cost": float(total_cost), "equity": float(equity)}

        reserved = True
        reserved_cost = float(total_cost)

        # PHASE 3: capture the ask used during selection.
        selector_ask = float(premium)

        # PHASE 3: re-fetch the ask immediately before broker submit.
        # On success this gives us a fresher quote (submit_ask) and a
        # quote_age_ms = 0 reading. On failure we fall through with
        # selector_ask (no chase block).
        # PR A: _refresh_ask_at_submit now returns a 5th value (quote_fields)
        # carrying submit_bid/submit_last/submit_mid/spread_pct for orders.meta.
        submit_ask, quote_age_ms, refresh_ok, refresh_reason, _submit_quote_fields = \
            _refresh_ask_at_submit(broker, contract)
        if refresh_ok and submit_ask > 0 and selector_ask > 0:
            gap_pct = (submit_ask / selector_ask) - 1.0
        else:
            gap_pct = 0.0

        # PHASE 3: chase-band guard. If the ask ran beyond SUBMIT_CHASE_BAND_PCT
        # while we were in the admission path, abort before paying it. The
        # signal is not invalidated yet (Phase 5 retry can re-arm), so we
        # mark with a distinct reason the dashboard can bucket.
        if refresh_ok and gap_pct > SUBMIT_CHASE_BAND_PCT:
            release_equity(client_id, reserved_cost)
            release_symbol_lock(client_id, symbol)
            reserved = False
            locked = False
            log.warning(
                "[%s] RUNAWAY_QUOTE_AT_SUBMIT symbol=%s contract=%s "
                "selector_ask=%.2f submit_ask=%.2f gap_pct=%.4f band=%.4f",
                client_id, symbol, contract,
                selector_ask, submit_ask, gap_pct, SUBMIT_CHASE_BAND_PCT,
            )
            audit(client_id, "WARNING", "RUNAWAY_QUOTE_AT_SUBMIT", {
                "symbol": symbol, "contract": contract,
                "selector_ask": float(selector_ask),
                "submit_ask": float(submit_ask),
                "gap_pct": float(gap_pct),
                "band": float(SUBMIT_CHASE_BAND_PCT),
            })
            return {
                "ok": False,
                "error": "runaway_quote_at_submit",
                "selector_ask": float(selector_ask),
                "submit_ask": float(submit_ask),
                "gap_pct": float(gap_pct),
            }

        # PR H — quote-refresh fail-safe.
        # If the refresh failed (refresh_ok=False) and fail-open is NOT set,
        # reject the submit with QUOTE_REFRESH_FAILED_AT_SUBMIT rather than
        # silently submitting at the stale selector_ask. Equity and symbol
        # lock are released so retry_engine can re-arm later. The retry path
        # gets the same protection because it routes through this same
        # process_signal() entrypoint.
        if (not refresh_ok) and (not QUOTE_REFRESH_FAIL_OPEN):
            release_equity(client_id, reserved_cost)
            release_symbol_lock(client_id, symbol)
            reserved = False
            locked = False
            log.warning(
                "[%s] QUOTE_REFRESH_FAILED_AT_SUBMIT symbol=%s contract=%s "
                "selector_ask=%.2f reason=%s",
                client_id, symbol, contract,
                selector_ask, refresh_reason or "unknown",
            )
            audit(client_id, "WARNING", "QUOTE_REFRESH_FAILED_AT_SUBMIT", {
                "symbol": symbol,
                "contract": contract,
                "selector_ask": float(selector_ask),
                "refresh_reason": str(refresh_reason or "unknown"),
                "fail_open": bool(QUOTE_REFRESH_FAIL_OPEN),
            })
            return {
                "ok": False,
                "error": "quote_refresh_failed",
                "selector_ask": float(selector_ask),
                "refresh_reason": str(refresh_reason or "unknown"),
            }

        # PHASE 3: use the fresher quote as the submit limit if available;
        # otherwise fall back to the selector ask (back-compat).
        # PR H: this fall-through ONLY fires now when QUOTE_REFRESH_FAIL_OPEN=1.
        submit_limit = float(submit_ask) if (refresh_ok and submit_ask > 0) else float(selector_ask)

        # ── P0B: final direct-quote hard gate before broker submit ────────────
        # Fetch a fresh direct option quote and enforce spread, premium,
        # affordability, and capital limits one final time.  This protects
        # against contracts whose quotes moved adversely between selection
        # and submission.  Non-blocking on quote-fetch error when
        # QUOTE_REFRESH_FAIL_OPEN=1 (same policy as the chase-band guard).
        # Does NOT touch scanner, exits, or any live-capital safety gate.
        _p0b_enabled = os.getenv("FINAL_QUOTE_CHECK_ENABLED", "true").lower() != "false"
        if _p0b_enabled:
            _p0b = _final_quote_check(
                broker,
                contract,
                max_spread_pct=float(os.getenv("MAX_SPREAD_PCT", "0.50")),
                min_premium=float(getattr(client_cfg, "min_premium", None) or
                                  os.getenv("MIN_PREMIUM", "10.0")),
                max_premium=float(getattr(client_cfg, "max_premium", None) or
                                  os.getenv("MAX_PREMIUM", "350.0")),
                budget_usd=float(total_cost),
                is_live=(mode == "LIVE"),
            )
            if not _p0b["ok"]:
                release_equity(client_id, reserved_cost)
                release_symbol_lock(client_id, symbol)
                reserved = False
                locked = False
                _p0b_reason = _p0b.get("reason_code") or "FINAL_CONTRACT_QUOTE_INVALID"
                log.warning(
                    "[%s] P0B_FINAL_QUOTE_GATE_REJECT symbol=%s contract=%s "
                    "reason=%s bid=%s ask=%s spread_pct=%s",
                    client_id, symbol, contract,
                    _p0b_reason,
                    _p0b.get("final_bid"), _p0b.get("final_ask"),
                    ("%.3f" % _p0b["spread_pct"]) if _p0b.get("spread_pct") else "N/A",
                )
                audit(client_id, "WARNING", "P0B_FINAL_QUOTE_GATE_REJECT", {
                    "symbol":      symbol,
                    "contract":    contract,
                    "reason_code": _p0b_reason,
                    "explanation": _p0b.get("explanation"),
                    "final_bid":   _p0b.get("final_bid"),
                    "final_ask":   _p0b.get("final_ask"),
                    "final_mid":   _p0b.get("final_mid"),
                    "spread_pct":  _p0b.get("spread_pct"),
                    "quote_age_ms": _p0b.get("quote_age_ms"),
                })
                return {
                    "ok":          False,
                    "error":       _p0b_reason,
                    "explanation": _p0b.get("explanation"),
                    "final_bid":   _p0b.get("final_bid"),
                    "final_ask":   _p0b.get("final_ask"),
                    "local_order_id": local_order_id,
                }
            else:
                # Use the final-gate ask as submit_limit for precision
                if _p0b.get("final_ask") and _p0b["final_ask"] > 0:
                    _p0b_ask = float(_p0b["final_ask"])
                    if mode == "LIVE":
                        submit_limit = _p0b_ask
                    else:
                        submit_limit = float(_p0b.get("final_mid") or submit_limit)
                    log.info(
                        "[%s] P0B_FINAL_QUOTE_VALID symbol=%s contract=%s "
                        "final_bid=%.4f final_ask=%.4f age_ms=%s",
                        client_id, symbol, contract,
                        float(_p0b.get("final_bid") or 0),
                        _p0b_ask,
                        _p0b.get("quote_age_ms"),
                    )
        # ── end P0B ──────────────────────────────────────────────────────────

        # ── QUOTE-DOMAIN AUDIT + PAPER/LIVE EXECUTION SEPARATION ──────────────
        # Record the actual quote source/base_url for selector (data_broker)
        # and submit (execution broker). These are read off the live broker
        # objects — never assumed.
        _is_paper = (mode == "PAPER")
        _selector_brk = getattr(broker, "data_broker", None) or broker
        _selector_qid = _broker_quote_identity(_selector_brk)
        _submit_qid   = _broker_quote_identity(broker)

        # Quote-domain mismatch (POINT 4: three-state, only true when PROVEN).
        #   True  — proven mismatch: paper, submit=sandbox, selector=live(known).
        #   False — proven no-mismatch: paper, both sources known + same domain,
        #           OR not paper.
        #   None  — cannot prove: a relevant source is 'unknown'. Prefer null
        #           over a misleading false so the dashboard/SQL don't claim
        #           "no mismatch" when we simply couldn't read the base_url.
        _sel_unknown = (_selector_qid["quote_source"] == "unknown")
        _sub_unknown = (_submit_qid["quote_source"] == "unknown")
        if not _is_paper:
            # LIVE: mismatch concept doesn't apply (it's about paper fills).
            _quote_domain_mismatch = False
        elif _sel_unknown or _sub_unknown:
            # Can't read one of the sources — don't assert either way.
            _quote_domain_mismatch = None
        elif _submit_qid["sandbox_mode"] and (not _selector_qid["sandbox_mode"]):
            # Proven: paper fills on sandbox, selector on live.
            _quote_domain_mismatch = True
        else:
            # Both known and same domain (or submit not sandbox): no mismatch.
            _quote_domain_mismatch = False

        # Paper fill-mode policy. LIVE is never affected: the branch below only
        # runs when _is_paper is True. Default paper_submitted_type='limit'.
        paper_submitted_type = "limit"
        paper_fill_mode_applied = "live_n/a" if not _is_paper else PAPER_ENTRY_FILL_MODE
        live_submit_limit = float(submit_limit)  # preserve the live-equivalent limit for evidence
        paper_cushion_applied = 0.0
        paper_market_order = False

        if _is_paper:
            if PAPER_ENTRY_FILL_MODE == "market":
                # Paper market order — allowed ONLY in paper. place_order treats
                # limit_price=None as a market order. We hard-guard below at the
                # broker call that mode is paper before passing None.
                # POINT 3: even in paper, do NOT fire a market order when the
                # quote refresh failed. A market order on a contract whose quote
                # we couldn't even fetch is exactly the blind submit we want to
                # avoid. Fall back to a plain limit at submit_limit (selector_ask
                # when fail-open let us through). If fail-open is OFF, we never
                # reach here — the QUOTE_REFRESH_FAILED return above fired first.
                if refresh_ok and submit_ask > 0:
                    paper_market_order = True
                    paper_submitted_type = "market"
                else:
                    paper_market_order = False
                    paper_submitted_type = "limit"
                    log.warning(
                        "[%s] PAPER market mode requested but quote refresh "
                        "failed (reason=%s) — falling back to limit at %.2f",
                        client_id, refresh_reason or "unknown", float(submit_limit),
                    )
            elif PAPER_ENTRY_FILL_MODE == "marketable_limit":
                # POINT 3: only apply the paper cushion on a fresh quote. If the
                # refresh failed (only reachable with fail-open=1), do NOT pad a
                # stale selector_ask — submit at submit_limit as-is.
                if refresh_ok and submit_ask > 0:
                    _paper_limit, paper_cushion_applied = _compute_paper_marketable_limit(float(submit_ask))
                    if _paper_limit > 0:
                        submit_limit = _paper_limit
                paper_submitted_type = "limit"
            # any other value → treat as plain limit (no change), still paper.

        # HARD INVARIANT enforcement: a market order can NEVER be produced in
        # LIVE from this patch. If somehow paper_market_order is True while not
        # paper, force it off. (Defensive; the branch above already gates on
        # _is_paper.)
        if paper_market_order and not _is_paper:
            paper_market_order = False
            paper_submitted_type = "limit"


        local_order_id = new_local_order_id()
        # AUDIT PHASE-2: persist meta so admission ordering (score) and re-peg
        # alignment gate (signal_entry_price) have what they need at decision time.
        # PHASE 3: persist selector_ask / submit_ask / submit_limit / quote_age_ms
        # / entry_attempt so the dashboard can chart submit-time quote drift.
        _meta = {
            "score":              float(signal_payload.get("score") or signal_payload.get("ev_score") or 0),
            "ticker":             symbol,
            "signal_entry_price": float(
                signal_payload.get("signal_entry_price")
                or (signal_payload.get("trigger") or {}).get("underlying_price")
                or (signal_payload.get("trigger") or {}).get("entry_price")
                or 0
            ),
            "signal_id":          str(signal_payload.get("signal_id") or ""),
            "source":             str(signal_payload.get("source") or ""),
            "repeg_attempts":     0,
            "last_repeg_ts":      0,
            # PHASE 3 + PR A: submit-time telemetry (forensics for orders.meta).
            # ALL fields documented in PR A spec:
            #   selector_bid/ask/mid/last — quote at contract-selection time.
            #     Only selector_ask is currently wired (the price the selector
            #     accepted). selector_bid/mid/last are None until PR F wires
            #     them through ap_options_intelligence.evaluate_contract.
            #   submit_bid/ask/mid/last — quote at broker-submit time (fresh).
            #     All four populated from the same broker.get_quote() call so
            #     they are internally consistent.
            #   submit_limit       — the actual limit price sent to the broker.
            #   quote_age_ms       — broker round-trip for the refresh quote.
            #   spread_pct         — (ask-bid)/mid at submit time.
            #   gap_pct            — (submit_ask/selector_ask) - 1.
            #   submit_refresh_ok  — True if refresh produced a usable ask.
            #   submit_refresh_reason — e.g. 'no_quote' / 'broker_error' / ''.
            "selector_bid":       None,  # PR F: wire from selector return
            "selector_ask":       float(selector_ask),
            "selector_mid":       None,  # PR F: wire from selector return
            "selector_last":      None,  # PR F: wire from selector return
            "submit_bid":         _submit_quote_fields.get("submit_bid"),
            "submit_ask":         (float(submit_ask) if refresh_ok
                                   else _submit_quote_fields.get("submit_ask")),
            "submit_mid":         _submit_quote_fields.get("submit_mid"),
            "submit_last":        _submit_quote_fields.get("submit_last"),
            "submit_limit":       float(submit_limit),
            "quote_age_ms":       int(quote_age_ms),
            "spread_pct":         _submit_quote_fields.get("spread_pct"),
            "gap_pct":             float(gap_pct) if isinstance(gap_pct, (int, float)) else None,
            "submit_refresh_ok":  bool(refresh_ok),
            "submit_refresh_reason": str(refresh_reason or ""),
            "entry_attempt":      0,
            # ── QUOTE-DOMAIN AUDIT (2026-05-xx) ──────────────────────────────
            # Source/base_url evidence so we can prove a paper no-fill was a
            # sandbox quote-domain mismatch vs. a genuine signal/contract fault.
            "selector_quote_source":   _selector_qid["quote_source"],
            "selector_quote_base_url": _selector_qid["quote_base_url"],
            "selector_sandbox_mode":   bool(_selector_qid["sandbox_mode"]),  # item 4: explicit
            "submit_quote_source":     _submit_qid["quote_source"],
            "submit_quote_base_url":   _submit_qid["quote_base_url"],
            "submit_sandbox_mode":     bool(_submit_qid["sandbox_mode"]),    # item 4: explicit
            "broker_base_url":         _submit_qid["quote_base_url"],         # item 4: alias (broker == submit env)
            # Existing alias kept for back-compat with dashboards already on main:
            "tradier_sandbox_mode":    bool(_submit_qid["sandbox_mode"]),
            "mode":                    mode,
            "quote_domain_mismatch_possible": _quote_domain_mismatch,
            # ── PAPER/LIVE EXECUTION SEPARATION ──────────────────────────────
            "paper_fill_mode":         paper_fill_mode_applied,
            "paper_submitted_type":    paper_submitted_type,
            "paper_cushion_applied":   float(paper_cushion_applied),
            "live_submit_limit":       float(live_submit_limit),  # what LIVE would have sent
            # PHASE 4: sizing telemetry
            "account_equity":     float(account_equity),
            "position_budget":    float(position_budget),
            "final_qty":          int(qty),
            "sizing_reason_code": str(sizing_reason_code),
        }
        insert_order(
            client_id=client_id,
            local_order_id=local_order_id,
            position_id=None,
            kind="ENTRY",
            status="NEW",
            symbol=symbol,
            contract=contract,
            direction=direction,
            qty=qty,
            limit_price=float(submit_limit),
            reserved_cost=float(reserved_cost),
            meta=_meta,
        )

        st2 = get_client_state(client_id)
        if st2.get("kill_switch") or (st2.get("mode") or "").upper() == "READ_ONLY":
            update_order(local_order_id, status="CANCELED", last_error="killed_before_submit")
            release_equity(client_id, reserved_cost)
            release_symbol_lock(client_id, symbol)
            return {"ok": False, "error": "killed_before_submit"}

        # PHASE 3: emit explicit entry_attempt=0 token on the original submit
        # for parity with retry_engine's REPEG_APPLIED entry_attempt=N logs.
        log.info(
            "[%s] ENTRY_SUBMIT order=%s entry_attempt=0 contract=%s qty=%d "
            "selector_ask=%.2f submit_ask=%s submit_limit=%.2f quote_age_ms=%d gap_pct=%.4f",
            client_id, local_order_id, contract, int(qty),
            selector_ask,
            (f"{submit_ask:.2f}" if refresh_ok else "NA"),
            submit_limit, int(quote_age_ms), gap_pct,
        )

        # HARD GUARD: market order_type is ONLY ever passed in paper mode.
        # paper_market_order can only be True when _is_paper is True (set above),
        # but we re-assert mode here so a future refactor can't leak a market
        # order into LIVE. In LIVE this is always "limit".
        _order_type = "market" if (paper_market_order and _is_paper and mode == "PAPER") else "limit"
        if _order_type == "market":
            log.warning(
                "[%s] PAPER_MARKET_ORDER order=%s contract=%s qty=%d "
                "(paper-only fill mode; live would have used limit=%.2f)",
                client_id, local_order_id, contract, int(qty), live_submit_limit,
            )
        ok, broker_order_id, err = _submit_order_with_retry(
            broker, symbol, contract, qty, float(submit_limit), order_type=_order_type,
        )
        if not ok:
            update_order(local_order_id, status="REJECTED", broker_order_id=broker_order_id, last_error=err)
            release_equity(client_id, reserved_cost)
            release_symbol_lock(client_id, symbol)
            audit(client_id, "ERROR", "ORDER_REJECTED", {
                "symbol": symbol, "contract": contract,
                "error": err, "local_order_id": local_order_id
            })
            # PR #30 LIVE-SAFETY: feed broker submit failure into the
            # circuit breaker. A burst of these in 120s opens the breaker
            # and blocks the next entry; exits are unaffected.
            try:
                from ap import safety_circuit as _sc
                opened = _sc.record_broker_error(client_id, err, op_kind="submit")
                if opened:
                    audit(client_id, "CRITICAL", "BROKER_ERROR_CIRCUIT_OPEN",
                          _sc.snapshot(client_id))
            except Exception:
                pass
            return {"ok": False, "error": "broker_rejected", "details": err,
                    "local_order_id": local_order_id}

        update_order(local_order_id, status="ACK", broker_order_id=broker_order_id)
        update_client_state(client_id, {
            "trades_taken_today": trades_today + 1,
            "current_equity": float(equity)
        })

        audit(client_id, "INFO", "TRADE_EXECUTED", {
            "symbol": symbol, "direction": direction, "contract": contract,
            "qty": int(qty), "premium": float(premium),
            "reserved_cost": float(reserved_cost),
            "local_order_id": local_order_id,
            "broker_order_id": broker_order_id,
            "spy_trend": spy_trend,
            "ms": int((time.time() - t0) * 1000),
            # PHASE 3: submit-time telemetry for dashboard
            "selector_ask": float(selector_ask),
            "submit_ask": float(submit_ask) if refresh_ok else None,
            "submit_limit": float(submit_limit),
            "quote_age_ms": int(quote_age_ms),
            "entry_attempt": 0,
            # PHASE 4: sizing telemetry
            "account_equity": float(account_equity),
            "position_budget": float(position_budget),
            "final_qty": int(qty),
            "sizing_reason_code": str(sizing_reason_code),
        })

        return {
            "ok": True,
            "symbol": symbol,
            "direction": direction,
            "contract": contract,
            "qty": int(qty),
            "premium": float(premium),  # back-compat: selector_ask value
            "reserved_cost": float(reserved_cost),
            "local_order_id": local_order_id,
            "broker_order_id": broker_order_id,
            "status": "PENDING_FILL",
            "spy_trend": spy_trend,
            # PHASE 3: expose submit-time telemetry to callers
            "selector_ask": float(selector_ask),
            "submit_ask": float(submit_ask) if refresh_ok else None,
            "submit_limit": float(submit_limit),
            "quote_age_ms": int(quote_age_ms),
            "entry_attempt": 0,
            # PHASE 4: sizing telemetry
            "account_equity": float(account_equity),
            "position_budget": float(position_budget),
            "final_qty": int(qty),
            "sizing_reason_code": str(sizing_reason_code),
        }

    except Exception as e:
        log.exception(f"EXECUTION_EXCEPTION: {e}")
        try:
            if reserved:
                release_equity(client_id, reserved_cost)
        except Exception:
            pass
        try:
            if locked and symbol:
                release_symbol_lock(client_id, symbol)
        except Exception:
            pass
        audit(client_id, "ERROR", "EXECUTION_FAILED", {
            "error": str(e), "symbol": symbol, "signal": signal_payload
        })
        return {"ok": False, "error": "execution_exception", "details": str(e)}
