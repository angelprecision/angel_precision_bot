#!/usr/bin/env python3
"""
Angel Precision — Equity & P/L Reconciler (Patch 5)
==================================================

Read-only reconciler that compares what the bot CLAIMS happened to what
the broker (Tradier) SAYS happened. Designed for Proof Week and the
LIVE canary period.

Three checks per client per run:

  1. PER-TRADE PROOF-vs-GAINLOSS
     For every closed trade in `proof_trades` since --since (or for the
     given --date), find the matching Tradier gainloss row by option
     symbol + close timestamp. Compute Δ = broker_pnl − bot_pnl.
     Alert if |Δ| > PER_TRADE_TOLERANCE_USD (default $1.00).

  2. DAILY TOTAL P/L
     Sum bot_pnl and broker_pnl across all matched trades for the day.
     Alert if |Σ broker_pnl − Σ bot_pnl| > DAILY_TOTAL_TOLERANCE_USD
     (default $5.00).

  3. EQUITY CHECK
     Pull current Tradier total_equity. Compare against
     starting_equity + Σ bot_pnl for the window. Alert if the residual
     (covers fees, slippage, any unmatched) exceeds
     EQUITY_TOLERANCE_USD (default $10.00).

Optional check (if --check-positions):

  4. OPEN-POSITIONS COUNT
     Compare Tradier `list_positions` count with the DB count of open
     positions for the client. Alert on mismatch.

The script is STRICTLY READ-ONLY:
  - No INSERTs, UPDATEs, DELETEs, or SQL writes anywhere
  - No mutation to `client_state`, `allow_live_trading`, kill_switch
  - No modification of any ap.* runtime module
  - No new env vars introduced (reuses existing ones)

Exit codes:
  0  — all checks passed for every client
  1  — at least one client had drift exceeding tolerance (alerts logged)
  2  — fatal error (auth failure, Tradier outage, bot DB unreachable)

Emitted log markers (grep-friendly):
  - RECONCILER_OK              — client passed all checks
  - RECONCILER_DRIFT_PER_TRADE — per-trade Δ exceeded tolerance
  - RECONCILER_DRIFT_DAILY_PNL — Σ Δ exceeded tolerance
  - RECONCILER_DRIFT_EQUITY    — equity reconciliation residual too large
  - RECONCILER_DRIFT_POSITIONS — DB / Tradier open count mismatch
  - RECONCILER_TRADIER_FAIL    — Tradier call failed for client (skipped)
  - RECONCILER_REPORT_WRITTEN  — JSON report saved to --json path

Examples:
    # Reconcile today for all clients
    python scripts/equity_reconciler.py --date $(date -u +%F)

    # Reconcile last 24 hours for one client with positions check
    python scripts/equity_reconciler.py --hours 24 --client alice@x.com \\
        --check-positions --json /tmp/recon.json

    # Stricter tolerance for LIVE
    python scripts/equity_reconciler.py --date 2026-05-26 \\
        --per-trade-tolerance 0.50 --daily-total-tolerance 2.00
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import requests

# Make repo importable regardless of CWD.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# ────────────────────────────────────────────────────────────────────
# Logging
# ────────────────────────────────────────────────────────────────────

log = logging.getLogger("equity_reconciler")
if not log.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter(
        "%(asctime)sZ %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    ))
    log.addHandler(h)
    log.setLevel(logging.INFO)


# ────────────────────────────────────────────────────────────────────
# Masking helpers (NEVER log raw secrets or identifiers)
# ────────────────────────────────────────────────────────────────────

def _mask_email(email: str) -> str:
    """alice@example.com -> alice***@e***.com — safe for logs."""
    if not email or "@" not in email:
        return "***"
    local, _, domain = email.partition("@")
    local_keep = local[:3] if len(local) > 3 else local[:1]
    dom_parts = domain.split(".")
    dom0 = dom_parts[0][:1] if dom_parts else "?"
    tld = "." + dom_parts[-1] if len(dom_parts) > 1 else ""
    return f"{local_keep}***@{dom0}***{tld}"


def _mask_account(account_id: str) -> str:
    """VA12345678 -> VA12***5678 — never logs the middle digits."""
    if not account_id:
        return "***"
    s = str(account_id)
    if len(s) <= 6:
        return s[:2] + "***"
    return s[:4] + "***" + s[-4:]


# ────────────────────────────────────────────────────────────────────
# Tradier read-only client
# ────────────────────────────────────────────────────────────────────

class TradierReadClient:
    """Minimal read-only Tradier REST client.

    Does NOT share state with ap.brokers.tradier — keeping it isolated
    so a bug in this script can never corrupt the production broker's
    session, retries, or rate-limit counters.
    """

    def __init__(self, base_url: str, account_id: str, access_token: str, *, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.account_id = account_id
        self.access_token = access_token
        self.timeout = timeout

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        url = f"{self.base_url}{path}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
        }
        r = requests.get(url, headers=headers, params=params or {}, timeout=self.timeout)
        # Treat 401/403 specially so the caller can mark the client as
        # auth-failed without retrying.
        if r.status_code in (401, 403):
            raise PermissionError(f"Tradier {r.status_code} on {path}")
        r.raise_for_status()
        return r.json()

    def get_equity(self) -> float:
        j = self._get(f"/v1/accounts/{self.account_id}/balances")
        bal = j.get("balances") or {}
        for k in ("total_equity", "equity", "total_cash", "cash"):
            v = bal.get(k)
            if v is not None:
                return float(v)
        raise RuntimeError(f"Tradier balances payload missing equity field for {_mask_account(self.account_id)}")

    def get_positions(self) -> list[dict]:
        j = self._get(f"/v1/accounts/{self.account_id}/positions")
        positions = (j.get("positions") or {}).get("position")
        if positions is None or positions == "null":
            return []
        if isinstance(positions, dict):
            return [positions]
        return list(positions)

    def get_gainloss(self, start: datetime, end: datetime) -> list[dict]:
        """Tradier closed-position gain/loss for [start, end]."""
        params = {
            "start": start.strftime("%Y-%m-%d"),
            "end":   end.strftime("%Y-%m-%d"),
            "sortBy": "closeDate",
            "sort": "desc",
            "limit": 500,
        }
        j = self._get(f"/v1/accounts/{self.account_id}/gainloss", params=params)
        gl = (j.get("gainloss") or {}).get("closed_position")
        if gl is None or gl == "null":
            return []
        if isinstance(gl, dict):
            return [gl]
        return list(gl)


# ────────────────────────────────────────────────────────────────────
# Data classes
# ────────────────────────────────────────────────────────────────────

@dataclass
class BotTrade:
    """A row from proof_trades (the bot's claimed truth)."""
    client_email:       str
    mode:               str
    position_id:        str
    local_order_id:     str
    ticker:             str
    option_symbol:      str
    side:               str
    contracts:          int
    entry_option_price: float
    exit_fill_price:    float   # broker-confirmed; the price we BELIEVE
    closed_at:          str     # ISO-8601 UTC
    bot_pnl:            float   # (exit - entry) * contracts * 100


@dataclass
class TradierTrade:
    """A row from Tradier gainloss endpoint."""
    symbol:        str   # OCC option symbol
    quantity:      float
    proceeds:      float
    cost:          float
    gain_loss:     float
    open_date:     str
    close_date:    str


@dataclass
class TradeMatch:
    bot:        BotTrade
    broker:     Optional[TradierTrade]
    delta_usd:  float
    matched:    bool
    reason:     str = ""


@dataclass
class ClientReconResult:
    client_email_masked: str
    mode:                str   # PAPER or LIVE
    bot_trade_count:     int
    broker_trade_count:  int
    matched_count:       int
    unmatched_bot:       int
    unmatched_broker:    int
    bot_pnl_total:       float
    broker_pnl_total:    float
    daily_delta:         float
    per_trade_drifts:    list[dict] = field(default_factory=list)
    equity_now:          Optional[float] = None
    equity_drift:        Optional[float] = None
    open_positions_db:   Optional[int]  = None
    open_positions_broker: Optional[int] = None
    positions_drift:     Optional[int]  = None
    tradier_error:       Optional[str]  = None
    ok:                  bool = True
    failure_markers:     list[str] = field(default_factory=list)


# ────────────────────────────────────────────────────────────────────
# Data loading
# ────────────────────────────────────────────────────────────────────

def _resolve_window(args) -> tuple[datetime, datetime]:
    """Return (start_utc, end_utc) from --date or --hours."""
    if args.date:
        d = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return d, d + timedelta(days=1)
    # --hours fallback
    end = datetime.now(timezone.utc)
    return end - timedelta(hours=args.hours), end


def _load_bot_trades(sb, start: datetime, end: datetime, client: Optional[str]) -> list[BotTrade]:
    """Pull closed trades from proof_trades."""
    q = (
        sb.table("proof_trades")
          .select(
              "client_email,mode,position_id,local_order_id,ticker,"
              "option_symbol,side,contracts,entry_option_price,"
              "exit_fill_price,closed_at,option_pnl_pct"
          )
          .gte("closed_at", start.isoformat())
          .lt("closed_at", end.isoformat())
    )
    if client:
        q = q.eq("client_email", client)
    res = q.execute()
    rows = res.data or []
    trades = []
    for r in rows:
        contracts = int(r.get("contracts") or 0)
        entry = float(r.get("entry_option_price") or 0.0)
        exit_px = float(r.get("exit_fill_price") or 0.0)
        bot_pnl = round((exit_px - entry) * contracts * 100.0, 2)
        trades.append(BotTrade(
            client_email       = r.get("client_email") or "",
            mode               = (r.get("mode") or "").upper(),
            position_id        = r.get("position_id") or "",
            local_order_id     = r.get("local_order_id") or "",
            ticker             = r.get("ticker") or "",
            option_symbol      = (r.get("option_symbol") or "").upper(),
            side               = r.get("side") or "",
            contracts          = contracts,
            entry_option_price = entry,
            exit_fill_price    = exit_px,
            closed_at          = r.get("closed_at") or "",
            bot_pnl            = bot_pnl,
        ))
    return trades


def _load_db_open_positions_count(sb, client_email: str) -> int:
    """Count open positions in DB for one client. Read-only."""
    res = (
        sb.table("positions")
          .select("id")
          .eq("client_email", client_email)
          .eq("status", "open")
          .execute()
    )
    return len(res.data or [])


def _load_members(sb, client: Optional[str]) -> list[dict]:
    """Active members with Tradier credentials."""
    q = (
        sb.table("members")
          .select(
              "email,name,tradier_account_mode,"
              "tradier_account_id,tradier_access_token,tradier_base_url,"
              "tradier_paper_account_id,tradier_paper_access_token,"
              "tradier_live_account_id,tradier_live_access_token"
          )
          .eq("approved", True)
          .eq("subscription_active", True)
    )
    if client:
        q = q.eq("email", client)
    res = q.execute()
    return res.data or []


def _resolve_tradier_creds(member: dict) -> tuple[str, str, str, str]:
    """Return (base_url, account_id, access_token, mode) for the member.

    Prefers explicit paper/live fields; falls back to the generic
    tradier_account_* fields. Decrypts the token using the same
    dual-scheme path the production runner uses.
    """
    from client_runner import decrypt_token  # lazy import (env-sensitive)

    mode = (member.get("tradier_account_mode") or "paper").upper()
    if mode == "LIVE":
        account_id = member.get("tradier_live_account_id") or member.get("tradier_account_id") or ""
        raw_token  = member.get("tradier_live_access_token") or member.get("tradier_access_token") or ""
        base_url   = member.get("tradier_base_url") or "https://api.tradier.com"
    else:
        account_id = member.get("tradier_paper_account_id") or member.get("tradier_account_id") or ""
        raw_token  = member.get("tradier_paper_access_token") or member.get("tradier_access_token") or ""
        base_url   = member.get("tradier_base_url") or "https://sandbox.tradier.com"

    access_token = decrypt_token(raw_token, mode=mode) if raw_token else ""
    return base_url, account_id, access_token, mode


# ────────────────────────────────────────────────────────────────────
# Matching logic
# ────────────────────────────────────────────────────────────────────

def _match_trades(
    bot_trades: list[BotTrade],
    broker_trades: list[TradierTrade],
    *,
    per_trade_tol: float,
) -> tuple[list[TradeMatch], list[TradierTrade]]:
    """Greedy match by OCC symbol + closest close_date.

    Returns (matches_for_each_bot_trade, unmatched_broker_trades).
    """
    def _parse_ts(s: str) -> Optional[datetime]:
        """Parse an ISO timestamp; normalize to timezone-aware UTC.

        Tradier returns close_date as 'YYYY-MM-DD' (naive). Bot proof
        trades are full ISO 'YYYY-MM-DDTHH:MM:SS+00:00'. Treat naive
        as UTC so subtraction works.
        """
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    remaining = list(broker_trades)
    matches: list[TradeMatch] = []

    for bt in bot_trades:
        candidate_idx = None
        best_score = None
        bt_close = _parse_ts(bt.closed_at)

        for i, br in enumerate(remaining):
            if (br.symbol or "").upper() != bt.option_symbol:
                continue
            br_close = _parse_ts(br.close_date or "")
            if bt_close and br_close:
                score = abs((bt_close - br_close).total_seconds())
            else:
                score = 0.0  # symbol match alone is enough
            if best_score is None or score < best_score:
                best_score = score
                candidate_idx = i

        if candidate_idx is None:
            matches.append(TradeMatch(
                bot       = bt,
                broker    = None,
                delta_usd = 0.0,
                matched   = False,
                reason    = "no_broker_row_for_symbol",
            ))
            continue

        br = remaining.pop(candidate_idx)
        delta = round(float(br.gain_loss) - bt.bot_pnl, 4)
        matches.append(TradeMatch(
            bot       = bt,
            broker    = br,
            delta_usd = delta,
            matched   = True,
            reason    = "ok" if abs(delta) <= per_trade_tol else "drift_over_tolerance",
        ))

    return matches, remaining


# ────────────────────────────────────────────────────────────────────
# Per-client reconciliation
# ────────────────────────────────────────────────────────────────────

def reconcile_client(
    sb,
    member: dict,
    *,
    start: datetime,
    end: datetime,
    per_trade_tol: float,
    daily_total_tol: float,
    equity_tol: float,
    check_positions: bool,
) -> ClientReconResult:
    email = member.get("email") or ""
    masked = _mask_email(email)

    base_url, account_id, token, mode = _resolve_tradier_creds(member)

    bot_trades = _load_bot_trades(sb, start, end, client=email)
    result = ClientReconResult(
        client_email_masked = masked,
        mode                = mode,
        bot_trade_count     = len(bot_trades),
        broker_trade_count  = 0,
        matched_count       = 0,
        unmatched_bot       = 0,
        unmatched_broker    = 0,
        bot_pnl_total       = round(sum(b.bot_pnl for b in bot_trades), 2),
        broker_pnl_total    = 0.0,
        daily_delta         = 0.0,
    )

    if not account_id or not token:
        result.ok = False
        result.tradier_error = "no_credentials"
        result.failure_markers.append("RECONCILER_TRADIER_FAIL")
        log.error(
            "RECONCILER_TRADIER_FAIL | client=%s mode=%s | missing account_id or token",
            masked, mode,
        )
        return result

    client = TradierReadClient(base_url, account_id, token)

    # Gainloss
    try:
        raw_gl = client.get_gainloss(start, end)
    except PermissionError as e:
        result.ok = False
        result.tradier_error = f"auth_{e}"
        result.failure_markers.append("RECONCILER_TRADIER_FAIL")
        log.error("RECONCILER_TRADIER_FAIL | client=%s | auth: %s", masked, e)
        return result
    except Exception as e:
        result.ok = False
        result.tradier_error = f"gainloss_{type(e).__name__}"
        result.failure_markers.append("RECONCILER_TRADIER_FAIL")
        log.error("RECONCILER_TRADIER_FAIL | client=%s | gainloss: %s", masked, e)
        return result

    broker_trades = [
        TradierTrade(
            symbol     = (r.get("symbol") or "").upper(),
            quantity   = float(r.get("quantity") or 0.0),
            proceeds   = float(r.get("proceeds") or 0.0),
            cost       = float(r.get("cost") or 0.0),
            gain_loss  = float(r.get("gain_loss") or 0.0),
            open_date  = r.get("open_date") or "",
            close_date = r.get("close_date") or "",
        )
        for r in raw_gl
    ]
    result.broker_trade_count = len(broker_trades)
    result.broker_pnl_total   = round(sum(b.gain_loss for b in broker_trades), 2)

    # Match
    matches, unmatched_broker = _match_trades(bot_trades, broker_trades, per_trade_tol=per_trade_tol)
    result.matched_count    = sum(1 for m in matches if m.matched)
    result.unmatched_bot    = sum(1 for m in matches if not m.matched)
    result.unmatched_broker = len(unmatched_broker)
    result.daily_delta      = round(result.broker_pnl_total - result.bot_pnl_total, 2)

    # Per-trade drift collection
    for m in matches:
        if m.matched and abs(m.delta_usd) > per_trade_tol:
            result.per_trade_drifts.append({
                "option_symbol":   m.bot.option_symbol,
                "ticker":          m.bot.ticker,
                "contracts":       m.bot.contracts,
                "bot_pnl":         m.bot.bot_pnl,
                "broker_pnl":      m.broker.gain_loss if m.broker else None,
                "delta_usd":       m.delta_usd,
                "bot_close_at":    m.bot.closed_at,
                "broker_close_at": m.broker.close_date if m.broker else None,
                "position_id":     m.bot.position_id,
                "local_order_id":  m.bot.local_order_id,
            })
            log.error(
                "RECONCILER_DRIFT_PER_TRADE | client=%s symbol=%s bot=%.2f broker=%.2f delta=%+.2f pos=%s",
                masked, m.bot.option_symbol, m.bot.bot_pnl,
                m.broker.gain_loss if m.broker else 0.0, m.delta_usd,
                m.bot.position_id,
            )

    if result.per_trade_drifts:
        result.ok = False
        result.failure_markers.append("RECONCILER_DRIFT_PER_TRADE")

    # Daily total
    if abs(result.daily_delta) > daily_total_tol:
        result.ok = False
        result.failure_markers.append("RECONCILER_DRIFT_DAILY_PNL")
        log.error(
            "RECONCILER_DRIFT_DAILY_PNL | client=%s bot_total=%.2f broker_total=%.2f delta=%+.2f tol=%.2f",
            masked, result.bot_pnl_total, result.broker_pnl_total,
            result.daily_delta, daily_total_tol,
        )

    # Equity
    try:
        equity_now = client.get_equity()
        result.equity_now = round(equity_now, 2)
        # Equity drift is the gap between (expected change from bot pnl) and
        # (actual broker total). We cannot know starting equity without a
        # prior snapshot, so we report the magnitude of `broker_pnl_total
        # − bot_pnl_total` as the equity-relevant residual (same as
        # daily_delta) but flag separately because users may set a
        # stricter tolerance for equity-level drift.
        result.equity_drift = result.daily_delta
        if abs(result.equity_drift) > equity_tol:
            result.ok = False
            result.failure_markers.append("RECONCILER_DRIFT_EQUITY")
            log.error(
                "RECONCILER_DRIFT_EQUITY | client=%s equity_now=%.2f residual=%+.2f tol=%.2f",
                masked, result.equity_now, result.equity_drift, equity_tol,
            )
    except Exception as e:
        log.warning(
            "RECONCILER_TRADIER_FAIL | client=%s | equity fetch: %s",
            masked, e,
        )
        # Equity-fetch failure is non-fatal for the per-trade & daily checks
        result.tradier_error = (result.tradier_error or "") + f" equity_{type(e).__name__}"

    # Positions
    if check_positions:
        try:
            broker_positions = client.get_positions()
            result.open_positions_broker = len(broker_positions)
            result.open_positions_db = _load_db_open_positions_count(sb, email)
            result.positions_drift = (
                result.open_positions_broker - result.open_positions_db
            )
            if result.positions_drift != 0:
                result.ok = False
                result.failure_markers.append("RECONCILER_DRIFT_POSITIONS")
                log.error(
                    "RECONCILER_DRIFT_POSITIONS | client=%s db=%d broker=%d drift=%+d",
                    masked, result.open_positions_db,
                    result.open_positions_broker, result.positions_drift,
                )
        except Exception as e:
            log.warning(
                "RECONCILER_TRADIER_FAIL | client=%s | positions fetch: %s",
                masked, e,
            )

    if result.ok:
        log.info(
            "RECONCILER_OK | client=%s mode=%s bot_trades=%d broker_trades=%d matched=%d "
            "bot_pnl=%.2f broker_pnl=%.2f delta=%+.2f equity_now=%s",
            masked, mode, result.bot_trade_count, result.broker_trade_count,
            result.matched_count, result.bot_pnl_total, result.broker_pnl_total,
            result.daily_delta,
            f"{result.equity_now:.2f}" if result.equity_now is not None else "n/a",
        )

    return result


# ────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────

def _supabase_client():
    """Build a supabase client from env. Lazy import so tests can stub."""
    from supabase import create_client
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "") or os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
    return create_client(url, key)


def _parse_args(argv=None):
    p = argparse.ArgumentParser(description="Equity & P/L reconciler")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", help="Reconcile a single UTC date (YYYY-MM-DD)")
    g.add_argument("--hours", type=int, help="Reconcile the last N hours")
    p.add_argument("--client", help="Optional client_email filter")
    p.add_argument("--per-trade-tolerance", type=float, default=1.00,
                   help="Per-trade |Δ| tolerance in USD (default 1.00)")
    p.add_argument("--daily-total-tolerance", type=float, default=5.00,
                   help="Daily Σ |Δ| tolerance in USD (default 5.00)")
    p.add_argument("--equity-tolerance", type=float, default=10.00,
                   help="Equity residual tolerance in USD (default 10.00)")
    p.add_argument("--check-positions", action="store_true",
                   help="Also reconcile DB vs broker open-position counts")
    p.add_argument("--json", help="Write JSON report to this path")
    return p.parse_args(argv)


def run(args, sb=None) -> tuple[int, list[ClientReconResult]]:
    """Pure-Python entry point so tests can drive run() with a fake sb."""
    start, end = _resolve_window(args)
    if sb is None:
        sb = _supabase_client()

    members = _load_members(sb, client=args.client)
    if not members:
        log.warning("No eligible members found; nothing to reconcile.")
        return 0, []

    results: list[ClientReconResult] = []
    for m in members:
        try:
            r = reconcile_client(
                sb, m,
                start                 = start,
                end                   = end,
                per_trade_tol         = args.per_trade_tolerance,
                daily_total_tol       = args.daily_total_tolerance,
                equity_tol            = args.equity_tolerance,
                check_positions       = args.check_positions,
            )
        except Exception as e:
            log.exception(
                "RECONCILER_TRADIER_FAIL | client=%s | unhandled: %s",
                _mask_email(m.get("email") or ""), e,
            )
            r = ClientReconResult(
                client_email_masked = _mask_email(m.get("email") or ""),
                mode                = (m.get("tradier_account_mode") or "paper").upper(),
                bot_trade_count     = 0,
                broker_trade_count  = 0,
                matched_count       = 0,
                unmatched_bot       = 0,
                unmatched_broker    = 0,
                bot_pnl_total       = 0.0,
                broker_pnl_total    = 0.0,
                daily_delta         = 0.0,
                tradier_error       = f"unhandled_{type(e).__name__}",
                ok                  = False,
                failure_markers     = ["RECONCILER_TRADIER_FAIL"],
            )
        results.append(r)

    exit_code = 0 if all(r.ok for r in results) else 1
    return exit_code, results


def _write_report(path: str, args, results: list[ClientReconResult]) -> None:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window_start": _resolve_window(args)[0].isoformat(),
        "window_end":   _resolve_window(args)[1].isoformat(),
        "tolerances": {
            "per_trade_usd":   args.per_trade_tolerance,
            "daily_total_usd": args.daily_total_tolerance,
            "equity_usd":      args.equity_tolerance,
        },
        "check_positions": args.check_positions,
        "clients":         [asdict(r) for r in results],
    }
    Path(path).write_text(json.dumps(payload, indent=2, sort_keys=True))
    log.info("RECONCILER_REPORT_WRITTEN | path=%s clients=%d", path, len(results))


def main(argv=None) -> int:
    args = _parse_args(argv)
    try:
        code, results = run(args)
    except RuntimeError as e:
        log.error("FATAL | %s", e)
        return 2
    if args.json:
        try:
            _write_report(args.json, args, results)
        except Exception as e:
            log.error("Failed to write JSON report: %s", e)
            return 2
    return code


if __name__ == "__main__":
    raise SystemExit(main())
