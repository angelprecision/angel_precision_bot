#!/usr/bin/env python3
"""
scripts/import_jason_missing_fills.py
======================================
One-shot manual broker fill import for jasoncosby1@gmail.com live account.

Queries Tradier order history for account 6yb82774, finds the sell-to-close
fills for the two missing positions, then writes:
  - positions: exit_price, exit_ts, realized_pnl, realized_pnl_pct,
               quantity_remaining=0, status=CLOSED, close_confidence=HIGH,
               close_source=BROKER_MANUAL_IMPORT
  - proof_trades: full row with all broker audit fields set

Run on Render or locally with env vars set:
  SUPABASE_URL, SUPABASE_KEY, ENCRYPTION_KEY, DATABASE_URL

Usage:
  python3 scripts/import_jason_missing_fills.py [--dry-run]
"""

from __future__ import annotations

import argparse
import os
import sys
import json
import requests
from datetime import datetime, timezone
from pathlib import Path

# ── repo-relative imports ─────────────────────────────────────────────────────
_REPO = Path(__file__).parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from ap.logger import get_logger
from ap.crypto import decrypt_token

log = get_logger("import_jason_fills")

# ── Constants ─────────────────────────────────────────────────────────────────
CLIENT_EMAIL    = "jasoncosby1@gmail.com"
ACCOUNT_ID      = "6yb82774"
TRADIER_LIVE_URL = "https://api.tradier.com"

POSITIONS = [
    {
        "contract":          "C260612C00136000",
        "entry_order_id":    "132254071",
        "entry_price":       1.58,
        "position_id":       "1244402b-f5a7-4f06-b165-1320490e7b92",
        "ticker":            "C",
        "side":              "CALL",
    },
    {
        "contract":          "RIVN260612P00016500",
        "entry_order_id":    "132250699",
        "entry_price":       0.61,
        "position_id":       "95bab29a-a6c1-4548-9e46-1a6f9c2798fd",
        "ticker":            "RIVN",
        "side":              "PUT",
    },
]


# ── Tradier helpers ───────────────────────────────────────────────────────────

def _tradier_get(token: str, path: str, params: dict | None = None) -> dict:
    url = f"{TRADIER_LIVE_URL}{path}"
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        params=params or {},
        timeout=(3.05, 20),
    )
    r.raise_for_status()
    return r.json() if r.content else {}


def fetch_order(token: str, order_id: str) -> dict:
    """Fetch a single order by broker_order_id."""
    j = _tradier_get(token, f"/v1/accounts/{ACCOUNT_ID}/orders/{order_id}")
    return j.get("order") or j


def fetch_all_orders(token: str, date: str = "2026-06-09") -> list[dict]:
    """
    Fetch full order history for the account.
    Tradier returns all orders; we filter by date client-side.
    """
    j = _tradier_get(token, f"/v1/accounts/{ACCOUNT_ID}/orders",
                     params={"includeTags": "true"})
    orders_raw = j.get("orders", {})
    if not orders_raw or orders_raw == "null":
        return []
    orders = orders_raw.get("order", [])
    if isinstance(orders, dict):
        orders = [orders]
    return orders or []


def parse_tradier_ts(ts_str: str | None) -> datetime | None:
    """Parse Tradier ISO timestamp to UTC datetime."""
    if not ts_str:
        return None
    try:
        # Tradier format: "2026-06-09T14:32:11.000Z" or "2026-06-09T14:32:11Z"
        ts_str = ts_str.replace("Z", "+00:00")
        return datetime.fromisoformat(ts_str).astimezone(timezone.utc)
    except Exception:
        return None


def find_exit_fill(orders: list[dict], contract: str, entry_order_id: str) -> dict | None:
    """
    Find the sell-to-close filled order for a given contract symbol.
    Rules:
      - side == 'sell_to_close'
      - symbol matches contract (case-insensitive)
      - status == 'filled'
      - order_id != entry_order_id (not the entry itself)
    Returns the best match (latest create_date if multiple).
    """
    contract_up = contract.upper()
    candidates = []
    for o in orders:
        sym = str(o.get("option_symbol") or o.get("symbol") or "").upper()
        side = str(o.get("side") or "").lower()
        status = str(o.get("status") or "").lower()
        oid = str(o.get("id") or "")
        if (sym == contract_up
                and side == "sell_to_close"
                and status == "filled"
                and oid != entry_order_id):
            candidates.append(o)

    if not candidates:
        return None
    # Take latest by create_date
    candidates.sort(key=lambda o: o.get("create_date") or "", reverse=True)
    return candidates[0]


def extract_fill(order: dict) -> dict:
    """Extract clean fill data from a Tradier order dict."""
    avg_fill = _to_float(order.get("avg_fill_price") or order.get("exec_quantity") and None)
    if avg_fill is None:
        # Try legs
        legs = order.get("leg", [])
        if isinstance(legs, dict):
            legs = [legs]
        for leg in (legs or []):
            avg_fill = _to_float(leg.get("avg_fill_price"))
            if avg_fill:
                break
    exec_qty = int(order.get("exec_quantity") or order.get("quantity") or 0)
    ts_raw   = order.get("transaction_date") or order.get("last_fill_date") or order.get("create_date")
    fill_ts  = parse_tradier_ts(ts_raw)
    return {
        "broker_order_id": str(order.get("id") or ""),
        "avg_fill_price":  avg_fill,
        "exec_quantity":   exec_qty,
        "fill_ts":         fill_ts,
        "fill_ts_iso":     fill_ts.isoformat() if fill_ts else None,
        "raw_status":      order.get("status"),
        "raw_side":        order.get("side"),
    }


def _to_float(x) -> float | None:
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_jason_token() -> str:
    """Load Jason's decrypted Tradier token from the members table."""
    from ap.db import conn, run_with_retry
    def _q():
        with conn() as c:
            c.execute(
                "SELECT broker_token, broker_base_url, broker_account_id "
                "FROM members WHERE email = %s LIMIT 1",
                (CLIENT_EMAIL,),
            )
            row = c.fetchone()
            return dict(row) if row else None
    row = run_with_retry(_q)
    if not row:
        raise RuntimeError(f"Client {CLIENT_EMAIL} not found in members table")
    token = decrypt_token(row["broker_token"])
    log.info("Token decrypted OK | base_url=%s account_id=%s",
             row.get("broker_base_url"), row.get("broker_account_id"))
    return token


def update_position(position_id: str, exit_price: float, exit_ts: datetime,
                    entry_price: float, qty: int, dry_run: bool) -> None:
    """Write exit fields to positions table."""
    pnl_dollars = round((exit_price - entry_price) * qty * 100, 2)
    pnl_pct     = round(((exit_price - entry_price) / entry_price) * 100, 2) if entry_price else 0.0

    log.info("POSITION UPDATE | id=%s exit_price=%.4f exit_ts=%s pnl=$%.2f (%.2f%%) qty=%d",
             position_id, exit_price, exit_ts.isoformat(), pnl_dollars, pnl_pct, qty)

    if dry_run:
        log.info("[DRY-RUN] skipping DB write for position %s", position_id)
        return

    from ap.db import conn, run_with_retry
    def _upd():
        with conn() as c:
            c.execute(
                """
                UPDATE positions
                SET    exit_price         = %s,
                       exit_ts            = %s,
                       realized_pnl       = %s,
                       realized_pnl_pct   = %s,
                       quantity_remaining = 0,
                       status             = 'CLOSED',
                       close_source       = 'BROKER_MANUAL_IMPORT',
                       close_confidence   = 'HIGH',
                       updated_at         = NOW()
                WHERE  id = %s
                  AND  client_id = %s
                RETURNING id, status
                """,
                (exit_price, exit_ts.isoformat(), pnl_dollars, pnl_pct,
                 position_id, CLIENT_EMAIL),
            )
            row = c.fetchone()
            if not row:
                raise RuntimeError(f"Position {position_id} not found / not updated")
            return dict(row)
    result = run_with_retry(_upd)
    log.info("Position updated: %s", result)


def get_entry_fill_ts(entry_order_id: str) -> datetime | None:
    """Pull the entry fill timestamp from the orders table."""
    from ap.db import conn, run_with_retry
    def _q():
        with conn() as c:
            c.execute(
                "SELECT filled_ts, updated_ts FROM orders "
                "WHERE broker_order_id = %s AND client_id = %s LIMIT 1",
                (entry_order_id, CLIENT_EMAIL),
            )
            row = c.fetchone()
            return dict(row) if row else None
    row = run_with_retry(_q)
    if not row:
        return None
    ts = row.get("filled_ts") or row.get("updated_ts")
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return parse_tradier_ts(str(ts)) if ts else None


def upsert_proof_trade(pos: dict, fill: dict, entry_order_id: str,
                       entry_fill_ts: datetime | None, dry_run: bool) -> None:
    """Write/upsert the proof_trade row with all broker audit fields."""
    import supabase as _sb_mod

    sb_url = os.environ["SUPABASE_URL"]
    sb_key = os.environ["SUPABASE_KEY"]
    sb = _sb_mod.create_client(sb_url, sb_key)

    entry_price = pos["entry_price"]
    exit_price  = fill["avg_fill_price"]
    qty         = fill["exec_quantity"] or 1
    pnl_pct     = round(((exit_price - entry_price) / entry_price) * 100, 2) if entry_price else 0.0
    win         = pnl_pct > 0

    now_iso = datetime.now(timezone.utc).isoformat()
    entry_ts_iso = entry_fill_ts.isoformat() if entry_fill_ts else now_iso
    exit_ts_iso  = fill["fill_ts_iso"] or now_iso

    # Derive underlying ticker from OCC symbol if not provided
    ticker = pos.get("ticker") or pos["contract"][:len(pos["contract"])-15] or "UNKNOWN"

    row = {
        # ── identity ──────────────────────────────────────────────────────────
        "client_email":                     CLIENT_EMAIL,
        "mode":                             "live",
        "execution_mode":                   "live",
        "system_version":                   "v2",
        "position_id":                      pos["position_id"],
        # ── trade metadata ────────────────────────────────────────────────────
        "ticker":                           ticker,
        "pattern":                          "BROKER_MANUAL_IMPORT",
        "side":                             pos["side"],
        "timeframe":                        "1d",
        "score":                            0.0,
        "tier":                             "A",
        "context_score":                    0.0,
        "setup_status":                     "broker_manual_import",
        "synthetic_entry":                  False,
        # ── pricing ───────────────────────────────────────────────────────────
        "entry_trigger":                    round(entry_price, 4),
        "entry_option_price":               round(entry_price, 4),
        "exit_option_price":                round(exit_price, 4),
        "underlying_entry":                 0.0,
        "underlying_exit":                  0.0,
        # ── P&L ───────────────────────────────────────────────────────────────
        "contracts":                        qty,
        "option_pnl_pct":                   round(pnl_pct / 100.0, 4),
        "underlying_pnl_pct":               0.0,
        "win":                              win,
        "spread_pct":                       0.0,
        "chain_grade":                      "",
        # ── timestamps ────────────────────────────────────────────────────────
        "opened_at":                        entry_ts_iso,
        "closed_at":                        exit_ts_iso,
        "exit_reason":                      "BROKER_MANUAL_IMPORT | sell_to_close",
        # ── broker audit fields (20260607 migration) ──────────────────────────
        "broker_reconciled":                True,
        "broker_entry_order_id":            entry_order_id,
        "broker_exit_order_id":             fill["broker_order_id"],
        "broker_entry_fill_ts":             entry_ts_iso,
        "broker_exit_fill_ts":              exit_ts_iso,
        "entry_price_source":               "broker_entry_fill",
        "exit_price_source":                "broker_manual_exit_fill",
        "official_live_performance_eligible": True,
    }

    log.info("PROOF_TRADE UPSERT | contract=%s pnl=%.2f%% win=%s broker_exit_order=%s",
             pos["contract"], pnl_pct, win, fill["broker_order_id"])

    if dry_run:
        log.info("[DRY-RUN] proof_trade row:\n%s", json.dumps(row, indent=2, default=str))
        return

    try:
        # Try full row first
        sb.table("proof_trades").upsert(
            row, on_conflict="position_id"
        ).execute()
        log.info("proof_trade upserted OK | position_id=%s", pos["position_id"])
    except Exception as e:
        log.warning("proof_trade full upsert failed (%s) — trying core-only", e)
        # Core fallback without broker audit cols
        core = {k: row[k] for k in (
            "client_email", "mode", "execution_mode", "system_version",
            "position_id", "ticker", "pattern", "side", "timeframe", "score",
            "tier", "entry_option_price", "exit_option_price", "contracts",
            "exit_reason", "option_pnl_pct", "underlying_pnl_pct", "win",
            "synthetic_entry", "opened_at", "closed_at",
        )}
        sb.table("proof_trades").upsert(core, on_conflict="position_id").execute()
        log.warning("proof_trade core-only upsert OK — broker audit cols may be missing")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Import Jason missing exit fills from Tradier")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be written without touching DB")
    args = parser.parse_args()

    if args.dry_run:
        log.info("=== DRY-RUN MODE — no DB writes ===")

    # ── 1. Get Jason's Tradier token ──────────────────────────────────────────
    log.info("Fetching Jason's decrypted Tradier token from members table...")
    token = get_jason_token()

    # ── 2. Fetch entry order details (confirms fills) ─────────────────────────
    log.info("Fetching entry orders from Tradier to confirm fills...")
    for pos in POSITIONS:
        entry_order = fetch_order(token, pos["entry_order_id"])
        log.info("Entry order %s | symbol=%s status=%s avg_fill=%.4f exec_qty=%s",
                 pos["entry_order_id"],
                 entry_order.get("option_symbol") or entry_order.get("symbol"),
                 entry_order.get("status"),
                 float(entry_order.get("avg_fill_price") or 0),
                 entry_order.get("exec_quantity"))

    # ── 3. Fetch all account orders for 2026-06-09 ───────────────────────────
    log.info("Fetching full order history for account %s...", ACCOUNT_ID)
    all_orders = fetch_all_orders(token)
    log.info("Total orders returned: %d", len(all_orders))

    # ── 4. Find exit fills for each position ─────────────────────────────────
    results = []
    for pos in POSITIONS:
        log.info("--- Looking for exit fill: %s ---", pos["contract"])

        exit_order = find_exit_fill(all_orders, pos["contract"], pos["entry_order_id"])

        if not exit_order:
            log.error("NO EXIT FILL FOUND for %s — checking entry order directly",
                      pos["contract"])
            # Check entry order for partial/full exit embedded
            entry_order = fetch_order(token, pos["entry_order_id"])
            log.info("Entry order detail: %s", json.dumps(entry_order, default=str))
            log.error("Cannot proceed for %s without confirmed exit fill. "
                      "Check Tradier account manually.", pos["contract"])
            continue

        fill = extract_fill(exit_order)
        log.info("EXIT FILL FOUND | contract=%s broker_order_id=%s fill_price=%.4f qty=%d ts=%s",
                 pos["contract"], fill["broker_order_id"], fill["avg_fill_price"] or 0,
                 fill["exec_quantity"], fill["fill_ts_iso"])

        if not fill["avg_fill_price"]:
            log.error("Fill price is zero/None for %s — cannot compute P&L. Skipping.",
                      pos["contract"])
            continue

        results.append((pos, fill))

    if not results:
        log.error("No exit fills found. Cannot import. Exiting.")
        sys.exit(1)

    # ── 5. Write to DB ────────────────────────────────────────────────────────
    for pos, fill in results:
        log.info("=== Importing %s ===", pos["contract"])

        entry_fill_ts = get_entry_fill_ts(pos["entry_order_id"])
        log.info("Entry fill_ts from DB: %s", entry_fill_ts)

        # Update positions table
        update_position(
            position_id = pos["position_id"],
            exit_price  = fill["avg_fill_price"],
            exit_ts     = fill["fill_ts"] or datetime.now(timezone.utc),
            entry_price = pos["entry_price"],
            qty         = fill["exec_quantity"] or 1,
            dry_run     = args.dry_run,
        )

        # Upsert proof_trades
        upsert_proof_trade(
            pos            = pos,
            fill           = fill,
            entry_order_id = pos["entry_order_id"],
            entry_fill_ts  = entry_fill_ts,
            dry_run        = args.dry_run,
        )

    log.info("=== Import complete. %d/%d positions processed ===",
             len(results), len(POSITIONS))

    # ── 6. Verification query ─────────────────────────────────────────────────
    if not args.dry_run:
        from ap.db import conn, run_with_retry
        def _verify():
            with conn() as c:
                ids = [pos["position_id"] for pos, _ in results]
                c.execute(
                    "SELECT id, status, exit_price, realized_pnl, "
                    "quantity_remaining, close_source "
                    "FROM positions WHERE id = ANY(%s)",
                    (ids,),
                )
                return [dict(r) for r in c.fetchall()]
        rows = run_with_retry(_verify)
        log.info("=== Verification ===")
        for row in rows:
            log.info("  %s", json.dumps(row, default=str))


if __name__ == "__main__":
    main()
