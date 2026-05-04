#!/usr/bin/env python3
"""
ap_premarket_invariants.py — Angel Precision Daily Pre-Market Invariant Check
=============================================================================
Runs every morning before market open (scheduled via cron or Render job).
Catches data corruption, split-brain orders, orphaned positions, and OSM
inconsistencies BEFORE the bot starts accepting signals.

If any CRITICAL invariant fails, the bot should enter degraded mode until
the issue is resolved. WARNING invariants are logged but non-blocking.

Exit codes:
  0 = all invariants passed (safe to trade)
  1 = critical invariant failed (do NOT trade until resolved)
  2 = warnings only (trade with caution)

Usage:
  python ap_premarket_invariants.py [--client-id tradefluencehq@gmail.com]
  python ap_premarket_invariants.py --all-clients
"""
import os, sys, json, logging, argparse
from datetime import datetime, timezone, timedelta
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s [invariant] %(levelname)s: %(message)s")
log = logging.getLogger("ap.invariants")

# ── DB connection ─────────────────────────────────────────────────────────────
def _get_conn():
    import psycopg2
    DATABASE_URL = os.getenv("DATABASE_URL", "")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    dsn = DATABASE_URL
    if "sslmode" not in dsn:
        dsn += ("?sslmode=require" if "?" not in dsn else "&sslmode=require")
    return psycopg2.connect(dsn, connect_timeout=10)


def _supabase():
    from supabase import create_client
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        return None
    return create_client(url, key)


# ── Individual invariant checks ───────────────────────────────────────────────

class InvariantResult:
    def __init__(self):
        self.critical: list[str] = []
        self.warnings: list[str] = []
        self.passed: list[str] = []

    def fail(self, msg: str):
        self.critical.append(msg)
        log.error("❌ CRITICAL: %s", msg)

    def warn(self, msg: str):
        self.warnings.append(msg)
        log.warning("⚠️  WARNING: %s", msg)

    def ok(self, msg: str):
        self.passed.append(msg)
        log.info("✅ PASS: %s", msg)

    @property
    def exit_code(self) -> int:
        if self.critical:
            return 1
        if self.warnings:
            return 2
        return 0


def check_submitted_orders_have_broker_id(conn, client_id: str, r: InvariantResult):
    """No SUBMITTED/ACKNOWLEDGED order should exist without a broker_order_id after >5 min."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, local_order_id, status, created_ts, last_error
            FROM orders
            WHERE client_id = %s
              AND status IN ('SUBMITTED', 'ACKNOWLEDGED')
              AND (broker_order_id IS NULL OR TRIM(COALESCE(broker_order_id,'')) = '')
              AND created_ts < NOW() - INTERVAL '5 minutes'
        """, (client_id,))
        rows = cur.fetchall()
    if rows:
        for row in rows:
            r.fail(
                f"Order {row[1]} ({row[2]}) has no broker_order_id after >{5}min "
                f"[created={row[3]}, error={row[4]}] — potential split-brain"
            )
    else:
        r.ok(f"[{client_id}] All SUBMITTED/ACKNOWLEDGED orders have broker_order_id")


def check_filled_entries_have_position_id(conn, client_id: str, r: InvariantResult):
    """Every FILLED entry order must have a position_id."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, local_order_id, created_ts
            FROM orders
            WHERE client_id = %s
              AND order_type = 'ENTRY'
              AND status = 'FILLED'
              AND (position_id IS NULL OR TRIM(COALESCE(position_id,'')) = '')
        """, (client_id,))
        rows = cur.fetchall()
    if rows:
        for row in rows:
            r.fail(
                f"FILLED entry order {row[1]} has no position_id — "
                f"exit engine may not be tracking this position [created={row[2]}]"
            )
    else:
        r.ok(f"[{client_id}] All FILLED entry orders have position_id")


def check_no_exit_submitted_without_broker_id(conn, client_id: str, r: InvariantResult):
    """EXIT_SUBMITTED orders must have a broker_order_id."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, local_order_id, created_ts
            FROM orders
            WHERE client_id = %s
              AND order_type = 'EXIT'
              AND status IN ('SUBMITTED', 'EXIT_SUBMITTED', 'ACKNOWLEDGED')
              AND (broker_order_id IS NULL OR TRIM(COALESCE(broker_order_id,'')) = '')
              AND created_ts < NOW() - INTERVAL '3 minutes'
        """, (client_id,))
        rows = cur.fetchall()
    if rows:
        for row in rows:
            r.fail(
                f"EXIT order {row[1]} submitted without broker_order_id "
                f"[created={row[2]}] — position may be unprotected"
            )
    else:
        r.ok(f"[{client_id}] All exit orders have broker_order_id or are fresh")


def check_watching_queue_not_stale(conn, client_id: str, r: InvariantResult):
    """WATCHING signals in trade_queue should not be older than 48h without resolution."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, signal_id, created_ts
            FROM trade_queue
            WHERE client_id = %s
              AND status = 'WATCHING'
              AND created_ts < NOW() - INTERVAL '48 hours'
        """, (client_id,))
        rows = cur.fetchall()
    if rows:
        for row in rows:
            r.warn(
                f"Stale WATCHING signal {row[1]} in queue [created={row[2]}] — "
                f"consider manual review or expiry"
            )
    else:
        r.ok(f"[{client_id}] No stale WATCHING signals (>48h)")


def check_open_positions_count(conn, client_id: str, r: InvariantResult):
    """Log open position count. Warn if >10 open at once."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*), STRING_AGG(underlying || ':' || side, ', ' ORDER BY underlying)
            FROM positions
            WHERE client_id = %s AND status = 'OPEN'
        """, (client_id,))
        row = cur.fetchone()
    count = row[0] if row else 0
    tickers = row[1] if row else ""
    if count > 10:
        r.warn(f"[{client_id}] High open position count: {count} — {tickers}")
    else:
        r.ok(f"[{client_id}] Open positions: {count}" + (f" ({tickers})" if tickers else ""))


def check_db_connection(r: InvariantResult) -> Any:
    """Database must be reachable."""
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        r.ok("PostgreSQL connection verified")
        return conn
    except Exception as e:
        r.fail(f"Cannot connect to PostgreSQL: {e}")
        return None


def check_supabase_connection(r: InvariantResult):
    """Supabase must be reachable."""
    try:
        sb = _supabase()
        if sb is None:
            r.warn("Supabase credentials not set — skipping Supabase check")
            return
        sb.table("client_state").select("client_id").limit(1).execute()
        r.ok("Supabase connection verified")
    except Exception as e:
        r.fail(f"Cannot connect to Supabase: {e}")


def check_kill_switch_off(r: InvariantResult, client_id: str):
    """Kill switch must be OFF before market open."""
    try:
        sb = _supabase()
        if not sb:
            r.warn("Cannot check kill switch — no Supabase credentials")
            return
        result = sb.table("client_state").select("kill_switch,mode").eq("client_id", client_id).execute()
        if result.data:
            ks = result.data[0].get("kill_switch", False)
            mode = result.data[0].get("mode", "PAPER")
            if ks:
                r.warn(f"[{client_id}] Kill switch is ON — no trades will execute. Disable before open if intentional.")
            else:
                r.ok(f"[{client_id}] Kill switch: OFF | Mode: {mode}")
        else:
            r.warn(f"[{client_id}] No client_state row found in Supabase")
    except Exception as e:
        r.warn(f"Kill switch check failed: {e}")


def check_orphaned_watching_signals(conn, client_id: str, r: InvariantResult):
    """WATCHING trade_queue jobs whose OSM order is CANCELED/REJECTED are orphaned."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tq.id, tq.signal_id, tq.created_ts, o.status as osm_status
                FROM trade_queue tq
                LEFT JOIN orders o ON o.local_order_id = tq.signal_id AND o.client_id = tq.client_id
                WHERE tq.client_id = %s
                  AND tq.status = 'WATCHING'
                  AND o.status IN ('CANCELED', 'CANCELLED', 'REJECTED', 'ERROR', 'FAILED')
            """, (client_id,))
            rows = cur.fetchall()
        if rows:
            for row in rows:
                r.warn(
                    f"WATCHING queue job {row[1]} has OSM order in terminal state "
                    f"'{row[3]}' — orphaned signal [created={row[2]}]"
                )
        else:
            r.ok(f"[{client_id}] No orphaned WATCHING/OSM mismatches")
    except Exception as e:
        r.warn(f"Orphaned signal check failed: {e}")


# ── Main runner ───────────────────────────────────────────────────────────────

def run_for_client(client_id: str) -> InvariantResult:
    r = InvariantResult()
    log.info("=" * 60)
    log.info("Pre-market invariant check | client=%s | %s UTC",
             client_id, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    # DB checks
    conn = check_db_connection(r)
    check_supabase_connection(r)
    check_kill_switch_off(r, client_id)

    if conn:
        try:
            check_submitted_orders_have_broker_id(conn, client_id, r)
            check_filled_entries_have_position_id(conn, client_id, r)
            check_no_exit_submitted_without_broker_id(conn, client_id, r)
            check_watching_queue_not_stale(conn, client_id, r)
            check_orphaned_watching_signals(conn, client_id, r)
            check_open_positions_count(conn, client_id, r)
        finally:
            conn.close()

    log.info("=" * 60)
    log.info("RESULT: %s critical | %s warnings | %s passed",
             len(r.critical), len(r.warnings), len(r.passed))
    if r.critical:
        log.error("CRITICAL FAILURES — DO NOT TRADE until resolved:")
        for c in r.critical:
            log.error("  • %s", c)
    if r.warnings:
        log.warning("Warnings:")
        for w in r.warnings:
            log.warning("  • %s", w)
    log.info("=" * 60)
    return r


def main():
    parser = argparse.ArgumentParser(description="Angel Precision pre-market invariant check")
    parser.add_argument("--client-id", default=os.getenv("DEFAULT_CLIENT_ID", "tradefluencehq@gmail.com"))
    parser.add_argument("--all-clients", action="store_true")
    args = parser.parse_args()

    if args.all_clients:
        # Fetch all active clients from Supabase
        try:
            sb = _supabase()
            members = sb.table("members").select("email").eq("approved", True).eq("subscription_active", True).execute()
            client_ids = [m["email"] for m in (members.data or [])]
        except Exception as e:
            log.error("Could not fetch client list: %s", e)
            sys.exit(1)
    else:
        client_ids = [args.client_id]

    worst_code = 0
    for cid in client_ids:
        result = run_for_client(cid)
        worst_code = max(worst_code, result.exit_code)

    sys.exit(worst_code)


if __name__ == "__main__":
    main()
