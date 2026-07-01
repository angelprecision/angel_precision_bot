#!/usr/bin/env python3
"""
scripts/backfill_jason_smci_20260625.py
─────────────────────────────────────────────────────────────────────────────
PR: fix/jason-smci-backfill-true-exit-price
ONE-TIME SCRIPT — run once after confirming true fill price from Tradier.

INCIDENT
  Date:     2026-06-25
  Client:   jasoncosby1@gmail.com
  Contract: SMCI260626P00032500 (PUT $3.25, expired 2026-06-26)
  Position: 74af11c2-6b5f-407e-a5cc-7cac2a59aebf

  Angel manually closed the position via Tradier. The reconciler auto-closed
  the DB record at 3:41 PM ET with exit_price=1.06 (market quote estimate).
  The true fill price from the manual sell order is unknown until pulled from
  Tradier order history.

USAGE
  1. Set the TRUE_FILL_PRICE and TRADIER_ORDER_ID constants below
     (get them from Tradier order history for the sell_to_close order)
  2. Run dry first:
       python3 scripts/backfill_jason_smci_20260625.py --dry-run
  3. Confirm output looks correct, then:
       python3 scripts/backfill_jason_smci_20260625.py --apply

REQUIRES
  - DATABASE_URL env var set (or .env loaded)
  - Migration 2026_06_26_operator_audit_log.sql applied
─────────────────────────────────────────────────────────────────────────────
"""
import argparse
import os
import sys
from datetime import timezone

# ── Fill these in after pulling from Tradier order history ───────────────────
TRUE_FILL_PRICE:  float = 0.0      # ← REQUIRED: set to actual Tradier fill
TRADIER_ORDER_ID: str   = ""       # ← OPTIONAL: Tradier sell order ID

# ── Incident constants — do not change ───────────────────────────────────────
CLIENT_ID   = "jasoncosby1@gmail.com"
POSITION_ID = "74af11c2-6b5f-407e-a5cc-7cac2a59aebf"
CONTRACT    = "SMCI260626P00032500"
ENTRY_FILL  = 1.40   # confirmed from entry order fill_price
QTY         = 1
REASON      = "operator_manual_close_june25_smci_backfill"


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true")
    group.add_argument("--apply",   action="store_true")
    args = parser.parse_args()

    if TRUE_FILL_PRICE <= 0:
        print("ERROR: TRUE_FILL_PRICE not set. Edit the constant at the top of this script.")
        sys.exit(1)

    # ── Calculate PnL ─────────────────────────────────────────────────────────
    entry_cost    = ENTRY_FILL * QTY * 100          # $140.00
    exit_proceeds = TRUE_FILL_PRICE * QTY * 100
    realized_pnl  = round(exit_proceeds - entry_cost, 2)
    realized_pnl_pct = round((realized_pnl / entry_cost) * 100, 4)

    print("=" * 60)
    print("SMCI BACKFILL — Jason 2026-06-25")
    print("=" * 60)
    print(f"  position_id      : {POSITION_ID}")
    print(f"  client_id        : {CLIENT_ID}")
    print(f"  contract         : {CONTRACT}")
    print(f"  entry_fill       : ${ENTRY_FILL}")
    print(f"  true_fill_price  : ${TRUE_FILL_PRICE}")
    print(f"  tradier_order_id : {TRADIER_ORDER_ID or '(not set)'}")
    print(f"  realized_pnl     : ${realized_pnl}")
    print(f"  realized_pnl_pct : {realized_pnl_pct}%")
    print(f"  mode             : {'DRY RUN' if args.dry_run else 'APPLY — MUTATING DB'}")
    print("=" * 60)

    if args.dry_run:
        print("\nDRY RUN — no mutations. Re-run with --apply to commit.")
        return

    # ── Connect ───────────────────────────────────────────────────────────────
    try:
        from ap.db import conn, run_with_retry
    except ImportError:
        import psycopg2
        import psycopg2.extras
        DATABASE_URL = os.environ["DATABASE_URL"]

        class _ConnCtx:
            def __init__(self):
                self._c = psycopg2.connect(DATABASE_URL)
                self._c.autocommit = False
                self._cur = self._c.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                )
            def __enter__(self): return self._cur
            def __exit__(self, *a):
                if a[0]: self._c.rollback()
                else:    self._c.commit()
                self._cur.close()
                self._c.close()

        def conn(): return _ConnCtx()
        def run_with_retry(fn): return fn()

    def _apply():
        with conn() as c:
            # ── 1. Verify current state ───────────────────────────────────────
            c.execute(
                "SELECT status, exit_price, close_source FROM positions WHERE id = %s AND client_id = %s",
                (POSITION_ID, CLIENT_ID)
            )
            row = c.fetchone()
            if not row:
                print(f"ERROR: position {POSITION_ID} not found for {CLIENT_ID}")
                sys.exit(1)

            print(f"\nCurrent DB state:")
            print(f"  status       : {row['status']}")
            print(f"  exit_price   : {row['exit_price']}")
            print(f"  close_source : {row['close_source']}")

            # ── 2. Update position ────────────────────────────────────────────
            c.execute(
                """
                UPDATE positions
                SET    exit_price         = %s,
                       exit_reason        = %s,
                       close_source       = 'operator_manual_close',
                       close_confidence   = 'HIGH',
                       status             = 'CLOSED',
                       realized_pnl       = %s,
                       realized_pnl_pct   = %s,
                       quantity_remaining = 0,
                       exit_in_flight     = false,
                       updated_at         = NOW()
                WHERE  id        = %s
                  AND  client_id = %s
                """,
                (TRUE_FILL_PRICE, REASON, realized_pnl, realized_pnl_pct,
                 POSITION_ID, CLIENT_ID),
            )
            print(f"\n✅ Position updated ({c.rowcount} row)")

            # ── 3. Write audit log (required for production repair proof) ─────
            c.execute(
                """
                INSERT INTO operator_audit_log
                  (event_type, client_id, position_id, contract,
                   true_fill_price, tradier_order_id, reason,
                   realized_pnl, realized_pnl_pct, operator_note, created_at)
                VALUES
                  ('manual_close', %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                (CLIENT_ID, POSITION_ID, CONTRACT,
                 TRUE_FILL_PRICE, TRADIER_ORDER_ID or None, REASON,
                 realized_pnl, realized_pnl_pct,
                 "backfill of SMCI 2026-06-25 incident — reconciler used market quote"),
            )
            print(f"✅ Audit log written ({c.rowcount} row)")

            # ── 4. Verify final state ─────────────────────────────────────────
            c.execute(
                """
                SELECT status, exit_price, realized_pnl, realized_pnl_pct,
                       close_source, exit_reason, exit_in_flight
                FROM positions WHERE id = %s
                """,
                (POSITION_ID,)
            )
            final = c.fetchone()
            print(f"\nFinal DB state:")
            for k, v in (final or {}).items():
                print(f"  {k:<20}: {v}")

    run_with_retry(_apply)
    print("\n✅ Backfill complete.")


if __name__ == "__main__":
    main()
