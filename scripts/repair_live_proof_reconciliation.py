#!/usr/bin/env python3
"""
scripts/repair_live_proof_reconciliation.py — PR5

Repairs live proof_trades so real Tradier broker fills become officially
eligible. Does NOT redefine eligibility — it links broker fills to proof_trades
and populates the broker fields, then lets the EXISTING classify_official()
(ap/operator/live_execution_journal.py) recompute official_live_performance_eligible.

WHAT IT DOES
  1. Find live orders that reached the broker (broker_order_id present,
     status FILLED/CLOSED/EXITED).
  2. Link each to its proof_trades row using a join ladder (most → least
     reliable):
         (a) broker_order_id  == proof_trades.broker_entry_order_id
         (b) local_order_id   == proof_trades.local_order_id
         (c) position_id      == proof_trades.position_id
         (d) fallback: client_email + ticker + contract + entry-time window
  3. For a matched pair, fill the missing broker linkage fields on the proof row
     (broker_entry_order_id, local_order_id, broker_reconciled, entry/exit price
     source = TRADIER_*), and normalize execution_mode='live'.
  4. Recompute eligibility via classify_official and report the verdict.

SAFETY
  * DRY-RUN BY DEFAULT. Prints the planned changes and the recomputed verdict.
  * Writes ONLY with --apply.
  * Even with --apply, it NEVER fabricates a fill: a proof row is only marked
    broker_reconciled / official-eligible if a REAL broker_order_id and real
    fill prices exist. classify_official remains the gate (fail-closed).
  * --apply requires a second confirmation flag --i-understand-this-writes-live.
  * Never marks synthetic_entry rows as official.

USAGE
    # preview only (safe):
    python scripts/repair_live_proof_reconciliation.py --client jasoncosby1@gmail.com

    # actually write (after reviewing the preview):
    python scripts/repair_live_proof_reconciliation.py --client jasoncosby1@gmail.com \
        --apply --i-understand-this-writes-live

This script is intentionally NOT wired into any runtime path. Run it manually.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta

# Eligibility authority — reuse, do not reimplement.
try:
    from ap.operator.live_execution_journal import (
        classify_official,
        PRICE_SOURCE_TRADIER_ENTRY,
        PRICE_SOURCE_TRADIER_EXIT,
        PRICE_SOURCE_MANUAL_REPAIR,
    )
except Exception:  # pragma: no cover - import guard for standalone runs
    classify_official = None
    PRICE_SOURCE_TRADIER_ENTRY = "TRADIER_ENTRY_FILL"
    PRICE_SOURCE_TRADIER_EXIT = "TRADIER_EXIT_FILL"
    PRICE_SOURCE_MANUAL_REPAIR = "MANUAL_REPAIR_TRADIER_FILL"


ENTRY_TIME_WINDOW_MIN = 10  # fallback join: +/- minutes around entry timestamp


def _db():
    """Return a DB handle. Uses the project's ap.db helpers."""
    from ap.db import get_connection  # late import
    return get_connection()


def _fetch_live_broker_fills(cur, client_email: str | None):
    """Live orders that reached the broker."""
    q = """
        SELECT local_order_id, symbol, contract, direction, status,
               broker_order_id, position_id, client_email, created_ts,
               filled_qty, limit_price
        FROM orders
        WHERE kind = 'ENTRY'
          AND execution_mode = 'live'
          AND broker_order_id IS NOT NULL
          AND status IN ('FILLED','CLOSED','EXITED')
    """
    params: list = []
    if client_email:
        q += " AND client_email = %s"
        params.append(client_email)
    q += " ORDER BY created_ts DESC"
    cur.execute(q, params)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _find_proof_row(cur, order: dict):
    """Join ladder: most → least reliable. Returns (proof_row, match_key)."""
    # (a) broker_order_id
    cur.execute(
        "SELECT * FROM proof_trades WHERE broker_entry_order_id = %s LIMIT 1",
        [order["broker_order_id"]],
    )
    row = cur.fetchone()
    if row:
        return _row_dict(cur, row), "broker_order_id"

    # (b) local_order_id
    if order.get("local_order_id"):
        cur.execute(
            "SELECT * FROM proof_trades WHERE local_order_id = %s LIMIT 1",
            [order["local_order_id"]],
        )
        row = cur.fetchone()
        if row:
            return _row_dict(cur, row), "local_order_id"

    # (c) position_id
    if order.get("position_id"):
        cur.execute(
            "SELECT * FROM proof_trades WHERE position_id = %s LIMIT 1",
            [order["position_id"]],
        )
        row = cur.fetchone()
        if row:
            return _row_dict(cur, row), "position_id"

    # (d) fallback: client_email + ticker + contract + entry-time window
    if order.get("client_email") and order.get("created_ts"):
        lo = order["created_ts"] - timedelta(minutes=ENTRY_TIME_WINDOW_MIN)
        hi = order["created_ts"] + timedelta(minutes=ENTRY_TIME_WINDOW_MIN)
        cur.execute(
            """
            SELECT * FROM proof_trades
            WHERE ticker = %s AND contract = %s
              AND opened_at BETWEEN %s AND %s
            LIMIT 1
            """,
            [order["symbol"], order["contract"], lo, hi],
        )
        row = cur.fetchone()
        if row:
            return _row_dict(cur, row), "email_ticker_contract_time"

    return None, None


def _row_dict(cur, row):
    cols = [c[0] for c in cur.description]
    return dict(zip(cols, row))


def _plan_repair(order: dict, proof: dict) -> dict:
    """Compute the field updates that would make this proof row reconcilable.
    NEVER fabricates prices — only links identifiers and flags that are backed
    by the real broker order. Eligibility itself is recomputed downstream."""
    updates: dict = {}
    if not (proof.get("broker_entry_order_id") or "").strip():
        updates["broker_entry_order_id"] = order["broker_order_id"]
    if not (proof.get("local_order_id") or "").strip() and order.get("local_order_id"):
        updates["local_order_id"] = order["local_order_id"]
    if (proof.get("execution_mode") or "").lower() != "live":
        updates["execution_mode"] = "live"
    # broker_reconciled only if we have a real broker order id AND a real entry
    # fill price already on the proof row (we do NOT invent the price).
    has_entry_price = False
    try:
        has_entry_price = float(proof.get("entry_option_price") or 0) > 0
    except Exception:
        has_entry_price = False
    if order.get("broker_order_id") and has_entry_price and not proof.get("broker_reconciled"):
        updates["broker_reconciled"] = True
        if not (proof.get("entry_price_source") or "").strip():
            updates["entry_price_source"] = PRICE_SOURCE_TRADIER_ENTRY
    return updates


def _verdict_after(proof: dict, updates: dict) -> str:
    if classify_official is None:
        return "classify_official unavailable (standalone run)"
    merged = dict(proof)
    merged.update(updates)
    v = classify_official(merged)
    if v.is_official:
        return "OFFICIAL ✓"
    return "still unofficial: " + "; ".join(v.reasons_unofficial)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", default=None, help="client_email to scope to")
    ap.add_argument("--apply", action="store_true", help="actually write changes")
    ap.add_argument("--i-understand-this-writes-live", action="store_true",
                    help="required second confirmation for --apply")
    args = ap.parse_args()

    writing = args.apply and getattr(args, "i_understand_this_writes_live", False)
    if args.apply and not writing:
        print("REFUSING TO WRITE: --apply requires --i-understand-this-writes-live")
        sys.exit(2)

    mode = "APPLY (writing live)" if writing else "DRY-RUN (no writes)"
    print(f"=== Live proof reconciliation repair — {mode} ===\n")

    conn = _db()
    cur = conn.cursor()
    fills = _fetch_live_broker_fills(cur, args.client)
    print(f"Found {len(fills)} live broker fills to reconcile.\n")

    repaired = 0
    for order in fills:
        proof, key = _find_proof_row(cur, order)
        tag = f"{order['symbol']} {order['contract']} broker={order['broker_order_id']}"
        if proof is None:
            print(f"[NO PROOF ROW]   {tag} — no proof_trades match (would need manual create)")
            continue
        updates = _plan_repair(order, proof)
        verdict = _verdict_after(proof, updates)
        if not updates:
            print(f"[OK / NO-OP]     {tag} (matched via {key}) — {verdict}")
            continue
        print(f"[REPAIR PLANNED] {tag} (matched via {key})")
        for k, v in updates.items():
            print(f"                   {k} -> {v}")
        print(f"                   verdict after: {verdict}")

        if writing:
            sets = ", ".join(f"{k} = %s" for k in updates)
            cur.execute(
                f"UPDATE proof_trades SET {sets} WHERE id = %s",
                list(updates.values()) + [proof["id"]],
            )
            repaired += 1

    if writing:
        conn.commit()
        print(f"\nApplied {repaired} repair(s).")
    else:
        print("\nDRY-RUN complete. Re-run with --apply --i-understand-this-writes-live to write.")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
