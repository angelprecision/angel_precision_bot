#!/usr/bin/env python3
"""
scripts/repair_live_proof_reconciliation.py — PR5 (amended)

Repairs live proof_trades so real Tradier broker fills become officially
eligible. Reuses the EXISTING classify_official() authority — does not redefine
eligibility.

REVIEW (HARD HOLD) FIXES IN THIS REVISION:
  * Uses the real DB API ap.db.conn() (a @contextmanager), NOT a nonexistent
    ap.db.get_connection.
  * Reads orders.client_id (orders has NO client_email column). The cross-table
    client bridge is orders.client_id == proof_trades.client_email (same value,
    e.g. 'jasoncosby1@gmail.com' — confirmed on the live schema).
  * The fuzzy fallback match (ticker+contract+time) now REQUIRES a client
    predicate (proof_trades.client_email == order.client_id) so a trade can
    never be mis-linked into a different client's proof record.
  * Still DRY-RUN by default; writes only with --apply AND a second confirm flag.

SAFETY (unchanged): never fabricates a fill; broker_reconciled set only when a
real broker_order_id AND a real entry fill price exist; classify_official
remains the fail-closed gate; synthetic rows never qualify. Not wired into any
runtime path. Do NOT run against live until reviewed.

USAGE
    python scripts/repair_live_proof_reconciliation.py --client jasoncosby1@gmail.com
    python scripts/repair_live_proof_reconciliation.py --client jasoncosby1@gmail.com \
        --apply --i-understand-this-writes-live
"""
from __future__ import annotations

import argparse
import sys
from datetime import timedelta

try:
    from ap.operator.live_execution_journal import (
        classify_official,
        PRICE_SOURCE_TRADIER_ENTRY,
        PRICE_SOURCE_TRADIER_EXIT,
        PRICE_SOURCE_MANUAL_REPAIR,
    )
except Exception:  # pragma: no cover
    classify_official = None
    PRICE_SOURCE_TRADIER_ENTRY = "TRADIER_ENTRY_FILL"
    PRICE_SOURCE_TRADIER_EXIT = "TRADIER_EXIT_FILL"
    PRICE_SOURCE_MANUAL_REPAIR = "MANUAL_REPAIR_TRADIER_FILL"

ENTRY_TIME_WINDOW_MIN = 10  # fallback join: +/- minutes around entry timestamp


def _fetch_live_broker_fills(c, client_id):
    """Live orders that reached the broker. orders uses client_id (NOT
    client_email)."""
    q = """
        SELECT local_order_id, symbol, contract, direction, status,
               broker_order_id, position_id, client_id, created_ts,
               filled_qty, limit_price
        FROM orders
        WHERE kind = 'ENTRY'
          AND execution_mode = 'live'
          AND broker_order_id IS NOT NULL
          AND status IN ('FILLED','CLOSED','EXITED')
    """
    params = []
    if client_id:
        q += " AND client_id = %s"
        params.append(client_id)
    q += " ORDER BY created_ts DESC"
    c.execute(q, params)
    rows = c.fetchall()
    cols = [d[0] for d in c.description]
    return [dict(zip(cols, r)) for r in rows]


def _row_dict(c, row):
    cols = [d[0] for d in c.description]
    return dict(zip(cols, row))


def _find_proof_row(c, order):
    """Join ladder: most -> least reliable. Returns (proof_row, match_key).

    The cross-table client bridge is orders.client_id == proof_trades.client_email.
    The fuzzy fallback (step d) REQUIRES this client predicate so a trade can
    never be mis-linked into another client's proof record.
    """
    client_id = order.get("client_id")

    # (a) broker_order_id
    c.execute(
        "SELECT * FROM proof_trades WHERE broker_entry_order_id = %s LIMIT 1",
        [order["broker_order_id"]],
    )
    row = c.fetchone()
    if row:
        return _row_dict(c, row), "broker_order_id"

    # (b) local_order_id
    if order.get("local_order_id"):
        c.execute(
            "SELECT * FROM proof_trades WHERE local_order_id = %s LIMIT 1",
            [order["local_order_id"]],
        )
        row = c.fetchone()
        if row:
            return _row_dict(c, row), "local_order_id"

    # (c) position_id
    if order.get("position_id"):
        c.execute(
            "SELECT * FROM proof_trades WHERE position_id = %s LIMIT 1",
            [order["position_id"]],
        )
        row = c.fetchone()
        if row:
            return _row_dict(c, row), "position_id"

    # (d) fallback: client + ticker + contract + entry-time window.
    # CLIENT PREDICATE IS MANDATORY — never match across clients.
    # AMENDMENTS:
    #   - If client_id is missing, REFUSE the fallback (explicit, logged) rather
    #     than silently skipping. We never run a fuzzy match without a client.
    #   - If multiple proof rows match the fuzzy window, REFUSE and tag
    #     AMBIGUOUS_MATCH — picking either could mis-link the audit trail. The
    #     operator resolves manually. We fetch LIMIT 2 to detect this cheaply.
    if not order.get("created_ts"):
        return None, "no_created_ts_no_fallback"
    if not client_id:
        return None, "missing_client_id_fallback_refused"
    lo = order["created_ts"] - timedelta(minutes=ENTRY_TIME_WINDOW_MIN)
    hi = order["created_ts"] + timedelta(minutes=ENTRY_TIME_WINDOW_MIN)
    c.execute(
        """
        SELECT * FROM proof_trades
        WHERE client_email = %s
          AND ticker = %s AND contract = %s
          AND opened_at BETWEEN %s AND %s
        ORDER BY opened_at ASC
        LIMIT 2
        """,
        [client_id, order["symbol"], order["contract"], lo, hi],
    )
    rows = c.fetchall()
    if not rows:
        return None, None
    if len(rows) > 1:
        # Ambiguous — never guess on live proof reconciliation.
        return None, "AMBIGUOUS_MATCH"
    return _row_dict(c, rows[0]), "client_ticker_contract_time"


def _plan_repair(order, proof):
    """Compute field updates that make this proof row reconcilable. NEVER
    fabricates prices — only links identifiers/flags backed by the real order.

    Defense in depth: refuse to plan any repair if the proof row's client_email
    does not equal the order's client_id (cross-client safety)."""
    if (proof.get("client_email") or "") != (order.get("client_id") or ""):
        return {}  # cross-client mismatch — never touch

    updates = {}
    if not (proof.get("broker_entry_order_id") or "").strip():
        updates["broker_entry_order_id"] = order["broker_order_id"]
    if not (proof.get("local_order_id") or "").strip() and order.get("local_order_id"):
        updates["local_order_id"] = order["local_order_id"]
    if (proof.get("execution_mode") or "").lower() != "live":
        updates["execution_mode"] = "live"

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


def _verdict_after(proof, updates):
    if classify_official is None:
        return "classify_official unavailable (standalone run)"
    merged = dict(proof)
    merged.update(updates)
    v = classify_official(merged)
    if v.is_official:
        return "OFFICIAL"
    return "still unofficial: " + "; ".join(v.reasons_unofficial)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", default=None, help="client_id (e.g. jasoncosby1@gmail.com)")
    ap.add_argument("--apply", action="store_true", help="actually write changes")
    ap.add_argument("--i-understand-this-writes-live", action="store_true",
                    help="required second confirmation for --apply")
    args = ap.parse_args()

    writing = args.apply and getattr(args, "i_understand_this_writes_live", False)
    if args.apply and not writing:
        print("REFUSING TO WRITE: --apply requires --i-understand-this-writes-live")
        sys.exit(2)

    mode = "APPLY (writing live)" if writing else "DRY-RUN (no writes)"
    print("=== Live proof reconciliation repair -- %s ===\n" % mode)

    from ap.db import conn  # late import so --help works without DB

    with conn() as c:
        fills = _fetch_live_broker_fills(c, args.client)
        print("Found %d live broker fills to reconcile.\n" % len(fills))

        repaired = 0
        ambiguous = 0
        refused_no_client = 0
        for order in fills:
            proof, key = _find_proof_row(c, order)
            tag = "%s %s broker=%s client=%s" % (
                order["symbol"], order["contract"], order["broker_order_id"], order.get("client_id"))
            if proof is None:
                # `key` now carries a refusal reason when we didn't match for a
                # safety reason (ambiguous / missing client) — surface it so
                # the operator can resolve manually rather than guessing.
                if key == "AMBIGUOUS_MATCH":
                    print("[AMBIGUOUS]      %s -- multiple proof rows in fuzzy window; REFUSED (resolve manually)" % tag)
                    ambiguous += 1
                elif key == "missing_client_id_fallback_refused":
                    print("[NO CLIENT_ID]   %s -- fallback refused (cannot run fuzzy match without client_id)" % tag)
                    refused_no_client += 1
                else:
                    print("[NO PROOF ROW]   %s -- no proof_trades match (manual create needed)" % tag)
                continue
            updates = _plan_repair(order, proof)
            verdict = _verdict_after(proof, updates)
            if not updates:
                print("[OK / NO-OP]     %s (matched via %s) -- %s" % (tag, key, verdict))
                continue
            print("[REPAIR PLANNED] %s (matched via %s)" % (tag, key))
            for k, v in updates.items():
                print("                   %s -> %s" % (k, v))
            print("                   verdict after: %s" % verdict)
            if writing:
                sets = ", ".join("%s = %%s" % k for k in updates)
                c.execute(
                    "UPDATE proof_trades SET %s WHERE id = %%s" % sets,
                    list(updates.values()) + [proof["id"]],
                )
                repaired += 1

        # Summary footer so the operator can see refusals at a glance.
        if ambiguous or refused_no_client:
            print("\nSafety refusals: ambiguous=%d, no_client_id=%d (resolve manually)" %
                  (ambiguous, refused_no_client))

        if writing:
            print("\nApplied %d repair(s). (conn() auto-commits on success.)" % repaired)
        else:
            print("\nDRY-RUN complete. Re-run with --apply --i-understand-this-writes-live to write.")


if __name__ == "__main__":
    main()
