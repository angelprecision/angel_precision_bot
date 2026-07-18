#!/usr/bin/env python3
"""Dry-run detector and SQL generator for duplicate synthetic broker imports.

This tool never executes repair SQL. It reads one exact client/mode/contract,
groups synthetic rows by economic and signal lineage, selects the earliest row
as the canonical lifecycle, and prints fenced quarantine SQL for operator review.
"""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict

import psycopg2
import psycopg2.extras


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--execution-mode", choices=("paper", "live"), required=True)
    parser.add_argument("--contract", required=True)
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL") or "",
    )
    return parser.parse_args()


def main() -> int:
    args = _args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL or --database-url is required")

    connection = psycopg2.connect(args.database_url)
    connection.set_session(readonly=True, autocommit=True)
    try:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT id, client_id, execution_mode, contract, qty, avg_fill,
                       signal_id, plan_id, status, entry_ts, created_at,
                       local_order_id, broker_order_id
                FROM positions
                WHERE client_id=%s
                  AND LOWER(COALESCE(execution_mode,''))=%s
                  AND UPPER(contract)=UPPER(%s)
                  AND plan_id LIKE 'reconciled:%%'
                  AND COALESCE(local_order_id,'')=''
                  AND COALESCE(broker_order_id,'')=''
                ORDER BY entry_ts ASC NULLS LAST, created_at ASC NULLS LAST, id ASC
                """,
                (args.client_id, args.execution_mode, args.contract),
            )
            rows = [dict(row) for row in cursor.fetchall()]

            grouped: dict[tuple, list[dict]] = defaultdict(list)
            for row in rows:
                key = (
                    int(row.get("qty") or 0),
                    round(float(row.get("avg_fill") or 0.0), 8),
                    str(row.get("signal_id") or ""),
                )
                grouped[key].append(row)

            report = []
            for key, candidates in grouped.items():
                if len(candidates) < 2:
                    continue
                canonical = candidates[0]
                duplicates = candidates[1:]
                duplicate_ids = [str(row["id"]) for row in duplicates]
                quarantine_sql = cursor.mogrify(
                    """
                    UPDATE positions
                       SET status='ERROR',
                           close_source='operator_duplicate_broker_import_quarantine',
                           close_confidence='OPERATOR_REVIEW',
                           updated_at=NOW()
                     WHERE client_id=%s
                       AND LOWER(COALESCE(execution_mode,''))=%s
                       AND UPPER(contract)=UPPER(%s)
                       AND plan_id LIKE 'reconciled:%%'
                       AND COALESCE(local_order_id,'')=''
                       AND COALESCE(broker_order_id,'')=''
                       AND id::text = ANY(%s::text[]);
                    """,
                    (
                        args.client_id,
                        args.execution_mode,
                        args.contract,
                        duplicate_ids,
                    ),
                ).decode("utf-8").strip()
                report.append(
                    {
                        "economic_identity": {
                            "qty": key[0],
                            "avg_fill": key[1],
                            "signal_id": key[2],
                        },
                        "canonical_position_id": str(canonical["id"]),
                        "duplicate_position_ids": duplicate_ids,
                        "generated_quarantine_sql_not_executed": quarantine_sql,
                    }
                )

            print(json.dumps({
                "dry_run": True,
                "client_id": args.client_id,
                "execution_mode": args.execution_mode,
                "contract": args.contract.upper(),
                "candidate_rows": len(rows),
                "duplicate_groups": report,
                "mutations_executed": 0,
            }, indent=2, default=str))
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
