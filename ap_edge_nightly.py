#!/usr/bin/env python3
# ap_edge_nightly.py -- Nightly Edge Intelligence Report
# =============================================================================
# Standalone script. Run via cron or scheduler after market close.
#
# Steps:
#   1. Runs APEdgeAnalyzer.run_nightly() for all active clients
#   2. Runs APWhitelistEngine.update_whitelist()
#   3. Prints a human-readable report
#   4. Saves report to /tmp/edge_report_YYYY-MM-DD.txt
#
# Usage:
#   python ap_edge_nightly.py
# =============================================================================

from __future__ import annotations

import sys
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from ap.db import conn, run_with_retry, init_db
from ap_edge_intelligence import APEdgeAnalyzer, APWhitelistEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("ap.edge_nightly")
ET = ZoneInfo("America/New_York")


def get_active_clients() -> list[str]:
    def _fn():
        with conn() as c:
            c.execute("SELECT client_id FROM clients WHERE status = 'ACTIVE'")
            return [row["client_id"] for row in c.fetchall()]
    try:
        return run_with_retry(_fn)
    except Exception as e:
        log.error(f"Failed to fetch active clients: {e}")
        return []


def build_report(all_results: list[dict]) -> str:
    lines = []
    today = datetime.now(ET).strftime("%Y-%m-%d")
    lines.append(f"=== NIGHTLY EDGE REPORT ({today}) ===")
    lines.append("")

    for client_result in all_results:
        client_id = client_result["client_id"]
        total = client_result.get("total_trades", 0)
        results = client_result.get("results", [])

        lines.append(f"--- Client: {client_id} ({total} total trades) ---")
        lines.append("")

        # Best performers (edge_score > 0.5)
        best = [r for r in results if r.get("edge_score", 0) > 0.5 and r.get("bucket_type") == "combo"]
        best.sort(key=lambda x: x.get("edge_score", 0), reverse=True)
        if best:
            lines.append("Best performers (edge_score > 0.5):")
            for r in best[:10]:
                key = r.get("bucket_key", "")
                parts = key.split("|")
                label = " ".join(parts) if len(parts) == 5 else key
                status = r.get("status", "watch").upper()
                lines.append(
                    f"  {label} — expectancy=${r.get('expectancy', 0):.0f} "
                    f"win_rate={r.get('win_rate', 0)*100:.0f}% "
                    f"n={r.get('n_trades', 0)} [{status}]"
                )
            lines.append("")

        # Dead setups (kill candidates)
        dead = [r for r in results if r.get("status") == "kill" and r.get("bucket_type") == "combo"]
        dead.sort(key=lambda x: x.get("expectancy", 0))
        if dead:
            lines.append("Dead setups (kill candidates):")
            for r in dead[:10]:
                key = r.get("bucket_key", "")
                parts = key.split("|")
                label = " ".join(parts) if len(parts) == 5 else key
                lines.append(
                    f"  {label} — expectancy=${r.get('expectancy', 0):.0f} "
                    f"n={r.get('n_trades', 0)} [KILL]"
                )
            lines.append("")

        # Needing data (n < 10, combo buckets only)
        low = [r for r in results if r.get("n_trades", 0) < 10 and r.get("bucket_type") == "combo"]
        low.sort(key=lambda x: x.get("n_trades", 0))
        if low:
            lines.append("Needing data (n < 10):")
            for r in low[:10]:
                key = r.get("bucket_key", "")
                parts = key.split("|")
                label = " ".join(parts) if len(parts) == 5 else key
                status = r.get("status", "watch").upper()
                lines.append(
                    f"  {label} — n={r.get('n_trades', 0)} [{status}]"
                )
            lines.append("")

        # Whitelist summary
        wl = APWhitelistEngine()
        summary = wl.get_summary(client_id)
        lines.append(
            f"Whitelist: {summary.get('active', 0)} active, "
            f"{summary.get('watch', 0)} watch, "
            f"{summary.get('killed', 0)} killed"
        )
        lines.append("")

    return "\n".join(lines)


def main():
    log.info("=== Starting nightly edge analysis ===")
    init_db()

    clients = get_active_clients()
    if not clients:
        log.warning("No active clients found — nothing to analyze")
        sys.exit(0)

    analyzer = APEdgeAnalyzer()
    whitelist = APWhitelistEngine()
    all_results = []

    for client_id in clients:
        log.info(f"Processing client: {client_id}")
        try:
            result = analyzer.run_nightly(client_id)
            all_results.append(result)

            # Update whitelist from combo bucket results
            bucket_results = result.get("results", [])
            whitelist.update_whitelist(client_id, bucket_results)
        except Exception as e:
            log.error(f"Edge analysis failed for {client_id}: {e}")
            all_results.append({
                "client_id": client_id,
                "total_trades": 0,
                "buckets_updated": 0,
                "results": [],
            })

    # Build and output report
    report = build_report(all_results)
    print(report)

    # Save to file
    today = datetime.now(ET).strftime("%Y-%m-%d")
    report_path = f"/tmp/edge_report_{today}.txt"
    try:
        with open(report_path, "w") as f:
            f.write(report)
        log.info(f"Report saved to {report_path}")
    except Exception as e:
        log.warning(f"Failed to save report: {e}")

    log.info("=== Nightly edge analysis complete ===")


if __name__ == "__main__":
    main()
