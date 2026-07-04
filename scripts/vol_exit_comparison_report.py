#!/usr/bin/env python3
# =============================================================================
# scripts/vol_exit_comparison_report.py — PR-G3
#
# Runs the volatility-scaled-exit R-multiple comparison and prints a Markdown
# report. READ-ONLY: only SELECTs. Safe against production at any time.
#
# Usage:
#   python3 scripts/vol_exit_comparison_report.py                  # 30d, all
#   python3 scripts/vol_exit_comparison_report.py --days 14
#   python3 scripts/vol_exit_comparison_report.py --client jose@example.com
#   python3 scripts/vol_exit_comparison_report.py --json           # raw dict
#
# Intended cadence: run during the PR-G paper soak; paste the Markdown output
# into the PR-G2 (live-enable) body as the required evidence.
# =============================================================================
import argparse
import json
import sys
from dataclasses import asdict


def main() -> int:
    ap = argparse.ArgumentParser(description="Vol-scaled exit R-multiple comparison (read-only).")
    ap.add_argument("--days", type=int, default=30, help="lookback window in days")
    ap.add_argument("--client", type=str, default=None, help="filter to one client_id/email")
    ap.add_argument("--json", action="store_true", help="emit raw JSON instead of Markdown")
    args = ap.parse_args()

    try:
        from ap.vol_exit_comparison import generate_comparison, to_markdown
    except Exception as exc:
        print(f"ERROR: could not import comparison harness: {exc}", file=sys.stderr)
        return 2

    try:
        report = generate_comparison(lookback_days=args.days, client_filter=args.client)
    except Exception as exc:
        print(f"ERROR: comparison failed: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(asdict(report), indent=2, default=str))
    else:
        print(to_markdown(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
