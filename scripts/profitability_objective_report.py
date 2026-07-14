#!/usr/bin/env python3
"""Evaluate a frozen opportunity-ranking policy from strict JSONL evidence."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ap.profitability_objective import (  # noqa: E402
    ProfitabilityDataError,
    ProfitabilityTargets,
    evaluate_frozen_policy,
)


def _read_jsonl(path: Path) -> list[dict]:
    if not path.is_file():
        raise ProfitabilityDataError(f"source JSONL not found: {path}")
    rows: list[dict] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw in enumerate(handle, start=1):
            text = raw.strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ProfitabilityDataError(
                    f"invalid JSON on line {line_number}: {exc.msg}"
                ) from exc
            if not isinstance(row, dict):
                raise ProfitabilityDataError(f"line {line_number} must contain a JSON object")
            rows.append(row)
    if not rows:
        raise ProfitabilityDataError("source JSONL contains no opportunity rows")
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Evaluate frozen top-5-to-7 policy evidence without changing trading behavior"
    )
    parser.add_argument("--source", required=True, type=Path, help="Strict opportunity JSONL")
    parser.add_argument("--policy-version", required=True)
    parser.add_argument(
        "--policy-frozen-at",
        required=True,
        help="Timezone-aware ISO timestamp; only later decisions are holdout evidence",
    )
    parser.add_argument("--min-score", type=float, default=0.0)
    parser.add_argument("--target-win-rate", type=float, default=0.80)
    parser.add_argument("--max-avg-loss", type=float, default=0.12)
    parser.add_argument("--min-avg-win", type=float, default=0.18)
    parser.add_argument("--min-sessions", type=int, default=20)
    parser.add_argument("--min-resolved", type=int, default=100)
    parser.add_argument("--min-outcome-coverage", type=float, default=0.95)
    args = parser.parse_args(argv)

    try:
        targets = ProfitabilityTargets(
            target_win_rate=args.target_win_rate,
            max_avg_loss_pct=args.max_avg_loss,
            min_avg_win_pct=args.min_avg_win,
            min_holdout_sessions=args.min_sessions,
            min_resolved_trades=args.min_resolved,
            min_outcome_coverage=args.min_outcome_coverage,
        )
        report = evaluate_frozen_policy(
            _read_jsonl(args.source),
            policy_version=args.policy_version,
            policy_frozen_at=args.policy_frozen_at,
            targets=targets,
            min_policy_score=args.min_score,
        )
    except ProfitabilityDataError as exc:
        parser.error(str(exc))

    print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
