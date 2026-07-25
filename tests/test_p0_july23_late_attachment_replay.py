"""
tests/test_p0_july23_late_attachment_replay.py
================================================
PR #388 Block-2 — July-23 arm_already_through_trigger replay.

Population-level regression: replays real 2026-07-23 rows through the
canonical classifier and asserts that setups which merely attached
cents-late are no longer terminalized as arm_already_through_trigger, while
far-missed moves still terminalize.

Fixture path (repo-relative):
    tests/fixtures/july23_late_attachment.json

Fixture schema (list of objects):
    [
      {
        "client_id": "jason@example.com",
        "execution_mode": "live",             // "live" | "paper"
        "ticker": "SPY",
        "side": "CALL",                        // "CALL" | "PUT"
        "trigger": 445.10,
        "arm_time_bid": 445.15,
        "arm_time_ask": 445.20,
        "stop": 442.50,
        "target_complete": false,
        "arm_time_iso": "2026-07-23T13:32:11+00:00",
        "expected_bucket": "within" | "waiting_reset" | "stop_broken"
                          | "target_complete" | "far_missed"
                          | "ordinary_below_trigger"
      },
      ...
    ]

'expected_bucket' is optional and only used to build a per-bucket summary.
When absent, the row is classified but not asserted against.

Skips cleanly when the fixture is missing so the amendment PR can merge
without data. Populate the fixture (see PR #388 Block-2 handoff notes) to
enable the full replay assertion.
"""
from __future__ import annotations

import json
import pathlib
from collections import Counter

import pytest

from ap.pending_trigger_classifier import (
    LATE_ATTACHMENT_WITHIN_CONTINUATION as _WITHIN,
    MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET as _WAITING,
    STOP_ALREADY_BROKEN_TERMINAL           as _STOP,
    TARGET_ALREADY_COMPLETE_TERMINAL       as _TARGET,
    LATE_ATTACHMENT_MOVE_MISSED_TERMINAL   as _MISSED,
    TRIGGER_TRUTH_UNAVAILABLE_RETRY        as _RETRY,
    classify_late_attachment,
)


FIXTURE_PATH = pathlib.Path(__file__).parent / "fixtures" / "july23_late_attachment.json"

# Expected per-client row counts from the incident report.
EXPECTED_TOTALS = {
    ("jason@example.com",       "live"):  33,
    ("jose@example.com",        "paper"): 41,
    ("tradefluence@example.com", "paper"): 45,
}


def _load_rows():
    if not FIXTURE_PATH.exists():
        pytest.skip(
            f"July-23 replay fixture not present at {FIXTURE_PATH}. "
            "See test module docstring for schema; drop the fixture in to enable."
        )
    try:
        return json.loads(FIXTURE_PATH.read_text())
    except json.JSONDecodeError as exc:
        pytest.fail(f"July-23 fixture is not valid JSON: {exc}")


def _classify_row(row: dict) -> str:
    return classify_late_attachment(
        side=str(row.get("side") or "").upper(),
        trigger_price=row.get("trigger"),
        bid=row.get("arm_time_bid"),
        ask=row.get("arm_time_ask"),
        stop=row.get("stop"),
        target_complete=bool(row.get("target_complete")),
        decisive_drift_exceeded=bool(row.get("decisive_drift_exceeded")),
    ).classification


# ── Sanity: fixture shape matches the incident report ────────────────────────

def test_fixture_row_counts_match_incident_totals():
    rows = _load_rows()
    counts = Counter()
    for r in rows:
        counts[(r.get("client_id"), str(r.get("execution_mode") or "").lower())] += 1
    for pair, expected in EXPECTED_TOTALS.items():
        assert counts.get(pair, 0) == expected, (
            f"July-23 fixture has {counts.get(pair, 0)} rows for {pair}, "
            f"expected {expected} per incident report"
        )


# ── Population invariant: rows that only barely crossed the trigger must NOT
# terminalize under the new classifier — that was the arm_already_through_trigger
# regression this PR exists to fix.

def test_no_row_within_continuation_zone_is_terminalized():
    rows = _load_rows()
    for r in rows:
        cls = _classify_row(r)
        if r.get("expected_bucket") == "within":
            assert cls == _WITHIN, (
                f"July-23 replay: row {r.get('ticker')} classified {cls}; "
                f"expected {_WITHIN} (this is the exact class of rows the "
                f"amendment must rescue). Row: {r}"
            )


def test_rows_flagged_far_missed_still_terminalize():
    rows = _load_rows()
    for r in rows:
        if r.get("expected_bucket") == "far_missed":
            cls = _classify_row(r)
            assert cls in (_MISSED, _WAITING), (
                f"July-23 replay: row {r.get('ticker')} classified {cls}; "
                f"expected terminal or waiting-reset (never WITHIN). Row: {r}"
            )


def test_bucketed_replay_summary_and_no_within_rows_are_terminal():
    """Aggregate view: classify every row, print per-bucket totals, and
    assert the population invariant — no row lands in WITHIN AND gets
    reported as terminal simultaneously."""
    rows = _load_rows()
    per_class = Counter()
    for r in rows:
        per_class[_classify_row(r)] += 1

    # Emit a compact summary via pytest's captured stdout — visible with -s.
    print("\nJuly-23 replay classification summary:")
    for cls, n in sorted(per_class.items()):
        print(f"  {cls:50s} {n:4d}")

    # The whole point of Block-2: WITHIN is a first-class non-terminal
    # classification. Assert it is not counted alongside any terminal code
    # for the same row (single-return classifier makes this trivially true,
    # but the assertion makes the intent explicit).
    assert per_class[_WITHIN] + per_class[_WAITING] + per_class[_STOP] + \
           per_class[_TARGET] + per_class[_MISSED] + per_class[_RETRY] == \
           sum(per_class.values()), \
           "Each row must classify into exactly one bucket."
