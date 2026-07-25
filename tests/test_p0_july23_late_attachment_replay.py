"""
tests/test_p0_july23_late_attachment_replay.py
================================================
PR #388 mandatory July-23 late-attachment replay.

The prior version skipped when a Supabase-derived fixture was absent —
which meant the incident replay never actually ran, and the softened
far_missed contract (permit terminal OR waiting_reset) hid the unwired
decisive-drift bug.

This version runs UNCONDITIONALLY. Row source:
  1. Real fixture at tests/fixtures/july23_late_attachment.json when
     present (populate from Supabase and drop the file in whenever
     available; contract preferred over the synthetic set).
  2. Otherwise a production-shaped synthetic fixture built in
     tests/fixtures/july23_late_attachment_synthetic.py — 33 Jason LIVE
     + 41 Jose PAPER + 45 Tradefluence PAPER = 119 rows across the five
     documented buckets.

The CONTRACT is now tight:
  * within         → LATE_ATTACHMENT_WITHIN_CONTINUATION (never terminal)
  * waiting_reset  → MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET
  * far_missed     → LATE_ATTACHMENT_MOVE_MISSED_TERMINAL (never resettable)
  * stop_broken    → STOP_ALREADY_BROKEN_TERMINAL
  * pre_trigger    → TRIGGER_TRUTH_UNAVAILABLE_RETRY with a valid quote
                     (ordinary pre-trigger arm)

Decisive-drift wiring is what makes far_missed → _MISSED reachable from
production data (see tests/test_p0_decisive_drift_wired.py). Because the
classifier itself is pure, the replay test computes decisive_drift_exceeded
the same way production does: canonical_quote beyond trigger by more than
MAX_INTRADAY_DRIFT_PCT (default 1.5%).
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

EXPECTED_TOTALS = {
    ("jason@example.com",        "live"):  33,
    ("jose@example.com",         "paper"): 41,
    ("tradefluence@example.com", "paper"): 45,
}


def _load_rows():
    """Real Supabase fixture takes precedence; synthetic fixture is the
    fallback so the replay always runs and always asserts the tight
    per-bucket contract."""
    if FIXTURE_PATH.exists():
        try:
            return "real", json.loads(FIXTURE_PATH.read_text())
        except json.JSONDecodeError as exc:
            pytest.fail(f"July-23 real fixture is not valid JSON: {exc}")
    from tests.fixtures.july23_late_attachment_synthetic import (
        build_july23_synthetic_rows,
    )
    return "synthetic", build_july23_synthetic_rows()


def _decisive_drift_exceeded(row: dict) -> bool:
    """Compute decisive drift the SAME way production does (see the wiring
    in ap_entry_watcher.py at all three classifier call sites). This
    guarantees the replay evaluates classification against exactly the
    production contract."""
    try:
        from ap_entry_watcher import MAX_INTRADAY_DRIFT_PCT as _MAX
    except Exception:
        _MAX = 0.015
    side = str(row.get("side") or "").upper()
    try:
        trigger = float(row.get("trigger") or 0)
        bid = float(row.get("arm_time_bid") or 0)
        ask = float(row.get("arm_time_ask") or 0)
    except (TypeError, ValueError):
        return False
    if trigger <= 0:
        return False
    if side == "CALL" and ask > 0:
        return ask > trigger * (1.0 + _MAX)
    if side == "PUT" and bid > 0:
        return bid < trigger * (1.0 - _MAX)
    return False


def _classify_row(row: dict) -> str:
    return classify_late_attachment(
        side=str(row.get("side") or "").upper(),
        trigger_price=row.get("trigger"),
        bid=row.get("arm_time_bid"),
        ask=row.get("arm_time_ask"),
        stop=row.get("stop"),
        target_complete=bool(row.get("target_complete")),
        decisive_drift_exceeded=_decisive_drift_exceeded(row),
    ).classification


# ── Fixture shape ──────────────────────────────────────────────────────────

def test_fixture_row_counts_match_incident_totals():
    source, rows = _load_rows()
    counts = Counter()
    for r in rows:
        counts[(r.get("client_id"), str(r.get("execution_mode") or "").lower())] += 1
    for pair, expected in EXPECTED_TOTALS.items():
        assert counts.get(pair, 0) == expected, (
            f"July-23 fixture ({source}) has {counts.get(pair, 0)} rows for "
            f"{pair}, expected {expected} per incident report"
        )


# ── Tight per-bucket contract ─────────────────────────────────────────────

_EXPECTED_CLASSIFICATION = {
    "within":          _WITHIN,
    "waiting_reset":   _WAITING,
    "far_missed":      _MISSED,
    "stop_broken":     _STOP,
    "pre_trigger":     _RETRY,
    "target_complete": _TARGET,
}


def test_every_bucketed_row_matches_its_exact_expected_classification():
    """The tight contract — no bucket permits an alternative. `far_missed`
    MUST land in _MISSED (not _WAITING), which forces decisive_drift wiring
    to be truthful in production. `within` MUST land in _WITHIN, forcing
    the whole late-attachment gate to remain active. And so on."""
    _, rows = _load_rows()
    mismatches = []
    for r in rows:
        expected_bucket = r.get("expected_bucket")
        if expected_bucket is None:
            continue
        expected_cls = _EXPECTED_CLASSIFICATION.get(expected_bucket)
        if expected_cls is None:
            mismatches.append(
                f"unknown expected_bucket={expected_bucket!r} for row {r.get('ticker')}"
            )
            continue
        got = _classify_row(r)
        if got != expected_cls:
            mismatches.append(
                f"{r.get('ticker')} ({r.get('client_id')} {r.get('execution_mode')}) "
                f"bucket={expected_bucket} expected={expected_cls} got={got} "
                f"quote(bid={r.get('arm_time_bid')}, ask={r.get('arm_time_ask')}) "
                f"trigger={r.get('trigger')} stop={r.get('stop')}"
            )
    assert not mismatches, (
        "July-23 replay classification mismatch — the amendment's contract "
        "is not upheld on these rows:\n" + "\n".join(mismatches)
    )


# ── Population invariants ─────────────────────────────────────────────────

def test_no_within_row_terminalizes():
    """The exact class of rows the amendment must rescue: none may
    terminalize as STOP/TARGET/MISSED."""
    _, rows = _load_rows()
    for r in rows:
        if r.get("expected_bucket") != "within":
            continue
        cls = _classify_row(r)
        assert cls not in (_STOP, _TARGET, _MISSED), (
            f"WITHIN row {r.get('ticker')} terminalized as {cls}. "
            f"Row: {r}"
        )


def test_no_far_missed_row_falls_into_waiting_reset():
    """far_missed must be terminal — never allowed to reset and rebreach.
    This is the exact softness the reviewer flagged in the previous replay
    version, and the wiring that makes it enforceable is Blocker #4."""
    _, rows = _load_rows()
    for r in rows:
        if r.get("expected_bucket") != "far_missed":
            continue
        cls = _classify_row(r)
        assert cls == _MISSED, (
            f"far_missed row {r.get('ticker')} classified {cls}; must be "
            f"{_MISSED} — never allowed to reset and rebreach. Row: {r}"
        )


def test_bucketed_replay_summary():
    """Aggregate view: classify every row and print per-bucket totals.
    Runs unconditionally so the incident replay always leaves a trail."""
    source, rows = _load_rows()
    per_class = Counter()
    for r in rows:
        per_class[_classify_row(r)] += 1
    print(f"\nJuly-23 replay ({source}) classification summary:")
    for cls, n in sorted(per_class.items()):
        print(f"  {cls:50s} {n:4d}")
    # Assert every row classified into exactly one bucket (single-return
    # classifier makes this trivially true; assertion makes intent explicit).
    assert sum(per_class.values()) == len(rows)
