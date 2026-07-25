"""
P0 (2026-07-02): overnight reeval honest status + backlog starvation.

Production paper reeval reported handoff_run_locks 'success' for four
consecutive sessions (2026-06-29 → 2026-07-02) while 486 WATCHING rows sat
frozen at after_hours_deferred:awaiting_overnight_reeval. Two defects:

1. SILENT STALL — every fetched row took a RETRY_LATER `continue` (paper
   sandbox broker cannot serve prior-day levels), so the run drained nothing
   yet reported green. `run_overnight_reeval` now returns
   fetched/stalled, and client_runner persists a dedicated
   stage='overnight_reeval' lock row with status='partial' on stall.

2. BACKLOG STARVATION — `_fetch_watching_signals` hardcoded LIMIT 100 with
   DESC ordering; once the backlog exceeded 100/client, older rows were
   never fetched again (rows from 2026-06-22 still WATCHING on 07-02).
   Limit is now env-tunable (OVERNIGHT_FETCH_LIMIT, default 500, floor 100,
   ceiling 2000).

These tests are source-level and pure-logic; they do not require Postgres.
"""

import inspect
import pathlib
import re

import ap_overnight_reeval as ov

OV_SRC = pathlib.Path(ov.__file__).with_suffix(".py").read_text()
CR_SRC = pathlib.Path("client_runner.py").read_text()


# ── 1. Result contract ───────────────────────────────────────────────────────

def test_result_dict_carries_fetched_and_stalled():
    assert '"fetched": 0' in OV_SRC
    assert '"stalled": False' in OV_SRC


def test_fetched_is_set_from_fetch_before_early_return():
    assert 'result["fetched"] = len(watching_signals or [])' in OV_SRC


def test_stall_condition_requires_work_and_zero_decisions():
    all_deferred = ov._classify_overnight_reeval_result(
        {
            "fetched": 3,
            "armed": 0,
            "terminal_rejected": 0,
            "terminal_errors": 0,
            "retryable_deferred": 3,
            "unresolved": 0,
        }
    )
    assert all_deferred["stalled"] is True
    assert all_deferred["result_class"] == "RETRYABLE_ALL_DEFERRED"

    mixed = ov._classify_overnight_reeval_result(
        {
            "fetched": 3,
            "armed": 1,
            "terminal_rejected": 1,
            "terminal_errors": 0,
            "retryable_deferred": 1,
            "unresolved": 0,
        }
    )
    assert mixed["stalled"] is False
    assert mixed["result_class"] == "RETRYABLE_PARTIAL_DEFERRED"


def test_stall_logs_structured_error_marker():
    assert "OVERNIGHT_REEVAL_STALLED" in OV_SRC
    assert "category=SILENT_STALL" in OV_SRC


# ── 2. Fetch limit ───────────────────────────────────────────────────────────

def test_hardcoded_limit_100_is_gone():
    for line in OV_SRC.splitlines():
        code = line.split("#", 1)[0]
        assert "LIMIT 100" not in code


def test_limit_is_parameterized_and_bounded():
    assert 'os.getenv("OVERNIGHT_FETCH_LIMIT", "500")' in OV_SRC
    assert "_fetch_limit = max(100, min(_fetch_limit, 2000))" in OV_SRC
    # Limit must be bound as a query parameter, not string-interpolated.
    assert "(client_id, _fetch_limit))" in OV_SRC
    assert "LIMIT %s" in OV_SRC


# ── 3. Honest lock row in client_runner ──────────────────────────────────────

def test_runner_writes_dedicated_overnight_reeval_stage_row():
    assert 'stage="overnight_reeval"' in CR_SRC


def test_runner_reports_partial_on_stall_and_never_marks_success():
    assert 'status = "partial"' in CR_SRC
    assert "mark_success=completed" in CR_SRC
    assert "OVERNIGHT_REEVAL_STALLED:all_fetched_rows_deferred" in CR_SRC


def test_lock_write_is_best_effort_never_fatal():
    m = re.search(
        r"def _persist_overnight_reeval_lock\(.*?try:\s*\n\s*from ap\.morning_handoff import _upsert_handoff_run_lock(.*?)except Exception as _lock_exc",
        CR_SRC,
        re.S,
    )
    assert m, "lock write must be wrapped in try/except"


def test_lock_details_carry_all_run_counts():
    for key in (
        "fetched",
        "armed",
        "terminal_rejected",
        "terminal_errors",
        "retryable_deferred",
        "unresolved",
        "result_class",
        "completed",
        "retryable",
        "retry_reason",
        "attempt_count",
        "attempted_at",
        "next_retry_at",
        "post_open_attempt",
    ):
        assert f'"{key}"' in CR_SRC


# ── 4. Regression fences ─────────────────────────────────────────────────────

def test_row_markers_untouched():
    """
    The release endpoint (app.py) and paper rescue guard match the row
    marker after_hours_deferred:awaiting_overnight_reeval EXACTLY. This PR
    must not rewrite row last_error values — only run-level reporting.
    """
    assert OV_SRC.count("awaiting_overnight_reeval") == \
        pathlib.Path(ov.__file__).with_suffix(".py").read_text().count("awaiting_overnight_reeval")
    assert "_mark_job" not in inspect.getsource(ov._classify_overnight_reeval_result)


def test_early_returns_before_fetch_cannot_stall():
    """Window/trading-day early returns leave fetched=0 → stalled stays False."""
    init_idx = OV_SRC.index('"stalled": False')
    fetch_idx = OV_SRC.index('result["fetched"] = len(')
    window_idx = OV_SRC.index("outside 9:00-9:45 AM ET window")
    assert init_idx < window_idx < fetch_idx
