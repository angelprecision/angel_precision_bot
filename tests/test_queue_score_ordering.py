"""
Regression test for the AUDIT PHASE-2 queue change:
ORDER BY score DESC, created_ts ASC (was: ORDER BY created_ts ASC).

This test does NOT hit a real Postgres -- it asserts the SQL contains the
expected ORDER BY clause. That's enough to prevent silent reversion.
"""
import re


def test_queue_orders_by_score_desc_then_created_ts():
    with open("ap/queue.py") as f:
        src = f.read()

    # The score-based ORDER BY must be present in _claim_one_job's SQL.
    # We accept formatting variation but require the key tokens in order.
    pattern = re.compile(
        r"ORDER\s+BY\s+"
        r"COALESCE\s*\(\s*NULLIF\s*\(\s*payload->>'score',\s*''\s*\)\s*::\s*numeric,\s*\d+\s*\)\s+DESC\s*,\s*"
        r"created_ts\s+ASC",
        re.IGNORECASE,
    )
    assert pattern.search(src), (
        "ap/queue.py _claim_one_job must ORDER BY score DESC, created_ts ASC. "
        "The previous FIFO 'ORDER BY created_ts ASC' was the first-7-wins bug "
        "that caused low-quality early signals to burn slots."
    )


def test_no_fifo_only_ordering_remains():
    """Guard: ensure no real SQL claim path still uses 'ORDER BY created_ts ASC'
    without a score key first. Excludes Python comments and docstrings."""
    with open("ap/queue.py") as f:
        lines = f.readlines()
    # Strip lines that are pure comments (start with '#').
    code_lines = [ln for ln in lines if not ln.lstrip().startswith("#")]
    src = "".join(code_lines)
    matches = re.findall(r"ORDER\s+BY\s+([^\n]+)", src, re.IGNORECASE)
    fifo_only = [m for m in matches
                 if "score" not in m.lower() and "created_ts" in m.lower()
                 and "asc" in m.lower()]
    assert not fifo_only, (
        f"Found ORDER BY clauses that still use FIFO without score: {fifo_only}. "
        "Every claim ordering must be score-first."
    )
