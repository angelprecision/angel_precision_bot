"""
Regression test for the AUDIT PHASE-2 queue change:
ORDER BY score DESC, created_ts ASC (was: ORDER BY created_ts ASC).

This test does NOT hit a real Postgres -- it asserts the SQL contains the
expected ORDER BY clause. That's enough to prevent silent reversion.
"""
import re


def test_queue_orders_by_score_desc_then_created_ts():
    """_claim_one_job must order by score DESC, created_ts ASC.

    Accepts either the legacy COALESCE(NULLIF(...)::numeric, X) form OR the
    new defensive CASE WHEN ... ~ regex ... THEN ... cast form. Either way,
    the ordering must lead with the score expression DESC then created_ts ASC.
    """
    with open("ap/queue.py") as f:
        src = f.read()

    # Look for the key shape: ORDER BY ... score ... DESC, created_ts ASC.
    # We just check the new-style CASE/WHEN/ELSE pattern is present together
    # with the score column reference.
    pattern = re.compile(
        r"ORDER\s+BY\s+CASE\s+.*?payload->>'score'.*?DESC\s*,\s*created_ts\s+ASC",
        re.IGNORECASE | re.DOTALL,
    )
    assert pattern.search(src), (
        "ap/queue.py _claim_one_job must ORDER BY (regex-guarded score DESC), created_ts ASC. "
        "The previous FIFO 'ORDER BY created_ts ASC' was the first-N-wins bug."
    )


def test_score_cast_is_regex_guarded():
    """BLOCKER-1 regression: the previous NULLIF(...)::numeric pattern crashed
    Postgres on non-numeric scores like 'A+'. Confirm the new defensive form
    is present (regex pre-check before cast)."""
    with open("ap/queue.py") as f:
        src = f.read()
    # Strip Python comment lines so the regex guard test below isn't fooled
    # by historical-context comments.
    code = "\n".join(ln for ln in src.splitlines() if not ln.lstrip().startswith("#"))
    assert "~ '^-?[0-9]+(\\.[0-9]+)?$'" in code, (
        "ap/queue.py must use a regex pre-check before casting score to numeric. "
        "NULLIF(payload->>'score','')::numeric crashes on malformed scores."
    )
    # The DRY guard must also be in execution.py for preemption candidate query.
    with open("ap/execution.py") as f:
        ex_src = f.read()
    ex_code = "\n".join(ln for ln in ex_src.splitlines() if not ln.lstrip().startswith("#"))
    assert "~ '^-?[0-9]+(\\.[0-9]+)?$'" in ex_code, (
        "ap/execution.py preemption query must regex-guard the meta->>'score' cast."
    )


def test_no_unguarded_score_casts_remain():
    """Guard: catch any future regression to the crash-prone
    NULLIF(payload->>'score','')::numeric or NULLIF(meta->>'score','')::numeric."""
    import re
    for path in ("ap/queue.py", "ap/execution.py"):
        with open(path) as f:
            lines = f.readlines()
        code_lines = [ln for ln in lines if not ln.lstrip().startswith("#")]
        src = "".join(code_lines)
        bad = re.search(r"NULLIF\(\s*(?:payload|meta)->>'score'\s*,\s*''\s*\)\s*::\s*numeric", src)
        assert not bad, (
            f"{path} still contains the crash-prone NULLIF(...)::numeric score cast. "
            "Use the CASE ... WHEN ... ~ '^-?[0-9]+(\\.[0-9]+)?$' ... pattern instead."
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
