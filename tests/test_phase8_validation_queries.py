"""
Phase 8 tests: post-deploy validation queries are read-only.

The queries under sql/validation/*.sql are run by ops after a deploy.
They MUST contain only SELECT/WITH statements; never INSERT, UPDATE,
DELETE, ALTER, DROP, CREATE TABLE/INDEX, TRUNCATE, GRANT, REVOKE.

This test runs the same greps the README documents, so the README's
audit recipe stays a single source of truth.

Run:
    pytest tests/test_phase8_validation_queries.py -xvs
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR   = REPO_ROOT / "sql" / "validation"


def _sql_files():
    return sorted(SQL_DIR.glob("*.sql"))


# ============================================================
# 1. Directory structure exists and has the expected files
# ============================================================

class TestDirectoryStructure:
    def test_sql_dir_exists(self):
        assert SQL_DIR.exists() and SQL_DIR.is_dir(), \
            f"sql/validation/ should exist at {SQL_DIR}"

    def test_readme_exists(self):
        assert (SQL_DIR / "README.md").exists()

    def test_all_expected_files_present(self):
        names = {p.name for p in _sql_files()}
        assert names == {
            "01_phase2_adaptive_autocancel.sql",
            "02_phase3_submit_time_refresh.sql",
            "03_phase4_account_equity_sizing.sql",
            "04_phase5_post_cancel_retry.sql",
            "05_phase6_dashboard_telemetry.sql",
            "06_phase7_exit_pricing.sql",
            "99_health_summary.sql",
        }, f"got: {names}"


# ============================================================
# 2. Read-only invariants (the grep recipes from README.md)
# ============================================================

WRITE_DDL_PATTERN = re.compile(
    r"(?:^|[^A-Za-z_])(INSERT|UPDATE|DELETE|ALTER|DROP|TRUNCATE|GRANT|REVOKE)"
    r"\s+(INTO|FROM|TABLE|INDEX|VIEW|ROLE|SCHEMA|DATABASE)",
    re.IGNORECASE | re.MULTILINE,
)

CREATE_DDL_PATTERN = re.compile(
    r"CREATE\s+(TABLE|INDEX|VIEW|ROLE|SCHEMA|DATABASE|FUNCTION)",
    re.IGNORECASE | re.MULTILINE,
)


def _strip_sql_comments_and_strings(sql: str) -> str:
    """Remove SQL line comments, block comments, and string literals so
    keywords appearing only inside literals don't trigger the safety check.
    """
    # Block comments
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    # Line comments
    sql = re.sub(r"--[^\n]*", " ", sql)
    # Single-quoted strings (handle escaped '' inside)
    sql = re.sub(r"'(?:''|[^'])*'", " ", sql)
    # Dollar-quoted strings $tag$...$tag$
    sql = re.sub(r"\$([A-Za-z_]*)\$.*?\$\1\$", " ", sql, flags=re.DOTALL)
    return sql


@pytest.mark.parametrize("sql_file", _sql_files(), ids=lambda p: p.name)
def test_no_write_or_destructive_ddl(sql_file: Path):
    body = sql_file.read_text(encoding="utf-8")
    cleaned = _strip_sql_comments_and_strings(body)
    write_matches = WRITE_DDL_PATTERN.findall(cleaned)
    create_matches = CREATE_DDL_PATTERN.findall(cleaned)
    assert not write_matches, (
        f"{sql_file.name} contains write/DDL statement(s): {write_matches}. "
        f"Validation queries MUST be read-only."
    )
    assert not create_matches, (
        f"{sql_file.name} contains CREATE DDL: {create_matches}. "
        f"Validation queries MUST be read-only."
    )


# ============================================================
# 3. Every query file declares at least one SELECT
# ============================================================

@pytest.mark.parametrize("sql_file", _sql_files(), ids=lambda p: p.name)
def test_has_at_least_one_select(sql_file: Path):
    body = sql_file.read_text(encoding="utf-8")
    # Must have a SELECT keyword somewhere outside comments.
    cleaned = _strip_sql_comments_and_strings(body)
    assert re.search(r"\bSELECT\b", cleaned, re.IGNORECASE), \
        f"{sql_file.name} has no SELECT \u2014 what is it doing?"


# ============================================================
# 4. Every query file has at least one \\echo line for human readability
# ============================================================

@pytest.mark.parametrize("sql_file", _sql_files(), ids=lambda p: p.name)
def test_has_echo_line(sql_file: Path):
    body = sql_file.read_text(encoding="utf-8")
    assert "\\echo" in body, \
        f"{sql_file.name} should have at least one \\echo line for ops readability"


# ============================================================
# 5. No file accidentally references a secret-bearing table
# ============================================================

FORBIDDEN_TABLES = (
    "clients",       # holds API keys, email, etc.
    "client_state",  # holds equity reads but also kill_switch state
    "tokens",
    "secrets",
    "api_keys",
)


@pytest.mark.parametrize("sql_file", _sql_files(), ids=lambda p: p.name)
def test_no_forbidden_tables(sql_file: Path):
    body = sql_file.read_text(encoding="utf-8")
    cleaned = _strip_sql_comments_and_strings(body).lower()
    # Look only for FROM/JOIN references to forbidden tables \u2014 a literal
    # mention in a column or comment doesn't count.
    for tbl in FORBIDDEN_TABLES:
        pattern = re.compile(r"\b(FROM|JOIN)\s+" + re.escape(tbl) + r"\b",
                             re.IGNORECASE)
        # client_state is allowed in 03_phase4 (JOINing on it for mode='LIVE'
        # check); whitelist that one file for that one table.
        if tbl == "client_state" and sql_file.name == "03_phase4_account_equity_sizing.sql":
            continue
        m = pattern.search(cleaned)
        assert not m, (
            f"{sql_file.name} references forbidden table '{tbl}'. "
            f"Validation queries must avoid secret-bearing tables."
        )


# ============================================================
# 6. README documents every file
# ============================================================

def test_readme_documents_every_file():
    readme = (SQL_DIR / "README.md").read_text()
    for p in _sql_files():
        assert p.name in readme, \
            f"README.md must document {p.name}"
