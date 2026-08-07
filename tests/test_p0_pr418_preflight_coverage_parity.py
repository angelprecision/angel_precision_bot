"""Focused closure for PR #418 preflight coverage and timestamp parity."""
from __future__ import annotations

import json
import os
from datetime import timezone
from urllib.parse import unquote, urlparse

# --- DB safety fence -----------------------------------------------------
#
# This file executes real DDL/DML (CREATE TABLE, DELETE, INSERT) against
# whatever DATABASE_URL resolves to. The previous `os.environ.setdefault(...)`
# only filled in a value when DATABASE_URL was unset -- it did nothing to
# protect against an already-configured DATABASE_URL pointing at staging or
# production. If that happened, this file would run destructive operations
# against the `orders` table of whatever database was configured.
#
# The guard below positively allowlists the exact local test database this
# repo's CI uses. It never silently overwrites an existing DATABASE_URL: an
# ambiguous or unsafe value raises immediately at import time, which aborts
# collection of this entire module before any fixture -- and therefore
# before any CREATE/DELETE/INSERT -- can run.
#
# Host/path validation alone is not sufficient: PostgreSQL/libpq connection
# URIs also honor connection-identity parameters carried in the query
# string (host, hostaddr, dbname, port, service, servicefile, ...). libpq
# resolves the *effective* connection target from these, which can silently
# override a hostname/dbname that already passed the scheme/host/path
# checks below. e.g. a URL with hostname=localhost and path=intelligence_test
# but a `?host=remote.example.com` query parameter would parse as "local"
# under urlparse() alone while libpq actually connects to remote.example.com.
# The query string must therefore be validated with an equally strict
# positive allowlist, not left unexamined.

_SAFE_TEST_DB_HOSTS = {"localhost", "127.0.0.1", "::1"}
_SAFE_TEST_DB_NAME = "intelligence_test"
_SAFE_DEFAULT_DATABASE_URL = (
    "postgresql://postgres:postgres@localhost:5432/intelligence_test?sslmode=disable"
)

# Positive allowlist of query-string keys this validator will accept, and
# the exact value(s) permitted for each. Everything else -- including every
# libpq connection-identity parameter (host, hostaddr, dbname, port,
# service, servicefile, and any other key not listed here) -- is rejected.
# This is intentionally not a blacklist: an unrecognized key is unsafe by
# default, not safe until proven otherwise.
_ALLOWED_QUERY_PARAMS = {
    "sslmode": {"disable"},
}


class _UnsafeTestDatabaseError(RuntimeError):
    """Raised when DATABASE_URL does not positively identify the repo's
    known local CI/test Postgres database."""


def _parse_query_pairs_strict(query: str) -> list[tuple[str, str]]:
    """Manually parse a URL query string into (key, value) pairs.

    Deliberately does not use urllib.parse.parse_qsl(): that function's
    default behavior silently drops blank-valued pairs and silently
    keeps only one of several duplicate keys, either of which would mean
    validating a *sanitized* view of the query string rather than the
    literal one libpq will actually receive. This parser preserves every
    pair, including duplicates and blanks, so the caller can positively
    reject anything that isn't clean and expected -- it never normalizes
    an unsafe query string into something that merely looks safe.
    """
    if not query:
        return []

    pairs: list[tuple[str, str]] = []
    for component in query.split("&"):
        if not component:
            # A stray '&' (e.g. "a=b&&c=d") produces an empty component --
            # ambiguous, reject rather than silently skip.
            raise _UnsafeTestDatabaseError(
                "DATABASE_URL query string contains an empty parameter "
                "segment (stray '&') -- refusing to run DB-backed tests."
            )
        if "=" not in component:
            raise _UnsafeTestDatabaseError(
                f"DATABASE_URL query parameter {component!r} is malformed "
                "(no '=') -- refusing to run DB-backed tests."
            )
        key, _, value = component.partition("=")
        key = unquote(key)
        value = unquote(value)
        if not key:
            raise _UnsafeTestDatabaseError(
                "DATABASE_URL query string contains a blank parameter "
                "key -- refusing to run DB-backed tests."
            )
        pairs.append((key, value))
    return pairs


def _validate_database_url(raw: str) -> None:
    """Pure validation: raises _UnsafeTestDatabaseError if `raw` is not a
    positively-identified safe local test database URL. Performs no I/O
    and no environment mutation, so it is safe to call directly in tests
    against arbitrary strings.
    """
    try:
        parsed = urlparse(raw)
    except ValueError as exc:
        raise _UnsafeTestDatabaseError(
            f"DATABASE_URL is malformed and cannot be validated as safe: "
            f"{exc}"
        ) from exc

    if parsed.scheme not in ("postgres", "postgresql"):
        raise _UnsafeTestDatabaseError(
            f"DATABASE_URL scheme {parsed.scheme!r} is not a recognized "
            "Postgres URL -- refusing to run DB-backed tests."
        )

    hostname = parsed.hostname
    if hostname not in _SAFE_TEST_DB_HOSTS:
        raise _UnsafeTestDatabaseError(
            f"DATABASE_URL host {hostname!r} is not an allow-listed local "
            f"test host ({sorted(_SAFE_TEST_DB_HOSTS)!r}). Refusing to run "
            "DB-backed tests against a non-local database."
        )

    dbname = (parsed.path or "").lstrip("/")
    if dbname != _SAFE_TEST_DB_NAME:
        raise _UnsafeTestDatabaseError(
            f"DATABASE_URL database name {dbname!r} is not the expected "
            f"local test database {_SAFE_TEST_DB_NAME!r}. Refusing to run "
            "DB-backed tests against an unexpected database."
        )

    # Query-string connection-identity parameters must not be able to
    # redirect libpq to a different host/hostaddr/dbname/port/service away
    # from the host/path already validated above.
    query_pairs = _parse_query_pairs_strict(parsed.query)

    seen_keys: set[str] = set()
    for key, value in query_pairs:
        if key in seen_keys:
            raise _UnsafeTestDatabaseError(
                f"DATABASE_URL query string contains a duplicate parameter "
                f"{key!r} -- refusing to run DB-backed tests rather than "
                "guess which occurrence libpq would honor."
            )
        seen_keys.add(key)

        if key not in _ALLOWED_QUERY_PARAMS:
            raise _UnsafeTestDatabaseError(
                f"DATABASE_URL query parameter {key!r} is not on the "
                f"allowlist ({sorted(_ALLOWED_QUERY_PARAMS)!r}). Refusing "
                "to run DB-backed tests -- unrecognized connection "
                "parameters are treated as unsafe by default, including "
                "libpq connection-identity overrides (host, hostaddr, "
                "dbname, port, service, servicefile)."
            )

        allowed_values = _ALLOWED_QUERY_PARAMS[key]
        if value not in allowed_values:
            raise _UnsafeTestDatabaseError(
                f"DATABASE_URL query parameter {key}={value!r} is not an "
                f"allow-listed value ({sorted(allowed_values)!r}). "
                "Refusing to run DB-backed tests."
            )


def _assert_safe_test_database_url() -> str:
    """Resolve and validate DATABASE_URL for this test module.

    If DATABASE_URL is unset or blank, adopts the explicit known-safe
    local default and validates that value like any other input (no
    bypass). If DATABASE_URL is already set, it is never overwritten --
    it is validated as-is, and an unsafe or ambiguous value raises before
    this module finishes importing, which prevents pytest from collecting
    any fixture or test in this file.
    """
    raw = os.environ.get("DATABASE_URL")
    if raw is None or not raw.strip():
        os.environ["DATABASE_URL"] = _SAFE_DEFAULT_DATABASE_URL
        raw = _SAFE_DEFAULT_DATABASE_URL
    _validate_database_url(raw)
    return raw


DATABASE_URL = _assert_safe_test_database_url()

import psycopg2
import pytest

import ap.selector_recovery_deploy_preflight as preflight_module
from ap.selector_recovery_deploy_preflight import (
    _classify_row,
    _fetch_candidate_rows,
    _parse_timestamp,
)

_PREFIX = "preflight-pr418-followup-"
_SIGNAL_ID = "sig-pr418-followup"
_CLIENT_ID = "preflight-pr418@example.com"


def _pg_conn():
    return psycopg2.connect(DATABASE_URL)


@pytest.fixture(scope="module", autouse=True)
def _ensure_orders_table_and_cleanup():
    with _pg_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS orders (
                    local_order_id TEXT PRIMARY KEY,
                    broker_order_id TEXT,
                    client_id TEXT,
                    position_id TEXT,
                    kind TEXT,
                    status TEXT,
                    meta JSONB DEFAULT '{}'::jsonb,
                    created_ts TIMESTAMPTZ DEFAULT NOW(),
                    updated_ts TIMESTAMPTZ DEFAULT NOW(),
                    direction TEXT,
                    contract TEXT,
                    qty INTEGER,
                    filled_qty INTEGER,
                    fill_price NUMERIC,
                    signal_id TEXT,
                    execution_mode TEXT,
                    canonical_signal_id TEXT,
                    submitted_ts TIMESTAMPTZ,
                    filled_ts TIMESTAMPTZ,
                    last_error TEXT
                )
                """
            )
            cursor.execute(
                "DELETE FROM orders WHERE local_order_id LIKE %s",
                (f"{_PREFIX}%",),
            )
        connection.commit()
    yield
    with _pg_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM orders WHERE local_order_id LIKE %s",
                (f"{_PREFIX}%",),
            )
        connection.commit()


def _provenance(local_order_id: str) -> dict:
    return {
        "canonical_signal_id": _SIGNAL_ID,
        "client_id": _CLIENT_ID,
        "execution_mode": "paper",
        "local_order_id": local_order_id,
    }


def _row(local_order_id: str, meta: dict) -> dict:
    return {
        "local_order_id": local_order_id,
        "client_id": _CLIENT_ID,
        "execution_mode": "paper",
        "signal_id": _SIGNAL_ID,
        "canonical_signal_id": _SIGNAL_ID,
        "status": "PENDING_TRIGGER",
        "meta": dict(meta),
        "updated_ts": None,
    }


def _insert(local_order_id: str, meta: dict) -> None:
    with _pg_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM orders WHERE local_order_id = %s",
                (local_order_id,),
            )
            cursor.execute(
                """
                INSERT INTO orders (
                    local_order_id,
                    client_id,
                    kind,
                    status,
                    signal_id,
                    canonical_signal_id,
                    execution_mode,
                    meta
                )
                VALUES (%s, %s, 'ENTRY', 'PENDING_TRIGGER', %s, %s, 'paper', %s::jsonb)
                """,
                (
                    local_order_id,
                    _CLIENT_ID,
                    _SIGNAL_ID,
                    _SIGNAL_ID,
                    json.dumps(meta),
                ),
            )
        connection.commit()


def _fetch_one(local_order_id: str) -> dict:
    rows = _fetch_candidate_rows()
    matching = [row for row in rows if row["local_order_id"] == local_order_id]
    assert len(matching) == 1
    return matching[0]


def test_provenance_only_row_is_fetched_and_rejected():
    local_order_id = f"{_PREFIX}provenance-only"
    _insert(
        local_order_id,
        {"trigger_crossed_at_provenance": _provenance(local_order_id)},
    )

    result = _classify_row(_fetch_one(local_order_id))

    assert result["safe"] is False
    assert "TRIGGER_CROSSED_TIMESTAMP_MISSING" in result["findings"]


def test_timestamp_only_row_is_fetched_and_rejected():
    local_order_id = f"{_PREFIX}timestamp-only"
    _insert(local_order_id, {"trigger_crossed_at": "2026-08-06T20:00:00+00:00"})

    result = _classify_row(_fetch_one(local_order_id))

    assert result["safe"] is False
    assert "TRIGGER_PROVENANCE_MISSING" in result["findings"]


@pytest.mark.parametrize(
    ("suffix", "timestamp", "expected_safe"),
    [
        ("timestamp-null", None, True),
        ("timestamp-empty", "", False),
        ("timestamp-whitespace", "   ", False),
        ("timestamp-false", False, False),
        ("timestamp-zero", 0, False),
    ],
)
def test_present_timestamp_key_is_fetched_and_classified_by_value(
    suffix,
    timestamp,
    expected_safe,
):
    local_order_id = f"{_PREFIX}{suffix}"
    _insert(local_order_id, {"trigger_crossed_at": timestamp})

    result = _classify_row(_fetch_one(local_order_id))

    assert result["safe"] is expected_safe
    if timestamp is None:
        assert "TRIGGER_CROSSED_TIMESTAMP_MALFORMED" not in result["findings"]
        assert "TRIGGER_PROVENANCE_MISSING" not in result["findings"]
    else:
        assert "TRIGGER_CROSSED_TIMESTAMP_MALFORMED" in result["findings"]
        assert "TRIGGER_PROVENANCE_MISSING" in result["findings"]


@pytest.mark.parametrize(
    ("suffix", "provenance"),
    [
        ("provenance-null", None),
        ("provenance-empty", ""),
        ("provenance-false", False),
        ("provenance-zero", 0),
        ("provenance-dict-empty", {}),
    ],
)
def test_present_provenance_key_is_fetched_regardless_of_value(
    suffix,
    provenance,
):
    local_order_id = f"{_PREFIX}{suffix}"
    _insert(local_order_id, {"trigger_crossed_at_provenance": provenance})

    result = _classify_row(_fetch_one(local_order_id))

    assert result["safe"] is False
    assert "TRIGGER_CROSSED_TIMESTAMP_MISSING" in result["findings"]
    assert "TRIGGER_PROVENANCE_INCOMPLETE" in result["findings"]


def test_both_trigger_claim_keys_absent_remains_outside_candidate_query():
    local_order_id = f"{_PREFIX}ordinary"
    _insert(local_order_id, {"unrelated_field": "ordinary-pending-trigger"})

    rows = _fetch_candidate_rows()

    assert all(row["local_order_id"] != local_order_id for row in rows)


def test_timezone_naive_trigger_timestamp_is_malformed():
    local_order_id = f"{_PREFIX}naive"
    result = _classify_row(
        _row(
            local_order_id,
            {
                "trigger_crossed_at": "2026-08-06T20:00:00",
                "trigger_crossed_at_provenance": _provenance(local_order_id),
            },
        )
    )

    assert result["safe"] is False
    assert "TRIGGER_CROSSED_TIMESTAMP_MALFORMED" in result["findings"]


def test_timezone_aware_trigger_timestamp_remains_valid():
    local_order_id = f"{_PREFIX}aware"
    result = _classify_row(
        _row(
            local_order_id,
            {
                "trigger_crossed_at": "2026-08-06T20:00:00+00:00",
                "trigger_crossed_at_provenance": _provenance(local_order_id),
            },
        )
    )

    assert result["safe"] is True
    assert result["findings"] == []


# Lease and retry timestamps intentionally retain the pre-#419 compatibility
# behavior. Only trigger-crossing evidence requires an explicit timezone.
@pytest.mark.parametrize(
    ("finding", "raw"),
    [
        ("LEASE_TIMESTAMP_MALFORMED", "2026-08-06T20:00:00"),
        ("RETRY_TIMESTAMP_MALFORMED", "2026-08-06T20:05:00"),
    ],
)
def test_naive_non_trigger_timestamps_preserve_legacy_utc_normalization(
    finding,
    raw,
):
    findings: list[str] = []

    parsed = _parse_timestamp(
        raw,
        finding=finding,
        findings=findings,
    )

    assert parsed is not None
    assert parsed.tzinfo is timezone.utc
    assert findings == []


# --- Post-#419 review findings: mutual exclusivity + whitespace parity ---
#
# These close two blocking findings raised against d520c96 (PR #419):
#
# 1. With provenance present, a falsy-but-present trigger_crossed_at value
#    (blank string, False, or 0) must produce exactly one of MISSING or
#    MALFORMED, never both. Blank counts as MISSING; a non-blank falsy
#    value (False, 0) counts as MALFORMED.
# 2. The preflight parser must accept the same padded/tabbed timestamps
#    the production runtime (ap_entry_watcher._parse_trigger_crossed_at)
#    accepts, since it strips whitespace before parsing.


@pytest.mark.parametrize(
    ("suffix", "timestamp", "expect_missing", "expect_malformed"),
    [
        ("paired-blank-empty", "", True, False),
        ("paired-blank-whitespace", "   ", True, False),
        ("paired-false", False, False, True),
        ("paired-zero", 0, False, True),
    ],
)
def test_missing_and_malformed_are_mutually_exclusive_with_provenance(
    suffix,
    timestamp,
    expect_missing,
    expect_malformed,
):
    """A row with BOTH keys present must never receive both diagnostics."""
    local_order_id = f"{_PREFIX}{suffix}"
    _insert(
        local_order_id,
        {
            "trigger_crossed_at": timestamp,
            "trigger_crossed_at_provenance": _provenance(local_order_id),
        },
    )

    result = _classify_row(_fetch_one(local_order_id))
    findings = result["findings"]

    has_missing = "TRIGGER_CROSSED_TIMESTAMP_MISSING" in findings
    has_malformed = "TRIGGER_CROSSED_TIMESTAMP_MALFORMED" in findings

    assert has_missing is expect_missing, findings
    assert has_malformed is expect_malformed, findings
    assert not (has_missing and has_malformed), (
        f"MISSING and MALFORMED both present for timestamp={timestamp!r}: "
        f"{findings}"
    )


def test_blank_timestamp_without_provenance_is_consistent_across_forms():
    """Empty-string and whitespace-only blanks must classify identically."""
    empty_id = f"{_PREFIX}blank-noprov-empty"
    ws_id = f"{_PREFIX}blank-noprov-whitespace"
    _insert(empty_id, {"trigger_crossed_at": ""})
    _insert(ws_id, {"trigger_crossed_at": "   "})

    empty_findings = _classify_row(_fetch_one(empty_id))["findings"]
    ws_findings = _classify_row(_fetch_one(ws_id))["findings"]

    assert "TRIGGER_CROSSED_TIMESTAMP_MALFORMED" in empty_findings
    assert "TRIGGER_CROSSED_TIMESTAMP_MALFORMED" in ws_findings
    assert set(empty_findings) == set(ws_findings)


@pytest.mark.parametrize(
    "raw",
    [
        " 2026-08-06T20:00:00+00:00 ",
        "\t2026-08-06T20:00:00Z\n",
    ],
)
def test_padded_trigger_timestamp_matches_production_parity(raw):
    """Preflight must accept what ap_entry_watcher._parse_trigger_crossed_at
    accepts: production strips whitespace before parsing, so padded/tabbed
    ISO strings are valid trigger evidence, not malformed."""
    local_order_id = f"{_PREFIX}padded-{hash(raw) & 0xffff}"
    _insert(
        local_order_id,
        {
            "trigger_crossed_at": raw,
            "trigger_crossed_at_provenance": _provenance(local_order_id),
        },
    )

    result = _classify_row(_fetch_one(local_order_id))

    assert "TRIGGER_CROSSED_TIMESTAMP_MALFORMED" not in result["findings"], (
        result["findings"]
    )


def test_padded_trigger_timestamp_parses_via_parse_timestamp_directly():
    """Unit-level confirmation of the strict (require_timezone=True) path,
    independent of the DB-backed classifier fixture above."""
    findings: list[str] = []

    parsed = _parse_timestamp(
        " 2026-08-06T20:00:00+00:00 ",
        finding="TRIGGER_CROSSED_TIMESTAMP_MALFORMED",
        findings=findings,
        require_timezone=True,
    )

    assert findings == []
    assert parsed is not None
    assert parsed.tzinfo is not None


# --- Regression coverage for the DB safety fence itself ------------------
#
# Proves the guard fails closed before any mutating SQL in this module can
# run, and that it never silently overwrites an ambiguous/unsafe
# pre-existing DATABASE_URL.


class TestDatabaseSafetyFence:
    @pytest.mark.parametrize(
        "url",
        [
            "postgresql://postgres:postgres@localhost:5432/intelligence_test?sslmode=disable",
            "postgresql://postgres:postgres@127.0.0.1:5432/intelligence_test?sslmode=disable",
        ],
    )
    def test_allowlisted_local_test_database_is_accepted(self, url):
        _validate_database_url(url)  # must not raise

    def test_remote_host_is_rejected(self):
        with pytest.raises(_UnsafeTestDatabaseError):
            _validate_database_url(
                "postgresql://user:pass@remote-host.example.com:5432/intelligence_test"
            )

    def test_supabase_style_host_is_rejected(self):
        with pytest.raises(_UnsafeTestDatabaseError):
            _validate_database_url(
                "postgresql://postgres:pw@db.abcprojectref.supabase.co:5432/postgres"
            )

    def test_localhost_with_wrong_database_name_is_rejected(self):
        with pytest.raises(_UnsafeTestDatabaseError):
            _validate_database_url(
                "postgresql://postgres:postgres@localhost:5432/postgres"
            )

    def test_malformed_database_url_is_rejected(self):
        with pytest.raises(_UnsafeTestDatabaseError):
            _validate_database_url("not a url at all :::")

    def test_missing_database_url_adopts_explicit_local_default(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)

        resolved = _assert_safe_test_database_url()

        assert resolved == _SAFE_DEFAULT_DATABASE_URL
        assert os.environ["DATABASE_URL"] == _SAFE_DEFAULT_DATABASE_URL

    def test_blank_database_url_adopts_explicit_local_default(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "   ")

        resolved = _assert_safe_test_database_url()

        assert resolved == _SAFE_DEFAULT_DATABASE_URL

    def test_preexisting_unsafe_database_url_is_not_overwritten(self, monkeypatch):
        """An unsafe pre-existing value must be rejected, not silently
        replaced with the safe default and then used anyway."""
        unsafe = "postgresql://user:pass@db.someproj.supabase.co:5432/postgres"
        monkeypatch.setenv("DATABASE_URL", unsafe)

        with pytest.raises(_UnsafeTestDatabaseError):
            _assert_safe_test_database_url()

        assert os.environ["DATABASE_URL"] == unsafe

    def test_rejection_occurs_before_any_database_connection_is_attempted(
        self, monkeypatch
    ):
        """The guard must raise on validation alone -- it must never reach
        psycopg2.connect (and therefore never reach CREATE/DELETE/INSERT)
        for an unsafe URL."""
        unsafe = (
            "postgresql://user:pass@remote-host.example.com:5432/intelligence_test"
        )
        monkeypatch.setenv("DATABASE_URL", unsafe)

        def _fail_if_called(*args, **kwargs):
            raise AssertionError(
                "psycopg2.connect was called despite a failed DB safety check"
            )

        monkeypatch.setattr(psycopg2, "connect", _fail_if_called)

        with pytest.raises(_UnsafeTestDatabaseError):
            _assert_safe_test_database_url()


# --- Regression coverage: query-string connection-identity bypass --------
#
# urlparse()'s hostname/path alone are not sufficient: libpq also honors
# connection-identity parameters (host, hostaddr, dbname, port, service,
# servicefile, ...) carried in the query string, which can redirect the
# *effective* connection target away from an already-validated-safe
# hostname/database. A URL can look local under host/path validation alone
# while libpq actually connects elsewhere. This class proves the query
# string itself is now validated against an equally strict allowlist.


class TestDatabaseSafetyFenceQueryParameterBypass:
    @pytest.mark.parametrize(
        ("label", "url"),
        [
            (
                "host",
                "postgresql://postgres:postgres@localhost:5432/"
                "intelligence_test?host=remote.example.com",
            ),
            (
                "hostaddr",
                "postgresql://postgres:postgres@localhost:5432/"
                "intelligence_test?hostaddr=8.8.8.8",
            ),
            (
                "dbname",
                "postgresql://postgres:postgres@localhost:5432/"
                "intelligence_test?dbname=postgres",
            ),
            (
                "port",
                "postgresql://postgres:postgres@localhost:5432/"
                "intelligence_test?port=6543",
            ),
            (
                "service",
                "postgresql://postgres:postgres@localhost:5432/"
                "intelligence_test?service=production",
            ),
            (
                "servicefile",
                "postgresql://postgres:postgres@localhost:5432/"
                "intelligence_test?servicefile=/tmp/pg_service.conf",
            ),
            (
                "unknown-parameter",
                "postgresql://postgres:postgres@localhost:5432/"
                "intelligence_test?some_parameter=value",
            ),
        ],
    )
    def test_connection_identity_query_override_is_rejected(self, label, url):
        with pytest.raises(_UnsafeTestDatabaseError):
            _validate_database_url(url)

    def test_duplicate_query_keys_are_rejected_even_when_values_match(self):
        """Duplicate keys must be rejected outright, never resolved by
        silently picking the first or last occurrence -- the caller must
        not guess which value libpq would actually honor."""
        with pytest.raises(_UnsafeTestDatabaseError):
            _validate_database_url(
                "postgresql://postgres:postgres@localhost:5432/"
                "intelligence_test?sslmode=disable&sslmode=disable"
            )

    def test_stray_ampersand_produces_empty_segment_and_is_rejected(self):
        with pytest.raises(_UnsafeTestDatabaseError):
            _validate_database_url(
                "postgresql://postgres:postgres@localhost:5432/"
                "intelligence_test?sslmode=disable&&host=evil.example.com"
            )

    def test_query_parameter_with_no_equals_sign_is_rejected(self):
        with pytest.raises(_UnsafeTestDatabaseError):
            _validate_database_url(
                "postgresql://postgres:postgres@localhost:5432/"
                "intelligence_test?sslmode"
            )

    def test_blank_query_key_is_rejected(self):
        with pytest.raises(_UnsafeTestDatabaseError):
            _validate_database_url(
                "postgresql://postgres:postgres@localhost:5432/"
                "intelligence_test?=disable"
            )

    def test_sslmode_with_unapproved_value_is_rejected(self):
        with pytest.raises(_UnsafeTestDatabaseError):
            _validate_database_url(
                "postgresql://postgres:postgres@localhost:5432/"
                "intelligence_test?sslmode=require"
            )

    def test_known_ci_url_with_sslmode_disable_remains_accepted(self):
        _validate_database_url(
            "postgresql://postgres:postgres@localhost:5432/"
            "intelligence_test?sslmode=disable"
        )  # must not raise

    def test_url_with_no_query_parameters_remains_accepted(self):
        _validate_database_url(
            "postgresql://postgres:postgres@localhost:5432/intelligence_test"
        )  # must not raise

    def test_query_override_fails_before_any_database_connection_is_attempted(
        self, monkeypatch
    ):
        """One representative query-string bypass case (host override),
        proving the same fail-before-connect invariant already established
        for host/dbname bypasses also holds for query-string bypasses."""
        unsafe = (
            "postgresql://postgres:postgres@localhost:5432/"
            "intelligence_test?host=remote.example.com"
        )
        monkeypatch.setenv("DATABASE_URL", unsafe)

        def _fail_if_called(*args, **kwargs):
            raise AssertionError(
                "psycopg2.connect was called despite a failed DB safety "
                "check (query-string host override)"
            )

        monkeypatch.setattr(psycopg2, "connect", _fail_if_called)

        with pytest.raises(_UnsafeTestDatabaseError):
            _assert_safe_test_database_url()

        assert os.environ["DATABASE_URL"] == unsafe


def test_candidate_fetch_executes_select_only(monkeypatch):
    class _Result:
        @staticmethod
        def fetchall():
            return []

    class _Connection:
        query = ""

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def execute(self, query):
            self.query = query
            normalized = " ".join(query.upper().split())
            assert normalized.startswith("SELECT ")
            for forbidden in (" INSERT ", " UPDATE ", " DELETE ", " MERGE "):
                assert forbidden not in f" {normalized} "
            assert "META ? 'TRIGGER_CROSSED_AT'" in normalized
            assert "META ? 'TRIGGER_CROSSED_AT_PROVENANCE'" in normalized
            return _Result()

    connection = _Connection()
    monkeypatch.setattr(preflight_module, "conn", lambda: connection)

    assert _fetch_candidate_rows() == []
    assert connection.query
