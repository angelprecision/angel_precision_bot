"""Ordered, recorded, idempotent SQL migration runner.

Incident this prevents (2026-07-17/18 audit):
    Migrations shipped in merged PRs (#361, #362) were applied by hand —
    and only partially. One PR's migration (#360's positions columns) was
    applied while two others from the same deploy window were not, leaving
    production schema silently behind deployed code.

Design (deliberately conservative for a hand-migrated history):
    * Ledger table ``schema_migrations`` (filename PK, sha256 checksum,
      applied_at, applied_by, execution mode) — created idempotently.
    * ``baseline()`` marks every existing file in ``migrations/`` as
      applied WITHOUT executing anything. This adopts the current
      hand-applied production state exactly once; it never runs SQL.
    * ``run_pending(apply=True)`` executes unrecorded files in
      deterministic filename order, one file per transaction, recording
      each. Mixed legacy naming (``2026_05_17_*`` vs ``20260717_*``) sorts
      digits-first consistently via a normalized sort key.
    * Checksum drift: if a RECORDED file's content changed on disk, the
      runner refuses to proceed (fail loudly — history must be immutable).
    * Startup auto-apply is OFF by default. ``ENABLE_MIGRATION_RUNNER=1``
      enables ``run_pending_on_startup()``; until then this module is a
      CLI/operator tool. A one-file rollout can use ``dry-run --only FILE`` /
      ``apply --only FILE`` only after an existing ledger proves the base
      migration set was adopted.
    * Migration files executed by this runner must not contain top-level
      transaction-control statements. The runner owns the transaction and
      atomically records the migration ledger entry before commit. Historical
      hand-applied migrations may be adopted through ``baseline()`` without
      execution.

The pairing contract with ap/schema_attestation.py: attestation tells you
the schema is behind; this runner is the sanctioned way to catch it up.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger("ap.migration_runner")

MIGRATIONS_DIR = Path(os.getenv("MIGRATIONS_DIR", "migrations"))

_LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename    TEXT PRIMARY KEY,
    checksum    TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_by  TEXT NOT NULL DEFAULT 'migration_runner',
    bot_mode    TEXT,
    baselined   BOOLEAN NOT NULL DEFAULT FALSE
)
"""


class MigrationChecksumDrift(RuntimeError):
    """A recorded migration file changed on disk after being applied."""


class MigrationTransactionControlError(RuntimeError):
    """Pending migration files contain top-level transaction-control SQL."""


class MigrationLedgerRequired(RuntimeError):
    """A targeted migration was requested before history was baselined."""


class MigrationTargetError(RuntimeError):
    """A targeted migration cannot be proven to be the only safe pending file."""


class BaselineAttestationError(RuntimeError):
    """Schema attestation failed before baseline could record any ledger rows.

    Raised when ``baseline()`` is called without ``force=True`` and the
    production database does not satisfy ``REQUIRED_SCHEMA``, or when the
    database cannot be reached.  Zero ``schema_migrations`` rows are written
    before this exception is raised.
    """


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _sort_key(filename: str) -> tuple:
    """Normalize mixed naming so chronology survives the sort.

    ``2026_05_17_foo.sql`` → (20260517, full name)
    ``20260717_bar.sql``   → (20260717, full name)
    Files with no leading date sort last, alphabetically.
    """
    m = re.match(r"^(\d{4})_?(\d{2})_?(\d{2})", filename)
    if m:
        return (0, int("".join(m.groups())), filename)
    return (1, 0, filename)


_TXN_CONTROL_KEYWORDS = ("BEGIN", "COMMIT", "ROLLBACK", "START TRANSACTION")


def _read_dollar_tag(sql: str, index: int) -> tuple[str, int] | None:
    """Return (tag, end_index) for a PostgreSQL dollar-quote opener."""
    if index >= len(sql) or sql[index] != "$":
        return None
    end = sql.find("$", index + 1)
    if end < 0:
        return None
    tag_body = sql[index + 1:end]
    if tag_body and not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", tag_body):
        return None
    return sql[index:end + 1], end + 1


def _strip_sql_comments_and_literals(sql: str) -> str:
    """Mask comments, quoted strings, and dollar-quoted blocks with spaces.

    The migration runner only rejects top-level transaction-control statements.
    Words inside comments, SQL strings, or PostgreSQL procedural DO $$ blocks
    are not statements and must not trigger the guard.
    """
    out = list(sql)
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        prev = sql[i - 1] if i > 0 else ""

        if ch == "-" and nxt == "-":
            j = i + 2
            while j < n and sql[j] not in "\r\n":
                j += 1
            for k in range(i, j):
                out[k] = " "
            i = j
            continue

        if ch == "/" and nxt == "*":
            depth = 1
            j = i + 2
            while j + 1 < n and depth:
                if sql[j] == "/" and sql[j + 1] == "*":
                    depth += 1
                    j += 2
                    continue
                if sql[j] == "*" and sql[j + 1] == "/":
                    depth -= 1
                    j += 2
                    continue
                j += 1
            j = min(n, j)
            for k in range(i, j):
                out[k] = " "
            i = j
            continue

        if ch in ("E", "e") and nxt == "'" and not (
            prev.isalnum() or prev in ("_", "$")
        ):
            j = i + 2
            while j < n:
                if sql[j] == "\\":
                    j += 2
                    continue
                if sql[j] == "'":
                    if j + 1 < n and sql[j + 1] == "'":
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            for k in range(i, min(j, n)):
                out[k] = " "
            i = j
            continue

        if ch == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'":
                    if j + 1 < n and sql[j + 1] == "'":
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            for k in range(i, min(j, n)):
                out[k] = " "
            i = j
            continue

        if ch == '"':
            j = i + 1
            while j < n:
                if sql[j] == '"':
                    if j + 1 < n and sql[j + 1] == '"':
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            for k in range(i, min(j, n)):
                out[k] = " "
            i = j
            continue

        dollar = _read_dollar_tag(sql, i)
        if dollar:
            tag, body_start = dollar
            close = sql.find(tag, body_start)
            j = n if close < 0 else close + len(tag)
            for k in range(i, j):
                out[k] = " "
            i = j
            continue

        i += 1
    return "".join(out)


def _top_level_transaction_control(sql: str) -> list[str]:
    """Return prohibited top-level transaction-control statements in *sql*."""
    masked = _strip_sql_comments_and_literals(sql)
    statements: list[str] = []
    for raw_stmt in masked.split(";"):
        stmt = raw_stmt.strip()
        if not stmt:
            continue
        compact = re.sub(r"\s+", " ", stmt).upper()
        for keyword in _TXN_CONTROL_KEYWORDS:
            if compact == keyword or compact.startswith(f"{keyword} "):
                statements.append(keyword)
                break
    return statements


def _migration_files(directory: Path | None = None) -> list[Path]:
    directory = directory or MIGRATIONS_DIR
    if not directory.is_dir():
        return []
    return sorted(
        (p for p in directory.iterdir() if p.suffix.lower() == ".sql" and p.is_file()),
        key=lambda p: _sort_key(p.name),
    )


def _ensure_ledger() -> None:
    from ap.db import conn, run_with_retry

    def _create() -> None:
        with conn() as c:
            c.execute(_LEDGER_DDL)

    run_with_retry(_create)


def _recorded() -> dict[str, dict[str, Any]]:
    from ap.db import conn, run_with_retry

    def _read() -> dict[str, dict[str, Any]]:
        with conn() as c:
            rows = c.execute(
                "SELECT filename, checksum, applied_at, baselined FROM schema_migrations"
            ).fetchall()
        return {str(dict(r)["filename"]): dict(r) for r in rows}

    return run_with_retry(_read)


def _ledger_exists() -> bool:
    """Read ledger existence without creating it."""
    from ap.db import conn, run_with_retry

    def _read() -> bool:
        with conn() as c:
            row = c.execute(
                "SELECT to_regclass('schema_migrations') AS ledger_name"
            ).fetchone()
        if not row:
            return False
        if isinstance(row, dict):
            return bool(row.get("ledger_name"))
        try:
            return bool(row[0])
        except (IndexError, TypeError, KeyError):
            return False

    return bool(run_with_retry(_read))


def _require_existing_ledger() -> None:
    if not _ledger_exists():
        raise MigrationLedgerRequired(
            "Migration apply refused: schema_migrations does not exist. "
            "Baseline the deployed/base migration set first; this command "
            "will not create a ledger or replay historical migrations."
        )


def _validate_migration_target(raw_target: str) -> str:
    target = str(raw_target or "").strip()
    if (
        not target
        or Path(target).name != target
        or Path(target).suffix.lower() != ".sql"
    ):
        raise MigrationTargetError(
            f"Invalid migration target {raw_target!r}; pass one filename ending in .sql"
        )
    return target


def _record(filename: str, checksum: str, *, baselined: bool) -> None:
    from ap.db import conn, run_with_retry
    bot_mode = os.getenv("BOT_MODE", os.getenv("MODE", "PAPER")).strip().upper()

    def _write() -> None:
        with conn() as c:
            c.execute(
                "INSERT INTO schema_migrations (filename, checksum, bot_mode, baselined) "
                "VALUES (%s,%s,%s,%s) ON CONFLICT (filename) DO NOTHING",
                (filename, checksum, bot_mode, baselined),
            )

    run_with_retry(_write)


def status(
    directory: Path | None = None,
    *,
    ensure_ledger: bool = True,
) -> dict[str, Any]:
    """Report pending / recorded / drifted files.

    The historical default creates the ledger for compatibility. Targeted
    apply/dry-run uses ``ensure_ledger=False`` after explicitly proving that
    the ledger already exists.
    """
    if ensure_ledger:
        _ensure_ledger()
    recorded = _recorded()
    files = _migration_files(directory)
    pending, applied, drifted = [], [], []
    for path in files:
        checksum = _sha256(path.read_text(encoding="utf-8"))
        row = recorded.get(path.name)
        if row is None:
            pending.append(path.name)
        elif str(row.get("checksum") or "") != checksum:
            drifted.append(path.name)
        else:
            applied.append(path.name)
    return {
        "pending": pending,
        "applied": applied,
        "drifted": drifted,
        "orphan_records": sorted(set(recorded) - {p.name for p in files}),
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


def baseline(directory: Path | None = None, *, force: bool = False) -> dict[str, Any]:
    """Mark every current file as applied WITHOUT executing SQL.

    One-time adoption of the hand-applied production history.  Idempotent:
    already-recorded files are untouched.

    Before inserting any ledger rows, runs strict schema attestation against
    ``REQUIRED_SCHEMA``.  If attestation fails, detects missing schema, or
    cannot reach the database: zero ``schema_migrations`` rows are written
    and ``BaselineAttestationError`` is raised.  This prevents the exact
    incident this module was built to prevent: marking migrations as applied
    when production does not actually have that schema.

    ``force=True`` bypasses attestation and emits a CRITICAL log warning.
    Use only for disaster recovery when you know the production database is
    already correct but attestation is unavailable.  Force is never the
    default.
    """
    _ensure_ledger()

    if force:
        log.critical(
            "MIGRATION_BASELINE_FORCE — schema attestation bypassed. "
            "The schema_migrations ledger may not reflect actual database schema. "
            "Use only when you have independently verified production schema correctness."
        )
    else:
        from ap.schema_attestation import (  # late import — keeps module import-safe
            SchemaAttestationError as _SAError,
            attest_schema,
        )
        try:
            attest_schema(strict=True)
        except _SAError as exc:
            raise BaselineAttestationError(
                f"Baseline refused: schema attestation failed — "
                f"zero ledger rows written. "
                f"Apply all pending migrations before running baseline. "
                f"Detail: {exc}"
            ) from exc

    recorded = _recorded()
    marked = []
    for path in _migration_files(directory):
        if path.name in recorded:
            continue
        _record(path.name, _sha256(path.read_text(encoding="utf-8")), baselined=True)
        marked.append(path.name)
    log.info("MIGRATION_BASELINE marked=%d %s", len(marked), marked)
    return {"baselined": marked}


def run_pending(
    *,
    apply: bool = False,
    directory: Path | None = None,
    only: str | None = None,
) -> dict[str, Any]:
    """Apply unrecorded migrations in deterministic order.

    ``apply=False`` (default) is a dry run: reports what WOULD run.
    Refuses to run anything while checksum drift exists on recorded files.
    Each file executes in its own transaction via ``ap.db.conn()`` and is
    recorded only after successful commit. Stops at the first failure.
    """
    if only is None:
        if apply:
            _require_existing_ledger()
        report = status(directory)
    else:
        target = _validate_migration_target(only)
        _require_existing_ledger()
        directory = directory or MIGRATIONS_DIR
        available = {path.name for path in _migration_files(directory)}
        if target not in available:
            raise MigrationTargetError(
                f"Targeted migration is not present in {directory}: {target}"
            )
        report = status(directory, ensure_ledger=False)
        if target not in report["pending"]:
            raise MigrationTargetError(
                f"Targeted migration is not pending: {target}"
            )
        other_pending = [name for name in report["pending"] if name != target]
        if other_pending:
            raise MigrationTargetError(
                "Targeted migration refused because other files are pending: "
                f"{other_pending}. Baseline the deployed/base set and retry."
            )
        report["pending"] = [target]
    if report["drifted"]:
        raise MigrationChecksumDrift(
            f"Recorded migration files changed on disk: {report['drifted']}. "
            "Migration history must be immutable — investigate before applying anything."
        )
    result: dict[str, Any] = {"would_apply": report["pending"], "applied": [], "failed": None}
    if not apply or not report["pending"]:
        return result

    from ap.db import conn

    bot_mode = os.getenv("BOT_MODE", os.getenv("MODE", "PAPER")).strip().upper()
    directory = directory or MIGRATIONS_DIR

    offenders: dict[str, list[str]] = {}
    for filename in report["pending"]:
        path = directory / filename
        sql_text = path.read_text(encoding="utf-8")
        statements = _top_level_transaction_control(sql_text)
        if statements:
            offenders[filename] = statements
    if offenders:
        detail = ", ".join(
            f"{filename}: {sorted(set(statements))}"
            for filename, statements in sorted(offenders.items())
        )
        log.critical(
            "MIGRATION_REFUSED_TXN_CONTROL offenders=%s — migration runner "
            "must own the transaction so SQL and schema_migrations ledger "
            "record commit atomically",
            offenders,
        )
        raise MigrationTransactionControlError(
            "Pending migrations contain top-level transaction-control statements; "
            "zero migration bodies executed and zero ledger rows inserted. "
            "Remove BEGIN, COMMIT, ROLLBACK, or START TRANSACTION from migration "
            f"files. Offenders: {detail}"
        )

    for filename in report["pending"]:
        path = directory / filename
        sql_text = path.read_text(encoding="utf-8")
        checksum = _sha256(sql_text)
        try:
            # Migration SQL and ledger INSERT share one connection and one
            # transaction.  Both commit together or both roll back together.
            # Calling _record() after conn() closes is unsafe: a crash between
            # commit and _record() leaves the migration applied but unrecorded,
            # causing it to re-execute on the next run.
            with conn() as c:
                c.execute(sql_text)
                c.execute(
                    "INSERT INTO schema_migrations "
                    "(filename, checksum, bot_mode, baselined) "
                    "VALUES (%s,%s,%s,%s) ON CONFLICT (filename) DO NOTHING",
                    (filename, checksum, bot_mode, False),
                )
            result["applied"].append(filename)
            log.info("MIGRATION_APPLIED %s", filename)
        except Exception as exc:  # noqa: BLE001 — stop, report, do not continue past failure
            result["failed"] = {"filename": filename, "error": str(exc)}
            log.critical("MIGRATION_FAILED %s error=%s — stopping run", filename, exc)
            break
    return result


def run_pending_on_startup() -> dict[str, Any] | None:
    """Startup hook — no-op unless ENABLE_MIGRATION_RUNNER=1."""
    if os.getenv("ENABLE_MIGRATION_RUNNER", "0").strip().lower() in ("", "0", "false", "no"):
        return None
    return run_pending(apply=True)


def _main(argv: list[str]) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    cmd = argv[1] if len(argv) > 1 else "status"
    if cmd == "status":
        print(status())
    elif cmd == "baseline":
        force = "--force" in argv[2:]
        print(baseline(force=force))
    elif cmd == "apply":
        only = None
        if "--only" in argv[2:]:
            index = argv.index("--only")
            if index + 1 >= len(argv):
                print("usage: python -m ap.migration_runner apply [--only FILE]")
                return 2
            only = argv[index + 1]
        print(run_pending(apply=True, only=only))
    elif cmd == "dry-run":
        only = None
        if "--only" in argv[2:]:
            index = argv.index("--only")
            if index + 1 >= len(argv):
                print("usage: python -m ap.migration_runner dry-run [--only FILE]")
                return 2
            only = argv[index + 1]
        print(run_pending(apply=False, only=only))
    else:
        print(
            "usage: python -m ap.migration_runner "
            "[status|baseline [--force]|dry-run [--only FILE]|apply [--only FILE]]"
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv))
