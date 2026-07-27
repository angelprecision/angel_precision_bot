"""Install optional production trade-lifecycle hardening guards.

Each guard is isolated in its own module and may be absent on branches where its
PR has not merged yet. Import failures are logged and never make ``import ap``
fail. Individual guard installers are idempotent.
"""
from __future__ import annotations

import importlib
import logging

from ap.db import conn, run_with_retry

log = logging.getLogger("ap.trade_lifecycle_guards")

_GUARDS: tuple[tuple[str, str, str, bool], ...] = (
    ("canonical_exit_fill_truth", "ap.exit_fill_truth_guard", "install_exit_fill_truth_guard", True),
    ("exit_decision_idempotency", "ap.exit_decision_idempotency_guard", "install_exit_decision_idempotency_guard", True),
    ("proof_taxonomy", "ap.proof_taxonomy_guard", "install_proof_taxonomy_guard", True),
    ("proof_taxonomy_fill_bridge", "ap.proof_taxonomy_fill_bridge", "install_proof_taxonomy_fill_bridge", True),
    ("one_contract_policy", "ap.one_contract_exit_guard", "install_one_contract_exit_guard", False),
    (
        "touched_profit_bid_confirmation",
        "ap.touched_profit_confirmation_guard",
        "install_touched_profit_confirmation_guard",
        True,
    ),
    ("partial_exit_ownership", "ap.partial_exit_ownership_guard", "install_partial_exit_ownership_guard", True),
    ("ambiguous_position_resolution", "ap.exit_position_ambiguity_guard", "install_exit_position_ambiguity_guard", True),
)

_LAST_INSTALLATION_MANIFEST: dict[str, dict[str, str | bool]] = {}


def install_trade_lifecycle_guards() -> dict[str, dict[str, str | bool]]:
    """Install present guards and return exact deployment attestation."""
    manifest: dict[str, dict[str, str | bool]] = {}
    # Schema attestation FIRST: guards fence money paths with SQL that
    # assumes specific tables/columns. Record the schema truth alongside the
    # guard truth so the deployment manifest states both. Non-raising here
    # regardless of mode (guard install must never crash `import ap`); the
    # LIVE fail-closed enforcement lives in client_runner._run_live_preflight.
    try:
        from ap.schema_attestation import attest_schema

        _schema_report = attest_schema(strict=False)
        manifest["_schema_attestation"] = {
            "name": "_schema_attestation",
            "module": "ap.schema_attestation",
            "installer": "attest_schema",
            "required_when_present": True,
            "status": "installed" if _schema_report.get("ok") else "failed",
            "detail": str({
                "missing_tables": _schema_report.get("missing_tables"),
                "missing_columns": _schema_report.get("missing_columns"),
                "skipped": _schema_report.get("skipped"),
                "error": _schema_report.get("error"),
            }),
        }
    except Exception as exc:  # pragma: no cover — attestation must not break import
        manifest["_schema_attestation"] = {
            "name": "_schema_attestation",
            "module": "ap.schema_attestation",
            "installer": "attest_schema",
            "required_when_present": True,
            "status": "attestation_error",
            "detail": str(exc),
        }
        log.error("schema attestation errored during guard install: %s", exc)
    for guard_name, module_name, installer_name, required in _GUARDS:
        record: dict[str, str | bool] = {
            "name": guard_name,
            "module": module_name,
            "installer": installer_name,
            "required_when_present": required,
        }
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            if exc.name == module_name:
                record["status"] = "absent"
                manifest[guard_name] = record
                continue
            record.update(status="import_failed", error=str(exc))
            manifest[guard_name] = record
            log.error("lifecycle guard dependency unavailable module=%s error=%s", module_name, exc)
            continue
        except Exception as exc:
            record.update(status="import_failed", error=str(exc))
            manifest[guard_name] = record
            log.error("lifecycle guard import failed module=%s error=%s", module_name, exc)
            continue

        try:
            installer = getattr(module, installer_name)
            installer()
            record["status"] = "installed"
        except Exception as exc:
            record.update(status="installation_failed", error=str(exc))
            log.exception("lifecycle guard install failed module=%s installer=%s error=%s", module_name, installer_name, exc)
        manifest[guard_name] = record

    _LAST_INSTALLATION_MANIFEST.clear()
    _LAST_INSTALLATION_MANIFEST.update(manifest)
    return {name: dict(record) for name, record in manifest.items()}


def _generation_claims_table_exists() -> bool:
    def _read() -> bool:
        with conn() as c:
            row = c.execute(
                "SELECT to_regclass('public.exit_decision_generation_claims') AS relation"
            ).fetchone()
            return bool(dict(row).get("relation")) if row else False

    return bool(run_with_retry(_read))


def lifecycle_guard_preflight(execution_mode: str) -> tuple[bool, dict]:
    """Fail LIVE deployment when present required guards or migration are absent."""
    mode = str(execution_mode or "").strip().lower()
    manifest = install_trade_lifecycle_guards()
    missing = [
        name
        for name, record in manifest.items()
        if record.get("required_when_present") is True
        and record.get("status") not in {"installed", "absent"}
    ]
    migration_ok = False
    migration_error = ""
    try:
        migration_ok = _generation_claims_table_exists()
    except Exception as exc:
        migration_error = str(exc)

    diagnostics = {
        "mode": mode,
        "manifest": manifest,
        "missing_required_guards": missing,
        "generation_claims_table_exists": migration_ok,
        "migration_error": migration_error,
    }
    if mode == "live" and (missing or not migration_ok):
        log.critical(
            "LIVE_LIFECYCLE_GUARD_PREFLIGHT_FAILED missing=%s migration_exists=%s migration_error=%s",
            missing,
            migration_ok,
            migration_error,
        )
        return False, diagnostics
    if mode != "live" and (missing or not migration_ok):
        log.error(
            "PAPER_LIFECYCLE_GUARD_DIAGNOSTIC missing=%s migration_exists=%s migration_error=%s",
            missing,
            migration_ok,
            migration_error,
        )
    return True, diagnostics
