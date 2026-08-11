"""Startup schema attestation — verify the database matches deployed code.

Incident this prevents (2026-07-17/18 audit):
    PRs #361 and #362 merged and deployed with migrations that were never
    applied to production. ``exit_decision_generation_claims`` did not exist,
    so in LIVE mode every actionable exit decision failed its durable claim
    and was silently suppressed (log-only), while PAPER mode silently ran
    with duplicate-exit fencing disabled. The deployed system and the
    reviewed code were not the same system.

Contract:
    * ``REQUIRED_SCHEMA`` declares every table/column that installed
      lifecycle guards and the core execution path reference verbatim in
      SQL. Only columns whose absence breaks execution belong here —
      dynamically-filtered optional columns do not.
    * ``INTELLIGENCE_REQUIRED_SCHEMA`` and ``attest_intelligence_schema``
      attest the separate observe-only learning plane; they are intentionally
      not part of broker/lifecycle ``REQUIRED_SCHEMA``.
    * ``attest_schema()`` compares the declaration against
      ``information_schema`` in one query and returns a structured report.
    * Strictness: LIVE mode (``BOT_MODE``/``MODE``) raises
      ``SchemaAttestationError`` on any mismatch — a LIVE process must not
      start against a database it was not reviewed against. PAPER/SIM logs
      CRITICAL and continues. ``SCHEMA_ATTESTATION_STRICT`` (``1``/``0``)
      overrides in either direction; ``SCHEMA_ATTESTATION_ENABLED=0``
      disables entirely (break-glass only).

Wiring:
    * ``ap.trade_lifecycle_guards.install_trade_lifecycle_guards()`` runs a
      non-raising attestation first and records the report in the
      installation manifest (deployment attestation surface).
    * ``client_runner._run_live_preflight`` runs a strict attestation for
      LIVE — schema mismatch fails preflight exactly like a broker
      credential failure would.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("ap.schema_attestation")


class SchemaAttestationError(RuntimeError):
    """Deployed code requires schema the database does not have."""


# Intelligence truth is an evidence-plane contract.  Keep this declaration
# separate so health/preflight can attest the learning plane without making
# broker trading depend on a successful intelligence read.
INTELLIGENCE_REQUIRED_SCHEMA: dict[str, frozenset[str]] = {
    "ap_intelligence_snapshots": frozenset({
        "id", "client_id", "execution_mode", "canonical_signal_id", "signal_id",
        "local_order_id", "phase", "context_revision", "profile_version",
        "parent_snapshot_id", "input_hash", "config_hash", "git_commit",
        "data_as_of", "computed_at", "status", "payload", "created_at",
    }),
    "ap_intelligence_jobs": frozenset({
        "id", "client_id", "execution_mode", "canonical_signal_id", "phase",
        "status", "attempt_count", "max_attempts", "next_attempt_at",
        "snapshot_id", "created_at", "updated_at",
    }),
    # The binder reads these proof-truth fields directly.  Keep this in the
    # intelligence contract rather than broadening broker preflight with an
    # analytics-only table dependency.
    "proof_trades": frozenset({
        "id", "client_email", "local_order_id", "execution_mode", "mode",
        "performance_taxonomy", "training_eligible",
        "official_live_performance_eligible",
    }),
    "blocked_signal_counterfactuals": frozenset({
        "id", "signal_id", "canonical_signal_id", "client_id", "execution_mode",
        "ticker", "direction", "block_stage", "block_reason", "reason_code",
        "blocked_at", "entry_ref", "target_ref", "stop_ref", "resolution",
        "hypothetical_r", "resolved_at", "meta",
    }),
    "ap_intelligence_outcome_bindings": frozenset({
        "id", "snapshot_id", "proof_trade_id", "client_id", "execution_mode",
        "originating_local_order_id", "canonical_signal_id", "phase",
        "profile_version", "input_hash", "config_hash", "binding_method",
        "binding_version", "bound_at",
    }),
}


# ---------------------------------------------------------------------------
# Declaration — tables/columns referenced VERBATIM by guard / lifecycle SQL.
# Grounded against production information_schema on 2026-07-18. When a new
# PR adds a migration, it must extend this declaration in the same PR.
# ---------------------------------------------------------------------------
REQUIRED_SCHEMA: dict[str, frozenset[str]] = {
    # PR #361 — durable one-decision-per-exit-generation fence.
    # ap/exit_decision_idempotency_guard.py references every column below in
    # UPDATE/INSERT/SELECT statements; a missing column fails the claim path.
    "exit_decision_generation_claims": frozenset({
        "generation_key", "client_id", "position_id", "remaining_qty",
        "requested_qty",
        "exit_generation", "decision_action", "decision_reason_code",
        "claim_state", "local_order_id", "broker_order_id", "last_error",
        "released_at", "claimed_at",
    }),
    # PR #362 — proof performance taxonomy / LIVE learning isolation.
    "proof_trades": frozenset({
        "position_id", "client_email", "closed_at", "execution_mode",
        "official_live_performance_eligible", "performance_taxonomy",
        "training_eligible", "taxonomy_reason", "quote_domain_consistent",
        "exit_local_order_id",
    }),
    # PR #360 — durable EXIT ownership on positions (pending_exit_action /
    # pending_exit_reason are intentionally NOT required: the fill-truth
    # guard filters them dynamically via _table_columns()).
    # PR #385 — QPM persists provenance-aware hard_exit_reference into
    # positions.meta and exit-engine restart hydration reads it back verbatim.
    # Missing meta therefore breaks durable quote/risk persistence and must
    # fail LIVE preflight rather than degrade into a runtime error loop.
    "positions": frozenset({
        "id", "client_id", "contract", "status", "qty", "entry_ts",
        "execution_mode", "meta",
        "exit_in_flight", "pending_exit_qty",
        "pending_exit_local_order_id", "pending_exit_broker_order_id",
        "quantity_remaining", "contracts_exited",
    }),
    # Core order lifecycle columns referenced verbatim across OSM, exit
    # guards, reconciler, and the durable-generation reader.
    "orders": frozenset({
        "local_order_id", "broker_order_id", "client_id", "position_id",
        "kind", "status", "meta", "created_ts", "direction", "contract",
        "qty", "filled_qty", "fill_price", "signal_id", "execution_mode",
        "canonical_signal_id",
    }),
    # PR #152 — morning handoff DB-level idempotency.
    "handoff_run_locks": frozenset({
        "client_id", "trading_date", "stage", "status", "execution_mode",
        "last_run_at", "last_success_at", "last_error",
    }),
    # Queue claiming (FOR UPDATE SKIP LOCKED path + idempotent enqueue).
    "trade_queue": frozenset({
        "id", "client_id", "signal_id", "idempotency_key", "status",
        "payload", "result_json", "last_error", "created_ts",
    }),
}


def _env_flag(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() not in ("", "0", "false", "no")


def _bot_mode() -> str:
    return os.getenv("BOT_MODE", os.getenv("MODE", "PAPER")).strip().upper()


def _strict_default() -> bool:
    override = os.getenv("SCHEMA_ATTESTATION_STRICT", "").strip().lower()
    if override in ("1", "true", "yes"):
        return True
    if override in ("0", "false", "no"):
        return False
    return _bot_mode() == "LIVE"


def _fetch_actual_schema(required: dict[str, frozenset[str]]) -> dict[str, set[str]]:
    """One information_schema query for every declared table."""
    from ap.db import conn, run_with_retry

    tables = sorted(required.keys())

    def _read() -> dict[str, set[str]]:
        with conn() as c:
            rows = c.execute(
                "SELECT table_name, column_name FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name = ANY(%s)",
                (tables,),
            ).fetchall()
        actual: dict[str, set[str]] = {}
        for row in rows:
            row = dict(row)
            actual.setdefault(str(row.get("table_name") or ""), set()).add(
                str(row.get("column_name") or "")
            )
        return actual

    return run_with_retry(_read)


def attest_schema(
    *,
    strict: bool | None = None,
    required: dict[str, frozenset[str]] | None = None,
) -> dict[str, Any]:
    """Verify required schema exists. Returns a structured report.

    Report shape::
        {
          "ok": bool,
          "strict": bool,
          "mode": "LIVE"|"PAPER"|...,
          "missing_tables": [table, ...],
          "missing_columns": {table: [col, ...]},
          "checked_tables": int,
          "checked_at": iso8601,
          "skipped": bool,        # SCHEMA_ATTESTATION_ENABLED=0
          "error": str|None,      # attestation itself failed (DB unreachable)
        }

    Raises ``SchemaAttestationError`` when strict and any table/column is
    missing. An unreachable database under strict also raises — a LIVE
    process that cannot prove its schema must not trade.
    """
    required = required if required is not None else REQUIRED_SCHEMA
    strict_effective = _strict_default() if strict is None else bool(strict)
    report: dict[str, Any] = {
        "ok": True,
        "strict": strict_effective,
        "mode": _bot_mode(),
        "missing_tables": [],
        "missing_columns": {},
        "checked_tables": len(required),
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "skipped": False,
        "error": None,
    }

    if not _env_flag("SCHEMA_ATTESTATION_ENABLED", "1"):
        report["skipped"] = True
        log.critical(
            "SCHEMA_ATTESTATION_SKIPPED — SCHEMA_ATTESTATION_ENABLED=0. "
            "The deployed code's schema requirements are UNVERIFIED."
        )
        return report

    try:
        actual = _fetch_actual_schema(required)
    except Exception as exc:  # noqa: BLE001 — must classify, not crash PAPER
        report["ok"] = False
        report["error"] = f"attestation_query_failed:{exc}"
        log.critical("SCHEMA_ATTESTATION_UNAVAILABLE error=%s strict=%s", exc, strict_effective)
        if strict_effective:
            raise SchemaAttestationError(
                f"Schema attestation could not run against the database: {exc}"
            ) from exc
        return report

    for table, cols in sorted(required.items()):
        have = actual.get(table)
        if not have:
            report["missing_tables"].append(table)
            continue
        missing = sorted(cols - have)
        if missing:
            report["missing_columns"][table] = missing

    if report["missing_tables"] or report["missing_columns"]:
        report["ok"] = False
        detail = (
            f"missing_tables={report['missing_tables']} "
            f"missing_columns={report['missing_columns']}"
        )
        log.critical(
            "SCHEMA_ATTESTATION_FAILED mode=%s strict=%s %s — deployed code "
            "requires schema the database does not have. Apply the pending "
            "migrations (see ap/migration_runner.py) before trading.",
            report["mode"], strict_effective, detail,
        )
        if strict_effective:
            raise SchemaAttestationError(f"Schema attestation failed: {detail}")
    else:
        log.info(
            "SCHEMA_ATTESTATION_OK tables=%d mode=%s strict=%s",
            len(required), report["mode"], strict_effective,
        )
    return report


def attest_intelligence_schema(*, strict: bool = False) -> dict[str, Any]:
    """Attest the observe-only intelligence plane without gating trading.

    This deliberately uses a separate declaration from ``REQUIRED_SCHEMA``.
    The latter is consumed by LIVE broker/lifecycle preflight; an unavailable
    analytics plane must be visible as intelligence health degradation without
    becoming a new broker authority in PR #432.
    """
    return attest_schema(strict=strict, required=INTELLIGENCE_REQUIRED_SCHEMA)
