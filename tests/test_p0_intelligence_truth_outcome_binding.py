"""PR #432 — exact intelligence truth and canonical outcome binding.

The semantic cases use the real PostgreSQL table shapes.  Tests that do not
need a database still run locally; PostgreSQL cases are enabled by the P0
workflow's disposable PostgreSQL 17 service.
"""
from __future__ import annotations

import json
import os
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import pytest

os.environ.setdefault("DATABASE_URL", os.getenv("INTELLIGENCE_POSTGRES_TEST_URL", "postgresql://test:test@127.0.0.1:5432/intelligence_test"))
os.environ.setdefault("PGSSLMODE", "disable")

from ap import counterfactual_tracker  # noqa: E402
from ap import intelligence_context_worker as worker  # noqa: E402
from ap import intelligence_outcome_binding as binding  # noqa: E402
from ap import performance_tracker  # noqa: E402
from ap import schema_attestation  # noqa: E402


REPO_ROOT = Path(__file__).resolve().parents[1]
POSTGRES_URL = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL", "")


def _row_value(row, key: str, index: int = 0):
    if isinstance(row, dict):
        return row.get(key)
    return row[index]


@contextmanager
def _postgres_conn():
    psycopg2 = pytest.importorskip("psycopg2")
    extras = pytest.importorskip("psycopg2.extras")
    connection = psycopg2.connect(POSTGRES_URL)
    cursor = connection.cursor(cursor_factory=extras.RealDictCursor)
    try:
        yield cursor
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


def _apply_postgres_prerequisites() -> None:
    with _postgres_conn() as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS public.proof_trades (
                id BIGSERIAL PRIMARY KEY,
                client_email TEXT,
                position_id TEXT,
                local_order_id TEXT,
                canonical_signal_id TEXT,
                signal_id TEXT,
                execution_mode TEXT,
                mode TEXT,
                performance_taxonomy TEXT,
                training_eligible BOOLEAN,
                official_live_performance_eligible BOOLEAN,
                ticker TEXT,
                contract TEXT,
                opened_at TIMESTAMPTZ,
                closed_at TIMESTAMPTZ
            )
            """
        )
        for column, sql_type in (
            ("client_email", "TEXT"),
            ("local_order_id", "TEXT"),
            ("canonical_signal_id", "TEXT"),
            ("signal_id", "TEXT"),
            ("execution_mode", "TEXT"),
            ("mode", "TEXT"),
            ("performance_taxonomy", "TEXT"),
            ("training_eligible", "BOOLEAN"),
            ("official_live_performance_eligible", "BOOLEAN"),
        ):
            c.execute(
                f"ALTER TABLE public.proof_trades ADD COLUMN IF NOT EXISTS {column} {sql_type}"
            )
        c.execute((REPO_ROOT / "migrations/20260712_intelligence_context_snapshots.sql").read_text())
        repair = (REPO_ROOT / "migrations/20260812_intelligence_truth_binding_repair.sql").read_text()
        c.execute(repair)
        c.execute(repair)


@pytest.fixture()
def pg_scope(monkeypatch):
    if not POSTGRES_URL:
        pytest.skip("INTELLIGENCE_POSTGRES_TEST_URL is not configured")
    _apply_postgres_prerequisites()
    monkeypatch.setattr(binding, "_db_conn", lambda: _postgres_conn)
    monkeypatch.setattr(binding, "_run_with_retry", lambda fn: fn())
    client_id = f"pr432-{uuid4().hex[:12]}@example.com"
    yield client_id
    with _postgres_conn() as c:
        c.execute("DELETE FROM ap_intelligence_outcome_bindings WHERE client_id=%s", (client_id,))
        c.execute("DELETE FROM ap_intelligence_jobs WHERE client_id=%s", (client_id,))
        c.execute("DELETE FROM ap_intelligence_snapshots WHERE client_id=%s", (client_id,))
        c.execute("DELETE FROM proof_trades WHERE client_email=%s", (client_id,))


def _insert_snapshot(
    client_id: str,
    execution_mode: str,
    *,
    canonical_signal_id: str = "canon-432",
    signal_id: str = "signal-432",
    local_order_id: str = "",
    phase: str = "PREOPEN",
    parent_snapshot_id: str | None = None,
    input_hash: str | None = None,
    config_hash: str = "config-432",
) -> str:
    snapshot_id = str(uuid4())
    with _postgres_conn() as c:
        c.execute(
            """
            INSERT INTO ap_intelligence_snapshots (
                id, client_id, execution_mode, canonical_signal_id, signal_id,
                local_order_id, phase, context_revision, profile_version,
                parent_snapshot_id, input_hash, config_hash, git_commit,
                data_as_of, status, payload
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,1,%s,%s,%s,%s,%s,now(),'COMPLETE',%s::jsonb)
            """,
            (
                snapshot_id,
                client_id,
                execution_mode,
                canonical_signal_id,
                signal_id,
                local_order_id,
                phase,
                "profile-432",
                parent_snapshot_id,
                input_hash or f"input-{snapshot_id}",
                config_hash,
                "main-432",
                json.dumps({"feature": "pre-entry", "future_candle": 999, "observe_only": True}),
            ),
        )
    return snapshot_id


def _insert_proof(
    client_id: str,
    local_order_id: str,
    *,
    execution_mode: str = "live",
    mode: str = "live",
    taxonomy: str = "LIVE_OFFICIAL",
    training_eligible: bool = True,
    official_live_performance_eligible: bool = True,
    ticker: str = "SPY",
    contract: str = "SPY260821C00500000",
) -> int:
    with _postgres_conn() as c:
        c.execute(
            """
            INSERT INTO proof_trades (
                client_email, position_id, local_order_id, canonical_signal_id,
                signal_id, execution_mode, mode, performance_taxonomy,
                training_eligible, official_live_performance_eligible,
                ticker, contract, opened_at, closed_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now())
            RETURNING id
            """,
            (
                client_id,
                f"position-{uuid4().hex[:10]}",
                local_order_id,
                "canon-432",
                "signal-432",
                execution_mode,
                mode,
                taxonomy,
                training_eligible,
                official_live_performance_eligible,
                ticker,
                contract,
            ),
        )
        return int(_row_value(c.fetchone(), "id"))


def _binding_rows(client_id: str):
    with _postgres_conn() as c:
        c.execute(
            "SELECT * FROM ap_intelligence_outcome_bindings WHERE client_id=%s ORDER BY id",
            (client_id,),
        )
        return [dict(row) for row in c.fetchall()]


def _insert_binding_row(
    snapshot_id: str,
    proof_trade_id: int,
    *,
    client_id: str,
    execution_mode: str,
    originating_local_order_id: str,
    phase: str,
    input_hash: str,
    binding_method: str = binding.DIRECT_BINDING_METHOD,
) -> None:
    with _postgres_conn() as c:
        c.execute(
            """
            INSERT INTO ap_intelligence_outcome_bindings (
                snapshot_id, proof_trade_id, client_id, execution_mode,
                originating_local_order_id, canonical_signal_id, phase,
                profile_version, input_hash, config_hash, binding_method,
                binding_version
            ) VALUES (%s,%s,%s,%s,%s,'canon-432',%s,'profile-432',%s,
                      'config-432',%s,'p0-432-v1')
            """,
            (
                snapshot_id,
                proof_trade_id,
                client_id,
                execution_mode,
                originating_local_order_id,
                phase,
                input_hash,
                binding_method,
            ),
        )


def test_normalization_and_taxonomy_are_strict_without_database():
    assert binding.normalize_client_id("  USER@EXAMPLE.COM ") == "user@example.com"
    assert binding.normalize_client_id(" Client-A ") == "Client-A"
    assert binding.normalize_execution_mode(" LIVE ") == "live"
    assert binding.normalize_execution_mode("sandbox") == ""
    assert binding._classify_proof_row(
        {
            "client_email": "client@example.com",
            "local_order_id": "entry-1",
            "execution_mode": "live",
            "mode": "paper",
        },
        client_id="client@example.com",
        execution_mode="live",
        local_order_id="entry-1",
    )["disposition"] == "MODE_CONFLICT"


def test_conflicting_secondary_client_candidate_holds_without_binding():
    rows = [
        {
            "id": 1,
            "client_email": "client@example.com",
            "client_id": "client@example.com",
            "local_order_id": "entry-1",
            "execution_mode": "live",
            "mode": "live",
            "performance_taxonomy": "LIVE_OFFICIAL",
            "training_eligible": True,
            "official_live_performance_eligible": True,
        },
        {
            "id": 2,
            "client_email": "client@example.com",
            "client_id": "other@example.com",
            "local_order_id": "entry-1",
            "execution_mode": "live",
            "mode": "live",
            "performance_taxonomy": "LIVE_OFFICIAL",
            "training_eligible": True,
            "official_live_performance_eligible": True,
        },
    ]

    result = binding._resolve_direct_proof(
        {
            "snapshot_id": "snapshot-1",
            "client_id": "client@example.com",
            "execution_mode": "live",
            "local_order_id": "entry-1",
        },
        rows,
    )

    assert result["disposition"] == "CLIENT_CONFLICT"
    assert result["bound"] is False


def test_migration_is_new_idempotent_and_has_no_transaction_control():
    migration = (REPO_ROOT / "migrations/20260812_intelligence_truth_binding_repair.sql").read_text()
    assert "BEGIN;" not in migration.upper()
    assert "COMMIT;" not in migration.upper()
    assert "CREATE TABLE IF NOT EXISTS public.blocked_signal_counterfactuals" in migration
    assert "CREATE TABLE IF NOT EXISTS public.ap_intelligence_outcome_bindings" in migration
    assert "UNIQUE (snapshot_id)" in migration
    assert "proof_trade_id BIGINT NOT NULL" in migration
    assert "ALTER TABLE public.blocked_signal_counterfactuals ENABLE ROW LEVEL SECURITY" in migration
    assert "ALTER TABLE public.ap_intelligence_outcome_bindings ENABLE ROW LEVEL SECURITY" in migration
    assert "REVOKE ALL ON public.blocked_signal_counterfactuals, public.ap_intelligence_outcome_bindings FROM anon" in migration
    assert "REVOKE ALL ON public.blocked_signal_counterfactuals, public.ap_intelligence_outcome_bindings FROM authenticated" in migration
    binding_sql = migration.split(
        "CREATE TABLE IF NOT EXISTS public.ap_intelligence_outcome_bindings", 1
    )[1]
    assert "realized_pnl" not in binding_sql
    assert "hypothetical_r" not in binding_sql


def test_schema_attestation_declares_the_intelligence_contract():
    required = schema_attestation.INTELLIGENCE_REQUIRED_SCHEMA
    assert {
        "ap_intelligence_snapshots",
        "blocked_signal_counterfactuals",
        "ap_intelligence_outcome_bindings",
    } <= set(required)
    assert {"id", "client_id", "input_hash", "config_hash", "payload"} <= required["ap_intelligence_snapshots"]
    assert {"snapshot_id", "proof_trade_id", "originating_local_order_id", "binding_version"} <= required["ap_intelligence_outcome_bindings"]
    assert {
        "id", "client_email", "local_order_id", "execution_mode", "mode",
        "performance_taxonomy", "training_eligible",
        "official_live_performance_eligible",
    } <= required["proof_trades"]
    assert {"signal_id", "execution_mode", "meta", "hypothetical_r"} <= required["blocked_signal_counterfactuals"]
    assert "trade_performance" not in schema_attestation.REQUIRED_SCHEMA
    assert "ap_intelligence_snapshots" not in schema_attestation.REQUIRED_SCHEMA
    assert callable(schema_attestation.attest_intelligence_schema)


def test_exact_live_snapshot_binds_only_live_official_proof(pg_scope):
    client_id = pg_scope
    proof_id = _insert_proof(client_id, "entry-live-1")
    snapshot_id = _insert_snapshot(client_id, "live", local_order_id="entry-live-1")

    result = binding.bind_snapshot_to_proof(snapshot_id)

    assert result["ok"] is True
    assert result["disposition"] == "BOUND"
    assert int(result["proof_trade_id"]) == proof_id
    assert result["training_eligible"] is True
    rows = _binding_rows(client_id)
    assert len(rows) == 1
    assert int(rows[0]["proof_trade_id"]) == proof_id
    assert rows[0]["binding_method"] == binding.DIRECT_BINDING_METHOD


@pytest.mark.parametrize(
    ("taxonomy", "training", "official"),
    [
        ("LIVE_UNRECONCILED", False, False),
        ("UNKNOWN_QUARANTINED", False, False),
    ],
)
def test_weaker_live_taxonomies_never_bind(pg_scope, taxonomy, training, official):
    client_id = pg_scope
    _insert_proof(
        client_id,
        "entry-live-weak",
        taxonomy=taxonomy,
        training_eligible=training,
        official_live_performance_eligible=official,
    )
    snapshot_id = _insert_snapshot(client_id, "live", local_order_id="entry-live-weak")

    result = binding.bind_snapshot_to_proof(snapshot_id)

    assert result["disposition"] == "PROOF_TAXONOMY_INELIGIBLE"
    assert _binding_rows(client_id) == []


def test_exact_paper_binds_as_research_only_and_never_live_truth(pg_scope):
    client_id = pg_scope
    _insert_proof(
        client_id,
        "entry-paper-1",
        execution_mode="paper",
        mode="paper",
        taxonomy="PAPER_UNVERIFIED",
        training_eligible=False,
        official_live_performance_eligible=False,
    )
    snapshot_id = _insert_snapshot(client_id, "paper", local_order_id="entry-paper-1")

    result = binding.bind_snapshot_to_proof(snapshot_id)

    assert result["disposition"] == "BOUND"
    assert result["research_only"] is True
    assert result["training_eligible"] is False
    assert _binding_rows(client_id)[0]["execution_mode"] == "paper"


def test_exact_local_order_separates_same_ticker_and_reused_contract(pg_scope):
    client_id = pg_scope
    first_proof = _insert_proof(client_id, "entry-a", ticker="SPY", contract="SPY260821C00500000")
    second_proof = _insert_proof(client_id, "entry-b", ticker="SPY", contract="SPY260821C00500000")
    first_snapshot = _insert_snapshot(client_id, "live", local_order_id="entry-a")
    second_snapshot = _insert_snapshot(client_id, "live", local_order_id="entry-b")

    summary = binding.reconcile_intelligence_outcome_bindings(
        client_id=client_id, execution_mode="live", limit=50
    )

    assert summary["ok"] is True
    assert summary["bound"] == 2
    rows = _binding_rows(client_id)
    assert {str(row["snapshot_id"]): int(row["proof_trade_id"]) for row in rows} == {
        first_snapshot: first_proof,
        second_snapshot: second_proof,
    }


def test_cross_client_signal_family_stays_unbound(pg_scope):
    client_id = pg_scope
    other_client = f"other-{uuid4().hex[:10]}@example.com"
    _insert_proof(other_client, "entry-cross-client")
    snapshot_id = _insert_snapshot(client_id, "live", local_order_id="entry-cross-client")

    result = binding.bind_snapshot_to_proof(snapshot_id)

    assert result["disposition"] == "CLIENT_CONFLICT"
    assert _binding_rows(client_id) == []
    with _postgres_conn() as c:
        c.execute("DELETE FROM proof_trades WHERE client_email=%s", (other_client,))


def test_existing_binding_revalidates_exact_proof_before_already_bound(pg_scope):
    client_id = pg_scope
    other_client = f"other-{uuid4().hex[:10]}@example.com"
    proof_id = _insert_proof(
        other_client,
        "entry-existing",
        execution_mode="paper",
        mode="paper",
        taxonomy="PAPER_UNVERIFIED",
        training_eligible=False,
        official_live_performance_eligible=False,
    )
    snapshot_id = _insert_snapshot(client_id, "live", local_order_id="entry-existing")
    _insert_binding_row(
        snapshot_id,
        proof_id,
        client_id=client_id,
        execution_mode="live",
        originating_local_order_id="entry-existing",
        phase="PREOPEN",
        input_hash=f"input-{snapshot_id}",
    )

    try:
        result = binding.bind_snapshot_to_proof(snapshot_id)
        assert result["disposition"] == "CLIENT_CONFLICT"
        assert result["bound"] is False
    finally:
        with _postgres_conn() as c:
            c.execute("DELETE FROM proof_trades WHERE id=%s", (proof_id,))


def test_existing_binding_rechecks_full_current_proof_set(pg_scope):
    client_id = pg_scope
    first_proof_id = _insert_proof(client_id, "entry-existing-ambiguous")
    snapshot_id = _insert_snapshot(client_id, "live", local_order_id="entry-existing-ambiguous")

    first = binding.bind_snapshot_to_proof(snapshot_id)
    assert first["disposition"] == "BOUND"

    second_proof_id = _insert_proof(client_id, "entry-existing-ambiguous")
    result = binding.bind_snapshot_to_proof(snapshot_id)

    assert result["disposition"] == "PROOF_AMBIGUOUS"
    assert result["ok"] is False
    assert result["bound"] is False
    assert {int(row["proof_trade_id"]) for row in _binding_rows(client_id)} == {first_proof_id}
    assert second_proof_id != first_proof_id


def test_conflicting_insert_race_holds_without_overwrite(pg_scope):
    client_id = pg_scope
    attempted_proof_id = _insert_proof(client_id, "entry-conflicting-race")
    existing_proof_id = _insert_proof(client_id, "entry-conflicting-race")
    snapshot_id = _insert_snapshot(client_id, "live", local_order_id="entry-conflicting-race")
    _insert_binding_row(
        snapshot_id,
        existing_proof_id,
        client_id=client_id,
        execution_mode="live",
        originating_local_order_id="entry-conflicting-race",
        phase="PREOPEN",
        input_hash=f"input-{snapshot_id}",
    )

    with _postgres_conn() as c:
        c.execute("SELECT * FROM ap_intelligence_snapshots WHERE id=%s", (snapshot_id,))
        snapshot_identity = binding._snapshot_identity(dict(c.fetchone()))
        c.execute("SELECT * FROM proof_trades WHERE id=%s", (attempted_proof_id,))
        attempted_proof = dict(c.fetchone())
        result = binding._insert_binding(
            c,
            snapshot_identity=snapshot_identity,
            proof_row=attempted_proof,
            binding_method=binding.DIRECT_BINDING_METHOD,
        )

    assert result["disposition"] == "BINDING_CONFLICT"
    assert result["ok"] is False
    assert result["bound"] is False
    assert {int(row["proof_trade_id"]) for row in _binding_rows(client_id)} == {existing_proof_id}


def test_existing_child_binding_cannot_authorize_parent_lineage(pg_scope):
    client_id = pg_scope
    other_client = f"other-{uuid4().hex[:10]}@example.com"
    proof_id = _insert_proof(
        other_client,
        "entry-lineage-existing",
        execution_mode="paper",
        mode="paper",
        taxonomy="PAPER_UNVERIFIED",
        training_eligible=False,
        official_live_performance_eligible=False,
    )
    parent_id = _insert_snapshot(client_id, "live", phase="PRETRIGGER", local_order_id="")
    child_id = _insert_snapshot(
        client_id,
        "live",
        phase="PREOPEN",
        local_order_id="entry-lineage-existing",
        parent_snapshot_id=parent_id,
    )
    _insert_binding_row(
        child_id,
        proof_id,
        client_id=client_id,
        execution_mode="live",
        originating_local_order_id="entry-lineage-existing",
        phase="PREOPEN",
        input_hash=f"input-{child_id}",
    )

    try:
        result = binding.bind_snapshot_to_proof(parent_id)
        assert result["disposition"] == "PARENT_LINEAGE_UNPROVEN"
        with _postgres_conn() as c:
            c.execute(
                "SELECT 1 FROM ap_intelligence_outcome_bindings WHERE snapshot_id=%s",
                (parent_id,),
            )
            assert c.fetchone() is None
    finally:
        with _postgres_conn() as c:
            c.execute("DELETE FROM proof_trades WHERE id=%s", (proof_id,))


def test_existing_child_binding_rechecks_full_proof_set_for_parent_lineage(pg_scope):
    client_id = pg_scope
    first_proof_id = _insert_proof(client_id, "entry-lineage-ambiguous")
    parent_id = _insert_snapshot(client_id, "live", phase="PRETRIGGER", local_order_id="")
    child_id = _insert_snapshot(
        client_id,
        "live",
        phase="PREOPEN",
        local_order_id="entry-lineage-ambiguous",
        parent_snapshot_id=parent_id,
    )
    _insert_binding_row(
        child_id,
        first_proof_id,
        client_id=client_id,
        execution_mode="live",
        originating_local_order_id="entry-lineage-ambiguous",
        phase="PREOPEN",
        input_hash=f"input-{child_id}",
    )
    _insert_proof(client_id, "entry-lineage-ambiguous")

    result = binding.bind_snapshot_to_proof(parent_id)

    assert result["disposition"] == "PARENT_LINEAGE_UNPROVEN"
    with _postgres_conn() as c:
        c.execute(
            "SELECT 1 FROM ap_intelligence_outcome_bindings WHERE snapshot_id=%s",
            (parent_id,),
        )
        assert c.fetchone() is None


def test_missing_or_conflicting_proof_mode_holds(pg_scope):
    client_id = pg_scope
    _insert_proof(
        client_id,
        "entry-missing-mode",
        mode="live",
        execution_mode=None,
        taxonomy="LIVE_OFFICIAL",
    )
    missing_mode_snapshot = _insert_snapshot(client_id, "live", local_order_id="entry-missing-mode")
    missing_result = binding.bind_snapshot_to_proof(missing_mode_snapshot)
    assert missing_result["disposition"] == "PROOF_TAXONOMY_INELIGIBLE"

    _insert_proof(
        client_id,
        "entry-conflict-mode",
        execution_mode="live",
        mode="paper",
        taxonomy="LIVE_OFFICIAL",
    )
    conflict_snapshot = _insert_snapshot(client_id, "live", local_order_id="entry-conflict-mode")
    conflict_result = binding.bind_snapshot_to_proof(conflict_snapshot)
    assert conflict_result["disposition"] == "MODE_CONFLICT"
    assert _binding_rows(client_id) == []


def test_duplicate_exact_proofs_are_ambiguous(pg_scope):
    client_id = pg_scope
    _insert_proof(client_id, "entry-duplicate")
    _insert_proof(client_id, "entry-duplicate")
    snapshot_id = _insert_snapshot(client_id, "live", local_order_id="entry-duplicate")

    result = binding.bind_snapshot_to_proof(snapshot_id)

    assert result["disposition"] == "PROOF_AMBIGUOUS"
    assert _binding_rows(client_id) == []


def test_missing_local_order_never_uses_signal_or_contract_fallback(pg_scope):
    client_id = pg_scope
    _insert_proof(client_id, "entry-only-signal")
    snapshot_id = _insert_snapshot(client_id, "paper", local_order_id="", phase="PREOPEN")

    result = binding.bind_snapshot_to_proof(snapshot_id)

    assert result["disposition"] == "LOCAL_ORDER_ID_MISSING"
    assert _binding_rows(client_id) == []


def test_pretrigger_binds_only_through_one_exact_descendant_lineage(pg_scope):
    client_id = pg_scope
    proof_id = _insert_proof(client_id, "entry-lineage")
    parent_id = _insert_snapshot(client_id, "live", phase="PRETRIGGER", local_order_id="")
    child_id = _insert_snapshot(
        client_id,
        "live",
        phase="PREOPEN",
        local_order_id="entry-lineage",
        parent_snapshot_id=parent_id,
    )

    result = binding.bind_snapshot_to_proof(parent_id)

    assert result["disposition"] == "BOUND"
    assert result["binding_method"] == binding.LINEAGE_BINDING_METHOD
    rows = _binding_rows(client_id)
    assert {str(row["snapshot_id"]) for row in rows} == {parent_id, child_id}
    assert {int(row["proof_trade_id"]) for row in rows} == {proof_id}


def test_pretrigger_with_two_descendant_economic_proofs_stays_unbound(pg_scope):
    client_id = pg_scope
    first_proof = _insert_proof(client_id, "entry-lineage-a")
    second_proof = _insert_proof(client_id, "entry-lineage-b")
    parent_id = _insert_snapshot(client_id, "live", phase="PRETRIGGER", local_order_id="")
    _insert_snapshot(
        client_id,
        "live",
        phase="PREOPEN",
        local_order_id="entry-lineage-a",
        parent_snapshot_id=parent_id,
    )
    _insert_snapshot(
        client_id,
        "live",
        phase="CONTRACT_SELECTED",
        local_order_id="entry-lineage-b",
        parent_snapshot_id=parent_id,
    )

    result = binding.bind_snapshot_to_proof(parent_id)

    assert result["disposition"] == "PARENT_LINEAGE_UNPROVEN"
    assert result["bound"] is False
    rows = _binding_rows(client_id)
    assert all(str(row["snapshot_id"]) != parent_id for row in rows)
    assert {int(row["proof_trade_id"]) for row in rows} == {first_proof, second_proof}


def test_binding_is_idempotent_and_concurrent_duplicate_attempts_create_one_row(pg_scope):
    client_id = pg_scope
    proof_id = _insert_proof(client_id, "entry-concurrent")
    snapshot_id = _insert_snapshot(client_id, "live", local_order_id="entry-concurrent")

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: binding.bind_snapshot_to_proof(snapshot_id), range(2)))

    assert {result["disposition"] for result in results} <= {"BOUND", "ALREADY_BOUND"}
    assert {int(result["proof_trade_id"]) for result in results} == {proof_id}
    assert len(_binding_rows(client_id)) == 1
    assert binding.bind_snapshot_to_proof(snapshot_id)["disposition"] == "ALREADY_BOUND"


def test_binding_does_not_mutate_snapshot_payload_or_proof_economics(pg_scope):
    client_id = pg_scope
    proof_id = _insert_proof(client_id, "entry-immutable")
    snapshot_id = _insert_snapshot(client_id, "live", local_order_id="entry-immutable", input_hash="frozen-input")
    with _postgres_conn() as c:
        c.execute("SELECT payload, input_hash, config_hash FROM ap_intelligence_snapshots WHERE id=%s", (snapshot_id,))
        before_snapshot = dict(c.fetchone())
        c.execute("SELECT * FROM proof_trades WHERE id=%s", (proof_id,))
        before_proof = dict(c.fetchone())

    assert binding.bind_snapshot_to_proof(snapshot_id)["disposition"] == "BOUND"

    with _postgres_conn() as c:
        c.execute("SELECT payload, input_hash, config_hash FROM ap_intelligence_snapshots WHERE id=%s", (snapshot_id,))
        after_snapshot = dict(c.fetchone())
        c.execute("SELECT * FROM proof_trades WHERE id=%s", (proof_id,))
        after_proof = dict(c.fetchone())
    assert after_snapshot == before_snapshot
    assert after_proof == before_proof


def test_health_reports_disabled_capture_and_not_healthy_zero_observations(pg_scope, monkeypatch):
    client_id = pg_scope
    monkeypatch.setenv("INTELLIGENCE_CONTEXT_WORKER_ENABLED", "0")
    monkeypatch.setattr(
        schema_attestation,
        "attest_intelligence_schema",
        lambda **_: {"ok": True, "skipped": False, "missing_tables": [], "missing_columns": {}},
    )
    monkeypatch.setattr(binding, "_run_with_retry", lambda fn: fn())

    report = binding.intelligence_truth_health(client_id=client_id, execution_mode="live")

    assert report["context_capture_enabled"] is False
    assert report["capture_diagnostic"] == "INTELLIGENCE_CONTEXT_CAPTURE_DISABLED"
    assert report["status"] == "CAPTURE_DISABLED"
    assert report["status"] != "HEALTHY"
    assert report["ok"] is False


def test_health_reports_enabled_pending_capture_as_degraded(pg_scope, monkeypatch):
    client_id = pg_scope
    monkeypatch.setenv("INTELLIGENCE_CONTEXT_WORKER_ENABLED", "1")
    monkeypatch.setattr(
        schema_attestation,
        "attest_intelligence_schema",
        lambda **_: {"ok": True, "skipped": False, "missing_tables": [], "missing_columns": {}},
    )
    with _postgres_conn() as c:
        c.execute(
            """
            INSERT INTO ap_intelligence_jobs (
                client_id, execution_mode, canonical_signal_id, signal_id,
                local_order_id, phase, context_revision, profile_version,
                input_hash, payload
            ) VALUES (%s,'live','canon-pending','signal-pending','',
                      'PRETRIGGER',1,'profile-432','pending-input','{}'::jsonb)
            """,
            (client_id,),
        )

    report = binding.intelligence_truth_health(client_id=client_id, execution_mode="live")

    assert report["context_capture_enabled"] is True
    assert report["jobs_pending"] == 1
    assert report["snapshot_count"] == 0
    assert report["status"] == "DEGRADED_SNAPSHOT_CAPTURE_NOT_PROGRESSING"
    assert report["ok"] is False


def test_legacy_bridge_and_tracker_perform_zero_official_mutation(monkeypatch):
    import intelligence_bridge

    monkeypatch.setattr(intelligence_bridge, "_get_audit_log", lambda: (_ for _ in ()).throw(AssertionError("audit must not be touched")))
    bridge_result = intelligence_bridge.record_trade_outcome("SPY", "signal-1", 0.25)
    assert bridge_result["disposition"] == "LEGACY_FUZZY_OUTCOME_BINDING_DISABLED"
    assert bridge_result["training_mutated"] is False

    class FailingSupabase:
        def table(self, *_args, **_kwargs):
            raise AssertionError("legacy tracker must not write Supabase")

    tracker = performance_tracker.PerformanceTracker(supabase_client=FailingSupabase())
    outcome = performance_tracker.TradeOutcome(client_id="client@example.com", position_id="position-1")
    tracker.record_outcome(outcome)
    assert tracker.stats()["total_trades"] == 1


def test_malformed_counterfactual_mode_performs_zero_insert():
    class NoWriteDB:
        def __init__(self):
            self.calls = 0

        def __call__(self):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args, **_kwargs):
            self.calls += 1
            raise AssertionError("malformed mode must be rejected before SQL")

    db = NoWriteDB()
    result = counterfactual_tracker.track_counterfactual_signal(
        signal={"signal_id": "signal-1", "ticker": "SPY", "side": "CALL"},
        client_id="client@example.com",
        execution_mode="",
        block_stage="blocked",
        block_reason="test",
        conn_factory=db,
    )
    assert result is False
    assert db.calls == 0


def test_worker_does_not_reconcile_when_capture_is_disabled(monkeypatch):
    monkeypatch.setenv("INTELLIGENCE_CONTEXT_STORE_BACKEND", "memory")
    monkeypatch.setenv("INTELLIGENCE_CONTEXT_WORKER_ENABLED", "0")
    from ap.intelligence_context_materializer import enqueue_pretrigger_context
    from ap.intelligence_snapshot_store import _reset_memory_store_for_tests

    _reset_memory_store_for_tests()
    enqueue_pretrigger_context(
        {"signal_id": "signal-worker", "canonical_signal_id": "canon-worker", "ticker": "SPY"},
        client_id="worker@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-worker",
    )
    called = []
    monkeypatch.setattr(worker, "reconcile_intelligence_outcome_bindings", lambda **kwargs: called.append(kwargs))

    result = worker.process_due_intelligence_jobs_once(
        client_id="worker@example.com", execution_mode="PAPER", claim_owner="worker-test"
    )

    assert result["completed"] == 1
    assert result["reconciliation"]["disabled"] is True
    assert called == []


def test_worker_reconciles_only_when_capture_is_enabled(monkeypatch):
    monkeypatch.setenv("INTELLIGENCE_CONTEXT_STORE_BACKEND", "memory")
    monkeypatch.setenv("INTELLIGENCE_CONTEXT_WORKER_ENABLED", "1")
    from ap.intelligence_context_materializer import enqueue_pretrigger_context
    from ap.intelligence_snapshot_store import _reset_memory_store_for_tests

    _reset_memory_store_for_tests()
    enqueue_pretrigger_context(
        {
            "signal_id": "signal-worker-enabled",
            "canonical_signal_id": "canon-worker-enabled",
            "ticker": "SPY",
        },
        client_id="worker-enabled@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-worker-enabled",
    )
    called = []
    monkeypatch.setattr(
        worker,
        "reconcile_intelligence_outcome_bindings",
        lambda **kwargs: called.append(kwargs) or {"ok": True, "processed": 0},
    )

    result = worker.process_due_intelligence_jobs_once(
        client_id="worker-enabled@example.com",
        execution_mode="PAPER",
        claim_owner="worker-enabled-test",
        limit=5,
    )

    assert result["completed"] == 1
    assert result["reconciliation"]["ok"] is True
    assert called == [
        {
            "client_id": "worker-enabled@example.com",
            "execution_mode": "PAPER",
            "limit": 5,
        }
    ]
