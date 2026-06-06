"""
tests/test_pr81_amendments_v3.py
================================
PR81 — FINAL AMENDMENT v2 (post-review)

Covers:
  §1 Monotonic update_opportunity (status never regresses)
  §2 Migration dedupe keeps terminal truth
  §3 Production paths invoke the opportunity-ledger helpers
  §4 Gate-order rebase note present
  §5 Pre-existing acceptance criteria still hold
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
os.environ.setdefault("DATABASE_URL",
                       "postgresql://test:test@127.0.0.1:5432/test_pr81")


# ── Fake Supabase with read-back support ──────────────────────────────────────
#
# Critical: the monotonic guard READS the current status before updating.
# The fake must let us seed an existing row's status so we can prove the
# guard kicks in.

class _Chain:
    def __init__(self, store, table_name, seed):
        self._store = store
        self._table = table_name
        self._seed  = seed
        self._op    = None
        self._row   = None
        self._filters: list[tuple[str, object]] = []
        self._on_conflict = None
        self._ignore = False

    def select(self, cols):
        self._op = "select"
        self._cols = cols
        return self
    def update(self, row):
        self._op = "update"; self._row = row; return self
    def upsert(self, row, on_conflict=None, ignore_duplicates=False):
        self._op = "upsert"; self._row = row
        self._on_conflict = on_conflict
        self._ignore = ignore_duplicates
        return self
    def insert(self, row):
        self._op = "insert"; self._row = row; return self
    def eq(self, k, v):
        self._filters.append((k, v)); return self
    def in_(self, k, v):
        self._filters.append((k, tuple(v))); return self
    def gte(self, k, v):
        self._filters.append((">=", k, v)); return self
    def limit(self, n):
        return self
    def execute(self):
        self._store.setdefault(self._table, []).append({
            "op": self._op,
            "row": self._row,
            "filters": list(self._filters),
            "on_conflict": self._on_conflict,
            "ignore_duplicates": self._ignore,
        })

        class _R:
            def __init__(self, data): self.data = data

        if self._op == "select" and self._table == "client_signal_opportunities":
            # If we have a seed status for this canonical/client combo,
            # return it; else empty.
            canonical = None
            client_id = None
            for k, v in self._filters:
                if k == "canonical_signal_id":
                    canonical = v
                elif k == "client_id":
                    client_id = v
            row = self._seed.get((canonical, client_id))
            return _R([row] if row else [])
        return _R([])


class _SB:
    def __init__(self, seed=None):
        self.calls: dict[str, list] = {}
        self.seed = seed or {}  # {(canonical, client_id): {"opportunity_status": ..., ...}}
    def table(self, name):
        return _Chain(self.calls, name, self.seed)


# ── §1 Monotonic update_opportunity ──────────────────────────────────────────

class TestStatusMonotonicity:
    def test_filled_cannot_become_watcher_armed(self):
        from ap.opportunity_ledger import update_opportunity, FILLED, WATCHER_ARMED
        sb = _SB(seed={("CANON", "c@x.com"): {"opportunity_status": FILLED}})
        ok = update_opportunity(
            "SIG", "c@x.com", WATCHER_ARMED,
            canonical_signal_id="CANON", sb=sb,
        )
        # Write succeeds (HTTP-level) but opportunity_status must NOT be in patch.
        upd = [c for c in sb.calls["client_signal_opportunities"] if c["op"] == "update"][-1]
        assert "opportunity_status" not in upd["row"], (
            f"FILLED must not regress to WATCHER_ARMED; patch={upd['row']}"
        )

    def test_broker_submitted_cannot_become_order_created(self):
        from ap.opportunity_ledger import update_opportunity, BROKER_SUBMITTED, ORDER_CREATED
        sb = _SB(seed={("CANON", "c@x.com"): {"opportunity_status": BROKER_SUBMITTED}})
        update_opportunity(
            "SIG", "c@x.com", ORDER_CREATED,
            canonical_signal_id="CANON", sb=sb,
        )
        upd = [c for c in sb.calls["client_signal_opportunities"] if c["op"] == "update"][-1]
        assert "opportunity_status" not in upd["row"], (
            f"BROKER_SUBMITTED must not regress to ORDER_CREATED; patch={upd['row']}"
        )

    def test_missed_cannot_become_preflight_passed(self):
        from ap.opportunity_ledger import update_opportunity, MISSED, PREFLIGHT_PASSED
        sb = _SB(seed={("CANON", "c@x.com"): {"opportunity_status": MISSED}})
        update_opportunity(
            "SIG", "c@x.com", PREFLIGHT_PASSED,
            canonical_signal_id="CANON", sb=sb,
        )
        upd = [c for c in sb.calls["client_signal_opportunities"] if c["op"] == "update"][-1]
        assert "opportunity_status" not in upd["row"], (
            f"MISSED (terminal) must not regress to PREFLIGHT_PASSED; patch={upd['row']}"
        )

    def test_created_can_become_order_created(self):
        from ap.opportunity_ledger import update_opportunity, CREATED, ORDER_CREATED
        sb = _SB(seed={("CANON", "c@x.com"): {"opportunity_status": CREATED}})
        update_opportunity(
            "SIG", "c@x.com", ORDER_CREATED,
            canonical_signal_id="CANON", sb=sb,
        )
        upd = [c for c in sb.calls["client_signal_opportunities"] if c["op"] == "update"][-1]
        assert upd["row"].get("opportunity_status") == ORDER_CREATED

    def test_order_created_can_become_broker_submitted(self):
        from ap.opportunity_ledger import update_opportunity, ORDER_CREATED, BROKER_SUBMITTED
        sb = _SB(seed={("CANON", "c@x.com"): {"opportunity_status": ORDER_CREATED}})
        update_opportunity(
            "SIG", "c@x.com", BROKER_SUBMITTED,
            canonical_signal_id="CANON", sb=sb,
        )
        upd = [c for c in sb.calls["client_signal_opportunities"] if c["op"] == "update"][-1]
        assert upd["row"].get("opportunity_status") == BROKER_SUBMITTED

    def test_terminal_same_status_idempotent(self):
        """Re-writing the same terminal status to itself is fine."""
        from ap.opportunity_ledger import update_opportunity, FILLED
        sb = _SB(seed={("CANON", "c@x.com"): {"opportunity_status": FILLED}})
        update_opportunity(
            "SIG", "c@x.com", FILLED,
            canonical_signal_id="CANON", sb=sb,
        )
        upd = [c for c in sb.calls["client_signal_opportunities"] if c["op"] == "update"][-1]
        # Same status is allowed; patch may or may not include the status field
        # (current impl drops it if "_is_terminal and current != incoming"
        # check passes). Either way, no regression occurred.
        assert upd["row"].get("opportunity_status", FILLED) == FILLED

    def test_filled_cannot_be_replaced_by_other_terminal(self):
        """Broker-truth FILLED is absolute. Amendment v3: nothing overwrites
        FILLED, including other non-FILLED terminals."""
        from ap.opportunity_ledger import update_opportunity, FILLED, EXPIRED
        sb = _SB(seed={("CANON", "c@x.com"): {"opportunity_status": FILLED}})
        update_opportunity(
            "SIG", "c@x.com", EXPIRED,
            canonical_signal_id="CANON", sb=sb,
        )
        upd = [c for c in sb.calls["client_signal_opportunities"] if c["op"] == "update"][-1]
        assert "opportunity_status" not in upd["row"], (
            f"FILLED must win against any non-FILLED terminal; patch={upd['row']}"
        )

    def test_metadata_enrichment_survives_blocked_regression(self):
        """Even when the status update is dropped, identifier/metadata fields
        in the same call must still be written."""
        from ap.opportunity_ledger import update_opportunity, FILLED, WATCHER_ARMED
        sb = _SB(seed={("CANON", "c@x.com"): {"opportunity_status": FILLED}})
        update_opportunity(
            "SIG", "c@x.com", WATCHER_ARMED,
            canonical_signal_id="CANON",
            broker_order_id="BRK-1", position_id="POS-1",
            sb=sb,
        )
        upd = [c for c in sb.calls["client_signal_opportunities"] if c["op"] == "update"][-1]
        assert "opportunity_status" not in upd["row"]
        # Enrichment still writes
        assert upd["row"].get("broker_order_id") == "BRK-1"
        assert upd["row"].get("position_id") == "POS-1"

    def test_regression_emits_named_log_tag(self, caplog):
        import logging
        from ap.opportunity_ledger import update_opportunity, FILLED, WATCHER_ARMED
        sb = _SB(seed={("CANON", "c@x.com"): {"opportunity_status": FILLED}})
        with caplog.at_level(logging.INFO, logger="ap.opportunity_ledger"):
            update_opportunity(
                "SIG", "c@x.com", WATCHER_ARMED,
                canonical_signal_id="CANON", sb=sb,
            )
        assert "CLIENT_OPPORTUNITY_STATUS_REGRESSION_BLOCKED" in caplog.text


# ── §1: STATUS_RANK shape ─────────────────────────────────────────────────────

class TestStatusRankShape:
    def test_required_rank_values(self):
        from ap.opportunity_ledger import STATUS_RANK
        # Amendment v3 (broker truth wins): FILLED is the single highest
        # truth (100); all other terminals share rank 90 so a later
        # broker-confirmed FILLED can repair a prior false terminal miss.
        assert STATUS_RANK["CREATED"]            ==  10
        assert STATUS_RANK["CLIENT_ELIGIBLE"]    ==  20
        assert STATUS_RANK["PREFLIGHT_WARNING"]  ==  30
        assert STATUS_RANK["PREFLIGHT_PASSED"]   ==  40
        assert STATUS_RANK["ORDER_CREATED"]      ==  50
        assert STATUS_RANK["WATCHER_ARMED"]      ==  60
        assert STATUS_RANK["BROKER_SUBMITTED"]   ==  70
        assert STATUS_RANK["BROKER_ACKED"]       ==  80
        for terminal in ("WATCHER_INVALIDATED", "ENTRY_CONFIRMATION_FAILED",
                          "BROKER_REJECTED", "EXPIRED", "CANCELED", "MISSED",
                          "CLIENT_SKIPPED", "INTERNAL_ERROR"):
            assert STATUS_RANK[terminal] == 90, terminal
        assert STATUS_RANK["FILLED"] == 100


# ── §2 Migration dedup ranking ────────────────────────────────────────────────

class TestMigrationDedupRanking:
    """The dedup CASE expression is reasoned about lexically — terminal CASE
    branches must each rank strictly above every non-terminal CASE branch."""

    @pytest.fixture(scope="class")
    def sql(self) -> str:
        return (REPO_ROOT / "migrations" / "opportunity_ledger.sql").read_text()

    @pytest.fixture(scope="class")
    def case_ranks(self, sql) -> dict[str, int]:
        # Parse every "WHEN '<STATUS>' THEN <N>" in the dedup CTE.
        ranks: dict[str, int] = {}
        for m in re.finditer(r"WHEN\s+'([A-Z_]+)'\s+THEN\s+(\d+)", sql):
            ranks[m.group(1)] = int(m.group(2))
        return ranks

    def test_all_terminals_outrank_all_nonterminals(self, case_ranks):
        terminals = {"FILLED", "BROKER_REJECTED", "EXPIRED", "CANCELED",
                     "MISSED", "CLIENT_SKIPPED", "INTERNAL_ERROR",
                     "WATCHER_INVALIDATED", "ENTRY_CONFIRMATION_FAILED"}
        nonterminals = {"BROKER_ACKED", "BROKER_SUBMITTED", "WATCHER_ARMED",
                        "ORDER_CREATED", "PREFLIGHT_PASSED", "PREFLIGHT_WARNING",
                        "CLIENT_ELIGIBLE", "CREATED"}
        for t in terminals:
            assert t in case_ranks, f"migration missing rank for {t}"
            for nt in nonterminals:
                if nt not in case_ranks:
                    continue
                assert case_ranks[t] > case_ranks[nt], (
                    f"terminal {t}={case_ranks[t]} must outrank non-terminal "
                    f"{nt}={case_ranks[nt]}"
                )

    def test_missed_outranks_preflight_passed(self, case_ranks):
        assert case_ranks["MISSED"] > case_ranks["PREFLIGHT_PASSED"], (
            "Amendment §2: a duplicate MISSED row must be preserved over a "
            "PREFLIGHT_PASSED duplicate."
        )

    def test_filled_outranks_every_other_status(self, case_ranks):
        for s, r in case_ranks.items():
            if s == "FILLED":
                continue
            assert case_ranks["FILLED"] > r, (
                f"Amendment §2: FILLED ({case_ranks['FILLED']}) must outrank {s} ({r})"
            )

    def test_tie_break_uses_updated_at(self, sql):
        # ROW_NUMBER OVER must include updated_at DESC after the CASE.
        assert re.search(r"END DESC,\s*updated_at DESC", sql), (
            "Tie-break ordering must use updated_at DESC after the rank CASE."
        )

    def test_dedup_runs_before_unique_index(self, sql):
        dedup = sql.find("ROW_NUMBER() OVER")
        unique = sql.find("idx_cso_canonical_client")
        assert dedup != -1 and unique != -1
        assert dedup < unique, "must dedup before adding the unique constraint"


# ── §3 Production wiring: OSM transition → opportunity ledger ─────────────────

class TestProductionWiringOSMtoLedger:
    """The OSM.transition() final stage must call update_opportunity()
    for ENTRY orders. We exercise the private helper directly with a
    fake order row to avoid bringing up the full DB."""

    def _patched_update(self, monkeypatch):
        """Replace update_opportunity with a recorder; return the recorder list."""
        calls = []
        def _fake(*args, **kwargs):
            calls.append((args, kwargs))
            return True
        import ap.opportunity_ledger as _ol
        monkeypatch.setattr(_ol, "update_opportunity", _fake)
        return calls

    def _osm_stub(self):
        """Build a minimal OSM stub that exposes _notify_opportunity_ledger
        and the class-level mapping tables."""
        from ap.order_state_machine import APOrderStateMachine
        # APOrderStateMachine.__init__ touches DB; we don't call it. Instead
        # we construct an instance via object.__new__ and only use the helper.
        osm = object.__new__(APOrderStateMachine)
        osm.client_id = "c@x.com"
        return osm

    def _order_row(self, **overrides):
        base = {
            "kind": "ENTRY",
            "client_id": "c@x.com",
            "local_order_id": "ORD-1",
            "signal_id": "SIG-1",
            "canonical_signal_id": "CANON",
            "broker_order_id": None,
            "position_id": None,
        }
        base.update(overrides)
        return base

    def test_submitted_triggers_broker_submitted(self, monkeypatch):
        from ap.order_state_machine import OrderStatus
        calls = self._patched_update(monkeypatch)
        osm = self._osm_stub()
        osm._notify_opportunity_ledger(
            current=self._order_row(),
            new_status=OrderStatus.SUBMITTED,
            broker_order_id="BRK-1",
        )
        assert calls, "expected an opportunity ledger update"
        args, kwargs = calls[-1]
        assert args[2] == "BROKER_SUBMITTED"
        assert kwargs["broker_order_id"] == "BRK-1"
        assert kwargs["canonical_signal_id"] == "CANON"

    def test_acknowledged_triggers_broker_acked(self, monkeypatch):
        from ap.order_state_machine import OrderStatus
        calls = self._patched_update(monkeypatch)
        osm = self._osm_stub()
        osm._notify_opportunity_ledger(
            current=self._order_row(),
            new_status=OrderStatus.ACKNOWLEDGED,
            broker_order_id="BRK-1",
        )
        args, _ = calls[-1]
        assert args[2] == "BROKER_ACKED"

    def test_filled_includes_position_id(self, monkeypatch):
        from ap.order_state_machine import OrderStatus
        calls = self._patched_update(monkeypatch)
        osm = self._osm_stub()
        osm._notify_opportunity_ledger(
            current=self._order_row(),
            new_status=OrderStatus.FILLED,
            broker_order_id="BRK-1",
            position_id="POS-7",
        )
        args, kwargs = calls[-1]
        assert args[2] == "FILLED"
        assert kwargs["position_id"] == "POS-7"
        assert kwargs["broker_order_id"] == "BRK-1"

    def test_rejected_writes_broker_rejected_with_miss_stage(self, monkeypatch):
        from ap.order_state_machine import OrderStatus
        calls = self._patched_update(monkeypatch)
        osm = self._osm_stub()
        osm._notify_opportunity_ledger(
            current=self._order_row(),
            new_status=OrderStatus.REJECTED,
            last_error="broker_rejected_400_insufficient_buying_power",
        )
        args, kwargs = calls[-1]
        assert args[2] == "BROKER_REJECTED"
        assert kwargs["miss_stage"] == "BROKER_ACK"
        assert "insufficient_buying_power" in kwargs["miss_reason"]

    def test_expired_writes_fill_monitor_miss_stage(self, monkeypatch):
        from ap.order_state_machine import OrderStatus
        calls = self._patched_update(monkeypatch)
        osm = self._osm_stub()
        osm._notify_opportunity_ledger(
            current=self._order_row(),
            new_status=OrderStatus.EXPIRED,
            last_error="entry_age_cap_exceeded",
        )
        args, kwargs = calls[-1]
        assert args[2] == "EXPIRED"
        assert kwargs["miss_stage"] == "FILL_MONITOR"
        assert kwargs["miss_reason"] == "entry_age_cap_exceeded"

    def test_canceled_writes_fill_monitor_miss_stage(self, monkeypatch):
        from ap.order_state_machine import OrderStatus
        calls = self._patched_update(monkeypatch)
        osm = self._osm_stub()
        osm._notify_opportunity_ledger(
            current=self._order_row(),
            new_status=OrderStatus.CANCELED,
            last_error="watcher_invalidated",
        )
        args, kwargs = calls[-1]
        assert args[2] == "CANCELED"
        assert kwargs["miss_stage"] == "FILL_MONITOR"

    def test_exit_order_is_ignored(self, monkeypatch):
        from ap.order_state_machine import OrderStatus
        calls = self._patched_update(monkeypatch)
        osm = self._osm_stub()
        osm._notify_opportunity_ledger(
            current=self._order_row(kind="EXIT"),
            new_status=OrderStatus.FILLED,
        )
        assert calls == [], "EXIT orders must not write to the opportunity ledger"

    def test_partial_fill_and_error_are_ignored(self, monkeypatch):
        from ap.order_state_machine import OrderStatus
        calls = self._patched_update(monkeypatch)
        osm = self._osm_stub()
        osm._notify_opportunity_ledger(
            current=self._order_row(),
            new_status=OrderStatus.PARTIAL_FILL,
        )
        osm._notify_opportunity_ledger(
            current=self._order_row(),
            new_status="ERROR",
        )
        assert calls == [], (
            "PARTIAL_FILL and ERROR must not map to a terminal opportunity status"
        )

    def test_missing_signal_or_client_is_safe(self, monkeypatch):
        from ap.order_state_machine import OrderStatus
        calls = self._patched_update(monkeypatch)
        osm = self._osm_stub()
        osm.client_id = ""
        osm._notify_opportunity_ledger(
            current=self._order_row(client_id="", signal_id="", canonical_signal_id=""),
            new_status=OrderStatus.FILLED,
        )
        assert calls == []


# ── §3 Production wiring: entry-confirmation failure ─────────────────────────

class TestEntryConfirmationFailureWiring:
    """The three ENTRY_CONFIRM failure return paths in ap_execution_core.py
    must each write ENTRY_CONFIRMATION_FAILED to the opportunity ledger
    with miss_stage=ENTRY_CONFIRMATION."""

    @pytest.fixture(scope="class")
    def src(self) -> str:
        return (REPO_ROOT / "ap_execution_core.py").read_text()

    def test_all_three_return_paths_have_ledger_write(self, src):
        # Each ENTRY_CONFIRMATION_FAILED write block contains both the
        # status string and the STAGE_ENTRY_CONFIRMATION import.
        block = "ENTRY_CONFIRMATION_FAILED"
        stage = "STAGE_ENTRY_CONFIRMATION"
        assert src.count(block) >= 3, (
            f"expected ENTRY_CONFIRMATION_FAILED ledger writes at all three "
            f"failure paths, found {src.count(block)} occurrence(s)"
        )
        assert src.count(stage) >= 3, (
            f"expected STAGE_ENTRY_CONFIRMATION at all three failure paths, "
            f"found {src.count(stage)} occurrence(s)"
        )


# ── §4 Gate-order rebase note present ────────────────────────────────────────

class TestGateOrderRebaseNote:
    def test_queue_py_documents_post_pr87_gate_order(self):
        src = (REPO_ROOT / "ap" / "queue.py").read_text()
        assert "REBASE NOTE (Final Amendment v2 §4)" in src
        # The four-step order must appear.
        for step in (
            "master_control.evaluate()",
            "live authorization gate",
            "client-state preflight",
            "create_entry_order",
        ):
            assert step in src, f"expected gate-order step {step!r} in queue.py"


# ── §5 Acceptance preservation ───────────────────────────────────────────────

class TestPriorAcceptancePreserved:
    def test_canonical_unique_index_still_present(self):
        sql = (REPO_ROOT / "migrations" / "opportunity_ledger.sql").read_text()
        assert "idx_cso_canonical_client" in sql
        assert "(canonical_signal_id, client_id)" in sql
        assert "ALTER COLUMN canonical_signal_id SET NOT NULL" in sql

    def test_observe_only_default_still_false(self):
        old = os.environ.pop("CLIENT_PREFLIGHT_ENFORCE", None)
        try:
            from ap.client_preflight import preflight_enforce
            assert preflight_enforce() is False
        finally:
            if old is not None:
                os.environ["CLIENT_PREFLIGHT_ENFORCE"] = old

    def test_no_retry_submission_in_pr81_modules(self):
        for path in ("ap/opportunity_ledger.py", "ap/client_preflight.py"):
            src = (REPO_ROOT / path).read_text()
            for bad in ("broker.place_order", "submit_order", "RETRY_SUBMITTED"):
                assert bad not in src, f"{path} must not contain {bad!r}"

    def test_admin_endpoints_still_registered(self):
        src = (REPO_ROOT / "app.py").read_text()
        assert "/admin/operator/client-opportunities" in src
        assert "/admin/operator/client-parity-signal/" in src

    def test_create_opportunities_still_uses_ignore_duplicates(self):
        src = (REPO_ROOT / "ap" / "opportunity_ledger.py").read_text()
        assert "ignore_duplicates=True" in src, (
            "Amendment §5 acceptance: repeated fanout must not reset progressed rows."
        )
