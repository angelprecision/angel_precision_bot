"""
tests/test_pr81_amendments_v2.py
================================
PR81 FINAL AMENDMENT compliance tests — round 2.

Covers the items the first amendment-test file missed:
  - §1 canonical_signal_id is the true unique key (on_conflict in code matches
    the migration's unique index)
  - §2 create_opportunities() NEVER regresses a progressed row
  - §3 full lifecycle helpers write the correct status + stages
  - §4 map_reason_to_stage() resolves real master_control reasons
  - §5 preflight reads the ACTUAL schema columns and resolves credentials
        the same way client_runner does
  - §5 UNKNOWN counters never trigger a cap block and are rendered as
        "<field>_unavailable" in to_dict()
  - §7 INTERNAL_ERROR / ORDER_CREATION mark helpers
  - Migration script declares canonical_signal_id NOT NULL and a unique
    index on (canonical_signal_id, client_id)
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
os.environ.setdefault("DATABASE_URL",
                       "postgresql://test:test@127.0.0.1:5432/test_pr81")


# ── Fake Supabase ─────────────────────────────────────────────────────────────

class _FakeResult:
    def __init__(self, data=None):
        self.data = data if data is not None else []


class _Chain:
    """Captures every call so we can assert on what the code attempted."""

    def __init__(self, store, table_name):
        self._store = store
        self._table = table_name
        self._op = None
        self._row = None
        self._filters = []
        self._on_conflict = None
        self._ignore = False
        self._existing = None

    # --- chained writes ---
    def upsert(self, row, on_conflict=None, ignore_duplicates=False):
        self._op = "upsert"
        self._row = row
        self._on_conflict = on_conflict
        self._ignore = ignore_duplicates
        return self

    def insert(self, row):
        self._op = "insert"
        self._row = row
        return self

    def update(self, patch):
        self._op = "update"
        self._row = patch
        return self

    def select(self, *_args, **_kw):
        self._op = "select"
        return self

    def eq(self, col, val):
        self._filters.append((col, val))
        return self

    def limit(self, n):
        return self

    def execute(self):
        # Record the operation
        self._store.setdefault(self._table, []).append({
            "op": self._op,
            "row": self._row,
            "filters": list(self._filters),
            "on_conflict": self._on_conflict,
            "ignore_duplicates": self._ignore,
        })
        # Selects return whatever the test pre-seeded
        if self._op == "select":
            key = (self._table, tuple(self._filters))
            return _FakeResult(self._store.get("__seed__", {}).get(key, []))
        return _FakeResult()


class _FakeSB:
    def __init__(self, seed=None):
        self.calls: dict[str, list] = {}
        if seed:
            self.calls["__seed__"] = seed

    def table(self, name):
        return _Chain(self.calls, name)


# ── §1 / §2: canonical idempotency + no-regress ──────────────────────────────

class TestCanonicalIdempotencyAndNoRegress:
    def test_on_conflict_matches_migration_unique_index(self):
        """create_opportunities must use on_conflict=canonical_signal_id,client_id."""
        from ap.opportunity_ledger import create_opportunities
        sb = _FakeSB()
        create_opportunities(
            "SIG-001", ["a@x.com"], {"ticker": "SPY"},
            canonical_signal_id="CANON-1", sb=sb,
        )
        upsert = [c for c in sb.calls["client_signal_opportunities"]
                  if c["op"] == "upsert"]
        assert upsert, "expected at least one upsert"
        assert upsert[0]["on_conflict"] == "canonical_signal_id,client_id", (
            f"on_conflict must match migration unique index, got {upsert[0]['on_conflict']!r}"
        )
        assert upsert[0]["ignore_duplicates"] is True, (
            "Amendment §2: create_opportunities must use ignore_duplicates=True"
        )

    def test_reeval_suffix_does_not_create_second_row(self):
        """Same canonical, different REEVAL suffixes → still one upsert per call,
        and both upserts pass the SAME canonical_signal_id so the DB unique
        constraint collapses them."""
        from ap.opportunity_ledger import create_opportunities
        sb = _FakeSB()
        canon = "REEVAL:abc"
        create_opportunities("REEVAL:abc:111", ["a@x.com"],
                              {"ticker": "SPY"}, canonical_signal_id=canon, sb=sb)
        create_opportunities("REEVAL:abc:222", ["a@x.com"],
                              {"ticker": "SPY"}, canonical_signal_id=canon, sb=sb)
        upserts = [c for c in sb.calls["client_signal_opportunities"]
                   if c["op"] == "upsert"]
        assert len(upserts) == 2
        for u in upserts:
            assert u["row"]["canonical_signal_id"] == canon

    def test_create_uses_insert_when_ignore_duplicates_unsupported(self):
        """If supabase-py is too old to accept ignore_duplicates, the code
        must fall back to a guarded read-then-insert that NEVER overwrites
        an existing progressed row."""
        from ap.opportunity_ledger import create_opportunities

        class _OldChain(_Chain):
            def upsert(self, row, on_conflict=None, ignore_duplicates=False):
                if ignore_duplicates:
                    raise TypeError("ignore_duplicates not supported")
                return super().upsert(row, on_conflict=on_conflict)

        class _OldSB(_FakeSB):
            def table(self, name):
                return _OldChain(self.calls, name)

        # Seed: existing FILLED row that must NOT be overwritten.
        seed = {("client_signal_opportunities",
                 (("canonical_signal_id", "CANON-1"), ("client_id", "a@x.com"))):
                 [{"id": 1, "opportunity_status": "FILLED"}]}
        sb = _OldSB(seed=seed)
        create_opportunities(
            "SIG-001", ["a@x.com"], {"ticker": "SPY"},
            canonical_signal_id="CANON-1", sb=sb,
        )
        ops = [c["op"] for c in sb.calls["client_signal_opportunities"]]
        # Must have done a select to check existence, then NO insert.
        assert "select" in ops, f"expected a guarded select, got {ops}"
        assert "insert" not in ops, (
            f"Amendment §2: must not insert when a progressed row exists; got ops={ops}"
        )

    def test_create_inserts_when_no_existing_row_in_fallback(self):
        """Fallback path inserts when no existing row is found."""
        from ap.opportunity_ledger import create_opportunities

        class _OldChain(_Chain):
            def upsert(self, row, on_conflict=None, ignore_duplicates=False):
                if ignore_duplicates:
                    raise TypeError("ignore_duplicates not supported")
                return super().upsert(row, on_conflict=on_conflict)

        class _OldSB(_FakeSB):
            def table(self, name):
                return _OldChain(self.calls, name)

        sb = _OldSB()  # no seed → empty select
        create_opportunities(
            "SIG-001", ["a@x.com"], {"ticker": "SPY"},
            canonical_signal_id="CANON-1", sb=sb,
        )
        ops = [c["op"] for c in sb.calls["client_signal_opportunities"]]
        assert "insert" in ops, f"expected insert when row missing, got {ops}"


# ── §3: full lifecycle helpers ───────────────────────────────────────────────

class TestLifecycleHelpers:
    def test_mark_watcher_armed(self):
        from ap.opportunity_ledger import mark_watcher_armed
        sb = _FakeSB()
        ok = mark_watcher_armed("SIG-1", "c@x.com",
                                canonical_signal_id="CANON",
                                order_local_id="ORD-1", sb=sb)
        assert ok is True
        upd = [c for c in sb.calls["client_signal_opportunities"]
               if c["op"] == "update"][-1]
        assert upd["row"]["opportunity_status"] == "WATCHER_ARMED"
        assert upd["row"]["order_local_id"] == "ORD-1"

    def test_mark_watcher_invalidated(self):
        from ap.opportunity_ledger import mark_watcher_invalidated
        sb = _FakeSB()
        mark_watcher_invalidated("SIG-1", "c@x.com", "stale_signal",
                                  canonical_signal_id="CANON",
                                  order_local_id="ORD-1", sb=sb)
        upd = [c for c in sb.calls["client_signal_opportunities"]
               if c["op"] == "update"][-1]
        assert upd["row"]["opportunity_status"] == "MISSED"
        assert upd["row"]["miss_stage"] == "WATCHER_ARM"
        assert upd["row"]["miss_reason"] == "stale_signal"

    def test_mark_broker_submitted(self):
        from ap.opportunity_ledger import mark_broker_submitted
        sb = _FakeSB()
        mark_broker_submitted("SIG-1", "c@x.com",
                               canonical_signal_id="CANON",
                               order_local_id="ORD-1",
                               broker_order_id="BRK-9", sb=sb)
        upd = [c for c in sb.calls["client_signal_opportunities"]
               if c["op"] == "update"][-1]
        assert upd["row"]["opportunity_status"] == "BROKER_SUBMITTED"
        assert upd["row"]["broker_order_id"] == "BRK-9"

    def test_mark_filled(self):
        from ap.opportunity_ledger import mark_filled
        sb = _FakeSB()
        mark_filled("SIG-1", "c@x.com", canonical_signal_id="CANON",
                    order_local_id="ORD-1", broker_order_id="BRK-9",
                    position_id="POS-3", sb=sb)
        upd = [c for c in sb.calls["client_signal_opportunities"]
               if c["op"] == "update"][-1]
        assert upd["row"]["opportunity_status"] == "FILLED"
        assert upd["row"]["position_id"] == "POS-3"

    def test_mark_internal_error(self):
        from ap.opportunity_ledger import mark_internal_error
        sb = _FakeSB()
        mark_internal_error("SIG-1", "c@x.com", "watcher_error:boom",
                             canonical_signal_id="CANON", sb=sb)
        upd = [c for c in sb.calls["client_signal_opportunities"]
               if c["op"] == "update"][-1]
        assert upd["row"]["opportunity_status"] == "INTERNAL_ERROR"
        assert upd["row"]["miss_stage"] == "INTERNAL_ERROR"
        assert upd["row"]["miss_reason"] == "watcher_error:boom"


# ── §4: reason → stage mapping ───────────────────────────────────────────────

class TestReasonToStageMapping:
    @pytest.mark.parametrize("reason,expected_stage", [
        ("daily_stop hit",        "CAP_BLOCK"),
        ("sizer_blocked: not enough buying_power", "CAP_BLOCK"),
        ("duplicate_symbol",      "DUPLICATE_SYMBOL"),
        ("opposite_side conflict", "DUPLICATE_SYMBOL"),
        ("kill_switch active",    "CLIENT_PREFLIGHT"),
        ("client_killswitch",     "CLIENT_PREFLIGHT"),
        ("entries_paused",        "CLIENT_PREFLIGHT"),
        ("subscription_inactive", "CLIENT_PREFLIGHT"),
        ("not_approved",          "CLIENT_PREFLIGHT"),
        ("missing_broker_credentials", "CLIENT_PREFLIGHT"),
        ("broker_mode_mismatch",  "CLIENT_PREFLIGHT"),
        ("watcher arm_failed",    "WATCHER_ARM"),
        ("stale_signal",          "WATCHER_ARM"),
        ("entry_confirmation failed", "ENTRY_CONFIRMATION"),
        ("broker_submit failure", "BROKER_SUBMIT"),
        ("broker_reject 422",     "BROKER_ACK"),
        ("expired no_fill",       "FILL_MONITOR"),
        ("fill_integrity unproven", "FILL_INTEGRITY"),
        ("order_create_error: db", "ORDER_CREATION"),
        ("",                      "UNKNOWN"),
        (None,                    "UNKNOWN"),
        ("something_totally_unmapped", "UNKNOWN"),
    ])
    def test_map_reason_to_stage(self, reason, expected_stage):
        from ap.opportunity_ledger import map_reason_to_stage
        assert map_reason_to_stage(reason) == expected_stage, (
            f"{reason!r} should map to {expected_stage}"
        )


# ── §5: preflight schema correctness ─────────────────────────────────────────

class TestPreflightSchemaCorrectness:
    """The preflight must read the SAME columns ap/entry_gate.py uses."""

    def _build_sb(self, member_row):
        """Returns an SB that yields member_row on members.select and
        empty lists for everything else."""
        calls = []

        class _M:
            def __init__(self, mr):
                self.mr = mr
                self._t = None
                self._filters = []
            def table(self, name):
                self._t = name
                self._filters = []
                return self
            def select(self, cols):
                self._select_cols = cols
                calls.append(("select", self._t, cols))
                return self
            def eq(self, k, v):
                self._filters.append((k, v))
                return self
            def in_(self, k, v):
                self._filters.append((k, tuple(v)))
                return self
            def gte(self, k, v):
                self._filters.append((">=", k, v))
                return self
            def limit(self, n):
                return self
            def execute(self):
                if self._t == "members":
                    return _FakeResult([self.mr] if self.mr else [])
                return _FakeResult([])

        return _M(member_row), calls

    def test_preflight_selects_real_member_columns(self):
        """Preflight's members select must include the columns entry_gate uses."""
        from ap.client_preflight import build_client_trade_preflight
        sb, calls = self._build_sb(member_row={
            "email": "a@x.com",
            "approved": True,
            "subscriptionactive": True,
            "killswitch": False,
            "entriespaused": False,
            "maintenancemode": False,
            "scannerroutingenabled": True,
            "tradier_account_mode": "paper",
            "tradier_paper_account_id": "ACC-1",
            "tradier_paper_access_token": "gAAAAA_encrypted_token",
        })

        class _Plan:
            max_position_usd = 100.0

        pf = build_client_trade_preflight("a@x.com", {"ticker": "SPY"}, _Plan(), sb=sb)

        # The select call must request the real column names.
        members_selects = [c for c in calls if c[1] == "members" and c[0] == "select"]
        assert members_selects, "expected a members select call"
        cols = members_selects[0][2]
        for required in ("approved", "subscriptionactive", "killswitch",
                         "entriespaused", "maintenancemode",
                         "scannerroutingenabled", "tradier_account_mode",
                         "tradier_paper_account_id", "tradier_paper_access_token",
                         "tradier_live_account_id", "tradier_live_access_token"):
            assert required in cols, f"members.select must request {required!r}"

        # Eligible because all hard gates pass.
        assert pf.eligible is True, f"unexpected block: {pf.block_reason}"
        assert pf.broker_credentials_present is True
        assert pf.tradier_active_mode == "paper"
        assert pf.expected_mode == "paper"

    def test_paper_credentials_use_paper_columns_first(self):
        """Paper mode must accept tradier_paper_* even when generic columns
        are missing (Amendment §5)."""
        from ap.client_preflight import build_client_trade_preflight
        sb, _ = self._build_sb(member_row={
            "approved": True, "subscriptionactive": True,
            "killswitch": False, "entriespaused": False,
            "tradier_account_mode": "paper",
            "tradier_paper_account_id": "PAPER-ACC",
            "tradier_paper_access_token": "gAAAAA...",
            # NO tradier_account_id / tradier_access_token at all.
        })
        class _Plan: max_position_usd = 100.0
        pf = build_client_trade_preflight("a@x.com", {"ticker": "SPY"}, _Plan(), sb=sb)
        assert pf.has_account_id is True
        assert pf.has_access_token is True
        assert pf.broker_credentials_present is True
        assert pf.eligible is True, f"unexpected block: {pf.block_reason}"

    def test_live_credentials_require_live_columns(self):
        """Live mode must NOT silently fall back to paper-only credentials."""
        from ap.client_preflight import build_client_trade_preflight
        sb, _ = self._build_sb(member_row={
            "approved": True, "subscriptionactive": True,
            "killswitch": False, "entriespaused": False,
            "tradier_account_mode": "live",
            # Live columns missing — paper columns present (must NOT count).
            "tradier_paper_account_id": "PAPER-ACC",
            "tradier_paper_access_token": "gAAAAA...",
        })
        class _Plan: max_position_usd = 100.0
        pf = build_client_trade_preflight("a@x.com", {"ticker": "SPY"}, _Plan(), sb=sb)
        assert pf.broker_credentials_present is False
        assert pf.block_reason == "missing_broker_credentials"

    def test_killswitch_blocks(self):
        from ap.client_preflight import build_client_trade_preflight
        sb, _ = self._build_sb(member_row={
            "approved": True, "subscriptionactive": True,
            "killswitch": True,            # ← block
            "entriespaused": False,
            "tradier_account_mode": "paper",
            "tradier_paper_account_id": "x", "tradier_paper_access_token": "y",
        })
        class _Plan: max_position_usd = 100.0
        pf = build_client_trade_preflight("a@x.com", {"ticker": "SPY"}, _Plan(), sb=sb)
        assert pf.eligible is False
        assert pf.block_reason == "kill_switch_on"

    def test_subscription_inactive_uses_real_column_name(self):
        """The real column is 'subscriptionactive' (no underscore)."""
        from ap.client_preflight import build_client_trade_preflight
        sb, _ = self._build_sb(member_row={
            "approved": True,
            "subscriptionactive": False,   # ← block
            "killswitch": False, "entriespaused": False,
            "tradier_account_mode": "paper",
            "tradier_paper_account_id": "x", "tradier_paper_access_token": "y",
        })
        class _Plan: max_position_usd = 100.0
        pf = build_client_trade_preflight("a@x.com", {"ticker": "SPY"}, _Plan(), sb=sb)
        assert pf.eligible is False
        assert pf.block_reason == "subscription_inactive"

    def test_unknown_counters_do_not_block(self):
        """When daily_trade_count etc. cannot be read, they stay UNKNOWN and
        must NOT trigger any cap block."""
        from ap.client_preflight import build_client_trade_preflight, UNKNOWN
        # No sb → all counters stay UNKNOWN.
        class _Plan: max_position_usd = 100.0
        pf = build_client_trade_preflight(
            "a@x.com", {"ticker": "SPY"}, _Plan(), sb=None, runner=None,
        )
        # Booleans default fail-closed so the block reason is one of the hard
        # gates, NOT a cap.
        assert pf.block_reason != "daily_cap_reached"
        assert pf.block_reason != "lane_cap_reached"
        assert pf.block_reason != "intraday_cap_reached"
        assert pf.daily_trade_count == UNKNOWN

    def test_to_dict_renders_unknown_counters_as_unavailable(self):
        from ap.client_preflight import (
            ClientTradePreflight, UNKNOWN,
        )
        from datetime import datetime, timezone
        pf = ClientTradePreflight(
            client_id="a@x.com", client_active=True, approved=True,
            subscription_active=True, kill_switch=False, entries_paused=False,
            maintenance_mode=False, scanner_routing_enabled=True,
            tradier_active_mode="paper", expected_mode="paper",
            has_account_id=True, has_access_token=True,
            broker_credentials_present=True,
            buying_power=0.0, estimated_cost=0.0, max_trade_cost=500.0,
            daily_trade_count=UNKNOWN, daily_lane_count=UNKNOWN,
            intraday_lane_count=UNKNOWN, same_symbol_count=UNKNOWN,
            open_positions_count=UNKNOWN, pending_entries_count=UNKNOWN,
            max_daily_trades=5, max_lane_trades=3, max_intraday_trades=2,
            max_same_symbol_trades=1, max_open_positions=10,
            max_pending_entries=3, eligible=True, block_reason=None,
            snapshot_ts=datetime.now(timezone.utc).isoformat(),
        )
        d = pf.to_dict()
        assert d["daily_trade_count"]       == "daily_trade_count_unavailable"
        assert d["daily_lane_count"]        == "daily_lane_count_unavailable"
        assert d["intraday_lane_count"]     == "intraday_lane_count_unavailable"
        assert d["same_symbol_count"]       == "same_symbol_count_unavailable"
        assert d["open_positions_count"]    == "open_positions_count_unavailable"
        assert d["pending_entries_count"]   == "pending_entries_count_unavailable"
        assert d["buying_power"]            == "buying_power_unavailable"

    def test_runner_buying_power_is_not_account_equity(self):
        """The runner exposes account_equity, but the preflight must NOT
        read buying_power from it (Amendment §5)."""
        from ap.client_preflight import build_client_trade_preflight

        class _R:
            kill_switch_active = False
            account_equity     = 50000.0      # MUST NOT become buying_power
            # no `buying_power` attribute on purpose
            member = None
            class _EA:
                @staticmethod
                def is_set(): return True
            entries_allowed = _EA()

        class _Plan: max_position_usd = 100.0
        pf = build_client_trade_preflight(
            "a@x.com", {"ticker": "SPY"}, _Plan(), sb=None, runner=_R(),
        )
        assert pf.buying_power == 0.0, (
            "Amendment §5: buying_power must not be populated from account_equity"
        )

    def test_expected_mode_is_client_aware(self):
        """expected_mode comes from the client/pod, not the global env var."""
        from ap.client_preflight import build_client_trade_preflight

        class _R:
            kill_switch_active = False
            expected_mode = "live"
            mode = "live"
            member = None
            class _EA:
                @staticmethod
                def is_set(): return True
            entries_allowed = _EA()

        old = os.environ.get("BOT_MODE")
        os.environ["BOT_MODE"] = "PAPER"   # global says paper
        try:
            class _Plan: max_position_usd = 100.0
            pf = build_client_trade_preflight(
                "a@x.com", {"ticker": "SPY"}, _Plan(), sb=None, runner=_R(),
            )
        finally:
            if old is None:
                os.environ.pop("BOT_MODE", None)
            else:
                os.environ["BOT_MODE"] = old

        assert pf.expected_mode == "live", (
            "expected_mode must follow the runner/pod, not BOT_MODE env"
        )


# ── §1 migration sanity ──────────────────────────────────────────────────────

class TestMigrationSql:
    def test_migration_declares_canonical_not_null(self):
        sql = (REPO_ROOT / "migrations" / "opportunity_ledger.sql").read_text()
        assert "ALTER COLUMN canonical_signal_id SET NOT NULL" in sql, (
            "migration must enforce canonical_signal_id NOT NULL"
        )

    def test_migration_unique_index_is_canonical(self):
        sql = (REPO_ROOT / "migrations" / "opportunity_ledger.sql").read_text()
        # The canonical unique index must exist…
        assert "idx_cso_canonical_client" in sql
        assert "UNIQUE INDEX" in sql.upper()
        assert "(canonical_signal_id, client_id)" in sql
        # …and the legacy one must be dropped.
        assert "DROP INDEX IF EXISTS idx_cso_signal_client" in sql

    def test_migration_backfills_canonical(self):
        sql = (REPO_ROOT / "migrations" / "opportunity_ledger.sql").read_text()
        assert "UPDATE client_signal_opportunities" in sql
        assert "SET canonical_signal_id = signal_id" in sql

    def test_migration_resolves_duplicates_before_unique(self):
        sql = (REPO_ROOT / "migrations" / "opportunity_ledger.sql").read_text()
        # Dedup CTE must appear before the unique index creation.
        dedup_idx  = sql.find("ROW_NUMBER() OVER")
        unique_idx = sql.find("idx_cso_canonical_client")
        assert dedup_idx != -1, "dedup CTE missing"
        assert unique_idx != -1, "unique index missing"
        assert dedup_idx < unique_idx, (
            "must resolve duplicates BEFORE creating the unique index"
        )


# ── §8 admin endpoints exist ────────────────────────────────────────────────-

class TestAdminEndpointsRegistered:
    def test_endpoints_registered_in_app(self):
        src = (REPO_ROOT / "app.py").read_text()
        assert "/admin/operator/client-opportunities" in src, (
            "client-opportunities admin endpoint missing"
        )
        assert "/admin/operator/client-parity-signal/" in src, (
            "client-parity-signal admin endpoint missing"
        )
