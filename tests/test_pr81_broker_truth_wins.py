"""
tests/test_pr81_broker_truth_wins.py
====================================
PR81 FINAL FIX \u2014 broker truth must override false terminal misses.

Acceptance:
  * FILLED can repair any prior non-FILLED status, including non-FILLED
    terminal no-fill/miss states (EXPIRED / CANCELED / MISSED /
    BROKER_REJECTED / WATCHER_INVALIDATED / ENTRY_CONFIRMATION_FAILED /
    INTERNAL_ERROR / CLIENT_SKIPPED).
  * Once FILLED is written, no non-FILLED status may overwrite it.
  * Non-FILLED terminals cannot displace each other (first no-fill terminal
    wins). Prevents reconciler flapping.
  * Lower-rank incoming never overwrites higher-rank current.
  * Repeated fanout still cannot reset a FILLED (or any progressed) row.
  * Blocked status updates still allow metadata / identifier enrichment.
  * Blocked status update emits CLIENT_OPPORTUNITY_STATUS_REGRESSION_BLOCKED
    with client_id, canonical_signal_id, current_status, incoming_status.
  * mark_filled() persists proof context (price/qty/ts/source) in metadata.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
os.environ.setdefault("DATABASE_URL",
                       "postgresql://test:test@127.0.0.1:5432/test_pr81")


# ── Fake Supabase that lets us seed the current row status ────────────────────

class _Chain:
    def __init__(self, store, table_name, seed):
        self._store = store
        self._table = table_name
        self._seed  = seed
        self._op    = None
        self._row   = None
        self._filters: list[tuple] = []
        self._on_conflict = None
        self._ignore = False

    def select(self, cols):
        self._op = "select"; self._cols = cols; return self
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
    """Seed maps (canonical, client_id) -> dict-like row (must include
    'opportunity_status' and optionally 'metadata')."""

    def __init__(self, seed=None):
        self.calls: dict[str, list] = {}
        self.seed = seed or {}
    def table(self, name):
        return _Chain(self.calls, name, self.seed)


def _last_update_patch(sb) -> dict:
    return [c for c in sb.calls["client_signal_opportunities"]
            if c["op"] == "update"][-1]["row"]


# ── §1 Allowed FILLED repairs from every non-FILLED terminal ─────────────────

class TestBrokerTruthRepairs:
    @pytest.mark.parametrize("current", [
        "EXPIRED", "CANCELED", "MISSED", "BROKER_REJECTED",
        "WATCHER_INVALIDATED", "ENTRY_CONFIRMATION_FAILED",
        "INTERNAL_ERROR", "CLIENT_SKIPPED",
    ])
    def test_filled_repairs_terminal_miss(self, current):
        from ap.opportunity_ledger import update_opportunity, FILLED
        sb = _SB(seed={("CANON", "c@x.com"): {"opportunity_status": current}})
        ok = update_opportunity(
            "SIG", "c@x.com", FILLED,
            canonical_signal_id="CANON",
            broker_order_id="BRK-1", position_id="POS-1", sb=sb,
        )
        assert ok is True
        patch = _last_update_patch(sb)
        assert patch.get("opportunity_status") == FILLED, (
            f"FILLED must repair prior terminal {current}; got patch={patch}"
        )
        # Identifiers are persisted alongside the FILLED status.
        assert patch.get("broker_order_id") == "BRK-1"
        assert patch.get("position_id") == "POS-1"


# ── §2 FILLED is absolute \u2014 nothing overwrites it ─────────────────────────────

class TestFilledIsAbsolute:
    @pytest.mark.parametrize("incoming", [
        "CANCELED", "EXPIRED", "MISSED", "BROKER_REJECTED",
        "WATCHER_INVALIDATED", "ENTRY_CONFIRMATION_FAILED",
        "INTERNAL_ERROR", "CLIENT_SKIPPED",
        # Non-terminals too
        "BROKER_SUBMITTED", "ORDER_CREATED", "PREFLIGHT_WARNING",
        "WATCHER_ARMED",
    ])
    def test_filled_cannot_be_overwritten(self, incoming):
        from ap.opportunity_ledger import update_opportunity, FILLED
        sb = _SB(seed={("CANON", "c@x.com"): {"opportunity_status": FILLED}})
        update_opportunity(
            "SIG", "c@x.com", incoming,
            canonical_signal_id="CANON", sb=sb,
        )
        patch = _last_update_patch(sb)
        assert "opportunity_status" not in patch, (
            f"FILLED must not be overwritten by {incoming}; got patch={patch}"
        )


# ── §3 Non-FILLED terminals don't displace each other ────────────────────────

class TestNonFilledTerminalDoesNotDisplace:
    """Reconciler flap protection: a different no-fill terminal cannot
    silently replace another no-fill terminal."""

    @pytest.mark.parametrize("current,incoming", [
        ("EXPIRED",          "CANCELED"),
        ("CANCELED",         "MISSED"),
        ("MISSED",           "BROKER_REJECTED"),
        ("BROKER_REJECTED",  "EXPIRED"),
        ("INTERNAL_ERROR",   "MISSED"),
        ("CLIENT_SKIPPED",   "CANCELED"),
        ("WATCHER_INVALIDATED", "ENTRY_CONFIRMATION_FAILED"),
    ])
    def test_terminal_to_other_terminal_blocked(self, current, incoming):
        from ap.opportunity_ledger import update_opportunity
        sb = _SB(seed={("CANON", "c@x.com"): {"opportunity_status": current}})
        update_opportunity(
            "SIG", "c@x.com", incoming,
            canonical_signal_id="CANON", sb=sb,
        )
        patch = _last_update_patch(sb)
        assert "opportunity_status" not in patch, (
            f"{current} \u2192 {incoming} must be blocked; got patch={patch}"
        )


# ── §4 Lower-rank non-terminal incoming cannot overwrite higher-rank ─────────

class TestNonTerminalLowerRankBlocked:
    @pytest.mark.parametrize("current,incoming", [
        ("BROKER_SUBMITTED", "ORDER_CREATED"),
        ("ORDER_CREATED",    "PREFLIGHT_PASSED"),
        ("WATCHER_ARMED",    "ORDER_CREATED"),
        ("BROKER_ACKED",     "BROKER_SUBMITTED"),
    ])
    def test_lower_rank_blocked(self, current, incoming):
        from ap.opportunity_ledger import update_opportunity
        sb = _SB(seed={("CANON", "c@x.com"): {"opportunity_status": current}})
        update_opportunity(
            "SIG", "c@x.com", incoming,
            canonical_signal_id="CANON", sb=sb,
        )
        patch = _last_update_patch(sb)
        assert "opportunity_status" not in patch


# ── §5 Repeated fanout cannot reset progressed rows ──────────────────────────

class TestRepeatedFanoutDoesNotReset:
    def test_create_opportunities_does_not_reset_filled(self):
        from ap.opportunity_ledger import create_opportunities
        # Seed an existing FILLED row.
        sb = _SB(seed={("CANON", "c@x.com"):
                       {"opportunity_status": "FILLED"}})
        # Repeated fanout must use ignore_duplicates=True so the existing
        # FILLED row is preserved.
        create_opportunities(
            "REEVAL:abc:123", ["c@x.com"], {"ticker": "SPY"},
            canonical_signal_id="CANON", sb=sb,
        )
        upserts = [c for c in sb.calls["client_signal_opportunities"]
                   if c["op"] == "upsert"]
        assert upserts, "expected at least one upsert"
        assert upserts[0]["ignore_duplicates"] is True
        assert upserts[0]["on_conflict"] == "canonical_signal_id,client_id"


# ── §6 Blocked update still enriches metadata ────────────────────────────────

class TestBlockedUpdateStillEnriches:
    def test_enrichment_fields_survive_blocked_status(self):
        from ap.opportunity_ledger import update_opportunity, FILLED, MISSED
        sb = _SB(seed={("CANON", "c@x.com"): {"opportunity_status": FILLED}})
        update_opportunity(
            "SIG", "c@x.com", MISSED,                 # would-be regression
            canonical_signal_id="CANON",
            broker_order_id="BRK-LATE", position_id="POS-LATE",
            quote_age_seconds=2.5,
            extra_meta={"reconciler_note": "late_broker_truth"},
            sb=sb,
        )
        patch = _last_update_patch(sb)
        # Status is preserved (FILLED stays FILLED).
        assert "opportunity_status" not in patch
        # Enrichment writes through.
        assert patch.get("broker_order_id") == "BRK-LATE"
        assert patch.get("position_id") == "POS-LATE"
        assert patch.get("quote_age_seconds") == 2.5
        assert (patch.get("metadata") or {}).get("reconciler_note") == "late_broker_truth"


# ── §7 Blocked update emits the named log tag ────────────────────────────────

class TestRegressionLogTag:
    def test_log_contains_all_required_fields(self, caplog):
        import logging
        from ap.opportunity_ledger import update_opportunity, FILLED, MISSED
        sb = _SB(seed={("CANON-Z", "client@x.com"):
                       {"opportunity_status": FILLED}})
        with caplog.at_level(logging.INFO, logger="ap.opportunity_ledger"):
            update_opportunity(
                "RAW-SIG-7", "client@x.com", MISSED,
                canonical_signal_id="CANON-Z", sb=sb,
            )
        text = caplog.text
        assert "CLIENT_OPPORTUNITY_STATUS_REGRESSION_BLOCKED" in text
        assert "client_id=client@x.com" in text
        assert "canonical_signal_id=CANON-Z" in text
        assert "current_status=FILLED" in text
        assert "incoming_status=MISSED" in text


# ── §8 mark_filled persists proof context ────────────────────────────────────

class TestMarkFilledProofContext:
    def test_fill_proof_lands_in_metadata(self):
        from ap.opportunity_ledger import mark_filled
        # No seed \u2014 the read returns empty, so the write goes through as FILLED.
        sb = _SB()
        ok = mark_filled(
            "SIG-1", "c@x.com",
            canonical_signal_id="CANON",
            order_local_id="ORD-1",
            broker_order_id="BRK-1",
            position_id="POS-1",
            fill_price=1.42,
            filled_qty=3,
            fill_ts="2026-06-05T22:30:00+00:00",
            source="osm_transition",
            sb=sb,
        )
        assert ok is True
        patch = _last_update_patch(sb)
        assert patch.get("opportunity_status") == "FILLED"
        assert patch.get("broker_order_id") == "BRK-1"
        assert patch.get("position_id") == "POS-1"
        meta = patch.get("metadata") or {}
        proof = meta.get("fill_proof") or {}
        assert proof.get("fill_price") == 1.42
        assert proof.get("filled_qty") == 3
        assert proof.get("fill_ts") == "2026-06-05T22:30:00+00:00"
        assert proof.get("fill_source") == "osm_transition"

    def test_filled_repair_preserves_proof_on_terminal_row(self):
        """Even when the row is currently EXPIRED, FILLED repair carries the
        proof context into the patch."""
        from ap.opportunity_ledger import mark_filled
        sb = _SB(seed={("CANON", "c@x.com"): {"opportunity_status": "EXPIRED"}})
        mark_filled(
            "SIG-1", "c@x.com",
            canonical_signal_id="CANON",
            broker_order_id="BRK-9",
            position_id="POS-9",
            fill_price=2.10, filled_qty=1,
            fill_ts="2026-06-05T22:31:00+00:00",
            source="broker_reconciliation",
            sb=sb,
        )
        patch = _last_update_patch(sb)
        assert patch.get("opportunity_status") == "FILLED"
        proof = (patch.get("metadata") or {}).get("fill_proof") or {}
        assert proof.get("fill_source") == "broker_reconciliation"
        assert proof.get("fill_price") == 2.10


# ── §9 STATUS_RANK exact shape (broker_truth_wins) ───────────────────────────

class TestStatusRankShape:
    def test_filled_is_strictly_above_other_terminals(self):
        from ap.opportunity_ledger import STATUS_RANK
        non_filled_terminals = [
            "WATCHER_INVALIDATED", "ENTRY_CONFIRMATION_FAILED",
            "BROKER_REJECTED", "EXPIRED", "CANCELED", "MISSED",
            "CLIENT_SKIPPED", "INTERNAL_ERROR",
        ]
        for t in non_filled_terminals:
            assert STATUS_RANK["FILLED"] > STATUS_RANK[t], (
                f"FILLED ({STATUS_RANK['FILLED']}) must be strictly above "
                f"{t} ({STATUS_RANK[t]})"
            )

    def test_non_filled_terminals_all_equal_to_90(self):
        from ap.opportunity_ledger import STATUS_RANK
        for t in ("WATCHER_INVALIDATED", "ENTRY_CONFIRMATION_FAILED",
                  "BROKER_REJECTED", "EXPIRED", "CANCELED", "MISSED",
                  "CLIENT_SKIPPED", "INTERNAL_ERROR"):
            assert STATUS_RANK[t] == 90, f"{t} should rank 90"

    def test_non_filled_terminals_outrank_every_non_terminal(self):
        from ap.opportunity_ledger import STATUS_RANK
        non_terminal_max = max(
            STATUS_RANK[s] for s in (
                "CREATED", "CLIENT_ELIGIBLE", "PREFLIGHT_WARNING",
                "PREFLIGHT_PASSED", "ORDER_CREATED", "WATCHER_ARMED",
                "BROKER_SUBMITTED", "BROKER_ACKED",
            )
        )
        for t in ("WATCHER_INVALIDATED", "ENTRY_CONFIRMATION_FAILED",
                  "BROKER_REJECTED", "EXPIRED", "CANCELED", "MISSED",
                  "CLIENT_SKIPPED", "INTERNAL_ERROR"):
            assert STATUS_RANK[t] > non_terminal_max
