"""
P0 — Canonical Signal ID helper (client parity)
================================================

Tests build_canonical_signal_id() against the four 2026-06-03 production
incidents that surfaced the parity bug:

    UNH CALL  : REEVAL:603bce5b-352e-44e0-b00b-6baa826ca2c9
    VZ  PUT   : REEVAL:c068cdf5-68fb-4052-92f4-24c8efba34d4
    META CALL : REEVAL:4c18bcf5-f437-4dab-ae7c-e24e0b5d502f
    PG   CALL : REEVAL:a4392a5d-76b1-458e-9f34-894e3f65fe2f

These are the wrapped canonical IDs as quoted in the PR. In production,
ap_overnight_reeval.py appends a 6-char random hex per client emit:

    REEVAL:<uuid>:<hex6>   e.g. REEVAL:603bce5b-...:f4dc44

build_canonical_signal_id() must strip the suffix so all three clients
that received the same opportunity collapse to the same canonical_id.

Source-grep tests additionally verify the OSM INSERTs stamp the new
canonical_signal_id column on both ENTRY and EXIT orders.
"""
from __future__ import annotations

import ast
import os
import re

import pytest

from ap_canonical_signal import build_canonical_signal_id


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ---------------------------------------------------------------------------
# Helper behaviour
# ---------------------------------------------------------------------------

# (input_signal_id, expected_canonical) — verbatim from the 2026-06-03 report
INCIDENTS = [
    "REEVAL:603bce5b-352e-44e0-b00b-6baa826ca2c9",   # UNH CALL
    "REEVAL:c068cdf5-68fb-4052-92f4-24c8efba34d4",   # VZ PUT
    "REEVAL:4c18bcf5-f437-4dab-ae7c-e24e0b5d502f",   # META CALL
    "REEVAL:a4392a5d-76b1-458e-9f34-894e3f65fe2f",   # PG CALL
]


def test_plain_uuid_passes_through_unchanged():
    uid = "603bce5b-352e-44e0-b00b-6baa826ca2c9"
    assert build_canonical_signal_id(uid) == uid


@pytest.mark.parametrize("canonical", INCIDENTS)
def test_reeval_with_per_client_suffix_strips_to_canonical(canonical):
    """Per-client emits append a 6-char random hex; canonical must match."""
    suffixed_jason   = f"{canonical}:f4dc44"
    suffixed_jose    = f"{canonical}:a91b27"
    suffixed_trade   = f"{canonical}:0c5e80"

    assert build_canonical_signal_id(suffixed_jason) == canonical
    assert build_canonical_signal_id(suffixed_jose)  == canonical
    assert build_canonical_signal_id(suffixed_trade) == canonical


def test_three_client_emits_collapse_to_one_canonical():
    """The merge-blocker invariant: all three clients group by canonical_id."""
    canonical = INCIDENTS[0]  # UNH CALL
    per_client = [
        f"{canonical}:f4dc44",
        f"{canonical}:a91b27",
        f"{canonical}:0c5e80",
    ]
    distinct = {build_canonical_signal_id(s) for s in per_client}
    assert distinct == {canonical}, (
        f"expected all per-client emits to collapse to {canonical!r}; "
        f"got {distinct!r}"
    )


def test_reeval_without_suffix_is_unchanged():
    """A canonical-form REEVAL id (no per-client tail) is returned as-is."""
    assert build_canonical_signal_id(INCIDENTS[0]) == INCIDENTS[0]


def test_empty_and_none_inputs_return_empty_string():
    assert build_canonical_signal_id("") == ""
    assert build_canonical_signal_id(None) == ""
    assert build_canonical_signal_id(12345) == ""  # non-string


def test_explicit_canonical_on_payload_takes_precedence():
    """If a scanner emits canonical_signal_id directly, trust it."""
    sig = {"canonical_signal_id": "explicit-canonical-xyz"}
    out = build_canonical_signal_id("REEVAL:abc:def", sig)
    assert out == "explicit-canonical-xyz"


def test_non_reeval_signal_id_unchanged():
    """Non-REEVAL strings (e.g. live-path signal_ids) are not mangled."""
    for sid in [
        "signal_a1b2c3d4e5f6_2026-06-03T12:00:00Z",
        "live-signal-XYZ",
        "8d9338d0-5dde-4b7b-81ea-208039999b72",
    ]:
        assert build_canonical_signal_id(sid) == sid


# ---------------------------------------------------------------------------
# OSM INSERTs stamp canonical_signal_id (source-grep, no DB)
# ---------------------------------------------------------------------------

def _read(path: str) -> str:
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def test_osm_imports_build_canonical_signal_id():
    src = _read(os.path.join(REPO_ROOT, "ap", "order_state_machine.py"))
    assert "from ap_canonical_signal import build_canonical_signal_id" in src


def test_osm_entry_insert_includes_canonical_signal_id_column():
    """create_entry_order INSERT must list canonical_signal_id."""
    src = _read(os.path.join(REPO_ROOT, "ap", "order_state_machine.py"))
    # find the first INSERT INTO orders and inspect the column list near it
    idx = src.find("INSERT INTO orders")
    assert idx >= 0
    # examine the next ~600 chars (covers the column list block)
    chunk = src[idx : idx + 600]
    assert "canonical_signal_id" in chunk, (
        "ENTRY INSERT must include canonical_signal_id column. Got:\n" + chunk
    )


def test_osm_exit_insert_includes_canonical_signal_id_column():
    """create_exit_order INSERT must also list canonical_signal_id."""
    src = _read(os.path.join(REPO_ROOT, "ap", "order_state_machine.py"))
    # find the SECOND INSERT INTO orders
    first  = src.find("INSERT INTO orders")
    second = src.find("INSERT INTO orders", first + 1)
    assert second >= 0
    chunk = src[second : second + 600]
    assert "canonical_signal_id" in chunk, (
        "EXIT INSERT must include canonical_signal_id column. Got:\n" + chunk
    )


# ---------------------------------------------------------------------------
# Migration sanity: file exists and contains the four required statements
# ---------------------------------------------------------------------------

def test_migration_file_contains_required_statements():
    mig = _read(os.path.join(
        REPO_ROOT, "migrations",
        "20260604_canonical_signal_id_and_ledger.sql",
    ))
    assert "ALTER TABLE orders" in mig
    assert "ADD COLUMN IF NOT EXISTS canonical_signal_id" in mig
    assert "idx_orders_canonical_signal_client" in mig
    assert "CREATE TABLE IF NOT EXISTS client_signal_opportunities" in mig
    assert "UNIQUE (canonical_signal_id, client_id)" in mig
    # Backfill regex matches REEVAL:<uuid>:<hex> only
    assert "regexp_replace(signal_id" in mig
