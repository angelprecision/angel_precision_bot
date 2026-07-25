"""
tests/test_p0_active_order_fence_full_family.py
==================================================
PR #388 amendment — the exact active-ENTRY order fence must recognize the
FULL nonterminal status family the repository's existing durable duplicate
logic uses:

    CREATED, PENDING_TRIGGER, SUBMITTED, ACCEPTED, ACKNOWLEDGED, OPEN,
    PARTIAL_FILL, PARTIALLY_FILLED, FILLED

Prior amendment only recognized {PENDING_TRIGGER, SUBMITTED, ACKNOWLEDGED,
PARTIAL_FILL, FILLED}. Any ENTRY row in ACCEPTED / OPEN / CREATED /
PARTIALLY_FILLED slipped through, letting the resolver reach NEW and
allowing a duplicate create_entry_order() call on retry.

Disposition rules:
  * PENDING_TRIGGER                                     → REATTACH_WATCHER
  * CREATED                                             → RETRYABLE (no new order)
  * SUBMITTED/ACCEPTED/ACKNOWLEDGED/OPEN/PARTIAL_FILL/
    PARTIALLY_FILLED/FILLED                             → ALREADY_OWNED
"""
from __future__ import annotations

import pytest

import ap_overnight_reeval as ov


NONTERMINAL_STATUSES = [
    "CREATED",
    "PENDING_TRIGGER",
    "SUBMITTED",
    "ACCEPTED",
    "ACKNOWLEDGED",
    "OPEN",
    "PARTIAL_FILL",
    "PARTIALLY_FILLED",
    "FILLED",
]


def test_full_nonterminal_family_present_in_active_status_set():
    """Every status in the durable duplicate family must be in the fence."""
    for st in NONTERMINAL_STATUSES:
        assert st in ov._ACTIVE_ENTRY_OWN_STATUSES, (
            f"status {st!r} missing from _ACTIVE_ENTRY_OWN_STATUSES — "
            f"the fence still lets duplicates through"
        )


@pytest.mark.parametrize("status", NONTERMINAL_STATUSES)
def test_disposition_never_returns_new_for_any_nonterminal_status(monkeypatch, status):
    """For every nonterminal status the resolver MUST return a disposition
    that PREVENTS a duplicate create_entry_order call — never NEW, never
    LOOKUP_FAILED. PENDING_TRIGGER→REATTACH_WATCHER; CREATED→RETRYABLE;
    everything else in the family→ALREADY_OWNED."""
    # Patch the opportunity lookup so the resolver falls through to the
    # active-order check.
    monkeypatch.setattr(
        ov, "_get_client_opportunity_row",
        lambda *_a, **_kw: ov._LookupResult(
            canonical_signal_id="canon-sig-1",
            lookup_status=ov._LS_NOT_FOUND,
            row=None,
            error=None,
        ),
    )
    # Patch the active-order query to return a row in the parametrized status.
    order_row = {
        "local_order_id":       "local-1",
        "status":               status,
        "canonical_signal_id":  "canon-sig-1",
        "execution_mode":       "paper",
        "client_id":            "jose@example.com",
        "kind":                 "ENTRY",
    }
    monkeypatch.setattr(
        ov, "_query_active_entry_order",
        lambda *_a, **_kw: (ov._LS_FOUND, order_row),
    )

    result = ov._resolve_shared_setup_disposition(
        signal_id="canon-sig-1",
        client_id="jose@example.com",
        signal={"signal_id": "canon-sig-1"},
        execution_mode="paper",
        session_key="2026-07-27",
    )

    # Never NEW (would trigger create_entry_order).
    assert result.disposition != ov._DISPOSITION_NEW, (
        f"status {status!r} produced NEW — duplicate create_entry_order path is open"
    )
    # Never LOOKUP_FAILED (test setup provided FOUND).
    assert result.disposition != ov._DISPOSITION_LOOKUP_FAILED

    if status == "PENDING_TRIGGER":
        assert result.disposition == ov._DISPOSITION_REATTACH_WATCHER
    elif status == "CREATED":
        assert result.disposition == ov._DISPOSITION_RETRYABLE
    else:
        assert result.disposition == ov._DISPOSITION_ALREADY_OWNED, (
            f"status {status!r}: expected ALREADY_OWNED, got {result.disposition!r}"
        )
