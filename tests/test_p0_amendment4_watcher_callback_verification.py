"""P0 tests for Amendment §4: durable verification of watcher callback dispositions.

Proves the mandatory matrix from PR #323 amendment §4:

  * callback claims SUBMITTED but row remains pending → watcher retained
  * callback claims TERMINAL but row remains pending → watcher retained
  * callback claims RETRY but no valid retry timestamp → watcher retained
  * callback claims TRANSFER but owner lease missing → watcher retained
  * callback returns None → watcher retained (or infers from row)
  * callback returns malformed dict → watcher retained
  * callback claims SUBMITTED and row proves submitted → watcher removed
  * callback claims TERMINAL and row proves terminal → watcher removed
  * callback claims RETRY and row proves durable retry → RETRY_WAIT returned

Also proves:

  * SUBMITTED claim + row status submitted-family but no broker_order_id
    and durable submit_intent_at → RECONCILE_BROKER_INTENT (§6 handoff)
  * non-deferred signals preserve prior behavior (trust the claim
    verbatim; no re-read)
  * OWNERSHIP_TRANSFERRED requires current_owner, owner_token,
    generation, and a still-valid lease
  * expired owner lease demotes OWNERSHIP_TRANSFERRED to KEEP_WATCHER
  * missing required identity fields for OWNERSHIP_TRANSFERRED demote
    to KEEP_WATCHER
"""

from __future__ import annotations

import types
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from ap_entry_watcher import APEntryWatcher


# ─────────────────────────────── Helpers ────────────────────────────────


def _watcher_with_row(row):
    """Build an APEntryWatcher whose OSM.get_order returns `row`."""
    osm = MagicMock()
    osm.get_order.return_value = row
    return APEntryWatcher(None, order_state_machine=osm, mode="LIVE")


def _deferred_watched(local_order_id="oid-1"):
    return types.SimpleNamespace(signal={
        "local_order_id": local_order_id,
        "contract_symbol": "DEFERRED:SPY",
    })


def _future(seconds=30):
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def _past(seconds=30):
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()


# ═══════════════════════════════════════════════════════════════════════
# SUBMITTED claim — verification against durable row
# ═══════════════════════════════════════════════════════════════════════


def test_submitted_claim_but_row_remains_pending_retains_watcher():
    """Row still shows PENDING_TRIGGER — the callback's SUBMITTED word is
    unverified; watcher MUST NOT be removed."""
    watcher = _watcher_with_row({
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {},
    })
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "SUBMITTED"}
    )
    assert disposition == "KEEP_WATCHER"


def test_submitted_claim_with_broker_id_removes_watcher():
    """Row proves submission with a broker_order_id — release as SUBMITTED."""
    watcher = _watcher_with_row({
        "status": "SUBMITTED",
        "broker_order_id": "TR-12345",
        "submitted_ts": "2026-07-10T15:30:00+00:00",
        "meta": {"submit_intent_at": "2026-07-10T15:29:58+00:00"},
    })
    disposition, next_retry = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "SUBMITTED"}
    )
    assert disposition == "SUBMITTED"
    assert next_retry is None


def test_submitted_family_row_with_only_intent_returns_reconcile_broker_intent():
    """Amendment §4: status submitted-family + no broker_order_id + durable
    submit_intent_at → RECONCILE_BROKER_INTENT (§6 handoff, watcher retained
    by consumer)."""
    watcher = _watcher_with_row({
        "status": "SUBMITTED",
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {
            "submit_intent_at": "2026-07-10T15:29:58+00:00",
            "broker_submit_payload_hash": "abc123",
            "broker_client_tag": "AP-1",
        },
    })
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "SUBMITTED"}
    )
    assert disposition == "RECONCILE_BROKER_INTENT"


def test_submitted_family_row_without_broker_id_or_intent_retains_watcher():
    """Inconsistent durable state (submitted status but no broker id AND
    no submit intent) — retain the watcher."""
    watcher = _watcher_with_row({
        "status": "SUBMITTED",
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {},
    })
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "SUBMITTED"}
    )
    assert disposition == "KEEP_WATCHER"


# ═══════════════════════════════════════════════════════════════════════
# TERMINAL_DURABLE claim — verification
# ═══════════════════════════════════════════════════════════════════════


def test_terminal_claim_but_row_remains_pending_retains_watcher():
    """Row still PENDING — TERMINAL claim is unverified."""
    watcher = _watcher_with_row({
        "status": "PENDING_TRIGGER",
        "meta": {"reason_code": "X"},  # reason present but status not terminal
    })
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "TERMINAL_DURABLE"}
    )
    assert disposition == "KEEP_WATCHER"


def test_terminal_claim_row_terminal_but_no_reason_retains_watcher():
    """Row status is terminal but no reason_code recorded — retain watcher."""
    watcher = _watcher_with_row({
        "status": "REJECTED",
        "meta": {},
    })
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "TERMINAL_DURABLE"}
    )
    assert disposition == "KEEP_WATCHER"


def test_terminal_claim_row_terminal_with_reason_removes_watcher():
    """Row proves terminal (status + reason) — release as TERMINAL_DURABLE.

    PR #597 corrections #1/#2: the row must also carry client_id and
    execution_mode for the identity guard; a production row that reached
    a terminal state through the canonical write path always carries them.
    """
    watcher = _watcher_with_row({
        "status": "EXPIRED",
        "local_order_id": "oid-1",
        "client_id": "test@example.com",
        "execution_mode": "live",
        "meta": {"reason_code": "DEFERRED_TIMEOUT"},
    })
    watcher.client_id = "test@example.com"
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "TERMINAL_DURABLE"}
    )
    assert disposition == "TERMINAL_DURABLE"


# ═══════════════════════════════════════════════════════════════════════
# RETRY_WAIT claim — verification
# ═══════════════════════════════════════════════════════════════════════


def _valid_retry_row(next_retry_at):
    return {
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "next_retry_at": next_retry_at,
            "retry_attempt": 1,
            "retry_max_attempts": 3,
            "broker_ready": False,
        },
    }


def test_retry_claim_missing_next_retry_at_retains_watcher():
    row = _valid_retry_row(next_retry_at=None)
    row["meta"]["next_retry_at"] = None
    row["meta"]["materialization_next_retry_at"] = None
    watcher = _watcher_with_row(row)
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "RETRY_WAIT"}
    )
    assert disposition == "KEEP_WATCHER"


def test_retry_claim_missing_retry_attempt_retains_watcher():
    row = _valid_retry_row(_future(20))
    row["meta"]["retry_attempt"] = None
    watcher = _watcher_with_row(row)
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "RETRY_WAIT"}
    )
    assert disposition == "KEEP_WATCHER"


def test_retry_claim_missing_retry_max_attempts_retains_watcher():
    row = _valid_retry_row(_future(20))
    row["meta"]["retry_max_attempts"] = None
    watcher = _watcher_with_row(row)
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "RETRY_WAIT"}
    )
    assert disposition == "KEEP_WATCHER"


def test_retry_claim_broker_ready_true_retains_watcher():
    """broker_ready=True on a RETRY claim is inconsistent — retain."""
    row = _valid_retry_row(_future(20))
    row["meta"]["broker_ready"] = True
    watcher = _watcher_with_row(row)
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "RETRY_WAIT"}
    )
    assert disposition == "KEEP_WATCHER"


def test_retry_claim_broker_order_id_present_retains_watcher():
    """A broker_order_id on the row disqualifies RETRY_WAIT."""
    row = _valid_retry_row(_future(20))
    row["broker_order_id"] = "TR-9"
    watcher = _watcher_with_row(row)
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "RETRY_WAIT"}
    )
    assert disposition == "KEEP_WATCHER"


def test_retry_claim_submitted_ts_present_retains_watcher():
    row = _valid_retry_row(_future(20))
    row["submitted_ts"] = "2026-07-10T15:00:00+00:00"
    watcher = _watcher_with_row(row)
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "RETRY_WAIT"}
    )
    assert disposition == "KEEP_WATCHER"


def test_retry_claim_lifecycle_not_retry_wait_retains_watcher():
    row = _valid_retry_row(_future(20))
    row["meta"]["lifecycle_state"] = "MATERIALIZING"
    watcher = _watcher_with_row(row)
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "RETRY_WAIT"}
    )
    assert disposition == "KEEP_WATCHER"


def test_retry_claim_full_durable_state_returns_retry_wait():
    """All fields present and consistent → RETRY_WAIT returned with the
    row's next_retry_at."""
    due = _future(20)
    watcher = _watcher_with_row(_valid_retry_row(due))
    disposition, next_retry = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "RETRY_WAIT"}
    )
    assert disposition == "RETRY_WAIT"
    assert next_retry == due


# ═══════════════════════════════════════════════════════════════════════
# OWNERSHIP_TRANSFERRED claim — verification
# ═══════════════════════════════════════════════════════════════════════


def _valid_transfer_row(lease_until):
    return {
        "status": "PENDING_TRIGGER",
        "meta": {
            "current_owner": "materializer:worker-1",
            "owner_token": "tok-xyz-1",
            "owner_generation": 3,
            "materialization_lease_until": lease_until,
        },
    }


def test_transfer_claim_missing_owner_lease_retains_watcher():
    row = _valid_transfer_row(lease_until=None)
    row["meta"]["materialization_lease_until"] = None
    watcher = _watcher_with_row(row)
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "OWNERSHIP_TRANSFERRED"}
    )
    assert disposition == "KEEP_WATCHER"


def test_transfer_claim_missing_current_owner_retains_watcher():
    row = _valid_transfer_row(_future(60))
    row["meta"]["current_owner"] = ""
    watcher = _watcher_with_row(row)
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "OWNERSHIP_TRANSFERRED"}
    )
    assert disposition == "KEEP_WATCHER"


def test_transfer_claim_missing_owner_token_retains_watcher():
    row = _valid_transfer_row(_future(60))
    row["meta"]["owner_token"] = ""
    watcher = _watcher_with_row(row)
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "OWNERSHIP_TRANSFERRED"}
    )
    assert disposition == "KEEP_WATCHER"


def test_transfer_claim_expired_lease_retains_watcher():
    """A past owner_lease_until means the owner effectively released —
    watcher must not be removed."""
    watcher = _watcher_with_row(_valid_transfer_row(_past(60)))
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "OWNERSHIP_TRANSFERRED"}
    )
    assert disposition == "KEEP_WATCHER"


def test_transfer_claim_valid_returns_ownership_transferred():
    watcher = _watcher_with_row(_valid_transfer_row(_future(60)))
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "OWNERSHIP_TRANSFERRED"}
    )
    assert disposition == "OWNERSHIP_TRANSFERRED"


# ═══════════════════════════════════════════════════════════════════════
# Absent / malformed callback dict
# ═══════════════════════════════════════════════════════════════════════


def test_callback_none_with_pending_row_returns_unknown():
    """No claim + row has no verifiable state → UNKNOWN (consumer raises
    → catches → retains watcher)."""
    watcher = _watcher_with_row({
        "status": "PENDING_TRIGGER",
        "meta": {"lifecycle_state": ""},
    })
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), None
    )
    assert disposition == "UNKNOWN"


def test_callback_malformed_dict_with_pending_row_returns_unknown():
    """Malformed disposition string collapses to no-claim path."""
    watcher = _watcher_with_row({
        "status": "PENDING_TRIGGER",
        "meta": {},
    })
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "GARBAGE_STRING"}
    )
    assert disposition == "UNKNOWN"


def test_callback_none_but_row_proves_terminal_returns_terminal_durable():
    """Even without a claim, a truly terminal row surfaces as
    TERMINAL_DURABLE from the fallback branch.

    PR #597 corrections #1/#2: production terminal rows always carry
    client_id and execution_mode from the canonical write path.
    """
    watcher = _watcher_with_row({
        "status": "REJECTED",
        "local_order_id": "oid-1",
        "client_id": "test@example.com",
        "execution_mode": "live",
        "meta": {"reason_code": "BROKER_REJECTED"},
    })
    watcher.client_id = "test@example.com"
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), None
    )
    assert disposition == "TERMINAL_DURABLE"


def test_get_order_raises_returns_unknown():
    """A DB read that raises collapses to UNKNOWN — never to a
    disposition that would remove the watcher."""
    osm = MagicMock()
    osm.get_order.side_effect = RuntimeError("db down")
    watcher = APEntryWatcher(None, order_state_machine=osm, mode="LIVE")
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "SUBMITTED"}
    )
    assert disposition == "UNKNOWN"


def test_missing_local_order_id_returns_unknown():
    """Deferred signal without a local_order_id cannot be verified;
    UNKNOWN is the correct outcome (retains watcher via consumer)."""
    watcher = _watcher_with_row({"status": "SUBMITTED", "meta": {}})
    watched = types.SimpleNamespace(signal={"contract_symbol": "DEFERRED:SPY"})
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        watched, {"disposition": "SUBMITTED"}
    )
    assert disposition == "UNKNOWN"


# ═══════════════════════════════════════════════════════════════════════
# KEEP_WATCHER explicit claim
# ═══════════════════════════════════════════════════════════════════════


def test_keep_watcher_claim_is_passthrough():
    """Explicit KEEP_WATCHER is always valid — no verification needed."""
    watcher = _watcher_with_row({"status": "PENDING_TRIGGER", "meta": {}})
    disposition, next_retry = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(),
        {"disposition": "KEEP_WATCHER", "next_retry_at": "2026-07-10T20:00:00+00:00"},
    )
    assert disposition == "KEEP_WATCHER"
    assert next_retry == "2026-07-10T20:00:00+00:00"


# ═══════════════════════════════════════════════════════════════════════
# Non-deferred signals: prior behaviour preserved
# ═══════════════════════════════════════════════════════════════════════


def test_non_deferred_signal_trusts_callback_verbatim():
    """Amendment §4 explicit rule: non-deferred paths preserve their
    prior behaviour; no re-read is performed."""
    osm = MagicMock()
    watcher = APEntryWatcher(None, order_state_machine=osm, mode="LIVE")
    # Non-deferred: no "DEFERRED:" contract_symbol
    watched = types.SimpleNamespace(signal={
        "local_order_id": "oid-live",
        "contract_symbol": "SPY260717C00600000",
    })
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        watched, {"disposition": "SUBMITTED"}
    )
    assert disposition == "SUBMITTED"
    osm.get_order.assert_not_called(), (
        "non-deferred path must NOT re-read the durable row"
    )


def test_non_deferred_signal_no_claim_defaults_to_ownership_transferred():
    """Non-deferred + no callback dict → OWNERSHIP_TRANSFERRED default,
    matches prior behaviour."""
    osm = MagicMock()
    watcher = APEntryWatcher(None, order_state_machine=osm, mode="LIVE")
    watched = types.SimpleNamespace(signal={
        "local_order_id": "oid-live",
        "contract_symbol": "SPY260717C00600000",
    })
    disposition, _ = watcher._resolve_trigger_callback_disposition(watched, None)
    assert disposition == "OWNERSHIP_TRANSFERRED"


# ═══════════════════════════════════════════════════════════════════════
# RECONCILE_BROKER_INTENT explicit claim
# ═══════════════════════════════════════════════════════════════════════


def test_reconcile_broker_intent_claim_requires_row_intent():
    """RECONCILE_BROKER_INTENT is only accepted when the row actually has
    a durable submit_intent_at; otherwise collapse to KEEP_WATCHER."""
    watcher = _watcher_with_row({
        "status": "PENDING_TRIGGER",
        "meta": {},
    })
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "RECONCILE_BROKER_INTENT"}
    )
    assert disposition == "KEEP_WATCHER"


def test_reconcile_broker_intent_claim_with_row_intent_passes():
    watcher = _watcher_with_row({
        "status": "SUBMITTED",
        "broker_order_id": None,
        "meta": {"submit_intent_at": "2026-07-10T15:29:58+00:00"},
    })
    disposition, _ = watcher._resolve_trigger_callback_disposition(
        _deferred_watched(), {"disposition": "RECONCILE_BROKER_INTENT"}
    )
    assert disposition == "RECONCILE_BROKER_INTENT"
