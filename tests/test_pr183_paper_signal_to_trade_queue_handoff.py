"""
tests/test_pr183_paper_signal_to_trade_queue_handoff.py
PR #183 amendment — production-shape tests for ap_signals → trade_queue WATCHING handoff.

All required tests per reviewer:
  1. members table credential shape (not clients.tradier_paper_*)
  2. ap_signals without canonical_signal_id
  3. shared ap_signal fanout to Jose and Tradefluence
  4. stale WATCHING rows not enqueued (date scope)
  5. same-day duplicate existing queue row marks watcher_started_at
  6. raw tokens not persisted in trade_queue.payload
  7. schema/query error visible in enqueue_result, not silently treated as no signals
  8. missing geometry after hydration blocks and writes blocked_at_breach to ap_signals
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from ap.morning_handoff import (
    _hydrate_trigger_from_raw_payload,
    _is_shared_watch_signal_row,
    _normalize_account_id,
    _resolve_paper_creds_from_member,
    _validate_trigger_geometry,
    enqueue_watching_signals_to_trade_queue,
)

JOSE_EMAIL  = "jose.vasquez4011@gmail.com"
TRADE_EMAIL = "tradefluencehq@gmail.com"
JASON_EMAIL = "jasoncosby1@gmail.com"
TODAY       = date.today().isoformat()
TODAY_NODASH= TODAY.replace("-", "")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _member(email=JOSE_EMAIL, paper_acct="VA12345", paper_tok="paper-tok-abc",
            generic_acct=None, generic_tok=None):
    """Production members-table row shape."""
    return {
        "email":                     email,
        "approved":                  True,
        "subscription_active":       True,
        "tradier_account_mode":      "paper",
        "tradier_active_mode":       "paper",
        "execution_pod":             "pod-a",
        "tradier_paper_account_id":  paper_acct,
        "tradier_paper_access_token": paper_tok,
        "tradier_account_id":        generic_acct,
        "tradier_access_token":      generic_tok,
    }


def _signal(signal_id="sig-001", client_email=JOSE_EMAIL, ticker="NKE",
            side="PUT", entry=88.5, stop=92.0, target=84.0,
            underlying=88.5, queued_at=None, raw_payload=None):
    """ap_signals row — NO canonical_signal_id column (production shape)."""
    return {
        "signal_id":           signal_id,
        "client_email":        client_email,   # source client, not filtered by
        "ticker":              ticker,
        "side":                side,
        "score":               72.0,
        "tier":                "B",
        "pattern":             "3-2D",
        "timeframe":           "1d",
        "decision_status":     "WATCHING",
        "entry_trigger":       entry,
        "stop_price":          stop,
        "target_price":        target,
        "underlying_at_signal": underlying,
        "raw_payload":         raw_payload or {},
        "context_notes":       "test signal",
        "queued_at":           queued_at or f"{TODAY}T09:00:00+00:00",
        "watcher_started_at":  None,
        # NOTE: no canonical_signal_id — production ap_signals does not have it
    }


def _shared_signal(**kwargs):
    raw_payload = {"stage": "contract_selection", "reason_code": "market_closed_deferred"}
    raw_payload.update(kwargs.pop("raw_payload", {}) or {})
    return _signal(raw_payload=raw_payload, **kwargs)


def _run(target_client, mode, signals, members=None, fetch_err=None,
         insert_return="inserted", exists=False, dry_run=False):
    members = members or [_member(email=target_client)]
    with patch("ap.morning_handoff._fetch_paper_members_for_pod", return_value=members), \
         patch("ap.morning_handoff._fetch_today_watching_signals",
               return_value=(None, fetch_err) if fetch_err else (signals, None)), \
         patch("ap.morning_handoff._watching_row_exists", return_value=exists), \
         patch("ap.morning_handoff._insert_trade_queue_watching",
               return_value=insert_return) as mock_ins, \
         patch("ap.morning_handoff._mark_shared_signal_handoff_started") as mock_mark, \
         patch("ap.morning_handoff._update_ap_signal_blocked") as mock_block:
        result = enqueue_watching_signals_to_trade_queue(
            target_client, mode, trading_date=TODAY, dry_run=dry_run,
        )
        return result, mock_ins, mock_mark, mock_block


# ─────────────────────────────────────────────────────────────────────────────
# 1. Members-table credential shape (not clients.tradier_paper_*)
# ─────────────────────────────────────────────────────────────────────────────

def test_1_resolves_credentials_from_members_not_clients():
    """_fetch_paper_members_for_pod queries members table — never clients."""
    import ap.morning_handoff as mh
    import inspect
    src = inspect.getsource(mh._fetch_paper_members_for_pod)
    assert "FROM members" in src, "must query members table"
    assert "FROM clients" not in src, "must NOT query clients table"
    assert "tradier_account_mode" in src
    assert "tradier_active_mode"  in src
    assert "execution_pod"        in src
    assert "approved"             in src
    assert "subscription_active"  in src
    assert "tradier_paper_account_id" in src
    assert "tradier_paper_access_token" in src


def test_1b_member_not_found_returns_error():
    with patch("ap.morning_handoff._fetch_paper_members_for_pod", return_value=[]):
        result = enqueue_watching_signals_to_trade_queue(JOSE_EMAIL, "paper", trading_date=TODAY)
    assert "member_not_found_in_pod" in result["errors"]
    assert len(result["inserted"]) == 0


def test_1c_missing_paper_credentials_returns_error():
    m = _member(paper_acct="", paper_tok="", generic_acct=None, generic_tok=None)
    with patch("ap.morning_handoff._fetch_paper_members_for_pod", return_value=[m]):
        result = enqueue_watching_signals_to_trade_queue(JOSE_EMAIL, "paper", trading_date=TODAY)
    assert "paper_credentials_missing" in result["errors"]


def test_1d_account_id_normalised_strips_all_whitespace():
    """chr(10), chr(13), chr(9), spaces all stripped."""
    m = _member(paper_acct="VA12345\n\r\t  ")
    account_id, _ = _resolve_paper_creds_from_member(m)
    assert account_id == "VA12345"
    assert "\n" not in account_id
    assert "\r" not in account_id
    assert "\t" not in account_id


# ─────────────────────────────────────────────────────────────────────────────
# 2. ap_signals without canonical_signal_id
# ─────────────────────────────────────────────────────────────────────────────

def test_2_ap_signals_select_has_no_canonical_signal_id():
    import inspect
    import ap.morning_handoff as mh
    src = inspect.getsource(mh._fetch_today_watching_signals)
    assert "canonical_signal_id" not in src, (
        "_fetch_today_watching_signals must not SELECT canonical_signal_id "
        "— production ap_signals does not have that column"
    )


def test_2b_signal_without_canonical_signal_id_still_enqueued():
    sig = _shared_signal(signal_id="sig-no-canon")
    assert "canonical_signal_id" not in sig, "test fixture must not have canonical_signal_id"
    result, mock_ins, _, _ = _run(JOSE_EMAIL, "paper", [sig])
    assert len(result["inserted"]) == 1
    payload = mock_ins.call_args[1]["payload"]
    # signal_id used as canonical fallback in payload
    assert payload["signal_id"] == "sig-no-canon"


# ─────────────────────────────────────────────────────────────────────────────
# 3. Shared ap_signal fanout to Jose AND Tradefluence
# ─────────────────────────────────────────────────────────────────────────────

def test_3_fanout_same_signal_to_multiple_paper_clients():
    """
    A single ap_signals row (e.g. from a shared scanner) must produce one
    trade_queue WATCHING row per eligible paper client. The client_id in
    the inserted row is the TARGET client, not the source signal client.
    source_signal_client_email preserves the original ap_signals.client_email.
    """
    shared_signal = _shared_signal(
        signal_id="sig-shared-nke",
        client_email="scanner@system.internal",
        ticker="NKE", side="PUT",
    )

    # Run for Jose
    result_jose, ins_jose, _, _ = _run(
        JOSE_EMAIL, "paper",
        signals=[shared_signal],
        members=[_member(email=JOSE_EMAIL)],
    )
    # Run for Tradefluence
    result_trade, ins_trade, _, _ = _run(
        TRADE_EMAIL, "paper",
        signals=[shared_signal],
        members=[_member(email=TRADE_EMAIL)],
    )

    # Both get the signal inserted
    assert len(result_jose["inserted"])  == 1
    assert len(result_trade["inserted"]) == 1

    # client_id is the TARGET client
    jose_payload  = ins_jose.call_args[1]["payload"]
    trade_payload = ins_trade.call_args[1]["payload"]
    assert jose_payload["client_id"]  == JOSE_EMAIL
    assert trade_payload["client_id"] == TRADE_EMAIL

    # source_signal_client_email tracks origin
    assert jose_payload["source_signal_client_email"]  == "scanner@system.internal"
    assert trade_payload["source_signal_client_email"] == "scanner@system.internal"

    # Idempotency keys are client-scoped so they're different rows
    jose_key  = ins_jose.call_args[1]["idempotency_key"]
    trade_key = ins_trade.call_args[1]["idempotency_key"]
    assert jose_key  == f"sig-shared-nke:{JOSE_EMAIL}:{TODAY_NODASH}"
    assert trade_key == f"sig-shared-nke:{TRADE_EMAIL}:{TODAY_NODASH}"
    assert jose_key != trade_key


def test_paper_fanout_marks_source_signal_but_creates_target_trade_queue_row():
    shared_signal = _shared_signal(
        signal_id="sig-paper-fanout",
        client_email="scanner@system.internal",
        ticker="AVGO",
        side="CALL",
        entry=145.0,
        stop=140.0,
        target=152.0,
        raw_payload={"tradier_paper_access_token": "do-not-store"},
    )

    result, mock_ins, mock_mark, _ = _run(
        JOSE_EMAIL,
        "paper",
        signals=[shared_signal],
        members=[_member(email=JOSE_EMAIL)],
    )

    assert len(result["inserted"]) == 1
    mock_mark.assert_called_once_with("scanner@system.internal", "sig-paper-fanout")

    payload = mock_ins.call_args[1]["payload"]
    assert payload["signal_id"] == "sig-paper-fanout"
    assert payload["client_id"] == JOSE_EMAIL
    assert payload["source_signal_client_email"] == "scanner@system.internal"
    assert payload["execution_mode"] == "paper"
    assert "tradier_paper_access_token" not in str(payload)
    assert "tradier_access_token" not in str(payload)

    live_result, mock_enqueue = _run_handoff_audit(
        mode="live",
        stage="post_overnight_reeval",
        enqueue_result={"errors": [], "inserted": [], "signals_found": 0, "skipped_duplicate": [], "rejected": []},
    )
    assert live_result["ok"] is True
    mock_enqueue.assert_not_called()


def test_3b_unmarked_watching_row_is_not_treated_as_shared():
    sig = _signal(raw_payload={})
    assert _is_shared_watch_signal_row(sig) is False


def test_3c_market_closed_deferred_row_is_shared():
    sig = _shared_signal()
    assert _is_shared_watch_signal_row(sig) is True


def test_3d_post_market_blocked_context_row_is_shared():
    sig = _signal(raw_payload={}, queued_at=f"{TODAY}T09:00:00+00:00")
    sig["context_notes"] = "post_market_blocked: after_hours"
    assert _is_shared_watch_signal_row(sig) is True


# ─────────────────────────────────────────────────────────────────────────────
# 4. Stale WATCHING rows are not enqueued
# ─────────────────────────────────────────────────────────────────────────────

def test_4_stale_signal_not_returned_by_date_scoped_query():
    """_fetch_today_watching_signals filters queued_at >= trading_date."""
    import inspect
    import ap.morning_handoff as mh
    src = inspect.getsource(mh._fetch_today_watching_signals)
    assert "queued_at" in src and ">=" in src, (
        "query must have queued_at >= date scope to prevent stale row re-arm"
    )


def test_4b_no_signals_returned_means_nothing_enqueued():
    """Empty signal list → nothing inserted, no errors, clean result."""
    result, mock_ins, _, _ = _run(JOSE_EMAIL, "paper", signals=[])
    assert result["signals_found"] == 0
    assert len(result["inserted"]) == 0
    mock_ins.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# 5. Same-day duplicate existing queue row marks watcher_started_at
# ─────────────────────────────────────────────────────────────────────────────

def test_5_on_conflict_duplicate_marks_watcher_started_at():
    """
    When _insert_trade_queue_watching returns 'duplicate' (ON CONFLICT),
    watcher_started_at must STILL be marked — so repeated handoff calls
    don't loop forever searching for WATCHING rows.
    """
    sig = _signal(signal_id="sig-dup")
    result, _, mock_mark, _ = _run(
        JOSE_EMAIL, "paper", [sig], insert_return="duplicate",
    )
    mock_mark.assert_called_once_with(JOSE_EMAIL, "sig-dup")
    assert len(result["skipped_duplicate"]) == 1
    assert result["skipped_duplicate"][0]["reason"] == "on_conflict_do_nothing"


def test_5b_existing_watching_row_also_marks_watcher_started_at():
    """
    When _watching_row_exists returns True (row exists with any key format),
    watcher_started_at must also be marked — same idempotency reasoning.
    """
    sig = _signal(signal_id="sig-exists")
    result, mock_ins, mock_mark, _ = _run(
        JOSE_EMAIL, "paper", [sig], exists=True,
    )
    mock_ins.assert_not_called()
    mock_mark.assert_called_once()
    assert len(result["skipped_duplicate"]) == 1
    assert result["skipped_duplicate"][0]["reason"] == "watching_row_already_exists"


def test_5c_insert_error_does_not_mark_watcher_started_at():
    """On 'error' from insert, watcher_started_at must NOT be marked."""
    sig = _signal(signal_id="sig-err")
    result, _, mock_mark, _ = _run(
        JOSE_EMAIL, "paper", [sig], insert_return="error",
    )
    mock_mark.assert_not_called()
    assert any("insert_error" in e for e in result["errors"])


# ─────────────────────────────────────────────────────────────────────────────
# 6. Raw tokens not persisted in trade_queue.payload
# ─────────────────────────────────────────────────────────────────────────────

def test_6_raw_tokens_not_in_trade_queue_payload():
    """
    tradier_paper_access_token / tradier_access_token must never appear
    in trade_queue.payload. The broker resolves credentials at execution
    time via client_id + execution_mode.
    """
    sig = _signal()
    _, mock_ins, _, _ = _run(JOSE_EMAIL, "paper", [sig])

    payload = mock_ins.call_args[1]["payload"]
    payload_str = str(payload)
    assert "tradier_paper_access_token" not in payload_str
    assert "tradier_access_token"       not in payload_str
    assert "tradier_live_access_token"  not in payload_str
    # Account IDs also must not be in payload
    assert "tradier_paper_account_id"   not in payload_str
    assert "tradier_account_id"         not in payload_str


def test_6b_payload_carries_client_id_and_execution_mode_for_broker_resolution():
    """Broker resolves credentials via client_id + execution_mode."""
    sig = _signal()
    _, mock_ins, _, _ = _run(JOSE_EMAIL, "paper", [sig])
    payload = mock_ins.call_args[1]["payload"]
    assert payload["client_id"]      == JOSE_EMAIL
    assert payload["execution_mode"] == "paper"


# ─────────────────────────────────────────────────────────────────────────────
# 7. Schema/query error visible in enqueue_result
# ─────────────────────────────────────────────────────────────────────────────

def test_7_fetch_schema_error_surfaces_in_enqueue_result():
    """
    UndefinedColumn or any fetch error must appear in enqueue_result.errors
    and the handoff result must be visible as failed — not treated as 'no signals'.
    """
    result, mock_ins, _, _ = _run(
        JOSE_EMAIL, "paper",
        signals=[],  # would be empty if silently swallowed
        fetch_err="fetch_watching_signals_failed:column canonical_signal_id does not exist",
    )
    assert any("fetch_watching_signals_failed" in e for e in result["errors"]), (
        "schema/fetch error must appear in enqueue_result.errors — "
        "not silently treated as 'no signals'"
    )
    mock_ins.assert_not_called()


def test_7b_fetch_error_fails_handoff_visibly():
    """enqueue_result with errors must propagate to run_morning_handoff_audit result."""
    result, _, _, _ = _run(
        JOSE_EMAIL, "paper", signals=[],
        fetch_err="fetch_watching_signals_failed:undefinedcolumn",
    )
    assert len(result["errors"]) > 0, "must have visible errors"
    assert result["signals_found"] == 0, "signals_found should be 0 on error"


# ─────────────────────────────────────────────────────────────────────────────
# 8. Missing geometry hard-blocks and writes blocked_at_breach to ap_signals
# ─────────────────────────────────────────────────────────────────────────────

def test_8_missing_geometry_after_hydration_blocks_signal():
    """
    Signal with null entry_trigger / stop_price / target_price even after
    raw_payload hydration attempt must be REJECTED, not enqueued.
    All-null geometry is not 'no geometry present' — it's a hard block.
    """
    sig = _signal(entry=None, stop=None, target=None, raw_payload={})
    result, mock_ins, _, _ = _run(JOSE_EMAIL, "paper", [sig])
    assert len(result["rejected"]) == 1
    assert "missing_after_hydration" in result["rejected"][0]["reason"]
    mock_ins.assert_not_called()


def test_8b_blocked_signal_writes_blocked_at_breach_to_ap_signals():
    """ap_signals.decision_status must be updated to blocked_at_breach on rejection."""
    sig = _signal(signal_id="sig-bad-geometry", entry=None, stop=None, target=None)
    _, _, _, mock_block = _run(JOSE_EMAIL, "paper", [sig])
    mock_block.assert_called_once()
    call_args = mock_block.call_args
    assert call_args[0][0] == "sig-bad-geometry"  # signal_id
    assert "missing_after_hydration" in call_args[0][1]  # reason


def test_8c_invalid_put_geometry_blocks():
    """PUT stop must be ABOVE entry. stop=85 < entry=88 → blocked."""
    sig = _signal(side="PUT", entry=88.5, stop=85.0, target=84.0)
    result, mock_ins, _, mock_block = _run(JOSE_EMAIL, "paper", [sig])
    assert len(result["rejected"]) == 1
    assert "put_geometry_invalid" in result["rejected"][0]["reason"]
    mock_ins.assert_not_called()
    mock_block.assert_called_once()


def test_8d_invalid_call_geometry_blocks():
    """CALL target must be ABOVE entry. target=130 < entry=145 → blocked."""
    valid, reason = _validate_trigger_geometry("CALL", 145.0, 140.0, 130.0)
    assert valid is False
    assert "call_geometry_invalid" in reason


def test_8e_valid_geometries_pass():
    valid, _ = _validate_trigger_geometry("PUT",  88.5, 92.0, 84.0)
    assert valid
    valid, _ = _validate_trigger_geometry("CALL", 145.0, 140.0, 152.0)
    assert valid


def test_8f_dry_run_does_not_call_update_ap_signal_blocked():
    """Dry run: geometry rejection is counted but ap_signals is NOT mutated."""
    sig = _signal(entry=None, stop=None, target=None)
    result, _, _, mock_block = _run(JOSE_EMAIL, "paper", [sig], dry_run=True)
    assert len(result["rejected"]) == 1
    mock_block.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# Hydration unit tests
# ─────────────────────────────────────────────────────────────────────────────

class TestHydrateTrigger:
    def test_nested_trigger_block(self):
        sig = _signal(entry=None, stop=None, target=None,
                      raw_payload={"trigger": {"entry_trigger": 88.5,
                                               "stop_price": 92.0,
                                               "target_price": 84.0}})
        h = _hydrate_trigger_from_raw_payload(sig)
        assert h["entry_trigger"] == 88.5
        assert h["stop_price"]    == 92.0
        assert h["target_price"]  == 84.0

    def test_flat_raw_keys(self):
        sig = _signal(entry=None, stop=None, target=None,
                      raw_payload={"entry_price": 88.5, "stop": 92.0, "target": 84.0})
        h = _hydrate_trigger_from_raw_payload(sig)
        assert h["entry_trigger"] == 88.5
        assert h["stop_price"]    == 92.0
        assert h["target_price"]  == 84.0

    def test_existing_non_zero_not_overwritten(self):
        sig = _signal(entry=88.5, raw_payload={"entry_trigger": 99.9})
        h = _hydrate_trigger_from_raw_payload(sig)
        assert h["entry_trigger"] == 88.5

    def test_all_null_after_hydration_signals_no_geometry(self):
        sig = _signal(entry=None, stop=None, target=None, raw_payload={})
        h = _hydrate_trigger_from_raw_payload(sig)
        assert h["entry_trigger"] is None
        assert h["stop_price"]    is None
        assert h["target_price"]  is None


# ─────────────────────────────────────────────────────────────────────────────
# _normalize_account_id
# ─────────────────────────────────────────────────────────────────────────────

def test_normalize_strips_chr10_chr13_chr9():
    assert _normalize_account_id("VA123\n")   == "VA123"
    assert _normalize_account_id("VA123\r\n") == "VA123"
    assert _normalize_account_id("\tVA123 ")  == "VA123"
    assert _normalize_account_id(None)         == ""


# ─────────────────────────────────────────────────────────────────────────────
# PR #217 final amendment tests
# ─────────────────────────────────────────────────────────────────────────────

from ap.morning_handoff import run_morning_handoff_audit


def _run_handoff_audit(
    mode,
    stage,
    enqueue_result=None,
    enqueue_raises=False,
    *,
    watchers_requeued=0,
    recovery_error=None,
):
    """
    Simulate run_morning_handoff_audit with runner/OSM/watcher patched out.
    enqueue_result: what enqueue_watching_signals_to_trade_queue returns.
    """
    fake_runner = MagicMock()
    fake_runner.core            = MagicMock()
    fake_runner.order_state_machine = MagicMock()
    fake_runner.master_control  = MagicMock()
    fake_runner.core.entry_watcher = MagicMock()
    fake_runner.core.broker     = MagicMock()
    fake_runner.core.exit_eng   = MagicMock()
    fake_runner.email = JOSE_EMAIL
    fake_runner.mode = mode.upper()
    fake_runner.order_state_machine.client_id = JOSE_EMAIL
    fake_runner.master_control.client_id = JOSE_EMAIL
    fake_runner.master_control.mode = mode.lower()
    fake_runner.core.client_id = JOSE_EMAIL
    fake_runner.core.client_email = JOSE_EMAIL
    fake_runner.core.mode = mode.upper()
    fake_runner.core.execution_mode = mode.upper()

    def _fake_recovery_reseed(result_dict):
        if recovery_error:
            raise RuntimeError(recovery_error)
        result_dict["watchers_requeued"] = watchers_requeued

    with patch("ap.morning_handoff._resolve_runner",        return_value=fake_runner), \
         patch("ap.morning_handoff._ensure_handoff_table"), \
         patch("ap.morning_handoff._load_handoff_run_lock",  return_value=None), \
         patch("ap.morning_handoff._upsert_handoff_run_lock"), \
         patch("ap.morning_handoff._count_state",            return_value={}), \
         patch("ap.morning_handoff._has_unowned_pending_trigger_orders", return_value=False), \
         patch("ap_recovery.APStartupRecovery") as MockRecovery, \
         patch("ap.morning_handoff.enqueue_watching_signals_to_trade_queue",
               side_effect=Exception("should not be called") if enqueue_raises
               else (lambda **kw: enqueue_result or {"errors": [], "inserted": [], "signals_found": 0,
                                                      "skipped_duplicate": [], "rejected": []})) as mock_enqueue:
        instance = MockRecovery.return_value
        instance._reseed_watchers.side_effect = _fake_recovery_reseed
        result = run_morning_handoff_audit(
            client_id=JOSE_EMAIL,
            execution_mode=mode,
            stage=stage,
            dry_run=False,
            now=None,
        )
        return result, mock_enqueue


# ── Test 1: live mode does not call enqueue ──────────────────────────────────

def test_final_1_live_mode_does_not_call_enqueue():
    """
    Live (Jason): enqueue_watching_signals_to_trade_queue must NEVER be called.
    The fanout is paper-only. Live already has trade_queue rows from overnight_reeval.
    """
    result, mock_enqueue = _run_handoff_audit(
        mode="live",
        stage="post_overnight_reeval",
        enqueue_raises=False,
    )
    mock_enqueue.assert_not_called(), (
        "enqueue_watching_signals_to_trade_queue must NOT be called for live mode"
    )


def test_final_1b_live_mode_startup_stage_also_skips_enqueue():
    """startup stage for live must not enqueue either."""
    _, mock_enqueue = _run_handoff_audit(mode="live", stage="startup")
    mock_enqueue.assert_not_called()


def test_final_1c_paper_startup_stage_enqueues_for_autonomous_restart():
    """Paper startup must fan out signals without waiting for a scheduled job."""
    enqueue_ok_result = {
        "errors": [],
        "inserted": [{"signal_id": "sig-001", "ticker": "NKE"}],
        "skipped_duplicate": [],
        "rejected": [],
        "signals_found": 1,
    }
    result, mock_enqueue = _run_handoff_audit(
        mode="paper",
        stage="startup",
        enqueue_result=enqueue_ok_result,
    )
    mock_enqueue.assert_called_once()
    assert result["ok"] is True
    assert result["enqueue_result"] == enqueue_ok_result


# ── Test 2: queued_at NULL rows not selected ─────────────────────────────────

def test_final_2_queued_at_null_rows_not_in_query():
    """
    _fetch_today_watching_signals must require queued_at IS NOT NULL.
    Stale WATCHING rows with queued_at=NULL must never be selected.
    """
    import inspect
    import ap.morning_handoff as mh
    src = inspect.getsource(mh._fetch_today_watching_signals)
    assert "queued_at IS NOT NULL" in src, (
        "_fetch_today_watching_signals must have AND queued_at IS NOT NULL"
    )
    assert "queued_at IS NULL OR" not in src, (
        "queued_at IS NULL OR must be removed — stale rows must not be re-armed"
    )


def test_final_2b_null_queued_at_signal_not_enqueued():
    """
    Signal with queued_at=NULL is filtered out at the SQL level.
    The handoff must report 0 signals_found for all-null queued_at inputs.
    (Simulated by returning empty list from the date-scoped fetch.)
    """
    # The SQL WHERE queued_at IS NOT NULL filters these out; we verify via
    # the source check in test_final_2. Here we verify that an empty signals
    # list from the fetch produces 0 inserted with no errors.
    result, _, _, _ = _run(JOSE_EMAIL, "paper", signals=[])
    assert result["signals_found"] == 0
    assert len(result["inserted"]) == 0
    assert len(result["errors"])   == 0


# ── Test 3: enqueue errors cause run_morning_handoff_audit ok=False ──────────

def test_final_3_member_not_found_causes_handoff_ok_false():
    """
    member_not_found_in_pod from enqueue must make run_morning_handoff_audit
    return ok=False. The error must be visible in the handoff result.
    """
    enqueue_err_result = {
        "errors": ["member_not_found_in_pod"],
        "inserted": [], "skipped_duplicate": [], "rejected": [],
        "signals_found": 0,
    }
    result, _ = _run_handoff_audit(
        mode="paper",
        stage="post_overnight_reeval",
        enqueue_result=enqueue_err_result,
    )
    assert result["ok"] is False, (
        "member_not_found_in_pod must cause handoff ok=False"
    )
    assert result.get("error") == "member_not_found_in_pod"


def test_final_3b_fetch_schema_error_causes_handoff_ok_false():
    """
    fetch_watching_signals_failed:<msg> from enqueue must make handoff ok=False.
    Schema errors must not be silently swallowed as 'no signals'.
    """
    enqueue_err_result = {
        "errors": ["fetch_watching_signals_failed:column canonical_signal_id does not exist"],
        "inserted": [], "skipped_duplicate": [], "rejected": [],
        "signals_found": 0,
    }
    result, _ = _run_handoff_audit(
        mode="paper",
        stage="post_overnight_reeval",
        enqueue_result=enqueue_err_result,
    )
    assert result["ok"] is False
    assert "fetch_watching_signals_failed" in (result.get("error") or "")


def test_final_3c_paper_credentials_missing_causes_handoff_ok_false():
    enqueue_err_result = {
        "errors": ["paper_credentials_missing"],
        "inserted": [], "skipped_duplicate": [], "rejected": [],
        "signals_found": 0,
    }
    result, _ = _run_handoff_audit(
        mode="paper",
        stage="post_overnight_reeval",
        enqueue_result=enqueue_err_result,
        watchers_requeued=2,
    )
    assert result["ok"] is False
    assert result.get("error") == "paper_credentials_missing"
    assert result["watchers_requeued"] == 2


def test_final_3d_insert_error_causes_handoff_ok_false():
    """insert_error:<...> must also fail the handoff."""
    enqueue_err_result = {
        "errors": ["insert_error:signal_id=sig-001:client=jose.vasquez4011@gmail.com"],
        "inserted": [], "skipped_duplicate": [], "rejected": [],
        "signals_found": 1,
    }
    result, _ = _run_handoff_audit(
        mode="paper",
        stage="post_overnight_reeval",
        enqueue_result=enqueue_err_result,
        watchers_requeued=3,
    )
    assert result["ok"] is False
    assert result["watchers_requeued"] == 3


def test_final_3e_enqueue_and_recovery_errors_are_reported_independently():
    enqueue_err_result = {
        "errors": ["paper_credentials_missing"],
        "inserted": [], "skipped_duplicate": [], "rejected": [],
        "signals_found": 0,
    }
    result, _ = _run_handoff_audit(
        mode="paper",
        stage="startup",
        enqueue_result=enqueue_err_result,
        recovery_error="reseed_failed",
    )
    assert result["ok"] is False
    assert result["error"] == "paper_credentials_missing"
    assert result["errors"] == ["reseed_failed"]


def test_final_3f_live_with_no_enqueue_still_succeeds():
    """Live mode skips enqueue entirely — handoff must still succeed."""
    result, mock_enqueue = _run_handoff_audit(mode="live", stage="post_overnight_reeval")
    mock_enqueue.assert_not_called()
    assert result["ok"] is True


def test_final_3g_paper_with_no_errors_still_succeeds():
    """Paper mode with clean enqueue result — handoff succeeds."""
    enqueue_ok_result = {
        "errors": [], "inserted": [{"signal_id": "sig-001", "ticker": "NKE"}],
        "skipped_duplicate": [], "rejected": [], "signals_found": 1,
    }
    result, _ = _run_handoff_audit(
        mode="paper",
        stage="post_overnight_reeval",
        enqueue_result=enqueue_ok_result,
    )
    assert result["ok"] is True
