"""
PR F — Queue truth hardening regression tests.

Covers four pre-LIVE fixes in ap/queue.py:

1. FIX-1: _log_signal_to_db() returns bool; after-hours flow writes
   ap_signals BEFORE marking the queue job WATCHING. On write failure,
   the queue job MUST be marked terminal with reason
   "ap_signals_write_failed:after_hours_deferred" so the operator can
   see the failure instead of a silent WATCHING job with no ap_signals
   row (which overnight_reeval can never find).

2. FIX-2: The 3:15 PM ET entry cutoff MUST run BEFORE
   order_state_machine.create_entry_order(plan). The previous order
   created an OSM row (PENDING_TRIGGER) and reserved capital, then
   rejected the job — leaking a stale OSM row + reserved capital every
   late-day signal.

3. FIX-3: Fallback signal_id MUST include a uuid suffix so burst
   enqueues that arrive in the same millisecond do not collide on
   idempotency key. Scanner-provided IDs MUST be preserved verbatim.

4. FIX-4: A log.critical line MUST fire at module import (or worker
   startup) whenever ALLOW_IMMEDIATE_EXECUTION is enabled. Today it is
   read silently — operators have no audit trail of the dangerous
   override.

Tests are mostly source-level (regex / ordered substring checks) plus
two behavioral tests for the bool return and the uuid fallback. Source
checks are sufficient for ordering invariants and are robust across
refactors that preserve the contract.
"""
from __future__ import annotations

import os
import re
import sys
import importlib
import pathlib

import pytest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
QUEUE_PATH = REPO_ROOT / "ap" / "queue.py"


def _src() -> str:
    return QUEUE_PATH.read_text(encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# FIX-1: _log_signal_to_db returns bool; after-hours order is signals-then-WATCHING
# ─────────────────────────────────────────────────────────────────────────────

def test_log_signal_to_db_returns_bool_type_hint():
    """_log_signal_to_db must declare -> bool, not -> None."""
    src = _src()
    # Find the function signature; allow whitespace/newlines between params and ->.
    m = re.search(r"def\s+_log_signal_to_db\s*\([^)]*\)\s*->\s*([A-Za-z_][A-Za-z_0-9]*)\s*:",
                  src, re.DOTALL)
    assert m, "Could not locate _log_signal_to_db signature in ap/queue.py"
    assert m.group(1) == "bool", (
        f"_log_signal_to_db must return bool (got {m.group(1)}). "
        "The after-hours flow needs True/False to decide whether to mark "
        "the queue job WATCHING or terminal."
    )


def test_log_signal_to_db_has_return_true_and_return_false():
    """Body must contain both 'return True' (success) and 'return False' (write failed)."""
    src = _src()
    # Slice just the _log_signal_to_db function body.
    m = re.search(
        r"def\s+_log_signal_to_db\s*\([^)]*\)\s*->\s*bool\s*:(.*?)(?=\ndef\s+\w)",
        src,
        re.DOTALL,
    )
    assert m, "Could not slice _log_signal_to_db body (does it return -> bool yet?)"
    body = m.group(1)
    assert re.search(r"\breturn\s+True\b", body), (
        "_log_signal_to_db must `return True` after the ap_signals upsert succeeds."
    )
    assert re.search(r"\breturn\s+False\b", body), (
        "_log_signal_to_db must `return False` in its except block so the "
        "after-hours flow can mark the queue job terminal on write failure."
    )


def test_after_hours_writes_ap_signals_before_marking_watching():
    """
    In the after-hours deferred branch (market_closed_deferred),
    _log_signal_to_db(..., decision_status="WATCHING", ...) MUST be called
    BEFORE _mark_job(job_id, "WATCHING", ...). Today the order is reversed:
    queue says WATCHING but ap_signals may be empty, so overnight_reeval
    can never find the signal.
    """
    src = _src()

    # Find the after-hours block by anchor on the deferral comment + call.
    # The block lives inside _dispatch() under the contract-selection skip.
    block_match = re.search(
        r"market_closed_deferred[^\n]*\n.*?(?=\nelse:|\n    except|\n\n\s{4}if contract_selector)",
        src,
        re.DOTALL,
    )
    # Fallback: just inspect the entire _dispatch body around the deferral.
    if not block_match:
        block_match = re.search(
            r"after_hours_deferred:awaiting_overnight_reeval(.*?)return\b",
            src,
            re.DOTALL,
        )
    assert block_match, "Could not locate the after-hours deferred block"

    # Find positions of the two relevant calls inside _dispatch's after-hours
    # branch. We scan the whole _dispatch span for safety.
    disp_match = re.search(r"def\s+_dispatch\s*\(.*?(?=\ndef\s+\w)", src, re.DOTALL)
    assert disp_match, "Could not locate _dispatch in ap/queue.py"
    disp = disp_match.group(0)

    # Locate the after-hours WATCHING flow:
    #   - the _log_signal_to_db(..., decision_status="WATCHING"...) call
    #   - the _mark_job(job_id, "WATCHING", error="after_hours_deferred...") call
    # Find the after-hours _log_signal_to_db call (allow nested parens in args).
    # Anchor on the call name and the WATCHING decision_status kwarg; positions
    # are sufficient for the ordering check.
    log_iter = list(re.finditer(
        r"_log_signal_to_db\s*\(",
        disp,
    ))
    # Filter to the call that has decision_status="WATCHING" in its args.
    def _is_watching_log(m):
        # Look ahead ~800 chars for the WATCHING kwarg
        window = disp[m.start():m.start() + 1200]
        return re.search(r'decision_status\s*=\s*["\']WATCHING["\']', window) is not None
    log_iter = [m for m in log_iter if _is_watching_log(m)]

    mark_iter = list(re.finditer(
        r"_mark_job\s*\([^)]*[\"']WATCHING[\"'][^)]*after_hours_deferred",
        disp,
        re.DOTALL,
    ))
    assert log_iter, (
        "After-hours flow must call _log_signal_to_db with decision_status='WATCHING'"
    )
    assert mark_iter, (
        "After-hours flow must call _mark_job(..., 'WATCHING', ...after_hours_deferred...)"
    )

    log_pos = log_iter[0].start()
    mark_pos = mark_iter[0].start()
    assert log_pos < mark_pos, (
        "After-hours order is wrong: _mark_job(WATCHING) currently runs BEFORE "
        "_log_signal_to_db(WATCHING). If the ap_signals write fails, the queue "
        "is marked WATCHING with no signals row — overnight_reeval can never "
        "find the signal. Reorder so ap_signals is written FIRST, then mark "
        "WATCHING only if the write returned True."
    )


def test_after_hours_failure_marks_job_terminal_with_explicit_reason():
    """
    On _log_signal_to_db returning False in the after-hours branch, the
    queue job MUST be marked terminal with reason
    'ap_signals_write_failed:after_hours_deferred'. Operators must see
    failures, not a silent WATCHING job.
    """
    src = _src()
    disp_match = re.search(r"def\s+_dispatch\s*\(.*?(?=\ndef\s+\w)", src, re.DOTALL)
    assert disp_match, "Could not locate _dispatch in ap/queue.py"
    disp = disp_match.group(0)
    assert "ap_signals_write_failed:after_hours_deferred" in disp, (
        "After-hours failure path missing required reason string "
        "'ap_signals_write_failed:after_hours_deferred'. The branch must "
        "mark the job terminal (REJECTED or ERROR) with this exact reason "
        "when _log_signal_to_db returns False."
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX-2: 3:15 PM ET cutoff runs BEFORE create_entry_order()
# ─────────────────────────────────────────────────────────────────────────────

def test_cutoff_runs_before_create_entry_order():
    """
    The 'too_late_in_session' cutoff (15:15 ET) MUST be checked BEFORE
    order_state_machine.create_entry_order(plan). Today's order leaks a
    PENDING_TRIGGER OSM row and reserved capital every late-day signal.
    """
    src = _src()
    disp_match = re.search(r"def\s+_dispatch\s*\(.*?(?=\ndef\s+\w)", src, re.DOTALL)
    assert disp_match, "Could not locate _dispatch in ap/queue.py"
    disp = disp_match.group(0)

    cutoff_marker = re.search(
        r"entry_cutoff:\s*too_late_in_session",
        disp,
    )
    create_marker = re.search(
        r"order_state_machine\.create_entry_order\s*\(\s*plan\s*\)",
        disp,
    )
    assert cutoff_marker, (
        "Could not find the 'entry_cutoff: too_late_in_session' rejection in _dispatch"
    )
    assert create_marker, (
        "Could not find order_state_machine.create_entry_order(plan) in _dispatch"
    )
    assert cutoff_marker.start() < create_marker.start(), (
        "3:15 PM ET cutoff must run BEFORE create_entry_order(). "
        "Today's order creates an OSM row + reserves capital, then rejects "
        "the late job — leaking a PENDING_TRIGGER row and locked capital."
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX-3: Fallback signal_id uses uuid; scanner-provided IDs preserved
# ─────────────────────────────────────────────────────────────────────────────

def test_fallback_signal_id_uses_uuid_suffix():
    """
    enqueue_signal must build the fallback signal_id with a uuid suffix —
    not just a timestamp. Timestamp-only collides on burst enqueues
    within the same millisecond.
    """
    src = _src()
    enq_match = re.search(r"def\s+enqueue_signal\s*\(.*?(?=\ndef\s+\w)", src, re.DOTALL)
    assert enq_match, "Could not locate enqueue_signal in ap/queue.py"
    enq = enq_match.group(0)

    # Match: signal_id = payload.get("signal_id") or f"signal_{uuid...
    fallback = re.search(
        r"signal_id\s*=\s*payload\.get\(\s*[\"']signal_id[\"']\s*\)\s*or\s*f?[\"'][^\"']*\{[^}]*uuid[^}]*\}",
        enq,
    )
    assert fallback, (
        "enqueue_signal fallback signal_id MUST embed a uuid suffix. "
        "Found pattern is still timestamp-only — burst enqueues collide. "
        "Expected: signal_id = payload.get('signal_id') or f'signal_{uuid.uuid4().hex[:12]}_{_now_iso()}'"
    )


def test_enqueue_signal_preserves_scanner_provided_id():
    """
    Behavioral: when payload carries a signal_id, enqueue_signal must
    NOT overwrite it. We assert via source: the fallback uses `or`, not
    unconditional reassignment. (Source-level guard is enough — a full
    behavioral test would need a DB stub the rest of the file doesn't
    set up.)
    """
    src = _src()
    enq_match = re.search(r"def\s+enqueue_signal\s*\(.*?(?=\ndef\s+\w)", src, re.DOTALL)
    assert enq_match
    enq = enq_match.group(0)
    # `payload.get("signal_id") or fallback` — `or` short-circuits when truthy,
    # preserving the scanner ID. An unconditional `signal_id = ...` would be wrong.
    assert re.search(
        r"signal_id\s*=\s*payload\.get\(\s*[\"']signal_id[\"']\s*\)\s*or\s+",
        enq,
    ), (
        "enqueue_signal must preserve scanner-provided signal_id via `or` "
        "short-circuit. Any unconditional assignment would clobber the "
        "scanner ID — breaking idempotency across scanner restarts."
    )


# ─────────────────────────────────────────────────────────────────────────────
# FIX-4: log.critical on ALLOW_IMMEDIATE_EXECUTION=1 at import/worker startup
# ─────────────────────────────────────────────────────────────────────────────

def test_allow_immediate_execution_logs_critical_at_import_or_startup():
    """
    When ALLOW_IMMEDIATE_EXECUTION is truthy, a log.critical MUST fire at
    module import OR inside worker_loop startup. Silent reads of a
    money-affecting kill switch are unacceptable.
    """
    src = _src()

    # Acceptable patterns: either
    #   - module-level guard right after ALLOW_IMMEDIATE_EXECUTION is set, OR
    #   - guard inside worker_loop() before the polling while loop.
    # Both look like: `if ALLOW_IMMEDIATE_EXECUTION: log.critical(...)`
    pattern = re.compile(
        r"if\s+ALLOW_IMMEDIATE_EXECUTION\s*:\s*\n\s*log\.critical\s*\(",
        re.DOTALL,
    )
    assert pattern.search(src), (
        "Missing required `if ALLOW_IMMEDIATE_EXECUTION: log.critical(...)` "
        "guard in ap/queue.py. The override must leave a loud audit trail "
        "at module import or worker startup."
    )


def test_allow_immediate_execution_critical_mentions_override():
    """The critical message must clearly identify what's happening."""
    src = _src()
    # Grab the immediate-execution warning context: any log.critical line
    # whose argument references ALLOW_IMMEDIATE_EXECUTION (verbatim or by
    # describing immediate execution).
    found = re.search(
        r"log\.critical\s*\([^)]*(?:ALLOW_IMMEDIATE_EXECUTION|IMMEDIATE_EXECUTION_ENABLED|immediate execution)[^)]*\)",
        src,
        re.IGNORECASE | re.DOTALL,
    )
    assert found, (
        "log.critical for ALLOW_IMMEDIATE_EXECUTION must mention the override "
        "by name (e.g. 'ALLOW_IMMEDIATE_EXECUTION=1') so the Render logs are "
        "self-explanatory."
    )


# ─────────────────────────────────────────────────────────────────────────────
# Behavioral: _log_signal_to_db actually returns True on success and False on failure
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def queue_mod(monkeypatch):
    """Import ap.queue with the supabase client patched to a controllable stub."""
    # Ensure repo root on path
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    # Fresh import so monkeypatched _get_sb_client takes effect across tests
    if "ap.queue" in sys.modules:
        importlib.reload(sys.modules["ap.queue"])
    import ap.queue as q  # noqa: WPS433
    return q


def test_log_signal_to_db_returns_true_on_success(queue_mod, monkeypatch):
    """When the upsert succeeds, _log_signal_to_db returns True."""
    class _Tbl:
        def upsert(self, *a, **kw):  # noqa: ANN001, ANN201, D401
            return self
        def execute(self):  # noqa: ANN201, D401
            return {"data": [{"signal_id": "x"}]}

    class _Sbc:
        def table(self, _name):  # noqa: ANN001, ANN201, D401
            return _Tbl()

    monkeypatch.setattr(queue_mod, "_get_sb_client", lambda: _Sbc())

    rv = queue_mod._log_signal_to_db(
        signal_id="sig-1",
        client_id="c1",
        ticker="AAPL",
        side="CALL",
        score=70.0,
        stage="contract_selection",
        reason_code="market_closed_deferred",
        human_reason="after hours",
        payload={"signal_id": "sig-1", "side": "CALL", "score": 70.0},
        decision_status="WATCHING",
    )
    assert rv is True, "Successful ap_signals upsert must return True"


def test_log_signal_to_db_returns_false_on_failure(queue_mod, monkeypatch):
    """When the upsert raises, _log_signal_to_db returns False (non-fatal logging preserved)."""
    class _BadTbl:
        def upsert(self, *a, **kw):  # noqa: ANN001, ANN201, D401
            raise RuntimeError("supabase down")
        def execute(self):  # noqa: ANN201, D401
            raise RuntimeError("never reached")

    class _BadSbc:
        def table(self, _name):  # noqa: ANN001, ANN201, D401
            return _BadTbl()

    monkeypatch.setattr(queue_mod, "_get_sb_client", lambda: _BadSbc())

    rv = queue_mod._log_signal_to_db(
        signal_id="sig-2",
        client_id="c1",
        ticker="AAPL",
        side="CALL",
        score=70.0,
        stage="contract_selection",
        reason_code="market_closed_deferred",
        human_reason="after hours",
        payload={"signal_id": "sig-2"},
        decision_status="WATCHING",
    )
    assert rv is False, "Failed ap_signals upsert must return False, not None"


def test_log_signal_to_db_returns_false_when_supabase_client_unavailable(queue_mod, monkeypatch):
    """No supabase client → no audit trail → return False (force terminal in after-hours)."""
    monkeypatch.setattr(queue_mod, "_get_sb_client", lambda: None)
    rv = queue_mod._log_signal_to_db(
        signal_id="sig-3",
        client_id="c1",
        ticker="AAPL",
        side="CALL",
        score=70.0,
        stage="contract_selection",
        reason_code="market_closed_deferred",
        human_reason="after hours",
        payload={"signal_id": "sig-3"},
        decision_status="WATCHING",
    )
    assert rv is False, (
        "When _get_sb_client() returns None we cannot persist the signal — "
        "must return False so after-hours flow does NOT lie about WATCHING."
    )
