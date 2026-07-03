"""
P1 (2026-07-02): reconciler must persist the broker's rejection reason.

The 2026-06-25 SMCI incident left 355+ REJECTED rows whose only forensic
record was the word "REJECTED". Tradier returns reason_description on
rejected orders; the reconciler had broker_raw in hand at the call site and
dropped it at the _advance_order_to_terminal boundary.

Source-level tests (repo pattern — importing ap_reconciler triggers heavy
module init).
"""

import pathlib
import re

_REPO = pathlib.Path(__file__).resolve().parents[1]
SRC = (_REPO / "ap_reconciler.py").read_text()

ADV = re.search(
    r"def _advance_order_to_terminal\(.*?(?=\n    def )", SRC, re.S
).group(0)


def test_broker_raw_is_threaded_into_terminal_advance():
    assert "broker_raw: dict | None = None" in ADV
    assert re.search(
        r"_advance_order_to_terminal\(\s*\n\s*order, broker_status, summary, broker_raw=broker_raw\s*\n\s*\)",
        SRC,
    ), "call site must pass broker_raw"


def test_reason_extracted_from_tradier_fields():
    for field in ("reason_description", '"reason"', "ReasonDescription"):
        assert field in ADV


def test_last_error_carries_reason_when_present():
    assert 'f"{_last_error} reason={_broker_reason[:160]}"' in ADV


def test_last_error_unchanged_when_no_reason():
    """
    Backward compatibility: with no reason available, last_error must remain
    exactly the historical 'reconciler: broker_status=<s>' string — existing
    dashboards and exit_safety patterns match on it.
    """
    assert '_last_error = f"reconciler: broker_status={broker_status}"' in ADV


def test_transition_uses_the_composed_error():
    assert re.search(
        r"self\.osm\.transition\(\s*local_id,\s*new_status,\s*last_error=_last_error,",
        ADV,
    )


def test_meta_merge_is_best_effort_and_post_transition():
    """Meta forensics must never block or precede the OSM correction."""
    trans_idx = ADV.index("self.osm.transition")
    merge_idx = ADV.index("broker_reject_reason")
    assert trans_idx < merge_idx
    assert "except Exception as _meta_exc" in ADV
    assert "non-fatal" in ADV


def test_meta_merge_is_jsonb_concat_not_overwrite():
    assert "COALESCE(meta, '{}'::jsonb) || %s::jsonb" in ADV


def test_meta_merge_scoped_to_order_and_client():
    where = re.search(r"UPDATE orders\s+SET meta =.*?WHERE(.*?)\"\"\"", ADV, re.S)
    assert where
    assert "local_order_id = %s" in where.group(1)
    assert "client_id = %s" in where.group(1)


def test_reason_truncation_bounds():
    assert "[:160]" in ADV   # last_error bound
    assert "[:400]" in ADV   # meta bound
    assert "[:120]" in ADV   # alert bound
