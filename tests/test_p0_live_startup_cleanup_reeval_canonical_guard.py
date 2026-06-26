from __future__ import annotations

import os
import re


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLIENT_RUNNER = os.path.join(REPO_ROOT, "client_runner.py")

UUID = "8d9338d0-5dde-4b7b-81ea-208039999b72"


def _read(path: str) -> str:
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def _extract_cleanup_method(src: str) -> str:
    start = src.find("def _clear_old_phantom_orders(self):")
    assert start >= 0, "could not locate _clear_old_phantom_orders"
    end_match = re.search(r"\n    def _read_entries_paused", src[start:])
    assert end_match, "could not locate end of _clear_old_phantom_orders"
    return src[start : start + end_match.start()]


def _real_signal_id(signal_id: str) -> str:
    if signal_id.startswith("REEVAL:"):
        parts = signal_id.split(":")
        if len(parts) >= 2:
            return parts[1]
    return signal_id


def _order_canon_id(signal_id: str, canonical_signal_id: str | None = None) -> str:
    if canonical_signal_id:
        return canonical_signal_id
    if signal_id.startswith("REEVAL:"):
        parts = signal_id.split(":")
        if len(parts) >= 2:
            return f"REEVAL:{parts[1]}"
    return signal_id


def test_reeval_suffix_normalizes_to_plain_uuid_for_signal_proof():
    assert _real_signal_id(f"REEVAL:{UUID}:f4dc44") == UUID
    assert _real_signal_id(f"REEVAL:{UUID}") == UUID
    assert _real_signal_id(UUID) == UUID


def test_reeval_suffix_normalizes_to_canonical_reeval_id_for_order_proof():
    assert _order_canon_id(f"REEVAL:{UUID}:f4dc44") == f"REEVAL:{UUID}"
    assert _order_canon_id(f"REEVAL:{UUID}") == f"REEVAL:{UUID}"
    assert _order_canon_id(UUID) == UUID
    assert _order_canon_id(f"REEVAL:{UUID}:f4dc44", f"REEVAL:{UUID}") == f"REEVAL:{UUID}"


def test_cleanup_sql_checks_ap_signals_using_real_signal_id():
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    assert "FROM   ap_signals s" in method_src
    assert "s.signal_id::text = ca.real_signal_id" in method_src
    assert "decision_status" in method_src
    assert "('WATCHING','ARMED')" in method_src


def test_cleanup_sql_checks_trade_queue_using_reeval_and_plain_signal_shapes():
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    assert "FROM   trade_queue tq" in method_src
    assert "tq.status IN ('NEW','PROCESSING','WATCHING')" in method_src
    assert "COALESCE(tq.signal_id, '') = ca.real_signal_id" in method_src
    assert "COALESCE(tq.signal_id, '') = ca.signal_id" in method_src
    assert "COALESCE(tq.signal_id, '') = ca.order_canon_id" in method_src
    assert "COALESCE(tq.signal_id, '') LIKE (ca.order_canon_id || ':%')" in method_src


def test_cleanup_sql_uses_normalized_order_canon_for_active_peer_proof():
    method_src = _extract_cleanup_method(_read(CLIENT_RUNNER))
    assert "active_proof AS" in method_src
    assert "COALESCE(p.canonical_signal_id, p.signal_id) = ca.order_canon_id" in method_src
    assert "p.local_order_id <> ca.local_order_id" in method_src
    assert "SELECT 1 FROM active_proof ap" in method_src

