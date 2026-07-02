from __future__ import annotations

import pathlib
import re

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
PM_PATH = REPO_ROOT / "ap" / "position_manager.py"


def _src() -> str:
    return PM_PATH.read_text(encoding="utf-8")


def _open_position_body(src: str) -> str:
    match = re.search(r"def\s+open_position\s*\(.*?(?=\n    def\s+\w)", src, re.DOTALL)
    assert match, "Could not locate APPositionManager.open_position()"
    return match.group(0)


def test_position_manager_normalizes_client_id_at_boundary():
    src = _src()
    assert 'self.client_id = str(client_id or "").strip().lower()' in src or "self.client_id = str(client_id or '').strip().lower()" in src


def test_position_manager_has_fail_closed_side_normalizer():
    src = _src()
    assert "def _normalize_position_side" in src
    assert "invalid_or_missing_position_side" in src


def test_open_position_inserts_normalized_side_not_raw_upper():
    body = _open_position_body(_src())
    assert "_normalize_position_side(side)" in body
    assert "side.upper()" not in body


def test_active_status_constant_matches_runtime_active_queries():
    src = _src()
    assert "ACTIVE_DB_STATUSES" in src or re.search(r"ACTIVE\s*=\s*\{[^}]*PARTIAL[^}]*ACTIVE", src, re.DOTALL)


# ─────────────────────────────────────────────────────────────────────────────
# PR #237: runtime behavioral tests for the fail-closed side normalizer.
# Structural grep tests above prove the code is present; these prove it works.
# ─────────────────────────────────────────────────────────────────────────────

import pytest


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("CALL",    "CALL"),
        ("call",    "CALL"),
        ("  Call ", "CALL"),
        ("BUY",     "CALL"),
        ("long",    "CALL"),
        ("CALLS",   "CALL"),
        ("bullish", "CALL"),
        ("PUT",     "PUT"),
        ("sell",    "PUT"),
        ("SHORT",   "PUT"),
        ("puts",    "PUT"),
        ("BEARISH", "PUT"),
    ],
)
def test_normalize_position_side_accepts_canonical_and_aliases(raw, expected):
    from ap.position_manager import _normalize_position_side
    assert _normalize_position_side(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [None, "", "   ", "UNKNOWN", "neutral", 0, "0", "sideways"],
)
def test_normalize_position_side_raises_on_invalid_or_missing(raw):
    from ap.position_manager import _normalize_position_side
    with pytest.raises(ValueError) as excinfo:
        _normalize_position_side(raw)
    assert str(excinfo.value).startswith("invalid_or_missing_position_side:")


def test_active_db_statuses_and_position_status_active_stay_in_sync():
    """PR #237 note in code says these MUST stay identical.  Lock that here so
    a future edit to one and not the other cannot silently drift again."""
    from ap.position_manager import ACTIVE_DB_STATUSES, PositionStatus
    assert ACTIVE_DB_STATUSES == PositionStatus.ACTIVE
