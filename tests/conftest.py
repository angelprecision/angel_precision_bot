"""Shared deterministic test-environment defaults."""

import pytest


@pytest.fixture(autouse=True)
def _open_deferred_retry_cutoff_for_tests(monkeypatch):
    """Keep lifecycle tests independent of the wall clock's ET cutoff.

    Tests that cover the cutoff explicitly override or delete this variable.
    """
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "2359")
