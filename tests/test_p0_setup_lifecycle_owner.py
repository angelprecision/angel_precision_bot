"""P0 regression coverage for process-local setup lifecycle ownership.

The setup cache must not reject the exact signal that owns a fresh setup key,
but it must continue to block a different signal and any legacy ownerless key.
Durable DB duplicate checks remain covered by test_p0_final_duplicate_honesty.py.
"""
from __future__ import annotations

import time

import ap_master_control as mc


def _setup_key() -> str:
    return "paper@example.com:MSFT:CALL:1d"


def test_package_loader_preserves_canonical_master_control_exports():
    assert mc.APMasterControl.__module__ == "ap_master_control"
    assert callable(mc._normalize_signal_side)
    assert mc._normalize_signal_side("buy") == "CALL"
    assert mc._normalize_signal_side("sell") == "PUT"


def test_same_signal_owner_is_hidden_from_setup_membership():
    key = _setup_key()
    cache = mc._LifecycleSeenSignals()
    cache.bind(setup_key=key, signal_id="sig-1")
    cache[key] = time.time()

    assert cache._owners[key] == "sig-1"
    assert key not in cache
    assert dict.__contains__(cache, key)


def test_different_signal_same_setup_remains_blocked():
    key = _setup_key()
    cache = mc._LifecycleSeenSignals()

    cache.bind(setup_key=key, signal_id="sig-1")
    cache[key] = time.time()
    cache.unbind()

    cache.bind(setup_key=key, signal_id="sig-2")
    assert key in cache


def test_legacy_ownerless_setup_key_remains_fail_closed():
    key = _setup_key()
    cache = mc._LifecycleSeenSignals()
    dict.__setitem__(cache, key, time.time())

    cache.bind(setup_key=key, signal_id="sig-1")
    assert key in cache
    assert key not in cache._owners


def test_setup_owner_is_client_isolated():
    key_a = "paper-a@example.com:MSFT:CALL:1d"
    key_b = "paper-b@example.com:MSFT:CALL:1d"
    cache = mc._LifecycleSeenSignals()

    cache.bind(setup_key=key_a, signal_id="sig-shared")
    cache[key_a] = time.time()
    cache.unbind()

    cache.bind(setup_key=key_b, signal_id="sig-shared")
    assert key_a in cache
    assert key_b not in cache


def test_pop_removes_setup_owner_metadata():
    key = _setup_key()
    cache = mc._LifecycleSeenSignals()
    cache.bind(setup_key=key, signal_id="sig-1")
    cache[key] = time.time()
    cache.unbind()

    cache.pop(key, None)
    assert not dict.__contains__(cache, key)
    assert key not in cache._owners


def test_clear_removes_all_setup_owner_metadata():
    cache = mc._LifecycleSeenSignals()
    for idx in range(2):
        key = f"paper@example.com:MSFT:CALL:{idx}d"
        cache.bind(setup_key=key, signal_id=f"sig-{idx}")
        cache[key] = time.time()
        cache.unbind()

    cache.clear()
    assert dict(cache) == {}
    assert cache._owners == {}


def test_prune_removes_expired_keys_and_owners():
    key = _setup_key()
    cache = mc._LifecycleSeenSignals()
    cache.bind(setup_key=key, signal_id="sig-1")
    cache[key] = 100.0
    cache.unbind()

    cache.prune(now_ts=2000.0, ttl_sec=1800.0)
    assert not dict.__contains__(cache, key)
    assert key not in cache._owners


def test_unrelated_signal_key_semantics_are_unchanged():
    cache = mc._LifecycleSeenSignals()
    signal_key = "sig:sig-1:paper@example.com"
    cache.bind(setup_key=_setup_key(), signal_id="sig-1")
    cache[signal_key] = time.time()

    assert signal_key in cache
    assert signal_key not in cache._owners
