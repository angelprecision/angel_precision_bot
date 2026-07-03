"""
P0 (2026-07-02): entry metadata guard key-shadowing fix.

`_positive()` returned on the first key that merely PARSED — a placeholder
zero (or unparseable string) in an early key shadowed a valid positive value
in a later key, rejecting the whole signal as `metadata_invalid:zero_*`.
This was the mechanism behind the 2026-07-01 mass rejection wave (366 daily
signals in one session). #241/#242 patched the daily path by ADDING key
aliases; the shadowing semantics remained and would fire again on the next
producer emitting a zeroed placeholder.

New semantics match `_first_positive_value` at the broker submit boundary
(#251): skip missing/unparseable/zero-or-negative, succeed on the first
strictly-positive value, fail only after exhausting all sources and keys.
"""

from ap.entry_metadata_guard import _positive, validate_entry_metadata


UNDERLYING_KEYS = (
    "underlying_entry", "underlying_at_signal", "underlying_price",
    "current_underlying_price", "current_underlying",
    "signal_underlying_price", "price_at_signal", "trigger.current_price",
)


# ── The exact production failure shape ───────────────────────────────────────

def test_placeholder_zero_no_longer_shadows_valid_trigger_price():
    """
    underlying_entry=0 (placeholder) + valid trigger.current_price must
    PASS. Pre-fix: rejected (False, 0.0) without ever reading the trigger.
    """
    payload = {
        "underlying_entry": 0,
        "trigger": {"current_price": 189.73},
    }
    ok, val = _positive([payload], UNDERLYING_KEYS)
    assert ok is True
    assert val == 189.73


def test_zero_string_placeholder_also_falls_through():
    payload = {"underlying_entry": "0", "price_at_signal": 512.34}
    ok, val = _positive([payload], UNDERLYING_KEYS)
    assert ok is True
    assert val == 512.34


def test_unparseable_early_value_keeps_searching():
    """Pre-fix: garbage in an early key hard-rejected the signal."""
    payload = {"underlying_entry": "N/A", "underlying_price": 44.10}
    ok, val = _positive([payload], UNDERLYING_KEYS)
    assert ok is True
    assert val == 44.10


def test_negative_value_falls_through():
    payload = {"underlying_entry": -1, "underlying_at_signal": 250.0}
    ok, val = _positive([payload], UNDERLYING_KEYS)
    assert ok is True
    assert val == 250.0


def test_shadowing_across_sources_not_only_keys():
    """A zeroed first SOURCE must not shadow a valid second source."""
    src_a = {"underlying_entry": 0}
    src_b = {"underlying_entry": 101.5}
    ok, val = _positive([src_a, src_b], UNDERLYING_KEYS)
    assert ok is True
    assert val == 101.5


# ── Fail-closed behavior preserved ───────────────────────────────────────────

def test_all_zero_still_rejects():
    payload = {"underlying_entry": 0, "trigger": {"current_price": 0}}
    ok, val = _positive([payload], UNDERLYING_KEYS)
    assert ok is False
    assert val == 0  # last candidate seen, for diagnostics


def test_all_missing_still_rejects():
    ok, val = _positive([{"unrelated": 1}], UNDERLYING_KEYS)
    assert ok is False
    assert val is None


def test_all_unparseable_still_rejects_with_diagnostic_value():
    payload = {"underlying_entry": "N/A", "underlying_price": "pending"}
    ok, val = _positive([payload], UNDERLYING_KEYS)
    assert ok is False
    assert val == "pending"


def test_first_positive_wins_ordering_unchanged():
    """When multiple positives exist, precedence order is unchanged."""
    payload = {"underlying_entry": 55.0, "trigger": {"current_price": 189.73}}
    ok, val = _positive([payload], UNDERLYING_KEYS)
    assert ok is True
    assert val == 55.0


# ── End-to-end through validate_entry_metadata ───────────────────────────────

def _full_payload(**overrides):
    base = {
        "signal_id": "sig-test-0001",
        "ticker": "AAPL",
        "side": "CALL",
        "direction": "CALL",
        "timeframe": "1d",
        "score": 71.2,
        "trigger_price": 190.55,
        "underlying_entry": 0,               # the placeholder
        "trigger": {"current_price": 189.73,  # the valid value
                    "entry": 190.55},
        "target_price": 195.0,
        "stop_price": 187.0,
        "client_id": "test@client.com",
        "execution_mode": "paper",
    }
    base.update(overrides)
    return base


def test_e2e_placeholder_zero_underlying_passes_validation():
    res = validate_entry_metadata(
        plan=_full_payload(),
        client_id="test@client.com",
        execution_mode="paper",
    )
    assert res.ok, f"rejected: {res.reason} {res.details}"


def test_e2e_truly_zero_underlying_still_fails_closed():
    res = validate_entry_metadata(
        plan=_full_payload(trigger={"current_price": 0, "entry": 190.55}),
        client_id="test@client.com",
        execution_mode="paper",
    )
    assert not res.ok
    assert "zero_underlying" in str(res.reason or "")
