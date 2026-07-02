# tests/test_p0_signal_payload_normalization.py
# =============================================================================
# P0: Normalize production signal payload into canonical execution metadata
#
# Invariants under test:
#   1. Real production-shaped payload with `trigger` (scalar) populates entry_trigger.
#   2. Real production-shaped payload with `entry_price` populates entry_trigger (fallback).
#   3. Real production-shaped payload with `price`/`last`/`close` populates underlying_at_signal.
#   4. Missing underlying becomes absent (not zero) — caller marks DATA_PENDING.
#   5. Missing trigger becomes absent — caller handles ZERO_TRIGGER rejection.
#   6. Existing clean canonical payload is unchanged.
#   7. side/direction/timeframe/pattern/score/client_id/execution_mode are preserved.
#   8. No scanner scoring or selector threshold changes.
#
# Uses REAL production-shaped payloads, not fake clean fixtures.
#
# NOT TESTED HERE (covered by existing suites):
#   - metadata guard DATA_PENDING marking → test_p0_entry_metadata_guard.py
#   - side fail-closed → test_queue_side_fail_closed.py
#   - selector threshold constants → test_selector_candidate_audit.py
# =============================================================================

from __future__ import annotations

import pytest
from ap_signal_normalizer import normalize_raw_signal_payload


# ---------------------------------------------------------------------------
# Production-shaped payloads
# ---------------------------------------------------------------------------

# Payload shape 1: overnight Strat scanner (uses `trigger` scalar + `underlying`)
_STRAT_OVERNIGHT_PAYLOAD = {
    "signal_id":     "8d9338d0-5dde-4b7b-81ea-208039999b72",
    "ticker":        "NVDA",
    "side":          "CALL",
    "direction":     "CALL",
    "pattern":       "2-3",
    "timeframe":     "1d",
    "score":         82.5,
    "client_id":     "jasoncosby1@gmail.com",
    "execution_mode": "live",
    # Non-canonical scanner fields:
    "trigger":       152.50,      # → entry_trigger
    "underlying":    150.75,      # → underlying_at_signal
    "stop":          148.00,      # → stop_price
    "pt1":           156.00,      # → target_price
}

# Payload shape 2: intraday scanner (uses `entry_price` + `price`)
_INTRADAY_SCANNER_PAYLOAD = {
    "signal_id":     "c4f21d07-0001-4c2b-9801-000000000001",
    "ticker":        "TSLA",
    "side":          "PUT",
    "direction":     "PUT",
    "pattern":       "3-1-2",
    "timeframe":     "1d",
    "score":         76.0,
    "client_id":     "jose.vasquez4011@gmail.com",
    "execution_mode": "paper",
    # Non-canonical scanner fields:
    "entry_price":   248.00,      # → entry_trigger fallback
    "price":         247.25,      # → underlying_at_signal fallback
    "stop_price":    252.00,      # canonical — should be unchanged
    "target":        242.00,      # → target_price
}

# Payload shape 3: quote-derived scanner (last + close feed)
_QUOTE_DERIVED_PAYLOAD = {
    "signal_id":     "d7a12b09-bbbb-4a09-a111-777777777777",
    "ticker":        "AAPL",
    "side":          "CALL",
    "direction":     "CALL",
    "pattern":       "3-3",
    "timeframe":     "1d",
    "score":         68.0,
    "client_id":     "tradefluencehq@gmail.com",
    "execution_mode": "paper",
    # Non-canonical scanner fields:
    "entry_price":   195.00,      # → entry_trigger (entry_trigger absent)
    "last":          194.80,      # → underlying_at_signal fallback
    "stop":          191.50,
    "target":        198.50,
}

# Payload shape 4: already-canonical payload (everything set correctly)
_CANONICAL_PAYLOAD = {
    "signal_id":          "aaaabbbb-0000-4321-8888-ccccddddeeee",
    "ticker":             "META",
    "side":               "CALL",
    "direction":          "CALL",
    "pattern":            "2-3",
    "timeframe":          "1d",
    "score":              91.0,
    "client_id":          "jasoncosby1@gmail.com",
    "execution_mode":     "live",
    "entry_trigger":      512.50,     # canonical
    "underlying_at_signal": 510.00,   # canonical
    "stop_price":         505.00,     # canonical
    "target_price":       520.00,     # canonical
}

# Payload shape 5: truly missing both underlying and trigger
_MISSING_BOTH_PAYLOAD = {
    "signal_id":     "f0000001-0000-0000-0000-000000000000",
    "ticker":        "AMD",
    "side":          "PUT",
    "direction":     "PUT",
    "pattern":       "3-1-2",
    "timeframe":     "1d",
    "score":         70.0,
    "client_id":     "jose.vasquez4011@gmail.com",
    "execution_mode": "paper",
    # No underlying or trigger in any form
    "stop":          165.00,
    "target":        160.00,
}

# Payload shape 6: nested trigger dict (Strat trigger object)
_NESTED_TRIGGER_PAYLOAD = {
    "signal_id":     "e1234567-aaaa-bbbb-cccc-dddddddddddd",
    "ticker":        "SBUX",
    "side":          "CALL",
    "direction":     "CALL",
    "pattern":       "2-3",
    "timeframe":     "1d",
    "score":         79.0,
    "client_id":     "jasoncosby1@gmail.com",
    "execution_mode": "live",
    "trigger": {
        "entry":          97.50,   # → entry_trigger via trigger.entry
        "stop":           94.00,   # → stop_price via trigger.stop
        "pt1":            101.00,  # → target_price via trigger.pt1
    },
    "underlying":    96.80,        # → underlying_at_signal
}

# Payload shape 7: mark price from options feed
_MARK_PRICE_PAYLOAD = {
    "signal_id":     "b9999999-cafe-babe-face-000011112222",
    "ticker":        "MSFT",
    "side":          "CALL",
    "direction":     "CALL",
    "pattern":       "3-3",
    "timeframe":     "1d",
    "score":         88.0,
    "client_id":     "jasoncosby1@gmail.com",
    "execution_mode": "live",
    "trigger":       420.00,       # scalar → entry_trigger
    "mark":          419.50,       # → underlying_at_signal
    "stop":          415.00,
    "pt1":           425.00,
}


# ---------------------------------------------------------------------------
# Test 1: `trigger` (scalar) → entry_trigger
# ---------------------------------------------------------------------------

class TestTriggerScalarToEntryTrigger:
    def test_strat_overnight_trigger_scalar(self):
        """`trigger` scalar populates entry_trigger when entry_trigger absent."""
        result = normalize_raw_signal_payload(_STRAT_OVERNIGHT_PAYLOAD)
        assert result["entry_trigger"] == 152.50, (
            f"entry_trigger expected 152.50, got {result.get('entry_trigger')}"
        )

    def test_mark_price_trigger_scalar(self):
        """`trigger` scalar works on a second production payload shape."""
        result = normalize_raw_signal_payload(_MARK_PRICE_PAYLOAD)
        assert result["entry_trigger"] == 420.00

    def test_trigger_dict_skipped_uses_nested_entry(self):
        """When `trigger` is a dict (not scalar), falls back to trigger.entry."""
        result = normalize_raw_signal_payload(_NESTED_TRIGGER_PAYLOAD)
        assert result["entry_trigger"] == 97.50, (
            f"nested trigger.entry expected 97.50, got {result.get('entry_trigger')}"
        )

    def test_input_dict_is_not_mutated(self):
        """normalize_raw_signal_payload must not mutate the input dict."""
        original = dict(_STRAT_OVERNIGHT_PAYLOAD)
        _ = normalize_raw_signal_payload(_STRAT_OVERNIGHT_PAYLOAD)
        assert _STRAT_OVERNIGHT_PAYLOAD == original


# ---------------------------------------------------------------------------
# Test 2: `entry_price` → entry_trigger (fallback)
# ---------------------------------------------------------------------------

class TestEntryPriceFallbackToEntryTrigger:
    def test_entry_price_used_when_trigger_absent(self):
        """`entry_price` populates entry_trigger when trigger/entry_trigger absent."""
        result = normalize_raw_signal_payload(_INTRADAY_SCANNER_PAYLOAD)
        assert result["entry_trigger"] == 248.00, (
            f"entry_trigger expected 248.00, got {result.get('entry_trigger')}"
        )

    def test_entry_price_used_on_quote_derived_payload(self):
        """Confirms the fallback chain on the quote-derived payload shape."""
        result = normalize_raw_signal_payload(_QUOTE_DERIVED_PAYLOAD)
        assert result["entry_trigger"] == 195.00

    def test_entry_trigger_not_overwritten_when_already_positive(self):
        """Canonical entry_trigger is never overwritten if already positive."""
        result = normalize_raw_signal_payload(_CANONICAL_PAYLOAD)
        assert result["entry_trigger"] == 512.50, (
            "existing entry_trigger should not be overwritten by normalizer"
        )

    def test_zero_entry_trigger_is_resolved_from_fallback(self):
        """Zero/falsy entry_trigger is treated as absent and resolved from fallback."""
        payload = {**_INTRADAY_SCANNER_PAYLOAD, "entry_trigger": 0}
        result = normalize_raw_signal_payload(payload)
        assert result["entry_trigger"] == 248.00


# ---------------------------------------------------------------------------
# Test 3: `price`/`last`/`close` → underlying_at_signal
# ---------------------------------------------------------------------------

class TestUnderlyingAliasesToUnderlyingAtSignal:
    def test_underlying_used_first(self):
        """`underlying` is highest-priority alias for underlying_at_signal."""
        result = normalize_raw_signal_payload(_STRAT_OVERNIGHT_PAYLOAD)
        assert result["underlying_at_signal"] == 150.75

    def test_price_fallback_used_when_underlying_absent(self):
        """`price` is used when underlying/underlying_price absent."""
        result = normalize_raw_signal_payload(_INTRADAY_SCANNER_PAYLOAD)
        assert result["underlying_at_signal"] == 247.25

    def test_last_fallback_used(self):
        """`last` price populates underlying_at_signal when other aliases absent."""
        result = normalize_raw_signal_payload(_QUOTE_DERIVED_PAYLOAD)
        assert result["underlying_at_signal"] == 194.80

    def test_mark_fallback_used(self):
        """`mark` option price populates underlying_at_signal when nothing else present."""
        result = normalize_raw_signal_payload(_MARK_PRICE_PAYLOAD)
        assert result["underlying_at_signal"] == 419.50

    def test_canonical_underlying_at_signal_not_overwritten(self):
        """canonical underlying_at_signal is preserved when already positive."""
        result = normalize_raw_signal_payload(_CANONICAL_PAYLOAD)
        assert result["underlying_at_signal"] == 510.00


# ---------------------------------------------------------------------------
# Test 4: Truly missing underlying → absent, not zero
# ---------------------------------------------------------------------------

class TestMissingUnderlyingBecomesAbsent:
    def test_underlying_at_signal_absent_not_zero(self):
        """When no underlying alias present, underlying_at_signal is NOT written as zero.
        It must remain absent so the metadata guard can classify it as DATA_PENDING."""
        result = normalize_raw_signal_payload(_MISSING_BOTH_PAYLOAD)
        # Must NOT be written as 0 or False — must be genuinely absent
        assert result.get("underlying_at_signal") is None or result.get("underlying_at_signal", "ABSENT") == "ABSENT", (
            f"underlying_at_signal should be absent, got {result.get('underlying_at_signal')}"
        )
        assert "underlying_at_signal" not in result or result["underlying_at_signal"] is None

    def test_execution_ready_flag_not_set_by_normalizer(self):
        """The normalizer itself does not set allowed_for_execution or DATA_PENDING —
        that is the metadata guard's responsibility. Normalizer just maps fields."""
        result = normalize_raw_signal_payload(_MISSING_BOTH_PAYLOAD)
        # normalizer should NOT write metadata_validation_status
        assert "metadata_validation_status" not in result
        assert "allowed_for_execution" not in result


# ---------------------------------------------------------------------------
# Test 5: Truly missing trigger → absent
# ---------------------------------------------------------------------------

class TestMissingTriggerBecomesAbsent:
    def test_entry_trigger_absent_not_zero_when_missing(self):
        """When no trigger alias present, entry_trigger is not written to the result."""
        result = normalize_raw_signal_payload(_MISSING_BOTH_PAYLOAD)
        assert result.get("entry_trigger") is None or "entry_trigger" not in result, (
            f"entry_trigger should be absent, got {result.get('entry_trigger')}"
        )

    def test_zero_trigger_value_not_populated(self):
        """A trigger value of 0 must NOT be written — only positive values are valid."""
        payload = {"ticker": "XOM", "trigger": 0, "underlying": 110.0, "score": 65.0}
        result = normalize_raw_signal_payload(payload)
        # Should NOT have entry_trigger set to 0
        assert result.get("entry_trigger") is None or result.get("entry_trigger", 0) > 0


# ---------------------------------------------------------------------------
# Test 6: Clean canonical payload is unchanged
# ---------------------------------------------------------------------------

class TestCanonicalPayloadUnchanged:
    def test_all_canonical_fields_preserved(self):
        """A payload with all canonical fields set must pass through unchanged."""
        result = normalize_raw_signal_payload(_CANONICAL_PAYLOAD)
        assert result["entry_trigger"]       == 512.50
        assert result["underlying_at_signal"] == 510.00
        assert result["stop_price"]           == 505.00
        assert result["target_price"]         == 520.00

    def test_non_canonical_fields_preserved_verbatim(self):
        """Fields not in the normalization mapping are preserved verbatim."""
        result = normalize_raw_signal_payload(_CANONICAL_PAYLOAD)
        assert result["signal_id"]      == _CANONICAL_PAYLOAD["signal_id"]
        assert result["ticker"]         == "META"
        assert result["score"]          == 91.0
        assert result["client_id"]      == "jasoncosby1@gmail.com"
        assert result["execution_mode"] == "live"

    def test_non_dict_input_returned_unchanged(self):
        """Non-dict input is returned as-is — no crash."""
        assert normalize_raw_signal_payload(None) is None
        assert normalize_raw_signal_payload("string") == "string"
        assert normalize_raw_signal_payload(42) == 42


# ---------------------------------------------------------------------------
# Test 7: side/direction/timeframe/pattern/score/client_id/execution_mode preserved
# ---------------------------------------------------------------------------

class TestPreservedFields:
    def test_side_not_defaulted(self):
        """Normalizer never sets or modifies `side`."""
        result = normalize_raw_signal_payload(_STRAT_OVERNIGHT_PAYLOAD)
        assert result["side"] == "CALL"

    def test_direction_not_defaulted(self):
        """Normalizer never sets or modifies `direction`."""
        result = normalize_raw_signal_payload(_STRAT_OVERNIGHT_PAYLOAD)
        assert result["direction"] == "CALL"

    def test_side_absent_stays_absent(self):
        """If `side` is absent, normalizer does NOT supply a default."""
        payload = {"ticker": "NVDA", "trigger": 100.0, "underlying": 99.0, "score": 70.0}
        result = normalize_raw_signal_payload(payload)
        assert "side" not in result

    def test_timeframe_preserved(self):
        result = normalize_raw_signal_payload(_STRAT_OVERNIGHT_PAYLOAD)
        assert result["timeframe"] == "1d"

    def test_pattern_preserved(self):
        result = normalize_raw_signal_payload(_STRAT_OVERNIGHT_PAYLOAD)
        assert result["pattern"] == "2-3"

    def test_score_preserved(self):
        result = normalize_raw_signal_payload(_STRAT_OVERNIGHT_PAYLOAD)
        assert result["score"] == 82.5

    def test_client_id_preserved(self):
        result = normalize_raw_signal_payload(_STRAT_OVERNIGHT_PAYLOAD)
        assert result["client_id"] == "jasoncosby1@gmail.com"

    def test_execution_mode_preserved(self):
        result = normalize_raw_signal_payload(_STRAT_OVERNIGHT_PAYLOAD)
        assert result["execution_mode"] == "live"

    def test_all_shapes_preserve_non_normalized_keys(self):
        """All production-shaped payloads preserve their non-normalized fields."""
        for payload in [
            _STRAT_OVERNIGHT_PAYLOAD,
            _INTRADAY_SCANNER_PAYLOAD,
            _QUOTE_DERIVED_PAYLOAD,
            _CANONICAL_PAYLOAD,
            _MISSING_BOTH_PAYLOAD,
            _NESTED_TRIGGER_PAYLOAD,
            _MARK_PRICE_PAYLOAD,
        ]:
            result = normalize_raw_signal_payload(payload)
            for key in ("ticker", "score", "client_id", "execution_mode", "signal_id"):
                if key in payload:
                    assert result[key] == payload[key], (
                        f"key={key!r} changed during normalization for "
                        f"ticker={payload.get('ticker')}: "
                        f"was {payload[key]!r}, now {result[key]!r}"
                    )


# ---------------------------------------------------------------------------
# Test 8: No scanner scoring or selector threshold changes
# ---------------------------------------------------------------------------

class TestNoThresholdChanges:
    def test_no_selector_thresholds_touched(self):
        """ap/contract_selector.py pro-quality constants must be unchanged.
        These are the production quality gate — any accidental change affects
        all clients immediately."""
        import sys
        import types as _types

        for mod in ["ap.observability", "ap.trace", "ap.contract_quote_revalidator"]:
            if mod not in sys.modules:
                stub = _types.ModuleType(mod)
                stub.emit_decision_event = lambda *a, **kw: None
                stub.get_git_commit = lambda: "test"
                stub.make_config_hash = lambda d: "hash"
                stub.trace_gate = lambda *a, **kw: None
                stub.revalidate_with_direct_quote = lambda *a, **kw: {"action": "SKIP"}
                stub.should_revalidate = lambda r: False
                stub.DEFAULT_REVALIDATE_TOP_N = 3
                sys.modules[mod] = stub

        import ap.contract_selector as cs
        assert cs._PRO_T1_SPREAD_HARD_MAX == 0.10
        assert cs._PRO_T2_SPREAD_HARD_MAX == 0.12
        assert cs._PRO_MIN_BID == 0.10
        assert cs._PRO_MIN_BID_SIZE_HARD == 3

    def test_no_scanner_imports_in_normalizer(self):
        """ap_signal_normalizer must not import any scanner or scoring module."""
        with open("ap_signal_normalizer.py", "r") as f:
            source = f.read()
        forbidden = ["ap_master_control", "ap_contract_selector", "ap_strat_agent",
                     "ap_scoring", "ap_scanner", "ap/contract_selector"]
        for mod in forbidden:
            assert mod not in source, (
                f"Unexpected import of {mod!r} in ap_signal_normalizer.py"
            )

    def test_normalizer_does_not_touch_score(self):
        """Normalization never writes or modifies the score field."""
        for payload in [_STRAT_OVERNIGHT_PAYLOAD, _INTRADAY_SCANNER_PAYLOAD]:
            result = normalize_raw_signal_payload(payload)
            assert result.get("score") == payload.get("score")


# ---------------------------------------------------------------------------
# Integration-level: stop_price and target_price normalization
# ---------------------------------------------------------------------------

class TestStopAndTargetNormalization:
    def test_stop_alias_populates_stop_price(self):
        """`stop` scalar populates stop_price when stop_price absent."""
        result = normalize_raw_signal_payload(_STRAT_OVERNIGHT_PAYLOAD)
        assert result["stop_price"] == 148.00

    def test_pt1_alias_populates_target_price(self):
        """`pt1` populates target_price when target_price absent."""
        result = normalize_raw_signal_payload(_STRAT_OVERNIGHT_PAYLOAD)
        assert result["target_price"] == 156.00

    def test_target_alias_populates_target_price(self):
        """`target` populates target_price when target_price absent."""
        result = normalize_raw_signal_payload(_INTRADAY_SCANNER_PAYLOAD)
        assert result["target_price"] == 242.00

    def test_nested_trigger_stop_populates_stop_price(self):
        """trigger.stop populates stop_price when stop_price absent."""
        result = normalize_raw_signal_payload(_NESTED_TRIGGER_PAYLOAD)
        assert result["stop_price"] == 94.00

    def test_nested_trigger_pt1_populates_target_price(self):
        """trigger.pt1 populates target_price when target_price absent."""
        result = normalize_raw_signal_payload(_NESTED_TRIGGER_PAYLOAD)
        assert result["target_price"] == 101.00

    def test_canonical_stop_price_not_overwritten(self):
        """Canonical stop_price is preserved when already positive."""
        result = normalize_raw_signal_payload(_INTRADAY_SCANNER_PAYLOAD)
        assert result["stop_price"] == 252.00

    def test_canonical_target_price_not_overwritten(self):
        """Canonical target_price is preserved when already positive."""
        result = normalize_raw_signal_payload(_CANONICAL_PAYLOAD)
        assert result["target_price"] == 520.00
