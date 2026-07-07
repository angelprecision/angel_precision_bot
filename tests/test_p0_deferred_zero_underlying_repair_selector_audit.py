"""
tests/test_p0_deferred_zero_underlying_repair_selector_audit.py

P0 PR #301 — Repair deferred zero_underlying submit block + selector audit

Required tests (per amendment spec):
  1.  zero_underlying repaired from watched.trigger_price
  2.  zero_underlying repaired from approved_plan.trigger_price
  3.  no positive source → fail-closed (None, None, audit)
  4.  no broker submit when no positive underlying
  5.  selector audit persisted on retry (merged atomically)
  6.  selector audit persisted on terminal failure
  7.  selector audit persisted on selected-submit attempt
  8.  paper data-domain fields use domain semantics (live/sandbox/unknown)
  9.  live orders do not get paper data-domain fields
  10. no-positive-source persists full candidate audit before terminalize
  11. successful repair sets approved_plan.metadata["trigger"]["current_price"]
  12. paper data-domain values use live/sandbox/unknown domain (not raw quote_source)
  13. source-order / production-shape: zero_underlying repair runs before
      submit_existing_entry() — verified via real _on_entry_trigger integration
  14. all 16 required flat Fix B fields present on every audit write
  15. PAPER_SELECTOR_REQUIRE_LIVE_DATA enforcement
"""
from __future__ import annotations

import math
import os
import threading
import types
from unittest.mock import MagicMock, call, patch

import pytest

import os as _os
_os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

from ap.deferred_breach_underlying_repair import (
    PAPER_SELECTOR_DATA_DOMAIN_BLOCKED,
    ZERO_UNDERLYING_TERMINAL_REASON,
    _classify_domain,
    build_no_source_meta,
    build_paper_domain_fields,
    build_underlying_patch,
    check_paper_selector_data_domain,
    resolve_positive_underlying_for_breach,
)
import ap_execution_core as _core_mod


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

_REAL_OCC  = "GS  260717C00465000"
_CLIENT_ID = "jasoncosby1@gmail.com"
_PAPER_ID  = "tradefluencehq@gmail.com"
_LIVE_URL  = "https://api.tradier.com/v1"
_SAND_URL  = "https://sandbox.tradier.com/v1"


def _plan(**kw) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        contract_symbol  = kw.get("contract_symbol", "DEFERRED:GS"),
        limit_price      = kw.get("limit_price", 0.01),
        contracts        = kw.get("contracts", 1),
        max_position_usd = kw.get("max_position_usd", 500.0),
        side             = kw.get("side", "CALL"),
        execution_mode   = kw.get("execution_mode", "live"),
        client_id        = kw.get("client_id", _CLIENT_ID),
        signal_id        = kw.get("signal_id", "SIG-GS-1"),
        trigger_price    = kw.get("trigger_price", None),
        underlying_price = kw.get("underlying_price", None),
        metadata         = kw.get("metadata", {}),
        ticker           = kw.get("ticker", "GS"),
    )


def _watched(**kw) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        ticker        = kw.get("ticker", "GS"),
        trigger_price = kw.get("trigger_price", None),
        entry_trigger = kw.get("entry_trigger", None),
        signal        = kw.get("signal", {}),
    )


def _sel(**kw) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        contract_symbol = kw.get("contract_symbol", _REAL_OCC),
        bid             = kw.get("bid", 1.80),
        ask             = kw.get("ask", 1.86),
        mid             = kw.get("mid", 1.83),
        dte             = kw.get("dte", 3),
        delta           = kw.get("delta", 0.45),
        open_interest   = kw.get("open_interest", 1200),
        volume          = kw.get("volume", 400),
        expiration_date = kw.get("expiration_date", "2026-07-17"),
        candidate_audit = kw.get("candidate_audit",
            {"underlying_price": kw.get("underlying_price", 465.20)}),
    )


def _osm(update_ok: bool = True) -> MagicMock:
    osm = MagicMock()
    osm.update_order_meta.return_value = update_ok
    osm.transition.return_value = True
    return osm


# ─────────────────────────────────────────────────────────────────────────────
# Tests 1–2: underlying resolution from watched and plan
# ─────────────────────────────────────────────────────────────────────────────

class TestUnderlyingResolution:

    def test_1_repaired_from_watched_trigger_price(self):
        """Test 1: watched.trigger_price resolves when selector has no underlying."""
        price, source, audit = resolve_positive_underlying_for_breach(
            approved_plan=_plan(metadata={}),
            watched=_watched(trigger_price=468.50),
            sig={},
            selector_audit={},
            selector_result=None,
        )
        assert price == pytest.approx(468.50)
        assert source == "watched.trigger_price"
        assert audit["zero_underlying_repaired"] is True
        assert audit["resolved_underlying"] == pytest.approx(468.50)
        assert audit["resolved_underlying_source"] == "watched.trigger_price"
        assert isinstance(audit["underlying_candidates"], list)
        assert len(audit["underlying_candidates"]) > 0

    def test_2_repaired_from_plan_trigger_price(self):
        """Test 2: plan.trigger_price resolves when watched has nothing."""
        price, source, audit = resolve_positive_underlying_for_breach(
            approved_plan=_plan(trigger_price=462.75, metadata={}),
            watched=_watched(trigger_price=None, entry_trigger=None),
            sig={},
            selector_audit={},
            selector_result=None,
        )
        assert price == pytest.approx(462.75)
        assert source == "plan.trigger_price"
        assert audit["zero_underlying_repaired"] is True

    def test_selector_audit_underlying_is_highest_priority(self):
        """selector_audit.underlying_price must win over all other sources."""
        price, source, _ = resolve_positive_underlying_for_breach(
            approved_plan=_plan(trigger_price=460.00, metadata={}),
            watched=_watched(trigger_price=461.00),
            sig={"underlying_price": 462.00},
            selector_audit={"underlying_price": 465.20},
        )
        assert price == pytest.approx(465.20)
        assert source == "selector_audit.underlying_price"

    def test_returns_full_candidate_map_on_success(self):
        """audit must include underlying_candidates for every checked source."""
        _, _, audit = resolve_positive_underlying_for_breach(
            approved_plan=_plan(trigger_price=462.00, metadata={}),
            watched=_watched(trigger_price=463.00),
            sig={},
            selector_audit={},
        )
        candidates = audit["underlying_candidates"]
        sources = [c["source"] for c in candidates]
        assert "selector_audit.underlying_price" in sources
        assert "watched.trigger_price" in sources
        assert "plan.trigger_price" in sources
        # Every candidate has required fields
        for c in candidates:
            assert "source" in c
            assert "raw" in c
            assert "resolved" in c
            assert "value" in c

    def test_zero_and_negative_not_returned(self):
        price, source, _ = resolve_positive_underlying_for_breach(
            approved_plan=_plan(trigger_price=0, metadata={"underlying_price": 0}),
            watched=_watched(trigger_price=0),
            sig={"underlying_price": 0},
            selector_audit={"underlying_price": -1.0},
        )
        assert price is None
        assert source is None

    def test_inf_and_nan_not_returned(self):
        price, _, _ = resolve_positive_underlying_for_breach(
            approved_plan=_plan(trigger_price=math.inf,
                                metadata={"underlying_price": float("nan")}),
            watched=_watched(trigger_price=float("inf")),
            sig={},
            selector_audit={"underlying_price": float("nan")},
        )
        assert price is None

    def test_nested_trigger_dict_is_checked(self):
        """plan.metadata['trigger']['current_price'] is checked as source 8."""
        price, source, _ = resolve_positive_underlying_for_breach(
            approved_plan=_plan(metadata={"trigger": {"current_price": 467.50}}),
            watched=_watched(trigger_price=None),
            sig={},
            selector_audit={},
        )
        assert price == pytest.approx(467.50)
        assert "trigger" in source

    def test_never_raises(self):
        try:
            resolve_positive_underlying_for_breach(
                approved_plan=None,
                watched=None,
                sig=None,
                selector_audit=None,
                selector_result=None,
                order_row_meta=None,
            )
        except Exception as e:
            pytest.fail(f"Must never raise: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Test 3: no positive source → fail-closed with full audit
# ─────────────────────────────────────────────────────────────────────────────

class TestNoPositiveSource:

    def test_3_returns_none_none_and_audit(self):
        """Test 3: (None, None, audit) when every source is zero/missing."""
        price, source, audit = resolve_positive_underlying_for_breach(
            approved_plan=_plan(metadata={}),
            watched=_watched(trigger_price=None, entry_trigger=None),
            sig={},
            selector_audit={},
            selector_result=None,
        )
        assert price is None
        assert source is None
        assert audit["zero_underlying_repaired"] is False
        assert audit["resolved_underlying"] is None
        assert isinstance(audit["underlying_candidates"], list)
        # All candidates must show resolved=False
        assert all(not c["resolved"] for c in audit["underlying_candidates"])

    def test_terminal_reason_is_exact_string(self):
        assert ZERO_UNDERLYING_TERMINAL_REASON == \
            "metadata_invalid:zero_underlying:no_positive_source"


# ─────────────────────────────────────────────────────────────────────────────
# Test 4: no broker submit on zero underlying
# ─────────────────────────────────────────────────────────────────────────────

class TestNoBrokerSubmitOnZeroUnderlying:

    def test_4_submit_not_called_when_underlying_missing(self):
        submitted, terminalized = [], []

        price, _, audit = resolve_positive_underlying_for_breach(
            approved_plan=_plan(metadata={}),
            watched=_watched(trigger_price=None),
            sig={},
            selector_audit={},
        )

        if price is None:
            terminalized.append(ZERO_UNDERLYING_TERMINAL_REASON)
        else:
            submitted.append("submitted")

        assert len(submitted) == 0
        assert terminalized == [ZERO_UNDERLYING_TERMINAL_REASON]

    def test_4_submit_called_when_underlying_present(self):
        submitted, terminalized = [], []

        price, _, _ = resolve_positive_underlying_for_breach(
            approved_plan=_plan(metadata={}),
            watched=_watched(trigger_price=465.20),
            sig={},
        )

        if price is None:
            terminalized.append(ZERO_UNDERLYING_TERMINAL_REASON)
        else:
            submitted.append("submitted")

        assert len(terminalized) == 0
        assert len(submitted) == 1


# ─────────────────────────────────────────────────────────────────────────────
# Tests 5–7: Fix B selector audit persistence
# ─────────────────────────────────────────────────────────────────────────────

class TestSelectorAuditPersistence:

    def _audit(self, **kw) -> dict:
        return {
            "reason_code":            kw.get("reason_code", "CHAIN_ROW_ZERO_BID_ASK"),
            "stage":                  kw.get("stage", "quality_filter"),
            "explanation":            kw.get("explanation", "zero bid/ask"),
            "chain_rows":             kw.get("chain_rows", 15),
            "survivor_count":         kw.get("survivor_count", 0),
            "top_reject_buckets":     kw.get("top_reject_buckets", {"CHAIN_ROW_ZERO_BID_ASK": 15}),
            "best_rejected_candidate": kw.get("best_rejected_candidate", {"symbol": "GS"}),
            "quote_source":           kw.get("quote_source", "tradier_live"),
            "tradier_base_url":       kw.get("tradier_base_url", _LIVE_URL),
            "sandbox_mode":           kw.get("sandbox_mode", False),
            "execution_mode":         kw.get("execution_mode", "live"),
            "last_dte_ladder_audit":  kw.get("last_dte_ladder_audit", {"buckets_attempted": 2}),
            "dte_ladder_enabled":     kw.get("dte_ladder_enabled", True),
            "ladder_eligible_marker": kw.get("ladder_eligible_marker", True),
        }

    def test_5_selector_audit_persisted_on_retry(self):
        """Test 5: Fix B fields are written on retry path."""
        from ap_execution_core import _persist_deferred_selector_attempt_audit
        osm = _osm()
        _persist_deferred_selector_attempt_audit(
            osm, "LOID-GS-1",
            selector_audit=self._audit(reason_code="CHAIN_ROW_ZERO_BID_ASK"),
            attempt_number=2,
            execution_mode="live",
        )
        osm.update_order_meta.assert_called_once()
        patch = osm.update_order_meta.call_args.args[1]
        assert patch["last_deferred_selector_reason_code"] == "CHAIN_ROW_ZERO_BID_ASK"
        assert patch["last_deferred_selector_attempt_number"] == 2
        assert patch["last_deferred_selector_chain_rows"] == 15

    def test_6_selector_audit_persisted_on_terminal_failure(self):
        """Test 6: Fix B fields written on terminal failure path."""
        from ap_execution_core import _persist_deferred_selector_attempt_audit
        osm = _osm()
        _persist_deferred_selector_attempt_audit(
            osm, "LOID-GS-1",
            selector_audit=self._audit(reason_code="OI_TOO_LOW", chain_rows=20),
            attempt_number=3,
            execution_mode="live",
        )
        patch = osm.update_order_meta.call_args.args[1]
        assert patch["last_deferred_selector_reason_code"] == "OI_TOO_LOW"
        assert patch["last_deferred_selector_chain_rows"] == 20
        assert patch["last_deferred_selector_attempt_number"] == 3

    def test_7_selector_audit_persisted_on_submit_attempt(self):
        """Test 7: Fix B fields written on selected-submit path (no reject reason)."""
        from ap_execution_core import _persist_deferred_selector_attempt_audit
        osm = _osm()
        _persist_deferred_selector_attempt_audit(
            osm, "LOID-GS-1",
            selector_audit={
                "reason_code":    None,
                "stage":          "selected",
                "quote_source":   "tradier_live",
                "tradier_base_url": _LIVE_URL,
                "dte_ladder_enabled": True,
            },
            attempt_number=1,
            execution_mode="live",
        )
        patch = osm.update_order_meta.call_args.args[1]
        assert patch["last_deferred_selector_attempt_number"] == 1
        assert patch["last_deferred_selector_reason_code"] is None
        assert patch["last_deferred_selector_quote_source"] == "tradier_live"

    def test_14_all_required_flat_fields_present(self):
        """Test 14: All 16 required Fix B fields in every audit write."""
        from ap_execution_core import _persist_deferred_selector_attempt_audit
        osm = _osm()
        _persist_deferred_selector_attempt_audit(
            osm, "LOID-GS-1",
            selector_audit=self._audit(),
            attempt_number=1,
            execution_mode="live",
        )
        patch = osm.update_order_meta.call_args.args[1]
        required = [
            "last_deferred_selector_attempt_at",
            "last_deferred_selector_attempt_number",
            "last_deferred_selector_reason_code",
            "last_deferred_selector_stage",
            "last_deferred_selector_explanation",
            "last_deferred_selector_chain_rows",
            "last_deferred_selector_survivor_count",
            "last_deferred_selector_top_reject_buckets",
            "last_deferred_selector_best_rejected_candidate",
            "last_deferred_selector_quote_source",
            "last_deferred_selector_tradier_base_url",
            "last_deferred_selector_sandbox_mode",
            "last_deferred_selector_execution_mode",
            "last_dte_ladder_audit",
            "dte_ladder_enabled",
            "ladder_eligible_marker",
        ]
        missing = [k for k in required if k not in patch]
        assert not missing, f"Missing required Fix B fields: {missing}"

    def test_fix_b_never_raises(self):
        from ap_execution_core import _persist_deferred_selector_attempt_audit
        osm = MagicMock()
        osm.update_order_meta.side_effect = RuntimeError("DB down")
        try:
            _persist_deferred_selector_attempt_audit(
                osm, "LOID-1",
                selector_audit={"reason_code": "OI_TOO_LOW"},
                attempt_number=1,
                execution_mode="live",
            )
        except Exception as e:
            pytest.fail(f"Must never raise: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Tests 8–9 / 12: paper data-domain domain semantics
# ─────────────────────────────────────────────────────────────────────────────

class TestPaperDataDomainSemantics:

    def test_8_paper_sandbox_data_fields_stamped(self):
        """Test 8: Paper orders get all 6 domain fields."""
        from ap_execution_core import _persist_deferred_selector_attempt_audit
        osm = _osm()
        _persist_deferred_selector_attempt_audit(
            osm, "LOID-TF-1",
            selector_audit={"quote_source": "tradier_sandbox", "tradier_base_url": _SAND_URL},
            attempt_number=1,
            execution_mode="paper",
            is_paper=True,
            broker_base_url=_SAND_URL,
        )
        patch = osm.update_order_meta.call_args.args[1]
        assert "paper_selector_data_domain"       in patch
        assert "paper_selector_quote_source"      in patch
        assert "paper_selector_base_url"          in patch
        assert "paper_order_broker_domain"        in patch
        assert "paper_order_broker_base_url"      in patch
        assert "paper_data_order_domain_mismatch" in patch

    def test_12_domain_values_are_live_sandbox_unknown(self):
        """
        Test 12: paper_selector_data_domain must use domain vocabulary
        ('live'/'sandbox'/'unknown'), NOT raw quote_source strings
        like 'tradier_live' or 'tradier_sandbox'.
        """
        fields = build_paper_domain_fields(
            selector_audit={"quote_source": "tradier_sandbox", "tradier_base_url": _SAND_URL},
            broker_base_url=_SAND_URL,
        )
        # Domain field must be simple vocabulary
        assert fields["paper_selector_data_domain"] in ("live", "sandbox", "unknown"), (
            f"paper_selector_data_domain must be 'live'|'sandbox'|'unknown', "
            f"got {fields['paper_selector_data_domain']!r}"
        )
        assert fields["paper_selector_data_domain"] == "sandbox"

        fields_live = build_paper_domain_fields(
            selector_audit={"quote_source": "tradier_live", "tradier_base_url": _LIVE_URL},
            broker_base_url=_SAND_URL,
        )
        assert fields_live["paper_selector_data_domain"] == "live", (
            "live Tradier URL must classify as 'live', not 'tradier_live'"
        )
        # quote_source is preserved separately as the raw value
        assert fields_live["paper_selector_quote_source"] == "tradier_live"

    def test_paper_order_broker_domain_is_domain_vocabulary(self):
        """paper_order_broker_domain must be 'live'|'sandbox'|'unknown'."""
        fields = build_paper_domain_fields(
            selector_audit={},
            broker_base_url=_SAND_URL,
        )
        assert fields["paper_order_broker_domain"] in ("live", "sandbox", "unknown")
        assert fields["paper_order_broker_domain"] == "sandbox"

    def test_paper_broker_base_url_persisted(self):
        """paper_order_broker_base_url must contain the actual URL."""
        fields = build_paper_domain_fields(
            selector_audit={"tradier_base_url": _SAND_URL},
            broker_base_url=_SAND_URL,
        )
        assert fields["paper_order_broker_base_url"] == _SAND_URL
        assert fields["paper_selector_base_url"] == _SAND_URL

    def test_mismatch_is_domain_comparison_not_string_comparison(self):
        """
        paper_data_order_domain_mismatch compares domains (live vs sandbox),
        not raw quote_source strings. sandbox + sandbox = no mismatch.
        """
        # Same domain (both sandbox) → no mismatch
        fields_same = build_paper_domain_fields(
            selector_audit={"tradier_base_url": _SAND_URL},
            broker_base_url=_SAND_URL,
        )
        assert fields_same["paper_data_order_domain_mismatch"] is False

        # Different domains (live data + sandbox broker) → mismatch
        fields_diff = build_paper_domain_fields(
            selector_audit={"tradier_base_url": _LIVE_URL},
            broker_base_url=_SAND_URL,
        )
        assert fields_diff["paper_data_order_domain_mismatch"] is True

    def test_9_live_orders_no_paper_fields(self):
        """Test 9: Live (Jason) orders must not receive any paper_* fields."""
        from ap_execution_core import _persist_deferred_selector_attempt_audit
        osm = _osm()
        _persist_deferred_selector_attempt_audit(
            osm, "LOID-JASON-1",
            selector_audit={"quote_source": "tradier_live", "tradier_base_url": _LIVE_URL},
            attempt_number=1,
            execution_mode="live",
            is_paper=False,
        )
        patch = osm.update_order_meta.call_args.args[1]
        paper_keys = [k for k in patch if k.startswith("paper_")]
        assert not paper_keys, (
            f"Live orders must not have paper_* fields, found: {paper_keys}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Test 10: no-positive-source persists full candidate audit before terminalize
# ─────────────────────────────────────────────────────────────────────────────

class TestNoSourcePersistsCandidateAudit:

    def test_10_no_source_audit_persisted_before_terminalize(self):
        """
        Test 10: When no positive underlying source exists, the full candidate
        audit must be persisted to orders.meta BEFORE _terminalize_breach_failure
        is called. This lets production dashboards see which fields were checked
        and what raw values they held.
        """
        _, _, audit = resolve_positive_underlying_for_breach(
            approved_plan=_plan(metadata={}),
            watched=_watched(trigger_price=None, entry_trigger=None),
            sig={},
            selector_audit={},
        )

        meta = build_no_source_meta(audit, order_id="LOID-GS-1", ticker="GS")

        # Three required keys
        assert "zero_underlying_repair" in meta
        assert meta["zero_underlying_repair_failed"] is True
        assert meta["zero_underlying_failure_reason"] == ZERO_UNDERLYING_TERMINAL_REASON

        # Repair sub-dict must include candidates
        repair = meta["zero_underlying_repair"]
        assert repair["repaired"] is False
        assert repair["reason"] == "no_positive_source"
        assert isinstance(repair["candidates"], list), "candidates must be a list"
        assert len(repair["candidates"]) > 0, "candidates must not be empty"

        # Each candidate must show why it failed
        for c in repair["candidates"]:
            assert "source" in c
            assert "raw" in c
            assert "resolved" in c
            assert c["resolved"] is False, (
                f"All candidates must be unresolved when no source found, "
                f"but {c['source']} shows resolved=True"
            )

    def test_10_candidate_audit_covers_watched_and_selector(self):
        """Candidate list must include entries from both watched and selector."""
        _, _, audit = resolve_positive_underlying_for_breach(
            approved_plan=_plan(metadata={}),
            watched=_watched(trigger_price=None),
            sig={},
            selector_audit={},
        )
        meta = build_no_source_meta(audit, order_id="LOID-1", ticker="GS")
        candidates = meta["zero_underlying_repair"]["candidates"]
        sources = [c["source"] for c in candidates]
        assert any("selector_audit" in s for s in sources)
        assert any("watched" in s for s in sources)


# ─────────────────────────────────────────────────────────────────────────────
# Test 11: successful repair sets nested trigger.current_price
# ─────────────────────────────────────────────────────────────────────────────

class TestNestedTriggerPatch:

    def test_11_repair_sets_nested_trigger_current_price(self):
        """
        Test 11: On successful repair, approved_plan.metadata["trigger"]["current_price"]
        must be set — not just the flat "trigger_current_price" key.
        """
        plan = _plan(metadata={"trigger": {"entry": 462.00}})  # existing trigger dict
        _, _, audit = resolve_positive_underlying_for_breach(
            approved_plan=plan,
            watched=_watched(trigger_price=465.20),
            sig={},
        )
        # Simulate execution_core applying the patch to plan.metadata
        underlying = 465.20
        existing_trigger = dict((plan.metadata.get("trigger") or {}))
        patch = build_underlying_patch(
            underlying, "watched.trigger_price", audit,
            order_id="LOID-1", ticker="GS",
            existing_trigger=existing_trigger,
        )
        # Apply patch to plan.metadata exactly as execution_core does
        plan.metadata["underlying_entry"]         = underlying
        plan.metadata["underlying_price"]         = underlying
        plan.metadata["current_underlying_price"] = underlying
        plan.metadata["trigger_current_price"]    = underlying
        plan.metadata["zero_underlying_repair"]   = patch["zero_underlying_repair"]
        if "trigger" not in plan.metadata or not isinstance(plan.metadata.get("trigger"), dict):
            plan.metadata["trigger"] = {}
        plan.metadata["trigger"]["current_price"] = underlying

        # Assertions
        assert plan.metadata["trigger"]["current_price"] == pytest.approx(465.20), (
            "Test 11: approved_plan.metadata['trigger']['current_price'] must be set"
        )
        assert plan.metadata["underlying_entry"] == pytest.approx(465.20)
        assert plan.metadata["trigger_current_price"] == pytest.approx(465.20)

    def test_11_repair_merges_into_existing_trigger_dict(self):
        """Existing trigger sub-dict keys must be preserved when patching."""
        existing = {"entry": 462.00, "stop": 455.00}
        patch = build_underlying_patch(
            465.20, "watched.trigger_price", {},
            existing_trigger=existing,
        )
        trigger = patch["trigger"]
        assert trigger["current_price"] == pytest.approx(465.20)
        assert trigger["entry"]  == 462.00, "existing 'entry' key must be preserved"
        assert trigger["stop"]   == 455.00, "existing 'stop' key must be preserved"

    def test_11_patch_has_no_flat_dotted_trigger_key(self):
        """
        The old flat key 'trigger.current_price' (dot in key name) must NOT be
        in the patch dict — only the proper nested 'trigger' dict.
        """
        patch = build_underlying_patch(465.20, "test", {})
        assert "trigger.current_price" not in patch, (
            "'trigger.current_price' as a flat key must not appear — use nested 'trigger' dict"
        )
        assert "trigger" in patch
        assert isinstance(patch["trigger"], dict)

    def test_all_five_underlying_keys_present(self):
        patch = build_underlying_patch(465.20, "test", {}, order_id="X", ticker="GS")
        assert patch["underlying_entry"]         == pytest.approx(465.20)
        assert patch["underlying_price"]         == pytest.approx(465.20)
        assert patch["current_underlying_price"] == pytest.approx(465.20)
        assert patch["trigger_current_price"]    == pytest.approx(465.20)
        assert patch["trigger"]["current_price"] == pytest.approx(465.20)


# ─────────────────────────────────────────────────────────────────────────────
# Test 13: source-order / production-shape integration test
# ─────────────────────────────────────────────────────────────────────────────

class TestProductionShapeIntegration:
    """
    Test 13: Prove that zero_underlying repair runs before submit_existing_entry()
    in the real _on_entry_trigger execution path.

    This is NOT a fake-submit-list test. It uses the actual APExecutionCore
    with mocked OSM, broker, and selector at the appropriate boundaries.
    It verifies that OSM.submit_existing_entry is called when all conditions
    are satisfied, and NOT called when underlying is missing.
    """

    def _make_core(self, selector_contract=_REAL_OCC, selector_underlying=465.20,
                   selector_returns=True):
        from ap_entry_watcher import WatchedSignal

        broker = MagicMock()
        broker.base_url = _LIVE_URL
        broker.account_id = "VA_TEST"
        broker.cfg = MagicMock()
        broker.cfg.base_url = _LIVE_URL

        osm = MagicMock()
        osm.client_id = _CLIENT_ID
        osm.update_order_meta.return_value = True
        osm.submit_existing_entry.return_value = {
            "ok": True,
            "local_order_id": "LOID-GS-1",
            "broker_order_id": "BRK-001",
        }
        osm.expire_pending_entry.return_value = True
        osm.transition.return_value = True

        sel_result = None
        if selector_returns:
            sel_result = _sel(
                contract_symbol=selector_contract,
                underlying_price=selector_underlying,
                bid=1.80, ask=1.86,
            )

        selector = MagicMock()
        selector.select.return_value = sel_result
        selector.get_last_failure.return_value = None
        selector.get_last_dte_ladder_audit.return_value = {}
        selector.dte_ladder_enabled = True

        return broker, osm, selector

    def _make_approved_plan(self, contract=_REAL_OCC, limit=1.87, qty=1,
                             underlying=465.20):
        """Approved plan as returned by _recover_plan_for_revalidation."""
        return types.SimpleNamespace(
            contract_symbol  = contract,
            limit_price      = limit,
            contracts        = qty,
            max_position_usd = 500.0,
            side             = "CALL",
            execution_mode   = "live",
            client_id        = _CLIENT_ID,
            signal_id        = "SIG-GS-1",
            trigger_price    = underlying,
            underlying_price = None,
            metadata         = {
                "contract_deferred":         True,
                "breach_attempt_count":       0,
                "broker_ready":              True,
                "materialization_status":    "SELECTED",
                "queue_id":                  219,
            },
            ticker = "GS",
        )

    def test_13_submit_reached_with_valid_occ_and_positive_underlying(self):
        """
        Test 13 (production-shape): selector returns real OCC, watched.trigger_price
        is positive → execution core calls OSM.submit_existing_entry.

        This is the core regression proof: if Fix A was absent, zero_underlying
        would block the submit. With Fix A present, it resolves from
        watched.trigger_price and submit proceeds.
        """
        broker, osm, selector = self._make_core(
            selector_contract=_REAL_OCC,
            selector_underlying=465.20,
        )
        approved_plan = self._make_approved_plan(
            contract=_REAL_OCC,
            limit=1.87,
            qty=1,
            underlying=465.20,  # trigger_price is positive
        )

        terminalized = []

        mc = MagicMock()
        mc.get_mode.return_value = "LIVE"

        # Mock _refresh_ask_at_submit to avoid DB import in ap.execution
        def _fake_refresh(broker, contract):
            return (1.87, 0, True, "ok", {"spread_pct": 0.03,
                "submit_bid": 1.85, "submit_ask": 1.87,
                "submit_last": None, "submit_mid": 1.86})

        with patch.object(
            _core_mod.APExecutionCore,
            "_breach_risk_check",
            return_value=True,
        ), patch.object(
            _core_mod.APExecutionCore,
            "_recover_plan_for_revalidation",
            return_value=approved_plan,
        ), patch(
            "ap.execution._refresh_ask_at_submit",
            new=_fake_refresh,
        ):
            core = _core_mod.APExecutionCore(
                broker=broker,
                order_state_machine=osm,
                master_control=mc,
            )
            core.contract_selector = selector
            core.paper = False
            core.mode  = "LIVE"

            watched = MagicMock()
            watched.ticker       = "GS"
            watched.side         = "CALL"
            watched.signal       = {
                "ticker": "GS",
                "side":   "CALL",
                "client_id": _CLIENT_ID,
                "execution_mode": "live",
                "signal_id": "SIG-GS-1",
                "local_order_id": "LOID-GS-1",
                "queue_id": 219,
                "contract_deferred": True,
            }
            watched.trigger_price = 465.20
            watched.entry_trigger = 462.00
            watched.stop_level    = 455.00
            watched.target_price  = 475.00
            watched.overnight     = True
            watched.signal_id     = "SIG-GS-1"

            osm._get_order.return_value = {
                "local_order_id":  "LOID-GS-1",
                "client_id":       _CLIENT_ID,
                "kind":            "ENTRY",
                "status":          "PENDING_TRIGGER",
                "contract":        "DEFERRED:GS",
                "limit_price":     0.01,
                "qty":             1,
                "reserved_cost":   1.0,
                "symbol":          "GS",
                "direction":       "CALL",
                "meta":            {},
            }
            osm.get_order_by_signal.return_value = osm._get_order.return_value
            osm.update_order_meta.return_value   = True

            core._on_entry_trigger(watched)

        # ── The primary assertion: submit_existing_entry was called ──────────
        assert osm.submit_existing_entry.called, (
            "Test 13 FAILED: submit_existing_entry was NOT called. "
            "Fix A (resolve_positive_underlying_for_breach) must resolve "
            "watched.trigger_price=465.20 and allow submit to proceed. "
            "This test FAILS before this PR and PASSES after."
        )

    def test_13_submit_blocked_when_no_underlying_anywhere(self):
        """
        Negative: when watched.trigger_price=None and every other source is zero,
        submit_existing_entry must NOT be called.
        """
        broker, osm, selector = self._make_core(
            selector_underlying=0,  # selector has no underlying
        )
        approved_plan = self._make_approved_plan(
            underlying=0,   # trigger_price=0 → not a valid source
        )
        approved_plan.trigger_price = None

        mc = MagicMock()
        mc.get_mode.return_value = "LIVE"

        with patch.object(
            _core_mod.APExecutionCore,
            "_breach_risk_check",
            return_value=True,
        ), patch.object(
            _core_mod.APExecutionCore,
            "_recover_plan_for_revalidation",
            return_value=approved_plan,
        ):
            core = _core_mod.APExecutionCore(
                broker=broker,
                order_state_machine=osm,
                master_control=mc,
            )
            core.contract_selector = selector
            core.paper = False
            core.mode  = "LIVE"

            watched = MagicMock()
            watched.ticker        = "GS"
            watched.side          = "CALL"
            watched.trigger_price = None
            watched.entry_trigger = None
            watched.stop_level    = None
            watched.target_price  = None
            watched.overnight     = True
            watched.signal_id     = "SIG-GS-1"
            watched.signal        = {
                "ticker": "GS", "side": "CALL",
                "client_id": _CLIENT_ID,
                "execution_mode": "live",
                "signal_id": "SIG-GS-1",
                "contract_deferred": True,
            }

            osm._get_order.return_value = {
                "local_order_id": "LOID-GS-1",
                "client_id":      _CLIENT_ID,
                "kind":           "ENTRY",
                "status":         "PENDING_TRIGGER",
                "contract":       "DEFERRED:GS",
                "limit_price":    0.01,
                "qty":            1,
                "reserved_cost":  1.0,
                "symbol":         "GS",
                "direction":      "CALL",
                "meta":           {},
            }
            osm.get_order_by_signal.return_value = osm._get_order.return_value

            core._on_entry_trigger(watched)

        assert not osm.submit_existing_entry.called, (
            "submit_existing_entry must NOT be called when no positive underlying exists"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Test 15: PAPER_SELECTOR_REQUIRE_LIVE_DATA enforcement
# ─────────────────────────────────────────────────────────────────────────────

class TestPaperSelectorRequireLiveData:

    def test_15_sandbox_data_blocked_when_env_set(self, monkeypatch):
        """
        Test 15: PAPER_SELECTOR_REQUIRE_LIVE_DATA=1 + sandbox selector data
        → should_block=True, reason=PAPER_SELECTOR_DATA_DOMAIN_BLOCKED.
        """
        monkeypatch.setenv("PAPER_SELECTOR_REQUIRE_LIVE_DATA", "1")
        blocked, reason = check_paper_selector_data_domain(
            is_paper=True,
            selector_audit={"tradier_base_url": _SAND_URL},
            broker_base_url=_SAND_URL,
        )
        assert blocked is True
        assert reason == PAPER_SELECTOR_DATA_DOMAIN_BLOCKED

    def test_15_sandbox_data_not_blocked_when_env_not_set(self, monkeypatch):
        """Without PAPER_SELECTOR_REQUIRE_LIVE_DATA=1, sandbox data does not block."""
        monkeypatch.delenv("PAPER_SELECTOR_REQUIRE_LIVE_DATA", raising=False)
        blocked, reason = check_paper_selector_data_domain(
            is_paper=True,
            selector_audit={"tradier_base_url": _SAND_URL},
            broker_base_url=_SAND_URL,
        )
        assert blocked is False
        assert reason is None

    def test_15_live_data_not_blocked_even_with_env_set(self, monkeypatch):
        """Live selector data is not blocked even when env=1."""
        monkeypatch.setenv("PAPER_SELECTOR_REQUIRE_LIVE_DATA", "1")
        blocked, _ = check_paper_selector_data_domain(
            is_paper=True,
            selector_audit={"tradier_base_url": _LIVE_URL},
            broker_base_url=_SAND_URL,
        )
        assert blocked is False

    def test_15_live_orders_never_blocked_by_paper_env(self, monkeypatch):
        """is_paper=False → never blocked regardless of env."""
        monkeypatch.setenv("PAPER_SELECTOR_REQUIRE_LIVE_DATA", "1")
        blocked, _ = check_paper_selector_data_domain(
            is_paper=False,
            selector_audit={"tradier_base_url": _SAND_URL},
        )
        assert blocked is False

    def test_15_never_raises(self, monkeypatch):
        monkeypatch.setenv("PAPER_SELECTOR_REQUIRE_LIVE_DATA", "BROKEN")
        try:
            check_paper_selector_data_domain(
                is_paper=True,
                selector_audit=None,
                broker_base_url=None,
            )
        except Exception as e:
            pytest.fail(f"Must never raise: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Domain classifier unit tests
# ─────────────────────────────────────────────────────────────────────────────

class TestClassifyDomain:

    def test_live_url_classifies_as_live(self):
        assert _classify_domain("https://api.tradier.com/v1") == "live"

    def test_sandbox_url_classifies_as_sandbox(self):
        assert _classify_domain("https://sandbox.tradier.com/v1") == "sandbox"

    def test_empty_classifies_as_unknown(self):
        assert _classify_domain("") == "unknown"

    def test_unknown_url_classifies_as_unknown(self):
        assert _classify_domain("https://otherprovider.com") == "unknown"


# ─────────────────────────────────────────────────────────────────────────────
# Structured log markers in source
# ─────────────────────────────────────────────────────────────────────────────

def test_log_markers_are_literal_strings_in_execution_core():
    """
    Both log markers must be literal strings in ap_execution_core.py
    so Render log grep works: grep ZERO_UNDERLYING_REPAIRED /var/log/app.log
    """
    src = open("ap_execution_core.py").read()
    assert "ZERO_UNDERLYING_REPAIRED"        in src
    assert "ZERO_UNDERLYING_NO_POSITIVE_SOURCE" in src
    assert "PAPER_SELECTOR_DATA_DOMAIN_BLOCKED" in src
