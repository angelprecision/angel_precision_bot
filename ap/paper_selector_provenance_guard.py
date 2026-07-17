"""Attest PAPER selector quote provenance before broker submission.

PAPER intentionally uses LIVE Tradier market data and the Tradier sandbox only
for order lifecycle exercise. The legacy audit could fall back to the sandbox
execution broker URL when selector provenance was absent, making diagnostics
lie; enforcement was default-off and unknown provenance passed.

This guard stamps the actual selector data transport onto every successful
selection and fails closed for PAPER when that transport is not provably the
production Tradier market-data domain. It does not alter the selected contract,
price, quantity, or broker payload.
"""
from __future__ import annotations

from typing import Any, Callable

from ap.logger import get_logger

log = get_logger("ap.paper_selector_provenance_guard")

_PATCHED_ATTR = "_AP_PAPER_SELECTOR_PROVENANCE_PATCHED"
_ORIGINAL_SELECT_ATTR = "_AP_PAPER_SELECTOR_PROVENANCE_SELECT_ORIGINAL"
_ORIGINAL_CHECK_ATTR = "_AP_PAPER_SELECTOR_PROVENANCE_CHECK_ORIGINAL"
_ORIGINAL_FIELDS_ATTR = "_AP_PAPER_SELECTOR_PROVENANCE_FIELDS_ORIGINAL"

PAPER_SELECTOR_PROVENANCE_BLOCKED = (
    "paper_selector_data_domain_blocked:live_selector_provenance_required"
)


def _domain(base_url: Any) -> str:
    text = str(base_url or "").strip().lower()
    if "api.tradier.com" in text and "sandbox" not in text:
        return "live"
    if "sandbox" in text:
        return "sandbox"
    return "unknown"


def selector_transport_attestation(selector: Any) -> dict[str, Any]:
    data_broker = getattr(selector, "data_broker", None)
    execution_broker = getattr(selector, "broker", None)
    data_base = str(getattr(data_broker, "base_url", "") or "").strip()
    execution_base = str(getattr(execution_broker, "base_url", "") or "").strip()
    mode = str(getattr(selector, "mode", "") or "").strip().lower()
    data_domain = _domain(data_base)
    execution_domain = _domain(execution_base)
    valid = bool(
        mode == "paper"
        and data_domain == "live"
        and execution_domain == "sandbox"
        and data_broker is not execution_broker
    )
    if mode == "live":
        valid = data_domain == "live"
    return {
        "selector_execution_mode": mode if mode in {"live", "paper"} else "unknown",
        "tradier_base_url": data_base,
        "quote_source": (
            "tradier_live" if data_domain == "live"
            else "tradier_sandbox" if data_domain == "sandbox"
            else "unknown"
        ),
        "sandbox_mode": data_domain == "sandbox",
        "selector_data_domain": data_domain,
        "selector_execution_broker_base_url": execution_base,
        "selector_execution_broker_domain": execution_domain,
        "selector_data_order_transport_separated": data_broker is not execution_broker,
        "selector_provenance_valid": valid,
    }


def _metadata(plan: Any) -> dict[str, Any] | None:
    try:
        if isinstance(plan, dict):
            current = plan.get("metadata")
            if not isinstance(current, dict):
                current = {}
                plan["metadata"] = current
            return current
        current = getattr(plan, "metadata", None)
        if not isinstance(current, dict):
            current = {}
            setattr(plan, "metadata", current)
        return current
    except Exception:
        return None


def _stamp_result(result: Any, attestation: dict[str, Any]) -> None:
    if result is None:
        return
    try:
        audit = getattr(result, "candidate_audit", None)
        if not isinstance(audit, dict):
            audit = {}
            setattr(result, "candidate_audit", audit)
        audit.update(attestation)
    except Exception:
        pass
    try:
        diagnostics = getattr(result, "selection_diagnostics", None)
        if isinstance(diagnostics, dict):
            diagnostics["transport_attestation"] = dict(attestation)
    except Exception:
        pass


def wrap_select(original: Callable[..., Any]) -> Callable[..., Any]:
    def guarded(self, plan, *args, **kwargs):
        result = original(self, plan, *args, **kwargs)
        attestation = selector_transport_attestation(self)
        meta = _metadata(plan)
        if meta is not None:
            meta["selector_transport_attestation"] = dict(attestation)
            meta.update({
                "paper_selector_base_url": attestation["tradier_base_url"],
                "paper_selector_data_domain": attestation["selector_data_domain"],
                "paper_selector_quote_source": attestation["quote_source"],
                "paper_selector_provenance_valid": attestation["selector_provenance_valid"],
            })
        _stamp_result(result, attestation)

        if (
            attestation["selector_execution_mode"] == "paper"
            and result is not None
            and not attestation["selector_provenance_valid"]
        ):
            try:
                self._set_last_failure({
                    "stage": "transport_provenance",
                    "reason_code": "PAPER_SELECTOR_PROVENANCE_BLOCKED",
                    "explanation": PAPER_SELECTOR_PROVENANCE_BLOCKED,
                    "transport_attestation": dict(attestation),
                })
            except Exception:
                pass
            if meta is not None:
                meta["selector_failure"] = {
                    "stage": "transport_provenance",
                    "reason_code": "PAPER_SELECTOR_PROVENANCE_BLOCKED",
                    "explanation": PAPER_SELECTOR_PROVENANCE_BLOCKED,
                    "transport_attestation": dict(attestation),
                }
            log.critical(
                "PAPER_SELECTOR_PROVENANCE_BLOCKED data_base=%s execution_base=%s "
                "data_domain=%s execution_domain=%s separated=%s",
                attestation["tradier_base_url"],
                attestation["selector_execution_broker_base_url"],
                attestation["selector_data_domain"],
                attestation["selector_execution_broker_domain"],
                attestation["selector_data_order_transport_separated"],
            )
            return None
        return result

    return guarded


def strict_check_paper_selector_data_domain(
    *,
    is_paper: bool,
    selector_audit: dict,
    broker_base_url: str = "",
) -> tuple[bool, str | None]:
    if not is_paper:
        return False, None
    audit = selector_audit or {}
    selector_base = str(audit.get("tradier_base_url") or "").strip()
    selector_domain = _domain(selector_base)
    quote_source = str(audit.get("quote_source") or "").strip().lower()
    attested_valid = audit.get("selector_provenance_valid")
    valid = bool(
        selector_domain == "live"
        and quote_source == "tradier_live"
        and attested_valid is not False
    )
    return (not valid), (None if valid else PAPER_SELECTOR_PROVENANCE_BLOCKED)


def truthful_build_paper_domain_fields(
    *,
    selector_audit: dict,
    broker_base_url: str = "",
) -> dict[str, Any]:
    audit = selector_audit or {}
    selector_base = str(audit.get("tradier_base_url") or "").strip()
    selector_domain = _domain(selector_base)
    quote_source = str(audit.get("quote_source") or "").strip() or "unknown"
    broker_base = str(broker_base_url or "").strip()
    broker_domain = _domain(broker_base)
    provenance_valid = bool(
        selector_domain == "live"
        and quote_source == "tradier_live"
        and audit.get("selector_provenance_valid") is not False
    )
    return {
        "paper_selector_data_domain": selector_domain,
        "paper_selector_quote_source": quote_source,
        "paper_selector_base_url": selector_base,
        "paper_selector_provenance_valid": provenance_valid,
        "paper_selector_provenance_complete": bool(selector_base and quote_source != "unknown"),
        "paper_order_broker_domain": broker_domain,
        "paper_order_broker_base_url": broker_base,
        "paper_data_order_domain_mismatch": selector_domain != broker_domain,
        "paper_data_order_domain_expected_split": bool(
            selector_domain == "live" and broker_domain == "sandbox"
        ),
    }


def install_paper_selector_provenance_guard() -> None:
    from ap import contract_selector
    from ap import deferred_breach_underlying_repair as repair

    if getattr(contract_selector, _PATCHED_ATTR, False):
        return

    setattr(contract_selector, _ORIGINAL_SELECT_ATTR, contract_selector.APContractSelectionEngine.select)
    setattr(repair, _ORIGINAL_CHECK_ATTR, repair.check_paper_selector_data_domain)
    setattr(repair, _ORIGINAL_FIELDS_ATTR, repair.build_paper_domain_fields)

    contract_selector.APContractSelectionEngine.select = wrap_select(
        contract_selector.APContractSelectionEngine.select
    )
    repair.check_paper_selector_data_domain = strict_check_paper_selector_data_domain
    repair.build_paper_domain_fields = truthful_build_paper_domain_fields
    setattr(contract_selector, _PATCHED_ATTR, True)
