"""
Trigger-anchored contract intent.

The intent is *identity and preference evidence*, prepared before the
scanner-qualified setup breaches its trigger. It never carries submit-time
price authority: pre-open chain bid/ask/midpoint values are captured for
provenance only and must be revalidated by a fresh direct quote at breach.

This structure exists so that the breach-time fast path knows exactly which
contract(s) to direct-quote, and does not fall back to an unrestricted
multi-expiration scan.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any


CONTRACT_INTENT_POLICY_VERSION = "p0-2026-07-25"


@dataclass(frozen=True)
class ContractIntent:
    # Identity of the setup this intent belongs to.
    ticker: str
    side: str
    timeframe: str
    trigger_price: float
    target: float | None
    instrument_class: str

    # Expiration preference (trigger-anchored, per playbook policy).
    preferred_expiration: str | None
    fallback_expiration: str | None

    # Strike preference (trigger-anchored, per playbook policy).
    preferred_strike: float | None
    fallback_strike: float | None

    # Resolved OCC symbols when the chain has been sampled pre-breach.
    preferred_contract_symbol: str | None
    fallback_contract_symbol: str | None

    # Provenance.
    prepared_at: str
    source_chain_timestamp: str | None
    source_chain_domain: str | None

    # Policy + identity fencing.
    policy_version: str
    client_id: str
    execution_mode: str
    signal_id: str | None
    setup_generation: int | None

    # Optional diagnostics — never treated as submit-time price authority.
    diagnostics: dict = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["diagnostics"] = dict(self.diagnostics or {})
        return payload


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _coerce_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        result = float(value)
    except (TypeError, ValueError):
        return None
    if result != result:  # NaN
        return None
    return result


def build_contract_intent(
    *,
    ticker: str,
    side: str,
    timeframe: str,
    trigger_price: float,
    target: float | None,
    instrument_class: str,
    preferred_expiration: str | None,
    fallback_expiration: str | None,
    preferred_strike: float | None,
    fallback_strike: float | None,
    preferred_contract_symbol: str | None = None,
    fallback_contract_symbol: str | None = None,
    source_chain_timestamp: str | None = None,
    source_chain_domain: str | None = None,
    client_id: str = "",
    execution_mode: str = "unknown",
    signal_id: str | None = None,
    setup_generation: int | None = None,
    diagnostics: dict | None = None,
    policy_version: str = CONTRACT_INTENT_POLICY_VERSION,
    prepared_at: str | None = None,
) -> ContractIntent:
    """
    Construct a serializable contract intent for downstream breach-time use.

    Callers must pass the exact ``client_id`` and ``execution_mode`` that
    will submit the trade — a mismatch between intent and submission is a
    hard identity failure at broker-ready time.
    """
    return ContractIntent(
        ticker=str(ticker or "").upper(),
        side=str(side or "").upper(),
        timeframe=str(timeframe or ""),
        trigger_price=float(trigger_price),
        target=_coerce_float(target),
        instrument_class=str(instrument_class or ""),
        preferred_expiration=(str(preferred_expiration) if preferred_expiration else None),
        fallback_expiration=(str(fallback_expiration) if fallback_expiration else None),
        preferred_strike=_coerce_float(preferred_strike),
        fallback_strike=_coerce_float(fallback_strike),
        preferred_contract_symbol=(str(preferred_contract_symbol) if preferred_contract_symbol else None),
        fallback_contract_symbol=(str(fallback_contract_symbol) if fallback_contract_symbol else None),
        prepared_at=str(prepared_at or _utcnow_iso()),
        source_chain_timestamp=(str(source_chain_timestamp) if source_chain_timestamp else None),
        source_chain_domain=(str(source_chain_domain) if source_chain_domain else None),
        policy_version=str(policy_version),
        client_id=str(client_id or ""),
        execution_mode=str(execution_mode or "unknown").lower(),
        signal_id=(str(signal_id) if signal_id is not None else None),
        setup_generation=(int(setup_generation) if setup_generation is not None else None),
        diagnostics=dict(diagnostics or {}),
    )


def build_contract_intent_from_playbook(
    spec,
    *,
    ticker: str,
    timeframe: str,
    trigger_price: float,
    target: float | None,
    candidates: list[dict] | None = None,
    preferred_contract_symbol: str | None = None,
    fallback_contract_symbol: str | None = None,
    client_id: str = "",
    execution_mode: str = "unknown",
    signal_id: str | None = None,
    setup_generation: int | None = None,
    source_chain_timestamp: str | None = None,
    source_chain_domain: str | None = None,
    prepared_at: str | None = None,
) -> ContractIntent:
    """
    Build a contract intent from an already-resolved
    :class:`ap.contract_playbook.ContractPlaybookSpec` and the current
    chain candidates.
    """
    from ap.contract_playbook import build_playbook_candidate_context

    context = build_playbook_candidate_context(spec, list(candidates or []))
    preferred_strikes = list(context.get("preferred_strikes") or [])
    preferred_strike = preferred_strikes[0] if preferred_strikes else None
    fallback_strike = preferred_strikes[1] if len(preferred_strikes) > 1 else None
    preferred_expiration = spec.preferred_expirations[0] if spec.preferred_expirations else None
    fallback_expiration = spec.preferred_expirations[1] if len(spec.preferred_expirations) > 1 else None

    diagnostics = {
        "policy_reason": spec.policy_reason,
        "strike_policy": spec.strike_policy,
        "fallback_policy": spec.fallback_policy,
        "atm_strike": context.get("atm_strike"),
        "one_step_otm_strike": context.get("one_step_otm_strike"),
    }
    return build_contract_intent(
        ticker=ticker,
        side=spec.side,
        timeframe=timeframe,
        trigger_price=float(trigger_price),
        target=target,
        instrument_class=spec.instrument_class,
        preferred_expiration=preferred_expiration,
        fallback_expiration=fallback_expiration,
        preferred_strike=preferred_strike,
        fallback_strike=fallback_strike,
        preferred_contract_symbol=preferred_contract_symbol,
        fallback_contract_symbol=fallback_contract_symbol,
        source_chain_timestamp=source_chain_timestamp,
        source_chain_domain=source_chain_domain,
        client_id=client_id,
        execution_mode=execution_mode,
        signal_id=signal_id,
        setup_generation=setup_generation,
        diagnostics=diagnostics,
        prepared_at=prepared_at,
    )
