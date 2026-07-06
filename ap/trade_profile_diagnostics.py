from __future__ import annotations

import logging
from typing import Any, Mapping

log = logging.getLogger("ap.trade_profile_diagnostics")

PROFILE_DIAGNOSTICS_VERSION = "ap_trade_profile_diagnostics_v1_observe_only"
_PATCHED_ATTR = "_AP_PROFILE_DIAGNOSTICS_PATCHED"
_ORIGINAL_ATTR = "_AP_PROFILE_DIAGNOSTICS_ORIGINAL_BUILD"


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def build_trade_profile_diagnostics(
    signal: Mapping[str, Any] | None,
    decision_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the AP profile learning payload without changing trade behavior.

    This intentionally uses only already-available production-shaped data from
    the signal and decision_context. It does not fetch market data, does not
    infer missing candles, does not gate, and does not mutate the input signal.
    """

    signal_snapshot = _as_dict(signal)
    decision_snapshot = _as_dict(decision_context)

    payload: dict[str, Any] = {
        "profile_diagnostics_version": PROFILE_DIAGNOSTICS_VERSION,
        "observe_only": True,
        "live_behavior_changed": False,
        "score_source": "trade_dossier_observe_only_profile",
        "market_context": None,
        "position_score_profile": None,
        "diagnostics": {
            "observe_only": True,
            "live_behavior_changed": False,
            "source": "ap.trade_profile_diagnostics",
            "broker_submit_touched": False,
            "broker_cancel_touched": False,
            "orders_mutated": False,
            "positions_mutated": False,
            "queue_mutated": False,
        },
        "warnings": [],
    }

    try:
        from ap.market_context_builder import build_market_context_for_signal

        market_context = build_market_context_for_signal(
            signal_snapshot,
            data_sources={"decision_context": decision_snapshot},
        )
        payload["market_context"] = market_context
    except Exception as exc:  # pragma: no cover - defensive production guard
        payload["warnings"].append(f"market_context_unavailable:{str(exc)[:120]}")
        market_context = {}

    try:
        from ap.position_score_profile import build_position_score_profile

        payload["position_score_profile"] = build_position_score_profile(
            signal_snapshot,
            market_context if isinstance(market_context, dict) else {},
        )
    except Exception as exc:  # pragma: no cover - defensive production guard
        payload["warnings"].append(f"position_score_profile_unavailable:{str(exc)[:120]}")

    return payload


def attach_trade_profile_diagnostics(
    dossier: dict[str, Any],
    signal: Mapping[str, Any] | None,
    decision_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Attach observe-only diagnostics to a trade dossier in-place if possible."""

    if not isinstance(dossier, dict):
        return dossier
    dossier_json = dossier.get("dossier")
    if not isinstance(dossier_json, dict):
        return dossier

    # Preserve existing keys if a future writer already supplied this payload.
    if "ap_trade_profile" not in dossier_json:
        dossier_json["ap_trade_profile"] = build_trade_profile_diagnostics(
            signal,
            decision_context,
        )
    return dossier


def install_trade_dossier_profile_diagnostics() -> None:
    """Wrap ap.trade_dossier.build_trade_dossier with observe-only profile output.

    The wrapper is deliberately narrow:
    - reads signal + decision_context only
    - appends diagnostics under dossier.ap_trade_profile only
    - preserves the original build_trade_dossier return shape
    - never blocks on profile failures
    """

    try:
        from ap import trade_dossier
    except Exception as exc:  # pragma: no cover - package import safety
        log.debug("trade_profile_diagnostics_install_skipped: %s", exc)
        return

    if getattr(trade_dossier, _PATCHED_ATTR, False):
        return

    original = getattr(trade_dossier, "build_trade_dossier", None)
    if not callable(original):
        return

    def _wrapped_build_trade_dossier(
        signal: dict,
        *,
        client_id: str,
        execution_mode: str,
        decision_context: dict | None = None,
    ) -> dict:
        dossier = original(
            signal,
            client_id=client_id,
            execution_mode=execution_mode,
            decision_context=decision_context,
        )
        try:
            return attach_trade_profile_diagnostics(dossier, signal, decision_context)
        except Exception as exc:  # pragma: no cover - diagnostics cannot block
            try:
                if isinstance(dossier, dict) and isinstance(dossier.get("dossier"), dict):
                    dossier["dossier"].setdefault(
                        "ap_trade_profile",
                        {
                            "profile_diagnostics_version": PROFILE_DIAGNOSTICS_VERSION,
                            "observe_only": True,
                            "live_behavior_changed": False,
                            "diagnostics_error": str(exc)[:200],
                        },
                    )
            except Exception:
                pass
            return dossier

    setattr(trade_dossier, _ORIGINAL_ATTR, original)
    setattr(trade_dossier, "build_trade_dossier", _wrapped_build_trade_dossier)
    setattr(trade_dossier, _PATCHED_ATTR, True)
