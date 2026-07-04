from __future__ import annotations

import hashlib
import json
import logging
import math
import os
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AdmissionThresholds:
    scanner_floor: float
    interrogation_floor: float
    mc_score_floor: float
    mc_priority_floor: float
    context_floor: float


@dataclass(frozen=True)
class AdmissionThresholdResolution:
    thresholds: AdmissionThresholds
    sources: dict[str, str]
    config_hash: str


_MC_SCORE_ENV_DEFAULT = 65.0
_MC_SCORE_RUNTIME_DEFAULT = 60.0
_MC_CONTEXT_ENV_DEFAULT = 0.0
_MC_CONTEXT_RUNTIME_DEFAULT = 6.0
_MC_PRIORITY_FLOOR_DEFAULT = 40.0
_SCANNER_FLOOR_DEFAULT = 70.0
_INTERROGATION_FLOOR_DEFAULT = 62.0


def _env_float(name: str, default: float) -> tuple[float, str]:
    raw = os.getenv(name)
    if raw is not None and str(raw).strip() != "":
        return float(raw), f"env:{name}"
    return float(default), f"default:{name}={default}"


def _current_source(
    *,
    current_value: float,
    env_name: str,
    env_default: float,
    runtime_default: float,
    runtime_default_label: str,
) -> str:
    raw = os.getenv(env_name)
    if raw is not None and str(raw).strip() != "":
        try:
            if math.isclose(float(raw), float(current_value), rel_tol=0.0, abs_tol=1e-9):
                return f"env:{env_name}"
        except (TypeError, ValueError):
            pass
    if math.isclose(float(current_value), float(runtime_default), rel_tol=0.0, abs_tol=1e-9):
        return runtime_default_label
    if math.isclose(float(current_value), float(env_default), rel_tol=0.0, abs_tol=1e-9):
        return f"default:{env_name}={env_default}"
    return "runtime_arg"


def resolve_admission_thresholds(
    *,
    mc_score_floor: float | None = None,
    mc_priority_floor: float | None = None,
    context_floor: float | None = None,
) -> AdmissionThresholdResolution:
    scanner_floor, scanner_source = _env_float(
        "GATE_G_SCANNER_MIN_ELIGIBLE",
        _SCANNER_FLOOR_DEFAULT,
    )
    interrogation_floor, interrogation_source = _env_float(
        "INTERROGATION_MIN_SCORE",
        _INTERROGATION_FLOOR_DEFAULT,
    )

    if mc_score_floor is None:
        mc_score_floor, mc_score_source = _env_float("SCORE_FLOOR", _MC_SCORE_ENV_DEFAULT)
    else:
        mc_score_floor = float(mc_score_floor)
        mc_score_source = _current_source(
            current_value=mc_score_floor,
            env_name="SCORE_FLOOR",
            env_default=_MC_SCORE_ENV_DEFAULT,
            runtime_default=_MC_SCORE_RUNTIME_DEFAULT,
            runtime_default_label=f"default:ap_master_control.score_floor={_MC_SCORE_RUNTIME_DEFAULT}",
        )

    if context_floor is None:
        context_floor, context_source = _env_float("CONTEXT_FLOOR", _MC_CONTEXT_ENV_DEFAULT)
    else:
        context_floor = float(context_floor)
        context_source = _current_source(
            current_value=context_floor,
            env_name="CONTEXT_FLOOR",
            env_default=_MC_CONTEXT_ENV_DEFAULT,
            runtime_default=_MC_CONTEXT_RUNTIME_DEFAULT,
            runtime_default_label=f"default:ap_master_control.context_floor={_MC_CONTEXT_RUNTIME_DEFAULT}",
        )

    if mc_priority_floor is None:
        mc_priority_floor = _MC_PRIORITY_FLOOR_DEFAULT
    mc_priority_floor = float(mc_priority_floor)
    priority_source = f"default:ap_master_control.priority_floor={_MC_PRIORITY_FLOOR_DEFAULT}"

    thresholds = AdmissionThresholds(
        scanner_floor=float(scanner_floor),
        interrogation_floor=float(interrogation_floor),
        mc_score_floor=float(mc_score_floor),
        mc_priority_floor=float(mc_priority_floor),
        context_floor=float(context_floor),
    )
    config_hash = admission_threshold_config_hash(thresholds)
    return AdmissionThresholdResolution(
        thresholds=thresholds,
        sources={
            "scanner_floor": scanner_source,
            "interrogation_floor": interrogation_source,
            "mc_score_floor": mc_score_source,
            "mc_priority_floor": priority_source,
            "context_floor": context_source,
        },
        config_hash=config_hash,
    )


def admission_threshold_config_hash(thresholds: AdmissionThresholds) -> str:
    payload = json.dumps(
        {
            "scanner_floor": float(thresholds.scanner_floor),
            "interrogation_floor": float(thresholds.interrogation_floor),
            "mc_score_floor": float(thresholds.mc_score_floor),
            "mc_priority_floor": float(thresholds.mc_priority_floor),
            "context_floor": float(thresholds.context_floor),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def build_threshold_trace(
    *,
    threshold_name: str,
    score_value: float | None,
    floor_value: float | None,
    passed: bool | None,
    source: str,
) -> dict[str, Any]:
    return {
        "threshold": threshold_name,
        "score_value": score_value,
        "floor_value": floor_value,
        "passed": passed,
        "source": source,
    }


def build_interrogation_packet_threshold_trace(
    packet: Any,
    *,
    resolved: AdmissionThresholdResolution,
) -> dict[str, Any]:
    floor = float(resolved.thresholds.interrogation_floor)
    if packet is None:
        return build_threshold_trace(
            threshold_name="interrogation_floor",
            score_value=None,
            floor_value=floor,
            passed=None,
            source="unavailable:interrogation_packet_missing",
        )

    if isinstance(packet, dict):
        score_value = packet.get("quality_score")
    else:
        score_value = getattr(packet, "quality_score", None)
    try:
        score_value = float(score_value)
    except (TypeError, ValueError):
        score_value = None
    if score_value is None:
        return build_threshold_trace(
            threshold_name="interrogation_floor",
            score_value=None,
            floor_value=floor,
            passed=None,
            source="unavailable:interrogation_quality_missing",
        )
    return build_threshold_trace(
        threshold_name="interrogation_floor",
        score_value=score_value,
        floor_value=floor,
        passed=score_value >= floor,
        source=resolved.sources["interrogation_floor"],
    )


def log_admission_thresholds(
    logger: logging.Logger,
    *,
    component: str,
    resolved: AdmissionThresholdResolution,
) -> None:
    thresholds = resolved.thresholds
    logger.info(
        "[%s] Admission thresholds | hash=%s | scanner_floor=%.1f (%s) | interrogation_floor=%.1f (%s) | "
        "mc_score_floor=%.1f (%s) | mc_priority_floor=%.1f (%s) | context_floor=%.1f (%s)",
        component,
        resolved.config_hash,
        thresholds.scanner_floor,
        resolved.sources["scanner_floor"],
        thresholds.interrogation_floor,
        resolved.sources["interrogation_floor"],
        thresholds.mc_score_floor,
        resolved.sources["mc_score_floor"],
        thresholds.mc_priority_floor,
        resolved.sources["mc_priority_floor"],
        thresholds.context_floor,
        resolved.sources["context_floor"],
    )
