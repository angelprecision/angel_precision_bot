from __future__ import annotations

"""Deterministic, read-only regression replay primitives.

The harness imports no database, broker, HTTP, queue, or order-state code. It
loads immutable recorded evidence, runs explicitly injected pure stage adapters,
and reports the first behavioral divergence between two traces.
"""

import argparse
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Iterable, Mapping, Sequence

UNKNOWN = "UNKNOWN"
SCHEMA_VERSION = 1
STAGE_ORDER: tuple[str, ...] = (
    "scanner",
    "queue_normalization",
    "master_control",
    "gate_g",
    "watcher",
    "breach",
    "contract_selector",
    "submit_gate",
    "fill",
    "exit_decision",
    "protective_exit_submission",
    "reconciliation",
)
_CRITICAL_EVIDENCE_FIELDS = (
    "execution_mode",
    "signal_id",
    "local_order_id",
    "selected_contract",
    "score",
    "pattern",
)
_SECRET_KEY_RE = re.compile(
    r"(?:password|passwd|secret|token|api[_-]?key|authorization|broker[_-]?order[_-]?id)$",
    re.IGNORECASE,
)
_EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
_SHA_RE = re.compile(r"^[0-9a-f]{7,40}$", re.IGNORECASE)


class FixtureValidationError(ValueError):
    """Raised when fixture data would conceal, invent, or leak evidence."""


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(k): _deep_freeze(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze(v) for v in value)
    if isinstance(value, set):
        return frozenset(_deep_freeze(v) for v in value)
    return value


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in sorted(value.items())}
    if isinstance(value, (tuple, list)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted(_json_safe(v) for v in value)
    return value


@dataclass(frozen=True)
class StageEvent:
    stage: str
    decision: str
    reason_code: str = UNKNOWN
    contract: str = UNKNOWN
    timestamp: str = UNKNOWN
    details: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "StageEvent":
        stage = str(payload.get("stage") or "").strip()
        if stage not in STAGE_ORDER:
            raise FixtureValidationError(f"unknown replay stage: {stage!r}")
        return cls(
            stage=stage,
            decision=str(payload.get("decision") or UNKNOWN).strip() or UNKNOWN,
            reason_code=str(payload.get("reason_code") or UNKNOWN).strip() or UNKNOWN,
            contract=str(payload.get("contract") or UNKNOWN).strip() or UNKNOWN,
            timestamp=str(payload.get("timestamp") or UNKNOWN).strip() or UNKNOWN,
            details=_deep_freeze(dict(payload.get("details") or {})),
        )


@dataclass(frozen=True)
class ReplayFixture:
    fixture_id: str
    source_window: str
    client_alias: str
    ticker: str
    side: str
    timeframe: str
    deployment_git_commits: tuple[str, ...]
    deployment_commit_confidence: str
    current_main: str
    evidence: Mapping[str, Any]
    outcome: Mapping[str, Any]
    historical_trace: tuple[StageEvent, ...]
    evidence_gaps: tuple[str, ...]

    @property
    def authoritative_pnl_pct(self) -> float:
        value = self.outcome.get("realized_pnl_pct")
        if value in (None, "", UNKNOWN):
            value = self.outcome.get("option_pnl_pct")
        try:
            return float(value)
        except (TypeError, ValueError) as exc:
            raise FixtureValidationError(
                f"{self.fixture_id}: no numeric authoritative P&L"
            ) from exc

    @property
    def derived_win(self) -> bool:
        return self.authoritative_pnl_pct > 0

    def read_only_view(self) -> Mapping[str, Any]:
        return _deep_freeze(
            {
                "fixture_id": self.fixture_id,
                "source_window": self.source_window,
                "client_alias": self.client_alias,
                "ticker": self.ticker,
                "side": self.side,
                "timeframe": self.timeframe,
                "deployment_git_commits": self.deployment_git_commits,
                "deployment_commit_confidence": self.deployment_commit_confidence,
                "current_main": self.current_main,
                "evidence": _json_safe(self.evidence),
                "outcome": _json_safe(self.outcome),
                "historical_trace": tuple(
                    _stage_event_to_dict(event) for event in self.historical_trace
                ),
                "evidence_gaps": self.evidence_gaps,
            }
        )


@dataclass(frozen=True)
class ReplayTrace:
    fixture_id: str
    version: str
    events: tuple[StageEvent, ...]
    authoritative_pnl_pct: float
    derived_win: bool

    def by_stage(self) -> Mapping[str, StageEvent]:
        """Return the final event per stage for convenient point lookups."""
        return MappingProxyType({event.stage: event for event in self.events})

    def events_for_stage(self, stage: str) -> tuple[StageEvent, ...]:
        if stage not in STAGE_ORDER:
            raise FixtureValidationError(f"unknown replay stage: {stage!r}")
        return tuple(event for event in self.events if event.stage == stage)


@dataclass(frozen=True)
class ReplayComparison:
    fixture_id: str
    baseline_version: str
    candidate_version: str
    first_divergence_stage: str | None
    baseline_event: StageEvent | None
    candidate_event: StageEvent | None
    changed: bool


StageRunnerResult = (
    StageEvent | Mapping[str, Any] | Sequence[StageEvent | Mapping[str, Any]]
)
StageRunner = Callable[
    [Mapping[str, Any], tuple[StageEvent, ...]], StageRunnerResult
]


def _walk(value: Any, path: str = "") -> Iterable[tuple[str, Any]]:
    if isinstance(value, Mapping):
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            yield child_path, child
            yield from _walk(child, child_path)
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            child_path = f"{path}[{index}]"
            yield child_path, child
            yield from _walk(child, child_path)


def _validate_no_sensitive_data(payload: Mapping[str, Any], fixture_id: str) -> None:
    for path, value in _walk(payload):
        key = path.rsplit(".", 1)[-1].split("[", 1)[0]
        if _SECRET_KEY_RE.search(key):
            raise FixtureValidationError(
                f"{fixture_id}: sensitive key is forbidden in fixtures: {path}"
            )
        if isinstance(value, str) and _EMAIL_RE.fullmatch(value.strip()):
            raise FixtureValidationError(
                f"{fixture_id}: raw email is forbidden; use a client_alias: {path}"
            )


def _validate_fixture_payload(payload: Mapping[str, Any], current_main: str) -> None:
    fixture_id = str(payload.get("fixture_id") or "").strip()
    if not fixture_id:
        raise FixtureValidationError("fixture_id is required")
    for key in ("source_window", "client_alias", "ticker", "side", "timeframe"):
        if not str(payload.get(key) or "").strip():
            raise FixtureValidationError(f"{fixture_id}: {key} is required")
    if not _SHA_RE.fullmatch(current_main):
        raise FixtureValidationError(f"{fixture_id}: invalid current_main SHA")

    commits = payload.get("deployment_git_commits")
    if not isinstance(commits, list) or not commits:
        raise FixtureValidationError(
            f"{fixture_id}: deployment_git_commits must be a non-empty list"
        )
    if any(
        commit != UNKNOWN and not _SHA_RE.fullmatch(str(commit)) for commit in commits
    ):
        raise FixtureValidationError(f"{fixture_id}: invalid deployment commit evidence")

    evidence = payload.get("evidence")
    if not isinstance(evidence, Mapping):
        raise FixtureValidationError(f"{fixture_id}: evidence must be an object")
    for field_name in _CRITICAL_EVIDENCE_FIELDS:
        if field_name not in evidence:
            raise FixtureValidationError(
                f"{fixture_id}: missing critical field {field_name}; write UNKNOWN explicitly"
            )
        if evidence[field_name] in (None, ""):
            raise FixtureValidationError(
                f"{fixture_id}: {field_name} must be a value or explicit UNKNOWN"
            )

    outcome = payload.get("outcome")
    if not isinstance(outcome, Mapping):
        raise FixtureValidationError(f"{fixture_id}: outcome must be an object")
    pnl = outcome.get("realized_pnl_pct")
    if pnl in (None, "", UNKNOWN):
        pnl = outcome.get("option_pnl_pct")
    try:
        float(pnl)
    except (TypeError, ValueError) as exc:
        raise FixtureValidationError(
            f"{fixture_id}: outcome requires numeric realized_pnl_pct or option_pnl_pct"
        ) from exc

    if not isinstance(payload.get("historical_trace"), list) or not payload[
        "historical_trace"
    ]:
        raise FixtureValidationError(f"{fixture_id}: historical_trace is required")
    if not isinstance(payload.get("evidence_gaps"), list):
        raise FixtureValidationError(f"{fixture_id}: evidence_gaps must be a list")
    _validate_no_sensitive_data(payload, fixture_id)


def load_fixture_bundle(path: str | Path) -> tuple[ReplayFixture, ...]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise FixtureValidationError("fixture bundle must be a JSON object")
    if int(raw.get("schema_version") or 0) != SCHEMA_VERSION:
        raise FixtureValidationError(
            f"unsupported schema_version={raw.get('schema_version')!r}"
        )
    current_main = str(raw.get("current_main") or "").strip()
    payloads = raw.get("fixtures")
    if not isinstance(payloads, list) or not payloads:
        raise FixtureValidationError("fixture bundle must contain fixtures")

    loaded: list[ReplayFixture] = []
    seen_ids: set[str] = set()
    for payload in payloads:
        if not isinstance(payload, Mapping):
            raise FixtureValidationError("each fixture must be an object")
        _validate_fixture_payload(payload, current_main)
        fixture_id = str(payload["fixture_id"])
        if fixture_id in seen_ids:
            raise FixtureValidationError(f"duplicate fixture_id: {fixture_id}")
        seen_ids.add(fixture_id)
        loaded.append(
            ReplayFixture(
                fixture_id=fixture_id,
                source_window=str(payload["source_window"]),
                client_alias=str(payload["client_alias"]),
                ticker=str(payload["ticker"]).upper(),
                side=str(payload["side"]).upper(),
                timeframe=str(payload["timeframe"]),
                deployment_git_commits=tuple(
                    str(value) for value in payload["deployment_git_commits"]
                ),
                deployment_commit_confidence=str(
                    payload.get("deployment_commit_confidence") or UNKNOWN
                ),
                current_main=current_main,
                evidence=_deep_freeze(dict(payload["evidence"])),
                outcome=_deep_freeze(dict(payload["outcome"])),
                historical_trace=tuple(
                    StageEvent.from_mapping(event)
                    for event in payload["historical_trace"]
                ),
                evidence_gaps=tuple(str(value) for value in payload["evidence_gaps"]),
            )
        )
    return tuple(loaded)


def historical_trace(fixture: ReplayFixture) -> ReplayTrace:
    return ReplayTrace(
        fixture_id=fixture.fixture_id,
        version="+".join(fixture.deployment_git_commits),
        events=fixture.historical_trace,
        authoritative_pnl_pct=fixture.authoritative_pnl_pct,
        derived_win=fixture.derived_win,
    )


def _coerce_runner_events(
    fixture_id: str, stage: str, result: StageRunnerResult
) -> tuple[StageEvent, ...]:
    if isinstance(result, StageEvent) or isinstance(result, Mapping):
        raw_events: Sequence[StageEvent | Mapping[str, Any]] = (result,)
    elif isinstance(result, Sequence) and not isinstance(result, (str, bytes)):
        raw_events = result
    else:
        raise FixtureValidationError(
            f"{fixture_id}: runner for {stage} returned unsupported result"
        )
    if not raw_events:
        raise FixtureValidationError(f"{fixture_id}: runner for {stage} returned no events")

    events: list[StageEvent] = []
    for raw_event in raw_events:
        event = (
            raw_event
            if isinstance(raw_event, StageEvent)
            else StageEvent.from_mapping(raw_event)
        )
        if event.stage != stage:
            raise FixtureValidationError(
                f"{fixture_id}: runner for {stage} returned stage={event.stage}"
            )
        events.append(event)
    return tuple(events)


def run_replay(
    fixture: ReplayFixture,
    *,
    version: str,
    stage_runners: Mapping[str, StageRunner],
) -> ReplayTrace:
    """Run supplied pure adapters in canonical stage order."""

    events: list[StageEvent] = []
    fixture_view = fixture.read_only_view()
    for stage in STAGE_ORDER:
        runner = stage_runners.get(stage)
        if runner is None:
            events.append(
                StageEvent(
                    stage=stage,
                    decision="NOT_RUN",
                    reason_code="NO_STAGE_RUNNER",
                )
            )
            continue
        events.extend(
            _coerce_runner_events(
                fixture.fixture_id, stage, runner(fixture_view, tuple(events))
            )
        )
    return ReplayTrace(
        fixture_id=fixture.fixture_id,
        version=str(version),
        events=tuple(events),
        authoritative_pnl_pct=fixture.authoritative_pnl_pct,
        derived_win=fixture.derived_win,
    )


def _event_signature(event: StageEvent) -> tuple[Any, ...]:
    return (
        event.decision,
        event.reason_code,
        event.contract,
        _json_safe(event.details),
    )


def compare_traces(
    baseline: ReplayTrace, candidate: ReplayTrace
) -> ReplayComparison:
    if baseline.fixture_id != candidate.fixture_id:
        raise FixtureValidationError("cannot compare traces from different fixtures")
    for stage in STAGE_ORDER:
        base_events = baseline.events_for_stage(stage)
        cand_events = candidate.events_for_stage(stage)
        if tuple(map(_event_signature, base_events)) == tuple(
            map(_event_signature, cand_events)
        ):
            continue
        mismatch_index = 0
        shared = min(len(base_events), len(cand_events))
        while (
            mismatch_index < shared
            and _event_signature(base_events[mismatch_index])
            == _event_signature(cand_events[mismatch_index])
        ):
            mismatch_index += 1
        return ReplayComparison(
            fixture_id=baseline.fixture_id,
            baseline_version=baseline.version,
            candidate_version=candidate.version,
            first_divergence_stage=stage,
            baseline_event=(
                base_events[mismatch_index]
                if mismatch_index < len(base_events)
                else None
            ),
            candidate_event=(
                cand_events[mismatch_index]
                if mismatch_index < len(cand_events)
                else None
            ),
            changed=True,
        )
    return ReplayComparison(
        fixture_id=baseline.fixture_id,
        baseline_version=baseline.version,
        candidate_version=candidate.version,
        first_divergence_stage=None,
        baseline_event=None,
        candidate_event=None,
        changed=False,
    )


def _stage_event_to_dict(event: StageEvent) -> dict[str, Any]:
    return {
        "stage": event.stage,
        "decision": event.decision,
        "reason_code": event.reason_code,
        "contract": event.contract,
        "timestamp": event.timestamp,
        "details": _json_safe(event.details),
    }


def comparison_to_dict(comparison: ReplayComparison) -> dict[str, Any]:
    return {
        "fixture_id": comparison.fixture_id,
        "baseline_version": comparison.baseline_version,
        "candidate_version": comparison.candidate_version,
        "first_divergence_stage": comparison.first_divergence_stage,
        "baseline_event": (
            _stage_event_to_dict(comparison.baseline_event)
            if comparison.baseline_event
            else None
        ),
        "candidate_event": (
            _stage_event_to_dict(comparison.candidate_event)
            if comparison.candidate_event
            else None
        ),
        "changed": comparison.changed,
    }


def build_fixture_summary(fixtures: Sequence[ReplayFixture]) -> dict[str, Any]:
    wins = sum(fixture.derived_win for fixture in fixtures)
    return {
        "fixtures": len(fixtures),
        "wins": wins,
        "losses": len(fixtures) - wins,
        "current_main": sorted({fixture.current_main for fixture in fixtures}),
        "deployment_commits": sorted(
            {
                commit
                for fixture in fixtures
                for commit in fixture.deployment_git_commits
            }
        ),
        "fixtures_with_unknown_execution_mode": sum(
            fixture.evidence.get("execution_mode") == UNKNOWN for fixture in fixtures
        ),
        "fixtures_with_missing_identity": sum(
            fixture.evidence.get("signal_id") == UNKNOWN
            or fixture.evidence.get("local_order_id") == UNKNOWN
            for fixture in fixtures
        ),
    }


def _default_fixture_path() -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "tests"
        / "fixtures"
        / "regression_replay"
        / "june_july_reference_trades.json"
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate read-only AP regression fixtures"
    )
    parser.add_argument("--fixtures", type=Path, default=_default_fixture_path())
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    summary = build_fixture_summary(load_fixture_bundle(args.fixtures))
    if args.json:
        print(json.dumps(summary, sort_keys=True))
    else:
        print(
            f"Regression fixtures: {summary['fixtures']} total, "
            f"{summary['wins']} wins, {summary['losses']} losses; "
            f"unknown_mode={summary['fixtures_with_unknown_execution_mode']}; "
            f"missing_identity={summary['fixtures_with_missing_identity']}"
        )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
