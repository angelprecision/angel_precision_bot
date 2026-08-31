"""Immutable per-client daily intelligence feed and outcome measurement.

Every scanner signal is retained in ``ap_intelligence_snapshots``.  This
module performs a separate, deterministic, point-in-time selection of at most
10 client opportunities and marks only the first 7 as the evaluation cohort.
Outcomes are joined and persisted after selection; they never enter ranking.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import date, datetime, time, timezone
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from ap.intelligence_score import score_matches_identity, score_policy_version
from ap.trade_dossier import build_intelligence_review


ET = ZoneInfo("America/New_York")
FEED_LIMIT = 10
EVALUATION_LIMIT = 7
DAILY_RANKING_VERSION = "daily_intelligence_ranking_v1"
DEFAULT_POLICY_FROZEN_AT = "2026-07-19T00:00:00+00:00"


def _row_value(row: Any, key: str, index: int, default: Any = None) -> Any:
    if isinstance(row, Mapping):
        return row.get(key, default)
    try:
        return row[index]
    except (IndexError, TypeError):
        return default


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _aware(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _session_date(value: Any = None) -> date:
    if value is None:
        return datetime.now(ET).date()
    if isinstance(value, datetime):
        return value.astimezone(ET).date() if value.tzinfo else value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _ranking_run_id(client_id: str, execution_mode: str, session: date, policy_version: str) -> str:
    key = "|".join((DAILY_RANKING_VERSION, client_id, execution_mode.upper(), session.isoformat(), policy_version))
    return str(uuid.uuid5(uuid.NAMESPACE_URL, key))


def _ranking_id(run_id: str, canonical_signal_id: str) -> str:
    return str(uuid.uuid5(uuid.UUID(run_id), canonical_signal_id))


def rank_pretrigger_candidates(
    rows: Iterable[Any],
    *,
    policy_version: str,
    policy_frozen_at: Any,
) -> dict[str, Any]:
    """Rank latest PRETRIGGER rows using only integrity-valid score envelopes."""

    frozen_at = _aware(policy_frozen_at)
    source = list(rows)
    latest_source: dict[str, tuple[tuple[int, str], Any]] = {}
    for row in source:
        payload = _mapping(_row_value(row, "payload", 7, {}))
        canonical = str(_row_value(row, "canonical_signal_id", 2, "") or payload.get("canonical_signal_id") or "")
        if not canonical:
            continue
        score = payload.get("intelligence_score")
        scored_at = str(score.get("scored_at") or "") if isinstance(score, Mapping) else ""
        revision = int(_row_value(row, "context_revision", 4, 0) or 0)
        sort_key = (revision, scored_at)
        previous = latest_source.get(canonical)
        if previous is None or sort_key > previous[0]:
            latest_source[canonical] = (sort_key, row)

    latest: dict[str, dict[str, Any]] = {}
    for _sort_key, row in latest_source.values():
        payload = _mapping(_row_value(row, "payload", 7, {}))
        score = payload.get("intelligence_score")
        if not isinstance(score, Mapping):
            continue
        canonical = str(_row_value(row, "canonical_signal_id", 2, "") or payload.get("canonical_signal_id") or "")
        client_id = str(_row_value(row, "client_id", 0, "") or payload.get("client_id") or "")
        mode = str(_row_value(row, "execution_mode", 1, "") or payload.get("execution_mode") or "").upper()
        ticker = str(payload.get("ticker") or ((score.get("identity") or {}).get("ticker") if isinstance(score.get("identity"), Mapping) else ""))
        side = payload.get("side") or ((score.get("identity") or {}).get("side") if isinstance(score.get("identity"), Mapping) else "")
        try:
            scored_at = _aware(score.get("scored_at"))
        except (TypeError, ValueError):
            continue
        if scored_at <= frozen_at:
            continue
        if str(score.get("policy_version") or "") != policy_version:
            continue
        if not score_matches_identity(
            score,
            canonical_signal_id=canonical,
            client_id=client_id,
            execution_mode=mode,
            ticker=ticker,
            side=side,
        ):
            continue
        if not bool(score.get("score_valid") and score.get("eligible_for_ranking")):
            continue
        policy_score = score.get("policy_score")
        if not isinstance(policy_score, (int, float)):
            continue
        candidate = {
            "score_snapshot_id": str(_row_value(row, "snapshot_id", 3, "")),
            "client_id": client_id,
            "execution_mode": mode,
            "canonical_signal_id": canonical,
            "signal_id": str(payload.get("signal_id") or ""),
            "ticker": ticker.upper(),
            "side": str(side or "").upper(),
            "policy_score": float(policy_score),
            "score_input_hash": str(score.get("input_hash") or ""),
            "score_integrity_hash": str(score.get("score_integrity_hash") or ""),
            "scored_at": scored_at.isoformat(),
            "data_as_of": str(score.get("data_as_of") or ""),
            "context": {
                "ranking_version": DAILY_RANKING_VERSION,
                "intelligence_review": build_intelligence_review(score),
                "components": dict(score.get("components") or {}),
                "trade_geometry": dict(payload.get("trade_geometry") or {}),
                "strategy_advisories": list(payload.get("strategy_advisories") or []),
                "data_quality_warnings": list(payload.get("data_quality_warnings") or []),
                "timeframe_context": dict(payload.get("timeframe_context") or {}),
                "observe_only": True,
                "affected_eligibility": False,
            },
        }
        previous = latest.get(canonical)
        if previous is None or candidate["scored_at"] > previous["scored_at"]:
            latest[canonical] = candidate

    eligible = sorted(
        latest.values(),
        key=lambda item: (
            -item["policy_score"],
            item["scored_at"],
            item["canonical_signal_id"],
        ),
    )
    selected = []
    for index, item in enumerate(eligible[:FEED_LIMIT], start=1):
        selected.append({
            **item,
            "opportunity_rank": index,
            "selected_for_feed": True,
            "selected_for_trade_evaluation": index <= EVALUATION_LIMIT,
        })
    return {
        "source_count": len(latest_source),
        "eligible_count": len(eligible),
        "selected_count": len(selected),
        "evaluation_count": min(len(selected), EVALUATION_LIMIT),
        "selected": selected,
    }


def fetch_daily_feed(
    cursor: Any,
    *,
    client_id: str,
    execution_mode: str,
    session_date: Any = None,
    policy_version: str | None = None,
) -> dict[str, Any]:
    session = _session_date(session_date)
    cursor.execute(
        """
        SELECT id, policy_version, policy_frozen_at, selection_frozen_at,
               source_count, eligible_count, selected_count, evaluation_count
        FROM ap_daily_ranking_runs
        WHERE client_id=%s AND upper(execution_mode)=upper(%s) AND session_date=%s
          AND (%s IS NULL OR policy_version=%s)
        ORDER BY selection_frozen_at DESC
        LIMIT 1
        """,
        (client_id, execution_mode, session, policy_version, policy_version),
    )
    run = cursor.fetchone()
    if not run:
        return {"ok": True, "frozen": False, "session_date": session.isoformat(), "opportunities": []}
    run_id = str(_row_value(run, "id", 0, ""))
    cursor.execute(
        """
        SELECT id, canonical_signal_id, signal_id, ticker, side,
               opportunity_rank, selected_for_trade_evaluation, policy_score,
               scored_at, data_as_of, context
        FROM ap_daily_opportunity_rankings
        WHERE ranking_run_id=%s AND selected_for_feed IS TRUE
        ORDER BY opportunity_rank ASC
        LIMIT 10
        """,
        (run_id,),
    )
    opportunities = []
    for row in cursor.fetchall() or []:
        opportunities.append({
            "ranking_id": str(_row_value(row, "id", 0, "")),
            "canonical_signal_id": str(_row_value(row, "canonical_signal_id", 1, "")),
            "signal_id": str(_row_value(row, "signal_id", 2, "")),
            "ticker": str(_row_value(row, "ticker", 3, "")),
            "side": str(_row_value(row, "side", 4, "")),
            "rank": int(_row_value(row, "opportunity_rank", 5, 0)),
            "selected_for_trade_evaluation": bool(_row_value(row, "selected_for_trade_evaluation", 6, False)),
            "policy_score": float(_row_value(row, "policy_score", 7, 0.0)),
            "scored_at": str(_row_value(row, "scored_at", 8, "")),
            "data_as_of": str(_row_value(row, "data_as_of", 9, "")),
            "context": _mapping(_row_value(row, "context", 10, {})),
        })
    return {
        "ok": True,
        "frozen": True,
        "ranking_run_id": run_id,
        "session_date": session.isoformat(),
        "policy_version": str(_row_value(run, "policy_version", 1, "")),
        "policy_frozen_at": str(_row_value(run, "policy_frozen_at", 2, "")),
        "selection_frozen_at": str(_row_value(run, "selection_frozen_at", 3, "")),
        "source_count": int(_row_value(run, "source_count", 4, 0)),
        "eligible_count": int(_row_value(run, "eligible_count", 5, 0)),
        "selected_count": int(_row_value(run, "selected_count", 6, 0)),
        "evaluation_count": int(_row_value(run, "evaluation_count", 7, 0)),
        "opportunities": opportunities,
    }


def freeze_daily_rankings(
    cursor: Any,
    *,
    client_id: str,
    execution_mode: str,
    session_date: Any = None,
    policy_version: str | None = None,
    policy_frozen_at: Any = DEFAULT_POLICY_FROZEN_AT,
    selection_frozen_at: Any = None,
    minimum_source_count: int = 0,
) -> dict[str, Any]:
    session = _session_date(session_date)
    mode = str(execution_mode or "").upper()
    if mode not in {"PAPER", "LIVE"}:
        raise ValueError("execution_mode must be PAPER or LIVE")
    version = str(policy_version or score_policy_version())
    policy_frozen = _aware(policy_frozen_at)
    selection_frozen = _aware(selection_frozen_at or datetime.now(timezone.utc))
    existing = fetch_daily_feed(
        cursor,
        client_id=client_id,
        execution_mode=mode,
        session_date=session,
        policy_version=version,
    )
    if existing.get("frozen") and existing.get("policy_version") == version:
        return {**existing, "created": False}
    cursor.execute(
        """
        SELECT DISTINCT ON (canonical_signal_id)
               client_id, execution_mode, canonical_signal_id, id AS snapshot_id,
               context_revision, created_at, computed_at, payload
        FROM ap_intelligence_snapshots
        WHERE client_id=%s
          AND upper(execution_mode)=upper(%s)
          AND phase='PRETRIGGER'
          AND (created_at AT TIME ZONE 'America/New_York')::date=%s
          AND created_at <= %s
        ORDER BY canonical_signal_id, context_revision DESC, computed_at DESC
        """,
        (client_id, mode, session, selection_frozen),
    )
    ranked = rank_pretrigger_candidates(
        cursor.fetchall() or [],
        policy_version=version,
        policy_frozen_at=policy_frozen,
    )
    required_source = max(0, int(minimum_source_count or 0))
    if ranked["source_count"] < required_source:
        return {
            "ok": True,
            "created": False,
            "frozen": False,
            "reason": "source_population_below_freeze_minimum",
            "required_source_count": required_source,
            **ranked,
        }
    run_id = _ranking_run_id(client_id, mode, session, version)
    cursor.execute(
        """
        INSERT INTO ap_daily_ranking_runs (
          id, client_id, execution_mode, session_date, policy_version,
          policy_frozen_at, selection_frozen_at, source_count, eligible_count,
          selected_count, evaluation_count, feed_limit, evaluation_limit, status
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,10,7,'FROZEN')
        ON CONFLICT (client_id, execution_mode, session_date, policy_version)
        DO NOTHING
        RETURNING id
        """,
        (
            run_id, client_id, mode, session, version,
            policy_frozen, selection_frozen,
            ranked["source_count"], ranked["eligible_count"],
            ranked["selected_count"], ranked["evaluation_count"],
        ),
    )
    inserted = cursor.fetchone()
    if not inserted:
        return {
            **fetch_daily_feed(
                cursor,
                client_id=client_id,
                execution_mode=mode,
                session_date=session,
                policy_version=version,
            ),
            "created": False,
        }
    for item in ranked["selected"]:
        ranking_id = _ranking_id(run_id, item["canonical_signal_id"])
        cursor.execute(
            """
            INSERT INTO ap_daily_opportunity_rankings (
              id, ranking_run_id, score_snapshot_id, client_id, execution_mode,
              session_date, policy_version, canonical_signal_id, signal_id,
              ticker, side, opportunity_rank, selected_for_feed,
              selected_for_trade_evaluation, policy_score, score_input_hash,
              score_integrity_hash, scored_at, data_as_of, context, frozen_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,TRUE,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
            """,
            (
                ranking_id, run_id, item["score_snapshot_id"], client_id, mode,
                session, version, item["canonical_signal_id"], item["signal_id"],
                item["ticker"], item["side"], item["opportunity_rank"],
                item["selected_for_trade_evaluation"], item["policy_score"],
                item["score_input_hash"], item["score_integrity_hash"],
                item["scored_at"], item["data_as_of"],
                json.dumps(item["context"], default=str), selection_frozen,
            ),
        )
    return {
        "ok": True,
        "created": True,
        "frozen": True,
        "ranking_run_id": run_id,
        "session_date": session.isoformat(),
        "policy_version": version,
        **ranked,
    }


def reconcile_official_outcomes(cursor: Any, *, client_id: str, lookback_days: int = 10) -> dict[str, Any]:
    """Append broker-proven LIVE outcomes for the frozen evaluation cohort."""

    cursor.execute(
        """
        SELECT r.id AS ranking_id, pt.id AS proof_trade_id, pt.position_id,
               pt.local_order_id, pt.option_pnl_pct, pt.closed_at,
               pt.performance_taxonomy, pt.training_eligible
        FROM ap_daily_opportunity_rankings r
        JOIN LATERAL (
          SELECT pt.id, pt.position_id, pt.local_order_id, pt.option_pnl_pct,
                 pt.closed_at, pt.performance_taxonomy, pt.training_eligible
          FROM orders o
          JOIN proof_trades pt
            ON pt.local_order_id=o.local_order_id
           AND pt.client_email=o.client_id
          WHERE o.client_id=r.client_id
            AND o.canonical_signal_id=r.canonical_signal_id
            AND upper(COALESCE(o.execution_mode, ''))='LIVE'
            AND upper(COALESCE(o.kind, 'ENTRY'))='ENTRY'
            AND o.created_ts >= r.scored_at
            AND pt.training_eligible IS TRUE
            AND pt.performance_taxonomy='LIVE_OFFICIAL'
            AND pt.option_pnl_pct IS NOT NULL
            AND pt.option_pnl_pct >= -100.0
            AND pt.closed_at > r.scored_at
          ORDER BY pt.closed_at ASC
          LIMIT 1
        ) pt ON TRUE
        LEFT JOIN ap_daily_opportunity_outcomes outcome
          ON outcome.ranking_id=r.id AND outcome.outcome_class='LIVE_OFFICIAL'
        WHERE r.client_id=%s
          AND upper(r.execution_mode)='LIVE'
          AND r.selected_for_trade_evaluation IS TRUE
          AND r.session_date >= (CURRENT_DATE - %s)
          AND outcome.id IS NULL
        ORDER BY pt.closed_at ASC
        """,
        (client_id, max(1, min(int(lookback_days or 10), 90))),
    )
    rows = list(cursor.fetchall() or [])
    inserted = 0
    for row in rows:
        ranking_id = str(_row_value(row, "ranking_id", 0, ""))
        proof_trade_id = str(_row_value(row, "proof_trade_id", 1, ""))
        option_pnl_pct = float(_row_value(row, "option_pnl_pct", 4, 0.0))
        outcome_id = str(uuid.uuid5(uuid.UUID(ranking_id), f"LIVE_OFFICIAL:{proof_trade_id}"))
        cursor.execute(
            """
            INSERT INTO ap_daily_opportunity_outcomes (
              id, ranking_id, outcome_class, eligible_for_promotion,
              proof_trade_id, position_id, local_order_id, return_fraction,
              win, known_at, evidence
            ) VALUES (%s,%s,'LIVE_OFFICIAL',TRUE,%s,%s,%s,%s,%s,%s,%s::jsonb)
            ON CONFLICT DO NOTHING
            """,
            (
                outcome_id, ranking_id, proof_trade_id,
                str(_row_value(row, "position_id", 2, "")),
                str(_row_value(row, "local_order_id", 3, "")),
                option_pnl_pct / 100.0,
                option_pnl_pct > 0.0,
                _row_value(row, "closed_at", 5),
                json.dumps({
                    "performance_taxonomy": _row_value(row, "performance_taxonomy", 6),
                    "training_eligible": bool(_row_value(row, "training_eligible", 7, False)),
                    "source": "proof_trades_joined_through_originating_live_order",
                }),
            ),
        )
        inserted += int(getattr(cursor, "rowcount", 1) == 1)
    return {"ok": True, "candidates": len(rows), "inserted": inserted}


def fetch_daily_performance(cursor: Any, *, client_id: str, session_date: Any = None) -> dict[str, Any]:
    session = _session_date(session_date)
    cursor.execute(
        """
        SELECT o.return_fraction
        FROM ap_daily_opportunity_rankings r
        JOIN ap_daily_opportunity_outcomes o ON o.ranking_id=r.id
        WHERE r.client_id=%s AND r.session_date=%s
          AND r.selected_for_trade_evaluation IS TRUE
          AND o.outcome_class='LIVE_OFFICIAL'
          AND o.eligible_for_promotion IS TRUE
        ORDER BY o.known_at ASC
        """,
        (client_id, session),
    )
    returns = [float(_row_value(row, "return_fraction", 0, 0.0)) for row in (cursor.fetchall() or [])]
    wins = [value for value in returns if value > 0]
    losses = [-value for value in returns if value < 0]
    return {
        "ok": True,
        "session_date": session.isoformat(),
        "measurement_class": "LIVE_OFFICIAL_ONLY",
        "resolved": len(returns),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / len(returns) if returns else None,
        "average_win": sum(wins) / len(wins) if wins else None,
        "average_loss": sum(losses) / len(losses) if losses else None,
        "expectancy": sum(returns) / len(returns) if returns else None,
        "targets": {
            "win_rate": 0.80,
            "minimum_average_win": 0.18,
            "maximum_average_loss": 0.12,
            "daily_evaluation_opportunities": EVALUATION_LIMIT,
        },
        "counterfactuals_included": False,
    }


def freeze_due_daily_rankings_best_effort(*, client_id: str, execution_mode: str) -> dict[str, Any]:
    if os.getenv("INTELLIGENCE_CONTEXT_STORE_BACKEND", "").lower() == "memory":
        return {"ok": True, "created": False, "reason": "memory_backend"}
    enabled = os.getenv("INTELLIGENCE_DAILY_RANKING_ENABLED", "1").strip().lower()
    if enabled not in {"1", "true", "yes", "on"}:
        return {"ok": True, "created": False, "reason": "disabled"}
    now_et = datetime.now(ET)
    if now_et.weekday() >= 5:
        return {"ok": True, "created": False, "reason": "non_market_day"}
    raw_freeze = os.getenv("INTELLIGENCE_DAILY_FREEZE_ET", "10:30")
    try:
        hour, minute = (int(part) for part in raw_freeze.split(":", 1))
        due_at = time(hour=hour, minute=minute)
    except (TypeError, ValueError):
        return {"ok": False, "created": False, "reason": "invalid_INTELLIGENCE_DAILY_FREEZE_ET"}
    if now_et.time() < due_at:
        return {"ok": True, "created": False, "reason": "not_due"}
    raw_hard_freeze = os.getenv("INTELLIGENCE_DAILY_HARD_FREEZE_ET", "11:00")
    try:
        hard_hour, hard_minute = (int(part) for part in raw_hard_freeze.split(":", 1))
        hard_due_at = time(hour=hard_hour, minute=hard_minute)
    except (TypeError, ValueError):
        return {"ok": False, "created": False, "reason": "invalid_INTELLIGENCE_DAILY_HARD_FREEZE_ET"}
    try:
        configured_minimum = max(1, int(os.getenv("INTELLIGENCE_DAILY_MIN_SOURCE_COUNT", "10")))
    except (TypeError, ValueError):
        return {"ok": False, "created": False, "reason": "invalid_INTELLIGENCE_DAILY_MIN_SOURCE_COUNT"}
    minimum_source_count = configured_minimum if now_et.time() < hard_due_at else 1
    policy_frozen_at = os.getenv("INTELLIGENCE_POLICY_FROZEN_AT", DEFAULT_POLICY_FROZEN_AT)
    try:
        from ap.db import conn
        with conn() as cursor:
            frozen = freeze_daily_rankings(
                cursor,
                client_id=client_id,
                execution_mode=execution_mode,
                session_date=now_et.date(),
                policy_frozen_at=policy_frozen_at,
                minimum_source_count=minimum_source_count,
            )
            outcomes = reconcile_official_outcomes(cursor, client_id=client_id)
            return {**frozen, "outcome_reconciliation": outcomes}
    except Exception as exc:
        return {"ok": False, "created": False, "reason": str(exc)[:500]}
