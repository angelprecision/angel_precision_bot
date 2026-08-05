"""
tests/fixtures/july23_late_attachment_synthetic.py
====================================================
Production-shaped synthetic July-23 fixture (119 rows total):
  * Jason LIVE:          33 distinct tickers
  * Jose PAPER:          41 distinct tickers
  * Tradefluence PAPER:  45 distinct tickers

Used by tests/test_p0_july23_late_attachment_replay.py when no real
Supabase-derived fixture is provided at tests/fixtures/july23_late_attachment.json.

Each row is tagged with an expected_bucket so the replay can assert the
population-level outcome:
  * within                    — arm-time canonical quote inside the
                                continuation window; must NOT terminalize
                                (this is the class of rows the amendment
                                exists to rescue)
  * waiting_reset             — past the continuation window but not decisive
                                drift; may transition to WAITING_RESET
  * far_missed                — beyond the decisive drift threshold
                                (MAX_INTRADAY_DRIFT_PCT ~ 1.5%); MUST
                                terminalize as
                                LATE_ATTACHMENT_MOVE_MISSED_TERMINAL
  * stop_broken               — stop-side quote broken; MUST terminalize
                                as STOP_ALREADY_BROKEN_TERMINAL
  * pre_trigger               — quote still on ordinary pre-trigger side;
                                classifier returns TRIGGER_TRUTH_UNAVAILABLE_
                                RETRY with a valid quote (ordinary arm)

Bucket distribution mirrors the July-23 incident: the vast majority of rows
were miscategorized as arm_already_through_trigger despite being inside a
tiny continuation window. A minority were legitimately far-missed.
"""
from __future__ import annotations

from typing import List


# Distribution per client — mostly WITHIN (the rescue population), with a
# realistic minority of WAITING_RESET, FAR_MISSED, STOP_BROKEN, PRE_TRIGGER.
# The counts sum to the per-client totals from the incident report.
#
# Jason LIVE (33): 21 within + 6 waiting_reset + 3 far_missed + 2 stop_broken + 1 pre_trigger
# Jose PAPER (41): 27 within + 8 waiting_reset + 4 far_missed + 1 stop_broken + 1 pre_trigger
# Tradefluence PAPER (45): 30 within + 8 waiting_reset + 4 far_missed + 2 stop_broken + 1 pre_trigger
#
# Buckets computed from bid/ask/stop/target parameters that will drive
# classify_late_attachment to the expected classification.

_TRIGGER_CALL_BASE = 100.0
_TRIGGER_PUT_BASE = 200.0
_STOP_CALL_BASE = 95.0
_STOP_PUT_BASE = 205.0
_TARGET_CALL_BASE = 150.0
_TARGET_PUT_BASE = 150.0


def _row(
    *,
    client_id: str,
    execution_mode: str,
    ticker: str,
    side: str,
    bucket: str,
    row_index: int,
) -> dict:
    trigger = _TRIGGER_CALL_BASE if side == "CALL" else _TRIGGER_PUT_BASE
    stop    = _STOP_CALL_BASE    if side == "CALL" else _STOP_PUT_BASE
    target  = _TARGET_CALL_BASE  if side == "CALL" else _TARGET_PUT_BASE

    # Allowed continuation window for THIS row's trigger:
    #   min(ENTRY_TRIGGER_CONTINUATION_MAX_ABS=0.15,
    #       trigger * ENTRY_TRIGGER_CONTINUATION_MAX_BPS=7.5 / 10000)
    # CALL trigger=100  → allowed = 0.075
    # PUT  trigger=200  → allowed = 0.15
    _allowed = min(0.15, trigger * 7.5 / 10000.0)
    _reset_tol = max(0.01, _allowed * 0.5)
    if bucket == "within":
        # Quote strictly inside the continuation zone.
        # Vary across rows without leaving the window. Ensure stop-side
        # quote (CALL bid / PUT ask) is safe (above stop).
        offset = min(_allowed * 0.5, 0.02 + (row_index % 4) * 0.005)
        # Clamp to a small safety margin below the upper bound.
        offset = min(offset, _allowed - 0.005)
        offset = max(offset, 0.005)
        if side == "CALL":
            ask = trigger + offset
            bid = trigger + offset - 0.005   # tight spread, still above stop
        else:
            bid = trigger - offset
            ask = trigger - offset + 0.005
    elif bucket == "waiting_reset":
        # Past the continuation zone but within decisive drift
        # (MAX_INTRADAY_DRIFT_PCT ~ 1.5%). Pick an offset in
        # (allowed_continuation, trigger * 0.01).
        base_pct = 0.003 + (row_index % 4) * 0.001   # 0.3%..0.6%
        if side == "CALL":
            ask = trigger * (1.0 + base_pct)
            # Ensure ask is definitely past the zone.
            if ask <= trigger + _allowed:
                ask = trigger + _allowed + 0.02
            bid = ask - 0.005
        else:
            bid = trigger * (1.0 - base_pct)
            if bid >= trigger - _allowed:
                bid = trigger - _allowed - 0.02
            ask = bid + 0.005
    elif bucket == "far_missed":
        # Beyond decisive drift threshold (1.5%). Use 5-10% offset.
        offset = 0.05 + (row_index % 3) * 0.02
        if side == "CALL":
            ask = trigger * (1.0 + offset)
            bid = ask - 0.02
        else:
            bid = trigger * (1.0 - offset)
            ask = bid + 0.02
    elif bucket == "stop_broken":
        # Stop-side quote crossed the stop level after the entry-direction
        # breach. For CALL: bid <= stop and ask >= trigger. For PUT: ask >=
        # stop and bid <= trigger. The explicit trigger-side breach matters:
        # scanner-stop geometry is dormant while a setup is still pre-trigger.
        if side == "CALL":
            bid = stop - 0.10
            ask = trigger + 0.05
        else:
            ask = stop + 0.10
            bid = trigger - 0.05
    elif bucket == "pre_trigger":
        # Quote on the ordinary pre-trigger side of the canonical lane.
        offset = 0.10 + (row_index % 3) * 0.05
        if side == "CALL":
            ask = trigger - offset
            bid = ask - 0.02
        else:
            bid = trigger + offset
            ask = bid + 0.02
    else:
        raise ValueError(f"unknown bucket {bucket!r}")

    return {
        "client_id":       client_id,
        "execution_mode":  execution_mode,
        "ticker":          ticker,
        "side":            side,
        "trigger":         trigger,
        "arm_time_bid":    round(float(bid), 4),
        "arm_time_ask":    round(float(ask), 4),
        "stop":            stop,
        "target_price":    target,
        "target_complete": False,
        "arm_time_iso":    "2026-07-23T13:32:00+00:00",
        "expected_bucket": bucket,
    }


def _rows_for_client(
    *,
    client_id: str,
    execution_mode: str,
    within: int,
    waiting_reset: int,
    far_missed: int,
    stop_broken: int,
    pre_trigger: int,
    ticker_prefix: str,
) -> List[dict]:
    plan = (
        [("within", within), ("waiting_reset", waiting_reset),
         ("far_missed", far_missed), ("stop_broken", stop_broken),
         ("pre_trigger", pre_trigger)]
    )
    rows: List[dict] = []
    idx = 0
    for bucket, count in plan:
        for i in range(count):
            side = "CALL" if idx % 2 == 0 else "PUT"
            ticker = f"{ticker_prefix}{idx:03d}"
            rows.append(_row(
                client_id=client_id,
                execution_mode=execution_mode,
                ticker=ticker,
                side=side,
                bucket=bucket,
                row_index=i,
            ))
            idx += 1
    return rows


def build_july23_synthetic_rows() -> List[dict]:
    rows: List[dict] = []
    rows.extend(_rows_for_client(
        client_id="jason@example.com", execution_mode="live",
        within=21, waiting_reset=6, far_missed=3,
        stop_broken=2, pre_trigger=1,
        ticker_prefix="JA_",
    ))
    rows.extend(_rows_for_client(
        client_id="jose@example.com", execution_mode="paper",
        within=27, waiting_reset=8, far_missed=4,
        stop_broken=1, pre_trigger=1,
        ticker_prefix="JO_",
    ))
    rows.extend(_rows_for_client(
        client_id="tradefluence@example.com", execution_mode="paper",
        within=30, waiting_reset=8, far_missed=4,
        stop_broken=2, pre_trigger=1,
        ticker_prefix="TF_",
    ))
    return rows
