# =============================================================================
# ap/vol_exit_comparison.py — PR-G3
#
# The R-MULTIPLE COMPARISON HARNESS.
#
# Purpose: turn the paper-soak record into the single artifact PR-G2 (live
# enable of the volatility-scaled exit ladder) is required to carry —
# a legacy-vs-vol_scaled EXPECTANCY comparison in R-multiples, not win-rate.
#
# This module is READ-ONLY. It executes only SELECT statements. It never
# writes, never trades, never touches the exit path. It is safe to run at
# any time against production.
#
# Data sources (all existing tables, verified schemas):
#   - trade_reviews          : per-closed-trade pnl_dollars/pnl_pct/mae/mfe/
#                              entry_price/qty/exit_reason_code/git_commit
#   - positions              : realized_pnl, sl_pct, entry_price, exit_reason,
#                              execution_mode, exit_ts (fallback + risk basis)
#   - exit_decision_ledger   : metadata.ladder.ladder_mode per decision
#                              (populated by PR-G3 ledger patch) — this is how
#                              a closed trade is attributed to legacy vs
#                              vol_scaled.
#
# R-multiple definition (documented, defensible):
#   planned_risk_dollars = entry_price_per_contract × qty × sl_pct
#     (sl_pct is the option-P&L hard-stop fraction the trade was opened with;
#      this is the risk the SIZER actually committed, so R is measured against
#      the system's own stated risk unit — the correct denominator.)
#   realized_R = realized_pnl_dollars / planned_risk_dollars
#   When sl_pct or entry basis is missing/zero, the trade is EXCLUDED from the
#   R computation and counted in `excluded_no_risk_basis` (never silently
#   defaulted — false precision is worse than a smaller n).
#
# Attribution rule (which ladder owned a trade):
#   A trade is attributed to 'vol_scaled' iff ANY exit_decision_ledger row for
#   its position_id carries metadata.ladder.ladder_mode == 'vol_scaled'.
#   Otherwise 'legacy'. Rationale: vol_scaled only ever appears in the stamp
#   when the resolver actually applied it (triple-guarded), so its presence is
#   proof the vol ladder governed that position's exits. A trade whose ledger
#   rows are all legacy (or absent) is legacy-governed.
# =============================================================================
from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger("ap.vol_exit_comparison")

# Minimum sample size below which we DO NOT claim a comparison is meaningful.
# This is a gate, not a filter: the report still prints, but flags itself
# NOT_ENOUGH_DATA so no one mistakes noise for evidence.
MIN_TRADES_PER_ARM = 20  # per PR-G gate: >=5 sessions; ~20 closed trades floor


# ── data structures ──────────────────────────────────────────────────────────
@dataclass
class TradeRecord:
    position_id: str
    client_id: str
    ticker: str
    ladder_mode: str            # 'legacy' | 'vol_scaled'
    realized_pnl: Optional[float]
    planned_risk: Optional[float]
    realized_r: Optional[float]
    pnl_pct: Optional[float]
    mfe_pct: Optional[float]
    mae_pct: Optional[float]
    exit_reason_code: str
    exit_ts: Optional[str]
    git_commit: str


@dataclass
class ArmStats:
    arm: str
    n: int = 0
    n_with_r: int = 0
    wins: int = 0
    losses: int = 0
    win_rate: Optional[float] = None
    # EXPECTANCY — the headline number. Mean R across trades with a risk basis.
    expectancy_r: Optional[float] = None
    expectancy_r_ci_low: Optional[float] = None
    expectancy_r_ci_high: Optional[float] = None
    total_r: Optional[float] = None
    avg_win_r: Optional[float] = None
    avg_loss_r: Optional[float] = None
    median_r: Optional[float] = None
    std_r: Optional[float] = None
    avg_mfe_pct: Optional[float] = None
    avg_mae_pct: Optional[float] = None
    total_pnl_dollars: Optional[float] = None
    exit_reason_breakdown: dict = field(default_factory=dict)


@dataclass
class ComparisonReport:
    generated_at: str
    lookback_days: int
    client_filter: Optional[str]
    legacy: ArmStats
    vol_scaled: ArmStats
    delta_expectancy_r: Optional[float]
    verdict: str
    verdict_detail: str
    excluded_no_risk_basis: int
    total_trades_examined: int


# ── pure statistics (no I/O; unit-tested independently) ──────────────────────
def _mean(xs: list) -> Optional[float]:
    return sum(xs) / len(xs) if xs else None


def _median(xs: list) -> Optional[float]:
    if not xs:
        return None
    s = sorted(xs)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0


def _stddev(xs: list) -> Optional[float]:
    if len(xs) < 2:
        return None
    m = _mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


def _mean_ci95(xs: list) -> tuple:
    """95% CI on the mean via normal approx (t≈1.96 for the n we target).
    Deliberately simple and conservative; the point is 'is the interval
    clear of zero / clear of legacy', not a p-value contest."""
    if len(xs) < 2:
        return (None, None)
    m = _mean(xs)
    sd = _stddev(xs)
    if sd is None:
        return (None, None)
    se = sd / math.sqrt(len(xs))
    return (m - 1.96 * se, m + 1.96 * se)


def compute_arm_stats(arm: str, trades: list) -> ArmStats:
    """Aggregate one arm's trades into ArmStats. Pure — no I/O."""
    st = ArmStats(arm=arm, n=len(trades))
    r_values = [t.realized_r for t in trades if t.realized_r is not None]
    st.n_with_r = len(r_values)

    # Win/loss defined on realized P&L (dollars) so trades WITHOUT an R basis
    # still count toward win-rate; R stats use only trades with a basis.
    pnl_trades = [t for t in trades if t.realized_pnl is not None]
    st.wins = sum(1 for t in pnl_trades if t.realized_pnl > 0)
    st.losses = sum(1 for t in pnl_trades if t.realized_pnl <= 0)
    if pnl_trades:
        st.win_rate = st.wins / len(pnl_trades)
        st.total_pnl_dollars = round(sum(t.realized_pnl for t in pnl_trades), 2)

    if r_values:
        st.expectancy_r = round(_mean(r_values), 4)
        st.total_r = round(sum(r_values), 4)
        st.median_r = round(_median(r_values), 4)
        sd = _stddev(r_values)
        st.std_r = round(sd, 4) if sd is not None else None
        lo, hi = _mean_ci95(r_values)
        st.expectancy_r_ci_low = round(lo, 4) if lo is not None else None
        st.expectancy_r_ci_high = round(hi, 4) if hi is not None else None
        wins_r = [r for r in r_values if r > 0]
        losses_r = [r for r in r_values if r <= 0]
        st.avg_win_r = round(_mean(wins_r), 4) if wins_r else None
        st.avg_loss_r = round(_mean(losses_r), 4) if losses_r else None

    mfe = [t.mfe_pct for t in trades if t.mfe_pct is not None]
    mae = [t.mae_pct for t in trades if t.mae_pct is not None]
    st.avg_mfe_pct = round(_mean(mfe), 4) if mfe else None
    st.avg_mae_pct = round(_mean(mae), 4) if mae else None

    breakdown: dict = {}
    for t in trades:
        breakdown[t.exit_reason_code] = breakdown.get(t.exit_reason_code, 0) + 1
    st.exit_reason_breakdown = breakdown
    return st


def render_verdict(legacy: ArmStats, vol: ArmStats) -> tuple:
    """Return (verdict, detail). Conservative by construction: only recommends
    live consideration when vol_scaled expectancy beats legacy AND the vol arm
    has enough data AND its expectancy CI lower bound clears legacy's point
    expectancy. Anything short of that returns HOLD."""
    if vol.n < MIN_TRADES_PER_ARM or legacy.n < MIN_TRADES_PER_ARM:
        return ("NOT_ENOUGH_DATA",
                f"Need >= {MIN_TRADES_PER_ARM} trades per arm "
                f"(legacy={legacy.n}, vol_scaled={vol.n}). Keep soaking.")
    if vol.expectancy_r is None or legacy.expectancy_r is None:
        return ("NOT_ENOUGH_DATA", "One arm has no trades with a risk basis.")

    delta = vol.expectancy_r - legacy.expectancy_r
    if delta <= 0:
        return ("HOLD_LEGACY",
                f"vol_scaled expectancy {vol.expectancy_r}R does not beat "
                f"legacy {legacy.expectancy_r}R (delta {delta:+.4f}R). "
                "Do NOT enable live.")
    # Require vol's CI lower bound to clear legacy's point estimate — i.e. the
    # improvement is not plausibly just noise.
    if vol.expectancy_r_ci_low is None or vol.expectancy_r_ci_low <= legacy.expectancy_r:
        return ("HOLD_INCONCLUSIVE",
                f"vol_scaled expectancy {vol.expectancy_r}R > legacy "
                f"{legacy.expectancy_r}R (delta {delta:+.4f}R), but the 95% CI "
                f"lower bound ({vol.expectancy_r_ci_low}R) does not clear "
                "legacy. Improvement not yet distinguishable from noise. "
                "Keep soaking; do NOT enable live.")
    return ("VOL_SCALED_FAVORED",
            f"vol_scaled expectancy {vol.expectancy_r}R beats legacy "
            f"{legacy.expectancy_r}R (delta {delta:+.4f}R) and its 95% CI "
            f"lower bound ({vol.expectancy_r_ci_low}R) clears legacy's point "
            "estimate. Evidence supports proceeding to a PR-G2 live-enable "
            "review. This is a recommendation to REVIEW, not to auto-enable.")


# ── DB access (read-only) ────────────────────────────────────────────────────
def _fetch_ladder_modes_by_position(client_filter: Optional[str], lookback_days: int) -> dict:
    """position_id -> 'vol_scaled' if any of its ledger rows stamped vol_scaled,
    else 'legacy'. Read-only."""
    from ap.db import conn, run_with_retry
    def _fn():
        with conn() as c:
            sql = """
                SELECT position_id, metadata
                FROM exit_decision_ledger
                WHERE created_at >= NOW() - (%s || ' days')::interval
                  AND position_id IS NOT NULL AND position_id <> ''
            """
            params: list = [str(int(lookback_days))]
            if client_filter:
                sql += " AND client_id = %s"
                params.append(client_filter)
            c.execute(sql, tuple(params))
            return c.fetchall()
    rows = run_with_retry(_fn)
    return attribute_ladder_modes(rows)


def attribute_ladder_modes(rows: list) -> dict:
    """Pure: ledger rows -> {position_id: 'vol_scaled'|'legacy'}.

    A position is 'vol_scaled' iff ANY of its ledger rows carries
    metadata.ladder.ladder_mode == 'vol_scaled' (sticky — vol presence wins),
    regardless of WHICH exit path produced that row. This is why PR-G must
    stamp _ladder on every exit path (target/hard-stop/EOD/HOLD, not just
    scale/window): a vol_scaled position that exits via hard stop or EOD still
    has vol_scaled ledger rows, so it is correctly attributed here. Factored
    out of DB access so it is unit-testable without a database.
    """
    modes: dict = {}
    for row in rows:
        pid = str(row.get("position_id") or "")
        if not pid:
            continue
        meta = row.get("metadata")
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        mode = ""
        if isinstance(meta, dict):
            ladder = meta.get("ladder")
            if isinstance(ladder, dict):
                mode = str(ladder.get("ladder_mode") or "")
        if mode == "vol_scaled":
            modes[pid] = "vol_scaled"          # sticky — vol presence wins
        elif pid not in modes:
            modes[pid] = "legacy"
    return modes


def _fetch_closed_trades(client_filter: Optional[str], lookback_days: int) -> list:
    """Join trade_reviews (primary) with positions (risk basis) for closed
    trades in the window. Read-only."""
    from ap.db import conn, run_with_retry
    def _fn():
        with conn() as c:
            sql = """
                SELECT
                    tr.position_id           AS position_id,
                    tr.client_id             AS client_id,
                    tr.symbol                AS ticker,
                    tr.pnl_dollars           AS pnl_dollars,
                    tr.pnl_pct               AS pnl_pct,
                    tr.mfe_pct               AS mfe_pct,
                    tr.mae_pct               AS mae_pct,
                    tr.exit_reason_code      AS exit_reason_code,
                    tr.exit_ts               AS exit_ts,
                    tr.git_commit            AS git_commit,
                    tr.entry_price           AS tr_entry_price,
                    tr.qty                   AS tr_qty,
                    p.entry_price            AS p_entry_price,
                    p.avg_fill               AS p_avg_fill,
                    p.qty                    AS p_qty,
                    p.sl_pct                 AS sl_pct,
                    p.realized_pnl           AS p_realized_pnl,
                    p.execution_mode         AS execution_mode
                FROM trade_reviews tr
                LEFT JOIN positions p
                       ON p.id = tr.position_id AND p.client_id = tr.client_id
                WHERE tr.exit_ts >= NOW() - (%s || ' days')::interval
            """
            params: list = [str(int(lookback_days))]
            if client_filter:
                sql += " AND tr.client_id = %s"
                params.append(client_filter)
            c.execute(sql, tuple(params))
            return c.fetchall()
    return run_with_retry(_fn)


def _to_float(v) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def build_trade_records(rows: list, modes: dict) -> tuple:
    """Rows → (TradeRecord list, excluded_no_risk_basis). Pure given inputs."""
    records: list = []
    excluded = 0
    for row in rows:
        pid = str(row.get("position_id") or "")
        realized = _to_float(row.get("pnl_dollars"))
        if realized is None:
            realized = _to_float(row.get("p_realized_pnl"))
        entry = _to_float(row.get("tr_entry_price")) or _to_float(row.get("p_avg_fill")) \
            or _to_float(row.get("p_entry_price"))
        qty = row.get("tr_qty") or row.get("p_qty") or 0
        try:
            qty = int(qty or 0)
        except (TypeError, ValueError):
            qty = 0
        sl_pct = _to_float(row.get("sl_pct"))

        planned_risk = None
        realized_r = None
        # entry_price stored per-share for options → ×100 for per-contract cost
        if entry and entry > 0 and qty > 0 and sl_pct and sl_pct > 0:
            planned_risk = entry * 100.0 * qty * sl_pct
            if planned_risk > 0 and realized is not None:
                realized_r = realized / planned_risk
        if realized_r is None:
            excluded += 1

        records.append(TradeRecord(
            position_id=pid,
            client_id=str(row.get("client_id") or ""),
            ticker=str(row.get("ticker") or ""),
            ladder_mode=modes.get(pid, "legacy"),
            realized_pnl=realized,
            planned_risk=round(planned_risk, 2) if planned_risk else None,
            realized_r=round(realized_r, 4) if realized_r is not None else None,
            pnl_pct=_to_float(row.get("pnl_pct")),
            mfe_pct=_to_float(row.get("mfe_pct")),
            mae_pct=_to_float(row.get("mae_pct")),
            exit_reason_code=str(row.get("exit_reason_code") or "UNKNOWN"),
            exit_ts=str(row.get("exit_ts") or ""),
            git_commit=str(row.get("git_commit") or ""),
        ))
    return records, excluded


def generate_comparison(
    lookback_days: int = 30,
    client_filter: Optional[str] = None,
) -> ComparisonReport:
    """Top-level entry: build the legacy-vs-vol_scaled R-multiple comparison.
    Read-only. Returns a ComparisonReport (also renderable via to_markdown)."""
    modes = _fetch_ladder_modes_by_position(client_filter, lookback_days)
    rows = _fetch_closed_trades(client_filter, lookback_days)
    records, excluded = build_trade_records(rows, modes)

    legacy_trades = [r for r in records if r.ladder_mode != "vol_scaled"]
    vol_trades = [r for r in records if r.ladder_mode == "vol_scaled"]
    legacy = compute_arm_stats("legacy", legacy_trades)
    vol = compute_arm_stats("vol_scaled", vol_trades)

    delta = None
    if vol.expectancy_r is not None and legacy.expectancy_r is not None:
        delta = round(vol.expectancy_r - legacy.expectancy_r, 4)
    verdict, detail = render_verdict(legacy, vol)

    return ComparisonReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        lookback_days=lookback_days,
        client_filter=client_filter,
        legacy=legacy,
        vol_scaled=vol,
        delta_expectancy_r=delta,
        verdict=verdict,
        verdict_detail=detail,
        excluded_no_risk_basis=excluded,
        total_trades_examined=len(records),
    )


def to_markdown(report: ComparisonReport) -> str:
    """Human-readable report suitable for pasting into the PR-G2 body."""
    def _r(v):
        return "—" if v is None else f"{v:+.4f}R" if isinstance(v, float) else str(v)
    def _p(v):
        return "—" if v is None else f"{v*100:.1f}%"
    L, V = report.legacy, report.vol_scaled
    lines = [
        "# Volatility-Scaled Exit — R-Multiple Comparison",
        "",
        f"Generated: {report.generated_at}  ·  lookback: {report.lookback_days}d"
        + (f"  ·  client: {report.client_filter}" if report.client_filter else "  ·  all clients"),
        f"Trades examined: {report.total_trades_examined}  ·  "
        f"excluded (no risk basis): {report.excluded_no_risk_basis}",
        "",
        f"## VERDICT: {report.verdict}",
        "",
        report.verdict_detail,
        "",
        "## Expectancy (the number that decides live enable)",
        "",
        "| Metric | Legacy | Vol-Scaled |",
        "|---|---|---|",
        f"| Trades (n) | {L.n} | {V.n} |",
        f"| Trades with R basis | {L.n_with_r} | {V.n_with_r} |",
        f"| **Expectancy (mean R)** | {_r(L.expectancy_r)} | {_r(V.expectancy_r)} |",
        f"| Expectancy 95% CI | [{_r(L.expectancy_r_ci_low)}, {_r(L.expectancy_r_ci_high)}] "
        f"| [{_r(V.expectancy_r_ci_low)}, {_r(V.expectancy_r_ci_high)}] |",
        f"| Total R | {_r(L.total_r)} | {_r(V.total_r)} |",
        f"| Win rate | {_p(L.win_rate)} | {_p(V.win_rate)} |",
        f"| Avg win / avg loss (R) | {_r(L.avg_win_r)} / {_r(L.avg_loss_r)} "
        f"| {_r(V.avg_win_r)} / {_r(V.avg_loss_r)} |",
        f"| Median R | {_r(L.median_r)} | {_r(V.median_r)} |",
        f"| Std R | {_r(L.std_r)} | {_r(V.std_r)} |",
        f"| Avg MFE | {_p(L.avg_mfe_pct)} | {_p(V.avg_mfe_pct)} |",
        f"| Avg MAE | {_p(L.avg_mae_pct)} | {_p(V.avg_mae_pct)} |",
        f"| Total P&L ($) | {L.total_pnl_dollars} | {V.total_pnl_dollars} |",
        "",
        f"Delta expectancy (vol − legacy): {_r(report.delta_expectancy_r)}",
        "",
        "## Exit-reason breakdown",
        "",
        f"Legacy: {json.dumps(L.exit_reason_breakdown)}",
        "",
        f"Vol-Scaled: {json.dumps(V.exit_reason_breakdown)}",
        "",
        "---",
        "_Read-only report. Win rate is context only; the live-enable decision "
        "is made on expectancy in R, per the PR-G gate._",
    ]
    return "\n".join(lines)
