# ap_edge_intelligence.py -- Edge Intelligence System for Angel Precision Bot
# =============================================================================
# Three classes:
#   APTradeLogger     – writes completed trades to ap_trade_log
#   APEdgeAnalyzer    – nightly recompute of edge buckets from trade log
#   APWhitelistEngine – manages production whitelist (promote/watch/kill)
#
# All DB ops use ap.db (conn, run_with_retry) — same pattern as rest of codebase.
# Nothing here should crash the main bot — all public methods are try/except safe.
# =============================================================================

from __future__ import annotations

import math
import uuid
import logging
import statistics
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from ap.db import conn, run_with_retry

log = logging.getLogger("ap.edge_intelligence")
ET = ZoneInfo("America/New_York")


# ── Time bucket derivation ───────────────────────────────────────────────────

def _derive_time_bucket(ts: datetime | str | None) -> str:
    """
    Map entry timestamp to a market session bucket (all times ET).
    09:30-10:00  = opening drive
    10:00-11:00  = morning continuation
    11:00-13:00  = midday
    13:00-15:00  = afternoon
    15:00-16:00  = power hour
    """
    if ts is None:
        return "unknown"
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts)
        except (ValueError, TypeError):
            return "unknown"
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    et = ts.astimezone(ET)
    h, m = et.hour, et.minute
    if h == 9 and m >= 30:
        return "09:30-10:00"
    if h == 10:
        return "10:00-11:00"
    if h in (11, 12):
        return "11:00-13:00"
    if h in (13, 14):
        return "13:00-15:00"
    if h == 15:
        return "15:00-16:00"
    return "outside_hours"


def _derive_day_of_week(ts: datetime | str | None) -> str:
    if ts is None:
        return "unknown"
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts)
        except (ValueError, TypeError):
            return "unknown"
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    et = ts.astimezone(ET)
    return et.strftime("%A")  # Monday, Tuesday, ...


# =============================================================================
# APTradeLogger
# =============================================================================

class APTradeLogger:
    """
    Writes completed trades to ap_trade_log.
    Called by the exit engine when a position closes.
    """

    def log_trade(self, position: dict, exit_info: dict, client_id: str) -> str:
        """
        Build full trade record from position + exit info.
        Returns trade_id.

        Derives:
        - time_bucket from entry_ts hour/minute
        - day_of_week from entry_ts
        - r_multiple from (pnl / (entry_price * contracts * 100 * stop_pct))
        - win_flag from net_pnl > 0
        - setup_combo from position signal pattern or payload pattern_id
        - scanner_type from signal source field
        """
        trade_id = str(uuid.uuid4())

        # Extract fields from position dict
        ticker = position.get("ticker") or position.get("underlying") or position.get("symbol") or ""
        direction = position.get("direction") or position.get("side") or ""
        timeframe = position.get("timeframe") or "1d"
        entry_ts = position.get("entry_ts") or position.get("opened_at")
        entry_price = float(position.get("entry_price") or position.get("avg_fill") or 0)
        underlying_entry = float(position.get("underlying_entry") or 0)
        contracts = int(position.get("contracts") or position.get("quantity") or position.get("qty") or 1)
        contract_symbol = position.get("option_symbol") or position.get("contract") or ""
        planned_stop = float(position.get("planned_stop") or position.get("stop_underlying") or position.get("underlying_stop") or 0)
        planned_target = float(position.get("planned_target") or position.get("target_underlying") or position.get("underlying_target") or 0)
        score = float(position.get("score") or 0)
        tier = position.get("tier") or ""
        signal_id_raw = position.get("signal_id") or ""

        # Signal / setup identity
        sig = position.get("signal") or {}
        setup_combo = (
            position.get("setup_combo")
            or position.get("pattern")
            or sig.get("pattern_id")
            or sig.get("pattern")
            or ""
        )
        scanner_type = sig.get("scanner_type") or sig.get("source") or ""
        trigger_price = float(sig.get("trigger", {}).get("entry", 0) or 0) if isinstance(sig.get("trigger"), dict) else 0
        spread_at_entry = float(position.get("spread_pct") or sig.get("spread_pct") or 0)

        # Exit info
        exit_price = float(exit_info.get("exit_price") or 0)
        exit_reason = exit_info.get("exit_reason") or exit_info.get("reason") or ""
        exit_ts = exit_info.get("exit_ts")
        underlying_exit = float(exit_info.get("underlying_exit") or position.get("current_underlying") or 0)

        # Derived fields
        time_bucket = _derive_time_bucket(entry_ts)
        day_of_week = _derive_day_of_week(entry_ts)

        # PnL
        gross_pnl = (exit_price - entry_price) * contracts * 100 if entry_price else 0
        net_pnl = gross_pnl  # no commission model yet
        return_pct = ((exit_price - entry_price) / entry_price * 100) if entry_price else 0
        win_flag = 1 if net_pnl > 0 else 0

        # R-multiple: pnl per contract / risk per contract
        # Risk = |entry - stop| / entry * entry_price * 100 (options risk proxy)
        r_multiple = None
        if planned_stop and underlying_entry and entry_price:
            stop_distance_pct = abs(underlying_entry - planned_stop) / underlying_entry
            risk_per_contract = entry_price * 100 * stop_distance_pct
            if risk_per_contract > 0:
                pnl_per_contract = (exit_price - entry_price) * 100
                r_multiple = pnl_per_contract / risk_per_contract

        # Parse signal_id to UUID or None
        signal_id_val = None
        if signal_id_raw:
            try:
                signal_id_val = str(uuid.UUID(str(signal_id_raw)))
            except (ValueError, AttributeError):
                signal_id_val = None

        # Derive 6 intelligence fields from signal payload
        import json as _json
        _setup_reason      = sig.get("setup_reason") or f"{setup_combo} on {timeframe}"
        _trigger_reason    = sig.get("trigger_reason") or (
            f"breach at {trigger_price}" if trigger_price else exit_reason
        )
        _entry_reason      = sig.get("entry_reason") or (
            f"score={score:.0f} tier={tier} setup={setup_combo}"
        )
        _confluence_json   = _json.dumps(sig.get("confluence") or {
            "timeframe":   timeframe,
            "setup_combo": setup_combo,
            "scanner":     scanner_type,
            "score":       score,
            "tier":        tier,
            "direction":   direction,
        })
        _htf_alignment     = sig.get("htf_alignment") or sig.get("spy_trend") or None
        _liquidity_context = sig.get("liquidity_context") or sig.get("vol_regime") or None

        def _insert():
            with conn() as c:
                c.execute(
                    """
                    INSERT INTO ap_trade_log (
                        trade_id, signal_id, client_id, date, entry_ts, exit_ts,
                        ticker, direction, timeframe, setup_combo, scanner_type,
                        score, tier, time_bucket, day_of_week,
                        trigger_price, underlying_entry, contract_symbol,
                        contract_entry_price, contracts, planned_stop, planned_target,
                        underlying_exit, contract_exit_price,
                        gross_pnl, net_pnl, return_pct, r_multiple, win_flag,
                        exit_reason, spread_at_entry,
                        setup_reason, trigger_reason, entry_reason,
                        confluence_json, htf_alignment, liquidity_context
                    ) VALUES (
                        %s, %s, %s, CURRENT_DATE, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s
                    )
                    """,
                    (
                        trade_id, signal_id_val, client_id, entry_ts, exit_ts,
                        ticker, direction, timeframe, setup_combo, scanner_type,
                        score, tier, time_bucket, day_of_week,
                        trigger_price or None, underlying_entry or None, contract_symbol,
                        entry_price or None, contracts, planned_stop or None, planned_target or None,
                        underlying_exit or None, exit_price or None,
                        gross_pnl, net_pnl, return_pct, r_multiple, win_flag,
                        exit_reason, spread_at_entry or None,
                        _setup_reason, _trigger_reason, _entry_reason,
                        _confluence_json, _htf_alignment, _liquidity_context,
                    ),
                )

        try:
            run_with_retry(_insert)
            log.info(f"[{ticker}] Trade logged | id={trade_id} pnl=${net_pnl:+.2f} r={r_multiple or 0:.2f}R")
        except Exception as e:
            log.warning(f"[{ticker}] Trade log insert failed (non-critical): {e}")

        return trade_id


# =============================================================================
# APEdgeAnalyzer
# =============================================================================

class APEdgeAnalyzer:
    """
    Nightly job that recomputes bucket stats from ap_trade_log.
    Buckets: ticker, timeframe, setup (setup_combo), time_of_day (time_bucket),
             combo (ticker+direction+timeframe+setup_combo+time_bucket)
    """

    BUCKET_DIMENSIONS = [
        ("ticker", "ticker"),
        ("timeframe", "timeframe"),
        ("setup", "setup_combo"),
        ("time_of_day", "time_bucket"),
        ("direction", "direction"),
    ]

    def run_nightly(self, client_id: str) -> dict:
        """
        Recompute all edge buckets for client.
        Returns summary of what changed.
        """
        trades = self._fetch_all_trades(client_id)
        if not trades:
            log.info(f"[{client_id}] No trades found for edge analysis")
            return {"client_id": client_id, "buckets_updated": 0, "total_trades": 0}

        results = []

        # Single-dimension buckets
        for bucket_type, field in self.BUCKET_DIMENSIONS:
            groups: dict[str, list] = {}
            for t in trades:
                key = t.get(field) or "unknown"
                groups.setdefault(key, []).append(t)
            for key, group in groups.items():
                stats = self._compute_bucket(group)
                stats["bucket_type"] = bucket_type
                stats["bucket_key"] = key
                stats["status"] = self._label_bucket(stats)
                results.append(stats)

        # Combo bucket: ticker+direction+timeframe+setup_combo+time_bucket
        combo_groups: dict[str, list] = {}
        for t in trades:
            combo_key = "|".join([
                t.get("ticker") or "?",
                t.get("direction") or "?",
                t.get("timeframe") or "?",
                t.get("setup_combo") or "?",
                t.get("time_bucket") or "?",
            ])
            combo_groups.setdefault(combo_key, []).append(t)
        for key, group in combo_groups.items():
            stats = self._compute_bucket(group)
            stats["bucket_type"] = "combo"
            stats["bucket_key"] = key
            stats["status"] = self._label_bucket(stats)
            results.append(stats)

        # Upsert all bucket results
        updated = self._upsert_buckets(client_id, results)

        summary = {
            "client_id": client_id,
            "total_trades": len(trades),
            "buckets_updated": updated,
            "results": results,
        }
        log.info(f"[{client_id}] Edge analysis complete | {len(trades)} trades, {updated} buckets updated")
        return summary

    def _fetch_all_trades(self, client_id: str) -> list[dict]:
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM ap_trade_log WHERE client_id = %s ORDER BY date DESC",
                    (client_id,),
                )
                return c.fetchall()
        try:
            return run_with_retry(_fn)
        except Exception as e:
            log.warning(f"Failed to fetch trades for edge analysis: {e}")
            return []

    def _compute_bucket(self, trades: list[dict]) -> dict:
        """
        Given a list of trades, compute edge statistics.
        """
        n = len(trades)
        if n == 0:
            return {
                "n_trades": 0, "win_rate": 0, "avg_win": 0, "avg_loss": 0,
                "profit_factor": 0, "expectancy": 0, "edge_score": 0,
                "max_drawdown": 0, "median_return": 0, "std_dev_return": 0,
            }

        returns = [float(t.get("net_pnl") or 0) for t in trades]
        wins = [r for r in returns if r > 0]
        losses = [r for r in returns if r <= 0]

        win_rate = len(wins) / n if n else 0
        avg_win = statistics.mean(wins) if wins else 0
        avg_loss = statistics.mean(losses) if losses else 0

        sum_wins = sum(wins)
        sum_losses = abs(sum(losses))
        profit_factor = (sum_wins / sum_losses) if sum_losses > 0 else (float("inf") if sum_wins > 0 else 0)

        # Expectancy = (win_rate * avg_win) - ((1 - win_rate) * abs(avg_loss))
        expectancy = (win_rate * avg_win) - ((1 - win_rate) * abs(avg_loss))

        # Return distribution
        return_pcts = [float(t.get("return_pct") or 0) for t in trades]
        median_return = statistics.median(return_pcts) if return_pcts else 0
        std_dev_return = statistics.stdev(return_pcts) if len(return_pcts) >= 2 else 0

        # Edge score = expectancy * ln(n + 1) * stability_factor
        # stability_factor = 1 / (1 + std_dev_return)
        stability_factor = 1.0 / (1.0 + std_dev_return)
        edge_score = expectancy * math.log(n + 1) * stability_factor

        # Max drawdown (peak-to-trough on cumulative PnL curve)
        max_drawdown = self._calc_max_drawdown(returns)

        return {
            "n_trades": n,
            "win_rate": round(win_rate, 4),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else 999.0,
            "expectancy": round(expectancy, 2),
            "edge_score": round(edge_score, 4),
            "max_drawdown": round(max_drawdown, 2),
            "median_return": round(median_return, 4),
            "std_dev_return": round(std_dev_return, 4),
        }

    @staticmethod
    def _calc_max_drawdown(returns: list[float]) -> float:
        if not returns:
            return 0
        peak = 0
        cumulative = 0
        max_dd = 0
        for r in returns:
            cumulative += r
            if cumulative > peak:
                peak = cumulative
            dd = peak - cumulative
            if dd > max_dd:
                max_dd = dd
        return max_dd

    def _label_bucket(self, stats: dict) -> str:
        """
        Returns: promote, watch, downgrade, kill

        Rules:
        PROMOTE: n>=25, expectancy>0, profit_factor>1.3
        KILL:    n>=15 and expectancy<0, OR profit_factor<0.9 after 20 trades
        DOWNGRADE: weak expectancy or low win rate with enough data
        WATCH:   everything else (insufficient data)
        """
        n = stats.get("n_trades", 0)
        expectancy = stats.get("expectancy", 0)
        pf = stats.get("profit_factor", 0)
        win_rate = stats.get("win_rate", 0)

        # PROMOTE: strong edge with enough data
        if n >= 25 and expectancy > 0 and pf > 1.3:
            return "promote"

        # KILL: proven negative edge
        if n >= 15 and expectancy < 0:
            return "kill"
        if n >= 20 and pf < 0.9:
            return "kill"

        # DOWNGRADE: marginal edge
        if n >= 15 and (expectancy <= 0 or win_rate < 0.40 or pf < 1.0):
            return "downgrade"

        # WATCH: not enough data or inconclusive
        return "watch"

    def _upsert_buckets(self, client_id: str, results: list[dict]) -> int:
        count = 0

        def _fn():
            nonlocal count
            with conn() as c:
                for r in results:
                    c.execute(
                        """
                        INSERT INTO ap_edge_buckets (
                            client_id, bucket_type, bucket_key,
                            n_trades, win_rate, avg_win, avg_loss,
                            profit_factor, expectancy, edge_score,
                            max_drawdown, median_return, std_dev_return,
                            status, last_computed_at
                        ) VALUES (
                            %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s,
                            %s, %s, %s,
                            %s, NOW()
                        )
                        ON CONFLICT (client_id, bucket_type, bucket_key)
                        DO UPDATE SET
                            n_trades = EXCLUDED.n_trades,
                            win_rate = EXCLUDED.win_rate,
                            avg_win = EXCLUDED.avg_win,
                            avg_loss = EXCLUDED.avg_loss,
                            profit_factor = EXCLUDED.profit_factor,
                            expectancy = EXCLUDED.expectancy,
                            edge_score = EXCLUDED.edge_score,
                            max_drawdown = EXCLUDED.max_drawdown,
                            median_return = EXCLUDED.median_return,
                            std_dev_return = EXCLUDED.std_dev_return,
                            status = EXCLUDED.status,
                            last_computed_at = NOW()
                        """,
                        (
                            client_id, r["bucket_type"], r["bucket_key"],
                            r["n_trades"], r["win_rate"], r["avg_win"], r["avg_loss"],
                            r["profit_factor"], r["expectancy"], r["edge_score"],
                            r["max_drawdown"], r["median_return"], r["std_dev_return"],
                            r["status"],
                        ),
                    )
                    count += 1

        try:
            run_with_retry(_fn)
        except Exception as e:
            log.warning(f"Edge bucket upsert failed: {e}")
        return count


# =============================================================================
# APWhitelistEngine
# =============================================================================

class APWhitelistEngine:
    """
    Manages the production whitelist.
    Updates after nightly analysis. Controls which setups are auto-traded.
    """

    # Below this total trade count, we're still collecting data — allow everything
    COLLECTION_PHASE_THRESHOLD = 50

    def update_whitelist(self, client_id: str, bucket_results: list[dict]):
        """
        Upsert whitelist rows based on bucket analysis.
        Only combo buckets map directly to whitelist rows.
        """
        combo_results = [r for r in bucket_results if r.get("bucket_type") == "combo"]
        if not combo_results:
            return

        def _fn():
            with conn() as c:
                for r in combo_results:
                    key = r.get("bucket_key", "")
                    parts = key.split("|")
                    if len(parts) != 5:
                        continue
                    ticker, direction, timeframe, setup_combo, time_bucket = parts

                    # Map edge label to whitelist status
                    edge_status = r.get("status", "watch")
                    if edge_status == "promote":
                        wl_status = "active"
                    elif edge_status == "kill":
                        wl_status = "killed"
                    else:
                        wl_status = "watch"

                    c.execute(
                        """
                        INSERT INTO ap_whitelist (
                            client_id, ticker, timeframe, setup_combo,
                            direction, time_bucket, status,
                            edge_score, n_trades, last_reviewed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (client_id, ticker, timeframe, setup_combo, direction, time_bucket)
                        DO UPDATE SET
                            status = EXCLUDED.status,
                            edge_score = EXCLUDED.edge_score,
                            n_trades = EXCLUDED.n_trades,
                            last_reviewed_at = NOW()
                        """,
                        (
                            client_id, ticker, timeframe, setup_combo,
                            direction, time_bucket, wl_status,
                            r.get("edge_score", 0), r.get("n_trades", 0),
                        ),
                    )

        try:
            run_with_retry(_fn)
            log.info(f"[{client_id}] Whitelist updated | {len(combo_results)} combos processed")
        except Exception as e:
            log.warning(f"Whitelist update failed: {e}")

    def is_allowed(
        self,
        client_id: str,
        ticker: str,
        timeframe: str,
        setup_combo: str,
        direction: str,
        time_bucket: str,
    ) -> tuple[bool, str]:
        """
        Check if a trade is on the whitelist.
        Returns (allowed: bool, status: str)

        During paper/collection phase (n_trades < 50 across all combos):
        - Always returns (True, "collection_phase") — collect all data

        During production phase:
        - Only allow if status='active' in whitelist
        - Return (False, "not_whitelisted") otherwise
        """
        def _fn():
            with conn() as c:
                # Check total trade count for collection phase detection
                c.execute(
                    "SELECT COUNT(*) AS cnt FROM ap_trade_log WHERE client_id = %s",
                    (client_id,),
                )
                row = c.fetchone()
                total_trades = int(row["cnt"]) if row else 0

                if total_trades < self.COLLECTION_PHASE_THRESHOLD:
                    return (True, "collection_phase")

                # Production phase: check whitelist
                c.execute(
                    """
                    SELECT status FROM ap_whitelist
                    WHERE client_id = %s
                      AND ticker = %s
                      AND timeframe = %s
                      AND setup_combo = %s
                      AND direction = %s
                      AND time_bucket = %s
                    LIMIT 1
                    """,
                    (client_id, ticker, timeframe, setup_combo, direction, time_bucket),
                )
                wl_row = c.fetchone()
                if wl_row and wl_row["status"] == "active":
                    return (True, "active")
                if wl_row:
                    return (False, wl_row["status"])
                return (False, "not_whitelisted")

        try:
            return run_with_retry(_fn)
        except Exception as e:
            log.warning(f"Whitelist check failed (allowing trade): {e}")
            return (True, "error_fallback")

    def get_summary(self, client_id: str) -> dict:
        """
        Returns counts by status for dashboard display.
        {active: N, watch: N, killed: N, total_trades: N}
        """
        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT status, COUNT(*) AS cnt
                    FROM ap_whitelist
                    WHERE client_id = %s
                    GROUP BY status
                    """,
                    (client_id,),
                )
                rows = c.fetchall()
                summary = {"active": 0, "watch": 0, "killed": 0}
                for row in rows:
                    s = row["status"]
                    if s in summary:
                        summary[s] = int(row["cnt"])

                c.execute(
                    "SELECT COUNT(*) AS cnt FROM ap_trade_log WHERE client_id = %s",
                    (client_id,),
                )
                t_row = c.fetchone()
                summary["total_trades"] = int(t_row["cnt"]) if t_row else 0
                return summary

        try:
            return run_with_retry(_fn)
        except Exception as e:
            log.warning(f"Whitelist summary failed: {e}")
            return {"active": 0, "watch": 0, "killed": 0, "total_trades": 0}
